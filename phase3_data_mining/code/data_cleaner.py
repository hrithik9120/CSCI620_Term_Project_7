#!/usr/bin/env python3
"""
Reddit May 2015 – Phase III Data Cleaning Script (PostgreSQL → *_cleaned)

Group 7: Hrithik Gaikwad, Jie Zhang, Siddharth Bhople

This script:

  • Reads data from the existing PostgreSQL tables:
        Users, Subreddit, Post, Comment, Moderation
  • Applies the data-cleaning rules identified in Phase III
  • Writes cleaned rows into new tables:
        Users_cleaned, Subreddit_cleaned, Post_cleaned, Comment_cleaned, Moderation_cleaned
  • Does NOT modify or delete any data from the original tables
  • Supports an optional --sample flag to process only the first N rows per table
  • Uses batch inserts + server-side cursors for scalability
  • Logs summary statistics for drops/fixes per table to cleaning_phase3.log

You can safely re-run this script:
  - Each run DROPs and recreates the *_cleaned tables before inserting.
"""

import argparse
import logging
from collections import Counter

import psycopg2
import psycopg2.extras
from tqdm import tqdm

# ---------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------
logging.basicConfig(
    filename="data_cleaner.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# PostgreSQL Helpers
# ---------------------------------------------------------------------
def connect_postgres(args):
    """Create and return a PostgreSQL connection."""
    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname,
    )
    conn.autocommit = False  # we manage commits explicitly
    logger.info("Connected to PostgreSQL database '%s' on %s:%s", args.dbname, args.host, args.port)
    return conn


def create_clean_tables(conn):
    """
    Drop and recreate *_cleaned tables.

    These tables intentionally have no foreign-key constraints so that
    cleaning can drop / null-out inconsistent rows without constraint failures.
    """
    with conn.cursor() as cur:
        logger.info("Dropping existing *_cleaned tables (if any).")
        cur.execute("DROP TABLE IF EXISTS Users_cleaned CASCADE;")
        cur.execute("DROP TABLE IF EXISTS Subreddit_cleaned CASCADE;")
        cur.execute("DROP TABLE IF EXISTS Post_cleaned CASCADE;")
        cur.execute("DROP TABLE IF EXISTS Comment_cleaned CASCADE;")
        cur.execute("DROP TABLE IF EXISTS Moderation_cleaned CASCADE;")

        logger.info("Creating fresh *_cleaned tables.")
        cur.execute("""
            CREATE TABLE Users_cleaned (
                author TEXT PRIMARY KEY,
                author_flair_text TEXT,
                author_flair_css_class TEXT
            );
        """)

        cur.execute("""
            CREATE TABLE Subreddit_cleaned (
                subreddit_id TEXT PRIMARY KEY,
                subreddit TEXT
            );
        """)

        cur.execute("""
            CREATE TABLE Post_cleaned (
                link_id TEXT PRIMARY KEY,
                subreddit_id TEXT,
                author TEXT,
                created_utc INTEGER,
                archived INTEGER,
                gilded INTEGER,
                edited BIGINT
            );
        """)

        cur.execute("""
            CREATE TABLE Comment_cleaned (
                id TEXT PRIMARY KEY,
                body TEXT,
                author TEXT,
                link_id TEXT,
                parent_id TEXT,
                created_utc BIGINT,
                retrieved_on BIGINT,
                score INTEGER,
                ups INTEGER,
                downs INTEGER,
                score_hidden INTEGER,
                gilded INTEGER,
                controversiality INTEGER,
                edited BIGINT
            );
        """)

        cur.execute("""
            CREATE TABLE Moderation_cleaned (
                mod_action_id SERIAL PRIMARY KEY,
                target_type TEXT,
                target_id TEXT,
                subreddit_id TEXT,
                removal_reason TEXT,
                distinguished TEXT,
                action_timestamp BIGINT
            );
        """)

    conn.commit()
    logger.info("Created *_cleaned tables successfully.")


def count_rows(conn, table_name):
    """Return total number of rows in a given table."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        (count,) = cur.fetchone()
    return count


def stream_table(conn, cursor_name, select_sql, sample=None, batch_size=50000):
    """
    Generator that streams rows from PostgreSQL using a server-side cursor.

    - cursor_name: name for the server-side cursor
    - select_sql: base SELECT statement (without LIMIT)
    - sample: if provided, adds LIMIT sample
    """
    cur = conn.cursor(name=cursor_name)
    if sample and sample > 0:
        select_sql = f"{select_sql} LIMIT %s"
        cur.execute(select_sql, (sample,))
    else:
        cur.execute(select_sql)

    cur.itersize = batch_size

    try:
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            yield rows
    finally:
        cur.close()


# ---------------------------------------------------------------------
# Cleaning Helpers (per-row)
# Each cleaner updates a stats Counter and returns either:
#   - cleaned_row (tuple)  OR
#   - None (drop row)
# ---------------------------------------------------------------------
def clean_user_row(row, stats: Counter):
    """
    Clean a Users row.

    Rules:
      - Drop row if author is NULL or empty
      - If author_flair_text is 'nonsense' (starts with only symbols), set to NULL
    """
    stats["seen"] += 1
    author, flair_text, flair_css = row

    if author is None or author.strip() == "":
        stats["dropped_missing_author"] += 1
        return None

    if flair_text:
        stripped = flair_text.strip()
        # Treat as dirty if flair starts with symbol chars only
        if all(c in "!@#$%^&*()[]{}<>?/\\|~`" for c in stripped):
            stats["flair_symbols_only_to_null"] += 1
            flair_text = None

    stats["kept"] += 1
    return (author, flair_text, flair_css)


def clean_subreddit_row(row, stats: Counter):
    """
    Clean a Subreddit row.

    Rules:
      - Drop if subreddit_id is NULL/empty
      - Keep everything else as-is
    """
    stats["seen"] += 1
    subreddit_id, subreddit = row

    if subreddit_id is None or subreddit_id.strip() == "":
        stats["dropped_missing_subreddit_id"] += 1
        return None

    stats["kept"] += 1
    return (subreddit_id, subreddit)


def clean_post_row(row, stats: Counter):
    """
    Clean a Post row.

    Rules:
      - Drop if subreddit_id is NULL/empty
      - Drop if created_utc is outside [1100000000, 1800000000] when non-null
      - If edited < 0, set edited = NULL
    """
    stats["seen"] += 1
    link_id, subreddit_id, author, created_utc, archived, gilded, edited = row

    if subreddit_id is None or subreddit_id.strip() == "":
        stats["dropped_missing_subreddit_id"] += 1
        return None

    if created_utc is not None and (created_utc < 1100000000 or created_utc > 1800000000):
        stats["dropped_invalid_created_utc"] += 1
        return None

    if edited is not None and edited < 0:
        stats["edited_negative_to_null"] += 1
        edited = None

    stats["kept"] += 1
    return (link_id, subreddit_id, author, created_utc, archived, gilded, edited)


def clean_comment_row(row, stats: Counter):
    """
    Clean a Comment row.

    Rules:
      - Drop if link_id is NULL/empty
      - If author is empty string, set to NULL (consistent with ON DELETE SET NULL)
      - Drop if body is NULL/empty
      - Drop if body is exactly '[deleted]' or '[removed]'
      - Drop if created_utc is outside [1100000000, 1800000000] when non-null
      - If score, ups, downs are all non-null and inconsistent, fix score = ups - downs
    """
    stats["seen"] += 1
    (
        cid, body, author, link_id, parent_id, created_utc, retrieved_on,
        score, ups, downs, score_hidden, gilded, controversiality, edited
    ) = row

    if link_id is None or link_id.strip() == "":
        stats["dropped_missing_link_id"] += 1
        return None

    if author is not None and author.strip() == "":
        stats["author_empty_to_null"] += 1
        author = None

    # body missing or unusable
    if body is None or body.strip() == "":
        stats["dropped_missing_body"] += 1
        return None

    if body in ("[deleted]", "[removed]"):
        stats["dropped_deleted_or_removed"] += 1
        return None

    if created_utc is not None and (created_utc < 1100000000 or created_utc > 1800000000):
        stats["dropped_invalid_created_utc"] += 1
        return None

    if score is not None and ups is not None and downs is not None:
        expected_score = ups - downs
        if score != expected_score:
            stats["score_fixed_from_ups_downs"] += 1
            score = expected_score

    stats["kept"] += 1
    return (
        cid, body, author, link_id, parent_id, created_utc, retrieved_on,
        score, ups, downs, score_hidden, gilded, controversiality, edited
    )


def clean_moderation_row(row, stats: Counter):
    """
    Clean a Moderation row.

    For this project, the moderation data is relatively small and synthetic.
    We apply minimal checks:

      - Drop if target_id is NULL/empty
      - Drop if subreddit_id is NULL/empty
      - Keep all other fields as-is
    """
    stats["seen"] += 1
    (mod_action_id, target_type, target_id,
     subreddit_id, removal_reason, distinguished, action_timestamp) = row

    if target_id is None or target_id.strip() == "":
        stats["dropped_missing_target_id"] += 1
        return None

    if subreddit_id is None or subreddit_id.strip() == "":
        stats["dropped_missing_subreddit_id"] += 1
        return None

    stats["kept"] += 1
    return (
        mod_action_id, target_type, target_id,
        subreddit_id, removal_reason, distinguished, action_timestamp
    )

def parse_args():
    parser = argparse.ArgumentParser(
        description="Reddit May 2015 – Phase III Data Cleaning Script"
    )

    parser.add_argument('--host', default='localhost',
                        help='PostgreSQL server host (default: localhost)')
    parser.add_argument('--port', default='5432',
                        help='PostgreSQL server port (default: 5432)')
    parser.add_argument('--user', default='postgres',
                        help='PostgreSQL username (default: postgres)')
    parser.add_argument('--password', required=True,
                        help='PostgreSQL password (required)')
    parser.add_argument('--dbname', required=True,
                        help='PostgreSQL database name (required)')

    parser.add_argument('--sample', type=int,
                        help='Process only first N rows per table (for testing, optional)')
    parser.add_argument('--batch-size', type=int, default=50000,
                        help='Batch size for streaming & inserts (default: 50000)')

    return parser.parse_args()

def main():
    args = parse_args()
    conn = connect_postgres(args)
    try:
        create_clean_tables(conn)

        # Configure per-table processing
        TABLE_CONFIG = [
            {
                "name": "Users",
                "select_sql": "SELECT author, author_flair_text, author_flair_css_class FROM Users",
                "insert_sql": """
                    INSERT INTO Users_cleaned(author, author_flair_text, author_flair_css_class)
                    VALUES (%s, %s, %s)
                """,
                "cleaner": clean_user_row,
            },
            {
                "name": "Subreddit",
                "select_sql": "SELECT subreddit_id, subreddit FROM Subreddit",
                "insert_sql": """
                    INSERT INTO Subreddit_cleaned(subreddit_id, subreddit)
                    VALUES (%s, %s)
                """,
                "cleaner": clean_subreddit_row,
            },
            {
                "name": "Post",
                "select_sql": """
                    SELECT link_id, subreddit_id, author, created_utc, archived, gilded, edited
                    FROM Post
                """,
                "insert_sql": """
                    INSERT INTO Post_cleaned(link_id, subreddit_id, author,
                                             created_utc, archived, gilded, edited)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                "cleaner": clean_post_row,
            },
            {
                "name": "Comment",
                "select_sql": """
                    SELECT id, body, author, link_id, parent_id,
                           created_utc, retrieved_on,
                           score, ups, downs, score_hidden,
                           gilded, controversiality, edited
                    FROM Comment
                """,
                "insert_sql": """
                    INSERT INTO Comment_cleaned( \
                        id, body, author, link_id, parent_id, \
                        created_utc, retrieved_on, \
                        score, ups, downs, score_hidden, \
                        gilded, controversiality, edited \
                    ) \
                    VALUES (%s, %s, %s, %s, %s, %s, %s, \
                            %s, %s, %s, %s, %s, %s, %s)
                """,
                "cleaner": clean_comment_row,
            },
            {
                "name": "Moderation",
                "select_sql": """
                    SELECT mod_action_id, target_type, target_id,
                           subreddit_id, removal_reason, distinguished, action_timestamp
                    FROM Moderation
                """,
                "insert_sql": """
                    INSERT INTO Moderation_cleaned(
                        mod_action_id, target_type, target_id,
                        subreddit_id, removal_reason, distinguished, action_timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                "cleaner": clean_moderation_row,
            },
        ]

        for cfg in TABLE_CONFIG:
            table_name = cfg["name"]
            logger.info("=== Cleaning table: %s ===", table_name)
            print(f"\n Cleaning {table_name}...")

            total_rows = count_rows(conn, table_name)
            if args.sample and args.sample > 0 and args.sample < total_rows:
                effective_total = args.sample
            else:
                effective_total = total_rows

            logger.info(
                "Table %s: total rows in source = %d, effective limit = %s",
                table_name, total_rows, effective_total
            )

            stats = Counter()
            cleaner = cfg["cleaner"]
            insert_sql = cfg["insert_sql"]

            with conn.cursor() as cur, tqdm(
                total=effective_total,
                desc=f"{table_name}",
                unit="rows"
            ) as pbar:

                for batch in stream_table(
                    conn,
                    cursor_name=f"csr_{table_name.lower()}",
                    select_sql=cfg["select_sql"],
                    sample=args.sample,
                    batch_size=args.batch_size,
                ):
                    cleaned_batch = []
                    for row in batch:
                        cleaned = cleaner(row, stats)
                        stats["processed"] += 1
                        if cleaned is not None:
                            cleaned_batch.append(cleaned)

                    if cleaned_batch:
                        psycopg2.extras.execute_batch(
                            cur,
                            insert_sql,
                            cleaned_batch,
                            page_size=10000
                        )
                    # progress by number of raw rows, not just kept rows
                    pbar.update(len(batch))

                conn.commit()

            logger.info("Table %s cleaning stats: %s", table_name, dict(stats))
            print(f"  -> kept {stats.get('kept', 0)} / {stats.get('seen', 0)} rows after cleaning.")

        print("\n Cleaning complete! Cleaned data loaded into *_cleaned tables.\n")
        logger.info("Data cleaning completed successfully.")

    except Exception as e:
        logger.exception("Error during cleaning. Rolling back current transaction.")
        conn.rollback()
        raise
    finally:
        conn.close()
        logger.info("PostgreSQL connection closed.")


if __name__ == "__main__":
    main()
