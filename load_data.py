#!/usr/bin/env python3
"""
Reddit May 2015 Dataset Loader (Multi-table Version, schema-aligned)

Compatible with the PostgreSQL schema where:
- Post.link_id is the primary key
- edited fields are BIGINT (timestamps)
- Moderation includes distinguished and removal_reason
"""

import argparse
import pandas as pd
import sqlite3
import psycopg2
import sys


# ----------------------------- #
# Argument Parser
# ----------------------------- #
def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Load Reddit dataset from SQLite into PostgreSQL (schema-aligned)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('--input', required=True, help='Path to SQLite database file (database.sqlite)')
    parser.add_argument('--host', default='localhost', help='PostgreSQL server host (default: localhost)')
    parser.add_argument('--port', default='5432', help='PostgreSQL server port (default: 5432)')
    parser.add_argument('--user', default='postgres', help='PostgreSQL username (default: postgres)')
    parser.add_argument('--password', required=True, help='PostgreSQL password (required)')
    parser.add_argument('--dbname', required=True, help='PostgreSQL database name (required)')
    parser.add_argument('--sample', type=int, help='Load only first N rows for testing (optional)')

    return parser.parse_args()


# ----------------------------- #
# Database Connection
# ----------------------------- #
def create_database_connection(host, port, user, password, dbname):
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
        print(f"Connected to PostgreSQL database '{dbname}' on {host}:{port}")
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        sys.exit(1)


# ----------------------------- #
# SQLite → Pandas → PostgreSQL Loader
# ----------------------------- #
def load_data(conn, sqlite_path, sqlite_table, pg_table, select_cols, insert_cols, sample_size=None):
    print(f"Loading data for table: {pg_table}")
    try:
        sqlite_conn = sqlite3.connect(sqlite_path)
        query = f"SELECT {', '.join(select_cols)} FROM {sqlite_table}"
        if sample_size:
            query += f" LIMIT {sample_size}"
        df = pd.read_sql_query(query, sqlite_conn)
        sqlite_conn.close()

        print(f"Read {len(df):,} rows from SQLite table '{sqlite_table}'")

        # -----------------------------
        # Table-specific preprocessing
        # -----------------------------
        if pg_table == "post_link":
            df = df[df["parent_id"].str.startswith("t3_", na=False)]
            df.rename(columns={"parent_id": "post_id"}, inplace=True)
            print(f"Filtered {len(df):,} rows where parent_id starts with 't3_'")

        elif pg_table == "comment":
            # first handle the parent_id
            df.loc[df["parent_id"].str.startswith("t3_", na=False), "parent_id"] = None
            print(f"Cleaned parent_id: replaced post references (t3_*) with NULLs")

            # 再过滤掉 link_id 在 post_link 表中不存在的记录
            # Then filter the link_id of record that not exist in the post_link table
            cursor = conn.cursor()
            cursor.execute("SELECT link_id FROM Post_Link")
            valid_links = set(r[0] for r in cursor.fetchall())
            before_count = len(df)
            df = df[df["link_id"].isin(valid_links)]
            print(f"Filtered out {before_count - len(df):,} invalid comments (link_id not in Post_Link)")

        elif pg_table == "moderation":
            # Identify post/comment targets
            df.loc[df["target_id"].str.startswith("t1_", na=False), "target_type"] = "comment"
            df.loc[df["target_id"].str.startswith("t3_", na=False), "target_type"] = "post"

            # Replace NaN with None for SQL compatibility
            df = df.where(pd.notnull(df), None)

            # Fill missing text fields
            df["removal_reason"] = df.get("removal_reason", None)
            df["distinguished"] = df.get("distinguished", None)

            # Logging distribution
            post_count = (df["target_type"] == "post").sum()
            comment_count = (df["target_type"] == "comment").sum()
            null_type = df["target_type"].isnull().sum()
            print(f"Moderation type stats → Post: {post_count:,}, Comment: {comment_count:,}, Null: {null_type:,}")

        # -----------------------------
        # Skip empty frames
        # -----------------------------
        if df.empty:
            print(f"No data found for {pg_table}, skipping...")
            return

        # Align column order
        df = df[insert_cols]

        # -----------------------------
        # Insert into PostgreSQL
        # -----------------------------
        insert_sql = f"""
        INSERT INTO {pg_table} ({', '.join(insert_cols)})
        VALUES ({', '.join(['%s'] * len(insert_cols))})
        ON CONFLICT DO NOTHING;
        """

        cursor = conn.cursor()
        # increase the batch size to 10000
        batch_size = 10000
        total_inserted = 0

        for i in range(0, len(df), batch_size):
            batch = [tuple(x) for x in df.iloc[i:i + batch_size].to_numpy()]
            try:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                total_inserted += len(batch)

                if total_inserted % 100000 == 0 or total_inserted == len(df):
                    print(f"   → Progress: {total_inserted:,}/{len(df):,} rows inserted into {pg_table}")
            except Exception as e:
                conn.rollback()
                print(f"Batch rollback due to error: {e}")

        print(f"Finished loading '{pg_table}' ({total_inserted:,} rows).")

    except Exception as e:
        print(f"Error loading table '{pg_table}': {e}")


# ----------------------------- #
# Main
# ----------------------------- #
def main():
    args = parse_arguments()
    conn = create_database_connection(args.host, args.port, args.user, args.password, args.dbname)

    TABLES = {
        "users": {
            "sqlite_table": "May2015",
            "select": ["author", "author_flair_text", "author_flair_css_class"],
            "insert": ["author", "author_flair_text", "author_flair_css_class"]
        },
        "subreddit": {
            "sqlite_table": "May2015",
            "select": ["subreddit_id", "subreddit"],
            "insert": ["subreddit_id", "subreddit"]
        },
        "post": {
            "sqlite_table": "May2015",
            # changed post_id -> link_id
            "select": ["link_id", "subreddit_id", "author", "created_utc", "archived", "gilded", "edited"],
            "insert": ["link_id", "subreddit_id", "author", "created_utc", "archived", "gilded", "edited"]
        },
        "post_link": {
            "sqlite_table": "May2015",
            # parent_id ≠ post_id semantically, but we keep mapping for relational integrity
            "select": ["link_id", "parent_id", "retrieved_on"],
            "insert": ["link_id", "post_id", "retrieved_on"]
        },
        "comment": {
            "sqlite_table": "May2015",
            "select": ["id", "body", "author", "link_id", "parent_id", "created_utc",
                       "retrieved_on", "score", "ups", "downs", "score_hidden", "gilded",
                       "controversiality", "edited"],
            "insert": ["id", "body", "author", "link_id", "parent_id", "created_utc",
                       "retrieved_on", "score", "ups", "downs", "score_hidden", "gilded",
                       "controversiality", "edited"]
        },
        "moderation": {
            "sqlite_table": "May2015",
            # added removal_reason to match new schema
            "select": ["id AS target_id", "subreddit_id", "'comment' AS target_type", "removal_reason",
                       "distinguished"],
            "insert": ["target_id", "subreddit_id", "target_type", "removal_reason", "distinguished"]
        }
    }

    load_order = ["users", "subreddit", "post", "post_link", "comment", "moderation"]

    print("=" * 65)
    print("Reddit May 2015 Multi-Table Data Loader (Schema-aligned)")
    print("=" * 65)

    try:
        for pg_table in load_order:
            info = TABLES[pg_table]

            load_data(
                conn,
                sqlite_path=args.input,
                sqlite_table=info["sqlite_table"],
                pg_table=pg_table,
                select_cols=info["select"],
                insert_cols=info["insert"],
                sample_size=args.sample
            )

        print("\n All tables loaded successfully!")

    except KeyboardInterrupt:
        print("\n Loading interrupted by user.")
    except Exception as e:
        print(f" Unexpected error: {e}")
    finally:
        conn.close()
        print("\n Database connection closed.")
        print("=" * 65)


if __name__ == "__main__":
    main()
