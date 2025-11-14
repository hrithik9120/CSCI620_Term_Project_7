#!/usr/bin/env python3
"""
benchmark_queries.py

Benchmark execution time of seven meaningful Reddit SQL queries
before and after creating indexes.
"""
import argparse
import time
import psycopg2
import pandas as pd

# ==========================
# Parse command-line arguments
# ==========================
def parse_args():
    parser = argparse.ArgumentParser(
        description="Benchmark Reddit SQL queries before and after index creation."
    )
    parser.add_argument("--host", default="localhost", help="Database host (default: localhost)")
    parser.add_argument("--port", default="5432", help="Database port (default: 5432)")
    parser.add_argument("--user", required=True, help="Database user")
    parser.add_argument("--password", required=True, help="Database password")
    parser.add_argument("--dbname", required=True, help="Database name")

    return parser.parse_args()

# ==========================
# Database Connection
# ==========================
def connect_db(args):
    """Connect to PostgreSQL using CLI arguments."""
    try:
        conn = psycopg2.connect(
            dbname=args.dbname,
            user=args.user,
            password=args.password,
            host=args.host,
            port=args.port
        )
        print(f"\n Connected to database '{args.dbname}' on {args.host}:{args.port}\n")
        return conn
    except Exception as e:
        print(f" Failed to connect: {e}")
        exit(1)


def run_queries(conn, label):
    """Run all test queries and record elapsed time."""
    queries = [
        ("Q1 AskReddit latest 50 posts", """
            SELECT p.link_id, p.author, s.subreddit, p.created_utc
            FROM Post p
            JOIN Subreddit s ON p.subreddit_id = s.subreddit_id
            WHERE s.subreddit = 'AskReddit'
              AND TO_TIMESTAMP(p.created_utc)
                  BETWEEN TIMESTAMP '2015-05-28 00:00:00'
                      AND TIMESTAMP '2015-05-29 00:00:00'
            ORDER BY p.created_utc DESC
            LIMIT 50;
        """),

        ("Q2 Top 20 subreddits with highest average comment score", """
            SELECT 
                p.link_id,
                s.subreddit,
                COUNT(c.id) AS comment_count
            FROM Post p
            JOIN Comment c ON p.link_id = c.link_id
            JOIN Subreddit s ON p.subreddit_id = s.subreddit_id
            WHERE TO_TIMESTAMP(p.created_utc)
                  BETWEEN TIMESTAMP '2015-05-25 00:00:00'
                      AND TIMESTAMP '2015-05-29 23:59:59'
            GROUP BY p.link_id, s.subreddit
            HAVING COUNT(c.id) > 5
            ORDER BY comment_count DESC
            LIMIT 10;

        """),

        ("Q3 Top 20 authors with the most posts overall", """
            SELECT p.author, COUNT(p.link_id) AS total_posts
            FROM Post p
            WHERE p.author IS NOT NULL
            GROUP BY p.author
            ORDER BY total_posts DESC
            LIMIT 20;
        """),

        ("Q4 Gilded but not archived posts on May 27", """
            SELECT p.link_id, p.author, s.subreddit, p.gilded, p.archived
            FROM Post p
            JOIN Subreddit s ON p.subreddit_id = s.subreddit_id
            WHERE p.gilded > 0
              AND p.archived = 0
              AND TO_TIMESTAMP(p.created_utc)
                  BETWEEN TIMESTAMP '2015-05-27 00:00:00'
                      AND TIMESTAMP '2015-05-27 23:59:59'
            ORDER BY p.created_utc DESC
            LIMIT 30;
        """),

        ("Q5 Posts by authors whose name contains 'cat'", """
            SELECT p.link_id, p.author, s.subreddit, p.created_utc
            FROM Post p
            JOIN Subreddit s ON p.subreddit_id = s.subreddit_id
            WHERE p.author LIKE '%cat%'
            ORDER BY p.created_utc DESC
            LIMIT 20;
        """),


        ("Q6 Average comments per post (top 10 subreddits)", """
            SELECT s.subreddit, ROUND(AVG(c_count.comment_count), 2) AS avg_comments_per_post
            FROM (
                SELECT p.subreddit_id, p.link_id, COUNT(c.id) AS comment_count
                FROM Post p
                LEFT JOIN Comment c ON p.link_id = c.link_id
                GROUP BY p.subreddit_id, p.link_id
            ) AS c_count
            JOIN Subreddit s ON s.subreddit_id = c_count.subreddit_id
            GROUP BY s.subreddit
            ORDER BY avg_comments_per_post DESC
            LIMIT 10;
        """)
    ]

    results = []
    cur = conn.cursor()
    for name, sql in queries:
        start = time.perf_counter()
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed = time.perf_counter() - start
        results.append((name, len(rows), round(elapsed, 4)))
        print(f"[{label}] {name:<55} -> {len(rows):>5} rows, {elapsed:.4f} s")
    cur.close()
    return results


def create_indexes(conn):
    """Create all indexes for optimization."""
    indexes = [
        # Q1: filter by subreddit + time
        "CREATE INDEX IF NOT EXISTS idx_post_subreddit_time ON Post(subreddit_id, created_utc DESC);",

        # Q2: join Comment.link_id
        "CREATE INDEX IF NOT EXISTS idx_comment_linkid ON Comment(link_id);",

        # Q3: group by author (most active authors)
        "CREATE INDEX IF NOT EXISTS idx_post_author_count ON Post(author);",

        # Q4: filter by gilded + archived + time
        "CREATE INDEX IF NOT EXISTS idx_post_gilded_archived_time ON Post(gilded, archived, created_utc);",

        # Q5: author name search (LIKE condition)
        "CREATE INDEX IF NOT EXISTS idx_post_author ON Post(author);",

        # Q6: join Post.link_id + subreddit_id
        "CREATE INDEX IF NOT EXISTS idx_post_linkid_sub ON Post(link_id, subreddit_id);"
    ]
    cur = conn.cursor()
    for idx in indexes:
        cur.execute(idx)
        print("Created:", idx.split("ON")[0].strip())
    conn.commit()
    cur.close()


def drop_indexes(conn):
    """Drop previously created indexes to restore baseline."""
    indexes = [
        "idx_post_subreddit_time",         # Q1
        "idx_comment_linkid",              # Q2
        "idx_post_author_count",           # Q3
        "idx_post_gilded_archived_time",   # Q4
        "idx_post_author",                 # Q5
        "idx_post_linkid_sub"              # Q6
    ]
    cur = conn.cursor()
    for idx in indexes:
        try:
            cur.execute(f"DROP INDEX IF EXISTS {idx};")
            print(f"Dropped index: {idx}")
        except Exception as e:
            print(f"Could not drop index {idx}: {e}")
    conn.commit()
    cur.close()




def save_to_csv(before, after):
    """Compare before and after and save to CSV."""
    df1 = pd.DataFrame(before, columns=["Query", "Rows", "Time_before"])
    df2 = pd.DataFrame(after, columns=["Query", "Rows", "Time_after"])
    df = df1.merge(df2, on="Query")
    df["Speedup"] = (df["Time_before"] / df["Time_after"]).round(2)
    df.sort_values("Speedup", ascending=False, inplace=True)
    df.to_csv("query_performance_comparison.csv", index=False)
    print("\n=== Results saved to query_performance_comparison.csv ===\n")
    print(df)


if __name__ == "__main__":
    args = parse_args()
    conn = connect_db(args)

    print("\n=== Drop indexes (restore baseline)")
    drop_indexes(conn)

    print("\n=== Running queries WITHOUT indexes ===")
    before = run_queries(conn, "Before Index")

    print("\n=== Creating indexes ===")
    create_indexes(conn)

    print("\n=== Running queries WITH indexes ===")
    after = run_queries(conn, "After Index")

    print("\n=== Comparing results and saving to CSV ===")
    save_to_csv(before, after)

    conn.close()
