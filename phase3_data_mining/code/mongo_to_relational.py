
"""
mongo_to_relational.py

Translate data from MongoDB document model (users, subreddits, posts,
comments, moderation) back into PostgreSQL relational schema.

Assumes the relational tables (Users, Subreddit, Post, Post_Link,
Comment, Moderation) already exist.
"""

import argparse
import psycopg2
from psycopg2.extras import execute_batch
from pymongo import MongoClient


def parse_args():
    p = argparse.ArgumentParser(description="Translate MongoDB -> relational DB")
    p.add_argument("--pg_dsn", required=True,
                   help="PostgreSQL DSN, e.g. 'dbname=reddit user=... password=... host=localhost'")
    p.add_argument("--mongo_uri", required=True,
                   help="MongoDB URI, e.g. 'mongodb://localhost:27017/'")
    p.add_argument("--mongo_dbname", required=True,
                   help="MongoDB database name, e.g. 'reddit_may2015'")
    p.add_argument("--batch-size", type=int, default=10000,
                   help="Batch size for bulk INSERTs (default: 10000)")
    return p.parse_args()


# ------------- Helpers -------------
def flush_batch(cur, sql, batch):
    """Execute a batch if non-empty."""
    if batch:
        execute_batch(cur, sql, batch, page_size=len(batch))
        batch.clear()


# ------------- Insert functions (batched) -------------

def insert_users(mongo_db, pg_conn, batch_size: int):
    print("Users collection -> Users table...")
    cur = pg_conn.cursor()
    sql = """
        INSERT INTO Users (author, author_flair_text, author_flair_css_class)
        VALUES (%s, %s, %s)
        ON CONFLICT (author) DO UPDATE
          SET author_flair_text = EXCLUDED.author_flair_text,
              author_flair_css_class = EXCLUDED.author_flair_css_class;
    """
    batch = []
    for doc in mongo_db.users.find({}, no_cursor_timeout=True).batch_size(batch_size):
        batch.append((
            doc["_id"],
            doc.get("author_flair_text"),
            doc.get("author_flair_css_class"),
        ))
        if len(batch) >= batch_size:
            flush_batch(cur, sql, batch)
            pg_conn.commit()
    flush_batch(cur, sql, batch)
    pg_conn.commit()
    cur.close()


def insert_subreddits(mongo_db, pg_conn, batch_size: int):
    print("subreddits collection -> Subreddit table...")
    cur = pg_conn.cursor()
    sql = """
        INSERT INTO Subreddit (subreddit_id, subreddit)
        VALUES (%s, %s)
        ON CONFLICT (subreddit_id) DO UPDATE
          SET subreddit = EXCLUDED.subreddit;
    """
    batch = []
    for doc in mongo_db.subreddits.find({}, no_cursor_timeout=True).batch_size(batch_size):
        batch.append((doc["_id"], doc.get("name")))
        if len(batch) >= batch_size:
            flush_batch(cur, sql, batch)
            pg_conn.commit()
    flush_batch(cur, sql, batch)
    pg_conn.commit()
    cur.close()


def insert_posts_and_postlink(mongo_db, pg_conn, batch_size: int):
    print("posts collection -> Post + Post_Link tables...")
    cur = pg_conn.cursor()

    post_sql = """
        INSERT INTO Post (
            link_id, subreddit_id, author, created_utc,
            archived, gilded, edited
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (link_id) DO UPDATE
          SET subreddit_id = EXCLUDED.subreddit_id,
              author = EXCLUDED.author,
              created_utc = EXCLUDED.created_utc,
              archived = EXCLUDED.archived,
              gilded = EXCLUDED.gilded,
              edited = EXCLUDED.edited;
    """

    sub_sql = """
        INSERT INTO Subreddit (subreddit_id, subreddit)
        VALUES (%s, %s)
        ON CONFLICT (subreddit_id) DO UPDATE
          SET subreddit = EXCLUDED.subreddit;
    """

    link_sql = """
        INSERT INTO Post_Link (link_id, post_id, retrieved_on)
        VALUES (%s, %s, %s)
        ON CONFLICT (link_id) DO UPDATE
          SET retrieved_on = EXCLUDED.retrieved_on;
    """

    sub_batch = []
    post_batch = []
    link_batch = []

    cursor = mongo_db.posts.find({}, no_cursor_timeout=True).batch_size(batch_size)
    for doc in cursor:
        subreddit = doc.get("subreddit") or {}
        subreddit_id = subreddit.get("id")
        subreddit_name = subreddit.get("name")

        if subreddit_id is not None:
            sub_batch.append((subreddit_id, subreddit_name))

        author = doc.get("author")
        if author == "[deleted]":
            author = None

        post_batch.append((
            doc["_id"],
            subreddit_id,
            author,
            doc.get("created_utc"),
            int(bool(doc.get("archived"))),
            doc.get("gilded"),
            int(bool(doc.get("edited"))),
        ))

        retrieved_on = doc.get("retrieved_on")
        if retrieved_on is not None:
            link_batch.append((doc["_id"], doc["_id"], retrieved_on))

        if len(post_batch) >= batch_size:
            # order：first subreddit，then post，at last post_link
            flush_batch(cur, sub_sql, sub_batch)
            flush_batch(cur, post_sql, post_batch)
            flush_batch(cur, link_sql, link_batch)
            pg_conn.commit()

    # flush the rest
    flush_batch(cur, sub_sql, sub_batch)
    flush_batch(cur, post_sql, post_batch)
    flush_batch(cur, link_sql, link_batch)
    pg_conn.commit()
    cur.close()


def insert_comments(mongo_db, pg_conn, batch_size: int):
    print("comments collection -> Comment table...")
    cur = pg_conn.cursor()
    sql = """
        INSERT INTO Comment (
            id, body, author, link_id, parent_id,
            created_utc, retrieved_on,
            score, ups, downs,
            score_hidden, gilded, controversiality, edited
        )
        VALUES (%s, %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
          SET body = EXCLUDED.body,
              author = EXCLUDED.author,
              link_id = EXCLUDED.link_id,
              parent_id = EXCLUDED.parent_id,
              created_utc = EXCLUDED.created_utc,
              retrieved_on = EXCLUDED.retrieved_on,
              score = EXCLUDED.score,
              ups = EXCLUDED.ups,
              downs = EXCLUDED.downs,
              score_hidden = EXCLUDED.score_hidden,
              gilded = EXCLUDED.gilded,
              controversiality = EXCLUDED.controversiality,
              edited = EXCLUDED.edited;
    """
    batch = []
    cursor = mongo_db.comments.find({}, no_cursor_timeout=True).batch_size(batch_size)
    for doc in cursor:
        author = doc.get("author")
        if author == "[deleted]":
            author = None

        batch.append((
            doc["id"],
            doc.get("body"),
            author,
            doc.get("post_id"),
            doc.get("parent_id"),
            doc.get("created_utc"),
            doc.get("retrieved_on"),
            doc.get("score"),
            doc.get("ups"),
            doc.get("downs"),
            int(bool(doc.get("score_hidden"))),
            doc.get("gilded"),
            doc.get("controversiality"),
            int(bool(doc.get("edited"))),
        ))

        if len(batch) >= batch_size:
            flush_batch(cur, sql, batch)
            pg_conn.commit()

    flush_batch(cur, sql, batch)
    pg_conn.commit()
    cur.close()


def insert_moderation(mongo_db, pg_conn, batch_size: int):
    print("moderation collection -> Moderation table...")
    cur = pg_conn.cursor()
    sql = """
        INSERT INTO Moderation (
            target_type, target_id, subreddit_id,
            removal_reason, distinguished, action_timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s);
    """
    batch = []
    cursor = mongo_db.moderation.find({}, no_cursor_timeout=True).batch_size(batch_size)
    for doc in cursor:
        batch.append((
            doc.get("target_type"),
            doc.get("target_id"),
            doc.get("subreddit_id"),
            doc.get("removal_reason"),
            doc.get("distinguished"),
            doc.get("action_timestamp"),
        ))
        if len(batch) >= batch_size:
            flush_batch(cur, sql, batch)
            pg_conn.commit()

    flush_batch(cur, sql, batch)
    pg_conn.commit()
    cur.close()


# ------------- Main -------------

def main():
    args = parse_args()

    pg_conn = psycopg2.connect(args.pg_dsn)
    client = MongoClient(args.mongo_uri)
    db = client[args.mongo_dbname]

    batch_size = args.batch_size
    print(f"Using batch size = {batch_size}")

    insert_users(db, pg_conn, batch_size)
    insert_subreddits(db, pg_conn, batch_size)
    insert_posts_and_postlink(db, pg_conn, batch_size)
    insert_comments(db, pg_conn, batch_size)
    insert_moderation(db, pg_conn, batch_size)

    pg_conn.close()
    client.close()
    print("\nTranslation Mongo -> relational finished.")


if __name__ == "__main__":
    main()
