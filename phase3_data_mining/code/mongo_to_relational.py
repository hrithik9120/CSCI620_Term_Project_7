#!/usr/bin/env python3
"""
mongo_to_relational.py

Translate data from MongoDB document model (users, subreddits, posts,
comments, moderation) back into PostgreSQL relational schema.

Assumes the relational tables (Users, Subreddit, Post, Post_Link,
Comment, Moderation) already exist.



How to run (command-line arguments)

Required arguments:
    --pg_dsn        PostgreSQL connection string (DSN)
    --mongo_uri     MongoDB connection URI
    --mongo_dbname  MongoDB database name

Example usage (based on my local setup):

    python3 mongo_to_relational.py \
        --pg_dsn "dbname=test_db user=postgresql password=YOUR_PASSWORD host=localhost port=5432" \
        --mongo_uri "mongodb://localhost:27017/" \
        --mongo_dbname redditcomments

Notes:
- test_db        : PostgreSQL database that stores the relational tables
- redditcomments : MongoDB database that stores the (cleaned) Reddit data
- user / password / host / port should be replaced with your actual configuration

"""


import argparse
import psycopg2
from pymongo import MongoClient


def parse_args():
    p = argparse.ArgumentParser(description="Translate MongoDB -> relational DB")
    p.add_argument("--pg_dsn", required=True,
                   help="PostgreSQL DSN, e.g. 'dbname=reddit user=... password=... host=localhost'")
    p.add_argument("--mongo_uri", required=True,
                   help="MongoDB URI, e.g. 'mongodb://localhost:27017/'")
    p.add_argument("--mongo_dbname", required=True,
                   help="MongoDB database name, e.g. 'reddit_may2015'")
    return p.parse_args()


def insert_users(mongo_db, pg_conn):
    print("Users collection -> Users table...")
    cur = pg_conn.cursor()
    for doc in mongo_db.users.find({}):
        cur.execute("""
            INSERT INTO Users (author, author_flair_text, author_flair_css_class)
            VALUES (%s, %s, %s)
            ON CONFLICT (author) DO UPDATE
              SET author_flair_text = EXCLUDED.author_flair_text,
                  author_flair_css_class = EXCLUDED.author_flair_css_class;
        """, (doc["_id"], doc.get("author_flair_text"), doc.get("author_flair_css_class")))
    pg_conn.commit()
    cur.close()


def insert_subreddits(mongo_db, pg_conn):
    print("subreddits collection -> Subreddit table...")
    cur = pg_conn.cursor()
    for doc in mongo_db.subreddits.find({}):
        cur.execute("""
            INSERT INTO Subreddit (subreddit_id, subreddit)
            VALUES (%s, %s)
            ON CONFLICT (subreddit_id) DO UPDATE
              SET subreddit = EXCLUDED.subreddit;
        """, (doc["_id"], doc.get("name")))
    pg_conn.commit()
    cur.close()

def insert_posts_and_postlink(mongo_db, pg_conn):
    print("posts collection -> Post + Post_Link tables...")
    cur = pg_conn.cursor()

    for doc in mongo_db.posts.find({}):


        subreddit = doc.get("subreddit") or {}
        subreddit_id = subreddit.get("id")
        subreddit_name = subreddit.get("name")

        if subreddit_id is not None:
            cur.execute(
                """
                INSERT INTO Subreddit (subreddit_id, subreddit)
                VALUES (%s, %s)
                ON CONFLICT (subreddit_id) DO UPDATE
                  SET subreddit = EXCLUDED.subreddit;
                """,
                (subreddit_id, subreddit_name),
            )


        author = doc.get("author")
        if author == "[deleted]":
            author = None

        # ---- 3) Insert into Post ----
        cur.execute(
            """
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
            """,
            (
                doc["_id"],
                subreddit_id,
                author,
                doc.get("created_utc"),
                int(bool(doc.get("archived"))),
                doc.get("gilded"),
                int(bool(doc.get("edited"))),
            ),
        )

        # ---- 4) insert to Post_link (if there is retrieved_on)------
        retrieved_on = doc.get("retrieved_on")
        if retrieved_on is not None:
            cur.execute(
                """
                INSERT INTO Post_Link (link_id, post_id, retrieved_on)
                VALUES (%s, %s, %s)
                ON CONFLICT (link_id) DO UPDATE
                  SET retrieved_on = EXCLUDED.retrieved_on;
                """,
                (doc["_id"], doc["_id"], retrieved_on),
            )

    pg_conn.commit()
    cur.close()


def insert_comments(mongo_db, pg_conn):
    print("comments collection -> Comment table...")
    cur = pg_conn.cursor()

    for doc in mongo_db.comments.find({}):

        author = doc.get("author")
        if author == "[deleted]":
            author = None

        cur.execute("""
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
        """, (
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

    pg_conn.commit()
    cur.close()


def insert_moderation(mongo_db, pg_conn):
    print("moderation collection -> Moderation table...")
    cur = pg_conn.cursor()

    for doc in mongo_db.moderation.find({}):
        cur.execute("""
            INSERT INTO Moderation (
                target_type, target_id, subreddit_id,
                removal_reason, distinguished, action_timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            doc.get("target_type"),
            doc.get("target_id"),
            doc.get("subreddit_id"),
            doc.get("removal_reason"),
            doc.get("distinguished"),
            doc.get("action_timestamp"),
        ))

    pg_conn.commit()
    cur.close()


def main():
    args = parse_args()

    pg_conn = psycopg2.connect(args.pg_dsn)
    client = MongoClient(args.mongo_uri)
    db = client[args.mongo_dbname]

    # make sure create relational tables according to schema
    insert_users(db, pg_conn)
    insert_subreddits(db, pg_conn)
    insert_posts_and_postlink(db, pg_conn)
    insert_comments(db, pg_conn)
    insert_moderation(db, pg_conn)

    pg_conn.close()
    client.close()
    print("\nTranslation Mongo -> relational finished.")


if __name__ == "__main__":
    main()
