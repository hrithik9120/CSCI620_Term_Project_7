#!/usr/bin/env python3
"""
MongoDB Loader for Reddit May 2015 Dataset (Document Model, Scalable)

Features
- Uses existing .sqlite if present; else extracts existing zip; else downloads from Kaggle
- Fast extraction via ZipFile.extractall (C-optimized)
- Chunked SQLite reads (no full-table loads)
- Bulk writes to MongoDB for speed
- Hybrid model: posts embed up to --embed-cap comments; all comments also stored in 'comments' collection
- Idempotent (upserts & safe indexes)

Usage:
  python load_to_mongo.py --input ../../data/database.sqlite --mongo_uri "mongodb://localhost:27017/" --dbname reddit_may2015 --reset --chunksize 50000 --embed-cap 200
"""

import argparse
import os
import json
import sqlite3
import zipfile
from collections import defaultdict

import requests
import pandas as pd
from tqdm import tqdm
from pymongo import MongoClient, errors
from pymongo import UpdateOne, ReplaceOne, InsertOne

# -----------------------------
# Download / Extract
# -----------------------------
def find_sqlite(root):
    for dirpath, _, files in os.walk(root):
        for f in files:
            if f.lower().endswith(".sqlite"):
                return os.path.join(dirpath, f)
    return None

def extract_zip_fast(zip_path, output_dir):
    if not zipfile.is_zipfile(zip_path):
        print(" Not a valid zip file. Skipping extraction.")
        return None
    print(f" Extracting zip to {output_dir} ...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(output_dir)
    sqlite_path = find_sqlite(output_dir)
    if not sqlite_path:
        print(" No .sqlite found after extraction.")
        return None
    print(f" Found SQLite file: {sqlite_path}")
    return sqlite_path

def download_kaggle_dataset(output_dir="../../data"):
    """
    Re-use existing files if possible:
      1) If a .sqlite exists anywhere under output_dir, return it.
      2) Else, if reddit_dataset.zip exists, extract and delete it.
      3) Else, download from Kaggle, extract, delete zip, return sqlite.
    """
    os.makedirs(output_dir, exist_ok=True)
    existing_sqlite = find_sqlite(output_dir)
    if existing_sqlite:
        print(f" Using existing SQLite: {existing_sqlite}")
        return existing_sqlite

    zip_path = os.path.join(output_dir, "reddit_dataset.zip")
    if os.path.exists(zip_path):
        print(f" Found existing ZIP at {zip_path}, extracting instead of downloading...")
        sqlite_path = extract_zip_fast(zip_path, output_dir)
        if sqlite_path:
            try:
                os.remove(zip_path)
                print(" Deleted ZIP file after extraction.")
            except OSError:
                pass
        return sqlite_path

    # Download
    dataset_name = "kaggle/reddit-comments-may-2015"
    creds_path = "../../kaggle.json"
    if not os.path.exists(creds_path):
        print(f" Missing kaggle.json at {creds_path}.")
        return None

    print(f"\nDownloading dataset '{dataset_name}' from Kaggle...")
    with open(creds_path, "r") as f:
        creds = json.load(f)
    username, key = creds["username"], creds["key"]

    url = f"https://www.kaggle.com/api/v1/datasets/download/{dataset_name}"
    headers = {"User-Agent": "kaggle/1.5.0 (Python requests)"}
    auth = (username, key)

    # Stream download
    with requests.get(url, stream=True, auth=auth, headers=headers) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0)) or (20 * 1024**3)
        with open(zip_path, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True, desc="Downloading", colour="cyan"
        ) as bar:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))

    print(" Download complete, extracting...")
    sqlite_path = extract_zip_fast(zip_path, output_dir)
    if sqlite_path:
        try:
            os.remove(zip_path)
            print(" Deleted downloaded ZIP file.")
        except OSError:
            pass
    return sqlite_path

# -----------------------------
# SQLite Streaming
# -----------------------------
USED_COLS = [
    "author", "author_flair_text", "author_flair_css_class",
    "subreddit_id", "subreddit",
    "link_id",
    "id", "parent_id", "body",
    "created_utc", "retrieved_on",
    "score", "ups", "downs", "score_hidden", "gilded",
    "distinguished", "edited", "controversiality", "archived",
]

def stream_sqlite(sqlite_path, chunksize):
    conn = sqlite3.connect(sqlite_path)
    cols_csv = ", ".join(USED_COLS)
    query = f"SELECT {cols_csv} FROM May2015"
    try:
        for chunk in pd.read_sql_query(query, conn, chunksize=chunksize):
            yield chunk
    finally:
        conn.close()

# -----------------------------
# Mongo: Indexes & Helpers
# -----------------------------
def ensure_indexes(db):
    # _id unique is implicit for collections with _id; don't pass unique=True for _id
    db.users.create_index("_id")
    db.subreddits.create_index("_id")
    db.posts.create_index("_id")
    db.posts.create_index("subreddit.id")
    db.comments.create_index("id", unique=True)
    db.comments.create_index("post_id")
    db.comments.create_index("author")
    db.moderation.create_index("_id")
    db.moderation.create_index("subreddit_id")

def coerce_bool(x):
    if pd.isna(x):
        return False
    return bool(int(x)) if isinstance(x, (int, float)) else bool(x)

# -----------------------------
# Chunk Loader (Hybrid Model)
# -----------------------------
def load_chunk_to_mongo(df, db, embed_cap=200):
    """
    Hybrid document model:
      - Upsert users and subreddits (dedup per chunk)
      - Upsert posts (no private pymongo fields)
      - Insert comments to 'comments' collection
      - Embed up to `embed_cap` comments per post into posts.comments via $push $each
      - Maintain per-post counters: comment_count, embedded_count
    """
    df = df.replace({pd.NA: None})
    df = df.where(pd.notnull(df), None)

    users_ops = {}
    subs_ops = {}
    posts_new_ops = {}
    comment_inserts = []
    comments_by_post = defaultdict(list)
    moderation_ops = {}

    for row in df.itertuples(index=False):
        author = getattr(row, "author", None)
        if author and author != "[deleted]":
            users_ops[author] = ReplaceOne(
                {"_id": author},
                {
                    "_id": author,
                    "author_flair_text": getattr(row, "author_flair_text", None),
                    "author_flair_css_class": getattr(row, "author_flair_css_class", None),
                },
                upsert=True,
            )

        sub_id = getattr(row, "subreddit_id", None)
        sub_name = getattr(row, "subreddit", None)
        if sub_id:
            subs_ops[sub_id] = ReplaceOne(
                {"_id": sub_id},
                {"_id": sub_id, "name": sub_name},
                upsert=True,
            )

        post_id = getattr(row, "link_id", None)
        if post_id and post_id not in posts_new_ops:
            posts_new_ops[post_id] = ReplaceOne(
                {"_id": post_id},
                {
                    "_id": post_id,
                    "subreddit": {"id": sub_id, "name": sub_name},
                    "author": author,
                    "created_utc": getattr(row, "created_utc", None),
                    "archived": coerce_bool(getattr(row, "archived", 0)),
                    "gilded": getattr(row, "gilded", None),
                    "edited": coerce_bool(getattr(row, "edited", 0)),
                    "retrieved_on": getattr(row, "retrieved_on", None),
                    "comment_count": 0,
                    "embedded_count": 0,
                    "comments": [],
                },
                upsert=True,
            )

        c_id = getattr(row, "id", None)
        if c_id and post_id:
            cdoc = {
                "id": c_id,
                "post_id": post_id,
                "parent_id": getattr(row, "parent_id", None),
                "author": author,
                "body": getattr(row, "body", None),
                "created_utc": getattr(row, "created_utc", None),
                "retrieved_on": getattr(row, "retrieved_on", None),
                "score": getattr(row, "score", None),
                "ups": getattr(row, "ups", None),
                "downs": getattr(row, "downs", None),
                "score_hidden": coerce_bool(getattr(row, "score_hidden", 0)),
                "gilded": getattr(row, "gilded", None),
                "distinguished": getattr(row, "distinguished", None),
                "edited": coerce_bool(getattr(row, "edited", 0)),
                "controversiality": getattr(row, "controversiality", None),
            }
            comment_inserts.append(InsertOne(cdoc))
            comments_by_post[post_id].append({
                "id": c_id,
                "parent_id": cdoc["parent_id"],
                "author": author,
                "body": cdoc["body"],
                "created_utc": cdoc["created_utc"],
                "score": cdoc["score"],
                "ups": cdoc["ups"],
                "downs": cdoc["downs"],
                "score_hidden": cdoc["score_hidden"],
                "gilded": cdoc["gilded"],
                "distinguished": cdoc["distinguished"],
                "edited": cdoc["edited"],
                "controversiality": cdoc["controversiality"],
            })

        if getattr(row, "removal_reason", None) or getattr(row, "distinguished", None):
            mid = f"{c_id}_{sub_id}"
            moderation_ops[mid] = ReplaceOne(
                {"_id": mid},
                {
                    "_id": mid,
                    "target_type": "comment",
                    "target_id": c_id,
                    "subreddit_id": sub_id,
                    "removal_reason": getattr(row, "removal_reason", None),
                    "distinguished": getattr(row, "distinguished", None),
                    "action_timestamp": getattr(row, "retrieved_on", None),
                },
                upsert=True,
            )

    # --- bulk writes
    if users_ops:
        db.users.bulk_write(list(users_ops.values()), ordered=False)
    if subs_ops:
        db.subreddits.bulk_write(list(subs_ops.values()), ordered=False)
    if posts_new_ops:
        # ✅ write the ReplaceOne ops directly (no private fields)
        db.posts.bulk_write(list(posts_new_ops.values()), ordered=False)

    if comment_inserts:
        try:
            db.comments.bulk_write(comment_inserts, ordered=False)
        except errors.BulkWriteError:
            # ignore duplicate comment ids on reruns
            pass

    if comments_by_post:
        embed_ops = []
        counter_ops = []
        for pid, clist in comments_by_post.items():
            to_embed = clist[:embed_cap] if embed_cap > 0 else []
            if to_embed:
                embed_ops.append(
                    UpdateOne({"_id": pid}, {"$push": {"comments": {"$each": to_embed}}})
                )
                counter_ops.append(
                    UpdateOne({"_id": pid}, {"$inc": {"embedded_count": len(to_embed)}})
                )
            counter_ops.append(
                UpdateOne({"_id": pid}, {"$inc": {"comment_count": len(clist)}})
            )
        if embed_ops:
            db.posts.bulk_write(embed_ops, ordered=False)
        if counter_ops:
            db.posts.bulk_write(counter_ops, ordered=False)

    if moderation_ops:
        db.moderation.bulk_write(list(moderation_ops.values()), ordered=False)

# -----------------------------
# CLI / Main
# -----------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Load Reddit May 2015 into MongoDB (hybrid document model)")
    p.add_argument("--input", required=True, help="Path to SQLite file (database.sqlite). If missing, downloader tries ../../data/")
    p.add_argument("--mongo_uri", required=True, help="MongoDB URI (e.g. mongodb://localhost:27017/ )")
    p.add_argument("--dbname", required=True, help="MongoDB database name")
    p.add_argument("--chunksize", type=int, default=50000, help="SQLite chunk size (default: 50000)")
    p.add_argument("--embed-cap", type=int, default=200, help="Max comments to embed per post per chunk (default: 200; 0 disables embedding)")
    p.add_argument("--reset", action="store_true", help="Drop collections before loading")
    return p.parse_args()

def reset_db(db):
    print(" Reset flag detected - dropping collections...")
    for col in ["users", "subreddits", "posts", "comments", "moderation"]:
        if col in db.list_collection_names():
            db[col].drop()
            print(f"   Dropped '{col}'")
    print(" Database cleared.\n")

def main():
    args = parse_args()

    # Resolve SQLite path (reuse if present; else download/extract)
    sqlite_path = args.input
    if not os.path.exists(sqlite_path):
        print(f" SQLite file not found at {sqlite_path}")
        sqlite_path = download_kaggle_dataset("../../data")
        if not sqlite_path:
            print(" Could not obtain dataset. Exiting.")
            return
        print(f" Using SQLite: {sqlite_path}")
    else:
        print(f" Found local SQLite file: {sqlite_path}")

    # Mongo setup
    client = MongoClient(args.mongo_uri)
    db = client[args.dbname]

    if args.reset:
        reset_db(db)
    ensure_indexes(db)

    # Stream + load
    total_rows = 0
    print(f"\nStarting streamed load (chunksize={args.chunksize}, embed_cap={args.embed_cap}) ...")
    for chunk in stream_sqlite(sqlite_path, args.chunksize):
        rows = len(chunk)
        total_rows += rows
        load_chunk_to_mongo(chunk, db, embed_cap=args.embed_cap)
        print(f" ✓ Loaded chunk of {rows:,} rows (total {total_rows:,})")

    print("\n Done! Full dataset streamed to MongoDB.")
    print(" Collections:")
    for name in ["users", "subreddits", "posts", "comments", "moderation"]:
        try:
            print(f"  - {name}: {db[name].estimated_document_count():,} docs")
        except Exception:
            pass

if __name__ == "__main__":
    main()
