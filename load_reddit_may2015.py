#!/usr/bin/env python3
"""
Reddit Comments May 2015 Dataset Loader

This program automatically loads the Kaggle dataset "Reddit Comments May 2015" from SQLite into PostgreSQL.
The program handles all steps including database connection, table creation, and data loading
without manual intervention.

Dataset: https://www.kaggle.com/datasets/kaggle/reddit-comments-may-2015

REQUIREMENTS:
- PostgreSQL server running and accessible
- psycopg2-binary package installed (pip install psycopg2-binary)
- Input file: database.sqlite

CONFIGURATION:
1. Ensure PostgreSQL is running on your system
2. Note your PostgreSQL connection details (host, username, password)
3. The program will automatically create the database if it doesn't exist

USAGE:
    # Full dataset load from SQLite
    python load_reddit_may2015.py --input database.sqlite --host localhost --port 5432 --user postgres --password mypass --dbname redditdb
    
    # Test with sample data (first 1000 comments)
    python load_reddit_may2015.py --input database.sqlite --host localhost --port 5432 --user postgres --password mypass --dbname redditdb --sample 1000

AUTOMATIC STEPS:
1. Connects to PostgreSQL server (using 'postgres' database)
2. Creates target database if it doesn't exist
3. Connects to the target database
4. Creates 'comments' table with proper schema if it doesn't exist
5. Reads data from SQLite database
6. Extracts required fields from each comment
7. Loads data in batches of 1000 records for optimal performance
8. Provides progress updates every 100,000 records
9. Handles errors gracefully and continues processing
10. Reports final statistics

OUTPUT:
- Progress messages during loading
- Final count of processed, inserted, and error records
- Success confirmation when complete
"""

import argparse
import sqlite3
import sys
import subprocess
import os
import json
import zipfile
import requests
from tqdm import tqdm

try:
    import psycopg2 # type: ignore
except ImportError:
    print("Error: psycopg2 is required. Install with: pip install psycopg2-binary")
    sys.exit(1)


def parse_arguments():
    """
    Parse command line arguments for database connection and file input.
    
    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Load Reddit Comments May 2015 dataset into PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Load full dataset from SQLite
    python load_reddit_may2015.py --input database.sqlite --host localhost --port 5432 --user postgres --password mypass --dbname redditdb
    
    # Test with sample data
    python load_reddit_may2015.py --input database.sqlite --host localhost --port 5432 --user postgres --password mypass --dbname redditdb --sample 1000
        """
    )
    
    parser.add_argument('--input', required=True, 
                       help='Path to SQLite database file (database.sqlite)')
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
                       help='Load only first N comments for testing (optional)')
    
    return parser.parse_args()

def download_kaggle_dataset(dataset_name, kaggle_json_file=None, output_dir="."):
    """
    Direct Kaggle dataset download with progress bar (manual 31.81 GB total).
    Works on Python 3.13 without using kaggle CLI.
    """
    try:
        print(f"\nStep: Setting up Kaggle API for dataset '{dataset_name}'")

        # --- Setup credentials ---
        creds_path = "./kaggle.json"
        if not os.path.exists(creds_path):
            print("❌ Missing kaggle.json in project folder.")
            return None

        with open(creds_path, "r") as f:
            creds = json.load(f)
        username, key = creds["username"], creds["key"]

        # --- Prepare download ---
        dataset_url = f"https://www.kaggle.com/api/v1/datasets/download/{dataset_name}"
        os.makedirs(output_dir, exist_ok=True)
        zip_path = os.path.join(output_dir, "dataset.zip")
        headers = {"User-Agent": "kaggle/1.5.0 (Python requests)"}
        auth = (username, key)

        print(f"Dataset URL: {dataset_url}")
        print("\nStarting download...\n")

        # --- Manual total (in bytes) ---
        known_total = int(20 * 1024**3)

        # --- Stream download with tqdm ---
        block = 1024 * 1024  # 1 MB chunks
        r = requests.get(dataset_url, stream=True, auth=auth, headers=headers)
        r.raise_for_status()

        with open(zip_path, "wb") as f, tqdm(
            total=known_total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc="Downloading (20GB zip)",
            ncols=80,
            ascii=True,
            colour="cyan",
            bar_format="{desc}: {percentage:3.0f}%|{bar:25}| "
                    "{n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
        ) as bar:
            bytes_downloaded = 0
            for chunk in r.iter_content(chunk_size=block):
                if chunk:
                    f.write(chunk)
                    bytes_downloaded += len(chunk)
                    bar.update(len(chunk))

        r.close()
        print(f"\n✅ Download complete: {bytes_downloaded / 1024**3:.2f} GB written")

        # --- Extraction ---
        if zipfile.is_zipfile(zip_path):
            print("Extracting dataset...")
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(output_dir)
            print(f"✅ Dataset extracted to {output_dir}")
        else:
            print("⚠️ File is not a zip, skipping extraction.")

        return output_dir

    except Exception as e:
        print(f"❌ Download failed: {e}")
        return None

def create_database_if_not_exists(host, port, user, password, dbname):
    """
    Create the target database if it doesn't exist.
    This function connects to the 'postgres' database first, then creates the target database.
    
    Args:
        host (str): PostgreSQL server host
        port (str): PostgreSQL server port
        user (str): PostgreSQL username
        password (str): PostgreSQL password
        dbname (str): Target database name to create
        
    Returns:
        bool: True if database exists or was created successfully, False otherwise
    """
    try:
        # First connect to the default 'postgres' database
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname='postgres')
        conn.autocommit = True  # Required for CREATE DATABASE
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        exists = cursor.fetchone()
        
        if not exists:
            # Create the database
            cursor.execute(f'CREATE DATABASE "{dbname}"')
            print(f"✓ Database '{dbname}' created successfully")
        else:
            print(f"✓ Database '{dbname}' already exists")
        
        cursor.close()
        conn.close()
        return True
        
    except psycopg2.Error as e:
        print(f"✗ Error creating database '{dbname}': {e}")
        print("Please check your PostgreSQL server is running and you have CREATE DATABASE permissions.")
        return False


def create_database_connection(host, port, user, password, dbname):
    """
    Create and return a PostgreSQL database connection.
    
    Args:
        host (str): PostgreSQL server host
        port (str): PostgreSQL server port
        user (str): PostgreSQL username
        password (str): PostgreSQL password
        dbname (str): PostgreSQL database name
        
    Returns:
        psycopg2.connection: Database connection object
        
    Exits:
        System exit if connection fails
    """
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
        print(f"Connected to database '{dbname}' on {host}:{port}")
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        print("Please check your PostgreSQL server is running and connection details are correct.")
        sys.exit(1)


def read_sqlite_data(sqlite_path, sample_size=None):
    """
    Read comment data from SQLite database and convert to the same format as JSON.
    This function handles the conversion from SQLite to PostgreSQL format.
    
    Args:
        sqlite_path (str): Path to the SQLite database file
        sample_size (int, optional): Limit to first N comments for testing
        
    Returns:
        list: List of comment dictionaries in the same format as JSON data
    """
    comments = []
    
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(sqlite_path)
        cursor = conn.cursor()
        
        # Get table information to understand the schema
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"Found tables in SQLite database: {[table[0] for table in tables]}")
        
        # Try to find the comments table (common names)
        table_name = None
        for table in tables:
            table_name_candidate = table[0].lower()
            if 'comment' in table_name_candidate or 'reddit' in table_name_candidate:
                table_name = table[0]
                break
        
        if not table_name:
            # If no obvious table name, use the first table
            table_name = tables[0][0]
        
        print(f"Using table: {table_name}")
        
        # Get column information
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        print(f"Available columns: {column_names}")
        
        # Build query with LIMIT if sample_size is specified
        query = f"SELECT * FROM {table_name}"
        if sample_size:
            query += f" LIMIT {sample_size}"
        
        cursor.execute(query)
        
        # Fetch all rows and convert to dictionary format
        rows = cursor.fetchall()
        for row in rows:
            comment_dict = {}
            for i, value in enumerate(row):
                if i < len(column_names):
                    comment_dict[column_names[i]] = value
            comments.append(comment_dict)
        
        conn.close()
        print(f"Successfully read {len(comments)} comments from SQLite database")
        
    except sqlite3.Error as e:
        print(f"Error reading SQLite database: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error reading SQLite database: {e}")
        return []
    
    return comments

def validate_and_convert_record(record):
    """
    Validate and convert data types for a comment record.
    Returns cleaned record or None if validation fails.
    """
    try:
        cleaned_record = {}
        
        # String fields (direct copy)
        for field in ['id', 'link_id', 'parent_id', 'subreddit', 'subreddit_id', 'author', 'body', 'distinguished']:
            cleaned_record[field] = record.get(field, '')
        
        # Numeric fields with conversion
        for field in ['created_utc', 'score', 'gilded', 'controversiality', 'retrieved_on', 'ups', 'downs']:
            value = record.get(field)
            cleaned_record[field] = int(value) if value is not None else None
        
        # Boolean fields
        archived = record.get('archived')
        cleaned_record['archived'] = int(archived) if archived is not None else 0
        
        # Handle edited field (can be boolean or string)
        edited = record.get('edited')
        if isinstance(edited, bool):
            cleaned_record['edited'] = int(edited)
        elif isinstance(edited, str):
            cleaned_record['edited'] = 1 if edited.lower() in ('true', '1', 'yes') else 0
        else:
            cleaned_record['edited'] = 0
        
        # Handle score_hidden (if present)
        cleaned_record['score_hidden'] = record.get('score_hidden', False)
        
        return cleaned_record
        
    except (ValueError, TypeError) as e:
        print(f"Warning: Error validating record: {e}")
        return None

def execute_schema(conn, schema_file="./relational_schema/create_ddl_queries.sql"):
    """
    Load and execute SQL schema file to create all tables.
    """
    print(f"\nStep: Executing schema file '{schema_file}'...")
    try:
        with open(schema_file, "r") as f:
            sql_script = f.read()
        cursor = conn.cursor()
        cursor.execute(sql_script)
        conn.commit()
        print("✓ Schema executed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"✗ Schema execution failed: {e}")
        sys.exit(1)

SCHEMA_COLUMNS = {
    'user': ['author', 'author_flair_text', 'author_flair_css_class'],
    'subreddit': ['subreddit_id', 'subreddit'],
    'post': ['post_id', 'subreddit_id', 'author', 'created_utc', 'archived', 'gilded', 'edited'],
    'post_link': ['link_id', 'post_id', 'retrieved_on'],
    'comment': ['id', 'body', 'author', 'link_id', 'parent_id', 'created_utc', 'retrieved_on',
                'score', 'ups', 'downs', 'score_hidden', 'gilded', 'controversiality', 'edited'],
    'moderation': ['mod_action_id', 'target_type', 'target_id', 'subreddit_id',
                   'removal_reason', 'distinguished', 'action_timestamp']
}


def separate_and_load_data(conn, comments_data):
    """
    Separate Reddit data into normalized tables (User, Subreddit, Post, Post_Link, Comment, Moderation)
    and load each table into PostgreSQL.
    Tracks progress, skipped rows, and FK violations.
    
    Args:
        conn: psycopg2 database connection
        comments_data (list[dict]): Extracted data from SQLite (raw)
    """
    cursor = conn.cursor()
    total_stats = {}

    # Helper to track and print progress
    def print_progress(table, processed, inserted, skipped):
        print(f"  [{table}] Processed: {processed:,} | Inserted: {inserted:,} | Skipped (FK): {skipped:,}")

    # =========================
    # 1️⃣ LOAD USERS
    # =========================
    print("\nStep 1: Loading USERS...")
    user_rows, processed, inserted, skipped = set(), 0, 0, 0
    for record in comments_data:
        processed += 1
        author = record.get('author')
        if not author or author == '[deleted]':
            skipped += 1
            continue
        key = (author, record.get('author_flair_text'), record.get('author_flair_css_class'))
        if key not in user_rows:
            user_rows.add(key)
    try:
        cursor.executemany("""
            INSERT INTO User (author, author_flair_text, author_flair_css_class)
            VALUES (%s, %s, %s)
            ON CONFLICT (author) DO NOTHING
        """, list(user_rows))
        inserted = cursor.rowcount
        conn.commit()
    except psycopg2.Error:
        conn.rollback()
    print_progress("User", processed, inserted, skipped)
    total_stats["User"] = (processed, inserted, skipped)

    # =========================
    # 2️⃣ LOAD SUBREDDITS
    # =========================
    print("\nStep 2: Loading SUBREDDITS...")
    sub_rows, processed, inserted, skipped = set(), 0, 0, 0
    for record in comments_data:
        processed += 1
        sub_id, sub_name = record.get('subreddit_id'), record.get('subreddit')
        if not sub_id or not sub_name:
            skipped += 1
            continue
        key = (sub_id, sub_name)
        sub_rows.add(key)
    try:
        cursor.executemany("""
            INSERT INTO Subreddit (subreddit_id, subreddit)
            VALUES (%s, %s)
            ON CONFLICT (subreddit_id) DO NOTHING
        """, list(sub_rows))
        inserted = cursor.rowcount
        conn.commit()
    except psycopg2.Error:
        conn.rollback()
    print_progress("Subreddit", processed, inserted, skipped)
    total_stats["Subreddit"] = (processed, inserted, skipped)

    # =========================
    # 3️⃣ LOAD POSTS
    # =========================
    print("\nStep 3: Loading POSTS...")
    post_rows, processed, inserted, skipped = set(), 0, 0, 0
    for record in comments_data:
        processed += 1
        post_id = record.get('link_id')
        if not post_id:
            skipped += 1
            continue
        subreddit_id = record.get('subreddit_id')
        author = record.get('author')
        created_utc = record.get('created_utc')
        archived = record.get('archived', 0)
        gilded = record.get('gilded', 0)
        edited = int(bool(record.get('edited')))
        key = (post_id, subreddit_id, author, created_utc, archived, gilded, edited)
        post_rows.add(key)
    try:
        cursor.executemany("""
            INSERT INTO Post (post_id, subreddit_id, author, created_utc, archived, gilded, edited)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (post_id) DO NOTHING
        """, list(post_rows))
        inserted = cursor.rowcount
        conn.commit()
    except psycopg2.Error as e:
        print(f"Post load error: {e}")
        conn.rollback()
    print_progress("Post", processed, inserted, skipped)
    total_stats["Post"] = (processed, inserted, skipped)

    # =========================
    # 4️⃣ LOAD POST_LINK
    # =========================
    print("\nStep 4: Loading POST_LINK...")
    pl_rows, processed, inserted, skipped = set(), 0, 0, 0
    for record in comments_data:
        processed += 1
        link_id, post_id, retrieved_on = record.get('link_id'), record.get('link_id'), record.get('retrieved_on')
        if not link_id or not post_id:
            skipped += 1
            continue
        pl_rows.add((link_id, post_id, retrieved_on))
    try:
        cursor.executemany("""
            INSERT INTO Post_Link (link_id, post_id, retrieved_on)
            VALUES (%s, %s, %s)
            ON CONFLICT (link_id) DO NOTHING
        """, list(pl_rows))
        inserted = cursor.rowcount
        conn.commit()
    except psycopg2.Error:
        conn.rollback()
    print_progress("Post_Link", processed, inserted, skipped)
    total_stats["Post_Link"] = (processed, inserted, skipped)

    # =========================
    # 5️⃣ LOAD COMMENTS
    # =========================
    print("\nStep 5: Loading COMMENTS...")
    processed, inserted, skipped = 0, 0, 0
    for record in comments_data:
        processed += 1
        
        # Validate and convert record data types
        validated_record = validate_and_convert_record(record)
        if validated_record is None:
            skipped += 1
            continue
        
        try:
            cursor.execute("""
                INSERT INTO Comment (
                    id, body, author, link_id, parent_id,
                    created_utc, retrieved_on, score, ups, downs,
                    score_hidden, gilded, controversiality, edited
                )
                VALUES (%(id)s, %(body)s, %(author)s, %(link_id)s, %(parent_id)s,
                        %(created_utc)s, %(retrieved_on)s, %(score)s, %(ups)s, %(downs)s,
                        %(score_hidden)s, %(gilded)s, %(controversiality)s, %(edited)s)
                ON CONFLICT (id) DO NOTHING
            """, validated_record)
            inserted += 1
        except psycopg2.Error as e:
            skipped += 1
            conn.rollback()
            if "foreign key" in str(e):
                continue
    conn.commit()
    print_progress("Comment", processed, inserted, skipped)
    total_stats["Comment"] = (processed, inserted, skipped)

    # =========================
    # 6️⃣ LOAD MODERATION (derived)
    # =========================
    print("\nStep 6: Loading MODERATION...")
    mod_rows, processed, inserted = set(), 0, 0
    for record in comments_data:
        processed += 1
        # Only include if removal_reason or distinguished present
        if record.get('distinguished') or record.get('removal_reason'):
            key = (
                'comment',  # since data is from comments table
                record.get('id'),
                record.get('subreddit_id'),
                record.get('removal_reason'),
                record.get('distinguished'),
                record.get('created_utc'),
            )
            mod_rows.add(key)
    try:
        cursor.executemany("""
            INSERT INTO Moderation (target_type, target_id, subreddit_id, removal_reason, distinguished, action_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, list(mod_rows))
        inserted = cursor.rowcount
        conn.commit()
    except psycopg2.Error:
        conn.rollback()
    print_progress("Moderation", processed, inserted, processed - inserted)
    total_stats["Moderation"] = (processed, inserted, processed - inserted)

    # =========================
    # FINAL SUMMARY
    # =========================
    print("\n===========================")
    print(" LOAD SUMMARY")
    print("===========================")
    for table, (proc, ins, skip) in total_stats.items():
        print(f"  {table:<12} | Processed: {proc:,} | Inserted: {ins:,} | Skipped: {skip:,}")
    print("===========================")


def main():
    """
    Main function that orchestrates the complete data loading process.
    This function handles all steps automatically without manual intervention:
    1. Parse command line arguments
    2. Create target database if it doesn't exist
    3. Connect to PostgreSQL database
    4. Create/verify comments table
    5. Load data from SQLite file
    6. Report final statistics
    """
    args = parse_arguments()
    
    print("=" * 60)
    print("Reddit Comments May 2015 Dataset Loader")
    print("=" * 60)
    
    if not os.path.exists(args.input):
        print(f"\n Step 0: '{args.input}' not found locally.")
        print("Attempting to download from Kaggle...")

        # Example: Kaggle dataset name for Reddit May 2015
        dataset_slug = "kaggle/reddit-comments-may-2015"

        # Optional: replace with your actual kaggle.json path if needed
        kaggle_json = os.path.expanduser("./kaggle.json")

        dataset_dir = download_kaggle_dataset(dataset_slug, kaggle_json_file=kaggle_json, output_dir=".")
        if dataset_dir:
            print(f" Dataset downloaded successfully")
        else:
            print(" Failed to download dataset. Exiting.")
            sys.exit(1)

        # locate the SQLite file in the extracted folder
        possible_files = [f for f in os.listdir(dataset_dir) if f.endswith(".sqlite")]
        if not possible_files:
            sys.exit(1)
        args.input = os.path.join(dataset_dir, possible_files[0])

    # Step 1: Create database if it doesn't exist
    print("\n Step 1: Creating database if it doesn't exist...")
    if not create_database_if_not_exists(args.host, args.port, args.user, args.password, args.dbname):
        print("Failed to create or verify database. Exiting.")
        sys.exit(1)
    
    # Step 2: Connect to database
    print("\n Step 2: Connecting to PostgreSQL database...")
    conn = create_database_connection(args.host, args.port, args.user, args.password, args.dbname)
    
    try:
        # Step 3: Create/verify table
        print("\n Step 3: Creating/Verifying all tables...")
        execute_schema(conn)
        
        # Step 4: Load data from SQLite
        print("\n Step 4: Loading data from file...")
        comments_data = read_sqlite_data(args.input, args.sample)
        if not comments_data:
            print("No data loaded from SQLite file. Exiting.")
            sys.exit(1)
        
        # Step 5: Separate and load data into normalized tables
        print("\n Step 5: Separating and loading data into normalized tables...")
        separate_and_load_data(conn, comments_data)
        
        print("\n All steps completed successfully!")
        
    except KeyboardInterrupt:
        print("\n Loading interrupted by user")
        print("Partial data may have been loaded.")
    except Exception as e:
        print(f"\n Unexpected error: {e}")
        print("Please check your input file and database connection.")
        sys.exit(1)
    finally:
        conn.close()
        print("\n Database connection closed")
        print("=" * 60)


if __name__ == "__main__":
    main()
