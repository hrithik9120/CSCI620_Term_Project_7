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
        creds_path = os.path.expanduser("~/.kaggle/kaggle.json")
        if kaggle_json_file:
            os.makedirs(os.path.dirname(creds_path), exist_ok=True)
            os.replace(kaggle_json_file, creds_path)
            os.chmod(creds_path, 0o600)
            print(" Kaggle credentials configured")

        if not os.path.exists(creds_path):
            print("❌ Missing kaggle.json file. Please provide path or place it in ~/.kaggle/")
            return None

        # Load username and key
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


def create_comments_table(conn):
    """
    Create the comments table with the required schema.
    This function automatically creates the table if it doesn't exist.
    
    Args:
        conn: PostgreSQL database connection object
        
    Exits:
        System exit if table creation fails
    """
    sql = """
    CREATE TABLE IF NOT EXISTS comments (
        id TEXT PRIMARY KEY,
        link_id TEXT,
        parent_id TEXT,
        subreddit TEXT,
        subreddit_id TEXT,
        author TEXT,
        body TEXT,
        created_utc INTEGER,
        score INTEGER,
        gilded INTEGER,
        controversiality INTEGER,
        edited BOOLEAN,
        distinguished TEXT
    );
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        print("Comments table created/verified successfully")
    except psycopg2.Error as e:
        print(f"Table creation failed: {e}")
        print("Please check your database permissions and try again.")
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


def extract_comment_fields(comment_data):
    """
    Extract and validate required fields from a Reddit comment JSON object.
    Handles type conversion and missing fields gracefully.
    
    Args:
        comment_data (dict): JSON object containing comment data
        
    Returns:
        tuple: Extracted comment fields in database order, or None if extraction fails
    """
    try:
        # Extract fields with proper type conversion
        comment_id = comment_data.get('id', '')
        link_id = comment_data.get('link_id', '')
        parent_id = comment_data.get('parent_id', '')
        subreddit = comment_data.get('subreddit', '')
        subreddit_id = comment_data.get('subreddit_id', '')
        author = comment_data.get('author', '')
        body = comment_data.get('body', '')
        
        # Convert numeric fields
        created_utc = comment_data.get('created_utc')
        if created_utc is not None:
            created_utc = int(created_utc)
        
        score = comment_data.get('score')
        if score is not None:
            score = int(score)
        
        gilded = comment_data.get('gilded')
        if gilded is not None:
            gilded = int(gilded)
        
        controversiality = comment_data.get('controversiality')
        if controversiality is not None:
            controversiality = int(controversiality)
        
        # Handle edited field (can be boolean or string)
        edited = comment_data.get('edited')
        if isinstance(edited, bool):
            edited = edited
        elif isinstance(edited, str):
            edited = edited.lower() in ('true', '1', 'yes')
        else:
            edited = False
        
        distinguished = comment_data.get('distinguished', '')
        
        return (
            comment_id, link_id, parent_id, subreddit, subreddit_id,
            author, body, created_utc, score, gilded, controversiality,
            edited, distinguished
        )
    except (ValueError, TypeError) as e:
        print(f"Warning: Error extracting fields from comment: {e}")
        return None


def load_comments(conn, file_path, sample_size=None):
    """
    Load Reddit comments from SQLite file into PostgreSQL database.
    This function handles the complete loading process automatically:
    - Reads data from SQLite database
    - Extracts required fields from each comment
    - Loads data in batches for optimal performance
    - Provides progress updates and error handling
    
    Args:
        conn: PostgreSQL database connection object
        file_path (str): Path to the SQLite database file
        sample_size (int, optional): Limit to first N comments for testing
    """
    batch_size = 1000
    batch_data = []
    total_processed = 0
    total_inserted = 0
    total_errors = 0
    
    # Prepare statement for batch inserts
    insert_sql = """
    INSERT INTO comments (
        id, link_id, parent_id, subreddit, subreddit_id, author, body,
        created_utc, score, gilded, controversiality, edited, distinguished
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING
    """
    
    print(f"Loading comments from: {file_path}")
    if sample_size:
        print(f"Sample mode: Loading only {sample_size:,} comments for testing")
    else:
        print("Full dataset mode: Loading all comments")
    
    try:
        cursor = conn.cursor()
        
        print("Reading data from SQLite database")
        # Read all data from SQLite
        comments_data = read_sqlite_data(file_path, sample_size)
        
        for comment_data in comments_data:
            # Extract required fields
            comment_tuple = extract_comment_fields(comment_data)
            if comment_tuple is not None:
                batch_data.append(comment_tuple)
                total_inserted += 1
            else:
                total_errors += 1
            
            total_processed += 1
            
            # Insert batch when it reaches batch_size
            if len(batch_data) >= batch_size:
                cursor.executemany(insert_sql, batch_data)
                conn.commit()
                batch_data = []
                
                # Progress every 100,000 rows
                if total_processed % 100000 == 0:
                    print(f"Progress: {total_processed:,} processed, {total_inserted:,} inserted, {total_errors:,} errors")
        
        # Insert remaining batch
        if batch_data:
            cursor.executemany(insert_sql, batch_data)
            conn.commit()
    
    except FileNotFoundError:
        print(f"Error: Input file not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file: {e}")
        sys.exit(1)
    
    print(f"Loading completed successfully!")
    print(f"Final Statistics:")
    print(f"   • Total processed: {total_processed:,}")
    print(f"   • Successfully inserted: {total_inserted:,}")
    print(f"   • Errors encountered: {total_errors:,}")
    if total_processed > 0:
        success_rate = (total_inserted / total_processed) * 100
        print(f"   • Success rate: {success_rate:.1f}%")


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
        kaggle_json_path = os.path.expanduser("~/.kaggle/kaggle.json")

        dataset_dir = download_kaggle_dataset(dataset_slug, kaggle_json_file=kaggle_json_path, output_dir=".")
        if dataset_dir:
            print(f" Dataset downloaded to: {dataset_dir}")
        else:
            print(" Failed to download dataset. Exiting.")
            sys.exit(1)

        # Try to locate the SQLite file in the extracted folder
        possible_files = [f for f in os.listdir(dataset_dir) if f.endswith(".sqlite")]
        if not possible_files:
            print(" No SQLite file found in downloaded dataset.")
            sys.exit(1)
        args.input = os.path.join(dataset_dir, possible_files[0])
        print(f"Using downloaded SQLite file: {args.input}")

    # Step 1: Connect to database
    print("\n Step 1: Connecting to PostgreSQL database...")
    conn = create_database_connection(args.host, args.user, args.password, args.dbname)
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
        print("\n Step 3: Creating/verifying comments table...")
        create_comments_table(conn)
        
        # Step 4: Load data
        print("\n Step 4: Loading data from file...")
        load_comments(conn, args.input, args.sample)
        
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
