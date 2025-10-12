#!/usr/bin/env python3
"""
Reddit Comments May 2015 Dataset Loader

This program automatically loads the Kaggle dataset "Reddit Comments May 2015" into PostgreSQL.
The program handles all steps including database connection, table creation, and data loading
without manual intervention.

Dataset: https://www.kaggle.com/datasets/kaggle/reddit-comments-may-2015

REQUIREMENTS:
- PostgreSQL server running and accessible
- psycopg2-binary package installed (pip install psycopg2-binary)
- Input file: RC_2015-05.json or RC_2015-05.json.gz

CONFIGURATION:
1. Ensure PostgreSQL is running on your system
2. Create a database for the Reddit data (e.g., 'redditdb')
3. Note your PostgreSQL connection details (host, username, password, database name)

USAGE:
    # Full dataset load
    python load_reddit_may2015.py --input RC_2015-05.json.gz --host localhost --user postgres --password mypass --dbname redditdb
    
    # Test with sample data (first 1000 comments)
    python load_reddit_may2015.py --input RC_2015-05.json.gz --host localhost --user postgres --password mypass --dbname redditdb --sample 1000

AUTOMATIC STEPS:
1. Connects to PostgreSQL database
2. Creates 'comments' table with proper schema if it doesn't exist
3. Streams the JSON file (handles both .json and .json.gz)
4. Extracts required fields from each comment
5. Loads data in batches of 1000 records for optimal performance
6. Provides progress updates every 100,000 records
7. Handles errors gracefully and continues processing
8. Reports final statistics

OUTPUT:
- Progress messages during loading
- Final count of processed, inserted, and error records
- Success confirmation when complete
"""

import argparse
import gzip
import json
import sys

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
    # Load full dataset
    python load_reddit_may2015.py --input RC_2015-05.json.gz --host localhost --user postgres --password mypass --dbname redditdb
    
    # Test with sample data
    python load_reddit_may2015.py --input RC_2015-05.json.gz --host localhost --user postgres --password mypass --dbname redditdb --sample 1000
        """
    )
    
    parser.add_argument('--input', required=True, 
                       help='Path to Reddit comments file (RC_2015-05.json or RC_2015-05.json.gz)')
    parser.add_argument('--host', default='localhost', 
                       help='PostgreSQL server host (default: localhost)')
    parser.add_argument('--user', default='postgres', 
                       help='PostgreSQL username (default: postgres)')
    parser.add_argument('--password', required=True, 
                       help='PostgreSQL password (required)')
    parser.add_argument('--dbname', required=True, 
                       help='PostgreSQL database name (required)')
    parser.add_argument('--sample', type=int, 
                       help='Load only first N comments for testing (optional)')
    
    return parser.parse_args()


def create_database_connection(host, user, password, dbname):
    """
    Create and return a PostgreSQL database connection.
    
    Args:
        host (str): PostgreSQL server host
        user (str): PostgreSQL username
        password (str): PostgreSQL password
        dbname (str): PostgreSQL database name
        
    Returns:
        psycopg2.connection: Database connection object
        
    Exits:
        System exit if connection fails
    """
    try:
        conn = psycopg2.connect(host=host, user=user, password=password, dbname=dbname)
        print(f"Connected to database '{dbname}' on {host}")
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


def open_file_stream(file_path):
    """
    Open file stream for JSON or compressed JSON files.
    Automatically handles both .json and .json.gz file formats.
    
    Args:
        file_path (str): Path to the input file
        
    Returns:
        file object: Open file stream for reading
    """
    if file_path.endswith('.gz'):
        return gzip.open(file_path, 'rt', encoding='utf-8')
    else:
        return open(file_path, 'r', encoding='utf-8')


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
    Load Reddit comments from JSON file into PostgreSQL database.
    This function handles the complete loading process automatically:
    - Streams the file line by line (memory efficient)
    - Parses JSON and extracts required fields
    - Loads data in batches for optimal performance
    - Provides progress updates and error handling
    
    Args:
        conn: PostgreSQL database connection object
        file_path (str): Path to the input JSON file
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
        with open_file_stream(file_path) as file_stream:
            for line_num, line in enumerate(file_stream, 1):
                # Check sample size limit
                if sample_size and total_processed >= sample_size:
                    break
                
                line = line.strip()
                if not line:
                    continue
                
                try:
                    # Parse JSON line
                    comment_data = json.loads(line)
                    
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
                
                except json.JSONDecodeError as e:
                    total_errors += 1
                    print(f"Warning: Malformed JSON at line {line_num}: {e}")
                    continue
                except Exception as e:
                    total_errors += 1
                    print(f"Warning: Unexpected error at line {line_num}: {e}")
                    continue
        
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
    2. Connect to PostgreSQL database
    3. Create/verify comments table
    4. Load data from JSON file
    5. Report final statistics
    """
    args = parse_arguments()
    
    print("=" * 60)
    print("Reddit Comments May 2015 Dataset Loader")
    print("=" * 60)
    
    # Step 1: Connect to database
    print("\n Step 1: Connecting to PostgreSQL database...")
    conn = create_database_connection(args.host, args.user, args.password, args.dbname)
    
    try:
        # Step 2: Create/verify table
        print("\n Step 2: Creating/verifying comments table...")
        create_comments_table(conn)
        
        # Step 3: Load data
        print("\n Step 3: Loading data from file...")
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
