#!/usr/bin/env python3
"""
Reddit May 2015 Dataset Loader (Multi-table Version, Schema-aligned)

This program automatically loads the Kaggle dataset "Reddit Comments May 2015" from SQLite into PostgreSQL
using a normalized multi-table approach. The program handles all steps including database connection,
table creation, and data loading without manual intervention.

Dataset: https://www.kaggle.com/datasets/kaggle/reddit-comments-may-2015

REQUIREMENTS:
- Python 3.6 or higher
- PostgreSQL server running and accessible
- psycopg2-binary package installed (pip install psycopg2-binary)
- pandas package installed (pip install pandas)
- SQLite database file containing Reddit comments data

CONFIGURATION:
1. Ensure PostgreSQL is running on your system
2. Note your PostgreSQL connection details (host, username, password)
3. The program will automatically create the database if it doesn't exist
4. Ensure the PostgreSQL schema is already created (run create_ddl_queries.sql first)

USAGE:
    # Full dataset load from SQLite
    python load_data.py --input database.sqlite --host localhost --port 5432 --user postgres --password mypass --dbname redditdb
    
    # Test with sample data (first 1000 rows)
    python load_data.py --input database.sqlite --host localhost --port 5432 --user postgres --password mypass --dbname redditdb --sample 1000

AUTOMATIC STEPS:
1. Connects to PostgreSQL database using provided credentials
2. Reads data from SQLite database using pandas for efficient processing
3. Separates data into normalized tables (User, Subreddit, Post, Post_Link, Comment, Moderation)
4. Applies table-specific preprocessing and data cleaning
5. Loads data in batches of 10,000 records for optimal performance
6. Provides progress updates every 100,000 records
7. Handles errors gracefully and continues processing
8. Reports final statistics for each table

OUTPUT:
- Progress messages during loading for each table
- Final count of processed and inserted records per table
- Success confirmation when complete

SCHEMA COMPATIBILITY:
- Post.link_id is the primary key
- edited fields are BIGINT (timestamps)
- Moderation includes distinguished and removal_reason
- Proper foreign key relationships maintained
"""

import argparse
import pandas as pd
import sqlite3
import psycopg2
import sys

# Check for required dependencies
try:
    import pandas as pd
except ImportError:
    print("Error: pandas is required. Install with: pip install pandas")
    sys.exit(1)

try:
    import psycopg2
except ImportError:
    print("Error: psycopg2 is required. Install with: pip install psycopg2-binary")
    sys.exit(1)


def parse_arguments():
    """
    Parse command line arguments for database connection and file input.
    
    Returns:
        argparse.Namespace: Parsed command line arguments with database connection details
    """
    parser = argparse.ArgumentParser(
        description="Load Reddit dataset from SQLite into PostgreSQL (schema-aligned)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Load full dataset from SQLite
    python load_data.py --input database.sqlite --host localhost --port 5432 --user postgres --password mypass --dbname redditdb
    
    # Test with sample data
    python load_data.py --input database.sqlite --host localhost --port 5432 --user postgres --password mypass --dbname redditdb --sample 1000
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
                       help='Load only first N rows for testing (optional)')

    return parser.parse_args()


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
        print(f"✓ Connected to PostgreSQL database '{dbname}' on {host}:{port}")
        return conn
    except psycopg2.Error as e:
        print(f"✗ Database connection failed: {e}")
        print("Please check your PostgreSQL server is running and connection details are correct.")
        sys.exit(1)


def load_data(conn, sqlite_path, sqlite_table, pg_table, select_cols, insert_cols, sample_size=None):
    """
    Load data from SQLite to PostgreSQL with table-specific preprocessing.
    
    This function handles the complete data loading process for a single table:
    1. Reads data from SQLite using pandas for efficient processing
    2. Applies table-specific preprocessing and data cleaning
    3. Loads data in batches for optimal performance
    4. Provides progress updates and error handling
    
    Args:
        conn: PostgreSQL database connection object
        sqlite_path (str): Path to the SQLite database file
        sqlite_table (str): Name of the source table in SQLite
        pg_table (str): Name of the target table in PostgreSQL
        select_cols (list): Columns to select from SQLite
        insert_cols (list): Columns to insert into PostgreSQL
        sample_size (int, optional): Limit to first N rows for testing
    """
    print(f"📊 Loading data for table: {pg_table}")
    try:
        # Connect to SQLite and read data using pandas
        sqlite_conn = sqlite3.connect(sqlite_path)
        query = f"SELECT {', '.join(select_cols)} FROM {sqlite_table}"
        if sample_size:
            query += f" LIMIT {sample_size}"
        df = pd.read_sql_query(query, sqlite_conn)
        sqlite_conn.close()

        print(f"✓ Read {len(df):,} rows from SQLite table '{sqlite_table}'")

        # -----------------------------
        # Table-specific preprocessing and data cleaning
        # -----------------------------
        if pg_table == "post_link":
            # Filter for posts only (parent_id starting with 't3_')
            df = df[df["parent_id"].str.startswith("t3_", na=False)]
            df.rename(columns={"parent_id": "post_id"}, inplace=True)
            print(f"✓ Filtered {len(df):,} rows where parent_id starts with 't3_'")

        elif pg_table == "comment":
            # Clean parent_id: replace post references (t3_*) with NULLs
            df.loc[df["parent_id"].str.startswith("t3_", na=False), "parent_id"] = None
            print(f"✓ Cleaned parent_id: replaced post references (t3_*) with NULLs")

            # Filter out comments with link_id not in Post_Link table (foreign key constraint)
            cursor = conn.cursor()
            cursor.execute("SELECT link_id FROM Post_Link")
            valid_links = set(r[0] for r in cursor.fetchall())
            before_count = len(df)
            df = df[df["link_id"].isin(valid_links)]
            print(f"✓ Filtered out {before_count - len(df):,} invalid comments (link_id not in Post_Link)")

        elif pg_table == "moderation":
            # Identify post/comment targets based on target_id prefix
            df.loc[df["target_id"].str.startswith("t1_", na=False), "target_type"] = "comment"
            df.loc[df["target_id"].str.startswith("t3_", na=False), "target_type"] = "post"

            # Replace NaN with None for SQL compatibility
            df = df.where(pd.notnull(df), None)

            # Fill missing text fields with None
            df["removal_reason"] = df.get("removal_reason", None)
            df["distinguished"] = df.get("distinguished", None)

            # Log moderation type distribution
            post_count = (df["target_type"] == "post").sum()
            comment_count = (df["target_type"] == "comment").sum()
            null_type = df["target_type"].isnull().sum()
            print(f"✓ Moderation type stats → Post: {post_count:,}, Comment: {comment_count:,}, Null: {null_type:,}")

        # -----------------------------
        # Skip empty dataframes
        # -----------------------------
        if df.empty:
            print(f"⚠️ No data found for {pg_table}, skipping...")
            return

        # Align column order to match PostgreSQL table schema
        df = df[insert_cols]

        # -----------------------------
        # Batch insert into PostgreSQL
        # -----------------------------
        insert_sql = f"""
        INSERT INTO {pg_table} ({', '.join(insert_cols)})
        VALUES ({', '.join(['%s'] * len(insert_cols))})
        ON CONFLICT DO NOTHING;
        """

        cursor = conn.cursor()
        batch_size = 10000  # Optimized batch size for performance
        total_inserted = 0

        # Process data in batches for optimal performance
        for i in range(0, len(df), batch_size):
            batch = [tuple(x) for x in df.iloc[i:i + batch_size].to_numpy()]
            try:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                total_inserted += len(batch)

                # Progress reporting every 100,000 rows
                if total_inserted % 100000 == 0 or total_inserted == len(df):
                    print(f"   📈 Progress: {total_inserted:,}/{len(df):,} rows inserted into {pg_table}")
            except Exception as e:
                conn.rollback()
                print(f"⚠️ Batch rollback due to error: {e}")

        print(f"✅ Finished loading '{pg_table}' ({total_inserted:,} rows).")

    except Exception as e:
        print(f"❌ Error loading table '{pg_table}': {e}")


def main():
    """
    Main function that orchestrates the complete multi-table data loading process.
    This function handles all steps automatically without manual intervention:
    1. Parse command line arguments
    2. Connect to PostgreSQL database
    3. Load data into normalized tables in proper order
    4. Report final statistics
    """
    args = parse_arguments()
    
    print("=" * 65)
    print("🔴 Reddit May 2015 Multi-Table Data Loader (Schema-aligned)")
    print("=" * 65)
    
    # Connect to PostgreSQL database
    print("\n🔌 Connecting to PostgreSQL database...")
    conn = create_database_connection(args.host, args.port, args.user, args.password, args.dbname)

    # Table configuration for normalized schema
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
            # Using link_id as primary key for posts
            "select": ["link_id", "subreddit_id", "author", "created_utc", "archived", "gilded", "edited"],
            "insert": ["link_id", "subreddit_id", "author", "created_utc", "archived", "gilded", "edited"]
        },
        "post_link": {
            "sqlite_table": "May2015",
            # Maps parent_id to post_id for relational integrity
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
            # Includes removal_reason and distinguished fields
            "select": ["id AS target_id", "subreddit_id", "'comment' AS target_type", "removal_reason",
                       "distinguished"],
            "insert": ["target_id", "subreddit_id", "target_type", "removal_reason", "distinguished"]
        }
    }

    # Load order respects foreign key dependencies
    load_order = ["users", "subreddit", "post", "post_link", "comment", "moderation"]

    try:
        print(f"\n📥 Loading data from: {args.input}")
        if args.sample:
            print(f"🧪 Sample mode: Loading only {args.sample:,} rows per table")
        
        # Load each table in dependency order
        for i, pg_table in enumerate(load_order, 1):
            print(f"\n📋 Step {i}: Loading {pg_table.upper()} table...")
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

        print("\n🎉 All tables loaded successfully!")

    except KeyboardInterrupt:
        print("\n⚠️ Loading interrupted by user.")
        print("Partial data may have been loaded.")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print("Please check your input file and database connection.")
    finally:
        conn.close()
        print("\n🔌 Database connection closed.")
        print("=" * 65)


if __name__ == "__main__":
    main()
