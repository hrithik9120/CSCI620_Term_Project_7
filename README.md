# Reddit Comments May 2015 Dataset Loader

Automated Python scripts to load the Kaggle dataset "Reddit Comments May 2015" from SQLite into PostgreSQL. The programs handle all setup steps automatically without manual intervention.

**Dataset**: https://www.kaggle.com/datasets/kaggle/reddit-comments-may-2015

## Available Scripts

### 1. `load_reddit_may2015.py` - Single Table Loader
- Loads data into a single `comments` table
- Includes automatic Kaggle dataset download
- Handles JSON/JSON.gz and SQLite input formats
- Automatic database and table creation

### 2. `load_data.py` - Multi-Table Normalized Loader
- Loads data into normalized tables (User, Subreddit, Post, Post_Link, Comment, Moderation)
- Uses pandas for efficient data processing
- Schema-aligned with proper foreign key relationships
- Optimized batch processing

## Requirements

- Python 3.6+
- PostgreSQL server running and accessible
- SQLite database file containing Reddit comments data

## Installation

```bash
pip install -r requirements.txt
```

### Dependencies
- `psycopg2-binary`: PostgreSQL database adapter
- `pandas`: Data manipulation and analysis
- `requests`: HTTP library for Kaggle API
- `tqdm`: Progress bars for downloads

## Configuration

1. Ensure PostgreSQL server is running on your system
2. Note your PostgreSQL connection details (host, username, password)
3. The programs will automatically create the database if it doesn't exist
4. For multi-table loader: Ensure PostgreSQL schema is created (run `create_ddl_queries.sql` first)

## Usage

### Single Table Loader (`load_reddit_may2015.py`)

```bash
# Load full dataset from SQLite (with port specified)
python load_reddit_may2015.py --input database.sqlite --host localhost --port 5432 --user postgres --password mypass --dbname redditdb

# Load sample for testing
python load_reddit_may2015.py --input database.sqlite --host localhost --port 5432 --user postgres --password mypass --dbname redditdb --sample 1000

# Using default port (5432)
python load_reddit_may2015.py --input database.sqlite --host localhost --user postgres --password mypass --dbname redditdb
```

### Multi-Table Loader (`load_data.py`)

```bash
# Load full dataset into normalized tables
python load_data.py --input database.sqlite --host localhost --port 5432 --user postgres --password mypass --dbname redditdb

# Load sample for testing
python load_data.py --input database.sqlite --host localhost --port 5432 --user postgres --password mypass --dbname redditdb --sample 1000
```

## Command Line Options

Both scripts support the same command line options:

- `--input`: Path to SQLite database file (required)
- `--host`: PostgreSQL server host (default: localhost)
- `--port`: PostgreSQL server port (default: 5432)
- `--user`: PostgreSQL username (default: postgres)
- `--password`: PostgreSQL password (required)
- `--dbname`: PostgreSQL database name (required)
- `--sample`: Load only first N rows for testing (optional)

## Automatic Steps

Both programs handle all steps automatically without manual intervention:

### Single Table Loader (`load_reddit_may2015.py`)
1. **Database Creation**: Creates target database if it doesn't exist
2. **Connection**: Connects to PostgreSQL database
3. **Table Setup**: Creates 'comments' table with proper schema
4. **Data Reading**: Reads data from SQLite database
5. **Data Processing**: Extracts and validates required fields
6. **Batch Loading**: Loads data in batches for optimal performance
7. **Progress Reporting**: Provides real-time progress updates
8. **Error Handling**: Gracefully handles errors and continues processing
9. **Statistics**: Reports comprehensive final statistics

### Multi-Table Loader (`load_data.py`)
1. **Connection**: Connects to PostgreSQL database
2. **Data Reading**: Reads data from SQLite using pandas
3. **Table Processing**: Loads data into normalized tables in dependency order
4. **Data Cleaning**: Applies table-specific preprocessing
5. **Batch Loading**: Loads data in batches of 10,000 records
6. **Progress Reporting**: Provides real-time progress updates
7. **Error Handling**: Gracefully handles errors and continues processing
8. **Statistics**: Reports final statistics for each table

## Features

### Common Features
- **Automatic Database Creation**: Creates PostgreSQL database if it doesn't exist
- **SQLite to PostgreSQL Conversion**: Automatically converts SQLite data to PostgreSQL format
- **Port Configuration**: Supports custom PostgreSQL ports
- **Error Handling**: Graceful error handling for data conversion issues
- **Sample Mode**: Test with limited data using --sample parameter
- **Memory Efficient**: Processes data in batches without loading entire dataset into memory
- **Progress Tracking**: Real-time progress updates and final statistics
- **No Manual Intervention**: Complete automation from start to finish

### Single Table Loader Specific
- **Kaggle Download**: Automatic dataset download from Kaggle
- **Multiple Formats**: Handles JSON/JSON.gz and SQLite input formats
- **Smart Table Detection**: Automatically finds the appropriate table in SQLite database

### Multi-Table Loader Specific
- **Normalized Schema**: Loads into 6 normalized tables (User, Subreddit, Post, Post_Link, Comment, Moderation)
- **Pandas Integration**: Uses pandas for efficient data manipulation
- **Foreign Key Management**: Maintains proper relationships between tables
- **Data Cleaning**: Table-specific preprocessing and validation
- **Schema Alignment**: Compatible with PostgreSQL schema requirements
