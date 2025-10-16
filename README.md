# Reddit Comments May 2015 Dataset Loader

Automated Python script to load the Kaggle dataset "Reddit Comments May 2015" from SQLite into PostgreSQL. The program handles all setup steps automatically without manual intervention.

**Dataset**: https://www.kaggle.com/datasets/kaggle/reddit-comments-may-2015

## Requirements

- Python 3.6+
- PostgreSQL server running and accessible
- psycopg2-binary
- SQLite database file containing Reddit comments data

## Installation

```bash
pip install psycopg2-binary
```

## Configuration

1. Ensure PostgreSQL server is running on your system
2. Note your PostgreSQL connection details (host, username, password)
3. The program will automatically create the database if it doesn't exist

## Usage

```bash
# Load full dataset from SQLite (with port specified)
python load_reddit_may2015.py --input database.sqlite --host localhost --port 5432 --user postgres --password mypass --dbname redditdb

# Load sample for testing
python load_reddit_may2015.py --input database.sqlite --host localhost --port 5432 --user postgres --password mypass --dbname redditdb --sample 1000

# Using default port (5432)
python load_reddit_may2015.py --input database.sqlite --host localhost --user postgres --password mypass --dbname redditdb
```

## Command Line Options

- `--input`: Path to SQLite database file (required)
- `--host`: PostgreSQL server host (default: localhost)
- `--port`: PostgreSQL server port (default: 5432)
- `--user`: PostgreSQL username (default: postgres)
- `--password`: PostgreSQL password (required)
- `--dbname`: PostgreSQL database name (required)
- `--sample`: Load only first N comments for testing (optional)

## Automatic Steps

The program handles all steps automatically without manual intervention:

1. **Database Creation**: Creates target database if it doesn't exist
2. **Connection**: Connects to PostgreSQL database
3. **Table Setup**: Creates 'comments' table with proper schema
4. **Data Reading**: Reads data from SQLite database
5. **Data Processing**: Extracts and validates required fields
6. **Batch Loading**: Loads data in batches for optimal performance
7. **Progress Reporting**: Provides real-time progress updates
8. **Error Handling**: Gracefully handles errors and continues processing
9. **Statistics**: Reports comprehensive final statistics

## Features

- **Automatic Database Creation**: Creates PostgreSQL database if it doesn't exist
- **SQLite to PostgreSQL Conversion**: Automatically converts SQLite data to PostgreSQL format
- **Smart Table Detection**: Automatically finds the appropriate table in SQLite database
- **Batch Processing**: Inserts data in batches with progress logging every 100,000 rows
- **Port Configuration**: Supports custom PostgreSQL ports
- **Error Handling**: Graceful error handling for data conversion issues
- **Sample Mode**: Test with limited data using --sample parameter
- **Memory Efficient**: Processes data in batches without loading entire dataset into memory
- **Progress Tracking**: Real-time progress updates and final statistics
- **No Manual Intervention**: Complete automation from start to finish
