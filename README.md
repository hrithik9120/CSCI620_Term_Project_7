# Reddit Comments May 2015 Dataset Loader

Simple Python script to load the Kaggle dataset "Reddit Comments May 2015" from SQLite into PostgreSQL.

**Dataset**: https://www.kaggle.com/datasets/kaggle/reddit-comments-may-2015

## Requirements

- Python 3.6+
- PostgreSQL database
- psycopg2-binary
- sqlite3 (built-in with Python)

## Installation

```bash
pip install psycopg2-binary
```

## Usage

```bash
# Load full dataset from SQLite
python load_reddit_may2015.py --input database.sqlite --host localhost --user postgres --password mypass --dbname redditdb

# Load sample for testing
python load_reddit_may2015.py --input database.sqlite --host localhost --user postgres --password mypass --dbname redditdb --sample 1000
```

## Features

- **SQLite to PostgreSQL conversion**: Automatically converts SQLite data to PostgreSQL format
- **Smart table detection**: Automatically finds the appropriate table in SQLite database
- **Batch processing**: Inserts data in batches with progress logging every 100,000 rows
- **Error handling**: Graceful error handling for data conversion issues
- **Sample mode**: Test with limited data using --sample parameter
- **Memory efficient**: Processes data in batches without loading entire dataset into memory
