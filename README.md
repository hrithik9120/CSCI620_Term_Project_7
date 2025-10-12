# Reddit Comments May 2015 Dataset Loader

Simple Python script to load the Kaggle dataset "Reddit Comments May 2015" into PostgreSQL.

**Dataset**: https://www.kaggle.com/datasets/kaggle/reddit-comments-may-2015

## Requirements

- Python 3.6+
- PostgreSQL database
- psycopg2-binary

## Installation

```bash
pip install psycopg2-binary
```

## Usage

```bash
# Load full dataset
python load_reddit_may2015.py --input RC_2015-05.json.gz --host localhost --user postgres --password mypass --dbname redditdb

# Load sample for testing
python load_reddit_may2015.py --input RC_2015-05.json.gz --host localhost --user postgres --password mypass --dbname redditdb --sample 1000
```

## Features

- Streams large files without loading into memory
- Handles both .json and .json.gz files
- Batch inserts with progress logging every 100,000 rows
- Graceful error handling for malformed JSON
- Sample mode for testing
