**Phase 2 – Query Benchmarking with Indexes**

1. Introduction:
phase2_queries.py benchmarks the execution time of several meaningful SQL queries on the Reddit May 2015 PostgreSQL database before and after creating indexes.
This demonstrates how well-chosen indexes can speed up query execution on our Phase 1 relational model (loaded with 50 million comments).

2. Features
* Connects to an existing PostgreSQL database with the Reddit schema (Post, Comment, Subreddit, etc.).
* Issues a set of 6 interesting queries over posts, authors, comments, and subreddits.
* Measures execution time without indexes and with indexes.
* Automatically:
  * Drops the benchmark indexes to restore a baseline.
  * Runs all queries and records timing.
  * Creates indexes tailored to each query (e.g., on `subreddit_id + created_utc`, `link_id`, `author`, `gilded`/`archived`, etc.).
  * Runs all queries again and records timing.
  * Saves a comparison CSV file: `query_performance_comparison.csv`.

The script is designed to run on top of the Phase 1 database, where the Reddit May 2015 dataset (about 50 million rows) has already been loaded into the normalized relational schema.

3. Command-Line Usage

Run the script from the command line:

```bash
python phase2_queries.py --user your_username --password your_password --dbname your_database
```

Arguments
--host : Database host (default: localhost)
--port : Database port (default: 5432)
--user : Database user (**required**)
--password : Database password (**required**)
--dbname : Database name (**required**)

Since --host and --port use default values, they do not need to be specified on the command line unless you are overriding them.

4. Output: CSV Comparison File
The script produces a CSV file:
* File: query_performance_comparison.csv

Columns:
* Query – Query label (Q1–Q6 with a short description)
* Rows – Number of result rows
* Time_before – Execution time without indexes (seconds)
* Time_after – Execution time with indexes (seconds)
* Speedup – Time_before / Time_after (how many times faster with indexes)

Note: Exact execution times may vary slightly between runs, but the overall pattern (indexed queries running faster than the baseline) remains consistent.