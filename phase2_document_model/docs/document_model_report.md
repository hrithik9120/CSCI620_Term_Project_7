# Reddit May 2015 – Document-Oriented Model (MongoDB)


# 1. Introduction

This document describes the document-oriented data model we designed for the  
Reddit Comments May 2015 dataset while migrating from the relational  
(PostgreSQL) schema of Phase I to a MongoDB-based document database.

The goals of our design were to:

- Reflect real-world Reddit structure  
- Exploit document-store strengths such as embedding and denormalization  
- Support scalable ingestion of the 50M-row dataset  
- Enable efficient analytical and post-centric queries  
- Eliminate unnecessary relational joins and foreign keys  

We implemented a hybrid model that embeds a limited number of comments inside post documents while also storing all comments in a separate collection for analytical use.

---

# 2. Dataset Columns (Original Kaggle Schema)

The original `May2015` table contains:
- author, author_flair_text, author_flair_css_class,
- subreddit_id, subreddit,
- link_id, id, parent_id, body,
- created_utc, retrieved_on,
- score, ups, downs, score_hidden, gilded,
- distinguished, edited, controversiality, archived

Our MongoDB model preserves these fields and maps them to appropriate document structures described in the next section.

---

# 3. Overview of Our Document Model

We designed five collections:

| Collection     | Purpose |
|----------------|---------|
| `users`        | Stores all unique Reddit authors |
| `subreddits`   | Stores subreddit metadata |
| `posts`        | Stores post-level data and embedded top comments |
| `comments`     | Stores all comments for analytics and full access |
| `moderation`   | Stores moderation-related signals extracted from the dataset |

In contrast to the relational model, which required up to six joins for common queries, the MongoDB model eliminates the need for such joins in most scenarios.

---

# 4. Detailed Document Model

Below is the detailed description of each collection and how dataset fields map into MongoDB documents.

---

## 4.1 `users` Collection

```json
{
  "_id": "someAuthorName",
  "author_flair_text": "AMA verified",
  "author_flair_css_class": "mod"
}
```
### Notes:
- _id is the natural primary key (the author name).
- Documents are deduplicated using replaceOne(..., upsert=True).
- The dataset includes several hundred thousand unique authors.

## 4.2 `subreddits` Collection

```json
{
  "_id": "t5_2qh33",
  "name": "AskReddit"
}
```
### Notes:
- _id equals the subreddit_id from the dataset.
- Post documents reference this structure directly.

## 4.3 `posts` Collection (Hybrid Model Core)
Each document is constructed using the dataset's link_id (post identifier):
```json
{
  "_id": "t3_abc123",
  "subreddit": {
    "id": "t5_2qh33",
    "name": "AskReddit"
  },
  "author": "someAuthor",
  "created_utc": 1430438400,
  "archived": false,
  "gilded": 0,
  "edited": false,
  "retrieved_on": 1430438500,

  "comment_count": 238,
  "embedded_count": 200,

  "comments": [
    {
      "id": "t1_xyz",
      "parent_id": "t3_abc123",
      "author": "anotherUser",
      "body": "This is a comment.",
      "created_utc": 1430438420,
      "score": 12,
      "ups": 12,
      "downs": 0,
      "distinguished": null,
      "edited": false,
      "controversiality": 0
    }
  ]
}
```

### Design Justification:
- Reddit's API structure returns posts with embedded top-level comments; the model mirrors this natively.
- Embedding supports fast read-dominated queries, such as:-
    - Fetching a post with its discussion
    - Computing comment statistics per post
    - Browsing top-level comment threads
    - A full comments collection avoids hitting MongoDB's 16 MB document size limit and enables large-scale comment analytics.
    - The hybrid design balances performance and data completeness.

## 4.4 `comments` Collection (Full Storage for Analytics)

```json
{
    "id": "t1_xyz",
  "post_id": "t3_abc123",
  "parent_id": "t1_def456",
  "author": "user123",
  "body": "This is a full comment.",
  "created_utc": 1430438420,
  "retrieved_on": 1430438500,
  "score": 12,
  "ups": 12,
  "downs": 0,
  "score_hidden": false,
  "gilded": 0,
  "distinguished": null,
  "edited": false,
  "controversiality": 0
}
```

### Design Justification:
- Some posts have thousands of comments, which cannot be safely embedded due to MongoDB's document size limits.
- Analytical queries (e.g., "most controversial authors", "per-subreddit comment volume") run efficiently only on a dedicated comment collection
- Embedding everything would lead to large document rewrites and inefficient update patterns.

## 4.5 `moderation` Collection

```json
{
  "_id": "t1_xyz_t5_2qh33",
  "target_type": "comment",
  "target_id": "t1_xyz",
  "subreddit_id": "t5_2qh33",
  "removal_reason": "spam",
  "distinguished": "moderator",
  "action_timestamp": 1430438500
}
```

### Design Justification:
- Moderation signals appear only on comments in this dataset.
- Synthetic moderation values are isolated cleanly here without polluting the main collections.
- Composite _id ensures deduplication on repeated runs.



# 5. Justification of the Document Model
## 1. Embedding vs. Referencing
- Embedding top comments provides fast access for post-centric reads.
- Storing all comments separately supports global analytics.
- This hybrid approach supports both workloads effectively.

## 2. Eliminating Joins

Relational joins such as:
- post → subreddit
- post → author
- post → comments
- comment → moderation

are replaced with:
- embedded structures
- pre-aligned document fields
- targeted indexes

## 3. High-Performance Ingestion

- Streaming SQLite reads (chunksize=50000)
- Bulk writes to MongoDB using bulk_write
- Idempotent upserts across all collections
- Embedded comment cap to prevent large documents

## 4. Natural Key Usage

- _id fields match real Reddit identifiers (link_id, id, author, subreddit_id).
- Avoids synthetic surrogate keys and simplifies validation.

# 6. Loader Program Summary (load_to_mongo.py)

The loader includes:
- SQLite streaming (no full-table reads)
- Automatic chunked ingestion
- Automatic de-duplication via upserts
- Hybrid embedding (--embed-cap)
- Bulk writes (ReplaceOne, InsertOne, UpdateOne)
- Automatic index creation for common query patterns
- Kaggle auto-download support when SQLite file is missing
- Safe for full 50M-row ingestion
- Usage instructions are provided in the project root README.md.


Usage instructions are provided in the project root README.md.

# 7. Comparison with Relational Model

| Aspect        | Relational (PostgreSQL)     | Document (MongoDB)      |
| ------------- | --------------------------- | ----------------------- |
| Data locality | Distributed across 6 tables | Co-located in documents |
| Read pattern  | Requires joins              | Single document fetches |
| Write pattern | Multi-table inserts         | Direct document writes  |
| Comments      | Fully normalized            | Embedded + referenced   |
| Moderation    | FK constraints              | Isolated collection     |
| Large posts   | Expensive JOINs             | Embedded comment cap    |

## Design Justification:
 - MongoDB allows modeling Reddit data similar to Reddit's real API responses, giving fast access to posts and their comment contexts while still enabling large-scale analytical queries through the separate comments collection.

# 8. Verification Queries

All validation and verification queries are contained in:

```json
document_model/sample_queries.js
```