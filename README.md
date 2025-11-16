# CSCI-620 – Phase 2: Document Model (MongoDB)
## Reddit Comments May 2015 – Document-Oriented Data Model + Loader + Queries

This repository contains the complete implementation for Phase 2 of the course project:
a MongoDB document-oriented model, data loader, functional dependency analysis, and verification queries for the Reddit May 2015 dataset.

The work builds on Phase 1 (relational schema) and re-models the dataset into a hybrid, high-performance MongoDB architecture.

This repository contains the full implementation for **Phase 2**, including:
- A complete **MongoDB document model**
- A scalable **SQLite → MongoDB loader**
- **Functional dependency discovery**
- **Phase 2 validation + analytical queries**
- All reports, diagrams, and final submission package

This phase extends the relational design from **Phase 1** into a high-performance **hybrid document model** built for MongoDB.

```
data/
├── reddit-comments-may-2015/
|   └── database.sqlite      
|       
phase1_relational/
├── code/
├── ddl/
├── diagrams/
├── docs/
└── README_phase1.md

phase2_document_model/
├── code/
│   ├── discover_functional_dependencies.py
│   ├── load_to_mongo.py
│   ├── phase2_queries.py
│   └── sample_queries.js
│
├── diagrams/
│   └── doc_model_visual.png
│
├── docs/
│   ├── document_model_report.md
│   ├── functional_dependencies_report.md
│   └── README_Queries.md
|
|submission/

.gitignore
kaggle.json
README.md
requirements.txt
```

## MongoDB Document Model

**Files:**
- `phase2_document_model/docs/document_model_report.md`  
- `phase2_document_model/diagrams/doc_model_visual.png`

**Collections implemented:**
- `users`
- `subreddits`
- `posts` *(hybrid: partial embedding + referencing)*
- `comments`
- `moderation`

The model balances **post-centric reads**, **analytics**, and **MongoDB document size limits**.

---

## MongoDB Loader 

**File:**
- `phase2_document_model/code/load_to_mongo.py`

**Features:**
- Streaming SQLite ingestion (`chunksize=50000`)
- Idempotent upserts for deduplication
- Embeds top-N comments directly inside posts
- Bulk writes for maximum throughput
- Automatic index creation
- Reuses existing `.sqlite` or pulls dataset from Kaggle

**Run example:**
```bash
python load_to_mongo.py \
    --input ../data/database.sqlite \
    --mongo_uri "mongodb://localhost:27017/" \
    --dbname reddit_may2015 \
    --chunksize 50000 \
    --embed-cap 200 \
    --reset
```

## Setup
### 1. Install Dependencies
```
pip install -r requirements.txt
```
### 2. Ensure MongoDB is running
Default expected:
```
mongodb://localhost:27017/
```
### 3. Confirm dataset location
Default expected:
```
data/reddit-comments-may-2015/database.sqlite
```

## Running the MongoDB Loader
```
python phase2_document_model/code/load_to_mongo.py \
    --input data/reddit-comments-may-2015/database.sqlite \
    --mongo_uri "mongodb://localhost:27017/" \
    --dbname reddit_may2015 \
    --chunksize 50000 \
    --embed-cap 200 \
    --reset
```


## Running Queries
### Phase 2 Python Query Suite
```
python phase2_document_model/code/phase2_queries.py
```
JavaScript Queries (Mongo Shell)

```
mongosh phase2_document_model/code/sample_queries.js
```
Functional Dependency Analysis
```
python phase2_document_model/code/discover_functional_dependencies.py --input data/ reddit-comments-may-2015/database.sqlite
```

