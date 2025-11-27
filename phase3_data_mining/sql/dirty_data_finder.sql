/* 
===============================================================================
 Redddit May 2015 – Dirty Data Identification Script
 Phase III – Data Cleaning
 Author: Hrithik Gaikwad,Jie Zhang,Siddharth Bhople (Group 7)
 Description:
     This file contains SQL queries designed to systematically identify all
     major classes of dirty data in the Reddit May 2015 dataset before
     performing cleaning or mining tasks.

     Each section detects a specific issue:
     - Missing values
     - Orphans (broken foreign keys)
     - Invalid timestamps
     - Duplicates
     - Contradictions
     - Textual corruption
     - Structural relationship errors

     These queries satisfy the “Identify Dirty Data” section of the rubric.
===============================================================================
*/


/* ============================================================================
   1. NULL / MISSING CRITICAL FIELDS
   These indicate incomplete or corrupted records and must be removed or filled.
============================================================================ */

/* 1.1 Missing or empty authors */
SELECT *
FROM Comment
WHERE author IS NULL OR author = '';
#empty

/* 1.2 Missing subreddit_id in Posts */
SELECT *
FROM Post
WHERE subreddit_id IS NULL OR subreddit_id = '';
#empty value

/* 1.3 Missing bodies in Comments */
SELECT *
FROM Comment
WHERE body IS NULL OR body = '';
#correct


/* ============================================================================
   2. INVALID OR IMPOSSIBLE TIMESTAMPS
   Reddit timestamps are UNIX integers.
   Valid range (approx): 1100000000 (2005) – 1800000000 (2027).
============================================================================ */

/* 2.1 Negative or zero timestamps */
SELECT *
FROM Comment
WHERE created_utc <= 0;
#empty value


/* 2.2 Timestamps outside realistic range */
SELECT *
FROM Comment
WHERE created_utc < 1100000000
   OR created_utc > 1800000000;
#empty



/* ============================================================================
   3. ORPHAN RECORDS (BROKEN FOREIGN KEYS)
   These occur when a row references a record that does not exist.
============================================================================ */

/* 3.1 Comments referencing posts that do not exist */
SELECT c.*
FROM Comment c
LEFT JOIN Post p ON c.link_id = p.link_id
WHERE p.link_id IS NULL;
#empty value


/* 3.2 Posts referencing missing subreddits */
SELECT p.*
FROM Post p
LEFT JOIN Subreddit s ON p.subreddit_id = s.subreddit_id
WHERE s.subreddit_id IS NULL;
#empty

/* 3.3 Comments referencing authors that are missing in Users table */
SELECT c.*
FROM Comment c
LEFT JOIN Users u ON c.author = u.author
WHERE u.author IS NULL
  AND c.author IS NOT NULL;
#empty


/* ============================================================================
   4. DUPLICATE RECORDS
   Duplicate IDs violate primary-key semantics and create mining distortions.
============================================================================ */

/* 4.1 Duplicate posts (duplicate link_id) */
SELECT link_id, COUNT(*)
FROM Post
GROUP BY link_id
HAVING COUNT(*) > 1;
#empty

/* 4.2 Duplicate comments */
SELECT id, COUNT(*)
FROM Comment
GROUP BY id
HAVING COUNT(*) > 1;
#empty

/* 4.3 Duplicate users (same author name counted multiple times) */
SELECT author, COUNT(*)
FROM Users
GROUP BY author
HAVING COUNT(*) > 1;
#empty


/* ============================================================================
   5. CONTRADICTIONS OR INCONSISTENT VALUES
   These indicate corrupted or logically invalid fields.
============================================================================ */

/* 5.1 Score inconsistent with ups - downs */
SELECT id, score, ups, downs
FROM Comment
WHERE score IS DISTINCT FROM (ups - downs);
#empty

/* 5.2 Invalid "edited" values */
SELECT *
FROM Comment
WHERE edited NOTNULL      -- must be numeric
  AND edited < 0;         -- negative timestamps are impossible
--empty

/* 5.3 Archived posts that are also marked edited (rare but invalid scenario) */
SELECT *
FROM Post
WHERE archived = 1
  AND edited > 0;
#empty


/* ============================================================================
   6. INVALID FLAIR FIELDS
   Flair is user metadata; incorrect flair may indicate corruption.
============================================================================ */

/* 6.1 Flair present but no corresponding user */
SELECT *
FROM Users
WHERE (author_flair_text IS NOT NULL OR author_flair_css_class IS NOT NULL)
  AND author IS NULL;
#empty

/* 6.2 Nonsensical or placeholder flair */
SELECT *
FROM Users
WHERE author_flair_text ~ '^[!@#$%^&*]';
#correct


/* ============================================================================
   7. PARENT–CHILD RELATIONSHIP ISSUES
   Reddit comments form a tree; broken parent links break thread structure.
============================================================================ */

/* 7.1 Comments referring to parent comments that do not exist */
SELECT c.*
FROM Comment c
LEFT JOIN Comment p ON c.parent_id = p.id
WHERE c.parent_id LIKE 't1_%'
  AND p.id IS NULL;
#correct

/* 7.2 Comments referring to posts that do not exist */
SELECT c.*
FROM Comment c
LEFT JOIN Post p ON c.parent_id = p.link_id
WHERE c.parent_id LIKE 't3_%'
  AND p.link_id IS NULL;
#empty


/* ============================================================================
   8. TEXTUAL DIRTY DATA
   "[deleted]" and "[removed]" indicate unusable comment content.
============================================================================ */

/* 8.1 Deleted or removed comments */
SELECT *
FROM Comment
WHERE body IN ('[deleted]', '[removed]')
   OR body ILIKE '%[deleted]%'
   OR body ILIKE '%[removed]%';
#correct    

/* 8.2 Comments with deleted or removed bodies */
SELECT *
FROM Comment
WHERE body IN ('[deleted]', '[removed]')
   OR body ILIKE '%[deleted]%'
   OR body ILIKE '%[removed]%';
#correct
