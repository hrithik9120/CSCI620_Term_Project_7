/* 
===============================================================================
 Reddit May 2015 – Cleaning Verification Script (Counts + 10 Examples)
 Group 7: Hrithik Gaikwad, Jie Zhang, Siddharth Bhople
-------------------------------------------------------------------------------
 This script verifies that cleaning occurred by comparing RAW vs CLEANED tables.
 It prints:
   • Count of removed rows
   • 10 example removed rows (if any)
   • Key validation checks should return ZERO rows
===============================================================================
*/

------------------------
-- 1. ROW COUNT SUMMARY
------------------------
SELECT
    (SELECT COUNT(*) FROM Users)              AS raw_users,
    (SELECT COUNT(*) FROM Users_cleaned)      AS clean_users,
    (SELECT COUNT(*) FROM Subreddit)          AS raw_subreddit,
    (SELECT COUNT(*) FROM Subreddit_cleaned)  AS clean_subreddit,
    (SELECT COUNT(*) FROM Post)               AS raw_post,
    (SELECT COUNT(*) FROM Post_cleaned)       AS clean_post,
    (SELECT COUNT(*) FROM Comment)            AS raw_comment,
    (SELECT COUNT(*) FROM Comment_cleaned)    AS clean_comment,
    (SELECT COUNT(*) FROM Moderation)         AS raw_mod,
    (SELECT COUNT(*) FROM Moderation_cleaned) AS clean_mod;

-------------------------------------------------------
-- 2. DIFFERENCE CHECKS: COUNTS + 10 SAMPLE ROWS
-------------------------------------------------------

-- =======================
-- USERS removed
-- =======================
SELECT COUNT(*) AS users_removed
FROM (
    SELECT * FROM Users
    EXCEPT
    SELECT * FROM Users_cleaned
) AS diff;

-- Show 10 example removed rows
SELECT *
FROM (
    SELECT * FROM Users
    EXCEPT
    SELECT * FROM Users_cleaned
) AS diff
LIMIT 10;


-- =======================
-- SUBREDDIT removed
-- =======================
SELECT COUNT(*) AS subreddit_removed
FROM (
    SELECT * FROM Subreddit
    EXCEPT
    SELECT * FROM Subreddit_cleaned
) AS diff;

SELECT *
FROM (
    SELECT * FROM Subreddit
    EXCEPT
    SELECT * FROM Subreddit_cleaned
) AS diff
LIMIT 10;


-- =======================
-- POSTS removed
-- =======================
SELECT COUNT(*) AS posts_removed
FROM (
    SELECT * FROM Post
    EXCEPT
    SELECT * FROM Post_cleaned
) AS diff;

SELECT *
FROM (
    SELECT * FROM Post
    EXCEPT
    SELECT * FROM Post_cleaned
) AS diff
LIMIT 10;


-- =======================
-- COMMENTS removed
-- =======================
SELECT COUNT(*) AS comments_removed
FROM (
    SELECT * FROM Comment
    EXCEPT
    SELECT * FROM Comment_cleaned
) AS diff;

SELECT *
FROM (
    SELECT * FROM Comment
    EXCEPT
    SELECT * FROM Comment_cleaned
) AS diff
LIMIT 10;


-- =======================
-- MODERATION removed
-- =======================
SELECT COUNT(*) AS moderation_removed
FROM (
    SELECT 
        mod_action_id, target_type, target_id, subreddit_id,
        removal_reason, distinguished, action_timestamp
    FROM Moderation
    EXCEPT
    SELECT 
        mod_action_id, target_type, target_id, subreddit_id,
        removal_reason, distinguished, action_timestamp
    FROM Moderation_cleaned
) AS diff;

SELECT *
FROM (
    SELECT 
        mod_action_id, target_type, target_id, subreddit_id,
        removal_reason, distinguished, action_timestamp
    FROM Moderation
    EXCEPT
    SELECT 
        mod_action_id, target_type, target_id, subreddit_id,
        removal_reason, distinguished, action_timestamp
    FROM Moderation_cleaned
) AS diff
LIMIT 10;


------------------------------------------------------------
-- 3. VALIDATION CHECKS — SHOULD RETURN ZERO ROWS
------------------------------------------------------------

-- Invalid timestamps
SELECT COUNT(*) AS invalid_comment_timestamps
FROM Comment_cleaned
WHERE created_utc < 1100000000 OR created_utc > 1800000000;

-- Deleted/removed bodies
SELECT COUNT(*) AS deleted_or_removed_bodies
FROM Comment_cleaned
WHERE body IN ('[deleted]', '[removed]')
   OR body ILIKE '%[deleted]%'
   OR body ILIKE '%[removed]%';

-- Score inconsistencies
SELECT COUNT(*) AS incorrect_scores
FROM Comment_cleaned
WHERE score IS NOT NULL AND ups IS NOT NULL AND downs IS NOT NULL
  AND score <> (ups - downs);

-- Orphan comments (broken FK)
SELECT COUNT(*) AS orphan_comments
FROM Comment_cleaned c
LEFT JOIN Post_cleaned p ON c.link_id = p.link_id
WHERE p.link_id IS NULL;