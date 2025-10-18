SELECT COUNT(*) AS user_count FROM Users;
SELECT COUNT(*) AS subreddit_count FROM Subreddit;
SELECT COUNT(*) AS post_count FROM Post;
SELECT COUNT(*) AS post_link_count FROM Post_Link;
SELECT COUNT(*) AS comment_count FROM Comment;
SELECT COUNT(*) AS moderation_count FROM Moderation;


--FK Validation

SELECT COUNT(*) AS missing_user_refs
FROM Post p
LEFT JOIN Users u ON p.author = u.author
WHERE p.author IS NOT NULL AND u.author IS NULL;

SELECT COUNT(*) AS missing_subreddit_refs
FROM Post p
LEFT JOIN Subreddit s ON p.subreddit_id = s.subreddit_id
WHERE s.subreddit_id IS NULL;

SELECT COUNT(*) AS invalid_post_links
FROM Post_Link pl
LEFT JOIN Post p ON pl.post_id = p.link_id
WHERE p.link_id IS NULL;

SELECT COUNT(*) AS invalid_comment_links
FROM Comment c
LEFT JOIN Post_Link pl ON c.link_id = pl.link_id
WHERE pl.link_id IS NULL;

SELECT COUNT(*) AS invalid_parent_comments
FROM Comment c
LEFT JOIN Comment parent ON c.parent_id = parent.id
WHERE c.parent_id IS NOT NULL AND parent.id IS NULL;

SELECT COUNT(*) AS invalid_moderation_subs
FROM Moderation m
LEFT JOIN Subreddit s ON m.subreddit_id = s.subreddit_id
WHERE s.subreddit_id IS NULL;

--table join validation
SELECT p.link_id, u.author, s.subreddit, p.created_utc
FROM Post p
JOIN Users u ON p.author = u.author
JOIN Subreddit s ON p.subreddit_id = s.subreddit_id
LIMIT 10;

SELECT c.id, c.body, p.link_id AS post_link, u.author
FROM Comment c
JOIN Post_Link pl ON c.link_id = pl.link_id
JOIN Post p ON pl.post_id = p.link_id
LEFT JOIN Users u ON c.author = u.author
LIMIT 10;

SELECT m.mod_action_id, m.target_type, m.target_id, s.subreddit, m.removal_reason
FROM Moderation m
JOIN Subreddit s ON m.subreddit_id = s.subreddit_id
LIMIT 10;

-- posts per subreddit
SELECT subreddit_id, COUNT(*) AS posts
FROM Post
GROUP BY subreddit_id
ORDER BY posts DESC
LIMIT 10;

-- comments per post
SELECT link_id, COUNT(*) AS comment_count
FROM Comment
GROUP BY link_id
ORDER BY comment_count DESC
LIMIT 10;

--orphan summary
SELECT
  (SELECT COUNT(*) FROM Post p LEFT JOIN Users u ON p.author=u.author WHERE p.author IS NOT NULL AND u.author IS NULL) AS orphan_posts_by_user,
  (SELECT COUNT(*) FROM Post p LEFT JOIN Subreddit s ON p.subreddit_id=s.subreddit_id WHERE s.subreddit_id IS NULL) AS orphan_posts_by_subreddit,
  (SELECT COUNT(*) FROM Post_Link pl LEFT JOIN Post p ON pl.post_id=p.link_id WHERE p.link_id IS NULL) AS orphan_postlinks,
  (SELECT COUNT(*) FROM Comment c LEFT JOIN Post_Link pl ON c.link_id=pl.link_id WHERE pl.link_id IS NULL) AS orphan_comments,
  (SELECT COUNT(*) FROM Comment c LEFT JOIN Comment parent ON c.parent_id=parent.id WHERE c.parent_id IS NOT NULL AND parent.id IS NULL) AS orphan_comment_parents,
  (SELECT COUNT(*) FROM Moderation m LEFT JOIN Subreddit s ON m.subreddit_id=s.subreddit_id WHERE s.subreddit_id IS NULL) AS orphan_moderations;

