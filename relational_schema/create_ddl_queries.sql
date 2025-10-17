DROP TABLE IF EXISTS Users CASCADE;
DROP TABLE IF EXISTS Subreddit CASCADE;
DROP TABLE IF EXISTS Post CASCADE;
DROP TABLE IF EXISTS Post_Link CASCADE;
DROP TABLE IF EXISTS Comment CASCADE;
DROP TABLE IF EXISTS Moderation CASCADE;
-- =========================
-- Reddit_Users
-- =========================

CREATE TABLE Users (
    author TEXT PRIMARY KEY,
    author_flair_text TEXT,
    author_flair_css_class TEXT
);

-- =========================
-- Subreddit
-- =========================
CREATE TABLE Subreddit(
    subreddit_id TEXT PRIMARY KEY,
    subreddit TEXT
);

-- =========================
-- Post
-- =========================

CREATE TABLE Post (
    link_id TEXT PRIMARY KEY,--change to link_id
    subreddit_id TEXT NOT NULL,
    author TEXT,
    created_utc INTEGER,              -- epoch time (seconds)
    archived INTEGER,  -- allow null
    gilded INTEGER DEFAULT 0,
    edited BIGINT, --change to timestamp or boolean
    FOREIGN KEY (subreddit_id) REFERENCES Subreddit(subreddit_id),
    FOREIGN KEY (author) REFERENCES Users(author) ON DELETE SET NULL
);

-- =========================
-- Post_Link
-- =========================
CREATE TABLE Post_Link(
    link_id TEXT PRIMARY KEY,
    post_id TEXT NOT NULL REFERENCES Post(link_id),--change here
    retrieved_on BIGINT            -- change

);

-- =========================
-- Comment
-- =========================
CREATE TABLE Comment(
    id TEXT PRIMARY KEY,
    body TEXT,
    author TEXT,
    link_id TEXT NOT NULL REFERENCES Post_Link(link_id),            -- points to Post_Link
    parent_id TEXT,
    created_utc BIGINT,              -- epoch time
    retrieved_on BIGINT,             -- epoch time
    score INTEGER,
    ups INTEGER,
    downs INTEGER,
    score_hidden INTEGER,  -- boolean flag
    gilded INTEGER DEFAULT 0,
    controversiality INTEGER,
    edited BIGINT,
    FOREIGN KEY (author) REFERENCES Users(author) ON DELETE SET NULL,
    FOREIGN KEY (parent_id) REFERENCES Comment(id) ON DELETE SET NULL
); 

-- =========================
-- Moderation
-- =========================
CREATE TABLE Moderation (
    mod_action_id SERIAL PRIMARY KEY,
    target_type TEXT CHECK (target_type IN ('post','comment')),
    target_id TEXT NOT NULL,          -- post_id or comment_id
    subreddit_id TEXT NOT NULL,
    removal_reason TEXT,
    distinguished TEXT,
    action_timestamp BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()), -- epoch timestamp, change here to Postgresql format
    FOREIGN KEY (subreddit_id) REFERENCES Subreddit(subreddit_id)
);