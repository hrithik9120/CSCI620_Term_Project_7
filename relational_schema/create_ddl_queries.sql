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
    link_id TEXT PRIMARY KEY,
    subreddit_id TEXT NOT NULL,
    author TEXT,
    created_utc INTEGER,
    archived INTEGER,
    gilded INTEGER DEFAULT 0,
    edited BIGINT,
    FOREIGN KEY (subreddit_id) REFERENCES Subreddit(subreddit_id),
    FOREIGN KEY (author) REFERENCES Users(author) ON DELETE SET NULL
);

-- =========================
-- Post_Link
-- =========================
CREATE TABLE Post_Link(
    link_id TEXT PRIMARY KEY,
    post_id TEXT NOT NULL REFERENCES Post(link_id),
    retrieved_on BIGINT
);

-- =========================
-- Comment
-- =========================
CREATE TABLE Comment(
    id TEXT PRIMARY KEY,
    body TEXT,
    author TEXT,
    link_id TEXT NOT NULL REFERENCES Post(link_id),  -- FIXED: References Post, not Post_Link
    parent_id TEXT,  -- REMOVED foreign key constraint
    created_utc BIGINT,
    retrieved_on BIGINT,
    score INTEGER,
    ups INTEGER,
    downs INTEGER,
    score_hidden INTEGER,
    gilded INTEGER DEFAULT 0,
    controversiality INTEGER,
    edited BIGINT,
    FOREIGN KEY (author) REFERENCES Users(author) ON DELETE SET NULL
    -- REMOVED: FOREIGN KEY (parent_id) REFERENCES Comment(id)
);

-- =========================
-- Moderation
-- =========================
CREATE TABLE Moderation (
    mod_action_id SERIAL PRIMARY KEY,
    target_type TEXT CHECK (target_type IN ('post','comment')),
    target_id TEXT NOT NULL,
    subreddit_id TEXT NOT NULL,
    removal_reason TEXT,
    distinguished TEXT,
    action_timestamp BIGINT DEFAULT EXTRACT(EPOCH FROM NOW()),
    FOREIGN KEY (subreddit_id) REFERENCES Subreddit(subreddit_id)
);