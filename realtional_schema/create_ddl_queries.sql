-- =========================
-- User
-- =========================
CREATE TABLE User (
    author TEXT PRIMARY KEY,
    author_flair_text TEXT,
    author_flair_css_class TEXT
);

-- =========================
-- Subreddit
-- =========================
CREATE TABLE Subreddit (
    subreddit_id TEXT PRIMARY KEY,
    subreddit TEXT
);

-- =========================
-- Post
-- =========================
CREATE TABLE Post (
    post_id TEXT PRIMARY KEY,
    subreddit_id TEXT NOT NULL,
    author TEXT,
    created_utc INTEGER,              -- epoch time (seconds)
    archived INTEGER CHECK (archived IN (0,1)),  -- boolean flag
    gilded INTEGER DEFAULT 0,
    edited INTEGER CHECK (edited IN (0,1)) DEFAULT 0,
    FOREIGN KEY (subreddit_id) REFERENCES Subreddit(subreddit_id),
    FOREIGN KEY (author) REFERENCES User(author) ON DELETE SET NULL
);

-- =========================
-- Post_Link
-- =========================
CREATE TABLE Post_Link (
    link_id TEXT PRIMARY KEY,
    post_id TEXT NOT NULL,
    retrieved_on INTEGER,             -- epoch time
    FOREIGN KEY (post_id) REFERENCES Post(post_id)
);

-- =========================
-- Comment
-- =========================
CREATE TABLE Comment (
    id TEXT PRIMARY KEY,
    body TEXT,
    author TEXT,
    link_id TEXT NOT NULL,            -- points to Post_Link
    parent_id TEXT,
    created_utc INTEGER,              -- epoch time
    retrieved_on INTEGER,             -- epoch time
    score INTEGER,
    ups INTEGER,
    downs INTEGER,
    score_hidden INTEGER CHECK (score_hidden IN (0,1)),  -- boolean flag
    gilded INTEGER DEFAULT 0,
    controversiality INTEGER CHECK (controversiality IN (0,1)),
    edited INTEGER CHECK (edited IN (0,1)) DEFAULT 0,
    FOREIGN KEY (author) REFERENCES User(author) ON DELETE SET NULL,
    FOREIGN KEY (link_id) REFERENCES Post_Link(link_id),
    FOREIGN KEY (parent_id) REFERENCES Comment(id) ON DELETE SET NULL
); 

-- =========================
-- Moderation
-- =========================
CREATE TABLE Moderation (
    mod_action_id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_type TEXT CHECK (target_type IN ('post','comment')),
    target_id TEXT NOT NULL,          -- post_id or comment_id
    subreddit_id TEXT NOT NULL,
    removal_reason TEXT,
    distinguished TEXT,
    action_timestamp INTEGER DEFAULT (strftime('%s','now')), -- epoch timestamp
    FOREIGN KEY (subreddit_id) REFERENCES Subreddit(subreddit_id)
);