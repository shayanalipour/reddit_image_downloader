LOGGER_NAME = "reddit_image_downloader"
CHUNK_SIZE = 1000
SUBREDDITS = ["ArchitecturePorn", "HousePorn"]

DB_COLUMNS = [
    "id TEXT PRIMARY KEY",
    "created DATE NOT NULL",
    "author TEXT NOT NULL",
    "title TEXT",
    "body TEXT",
    "score INTEGER",
    "num_comments INTEGER",
    "subreddit TEXT NOT NULL",
    "subreddit_id TEXT NOT NULL",
    "image_url TEXT",
    "image_name TEXT DEFAULT NULL",
    "matched_query TEXT NOT NULL",
]
