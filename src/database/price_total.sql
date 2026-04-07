CREATE TABLE IF NOT EXISTS price_total (
    ruid TEXT PRIMARY KEY,
    series TEXT NOT NULL,
    price_cny REAL NOT NULL,
    date TEXT NOT NULL,
    location TEXT,
    source_url TEXT,
    gid TEXT,
    evidence_where TEXT,
    evidence_content TEXT
);