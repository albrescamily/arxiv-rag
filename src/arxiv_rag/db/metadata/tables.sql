CREATE TABLE IF NOT EXISTS papers (

    arxiv_id TEXT PRIMARY KEY,

    title TEXT NOT NULL,

    summary TEXT,

    published TIMESTAMPTZ,

    updated TIMESTAMPTZ,

    authors JSONB NOT NULL,

    categories JSONB NOT NULL

);
