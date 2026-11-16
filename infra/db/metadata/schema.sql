CREATE TABLE IF NOT EXISTS papers (

    arxiv_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL CHECK (version > 0),

    title TEXT NOT NULL,

    summary TEXT NOT NULL,

    published TIMESTAMPTZ NOT NULL,

    updated TIMESTAMPTZ NOT NULL,

    authors TEXT[] NOT NULL CHECK (cardinality(authors) > 0),

    categories TEXT[] NOT NULL CHECK (cardinality(categories) > 0),

    primary_category TEXT NOT NULL,

    pdf_url TEXT NOT NULL,

    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CHECK (updated >= published)
);

CREATE INDEX IF NOT EXISTS papers_published_idx  ON papers (published);
CREATE INDEX IF NOT EXISTS papers_categories_idx ON papers USING GIN (categories);
