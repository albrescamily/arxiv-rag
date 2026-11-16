CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS embeddings (

    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    arxiv_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),

    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),

    content TEXT NOT NULL,

    embedding VECTOR(768) NOT NULL,

    embedded_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (arxiv_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS embeddings_embedding_idx ON embeddings USING hnsw (embedding vector_cosine_ops);
