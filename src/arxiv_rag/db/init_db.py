from sqlalchemy import text

from arxiv_rag.db.connection import (
    metadata_engine,
    vector_engine,
)

def create_metadata_schema():

    query = """
    CREATE TABLE IF NOT EXISTS papers (
        id BIGSERIAL PRIMARY KEY,

        arxiv_id TEXT UNIQUE NOT NULL,

        title TEXT NOT NULL,

        summary TEXT,

        published TIMESTAMPTZ,

        updated TIMESTAMPTZ,

        authors JSONB NOT NULL,

        categories JSONB NOT NULL
    );
    """

    with metadata_engine.begin() as conn:
        conn.execute(text(query))


def initialize_databases():

    create_metadata_schema()

    print("Database initialized.")


if __name__ == "__main__":
    initialize_databases()