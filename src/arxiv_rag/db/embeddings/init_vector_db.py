from pathlib import Path

from arxiv_rag.db.embeddings.connection import (
    vector_engine,
)


SCHEMA_PATH = Path(__file__).with_name("embedding_tables.sql")


def create_vector_schema():

    query = SCHEMA_PATH.read_text(encoding="utf-8")

    with vector_engine.begin() as conn:
        conn.exec_driver_sql(query)


def initialize_vector_database():

    create_vector_schema()

    print("Vector database initialized.")


if __name__ == "__main__":
    initialize_vector_database()
