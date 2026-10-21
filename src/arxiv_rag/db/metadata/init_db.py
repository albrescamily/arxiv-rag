from pathlib import Path

from arxiv_rag.db.metadata.connection import (
    metadata_engine,
)


SCHEMA_PATH = Path(__file__).with_name("tables.sql")


def create_metadata_schema():

    query = SCHEMA_PATH.read_text(encoding="utf-8")

    with metadata_engine.begin() as conn:
        conn.exec_driver_sql(query)


def initialize_databases():

    create_metadata_schema()

    print("Database initialized.")


if __name__ == "__main__":
    initialize_databases()