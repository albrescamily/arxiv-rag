from pathlib import Path

from arxiv_rag.db.metadata.connection import (
    metadata_engine,
)
from arxiv_rag.db.vectors.connection import (
    vector_engine,
)


DB_DIR = Path(__file__).parent

SCHEMAS = (
    (metadata_engine, DB_DIR / "metadata" / "schema.sql"),
    (vector_engine, DB_DIR / "vectors" / "schema.sql"),
)


def apply_schema(engine, schema_path: Path):

    query = schema_path.read_text(encoding="utf-8")

    with engine.begin() as conn:
        conn.exec_driver_sql(query)


def initialize_databases():

    for engine, schema_path in SCHEMAS:
        apply_schema(engine, schema_path)

    print("Databases initialized.")


if __name__ == "__main__":
    initialize_databases()
