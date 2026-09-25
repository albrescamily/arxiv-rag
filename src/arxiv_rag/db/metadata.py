import json

from sqlalchemy import text

from arxiv_rag.db.connection import (
    metadata_engine,
)


def prepare_paper(
    paper: dict,
) -> dict:

    paper = paper.copy()

    paper["authors"] = json.dumps(
        paper["authors"]
    )

    paper["categories"] = json.dumps(
        paper["categories"]
    )

    return paper


def insert_papers(
    papers: list[dict],
):

    if not papers:
        return

    prepared = [
        prepare_paper(paper)
        for paper in papers
    ]

    query = text(
        """
        INSERT INTO papers (
            arxiv_id,
            title,
            summary,
            published,
            updated,
            authors,
            categories
        )

        VALUES (
            :arxiv_id,
            :title,
            :summary,
            :published,
            :updated,
            CAST(:authors AS JSONB),
            CAST(:categories AS JSONB)
        )

        ON CONFLICT (arxiv_id)

        DO UPDATE SET
            title = EXCLUDED.title,
            summary = EXCLUDED.summary,
            updated = EXCLUDED.updated,
            authors = EXCLUDED.authors,
            categories = EXCLUDED.categories;
        """
    )

    with metadata_engine.begin() as conn:

        conn.execute(
            query,
            prepared,
        )