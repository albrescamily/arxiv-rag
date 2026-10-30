
from sqlalchemy import text

from arxiv_rag.db.metadata.connection import metadata_engine


QUERY = text(
    """
    INSERT INTO papers (
        arxiv_id, version, title, summary, published, updated,
        authors, categories, primary_category, pdf_url
    )
    VALUES (
        :arxiv_id, :version, :title, :summary, :published, :updated,
        :authors, :categories, :primary_category, :pdf_url
    )
    ON CONFLICT (arxiv_id) DO UPDATE SET
        version          = EXCLUDED.version,
        title            = EXCLUDED.title,
        summary          = EXCLUDED.summary,
        updated          = EXCLUDED.updated,
        authors          = EXCLUDED.authors,
        categories       = EXCLUDED.categories,
        primary_category = EXCLUDED.primary_category,
        pdf_url          = EXCLUDED.pdf_url,
        changed_at       = now()
    WHERE EXCLUDED.updated > papers.updated
    """
)

def insert_papers(papers: list[dict],):

    if not papers:
        return


    with metadata_engine.begin() as conn:

        conn.execute(
            QUERY,
            papers,
        )