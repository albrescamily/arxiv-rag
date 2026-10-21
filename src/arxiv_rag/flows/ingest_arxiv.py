import time

from prefect import flow, task
from prefect.logging import get_run_logger

from arxiv_rag.ingestion.client import (
    fetch_page as fetch_arxiv_page,
)

from arxiv_rag.db.metadata.papers import (
    insert_papers,
)


@task(
    retries=3,
    retry_delay_seconds=10,
)
def fetch_page(search_query: str, start: int, batch_size: int) -> dict:

    logger = get_run_logger()

    logger.info(
        f"Fetching arXiv page "
        f"start={start} "
        f"batch_size={batch_size}"
    )

    result = fetch_arxiv_page(
        search_query=search_query,
        start=start,
        batch_size=batch_size,
    )

    logger.info(
        f"Received "
        f"{len(result['papers'])} papers"
    )

    return result


@task(
    retries=3,
    retry_delay_seconds=5,
)
def load_metadata(papers: list[dict]):

    logger = get_run_logger()

    logger.info(
        f"Loading {len(papers)} "
        f"papers into PostgreSQL"
    )

    insert_papers(papers)

    logger.info(
        "Metadata loaded successfully"
    )


@flow(
    name="arxiv-ingestion",
    log_prints=True,
)
def ingestion_flow(search_query: str, batch_size: int = 10, max_papers: int = 50, wait_time: int = 3):

    logger = get_run_logger()

    start = 0
    processed = 0
    total_available = None

    while processed < max_papers:

        remaining = (
            max_papers - processed
        )

        current_batch_size = min(
            batch_size,
            remaining,
        )

        result = fetch_page(
            search_query=search_query,
            start=start,
            batch_size=current_batch_size,
        )

        papers = result["papers"]

        if total_available is None:

            total_available = (
                result["total"]
            )

            logger.info(
                f"arXiv query has "
                f"{total_available} results"
            )

        if not papers:
            logger.info(
                "No papers returned."
            )
            break

        load_metadata(
            papers
        )

        count = len(papers)

        start += count
        processed += count

        logger.info(
            f"Processed: "
            f"{processed}/{max_papers}"
        )

        if start >= total_available:
            logger.info(
                "All available papers "
                "processed."
            )
            break

        if processed >= max_papers:
            break

        time.sleep(
            wait_time
        )


if __name__ == "__main__":

    query = (
        "cat:cs.AI OR "
        "cat:cs.LG OR "
        "cat:cs.CL"
    )

    ingestion_flow(
        search_query=query,
        batch_size=5,
        max_papers=20,
    )