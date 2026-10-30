import time
from datetime import datetime, timedelta, timezone

from prefect import flow, task
from prefect.logging import get_run_logger

from arxiv_rag.ingestion.client import (fetch_page as fetch_arxiv_page,)
from arxiv_rag.db.metadata.papers import (insert_papers)


ARXIV_MAX_RESULTS = 30_000

ARXIV_PAGE_LIMIT = 1_000


def next_batch_size(remaining: int, page_limit: int = ARXIV_PAGE_LIMIT) -> int:

    return max(0, min(page_limit, remaining))


def recent_window(days: int = 5, now: datetime | None = None) -> tuple[datetime, datetime]:

    now = now or datetime.now(timezone.utc)

    return now - timedelta(days=days), now



def build_query(categories: str, start: datetime, end: datetime) -> str:

    return (
        f"({categories}) AND submittedDate:"
        f"[{start.strftime("%Y%m%d%H%M")} TO {end.strftime("%Y%m%d%H%M")}]"
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
def ingestion_flow(search_query: str, page_limit: int = ARXIV_PAGE_LIMIT, max_papers: int = 10_000, wait_time: int = 3):

    logger = get_run_logger()

    if max_papers > ARXIV_MAX_RESULTS:
        raise ValueError(
            f"max_papers={max_papers} is above the arXiv API limit "
            f"of {ARXIV_MAX_RESULTS}. Split the window instead."
        )

    if not 1 <= page_limit <= 2_000:
        raise ValueError(f"page_limit={page_limit} must be between 1 and 2000.")

    start = 0
    total_available = None

    while True:

        target = max_papers if total_available is None else total_available

        batch_size = next_batch_size(target - start, page_limit)

        if batch_size == 0:
            break

        result = fetch_page(
            search_query=search_query,
            start=start,
            batch_size=batch_size,
        )

        papers = result["papers"]

        if total_available is None:

            total_available = result["total"]

            logger.info(f"arXiv query has {total_available} results")

            if total_available > max_papers:
                raise RuntimeError(
                    f"Window has {total_available} papers, above "
                    f"max_papers={max_papers}. Shrink the window or raise the cap."
                )

        if not papers:
            logger.info("No papers returned.")
            break

        load_metadata(papers)

        start += len(papers)

        logger.info(f"Processed: {start}/{total_available}")

        if start >= total_available:
            logger.info("All available papers processed.")
            break

        time.sleep(wait_time)


if __name__ == "__main__":

    categories = (
        "cat:cs.AI OR "   # artificial intelligence
        "cat:cs.LG OR "   # machine learning
        "cat:stat.ML OR " # machine learning (statistics side)
        "cat:cs.CL OR "   # computation and language: where LLM papers live
        "cat:cs.MA"       # multiagent systems
    )


    start, end = recent_window()

    ingestion_flow(
        search_query=build_query(categories, start, end),
    )