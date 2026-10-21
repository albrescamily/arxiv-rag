import feedparser
import requests


BASE_URL = "https://export.arxiv.org/api/query"


def parse_entry(entry) -> dict:
    return {
        "arxiv_id": entry.id.split("/abs/")[-1],
        "title": entry.title.strip(),
        "summary": entry.summary.strip(),
        "published": entry.published,
        "updated": entry.updated,
        "authors": [
            author.name
            for author in entry.authors
        ],
        "categories": [
            tag["term"]
            for tag in entry.tags
        ],
    }


def fetch_page(search_query: str,start: int,batch_size: int) -> dict:

    params = {
        "search_query": search_query,
        "start": start,
        "max_results": batch_size,
        "sortBy": "submittedDate",
        "sortOrder": "ascending",
    }

    response = requests.get(BASE_URL,params=params,timeout=(5, 30),)

    response.raise_for_status()

    feed = feedparser.parse(response.content)

    if feed.bozo:
        raise ValueError(f"Malformed feed: "f"{feed.bozo_exception}")

    if (len(feed.entries) == 1 and "api/errors" in feed.entries[0].id):
        raise ValueError("arXiv rejected query: "f"{feed.entries[0].summary}")

    total = int(feed.feed.opensearch_totalresults)

    papers = [parse_entry(entry)for entry in feed.entries]

    return {
        "papers": papers,
        "total": total,
    }