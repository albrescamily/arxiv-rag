import re

import feedparser
import requests


BASE_URL = "https://export.arxiv.org/api/query"

ARXIV_ID = re.compile(r"https?://arxiv\.org/abs/(?P<id>.+?)v(?P<version>\d+)")


def parse_arxiv_id(raw: str) -> tuple[str, int]:

    match = ARXIV_ID.fullmatch(raw)

    if match is None:
        raise ValueError(f"Unexpected arXiv id: {raw!r}")

    return match["id"], int(match["version"])


def parse_pdf_url(entry) -> str:

    for link in entry.links:
        if link.get("title") == "pdf":
            return link.href

    raise ValueError(f"No PDF link for {entry.id!r}")


def parse_entry(entry) -> dict:

    arxiv_id, version = parse_arxiv_id(entry.id)

    return {
        "arxiv_id": arxiv_id,
        "version": version,
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
        "primary_category": entry.arxiv_primary_category["term"],
        "pdf_url": parse_pdf_url(entry),
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