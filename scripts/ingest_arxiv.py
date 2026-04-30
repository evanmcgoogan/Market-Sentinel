"""
ingest_arxiv.py — Poll arXiv for new AI/ML/economics research papers.

Uses the arXiv Atom API (free, no key required) to discover papers by category
and by S-tier author watchlist. Stores title + abstract as raw files for
downstream extraction. Full PDFs are not fetched — abstracts carry enough
signal for the extraction pipeline to identify key claims.

Discovery strategy:
  1. Category feeds: cs.AI, cs.LG, cs.CL, cs.NE, econ.GN, q-fin.GN
  2. Author watchlist: S-tier researchers (Karpathy, Sutskever, LeCun, etc.)
  3. Dedup by arXiv ID across both passes.

Output: raw/papers/{category}/YYYY-MM-DD--{arxiv-id}--{title-slug}.md

Usage:
    python scripts/ingest_arxiv.py               # Poll all categories + authors
    python scripts/ingest_arxiv.py --category cs.AI
    python scripts/ingest_arxiv.py --author "Andrej Karpathy"
    python scripts/ingest_arxiv.py --dry-run     # Show what would be fetched

No API key required.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
import xml.etree.ElementTree as ET
from typing import Any
from urllib.parse import urlencode

import aiohttp

from brain_io import (
    append_log,
    is_duplicate,
    load_sources_config,
    record_hash,
    slugify,
    utcnow,
    write_raw_file,
)

logger = logging.getLogger(__name__)

ARXIV_API_BASE = "https://export.arxiv.org/api/query"
ARXIV_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "arxiv": "http://arxiv.org/schemas/atom",
    "opensearch": "http://a9.com/-/spec/opensearch/1.1/",
}
MAX_RESULTS_PER_QUERY = 25
REQUEST_DELAY_S = 3.0  # arXiv asks for ≥3s between requests


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_arxiv_config() -> dict[str, Any]:
    """Load arxiv section from sources.yaml."""
    config = load_sources_config()
    return config.get("arxiv", {})


# ---------------------------------------------------------------------------
# arXiv API fetch
# ---------------------------------------------------------------------------

async def fetch_arxiv_query(
    session: aiohttp.ClientSession,
    search_query: str,
    max_results: int = MAX_RESULTS_PER_QUERY,
) -> list[dict[str, Any]]:
    """Fetch papers from the arXiv API for a given query string.

    Returns a list of paper dicts with: arxiv_id, title, authors, abstract,
    categories, primary_category, published, updated, url, pdf_url.
    """
    params = urlencode({
        "search_query": search_query,
        "max_results": max_results,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    })
    url = f"{ARXIV_API_BASE}?{params}"

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                logger.warning("arXiv API returned HTTP %d for query: %s", resp.status, search_query)
                return []
            text = await resp.text()
    except Exception as e:
        logger.error("arXiv API error for query '%s': %s", search_query, e)
        return []

    return _parse_arxiv_feed(text)


def _parse_arxiv_feed(xml_text: str) -> list[dict[str, Any]]:
    """Parse the arXiv Atom feed into a list of paper dicts."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.error("arXiv XML parse error: %s", e)
        return []

    papers = []
    for entry in root.findall("atom:entry", ARXIV_NS):
        # Skip the "no results" pseudo-entry arXiv sometimes returns
        title_el = entry.find("atom:title", ARXIV_NS)
        if title_el is None:
            continue
        title = (title_el.text or "").strip().replace("\n", " ")
        if not title or title.lower().startswith("error:"):
            continue

        # arXiv ID — strip version suffix (e.g. "2504.01234v2" → "2504.01234")
        id_el = entry.find("atom:id", ARXIV_NS)
        raw_id = (id_el.text or "").strip() if id_el is not None else ""
        arxiv_id = re.sub(r"v\d+$", "", raw_id.split("/abs/")[-1])
        if not arxiv_id:
            continue

        # Authors
        authors = []
        for author_el in entry.findall("atom:author", ARXIV_NS):
            name_el = author_el.find("atom:name", ARXIV_NS)
            if name_el is not None and name_el.text:
                authors.append(name_el.text.strip())

        # Abstract
        summary_el = entry.find("atom:summary", ARXIV_NS)
        abstract = (summary_el.text or "").strip().replace("\n", " ") if summary_el is not None else ""

        # Categories
        primary_cat_el = entry.find("arxiv:primary_category", ARXIV_NS)
        primary_cat = primary_cat_el.get("term", "") if primary_cat_el is not None else ""
        all_cats = [
            el.get("term", "")
            for el in entry.findall("atom:category", ARXIV_NS)
            if el.get("term")
        ]

        # Published date (YYYY-MM-DD)
        pub_el = entry.find("atom:published", ARXIV_NS)
        published = (pub_el.text or "")[:10] if pub_el is not None else ""

        # URLs
        arxiv_url = f"https://arxiv.org/abs/{arxiv_id}"
        pdf_url = f"https://arxiv.org/pdf/{arxiv_id}"

        papers.append({
            "arxiv_id": arxiv_id,
            "title": title,
            "authors": authors,
            "abstract": abstract,
            "primary_category": primary_cat,
            "categories": all_cats,
            "published": published,
            "url": arxiv_url,
            "pdf_url": pdf_url,
        })

    return papers


# ---------------------------------------------------------------------------
# Raw file formatting
# ---------------------------------------------------------------------------

def format_paper_body(paper: dict[str, Any]) -> str:
    """Format an arXiv paper as a readable markdown body."""
    authors_str = ", ".join(paper["authors"][:8])
    if len(paper["authors"]) > 8:
        authors_str += f" et al. ({len(paper['authors'])} total)"

    cats_str = ", ".join(paper["categories"])

    lines = [
        f"# {paper['title']}",
        "",
        f"**Authors:** {authors_str}",
        f"**arXiv:** [{paper['arxiv_id']}]({paper['url']})",
        f"**PDF:** {paper['pdf_url']}",
        f"**Published:** {paper['published']}",
        f"**Categories:** {cats_str}",
        "",
        "---",
        "",
        "## Abstract",
        "",
        paper["abstract"],
        "",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Ingestion logic
# ---------------------------------------------------------------------------

async def ingest_category(
    session: aiohttp.ClientSession,
    category_id: str,
    tier: str,
    domains: list[str],
    dry_run: bool = False,
) -> dict[str, int]:
    """Ingest new papers from a single arXiv category feed."""
    stats = {"papers_found": 0, "papers_new": 0, "files_written": 0}

    query = f"cat:{category_id}"
    papers = await fetch_arxiv_query(session, query)
    stats["papers_found"] = len(papers)

    for paper in papers:
        arxiv_id = paper["arxiv_id"]

        if is_duplicate("arxiv", arxiv_id):
            continue

        stats["papers_new"] += 1

        if dry_run:
            logger.info(
                "[DRY RUN] Would write: %s — %s (%d authors)",
                arxiv_id, paper["title"][:60], len(paper["authors"])
            )
            continue

        # Determine storage path using primary category (may differ from query category)
        store_cat = (paper["primary_category"] or category_id).replace(".", "-").lower()
        date_str = paper["published"] or utcnow().strftime("%Y-%m-%d")
        title_slug = slugify(paper["title"])
        relative_path = f"raw/papers/{store_cat}/{date_str}--{arxiv_id}--{title_slug}.md"

        frontmatter: dict[str, Any] = {
            "source": "arxiv",
            "arxiv_id": arxiv_id,
            "title": paper["title"],
            "authors": paper["authors"][:8],
            "primary_category": paper["primary_category"],
            "categories": paper["categories"],
            "published_at": paper["published"],
            "url": paper["url"],
            "pdf_url": paper["pdf_url"],
            "tier": tier,
            "domains": domains,
            "collected_at": utcnow(),
        }

        body = format_paper_body(paper)
        write_raw_file(relative_path, frontmatter, body)
        record_hash("arxiv", arxiv_id)
        stats["files_written"] += 1

    if not dry_run and stats["papers_new"] > 0:
        append_log(
            f"INGEST_ARXIV {category_id} | "
            f"found: {stats['papers_found']} | "
            f"new: {stats['papers_new']} | "
            f"files: {stats['files_written']}"
        )

    return stats


async def ingest_author(
    session: aiohttp.ClientSession,
    author_name: str,
    tier: str,
    domains: list[str],
    dry_run: bool = False,
) -> dict[str, int]:
    """Ingest new papers from a specific arXiv author."""
    stats = {"papers_found": 0, "papers_new": 0, "files_written": 0}

    query = f'au:"{author_name}"'
    papers = await fetch_arxiv_query(session, query, max_results=10)
    stats["papers_found"] = len(papers)

    for paper in papers:
        arxiv_id = paper["arxiv_id"]

        if is_duplicate("arxiv", arxiv_id):
            continue

        stats["papers_new"] += 1

        if dry_run:
            logger.info(
                "[DRY RUN] Author %s paper: %s — %s",
                author_name, arxiv_id, paper["title"][:60]
            )
            continue

        store_cat = (paper["primary_category"] or "cs-ai").replace(".", "-").lower()
        date_str = paper["published"] or utcnow().strftime("%Y-%m-%d")
        title_slug = slugify(paper["title"])
        relative_path = f"raw/papers/{store_cat}/{date_str}--{arxiv_id}--{title_slug}.md"

        frontmatter: dict[str, Any] = {
            "source": "arxiv",
            "arxiv_id": arxiv_id,
            "title": paper["title"],
            "authors": paper["authors"][:8],
            "primary_category": paper["primary_category"],
            "categories": paper["categories"],
            "published_at": paper["published"],
            "url": paper["url"],
            "pdf_url": paper["pdf_url"],
            "tier": tier,
            "domains": domains,
            "collected_at": utcnow(),
            # Tag author watchlist hit so extraction can prioritize
            "watchlist_author": author_name,
        }

        body = format_paper_body(paper)
        write_raw_file(relative_path, frontmatter, body)
        record_hash("arxiv", arxiv_id)
        stats["files_written"] += 1

    return stats


async def ingest_all(
    category_filter: str | None = None,
    author_filter: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run arXiv ingestion for all configured categories and authors."""
    config = load_arxiv_config()
    categories = config.get("categories", [])
    authors = config.get("author_watchlist", [])

    if not categories and not authors:
        logger.warning("No arXiv categories or authors configured in sources.yaml")
        return {"categories_processed": 0, "authors_processed": 0}

    if category_filter:
        categories = [c for c in categories if c.get("id", "") == category_filter]
    if author_filter:
        authors = [a for a in authors if a.get("name", "").lower() == author_filter.lower()]

    totals: dict[str, Any] = {
        "categories_processed": 0,
        "authors_processed": 0,
        "papers_found": 0,
        "papers_new": 0,
        "files_written": 0,
    }

    async with aiohttp.ClientSession() as session:
        # --- Category sweep ---
        for cat in categories:
            cat_id = cat.get("id", "")
            if not cat_id:
                continue

            tier = cat.get("tier", "B")
            domains = cat.get("domains", [cat_id.replace(".", "-").lower()])

            logger.info("Ingesting arXiv category: %s (%s)", cat_id, cat.get("label", ""))
            stats = await ingest_category(session, cat_id, tier, domains, dry_run)
            totals["categories_processed"] += 1
            totals["papers_found"] += stats["papers_found"]
            totals["papers_new"] += stats["papers_new"]
            totals["files_written"] += stats["files_written"]

            # arXiv asks for polite intervals
            await asyncio.sleep(REQUEST_DELAY_S)

        # --- Author watchlist sweep ---
        for author in authors:
            name = author.get("name", "")
            if not name:
                continue

            tier = author.get("tier", "A")
            domains = author.get("domains", ["ai", "deep-learning"])

            logger.info("Ingesting arXiv author: %s", name)
            stats = await ingest_author(session, name, tier, domains, dry_run)
            totals["authors_processed"] += 1
            totals["papers_new"] += stats["papers_new"]
            totals["files_written"] += stats["files_written"]

            await asyncio.sleep(REQUEST_DELAY_S)

    logger.info(
        "arXiv ingestion complete: %d categories, %d authors, %d new papers, %d files",
        totals["categories_processed"],
        totals["authors_processed"],
        totals["papers_new"],
        totals["files_written"],
    )

    if not dry_run and totals["papers_new"] > 0:
        append_log(
            f"INGEST_ARXIV BATCH | "
            f"categories: {totals['categories_processed']} | "
            f"authors: {totals['authors_processed']} | "
            f"new_papers: {totals['papers_new']} | "
            f"files: {totals['files_written']}"
        )

    return totals


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest arXiv papers for configured categories and author watchlist"
    )
    parser.add_argument("--category", help="Ingest only this arXiv category (e.g. cs.AI)")
    parser.add_argument("--author", help="Ingest only papers from this author name")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fetched")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    result = asyncio.run(ingest_all(
        category_filter=args.category,
        author_filter=args.author,
        dry_run=args.dry_run,
    ))
    if "error" in result:
        sys.exit(1)


if __name__ == "__main__":
    main()
