"""Tests for ingest_arxiv.py — arXiv paper ingestion pipeline.

No network calls. All feed content is injected as fixture strings.
Tests cover: feed parsing, ID dedup, category sweep, author watchlist,
file formatting, error handling, and CLI argument mapping.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import ingest_arxiv


# ---------------------------------------------------------------------------
# Feed fixtures
# ---------------------------------------------------------------------------

SAMPLE_ARXIV_ATOM = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:arxiv="http://arxiv.org/schemas/atom"
      xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
  <title>ArXiv Query: cs.AI</title>
  <opensearch:totalResults>2</opensearch:totalResults>
  <entry>
    <id>https://arxiv.org/abs/2504.12345v1</id>
    <title>Advances in Large Language Models for Reasoning</title>
    <summary>We present a novel training approach that improves reasoning
    capabilities in LLMs by combining chain-of-thought and self-consistency.
    Results show significant improvements on math benchmarks.</summary>
    <published>2025-04-10T00:00:00Z</published>
    <updated>2025-04-10T00:00:00Z</updated>
    <author><name>Andrej Karpathy</name></author>
    <author><name>Jane Smith</name></author>
    <category term="cs.AI" scheme="http://arxiv.org/schemas/atom"/>
    <category term="cs.LG" scheme="http://arxiv.org/schemas/atom"/>
    <arxiv:primary_category term="cs.AI" scheme="http://arxiv.org/schemas/atom"/>
    <link rel="alternate" href="https://arxiv.org/abs/2504.12345"/>
  </entry>
  <entry>
    <id>https://arxiv.org/abs/2504.99999v2</id>
    <title>Scaling Laws for Neural Language Models Revisited</title>
    <summary>This paper revisits scaling laws for neural language models and
    finds new relationships between compute, parameters, and performance.</summary>
    <published>2025-04-09T00:00:00Z</published>
    <updated>2025-04-10T00:00:00Z</updated>
    <author><name>Alice Researcher</name></author>
    <category term="cs.LG" scheme="http://arxiv.org/schemas/atom"/>
    <arxiv:primary_category term="cs.LG" scheme="http://arxiv.org/schemas/atom"/>
    <link rel="alternate" href="https://arxiv.org/abs/2504.99999"/>
  </entry>
</feed>"""

EMPTY_ARXIV_ATOM = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">
  <title>ArXiv Query: no results</title>
  <opensearch:totalResults>0</opensearch:totalResults>
</feed>"""

MALFORMED_XML = "this is not valid xml <<<<"


# ---------------------------------------------------------------------------
# Feed parsing tests
# ---------------------------------------------------------------------------

class TestParseArxivFeed:
    def test_parses_two_entries(self):
        papers = ingest_arxiv._parse_arxiv_feed(SAMPLE_ARXIV_ATOM)
        assert len(papers) == 2

    def test_extracts_arxiv_id(self):
        papers = ingest_arxiv._parse_arxiv_feed(SAMPLE_ARXIV_ATOM)
        assert papers[0]["arxiv_id"] == "2504.12345"
        assert papers[1]["arxiv_id"] == "2504.99999"

    def test_strips_version_from_id(self):
        """arXiv IDs like 2504.12345v1 should be stored as 2504.12345."""
        papers = ingest_arxiv._parse_arxiv_feed(SAMPLE_ARXIV_ATOM)
        assert "v1" not in papers[0]["arxiv_id"]
        assert "v2" not in papers[1]["arxiv_id"]

    def test_extracts_title(self):
        papers = ingest_arxiv._parse_arxiv_feed(SAMPLE_ARXIV_ATOM)
        assert "Large Language Models" in papers[0]["title"]

    def test_extracts_authors(self):
        papers = ingest_arxiv._parse_arxiv_feed(SAMPLE_ARXIV_ATOM)
        assert "Andrej Karpathy" in papers[0]["authors"]
        assert "Jane Smith" in papers[0]["authors"]

    def test_extracts_abstract(self):
        papers = ingest_arxiv._parse_arxiv_feed(SAMPLE_ARXIV_ATOM)
        assert "chain-of-thought" in papers[0]["abstract"]

    def test_extracts_primary_category(self):
        papers = ingest_arxiv._parse_arxiv_feed(SAMPLE_ARXIV_ATOM)
        assert papers[0]["primary_category"] == "cs.AI"
        assert papers[1]["primary_category"] == "cs.LG"

    def test_extracts_all_categories(self):
        papers = ingest_arxiv._parse_arxiv_feed(SAMPLE_ARXIV_ATOM)
        assert "cs.AI" in papers[0]["categories"]
        assert "cs.LG" in papers[0]["categories"]

    def test_extracts_published_date(self):
        papers = ingest_arxiv._parse_arxiv_feed(SAMPLE_ARXIV_ATOM)
        assert papers[0]["published"] == "2025-04-10"

    def test_builds_arxiv_url(self):
        papers = ingest_arxiv._parse_arxiv_feed(SAMPLE_ARXIV_ATOM)
        assert "arxiv.org/abs/2504.12345" in papers[0]["url"]

    def test_builds_pdf_url(self):
        papers = ingest_arxiv._parse_arxiv_feed(SAMPLE_ARXIV_ATOM)
        assert "arxiv.org/pdf/2504.12345" in papers[0]["pdf_url"]

    def test_empty_feed_returns_empty_list(self):
        papers = ingest_arxiv._parse_arxiv_feed(EMPTY_ARXIV_ATOM)
        assert papers == []

    def test_malformed_xml_returns_empty_list(self):
        papers = ingest_arxiv._parse_arxiv_feed(MALFORMED_XML)
        assert papers == []

    def test_skips_error_entries(self):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry><id>fake</id><title>Error: Bad request</title></entry>
  <entry>
    <id>https://arxiv.org/abs/2504.11111</id>
    <title>Valid Paper</title>
    <summary>Good abstract.</summary>
    <published>2025-04-10T00:00:00Z</published>
    <author><name>Author One</name></author>
  </entry>
</feed>"""
        papers = ingest_arxiv._parse_arxiv_feed(xml)
        # Error entry should be skipped
        assert all("error" not in p["title"].lower() for p in papers)


# ---------------------------------------------------------------------------
# Paper body formatting
# ---------------------------------------------------------------------------

class TestFormatPaperBody:
    def _make_paper(self) -> dict:
        return {
            "arxiv_id": "2504.12345",
            "title": "Test Paper on AI",
            "authors": ["Alice Author", "Bob Builder"],
            "abstract": "This paper presents novel results.",
            "primary_category": "cs.AI",
            "categories": ["cs.AI", "cs.LG"],
            "published": "2025-04-10",
            "url": "https://arxiv.org/abs/2504.12345",
            "pdf_url": "https://arxiv.org/pdf/2504.12345",
        }

    def test_contains_title(self):
        body = ingest_arxiv.format_paper_body(self._make_paper())
        assert "# Test Paper on AI" in body

    def test_contains_authors(self):
        body = ingest_arxiv.format_paper_body(self._make_paper())
        assert "Alice Author" in body

    def test_contains_arxiv_id(self):
        body = ingest_arxiv.format_paper_body(self._make_paper())
        assert "2504.12345" in body

    def test_contains_abstract(self):
        body = ingest_arxiv.format_paper_body(self._make_paper())
        assert "novel results" in body

    def test_contains_pdf_link(self):
        body = ingest_arxiv.format_paper_body(self._make_paper())
        assert "arxiv.org/pdf" in body

    def test_truncates_long_author_list(self):
        paper = self._make_paper()
        paper["authors"] = [f"Author {i}" for i in range(20)]
        body = ingest_arxiv.format_paper_body(paper)
        assert "et al." in body


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

class TestArxivDeduplication:
    @pytest.mark.asyncio
    async def test_skips_already_ingested_paper(self, tmp_brain):
        import brain_io
        brain_io.record_hash("arxiv", "2504.12345")

        mock_session = MagicMock()
        mock_session.get = MagicMock()

        async def mock_fetch(session, query, max_results=25):
            return [{"arxiv_id": "2504.12345", "title": "Already seen", "authors": [],
                      "abstract": "x", "primary_category": "cs.AI", "categories": ["cs.AI"],
                      "published": "2025-04-10", "url": "u", "pdf_url": "p"}]

        with patch.object(ingest_arxiv, "fetch_arxiv_query", new=mock_fetch):
            stats = await ingest_arxiv.ingest_category(
                mock_session, "cs.AI", "A", ["ai"]
            )

        assert stats["papers_new"] == 0
        assert stats["files_written"] == 0

    @pytest.mark.asyncio
    async def test_writes_new_paper(self, tmp_brain):
        mock_session = MagicMock()

        async def mock_fetch(session, query, max_results=25):
            return [{
                "arxiv_id": "2504.99001",
                "title": "Fresh New Paper",
                "authors": ["Jane Doe"],
                "abstract": "A great discovery.",
                "primary_category": "cs.AI",
                "categories": ["cs.AI"],
                "published": "2025-04-10",
                "url": "https://arxiv.org/abs/2504.99001",
                "pdf_url": "https://arxiv.org/pdf/2504.99001",
            }]

        with patch.object(ingest_arxiv, "fetch_arxiv_query", new=mock_fetch):
            stats = await ingest_arxiv.ingest_category(
                mock_session, "cs.AI", "A", ["ai"]
            )

        assert stats["papers_new"] == 1
        assert stats["files_written"] == 1


# ---------------------------------------------------------------------------
# Category ingestion
# ---------------------------------------------------------------------------

class TestIngestCategory:
    @pytest.mark.asyncio
    async def test_dry_run_writes_nothing(self, tmp_brain):
        mock_session = MagicMock()

        async def mock_fetch(session, query, max_results=25):
            return [{
                "arxiv_id": "2504.11111",
                "title": "Dry Run Paper",
                "authors": ["Test Author"],
                "abstract": "Abstract.",
                "primary_category": "cs.AI",
                "categories": ["cs.AI"],
                "published": "2025-04-10",
                "url": "https://arxiv.org/abs/2504.11111",
                "pdf_url": "https://arxiv.org/pdf/2504.11111",
            }]

        with patch.object(ingest_arxiv, "fetch_arxiv_query", new=mock_fetch):
            stats = await ingest_arxiv.ingest_category(
                mock_session, "cs.AI", "A", ["ai"], dry_run=True
            )

        assert stats["papers_new"] == 1
        assert stats["files_written"] == 0
        # No file actually written
        papers_dir = tmp_brain / "raw" / "papers"
        assert not papers_dir.exists() or len(list(papers_dir.rglob("*.md"))) == 0

    @pytest.mark.asyncio
    async def test_stores_in_primary_category_dir(self, tmp_brain):
        mock_session = MagicMock()

        async def mock_fetch(session, query, max_results=25):
            return [{
                "arxiv_id": "2504.22222",
                "title": "LG Paper stored in cs.LG dir",
                "authors": ["Author A"],
                "abstract": "Machine learning.",
                "primary_category": "cs.LG",  # different from query category cs.AI
                "categories": ["cs.AI", "cs.LG"],
                "published": "2025-04-10",
                "url": "https://arxiv.org/abs/2504.22222",
                "pdf_url": "https://arxiv.org/pdf/2504.22222",
            }]

        with patch.object(ingest_arxiv, "fetch_arxiv_query", new=mock_fetch):
            await ingest_arxiv.ingest_category(mock_session, "cs.AI", "A", ["ai"])

        # Should be stored under cs-lg (primary_category)
        papers = list((tmp_brain / "raw" / "papers").rglob("*.md"))
        assert len(papers) == 1
        assert "cs-lg" in str(papers[0])


# ---------------------------------------------------------------------------
# Author watchlist
# ---------------------------------------------------------------------------

class TestIngestAuthor:
    @pytest.mark.asyncio
    async def test_author_paper_gets_watchlist_tag(self, tmp_brain):
        mock_session = MagicMock()

        async def mock_fetch(session, query, max_results=10):
            return [{
                "arxiv_id": "2504.33333",
                "title": "Karpathy Paper",
                "authors": ["Andrej Karpathy"],
                "abstract": "About neural nets.",
                "primary_category": "cs.AI",
                "categories": ["cs.AI"],
                "published": "2025-04-10",
                "url": "https://arxiv.org/abs/2504.33333",
                "pdf_url": "https://arxiv.org/pdf/2504.33333",
            }]

        with patch.object(ingest_arxiv, "fetch_arxiv_query", new=mock_fetch):
            await ingest_arxiv.ingest_author(
                mock_session, "Andrej Karpathy", "S", ["ai"]
            )

        papers = list((tmp_brain / "raw" / "papers").rglob("*.md"))
        assert len(papers) == 1
        content = papers[0].read_text()
        assert "watchlist_author" in content
        assert "Andrej Karpathy" in content


# ---------------------------------------------------------------------------
# ingest_all
# ---------------------------------------------------------------------------

class TestIngestAll:
    @pytest.mark.asyncio
    async def test_returns_counts(self, tmp_brain):
        async def mock_cat(session, cat_id, tier, domains, dry_run=False):
            return {"papers_found": 3, "papers_new": 2, "files_written": 2}

        async def mock_auth(session, name, tier, domains, dry_run=False):
            return {"papers_found": 1, "papers_new": 1, "files_written": 1}

        with patch.object(ingest_arxiv, "ingest_category", new=mock_cat), \
             patch.object(ingest_arxiv, "ingest_author", new=mock_auth), \
             patch("asyncio.sleep", new=AsyncMock()):
            result = await ingest_arxiv.ingest_all()

        assert result["categories_processed"] > 0
        assert result["authors_processed"] > 0

    @pytest.mark.asyncio
    async def test_category_filter(self, tmp_brain):
        called_with = []

        async def mock_cat(session, cat_id, tier, domains, dry_run=False):
            called_with.append(cat_id)
            return {"papers_found": 0, "papers_new": 0, "files_written": 0}

        async def mock_auth(session, name, tier, domains, dry_run=False):
            return {"papers_found": 0, "papers_new": 0, "files_written": 0}

        with patch.object(ingest_arxiv, "ingest_category", new=mock_cat), \
             patch.object(ingest_arxiv, "ingest_author", new=mock_auth), \
             patch("asyncio.sleep", new=AsyncMock()):
            await ingest_arxiv.ingest_all(category_filter="cs.AI")

        assert called_with == ["cs.AI"]

    @pytest.mark.asyncio
    async def test_no_config_returns_zero(self, tmp_brain):
        with patch.object(ingest_arxiv, "load_arxiv_config", return_value={}):
            result = await ingest_arxiv.ingest_all()
        assert result["categories_processed"] == 0
        assert result["authors_processed"] == 0
