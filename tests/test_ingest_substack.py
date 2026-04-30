"""Tests for ingest_substack.py — newsletter RSS ingestion pipeline.

No network calls. All feed content is injected as fixture strings.
Tests cover: RSS 2.0 parsing, Atom parsing, paywall detection, HTML stripping,
date parsing, dedup, dry-run mode, and full ingest_all flow.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import ingest_substack


# ---------------------------------------------------------------------------
# Feed fixtures
# ---------------------------------------------------------------------------

RSS2_FULL = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/"
                   xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel>
    <title>Not Boring</title>
    <link>https://www.notboring.co</link>
    <item>
      <title>The AI Value Chain</title>
      <link>https://www.notboring.co/p/ai-value-chain</link>
      <guid>https://www.notboring.co/p/ai-value-chain</guid>
      <dc:creator>Packy McCormick</dc:creator>
      <pubDate>Thu, 10 Apr 2025 12:00:00 +0000</pubDate>
      <content:encoded><![CDATA[<p>This is a long essay about the AI value chain.
      It has many paragraphs and covers many topics in depth.
      We cover infrastructure models applications and distribution across the stack.
      The key thesis is that value will accrue to whoever controls the bottleneck.
      Right now that is GPU compute but this is changing rapidly as new entrants emerge.
      We also discuss how the App Layer is becoming increasingly competitive
      as model APIs commoditize and differentiation moves up the stack.
      The real moat is distribution and proprietary data flywheels.
      This analysis provides rich signals for our market intelligence system.
      Vertical integration is accelerating as hyperscalers invest in custom silicon.
      The inference cost curve is collapsing and this changes the economics fundamentally.
      Open source models are pressuring closed model providers on price performance.
      Enterprise AI adoption is accelerating with major workflow automation use cases.
      The regulatory environment remains uncertain but broadly permissive for now.
      Capital allocation to AI infrastructure continues at unprecedented rates globally.</p>]]></content:encoded>
    </item>
    <item>
      <title>Premium Exclusive</title>
      <link>https://www.notboring.co/p/premium</link>
      <guid>https://www.notboring.co/p/premium</guid>
      <dc:creator>Packy McCormick</dc:creator>
      <pubDate>Wed, 09 Apr 2025 12:00:00 +0000</pubDate>
      <content:encoded><![CDATA[<p>Subscribe to read more...</p>]]></content:encoded>
    </item>
  </channel>
</rss>"""

ATOM_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Noahpinion</title>
  <entry>
    <id>https://noahpinion.substack.com/p/ai-jobs</id>
    <title>AI and the Jobs of the Future</title>
    <updated>2025-04-10T12:00:00Z</updated>
    <published>2025-04-10T12:00:00Z</published>
    <author><name>Noah Smith</name></author>
    <link rel="alternate" href="https://noahpinion.substack.com/p/ai-jobs"/>
    <content type="html"><![CDATA[<p>This is an extensive analysis of how AI will
    reshape the labor market over the next decade. I look at historical precedent
    from past technology transitions and what they tell us about adjustment dynamics.
    Key finding: labor market disruption is real but the timeline is contested.
    The most at-risk jobs involve routine cognitive tasks, but creative and
    interpersonal work shows more resilience. Policy responses matter enormously.
    Universal Basic Income proposals deserve more serious analysis than they receive.
    The Nordic model offers some useful lessons for the US context.</p>]]></content>
  </entry>
</feed>"""

MALFORMED_RSS = "not xml at all <<<<"

EMPTY_RSS = """<?xml version="1.0"?><rss version="2.0"><channel></channel></rss>"""


# ---------------------------------------------------------------------------
# HTML stripping
# ---------------------------------------------------------------------------

class TestHtmlToText:
    def test_strips_tags(self):
        result = ingest_substack.html_to_text("<p>Hello <b>world</b></p>")
        assert "<" not in result
        assert "Hello" in result
        assert "world" in result

    def test_preserves_content(self):
        result = ingest_substack.html_to_text("<p>AI is transforming markets</p>")
        assert "AI is transforming markets" in result

    def test_handles_empty_string(self):
        assert ingest_substack.html_to_text("") == ""

    def test_handles_no_tags(self):
        result = ingest_substack.html_to_text("plain text no tags")
        assert result == "plain text no tags"

    def test_paragraph_breaks(self):
        html = "<p>First paragraph</p><p>Second paragraph</p>"
        result = ingest_substack.html_to_text(html)
        assert "First paragraph" in result
        assert "Second paragraph" in result

    def test_cdata_content(self):
        """CDATA content is passed as-is by ElementTree."""
        result = ingest_substack.html_to_text("<p>Important analysis</p>")
        assert "Important analysis" in result


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

class TestParseDate:
    def test_iso_date(self):
        assert ingest_substack._parse_date("2025-04-10T12:00:00Z") == "2025-04-10"

    def test_rfc2822_date(self):
        result = ingest_substack._parse_date("Thu, 10 Apr 2025 12:00:00 +0000")
        assert result == "2025-04-10"

    def test_empty_returns_today(self):
        result = ingest_substack._parse_date("")
        assert len(result) == 10  # YYYY-MM-DD format

    def test_malformed_returns_today(self):
        result = ingest_substack._parse_date("not a date")
        assert len(result) == 10

    def test_all_months_parse(self):
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        for i, mon in enumerate(months, 1):
            result = ingest_substack._parse_date(f"01 {mon} 2025 00:00:00 +0000")
            assert result == f"2025-{i:02d}-01"


# ---------------------------------------------------------------------------
# RSS 2.0 parsing
# ---------------------------------------------------------------------------

class TestParseRss2:
    def test_parses_two_items(self):
        posts = ingest_substack.parse_rss_feed(RSS2_FULL)
        assert len(posts) == 2

    def test_extracts_title(self):
        posts = ingest_substack.parse_rss_feed(RSS2_FULL)
        assert posts[0]["title"] == "The AI Value Chain"

    def test_extracts_url(self):
        posts = ingest_substack.parse_rss_feed(RSS2_FULL)
        assert "notboring.co" in posts[0]["url"]

    def test_extracts_author(self):
        posts = ingest_substack.parse_rss_feed(RSS2_FULL)
        assert posts[0]["author"] == "Packy McCormick"

    def test_extracts_published_date(self):
        posts = ingest_substack.parse_rss_feed(RSS2_FULL)
        assert posts[0]["published"] == "2025-04-10"

    def test_full_post_not_paywalled(self):
        posts = ingest_substack.parse_rss_feed(RSS2_FULL)
        assert posts[0]["paywall"] is False

    def test_teaser_post_is_paywalled(self):
        posts = ingest_substack.parse_rss_feed(RSS2_FULL)
        # Second item has very short content
        assert posts[1]["paywall"] is True

    def test_word_count_populated(self):
        posts = ingest_substack.parse_rss_feed(RSS2_FULL)
        assert posts[0]["word_count"] > 0
        assert posts[1]["word_count"] < ingest_substack.PAYWALL_WORD_THRESHOLD

    def test_empty_feed_returns_empty(self):
        posts = ingest_substack.parse_rss_feed(EMPTY_RSS)
        assert posts == []

    def test_malformed_returns_empty(self):
        posts = ingest_substack.parse_rss_feed(MALFORMED_RSS)
        assert posts == []


# ---------------------------------------------------------------------------
# Atom parsing
# ---------------------------------------------------------------------------

class TestParseAtom:
    def test_parses_atom_entry(self):
        posts = ingest_substack.parse_rss_feed(ATOM_FEED)
        assert len(posts) == 1

    def test_atom_title(self):
        posts = ingest_substack.parse_rss_feed(ATOM_FEED)
        assert posts[0]["title"] == "AI and the Jobs of the Future"

    def test_atom_author(self):
        posts = ingest_substack.parse_rss_feed(ATOM_FEED)
        assert posts[0]["author"] == "Noah Smith"

    def test_atom_published(self):
        posts = ingest_substack.parse_rss_feed(ATOM_FEED)
        assert posts[0]["published"] == "2025-04-10"

    def test_atom_url(self):
        posts = ingest_substack.parse_rss_feed(ATOM_FEED)
        assert "noahpinion.substack.com" in posts[0]["url"]

    def test_atom_content_extracted(self):
        posts = ingest_substack.parse_rss_feed(ATOM_FEED)
        assert posts[0]["word_count"] > 0
        assert "labor market" in posts[0]["body_text"]


# ---------------------------------------------------------------------------
# Body formatting
# ---------------------------------------------------------------------------

class TestFormatArticleBody:
    def _make_post(self, paywall: bool = False, word_count: int = 300) -> dict:
        return {
            "title": "Test Newsletter Post",
            "author": "Test Author",
            "published": "2025-04-10",
            "url": "https://example.substack.com/p/test",
            "paywall": paywall,
            "body_text": "word " * word_count,
            "word_count": word_count,
        }

    def test_contains_title(self):
        body = ingest_substack.format_article_body(self._make_post(), "Test Newsletter")
        assert "# Test Newsletter Post" in body

    def test_contains_publication(self):
        body = ingest_substack.format_article_body(self._make_post(), "Not Boring")
        assert "Not Boring" in body

    def test_paywall_post_has_warning(self):
        body = ingest_substack.format_article_body(self._make_post(paywall=True), "The Diff")
        assert "Paywalled" in body or "paywalled" in body

    def test_full_post_no_paywall_warning(self):
        body = ingest_substack.format_article_body(self._make_post(paywall=False), "Not Boring")
        assert "Paywalled" not in body

    def test_very_long_body_truncated(self):
        post = self._make_post(word_count=ingest_substack.MAX_BODY_WORDS + 500)
        body = ingest_substack.format_article_body(post, "Long Newsletter")
        assert "Truncated" in body

    def test_normal_length_not_truncated(self):
        post = self._make_post(word_count=500)
        body = ingest_substack.format_article_body(post, "Normal Newsletter")
        assert "Truncated" not in body


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

class TestSubstackDeduplication:
    @pytest.mark.asyncio
    async def test_skips_already_ingested_post(self, tmp_brain):
        import brain_io
        guid = "https://www.notboring.co/p/already-seen"
        brain_io.record_hash("substack", guid)

        async def mock_fetch(session, rss_url):
            return [{
                "guid": guid,
                "title": "Already Seen Post",
                "url": guid,
                "published": "2025-04-10",
                "author": "Packy",
                "body_text": "content " * 200,
                "word_count": 200,
                "paywall": False,
            }]

        mock_session = MagicMock()
        with patch.object(ingest_substack, "fetch_rss", new=mock_fetch):
            stats = await ingest_substack.ingest_newsletter(
                mock_session, "Not Boring", "Packy McCormick",
                "https://fake.rss", "A", ["ai"]
            )

        assert stats["posts_new"] == 0

    @pytest.mark.asyncio
    async def test_writes_new_post(self, tmp_brain):
        async def mock_fetch(session, rss_url):
            return [{
                "guid": "https://www.notboring.co/p/brand-new",
                "title": "Brand New Post",
                "url": "https://www.notboring.co/p/brand-new",
                "published": "2025-04-10",
                "author": "Packy McCormick",
                "body_text": "This is a long enough post " * 20,
                "word_count": 160,
                "paywall": False,
            }]

        mock_session = MagicMock()
        with patch.object(ingest_substack, "fetch_rss", new=mock_fetch):
            stats = await ingest_substack.ingest_newsletter(
                mock_session, "Not Boring", "Packy McCormick",
                "https://fake.rss", "A", ["ai"]
            )

        assert stats["posts_new"] == 1
        assert stats["files_written"] == 1


# ---------------------------------------------------------------------------
# ingest_newsletter
# ---------------------------------------------------------------------------

class TestIngestNewsletter:
    @pytest.mark.asyncio
    async def test_dry_run_no_files(self, tmp_brain):
        async def mock_fetch(session, rss_url):
            return [{
                "guid": "https://example.com/p/test",
                "title": "Some Post",
                "url": "https://example.com/p/test",
                "published": "2025-04-10",
                "author": "Author",
                "body_text": "content " * 200,
                "word_count": 200,
                "paywall": False,
            }]

        mock_session = MagicMock()
        with patch.object(ingest_substack, "fetch_rss", new=mock_fetch):
            stats = await ingest_substack.ingest_newsletter(
                mock_session, "Test NL", "Author", "https://fake.rss",
                "B", ["ai"], dry_run=True
            )

        assert stats["posts_new"] == 1
        assert stats["files_written"] == 0
        articles = list((tmp_brain / "raw" / "articles").rglob("*.md")) if (tmp_brain / "raw" / "articles").exists() else []
        assert len(articles) == 0

    @pytest.mark.asyncio
    async def test_file_path_uses_publication_slug(self, tmp_brain):
        async def mock_fetch(session, rss_url):
            return [{
                "guid": "https://notboring.co/p/new",
                "title": "New Essay",
                "url": "https://notboring.co/p/new",
                "published": "2025-04-10",
                "author": "Packy",
                "body_text": "content " * 200,
                "word_count": 200,
                "paywall": False,
            }]

        mock_session = MagicMock()
        with patch.object(ingest_substack, "fetch_rss", new=mock_fetch):
            await ingest_substack.ingest_newsletter(
                mock_session, "Not Boring", "Packy McCormick",
                "https://fake.rss", "A", ["ai"]
            )

        articles = list((tmp_brain / "raw" / "articles").rglob("*.md"))
        assert len(articles) == 1
        assert "not-boring" in str(articles[0])

    @pytest.mark.asyncio
    async def test_paywall_flagged_in_frontmatter(self, tmp_brain):
        async def mock_fetch(session, rss_url):
            return [{
                "guid": "https://thediff.co/p/paywalled",
                "title": "Paywalled Post",
                "url": "https://thediff.co/p/paywalled",
                "published": "2025-04-10",
                "author": "Byrne Hobart",
                "body_text": "Subscribe to read more...",
                "word_count": 5,
                "paywall": True,
            }]

        mock_session = MagicMock()
        with patch.object(ingest_substack, "fetch_rss", new=mock_fetch):
            await ingest_substack.ingest_newsletter(
                mock_session, "The Diff", "Byrne Hobart",
                "https://fake.rss", "A", ["finance"]
            )

        articles = list((tmp_brain / "raw" / "articles").rglob("*.md"))
        assert len(articles) == 1
        content = articles[0].read_text()
        assert "paywall: true" in content


# ---------------------------------------------------------------------------
# ingest_all
# ---------------------------------------------------------------------------

class TestIngestAll:
    @pytest.mark.asyncio
    async def test_processes_all_newsletters(self, tmp_brain):
        call_log: list[str] = []

        async def mock_ingest(session, name, author, rss_url, tier, domains, dry_run=False):
            call_log.append(name)
            return {"posts_found": 2, "posts_new": 1, "files_written": 1}

        with patch.object(ingest_substack, "ingest_newsletter", new=mock_ingest), \
             patch("asyncio.sleep", new=AsyncMock()):
            result = await ingest_substack.ingest_all()

        assert result["newsletters_processed"] > 0
        assert len(call_log) > 0

    @pytest.mark.asyncio
    async def test_name_filter_limits_to_one(self, tmp_brain):
        call_log: list[str] = []

        async def mock_ingest(session, name, author, rss_url, tier, domains, dry_run=False):
            call_log.append(name)
            return {"posts_found": 1, "posts_new": 1, "files_written": 1}

        with patch.object(ingest_substack, "ingest_newsletter", new=mock_ingest), \
             patch("asyncio.sleep", new=AsyncMock()):
            result = await ingest_substack.ingest_all(name_filter="Not Boring")

        assert len(call_log) == 1
        assert call_log[0] == "Not Boring"

    @pytest.mark.asyncio
    async def test_unknown_name_returns_error(self, tmp_brain):
        with patch("asyncio.sleep", new=AsyncMock()):
            result = await ingest_substack.ingest_all(name_filter="Nonexistent Newsletter")
        assert "error" in result

    @pytest.mark.asyncio
    async def test_no_config_returns_zero(self, tmp_brain):
        with patch.object(ingest_substack, "load_substack_config", return_value={}):
            result = await ingest_substack.ingest_all()
        assert result["newsletters_processed"] == 0
