"""Tests for ingest_twitter.py — SocialData API ingester.

No network calls. SocialData responses mocked. Focus areas:
- Tweet formatting (engagement metrics, replies, quotes, URLs)
- Retweet filtering
- Date extraction
- Account ingestion: dedup, grouping by date, append mode
- ingest_all: API key gating, handle filter, batch totals
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import ingest_twitter


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_tweet(
    tweet_id: str = "12345",
    text: str = "Sample tweet text",
    created_at: str = "2026-05-18T14:30:00.000Z",
    screen_name: str = "karpathy",
    likes: int = 100,
    retweets: int = 20,
    replies: int = 5,
    is_rt: bool = False,
    is_reply_to: str | None = None,
    quote: dict | None = None,
) -> dict:
    """Build a SocialData-shaped tweet dict for tests."""
    tweet: dict = {
        "id_str": tweet_id,
        "full_text": text,
        "tweet_created_at": created_at,
        "user": {"screen_name": screen_name},
        "favorite_count": likes,
        "retweet_count": retweets,
        "reply_count": replies,
        "quote_count": 0,
        "views_count": likes * 10,
    }
    if is_rt:
        tweet["retweeted_status"] = {"id_str": "rt-source"}
    if is_reply_to:
        tweet["in_reply_to_screen_name"] = is_reply_to
    if quote:
        tweet["quoted_status"] = quote
        tweet["is_quote_status"] = True
    return tweet


# ---------------------------------------------------------------------------
# Tweet formatting
# ---------------------------------------------------------------------------


class TestFormatTweet:
    def test_contains_tweet_id_and_text(self):
        tweet = _make_tweet(tweet_id="abc", text="Hello world")
        formatted = ingest_twitter.format_tweet(tweet)
        assert "abc" in formatted
        assert "Hello world" in formatted

    def test_contains_screen_name(self):
        tweet = _make_tweet(screen_name="sama")
        formatted = ingest_twitter.format_tweet(tweet)
        assert "@sama" in formatted

    def test_engagement_metrics(self):
        tweet = _make_tweet(likes=1000, retweets=200, replies=50)
        formatted = ingest_twitter.format_tweet(tweet)
        assert "1,000 likes" in formatted
        assert "200 RTs" in formatted
        assert "50 replies" in formatted

    def test_omits_zero_metrics(self):
        tweet = _make_tweet(likes=0, retweets=0, replies=0)
        tweet["views_count"] = 0
        formatted = ingest_twitter.format_tweet(tweet)
        # No engagement line at all when everything is zero
        assert "0 likes" not in formatted

    def test_reply_context(self):
        tweet = _make_tweet(is_reply_to="elonmusk")
        formatted = ingest_twitter.format_tweet(tweet)
        assert "Replying to @elonmusk" in formatted

    def test_quote_tweet_embedded(self):
        quoted = {"user": {"screen_name": "naval"}, "full_text": "wisdom"}
        tweet = _make_tweet(quote=quoted)
        formatted = ingest_twitter.format_tweet(tweet)
        assert "@naval" in formatted
        assert "wisdom" in formatted

    def test_urls_extracted(self):
        tweet = _make_tweet()
        tweet["entities"] = {"urls": [
            {"expanded_url": "https://arxiv.org/abs/2504.12345"},
            {"expanded_url": "https://github.com/test"},
        ]}
        formatted = ingest_twitter.format_tweet(tweet)
        assert "arxiv.org" in formatted
        assert "github.com/test" in formatted


# ---------------------------------------------------------------------------
# Retweet detection and date extraction
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_is_retweet_true(self):
        assert ingest_twitter.is_retweet(_make_tweet(is_rt=True)) is True

    def test_is_retweet_false_for_quote(self):
        # Quote tweet has retweeted_status but is_quote_status=True
        tweet = _make_tweet(quote={"user": {"screen_name": "x"}, "full_text": "y"})
        tweet["retweeted_status"] = {"id_str": "q"}
        assert ingest_twitter.is_retweet(tweet) is False

    def test_is_retweet_false_for_normal(self):
        assert ingest_twitter.is_retweet(_make_tweet()) is False

    def test_tweet_date_extracts_iso(self):
        tweet = _make_tweet(created_at="2026-05-18T14:30:00.000Z")
        assert ingest_twitter.tweet_date(tweet) == "2026-05-18"

    def test_tweet_date_falls_back_to_today(self):
        tweet = _make_tweet(created_at="")
        date = ingest_twitter.tweet_date(tweet)
        assert len(date) == 10  # YYYY-MM-DD

    def test_tweet_date_handles_malformed(self):
        tweet = _make_tweet(created_at="not-a-date")
        date = ingest_twitter.tweet_date(tweet)
        assert len(date) == 10


# ---------------------------------------------------------------------------
# Account ingestion (mocked SocialData)
# ---------------------------------------------------------------------------


class TestIngestAccount:
    @pytest.mark.asyncio
    async def test_dry_run_writes_nothing(self, tmp_brain):
        async def mock_resolve(session, handle):
            return 12345

        async def mock_fetch(session, user_id, cursor=None):
            return [_make_tweet(tweet_id="t1", screen_name="karpathy")], None

        with patch.object(ingest_twitter, "resolve_user_id", new=mock_resolve), \
             patch.object(ingest_twitter, "fetch_user_tweets", new=mock_fetch):
            session = MagicMock()
            stats = await ingest_twitter.ingest_account(
                session, "karpathy", "S", ["ai"], dry_run=True
            )

        assert stats["tweets_new"] == 1
        # Nothing written
        tweets_dir = tmp_brain / "raw" / "tweets" / "karpathy"
        assert not tweets_dir.exists() or len(list(tweets_dir.glob("*.md"))) == 0

    @pytest.mark.asyncio
    async def test_writes_new_tweets(self, tmp_brain):
        async def mock_resolve(session, handle):
            return 999

        async def mock_fetch(session, user_id, cursor=None):
            return [
                _make_tweet(tweet_id="new1", text="First", screen_name="karpathy"),
                _make_tweet(tweet_id="new2", text="Second", screen_name="karpathy"),
            ], None

        with patch.object(ingest_twitter, "resolve_user_id", new=mock_resolve), \
             patch.object(ingest_twitter, "fetch_user_tweets", new=mock_fetch):
            session = MagicMock()
            stats = await ingest_twitter.ingest_account(
                session, "karpathy", "S", ["ai"]
            )

        assert stats["tweets_new"] == 2
        assert stats["files_written"] >= 1
        files = list((tmp_brain / "raw" / "tweets" / "karpathy").glob("*.md"))
        assert len(files) >= 1
        content = files[0].read_text()
        assert "First" in content and "Second" in content

    @pytest.mark.asyncio
    async def test_filters_retweets(self, tmp_brain):
        async def mock_resolve(session, handle):
            return 555

        async def mock_fetch(session, user_id, cursor=None):
            return [
                _make_tweet(tweet_id="real1", text="Original", screen_name="sama"),
                _make_tweet(tweet_id="rt1", text="RT @x: stuff", screen_name="sama", is_rt=True),
            ], None

        with patch.object(ingest_twitter, "resolve_user_id", new=mock_resolve), \
             patch.object(ingest_twitter, "fetch_user_tweets", new=mock_fetch):
            session = MagicMock()
            stats = await ingest_twitter.ingest_account(session, "sama", "S", ["ai"])

        assert stats["tweets_new"] == 1
        assert stats["tweets_skipped"] >= 1

    @pytest.mark.asyncio
    async def test_dedup_via_hash_cache(self, tmp_brain):
        import brain_io
        brain_io.record_hash("twitter", "already-seen")

        async def mock_resolve(session, handle):
            return 1

        async def mock_fetch(session, user_id, cursor=None):
            return [_make_tweet(tweet_id="already-seen", screen_name="x")], None

        with patch.object(ingest_twitter, "resolve_user_id", new=mock_resolve), \
             patch.object(ingest_twitter, "fetch_user_tweets", new=mock_fetch):
            session = MagicMock()
            stats = await ingest_twitter.ingest_account(session, "x", "B", ["ai"])

        assert stats["tweets_new"] == 0
        assert stats["files_written"] == 0

    @pytest.mark.asyncio
    async def test_user_id_lookup_failure(self, tmp_brain):
        async def mock_resolve(session, handle):
            return None

        with patch.object(ingest_twitter, "resolve_user_id", new=mock_resolve):
            session = MagicMock()
            stats = await ingest_twitter.ingest_account(session, "ghost", "A", ["ai"])

        assert stats["tweets_new"] == 0
        assert stats["files_written"] == 0

    @pytest.mark.asyncio
    async def test_groups_by_date(self, tmp_brain):
        async def mock_resolve(session, handle):
            return 100

        async def mock_fetch(session, user_id, cursor=None):
            return [
                _make_tweet(tweet_id="d1a", screen_name="x", created_at="2026-05-15T10:00:00Z"),
                _make_tweet(tweet_id="d1b", screen_name="x", created_at="2026-05-15T16:00:00Z"),
                _make_tweet(tweet_id="d2a", screen_name="x", created_at="2026-05-16T08:00:00Z"),
            ], None

        with patch.object(ingest_twitter, "resolve_user_id", new=mock_resolve), \
             patch.object(ingest_twitter, "fetch_user_tweets", new=mock_fetch):
            session = MagicMock()
            stats = await ingest_twitter.ingest_account(session, "x", "B", ["ai"])

        assert stats["tweets_new"] == 3
        # Two files (one per date)
        files = sorted((tmp_brain / "raw" / "tweets" / "x").glob("*.md"))
        assert len(files) == 2


# ---------------------------------------------------------------------------
# ingest_all
# ---------------------------------------------------------------------------


class TestIngestAll:
    @pytest.mark.asyncio
    async def test_no_api_key_returns_error(self, tmp_brain):
        with patch.dict(os.environ, {}, clear=True):
            result = await ingest_twitter.ingest_all()
        assert result.get("error") == "no_api_key"

    @pytest.mark.asyncio
    async def test_handle_filter_unknown(self, tmp_brain):
        with patch.dict(os.environ, {"SOCIALDATA_API_KEY": "key"}):
            result = await ingest_twitter.ingest_all(handle_filter="nonexistent-handle")
        assert "error" in result
        assert "handle_not_found" in result["error"]

    @pytest.mark.asyncio
    async def test_processes_all_accounts(self, tmp_brain):
        async def mock_ingest_account(session, handle, tier, domains, dry_run=False):
            return {"tweets_fetched": 5, "tweets_new": 3, "tweets_skipped": 2, "files_written": 1}

        with patch.dict(os.environ, {"SOCIALDATA_API_KEY": "key"}):
            with patch.object(ingest_twitter, "ingest_account", new=mock_ingest_account):
                with patch("asyncio.sleep", new=AsyncMock()):
                    result = await ingest_twitter.ingest_all()

        assert result["accounts_processed"] >= 1
        assert result["tweets_new"] >= 3
