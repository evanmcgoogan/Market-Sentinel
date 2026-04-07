"""Tests for ingestion scripts — pure parsing/formatting logic only (no network)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import ingest_twitter
import ingest_youtube
import ingest_markets


# ---------------------------------------------------------------------------
# Twitter ingestion tests
# ---------------------------------------------------------------------------


class TestTweetFormatting:
    SAMPLE_TWEET = {
        "id_str": "1234567890",
        "full_text": "Token throughput shifting from code to knowledge manipulation.",
        "tweet_created_at": "2026-04-06T14:30:00.000000Z",
        "user": {"screen_name": "karpathy"},
        "favorite_count": 5200,
        "retweet_count": 890,
        "reply_count": 134,
        "quote_count": 67,
        "views_count": 450000,
        "in_reply_to_screen_name": None,
        "quoted_status": None,
        "retweeted_status": None,
        "is_quote_status": False,
        "entities": {"urls": [], "hashtags": [], "user_mentions": [], "symbols": []},
    }

    def test_basic_format(self):
        result = ingest_twitter.format_tweet(self.SAMPLE_TWEET)
        assert "### Tweet 1234567890" in result
        assert "@karpathy" in result
        assert "Token throughput" in result

    def test_engagement_metrics(self):
        result = ingest_twitter.format_tweet(self.SAMPLE_TWEET)
        assert "450,000 views" in result
        assert "5,200 likes" in result
        assert "890 RTs" in result

    def test_reply_context(self):
        tweet = {**self.SAMPLE_TWEET, "in_reply_to_screen_name": "elonmusk"}
        result = ingest_twitter.format_tweet(tweet)
        assert "Replying to @elonmusk" in result

    def test_quote_tweet(self):
        quoted = {
            "user": {"screen_name": "sama"},
            "full_text": "AGI is near",
        }
        tweet = {**self.SAMPLE_TWEET, "quoted_status": quoted}
        result = ingest_twitter.format_tweet(tweet)
        assert "@sama" in result
        assert "AGI is near" in result

    def test_urls(self):
        entities = {
            "urls": [{"expanded_url": "https://example.com/article"}],
            "hashtags": [], "user_mentions": [], "symbols": [],
        }
        tweet = {**self.SAMPLE_TWEET, "entities": entities}
        result = ingest_twitter.format_tweet(tweet)
        assert "https://example.com/article" in result
        assert "**Links:**" in result


class TestRetweetDetection:
    def test_retweet_detected(self):
        tweet = {"retweeted_status": {"id": 1}, "is_quote_status": False}
        assert ingest_twitter.is_retweet(tweet)

    def test_quote_tweet_not_retweet(self):
        tweet = {"retweeted_status": {"id": 1}, "is_quote_status": True}
        assert not ingest_twitter.is_retweet(tweet)

    def test_original_tweet_not_retweet(self):
        tweet = {"retweeted_status": None, "is_quote_status": False}
        assert not ingest_twitter.is_retweet(tweet)


class TestThreadDetection:
    def test_self_reply_is_thread(self):
        tweets = [
            {"user": {"screen_name": "karpathy"}, "in_reply_to_screen_name": None},
            {"user": {"screen_name": "karpathy"}, "in_reply_to_screen_name": "karpathy"},
        ]
        assert ingest_twitter._detect_thread(tweets)

    def test_reply_to_others_not_thread(self):
        tweets = [
            {"user": {"screen_name": "karpathy"}, "in_reply_to_screen_name": None},
            {"user": {"screen_name": "karpathy"}, "in_reply_to_screen_name": "elonmusk"},
        ]
        assert not ingest_twitter._detect_thread(tweets)

    def test_single_tweet_not_thread(self):
        tweets = [{"user": {"screen_name": "karpathy"}, "in_reply_to_screen_name": None}]
        assert not ingest_twitter._detect_thread(tweets)


class TestTweetDate:
    def test_parses_iso_date(self):
        tweet = {"tweet_created_at": "2026-04-06T14:30:00.000000Z"}
        assert ingest_twitter.tweet_date(tweet) == "2026-04-06"

    def test_handles_missing(self):
        tweet = {"tweet_created_at": ""}
        result = ingest_twitter.tweet_date(tweet)
        # Should return today's date, not crash
        assert len(result) == 10  # YYYY-MM-DD format


# ---------------------------------------------------------------------------
# YouTube ingestion tests
# ---------------------------------------------------------------------------


class TestAtomFeedParsing:
    SAMPLE_FEED = """<?xml version="1.0" encoding="UTF-8"?>
    <feed xmlns:yt="http://www.youtube.com/xml/schemas/2015"
          xmlns:media="http://search.yahoo.com/mrss/"
          xmlns="http://www.w3.org/2005/Atom">
      <title>Test Channel</title>
      <entry>
        <yt:videoId>abc123def</yt:videoId>
        <title>The Future of AI Infrastructure</title>
        <published>2026-04-05T10:00:00+00:00</published>
        <link rel="alternate" href="https://www.youtube.com/watch?v=abc123def"/>
      </entry>
      <entry>
        <yt:videoId>xyz789ghi</yt:videoId>
        <title>Why Semiconductors Matter</title>
        <published>2026-04-04T08:00:00+00:00</published>
        <link rel="alternate" href="https://www.youtube.com/watch?v=xyz789ghi"/>
      </entry>
    </feed>"""

    def test_parses_videos(self):
        videos = ingest_youtube.parse_atom_feed(self.SAMPLE_FEED)
        assert len(videos) == 2

    def test_extracts_video_id(self):
        videos = ingest_youtube.parse_atom_feed(self.SAMPLE_FEED)
        assert videos[0]["video_id"] == "abc123def"

    def test_extracts_title(self):
        videos = ingest_youtube.parse_atom_feed(self.SAMPLE_FEED)
        assert videos[0]["title"] == "The Future of AI Infrastructure"

    def test_extracts_date(self):
        videos = ingest_youtube.parse_atom_feed(self.SAMPLE_FEED)
        assert videos[0]["published"] == "2026-04-05"

    def test_extracts_link(self):
        videos = ingest_youtube.parse_atom_feed(self.SAMPLE_FEED)
        assert "youtube.com" in videos[0]["link"]

    def test_handles_invalid_xml(self):
        videos = ingest_youtube.parse_atom_feed("not xml at all")
        assert videos == []

    def test_handles_empty_feed(self):
        empty = '<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"></feed>'
        videos = ingest_youtube.parse_atom_feed(empty)
        assert videos == []


class TestDurationEstimate:
    def test_short_video(self):
        # 300 words ~ 2 minutes
        text = " ".join(["word"] * 300)
        assert ingest_youtube.estimate_duration(text) == 2

    def test_long_video(self):
        # 15000 words ~ 100 minutes
        text = " ".join(["word"] * 15000)
        assert ingest_youtube.estimate_duration(text) == 100

    def test_minimum_one_minute(self):
        assert ingest_youtube.estimate_duration("short") == 1


class TestTranscriptFormatting:
    def test_includes_title(self):
        result = ingest_youtube.format_transcript_body("Test Title", "Some transcript text.", "vid123")
        assert "# Test Title" in result

    def test_includes_link(self):
        result = ingest_youtube.format_transcript_body("Title", "Text.", "vid123")
        assert "https://www.youtube.com/watch?v=vid123" in result

    def test_includes_transcript(self):
        result = ingest_youtube.format_transcript_body("Title", "The actual transcript content here.", "vid123")
        assert "actual transcript content" in result


# ---------------------------------------------------------------------------
# Market ingestion tests
# ---------------------------------------------------------------------------


class TestPolymarketFiltering:
    def test_filters_signal_events(self):
        events = [
            {"title": "Will the Fed cut rates?", "tags": [{"label": "economics"}], "categories": []},
            {"title": "Super Bowl winner", "tags": [{"label": "sports"}], "categories": []},
            {"title": "AI regulation bill", "tags": [], "categories": [{"label": "technology"}]},
        ]
        filtered = ingest_markets.filter_signal_events(events)
        titles = [e["title"] for e in filtered]
        assert "Will the Fed cut rates?" in titles
        assert "AI regulation bill" in titles
        assert "Super Bowl winner" not in titles

    def test_keyword_fallback(self):
        events = [
            {"title": "Trump wins election", "tags": [], "categories": []},
            {"title": "Bitcoin to 100k", "tags": [], "categories": []},
        ]
        filtered = ingest_markets.filter_signal_events(events)
        assert len(filtered) == 2

    def test_empty_input(self):
        assert ingest_markets.filter_signal_events([]) == []


class TestPolymarketFormatting:
    def test_formats_event(self):
        event = {
            "title": "Fed Rate Decision",
            "volume": 5000000,
            "volume24hr": 250000,
            "liquidity": 1200000,
            "markets": [
                {
                    "question": "Will the Fed cut rates in June 2026?",
                    "outcomePrices": '[0.65, 0.35]',
                    "volume24hr": 125000,
                    "liquidity": 600000,
                },
            ],
        }
        result = ingest_markets.format_polymarket_event(event)
        assert "Fed Rate Decision" in result
        assert "65%" in result
        assert "$5.0M" in result

    def test_handles_missing_prices(self):
        event = {
            "title": "Test",
            "volume": 0,
            "volume24hr": 0,
            "liquidity": 0,
            "markets": [{"question": "Test?", "outcomePrices": "[]", "volume24hr": 0, "liquidity": 0}],
        }
        # Should not crash
        result = ingest_markets.format_polymarket_event(event)
        assert "Test" in result


class TestCompactNum:
    def test_billions(self):
        assert ingest_markets._compact_num(1_500_000_000) == "1.5B"

    def test_millions(self):
        assert ingest_markets._compact_num(5_000_000) == "5.0M"

    def test_thousands(self):
        assert ingest_markets._compact_num(45_000) == "45K"

    def test_small(self):
        assert ingest_markets._compact_num(999) == "999"

    def test_zero(self):
        assert ingest_markets._compact_num(0) == "0"

    def test_none(self):
        assert ingest_markets._compact_num(None) == "0"


class TestPriceFormatting:
    def test_formats_table(self):
        prices = {
            "SPY": {"price": 520.45, "prev_close": 518.20, "change": 2.25, "change_pct": 0.43},
            "BTC": {"price": 98500.00, "prev_close": 97000.00, "change": 1500.00, "change_pct": 1.55},
        }
        result = ingest_markets.format_price_table(prices)
        assert "| Ticker |" in result
        assert "BTC" in result
        assert "SPY" in result
        assert "$520.45" in result

    def test_negative_change(self):
        prices = {
            "VIX": {"price": 18.50, "prev_close": 20.00, "change": -1.50, "change_pct": -7.50},
        }
        result = ingest_markets.format_price_table(prices)
        assert "-1.50" in result
        assert "-7.50%" in result


# ---------------------------------------------------------------------------
# End-to-end file writing (uses tmp_brain fixture)
# ---------------------------------------------------------------------------


class TestEndToEndFileWriting:
    def test_twitter_file_matches_schema(self, tmp_brain):
        """Verify a written Twitter raw file has correct frontmatter fields."""
        import brain_io

        frontmatter = {
            "source": "twitter",
            "handle": "testuser",
            "tier": "S",
            "domains": ["ai", "macro"],
            "collected_at": brain_io.utcnow(),
            "tweet_count": 2,
            "contains_thread": False,
        }
        body = "### Tweet 123\n**@testuser** — 2026-04-06\n\nTest tweet content"
        path = brain_io.write_raw_file("raw/tweets/testuser/2026-04-06.md", frontmatter, body)

        content = path.read_text()
        assert "source: twitter" in content
        assert "handle: testuser" in content
        assert "tier: S" in content
        assert "tweet_count: 2" in content
        assert "contains_thread: false" in content
        assert "Test tweet content" in content

    def test_youtube_file_matches_schema(self, tmp_brain):
        """Verify a written YouTube raw file has correct frontmatter fields."""
        import brain_io

        frontmatter = {
            "source": "youtube",
            "channel": "Test Channel",
            "channel_slug": "test-channel",
            "tier": "S",
            "domains": ["ai", "technology"],
            "video_id": "abc123",
            "title": "The Future of AI",
            "published_at": "2026-04-05",
            "duration_minutes": 97,
            "collected_at": brain_io.utcnow(),
            "transcript_method": "youtube-transcript-api",
        }
        body = "# The Future of AI\n\nTranscript content here."
        path = brain_io.write_raw_file(
            "raw/transcripts/test-channel/abc123--the-future-of-ai.md",
            frontmatter,
            body,
        )

        content = path.read_text()
        assert "source: youtube" in content
        assert "video_id: abc123" in content
        assert "duration_minutes: 97" in content
        assert "transcript_method: youtube-transcript-api" in content

    def test_market_file_matches_schema(self, tmp_brain):
        """Verify a written market snapshot has correct frontmatter fields."""
        import brain_io

        frontmatter = {
            "source": "polymarket",
            "collected_at": brain_io.utcnow(),
            "event_count": 15,
            "total_events_fetched": 100,
        }
        body = "### Fed Rate Decision\n\n| Market | Price |\n|--------|-------|\n| Cut | 65% |"
        path = brain_io.write_raw_file(
            "raw/markets/polymarket/2026-04-06-snapshot.md",
            frontmatter,
            body,
        )

        content = path.read_text()
        assert "source: polymarket" in content
        assert "event_count: 15" in content

    def test_dedup_prevents_double_write(self, tmp_brain):
        """Verify dedup works end-to-end with file writes."""
        import brain_io

        brain_io.record_hash("twitter", "already_seen")
        assert brain_io.is_duplicate("twitter", "already_seen")

        # A second ingestion of the same tweet ID should be caught
        assert brain_io.is_duplicate("twitter", "already_seen")
        # But a different ID should not
        assert not brain_io.is_duplicate("twitter", "new_tweet")

    def test_log_records_operations(self, tmp_brain):
        """Verify log.md captures operations."""
        import brain_io

        brain_io.append_log("INGEST_TWITTER @testuser | fetched: 5 | new: 3 | files: 1")
        brain_io.append_log("INGEST_YOUTUBE Test Channel | found: 2 | new: 1")

        log = (tmp_brain / "log.md").read_text()
        assert "INGEST_TWITTER" in log
        assert "INGEST_YOUTUBE" in log
        # Should have timestamps
        assert log.count("[202") >= 2
