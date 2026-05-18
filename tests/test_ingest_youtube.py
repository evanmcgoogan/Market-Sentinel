"""Tests for ingest_youtube.py — focused on the new YouTube Data API path.

No network calls. Data API responses are mocked via async context managers.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import ingest_youtube


# ---------------------------------------------------------------------------
# Helper: build an async context manager that mimics aiohttp.ClientSession.get
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, status: int, json_data: dict | None = None, text_data: str = ""):
        self.status = status
        self._json = json_data or {}
        self._text = text_data

    async def json(self):
        return self._json

    async def text(self):
        return self._text


class _AsyncCM:
    """Mimics `async with session.get(...) as resp:` semantics."""
    def __init__(self, resp: _FakeResponse):
        self.resp = resp

    async def __aenter__(self):
        return self.resp

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _make_session(resp: _FakeResponse) -> MagicMock:
    sess = MagicMock()
    sess.get = MagicMock(return_value=_AsyncCM(resp))
    return sess


# ---------------------------------------------------------------------------
# uploads playlist conversion
# ---------------------------------------------------------------------------


class TestUploadsPlaylistConversion:
    def test_uc_prefix_swap(self):
        assert ingest_youtube._channel_id_to_uploads_playlist("UCabcdef") == "UUabcdef"

    def test_lex_fridman(self):
        assert ingest_youtube._channel_id_to_uploads_playlist("UCSHZKyawb77ixDdsGog4iWA") == "UUSHZKyawb77ixDdsGog4iWA"

    def test_already_playlist_pass_through(self):
        assert ingest_youtube._channel_id_to_uploads_playlist("PLabc") == "PLabc"

    def test_short_input(self):
        assert ingest_youtube._channel_id_to_uploads_playlist("UC") == "UC"


# ---------------------------------------------------------------------------
# Data API path
# ---------------------------------------------------------------------------


SAMPLE_API_RESPONSE = {
    "items": [
        {
            "snippet": {
                "title": "Lex Fridman talks to Andrej Karpathy about LLMs",
                "publishedAt": "2026-05-15T14:30:00Z",
                "resourceId": {"videoId": "abc123XYZ"},
            }
        },
        {
            "snippet": {
                "title": "Lex Fridman: How to disagree with people",
                "publishedAt": "2026-05-12T10:00:00Z",
                "resourceId": {"videoId": "def456UVW"},
            }
        },
    ]
}


class TestDataAPIFetch:
    @pytest.mark.asyncio
    async def test_returns_videos_on_200(self):
        resp = _FakeResponse(200, json_data=SAMPLE_API_RESPONSE)
        session = _make_session(resp)
        videos = await ingest_youtube.fetch_channel_feed_data_api(session, "UCxxx", "fake-key")
        assert len(videos) == 2
        assert videos[0]["video_id"] == "abc123XYZ"
        assert videos[0]["title"].startswith("Lex Fridman")
        assert videos[0]["published"] == "2026-05-15"

    @pytest.mark.asyncio
    async def test_returns_empty_on_404(self):
        resp = _FakeResponse(404, text_data='{"error":"not found"}')
        session = _make_session(resp)
        videos = await ingest_youtube.fetch_channel_feed_data_api(session, "UCxxx", "key")
        assert videos == []

    @pytest.mark.asyncio
    async def test_skips_private_videos(self):
        resp = _FakeResponse(200, json_data={
            "items": [
                {"snippet": {"title": "Private video", "publishedAt": "2026-05-15T14:30:00Z",
                              "resourceId": {"videoId": "priv1"}}},
                {"snippet": {"title": "Real video", "publishedAt": "2026-05-15T14:30:00Z",
                              "resourceId": {"videoId": "real1"}}},
            ]
        })
        session = _make_session(resp)
        videos = await ingest_youtube.fetch_channel_feed_data_api(session, "UCxxx", "key")
        assert len(videos) == 1
        assert videos[0]["video_id"] == "real1"

    @pytest.mark.asyncio
    async def test_skips_missing_video_id(self):
        resp = _FakeResponse(200, json_data={
            "items": [
                {"snippet": {"title": "x", "publishedAt": "2026-05-15T14:30:00Z",
                              "resourceId": {}}},  # missing videoId
                {"snippet": {"title": "Real", "publishedAt": "2026-05-15T14:30:00Z",
                              "resourceId": {"videoId": "real2"}}},
            ]
        })
        session = _make_session(resp)
        videos = await ingest_youtube.fetch_channel_feed_data_api(session, "UCxxx", "key")
        assert len(videos) == 1


# ---------------------------------------------------------------------------
# Discovery routing — Data API when key present, RSS fallback otherwise
# ---------------------------------------------------------------------------


class TestDiscoveryRouting:
    @pytest.mark.asyncio
    async def test_uses_data_api_when_key_set(self):
        async def mock_data_api(session, cid, key):
            return [{"video_id": "v1", "title": "t", "published": "2026-05-15", "link": "l"}]

        async def mock_rss(session, cid):
            return [{"video_id": "rss_v", "title": "RSS", "published": "2026-05-15", "link": "l"}]

        with patch.dict(os.environ, {"YOUTUBE_API_KEY": "my-key"}):
            with patch.object(ingest_youtube, "fetch_channel_feed_data_api", new=mock_data_api):
                with patch.object(ingest_youtube, "fetch_channel_feed_rss", new=mock_rss):
                    videos = await ingest_youtube.fetch_channel_feed(MagicMock(), "UCx")

        assert videos[0]["video_id"] == "v1"  # Data API result, not RSS

    @pytest.mark.asyncio
    async def test_falls_back_to_rss_when_no_key(self):
        async def mock_rss(session, cid):
            return [{"video_id": "rss_v", "title": "RSS", "published": "2026-05-15", "link": "l"}]

        with patch.dict(os.environ, {}, clear=True):
            with patch.object(ingest_youtube, "fetch_channel_feed_rss", new=mock_rss):
                videos = await ingest_youtube.fetch_channel_feed(MagicMock(), "UCx")

        assert videos[0]["video_id"] == "rss_v"

    @pytest.mark.asyncio
    async def test_falls_back_to_rss_when_data_api_returns_empty(self):
        async def mock_data_api(session, cid, key):
            return []  # Data API failed/empty

        async def mock_rss(session, cid):
            return [{"video_id": "rss_fallback", "title": "RSS", "published": "2026-05-15", "link": "l"}]

        with patch.dict(os.environ, {"YOUTUBE_API_KEY": "my-key"}):
            with patch.object(ingest_youtube, "fetch_channel_feed_data_api", new=mock_data_api):
                with patch.object(ingest_youtube, "fetch_channel_feed_rss", new=mock_rss):
                    videos = await ingest_youtube.fetch_channel_feed(MagicMock(), "UCx")

        assert videos[0]["video_id"] == "rss_fallback"

    @pytest.mark.asyncio
    async def test_empty_key_treated_as_unset(self):
        async def mock_rss(session, cid):
            return [{"video_id": "rss_only", "title": "RSS", "published": "2026-05-15", "link": "l"}]

        with patch.dict(os.environ, {"YOUTUBE_API_KEY": "   "}):  # whitespace only
            with patch.object(ingest_youtube, "fetch_channel_feed_rss", new=mock_rss):
                videos = await ingest_youtube.fetch_channel_feed(MagicMock(), "UCx")

        assert videos[0]["video_id"] == "rss_only"
