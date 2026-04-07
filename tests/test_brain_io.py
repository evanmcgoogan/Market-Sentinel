"""Tests for brain_io — the shared I/O layer."""

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import brain_io


class TestFormatFrontmatter:
    def test_basic_fields(self):
        result = brain_io.format_frontmatter({"source": "twitter", "count": 3})
        assert result.startswith("---")
        assert result.endswith("---")
        assert "source: twitter" in result
        assert "count: 3" in result

    def test_string_with_special_chars(self):
        result = brain_io.format_frontmatter({"title": "Fed: Rate Cut?"})
        assert '"Fed: Rate Cut?"' in result

    def test_boolean(self):
        result = brain_io.format_frontmatter({"active": True, "stale": False})
        assert "active: true" in result
        assert "stale: false" in result

    def test_none(self):
        result = brain_io.format_frontmatter({"score": None})
        assert "score: null" in result

    def test_list_inline(self):
        result = brain_io.format_frontmatter({"domains": ["ai", "macro"]})
        assert "domains: [ai, macro]" in result

    def test_empty_list(self):
        result = brain_io.format_frontmatter({"tags": []})
        assert "tags: []" in result

    def test_datetime(self):
        dt = datetime(2026, 4, 6, 14, 30, 0, tzinfo=timezone.utc)
        result = brain_io.format_frontmatter({"collected_at": dt})
        assert "2026-04-06T14:30:00" in result


class TestSlugify:
    def test_basic(self):
        assert brain_io.slugify("Hello World") == "hello-world"

    def test_special_chars(self):
        assert brain_io.slugify("Fed: Rate Cut? Yes!") == "fed-rate-cut-yes"

    def test_collapse_hyphens(self):
        assert brain_io.slugify("a --- b") == "a-b"

    def test_truncation(self):
        long_text = "this is a very long title that should be truncated at some reasonable point"
        result = brain_io.slugify(long_text, max_length=30)
        assert len(result) <= 30

    def test_empty(self):
        assert brain_io.slugify("") == ""

    def test_already_slug(self):
        assert brain_io.slugify("already-a-slug") == "already-a-slug"


class TestDeduplication:
    def test_not_duplicate_on_fresh_brain(self, tmp_brain):
        assert not brain_io.is_duplicate("twitter", "123456")

    def test_record_then_detect(self, tmp_brain):
        brain_io.record_hash("twitter", "123456", "some content")
        assert brain_io.is_duplicate("twitter", "123456")

    def test_different_ids_not_duplicate(self, tmp_brain):
        brain_io.record_hash("twitter", "111")
        assert not brain_io.is_duplicate("twitter", "222")

    def test_different_source_types_not_duplicate(self, tmp_brain):
        brain_io.record_hash("twitter", "123")
        assert not brain_io.is_duplicate("youtube", "123")

    def test_persists_to_file(self, tmp_brain):
        brain_io.record_hash("twitter", "persist_test")
        hashes_content = (tmp_brain / "raw" / ".hashes").read_text()
        assert "twitter:persist_test:" in hashes_content

    def test_reset_cache_forces_reload(self, tmp_brain):
        brain_io.record_hash("twitter", "cache_test")
        brain_io.reset_hash_cache()
        brain_io._HASHES_PATH = None
        # After reset, should still find it by reading file
        assert brain_io.is_duplicate("twitter", "cache_test")


class TestWriteRawFile:
    def test_creates_file(self, tmp_brain):
        path = brain_io.write_raw_file(
            "raw/tweets/testuser/2026-04-06.md",
            {"source": "twitter", "handle": "testuser"},
            "Tweet content here",
        )
        assert path.exists()
        content = path.read_text()
        assert content.startswith("---")
        assert "source: twitter" in content
        assert "Tweet content here" in content

    def test_creates_parent_dirs(self, tmp_brain):
        path = brain_io.write_raw_file(
            "raw/tweets/newuser/2026-04-06.md",
            {"source": "twitter"},
            "Content",
        )
        assert path.exists()
        assert path.parent.name == "newuser"

    def test_frontmatter_body_separation(self, tmp_brain):
        path = brain_io.write_raw_file(
            "raw/articles/test.md",
            {"source": "article"},
            "Body text",
        )
        content = path.read_text()
        parts = content.split("---")
        # Should have: empty before first ---, frontmatter, body after second ---
        assert len(parts) >= 3


class TestAppendLog:
    def test_appends_to_log(self, tmp_brain):
        brain_io.append_log("TEST operation completed")
        log_content = (tmp_brain / "log.md").read_text()
        assert "TEST operation completed" in log_content

    def test_includes_timestamp(self, tmp_brain):
        brain_io.append_log("INGEST test")
        log_content = (tmp_brain / "log.md").read_text()
        # Should have ISO timestamp in brackets
        assert "[202" in log_content
        assert "Z]" in log_content

    def test_multiple_appends(self, tmp_brain):
        brain_io.append_log("First op")
        brain_io.append_log("Second op")
        log_content = (tmp_brain / "log.md").read_text()
        assert "First op" in log_content
        assert "Second op" in log_content


class TestLoadSourcesConfig:
    def test_loads_yaml(self, tmp_brain):
        config = brain_io.load_sources_config()
        assert "twitter" in config
        assert "youtube" in config
        assert "markets" in config

    def test_twitter_accounts(self, tmp_brain):
        config = brain_io.load_sources_config()
        accounts = config["twitter"]["accounts"]
        assert len(accounts) == 1
        assert accounts[0]["handle"] == "testuser"
        assert accounts[0]["tier"] == "S"
