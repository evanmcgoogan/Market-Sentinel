"""Tests for extract.py — the Haiku extraction pipeline.

Tests cover all deterministic logic: scoring, triage, frontmatter parsing,
prompt building, response parsing, file discovery, and output format.
No LLM calls are made.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import extract
import brain_io


# ---------------------------------------------------------------------------
# Frontmatter parsing
# ---------------------------------------------------------------------------

class TestParseFrontmatter:
    def test_basic_fields(self, tmp_brain):
        path = brain_io.write_raw_file(
            "raw/tweets/testuser/2026-04-06.md",
            {"source": "twitter", "handle": "testuser", "tier": "S", "tweet_count": 3},
            "Tweet body",
        )
        fm = extract.parse_raw_frontmatter("raw/tweets/testuser/2026-04-06.md")
        assert fm["source"] == "twitter"
        assert fm["handle"] == "testuser"
        assert fm["tier"] == "S"
        assert fm["tweet_count"] == 3

    def test_boolean_fields(self, tmp_brain):
        brain_io.write_raw_file(
            "raw/tweets/test/2026-04-06.md",
            {"source": "twitter", "contains_thread": True},
            "Body",
        )
        fm = extract.parse_raw_frontmatter("raw/tweets/test/2026-04-06.md")
        assert fm["contains_thread"] is True

    def test_list_fields(self, tmp_brain):
        brain_io.write_raw_file(
            "raw/tweets/test/2026-04-06.md",
            {"source": "twitter", "domains": ["ai", "macro"]},
            "Body",
        )
        fm = extract.parse_raw_frontmatter("raw/tweets/test/2026-04-06.md")
        assert fm["domains"] == ["ai", "macro"]

    def test_null_fields(self, tmp_brain):
        brain_io.write_raw_file(
            "raw/tweets/test/2026-04-06.md",
            {"source": "twitter", "score": None},
            "Body",
        )
        fm = extract.parse_raw_frontmatter("raw/tweets/test/2026-04-06.md")
        assert fm["score"] is None

    def test_no_frontmatter(self, tmp_brain):
        p = tmp_brain / "raw" / "tweets" / "test" / "no-fm.md"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("Just a body, no frontmatter.\n")
        fm = extract.parse_raw_frontmatter("raw/tweets/test/no-fm.md")
        assert fm == {}


class TestReadRawBody:
    def test_extracts_body(self, tmp_brain):
        brain_io.write_raw_file(
            "raw/tweets/test/2026-04-06.md",
            {"source": "twitter"},
            "This is the body content.",
        )
        body = extract.read_raw_body("raw/tweets/test/2026-04-06.md")
        assert "This is the body content." in body
        assert "source:" not in body

    def test_no_frontmatter(self, tmp_brain):
        p = tmp_brain / "raw" / "tweets" / "test" / "no-fm.md"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("Just body text.\n")
        body = extract.read_raw_body("raw/tweets/test/no-fm.md")
        assert "Just body text." in body


# ---------------------------------------------------------------------------
# LLM response parsing
# ---------------------------------------------------------------------------

class TestParseLLMResponse:
    def test_clean_json(self):
        raw = '{"extractions": [{"type": "claim", "content": "test"}], "affected_wiki_pages": []}'
        result = extract.parse_llm_response(raw)
        assert result is not None
        assert len(result["extractions"]) == 1

    def test_json_with_markdown_fences(self):
        raw = '```json\n{"extractions": [], "affected_wiki_pages": []}\n```'
        result = extract.parse_llm_response(raw)
        assert result is not None
        assert result["extractions"] == []

    def test_json_with_surrounding_text(self):
        raw = 'Here is the extraction:\n{"extractions": [{"type": "event"}], "affected_wiki_pages": []}\nDone!'
        result = extract.parse_llm_response(raw)
        assert result is not None

    def test_empty_input(self):
        assert extract.parse_llm_response("") is None
        assert extract.parse_llm_response(None) is None

    def test_invalid_json(self):
        assert extract.parse_llm_response("not json at all") is None

    def test_complex_extraction(self):
        raw = json.dumps({
            "extractions": [
                {
                    "type": "prediction",
                    "content": "All frontier labs will adopt autoresearch within 12 months",
                    "confidence": "high_conviction",
                    "entities": ["Karpathy", "OpenAI", "Anthropic"],
                    "themes": ["autoresearch", "ai-research-automation"],
                    "sentiment": "bullish",
                    "temporal": "forward_looking",
                    "falsifiable": True,
                    "actionable": False,
                },
                {
                    "type": "claim",
                    "content": "Token throughput shifting from code to knowledge",
                    "confidence": "stated_as_fact",
                    "entities": ["Karpathy"],
                    "themes": ["ai-development-practices"],
                    "sentiment": "neutral",
                    "temporal": "current",
                    "falsifiable": False,
                    "actionable": False,
                },
            ],
            "affected_wiki_pages": [
                "wiki/entities/people/andrej-karpathy.md",
                "wiki/themes/autoresearch.md",
            ],
        })
        result = extract.parse_llm_response(raw)
        assert result is not None
        assert len(result["extractions"]) == 2
        assert result["extractions"][0]["type"] == "prediction"
        assert len(result["affected_wiki_pages"]) == 2


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

class TestScoreExtraction:
    """Test deterministic scoring using signal-weights.json config."""

    WEIGHTS = {
        "extraction": {
            "claim": {"base_weight": 0.5, "s_tier_multiplier": 1.5, "a_tier_multiplier": 1.2, "b_tier_multiplier": 1.0, "c_tier_multiplier": 0.7},
            "prediction": {"base_weight": 0.9, "s_tier_multiplier": 1.5, "a_tier_multiplier": 1.3, "b_tier_multiplier": 1.0, "c_tier_multiplier": 0.8},
            "data_point": {"base_weight": 0.3, "tier_multiplier_applies": False},
            "event": {"base_weight": 0.7, "s_tier_multiplier": 1.3, "a_tier_multiplier": 1.1, "b_tier_multiplier": 1.0, "c_tier_multiplier": 0.8},
            "opinion": {"base_weight": 0.2, "s_tier_multiplier": 2.0, "a_tier_multiplier": 1.0, "b_tier_multiplier": 0.5, "c_tier_multiplier": 0.3},
        }
    }

    def test_s_tier_prediction_scores_high(self):
        ext = {"type": "prediction", "confidence": "high_conviction", "falsifiable": True,
               "temporal": "forward_looking", "actionable": True}
        score = extract.score_extraction(ext, "S", self.WEIGHTS)
        # 0.9 * 1.5 * 1.15 * 1.1 * 1.1 * 1.05 = ~2.0 → clamped to 1.0
        assert score == 1.0

    def test_c_tier_opinion_scores_low(self):
        ext = {"type": "opinion", "confidence": "speculative", "falsifiable": False,
               "temporal": "current", "actionable": False}
        score = extract.score_extraction(ext, "C", self.WEIGHTS)
        # 0.2 * 0.3 * 0.85 = 0.051
        assert score < 0.1

    def test_s_tier_claim_scores_moderate(self):
        ext = {"type": "claim", "confidence": "stated_as_fact", "falsifiable": False,
               "temporal": "current", "actionable": False}
        score = extract.score_extraction(ext, "S", self.WEIGHTS)
        # 0.5 * 1.5 * 1.0 = 0.75
        assert 0.7 <= score <= 0.8

    def test_data_point_ignores_tier(self):
        ext = {"type": "data_point", "confidence": "stated_as_fact", "falsifiable": False,
               "temporal": "current", "actionable": False}
        score_s = extract.score_extraction(ext, "S", self.WEIGHTS)
        score_c = extract.score_extraction(ext, "C", self.WEIGHTS)
        assert score_s == score_c  # Tier multiplier disabled for data_points

    def test_falsifiable_bonus(self):
        base = {"type": "claim", "confidence": "stated_as_fact", "temporal": "current", "actionable": False}
        not_falsifiable = {**base, "falsifiable": False}
        falsifiable = {**base, "falsifiable": True}
        s1 = extract.score_extraction(not_falsifiable, "B", self.WEIGHTS)
        s2 = extract.score_extraction(falsifiable, "B", self.WEIGHTS)
        assert s2 > s1

    def test_forward_looking_bonus(self):
        base = {"type": "event", "confidence": "stated_as_fact", "falsifiable": False, "actionable": False}
        current = {**base, "temporal": "current"}
        forward = {**base, "temporal": "forward_looking"}
        s1 = extract.score_extraction(current, "A", self.WEIGHTS)
        s2 = extract.score_extraction(forward, "A", self.WEIGHTS)
        assert s2 > s1

    def test_score_clamped_to_0_1(self):
        # Stack all bonuses on a high-base type
        ext = {"type": "prediction", "confidence": "high_conviction", "falsifiable": True,
               "temporal": "forward_looking", "actionable": True}
        score = extract.score_extraction(ext, "S", self.WEIGHTS)
        assert 0.0 <= score <= 1.0


# ---------------------------------------------------------------------------
# Triage
# ---------------------------------------------------------------------------

class TestTriageVerdict:
    WEIGHTS = TestScoreExtraction.WEIGHTS
    THRESHOLDS = {
        "triage": {
            "high_signal_min_score": 0.7,
            "medium_signal_min_score": 0.4,
            "s_tier_always_extract": True,
            "a_tier_always_extract": True,
        }
    }

    def test_high_signal(self):
        exts = [{"type": "prediction", "confidence": "high_conviction",
                 "falsifiable": True, "temporal": "forward_looking", "actionable": True}]
        verdict, score = extract.compute_triage_verdict(exts, "S", self.WEIGHTS, self.THRESHOLDS)
        assert verdict == "high_signal"

    def test_noise_for_c_tier_opinion(self):
        exts = [{"type": "opinion", "confidence": "speculative",
                 "falsifiable": False, "temporal": "current", "actionable": False}]
        verdict, score = extract.compute_triage_verdict(exts, "C", self.WEIGHTS, self.THRESHOLDS)
        assert verdict == "noise"

    def test_s_tier_always_extract(self):
        """S-tier sources get promoted even with moderate scores."""
        exts = [{"type": "claim", "confidence": "medium_conviction",
                 "falsifiable": False, "temporal": "current", "actionable": False}]
        verdict, score = extract.compute_triage_verdict(exts, "S", self.WEIGHTS, self.THRESHOLDS)
        # S-tier always_extract should promote this from noise to at least medium_signal
        assert verdict in ("high_signal", "medium_signal")

    def test_empty_extractions_are_noise(self):
        verdict, score = extract.compute_triage_verdict([], "S", self.WEIGHTS, self.THRESHOLDS)
        assert verdict == "noise"
        assert score == 0.0

    def test_multiple_extractions_uses_max(self):
        """Triage should use the highest-scoring extraction."""
        exts = [
            {"type": "opinion", "confidence": "speculative", "falsifiable": False,
             "temporal": "current", "actionable": False},
            {"type": "prediction", "confidence": "high_conviction", "falsifiable": True,
             "temporal": "forward_looking", "actionable": True},
        ]
        verdict, score = extract.compute_triage_verdict(exts, "A", self.WEIGHTS, self.THRESHOLDS)
        assert verdict == "high_signal"

    def test_scores_written_back_to_extractions(self):
        """compute_triage_verdict should set signal_strength on each extraction."""
        exts = [{"type": "claim", "confidence": "stated_as_fact",
                 "falsifiable": False, "temporal": "current", "actionable": False}]
        extract.compute_triage_verdict(exts, "B", self.WEIGHTS, self.THRESHOLDS)
        assert "signal_strength" in exts[0]
        assert isinstance(exts[0]["signal_strength"], float)


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------

class TestPromptBuilding:
    def test_includes_source_metadata(self):
        system, user = extract.build_extraction_prompt(
            "raw/tweets/karpathy/2026-04-06.md",
            {"source": "twitter", "tier": "S", "domains": ["ai", "frontier-models"]},
            "Tweet content here",
        )
        assert "karpathy" in user
        assert "twitter" in user
        assert "S" in user
        assert "ai" in user
        assert "Tweet content here" in user

    def test_system_prompt_is_analyst_role(self):
        system, user = extract.build_extraction_prompt("f.md", {}, "body")
        assert "intelligence analyst" in system.lower()

    def test_truncates_long_content(self):
        long_body = "word " * 20_000  # 100K chars
        system, user = extract.build_extraction_prompt("f.md", {}, long_body)
        assert "[... truncated" in user
        assert len(user) < 60_000

    def test_default_tier_is_c(self):
        system, user = extract.build_extraction_prompt("f.md", {}, "body")
        assert "Source tier: C" in user


# ---------------------------------------------------------------------------
# File discovery and tracking
# ---------------------------------------------------------------------------

class TestFileDiscovery:
    def test_finds_raw_files(self, tmp_brain):
        brain_io.write_raw_file("raw/tweets/test/2026-04-06.md", {"source": "twitter"}, "Body")
        brain_io.write_raw_file("raw/transcripts/ch/vid1--title.md", {"source": "youtube"}, "Body")
        files = extract.find_unextracted_files()
        assert len(files) == 2

    def test_excludes_already_extracted(self, tmp_brain):
        brain_io.write_raw_file("raw/tweets/test/2026-04-06.md", {"source": "twitter"}, "Body")
        brain_io.write_raw_file("raw/tweets/test/2026-04-07.md", {"source": "twitter"}, "Body")
        extract.mark_extracted("raw/tweets/test/2026-04-06.md")
        files = extract.find_unextracted_files()
        assert len(files) == 1
        assert "2026-04-07" in files[0]

    def test_empty_raw_dir(self, tmp_brain):
        files = extract.find_unextracted_files()
        assert files == []


class TestExtractionOutput:
    def test_saves_json(self, tmp_brain):
        result = {
            "source_file": "raw/tweets/test/2026-04-06.md",
            "extractions": [{"type": "claim", "content": "test"}],
            "triage_verdict": "medium_signal",
        }
        json_path = extract.save_extraction("raw/tweets/test/2026-04-06.md", result)
        assert json_path.exists()
        assert json_path.suffix == ".json"

        loaded = json.loads(json_path.read_text())
        assert loaded["triage_verdict"] == "medium_signal"

    def test_mirrors_raw_structure(self, tmp_brain):
        result = {"source_file": "raw/transcripts/20vc/vid1--title.md", "extractions": []}
        json_path = extract.save_extraction("raw/transcripts/20vc/vid1--title.md", result)
        assert "extractions/transcripts/20vc/" in str(json_path)
        assert json_path.name == "vid1--title.json"

    def test_mark_and_check_extracted(self, tmp_brain):
        extract.mark_extracted("raw/tweets/test/2026-04-06.md")
        already = extract.load_extracted_set()
        assert "raw/tweets/test/2026-04-06.md" in already
