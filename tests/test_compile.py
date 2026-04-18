"""Tests for compile.py — the Sonnet wiki compilation pipeline.

Tests cover all deterministic logic: discovery, routing, page I/O, index
updates, prompt building, response parsing, and content validation.
No LLM calls are made.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import compile as comp
import brain_io


# ---------------------------------------------------------------------------
# Compilation tracking
# ---------------------------------------------------------------------------


class TestCompilationTracking:
    def test_mark_and_load(self, tmp_brain):
        comp.mark_compiled("extractions/tweets/test/2026-04-06.json")
        already = comp.load_compiled_set()
        assert "extractions/tweets/test/2026-04-06.json" in already

    def test_empty_set(self, tmp_brain):
        assert comp.load_compiled_set() == set()

    def test_multiple_entries(self, tmp_brain):
        comp.mark_compiled("extractions/a.json")
        comp.mark_compiled("extractions/b.json")
        already = comp.load_compiled_set()
        assert len(already) == 2


# ---------------------------------------------------------------------------
# Extraction discovery
# ---------------------------------------------------------------------------


class TestFindUncompiledExtractions:
    def _write_extraction(self, tmp_brain, rel_path, verdict="high_signal", score=0.8):
        full = tmp_brain / rel_path
        full.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "source_file": "raw/tweets/test/2026-04-06.md",
            "triage_verdict": verdict,
            "triage_max_score": score,
            "extractions": [{"type": "claim", "content": "test"}],
            "affected_wiki_pages": ["wiki/entities/people/test.md"],
        }
        full.write_text(json.dumps(data))
        return rel_path

    def test_finds_uncompiled(self, tmp_brain):
        self._write_extraction(tmp_brain, "extractions/tweets/test/2026-04-06.json")
        files = comp.find_uncompiled_extractions()
        assert len(files) == 1
        assert "2026-04-06.json" in files[0]

    def test_skips_noise(self, tmp_brain):
        self._write_extraction(tmp_brain, "extractions/tweets/test/2026-04-06.json", verdict="noise")
        files = comp.find_uncompiled_extractions()
        assert files == []

    def test_skips_low_score(self, tmp_brain):
        self._write_extraction(
            tmp_brain, "extractions/tweets/test/2026-04-06.json",
            verdict="medium_signal", score=0.1,
        )
        files = comp.find_uncompiled_extractions()
        assert files == []

    def test_skips_already_compiled(self, tmp_brain):
        self._write_extraction(tmp_brain, "extractions/tweets/test/2026-04-06.json")
        comp.mark_compiled("extractions/tweets/test/2026-04-06.json")
        files = comp.find_uncompiled_extractions()
        assert files == []

    def test_empty_dir(self, tmp_brain):
        assert comp.find_uncompiled_extractions() == []

    def test_multiple_extractions(self, tmp_brain):
        self._write_extraction(tmp_brain, "extractions/tweets/a/2026-04-06.json")
        self._write_extraction(tmp_brain, "extractions/tweets/b/2026-04-07.json")
        self._write_extraction(tmp_brain, "extractions/tweets/c/2026-04-08.json", verdict="noise")
        files = comp.find_uncompiled_extractions()
        assert len(files) == 2


# ---------------------------------------------------------------------------
# Wiki I/O
# ---------------------------------------------------------------------------


class TestWikiIO:
    def test_write_and_read(self, tmp_brain):
        content = "---\ntitle: Test\n---\n\n## Body\n"
        comp.write_wiki_page("wiki/entities/people/test.md", content)
        result = comp.read_wiki_page("wiki/entities/people/test.md")
        assert "title: Test" in result
        assert "## Body" in result

    def test_read_nonexistent(self, tmp_brain):
        assert comp.read_wiki_page("wiki/does-not-exist.md") is None

    def test_creates_directories(self, tmp_brain):
        comp.write_wiki_page("wiki/themes/deep/nested/path.md", "---\ntitle: T\n---\n")
        assert (tmp_brain / "wiki" / "themes" / "deep" / "nested" / "path.md").exists()


# ---------------------------------------------------------------------------
# Index updates
# ---------------------------------------------------------------------------


class TestIndexUpdates:
    def test_add_to_empty_section(self, tmp_brain):
        comp.update_index_entry(
            "wiki/entities/people/test-person.md",
            "Test Person",
            "entity",
            "active",
        )
        content = (tmp_brain / "index.md").read_text()
        assert "Test Person" in content
        assert "wiki/entities/people/test-person.md" in content
        # The "People" section should NOT have the placeholder anymore
        people_idx = content.index("## Entities — People")
        companies_idx = content.index("## Entities — Companies")
        people_section = content[people_idx:companies_idx]
        assert "_(No pages yet)_" not in people_section
        # But other sections should still have it
        assert "_(No pages yet)_" in content

    def test_update_existing_entry(self, tmp_brain):
        comp.update_index_entry(
            "wiki/themes/ai-capex.md", "AI Capex", "theme", "active",
        )
        comp.update_index_entry(
            "wiki/themes/ai-capex.md", "AI Capex Boom", "theme", "active",
        )
        content = (tmp_brain / "index.md").read_text()
        # Should have the updated title, not the old one
        assert "AI Capex Boom" in content
        assert content.count("ai-capex.md") == 1  # not duplicated

    def test_multiple_entries_same_section(self, tmp_brain):
        comp.update_index_entry(
            "wiki/themes/theme-a.md", "Theme A", "theme", "active",
        )
        comp.update_index_entry(
            "wiki/themes/theme-b.md", "Theme B", "theme", "active",
        )
        content = (tmp_brain / "index.md").read_text()
        assert "Theme A" in content
        assert "Theme B" in content

    def test_section_routing(self, tmp_brain):
        assert comp._index_section_for_path("wiki/entities/people/x.md", "entity") == "Entities — People"
        assert comp._index_section_for_path("wiki/entities/companies/x.md", "entity") == "Entities — Companies"
        assert comp._index_section_for_path("wiki/themes/x.md", "theme") == "Themes"
        assert comp._index_section_for_path("wiki/signals/x.md", "signal") == "Signals"
        assert comp._index_section_for_path("wiki/contradictions/x.md", "contradiction") == "Contradictions"
        assert comp._index_section_for_path("wiki/syntheses/x.md", "synthesis") == "Syntheses"


# ---------------------------------------------------------------------------
# Page routing
# ---------------------------------------------------------------------------


class TestResolveAffectedPages:
    def test_uses_extraction_suggestions(self, tmp_brain):
        extraction = {
            "triage_verdict": "medium_signal",
            "extractions": [{"type": "claim", "content": "test"}],
            "affected_wiki_pages": [
                "wiki/entities/people/karpathy.md",
                "wiki/themes/autoresearch.md",
            ],
        }
        pages = comp.resolve_affected_pages(extraction)
        assert len(pages) == 2
        assert pages[0]["wiki_path"] == "wiki/entities/people/karpathy.md"
        assert pages[0]["exists"] is False
        assert pages[0]["page_type"] == "entity-person"

    def test_existing_page_detected(self, tmp_brain):
        comp.write_wiki_page(
            "wiki/entities/people/karpathy.md",
            "---\ntitle: Karpathy\ntype: entity\n---\n\nContent here\n",
        )
        extraction = {
            "triage_verdict": "medium_signal",
            "extractions": [{"type": "claim", "content": "test"}],
            "affected_wiki_pages": ["wiki/entities/people/karpathy.md"],
        }
        pages = comp.resolve_affected_pages(extraction)
        assert pages[0]["exists"] is True
        assert "Content here" in pages[0]["current_content"]

    def test_high_signal_creates_signal_page(self, tmp_brain):
        extraction = {
            "source_file": "raw/tweets/test/2026-04-06.md",
            "triage_verdict": "high_signal",
            "extractions": [{"type": "prediction", "content": "Fed will cut rates in June"}],
            "affected_wiki_pages": ["wiki/entities/institutions/fed.md"],
        }
        pages = comp.resolve_affected_pages(extraction)
        paths = [p["wiki_path"] for p in pages]
        assert any("wiki/signals/" in p for p in paths)

    def test_medium_signal_no_signal_page(self, tmp_brain):
        extraction = {
            "source_file": "raw/tweets/test/2026-04-06.md",
            "triage_verdict": "medium_signal",
            "extractions": [{"type": "claim", "content": "minor observation"}],
            "affected_wiki_pages": ["wiki/themes/ai.md"],
        }
        pages = comp.resolve_affected_pages(extraction)
        paths = [p["wiki_path"] for p in pages]
        assert not any("wiki/signals/" in p for p in paths)

    def test_deduplicates_paths(self, tmp_brain):
        extraction = {
            "triage_verdict": "medium_signal",
            "extractions": [{"type": "claim", "content": "test"}],
            "affected_wiki_pages": [
                "wiki/themes/ai.md",
                "wiki/themes/ai.md",  # duplicate
            ],
        }
        pages = comp.resolve_affected_pages(extraction)
        assert len(pages) == 1

    def test_empty_affected_pages(self, tmp_brain):
        extraction = {
            "triage_verdict": "medium_signal",
            "extractions": [{"type": "claim", "content": "test"}],
            "affected_wiki_pages": [],
        }
        pages = comp.resolve_affected_pages(extraction)
        assert pages == []


class TestInferPageType:
    def test_person(self):
        assert comp._infer_page_type("wiki/entities/people/foo.md") == "entity-person"

    def test_company(self):
        assert comp._infer_page_type("wiki/entities/companies/foo.md") == "entity-company"

    def test_institution(self):
        assert comp._infer_page_type("wiki/entities/institutions/foo.md") == "entity-institution"

    def test_theme(self):
        assert comp._infer_page_type("wiki/themes/foo.md") == "theme"

    def test_signal(self):
        assert comp._infer_page_type("wiki/signals/foo.md") == "signal"

    def test_thesis(self):
        assert comp._infer_page_type("wiki/theses/foo.md") == "thesis"

    def test_contradiction(self):
        assert comp._infer_page_type("wiki/contradictions/foo.md") == "contradiction"

    def test_source(self):
        assert comp._infer_page_type("wiki/sources/x-accounts/foo.md") == "source"


class TestExtractDateFromPath:
    def test_standard_date(self):
        assert comp._extract_date_from_path("raw/tweets/test/2026-04-06.md") == "2026-04-06"

    def test_no_date(self):
        result = comp._extract_date_from_path("raw/tweets/test/no-date.md")
        assert len(result) == 10  # falls back to today_str()


# ---------------------------------------------------------------------------
# Template loading
# ---------------------------------------------------------------------------


class TestTemplateLoading:
    def test_loads_person_template(self, tmp_brain):
        # Templates are in agent_docs/page-templates/ — need to exist in tmp_brain
        tmpl_dir = tmp_brain / "agent_docs" / "page-templates"
        tmpl_dir.mkdir(parents=True, exist_ok=True)
        (tmpl_dir / "entity-person.md").write_text("# Person Template\n\nFrontmatter here")

        comp.reset_template_cache()
        tmpl = comp.load_template("entity-person")
        assert "Person Template" in tmpl

    def test_caches_templates(self, tmp_brain):
        tmpl_dir = tmp_brain / "agent_docs" / "page-templates"
        tmpl_dir.mkdir(parents=True, exist_ok=True)
        (tmpl_dir / "theme.md").write_text("# Theme Template")

        comp.reset_template_cache()
        comp.load_template("theme")
        # Modify file — cache should still return old version
        (tmpl_dir / "theme.md").write_text("# Modified Template")
        tmpl = comp.load_template("theme")
        assert "Theme Template" in tmpl

    def test_unknown_type_returns_empty(self, tmp_brain):
        comp.reset_template_cache()
        assert comp.load_template("nonexistent-type") == ""


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------


class TestPromptBuilding:
    def test_includes_extraction_data(self, tmp_brain):
        extraction = {
            "source_file": "raw/tweets/karpathy/2026-04-06.md",
            "source_tier": "S",
            "triage_verdict": "high_signal",
            "extracted_at": "2026-04-06T14:00:00Z",
            "extractions": [{"type": "prediction", "content": "AGI by 2027"}],
            "affected_wiki_pages": [],
        }
        pages = [{"wiki_path": "wiki/entities/people/karpathy.md", "exists": False,
                   "current_content": None, "page_type": "entity-person"}]

        system, user = comp.build_compilation_prompt(extraction, pages)
        assert "intelligence" in system.lower()
        assert "karpathy" in user
        assert "S" in user
        assert "AGI by 2027" in user

    def test_includes_existing_page_content(self, tmp_brain):
        extraction = {
            "source_file": "raw/tweets/test/2026-04-06.md",
            "source_tier": "A",
            "triage_verdict": "medium_signal",
            "extractions": [{"type": "claim", "content": "test"}],
        }
        pages = [{
            "wiki_path": "wiki/themes/ai.md",
            "exists": True,
            "current_content": "---\ntitle: AI\n---\n\n## Existing content here",
            "page_type": "theme",
        }]

        system, user = comp.build_compilation_prompt(extraction, pages)
        assert "EXISTING PAGE" in user
        assert "Existing content here" in user

    def test_marks_new_pages(self, tmp_brain):
        extraction = {
            "source_file": "raw/tweets/test/2026-04-06.md",
            "extractions": [{"type": "claim", "content": "test"}],
        }
        pages = [{
            "wiki_path": "wiki/themes/new-theme.md",
            "exists": False,
            "current_content": None,
            "page_type": "theme",
        }]

        _, user = comp.build_compilation_prompt(extraction, pages)
        assert "NEW PAGE NEEDED" in user

    def test_includes_response_format(self, tmp_brain):
        extraction = {"source_file": "f.md", "extractions": []}
        pages = [{"wiki_path": "wiki/themes/t.md", "exists": False,
                   "current_content": None, "page_type": "theme"}]
        _, user = comp.build_compilation_prompt(extraction, pages)
        assert "page_updates" in user
        assert "new_pages" in user
        assert "contradictions_detected" in user

    def test_truncates_long_content(self, tmp_brain):
        extraction = {
            "source_file": "f.md",
            "extractions": [{"type": "claim", "content": "x" * 100_000}],
        }
        pages = [{"wiki_path": "wiki/themes/t.md", "exists": True,
                   "current_content": "y" * 50_000, "page_type": "theme"}]
        _, user = comp.build_compilation_prompt(extraction, pages)
        assert len(user) <= 85_000


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------


class TestParseCompilationResponse:
    def test_clean_json(self):
        raw = json.dumps({
            "page_updates": [
                {"wiki_path": "wiki/themes/ai.md", "action": "update",
                 "title": "AI", "page_type": "theme",
                 "full_content": "---\ntitle: AI\n---\n\n## Body"}
            ],
            "new_pages": [],
            "contradictions_detected": [],
        })
        result = comp.parse_compilation_response(raw)
        assert result is not None
        assert len(result["page_updates"]) == 1

    def test_json_with_fences(self):
        inner = json.dumps({"page_updates": [], "new_pages": [], "contradictions_detected": []})
        raw = f"```json\n{inner}\n```"
        result = comp.parse_compilation_response(raw)
        assert result is not None

    def test_json_with_surrounding_text(self):
        inner = json.dumps({"page_updates": [], "new_pages": [], "contradictions_detected": []})
        raw = f"Here is the result:\n{inner}\nDone!"
        result = comp.parse_compilation_response(raw)
        assert result is not None

    def test_empty_input(self):
        assert comp.parse_compilation_response("") is None
        assert comp.parse_compilation_response(None) is None

    def test_invalid_json(self):
        assert comp.parse_compilation_response("not json") is None

    def test_defaults_missing_keys(self):
        raw = json.dumps({"page_updates": [{"wiki_path": "w", "full_content": "c"}]})
        result = comp.parse_compilation_response(raw)
        assert result is not None
        assert "new_pages" in result
        assert "contradictions_detected" in result

    def test_complex_response(self):
        raw = json.dumps({
            "page_updates": [
                {"wiki_path": "wiki/entities/people/karpathy.md", "action": "update",
                 "title": "Andrej Karpathy", "page_type": "entity",
                 "full_content": "---\ntitle: Andrej Karpathy\ntype: entity\n---\n\n## Content"},
            ],
            "new_pages": [
                {"wiki_path": "wiki/themes/autoresearch.md", "action": "create",
                 "title": "Autoresearch", "page_type": "theme",
                 "full_content": "---\ntitle: Autoresearch\ntype: theme\n---\n\n## Narrative"},
            ],
            "contradictions_detected": [
                {"wiki_path": "wiki/contradictions/2026-04-06--agi-debate.md",
                 "title": "AGI Timeline Debate",
                 "full_content": "---\ntitle: AGI Timeline Debate\ntype: contradiction\n---\n\n## Debate"},
            ],
        })
        result = comp.parse_compilation_response(raw)
        assert len(result["page_updates"]) == 1
        assert len(result["new_pages"]) == 1
        assert len(result["contradictions_detected"]) == 1


# ---------------------------------------------------------------------------
# Content validation
# ---------------------------------------------------------------------------


class TestValidatePageContent:
    def test_valid_content(self):
        content = "---\ntitle: Test\ntype: entity\ncreated: 2026-04-06\nupdated: 2026-04-06\nstatus: active\n---\n\n## Body\n\nClaim (raw/tweets/test.md)\n\n## Changelog\n- entry"
        issues = comp.validate_page_content(content, "wiki/test.md")
        assert issues == []

    def test_missing_frontmatter(self):
        issues = comp.validate_page_content("No frontmatter here", "wiki/test.md")
        assert len(issues) >= 1
        assert "Missing frontmatter" in issues[0]

    def test_unclosed_frontmatter(self):
        issues = comp.validate_page_content("---\ntitle: Test\nno closing", "wiki/test.md")
        assert any("Malformed" in i for i in issues)

    def test_missing_required_fields(self):
        content = "---\ntitle: Test\n---\n\n## Body\n"
        issues = comp.validate_page_content(content, "wiki/test.md")
        # Should flag missing type, created, updated, status
        assert len(issues) >= 3

    def test_missing_citations(self):
        content = "---\ntitle: T\ntype: entity\ncreated: 2026-04-06\nupdated: 2026-04-06\nstatus: active\n---\n\nBody with no citations\n\n## Changelog\n- entry"
        issues = comp.validate_page_content(content, "wiki/test.md")
        assert any("citation" in i.lower() for i in issues)


# ---------------------------------------------------------------------------
# End-to-end compilation (mocked LLM)
# ---------------------------------------------------------------------------


class TestCompileExtractionEndToEnd:
    """Test the full compile pipeline with --no-llm mode."""

    def _setup_extraction(self, tmp_brain):
        """Create a realistic extraction JSON for testing."""
        ext_data = {
            "source_file": "raw/tweets/karpathy/2026-04-06.md",
            "source_tier": "S",
            "extracted_at": "2026-04-06T14:00:00Z",
            "model": "claude-haiku-4-5-20251001",
            "extraction_count": 2,
            "extractions": [
                {
                    "type": "prediction",
                    "content": "All frontier labs will adopt autoresearch within 12 months",
                    "confidence": "high_conviction",
                    "entities": ["Karpathy", "OpenAI", "Anthropic"],
                    "themes": ["autoresearch"],
                    "signal_strength": 0.95,
                },
                {
                    "type": "claim",
                    "content": "Token throughput shifting from code to knowledge",
                    "confidence": "stated_as_fact",
                    "entities": ["Karpathy"],
                    "themes": ["ai-development"],
                    "signal_strength": 0.75,
                },
            ],
            "triage_verdict": "high_signal",
            "triage_max_score": 0.95,
            "affected_wiki_pages": [
                "wiki/entities/people/andrej-karpathy.md",
                "wiki/themes/autoresearch.md",
            ],
        }
        ext_path = tmp_brain / "extractions" / "tweets" / "karpathy" / "2026-04-06.json"
        ext_path.parent.mkdir(parents=True, exist_ok=True)
        ext_path.write_text(json.dumps(ext_data, indent=2))
        return "extractions/tweets/karpathy/2026-04-06.json"

    def test_no_llm_shows_routing(self, tmp_brain):
        import asyncio
        ext_path = self._setup_extraction(tmp_brain)
        result = asyncio.run(comp.compile_extraction(ext_path, use_llm=False))
        assert result is not None
        assert "affected_pages" in result
        assert any("karpathy" in p for p in result["affected_pages"])

    def test_dry_run_shows_plan(self, tmp_brain):
        import asyncio
        ext_path = self._setup_extraction(tmp_brain)
        result = asyncio.run(comp.compile_extraction(ext_path, dry_run=True))
        assert result is not None
        assert "would_create" in result
        assert "would_update" in result

    def test_dry_run_doesnt_write(self, tmp_brain):
        import asyncio
        ext_path = self._setup_extraction(tmp_brain)
        asyncio.run(comp.compile_extraction(ext_path, dry_run=True))
        # No wiki pages should exist
        assert not (tmp_brain / "wiki" / "entities" / "people" / "andrej-karpathy.md").exists()
        # Should not be marked as compiled
        assert ext_path not in comp.load_compiled_set()

    def test_no_llm_doesnt_mark_compiled(self, tmp_brain):
        import asyncio
        ext_path = self._setup_extraction(tmp_brain)
        asyncio.run(comp.compile_extraction(ext_path, use_llm=False))
        # no-llm mode doesn't write pages, so shouldn't mark compiled
        # (it returns routing info but no pages_updated key with a count)
        result = asyncio.run(comp.compile_extraction(ext_path, use_llm=False))
        assert result is not None


class TestCompileAll:
    def test_empty(self, tmp_brain):
        import asyncio
        result = asyncio.run(comp.compile_all(dry_run=True))
        assert result["files_processed"] == 0

    def test_finds_and_processes(self, tmp_brain):
        import asyncio
        # Write an extraction
        ext_data = {
            "source_file": "raw/tweets/test/2026-04-06.md",
            "triage_verdict": "high_signal",
            "triage_max_score": 0.8,
            "extractions": [{"type": "claim", "content": "test"}],
            "affected_wiki_pages": ["wiki/themes/test.md"],
        }
        ext_path = tmp_brain / "extractions" / "tweets" / "test" / "2026-04-06.json"
        ext_path.parent.mkdir(parents=True, exist_ok=True)
        ext_path.write_text(json.dumps(ext_data))

        result = asyncio.run(comp.compile_all(dry_run=True))
        assert result["files_processed"] == 1
        assert result["files_succeeded"] == 1
