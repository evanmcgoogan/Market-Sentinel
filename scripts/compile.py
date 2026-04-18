"""
compile.py — Sonnet-powered wiki compilation pipeline.

Reads extraction JSONs that passed triage, determines which wiki pages to
create or update, calls Claude Sonnet to generate incremental page updates,
and writes the results. This is where knowledge compounds.

The LLM decides *what to write*; the deterministic layer handles discovery,
routing, I/O, index maintenance, and compilation tracking.

Usage:
    python scripts/compile.py                           # Compile all pending extractions
    python scripts/compile.py --file extractions/tweets/karpathy/2026-04-06.json
    python scripts/compile.py --dry-run                 # Show what would be compiled
    python scripts/compile.py --no-llm                  # Validate routing only (no Sonnet calls)

Requires ANTHROPIC_API_KEY environment variable.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from brain_io import append_log, brain_root, slugify, today_str, utcnow

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 8192


# ---------------------------------------------------------------------------
# Compilation tracking
# ---------------------------------------------------------------------------

def _compiled_registry_path() -> Path:
    return brain_root() / "extractions" / ".compiled"


def load_compiled_set() -> set[str]:
    """Load set of already-compiled extraction file paths."""
    path = _compiled_registry_path()
    if not path.exists():
        return set()
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }


def mark_compiled(extraction_rel_path: str) -> None:
    """Record that an extraction has been compiled into wiki pages."""
    path = _compiled_registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(extraction_rel_path + "\n")


# ---------------------------------------------------------------------------
# Extraction discovery
# ---------------------------------------------------------------------------

def find_uncompiled_extractions() -> list[str]:
    """Find extraction JSONs that haven't been compiled yet.

    Only returns extractions with triage_verdict != "noise".
    Returns paths relative to brain root.
    """
    already = load_compiled_set()
    ext_dir = brain_root() / "extractions"
    if not ext_dir.exists():
        return []

    uncompiled = []
    for json_file in sorted(ext_dir.rglob("*.json")):
        rel = str(json_file.relative_to(brain_root()))
        if rel in already:
            continue

        # Quick-check: skip noise verdicts without full parse
        try:
            with open(json_file) as f:
                data = json.load(f)
            verdict = data.get("triage_verdict", "noise")
            if verdict == "noise":
                # Mark as compiled so we don't re-check every run
                mark_compiled(rel)
                continue
            # Check min signal strength
            max_score = data.get("triage_max_score", 0.0)
            if max_score < 0.3:
                mark_compiled(rel)
                continue
        except (json.JSONDecodeError, OSError):
            logger.warning("Skipping malformed extraction: %s", rel)
            continue

        uncompiled.append(rel)

    return uncompiled


def load_extraction(rel_path: str) -> dict[str, Any] | None:
    """Load an extraction JSON by its path relative to brain root."""
    full = brain_root() / rel_path
    if not full.exists():
        return None
    try:
        with open(full) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Failed to load extraction %s: %s", rel_path, e)
        return None


# ---------------------------------------------------------------------------
# Wiki I/O
# ---------------------------------------------------------------------------

def read_wiki_page(wiki_path: str) -> str | None:
    """Read a wiki page's full content. Returns None if page doesn't exist."""
    full = brain_root() / wiki_path
    if not full.exists():
        return None
    return full.read_text(encoding="utf-8")


def write_wiki_page(wiki_path: str, content: str) -> Path:
    """Write a wiki page (create or overwrite)."""
    full = brain_root() / wiki_path
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_text(content.rstrip() + "\n", encoding="utf-8")
    logger.info("Wrote wiki page: %s", wiki_path)
    return full


def read_index() -> str:
    """Read the current index.md content."""
    path = brain_root() / "index.md"
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def update_index_entry(
    wiki_path: str,
    title: str,
    page_type: str,
    status: str,
) -> None:
    """Add or update a page entry in index.md.

    Finds the correct section based on page_type and wiki_path,
    then adds or updates the entry line.
    """
    index_path = brain_root() / "index.md"
    content = index_path.read_text(encoding="utf-8")

    entry_line = f"- [{title}]({wiki_path}) | {page_type} | {status} | {today_str()}"

    # Determine which section this entry belongs to
    section = _index_section_for_path(wiki_path, page_type)

    # Check if this path already has an entry (update it)
    # Match any line containing the wiki_path
    pattern = re.compile(rf"^- \[.*?\]\({re.escape(wiki_path)}\).*$", re.MULTILINE)
    if pattern.search(content):
        content = pattern.sub(entry_line, content)
    else:
        # Add new entry under the right section, replacing "_(No pages yet)_" if present
        section_pattern = re.compile(
            rf"(## {re.escape(section)}\n)\n_\(No pages yet\)_",
            re.MULTILINE,
        )
        match = section_pattern.search(content)
        if match:
            content = content[:match.start()] + match.group(1) + "\n" + entry_line + content[match.end():]
        else:
            # Section exists with entries — append after last entry in section
            # Find section header, then find the next section header or EOF
            sec_match = re.search(rf"## {re.escape(section)}\n", content)
            if sec_match:
                # Find next section header
                next_sec = re.search(r"\n## ", content[sec_match.end():])
                if next_sec:
                    insert_pos = sec_match.end() + next_sec.start()
                else:
                    insert_pos = len(content)
                # Insert before the blank line preceding next section
                content = content[:insert_pos].rstrip() + "\n" + entry_line + "\n" + content[insert_pos:]
            else:
                # Fallback: append to end
                content = content.rstrip() + "\n\n" + entry_line + "\n"

    index_path.write_text(content, encoding="utf-8")


def _index_section_for_path(wiki_path: str, page_type: str) -> str:
    """Map a wiki page path to its index.md section header."""
    if "entities/people" in wiki_path:
        return "Entities — People"
    elif "entities/companies" in wiki_path:
        return "Entities — Companies"
    elif "entities/institutions" in wiki_path:
        return "Entities — Institutions"
    elif "syntheses/" in wiki_path:
        return "Syntheses"
    elif "themes/" in wiki_path:
        return "Themes"
    elif "signals/" in wiki_path:
        return "Signals"
    elif "theses/" in wiki_path:
        return "Theses"
    elif "sources/x-accounts" in wiki_path:
        return "Sources — X Accounts"
    elif "sources/youtube-channels" in wiki_path:
        return "Sources — YouTube Channels"
    elif "contradictions/" in wiki_path:
        return "Contradictions"
    # Fallback based on type
    return {
        "entity": "Entities — People",
        "theme": "Themes",
        "signal": "Signals",
        "thesis": "Theses",
        "contradiction": "Contradictions",
        "synthesis": "Syntheses",
    }.get(page_type, "Signals")


# ---------------------------------------------------------------------------
# Page routing — map extraction entities/themes to wiki paths
# ---------------------------------------------------------------------------

def resolve_affected_pages(extraction: dict[str, Any]) -> list[dict[str, Any]]:
    """Determine which wiki pages an extraction affects.

    Returns a list of dicts with:
      - wiki_path: str (the expected wiki path)
      - exists: bool (whether the page currently exists)
      - current_content: str | None
      - page_type: str (entity, theme, signal, etc.)
    """
    affected = []
    seen_paths = set()

    # 1. Use extraction's affected_wiki_pages (Haiku already suggested these)
    for suggested_path in extraction.get("affected_wiki_pages", []):
        if suggested_path in seen_paths:
            continue
        seen_paths.add(suggested_path)

        content = read_wiki_page(suggested_path)
        page_type = _infer_page_type(suggested_path)
        affected.append({
            "wiki_path": suggested_path,
            "exists": content is not None,
            "current_content": content,
            "page_type": page_type,
        })

    # 2. Always create a signal page for high_signal extractions
    if extraction.get("triage_verdict") == "high_signal":
        source_file = extraction.get("source_file", "")
        source_date = _extract_date_from_path(source_file)
        # Build a slug from the first extraction's content
        first_ext = (extraction.get("extractions") or [{}])[0]
        slug = slugify(first_ext.get("content", "signal")[:60])
        signal_path = f"wiki/signals/{source_date}--{slug}.md"

        if signal_path not in seen_paths:
            seen_paths.add(signal_path)
            content = read_wiki_page(signal_path)
            affected.append({
                "wiki_path": signal_path,
                "exists": content is not None,
                "current_content": content,
                "page_type": "signal",
            })

    return affected


def _infer_page_type(wiki_path: str) -> str:
    """Infer the page type from its wiki path."""
    if "entities/people" in wiki_path:
        return "entity-person"
    elif "entities/companies" in wiki_path:
        return "entity-company"
    elif "entities/institutions" in wiki_path:
        return "entity-institution"
    elif "themes/" in wiki_path:
        return "theme"
    elif "signals/" in wiki_path:
        return "signal"
    elif "theses/" in wiki_path:
        return "thesis"
    elif "contradictions/" in wiki_path:
        return "contradiction"
    elif "sources/" in wiki_path:
        return "source"
    elif "syntheses/" in wiki_path:
        return "synthesis"
    return "signal"  # safe default


def _extract_date_from_path(source_file: str) -> str:
    """Extract a date from a raw file path, or return today."""
    match = re.search(r"(\d{4}-\d{2}-\d{2})", source_file)
    if match:
        return match.group(1)
    return today_str()


# ---------------------------------------------------------------------------
# Template loading
# ---------------------------------------------------------------------------

_template_cache: dict[str, str] = {}


def load_template(page_type: str) -> str:
    """Load a page template from agent_docs/page-templates/."""
    if page_type in _template_cache:
        return _template_cache[page_type]

    template_map = {
        "entity-person": "entity-person.md",
        "entity-company": "entity-company.md",
        "entity-institution": "entity-institution.md",
        "theme": "theme.md",
        "signal": "signal.md",
        "thesis": "thesis.md",
        "contradiction": "contradiction.md",
        "source": "source-profile.md",
        "synthesis": "synthesis.md",
    }

    filename = template_map.get(page_type)
    if not filename:
        return ""

    path = brain_root() / "agent_docs" / "page-templates" / filename
    if not path.exists():
        return ""

    content = path.read_text(encoding="utf-8")
    _template_cache[page_type] = content
    return content


def reset_template_cache() -> None:
    """Clear template cache (for testing)."""
    _template_cache.clear()


# ---------------------------------------------------------------------------
# Compilation prompt
# ---------------------------------------------------------------------------

COMPILATION_SYSTEM = """You are a wiki compiler for a market intelligence knowledge base. Your job: take extraction data from raw sources and produce precise, citation-rich wiki page updates.

RULES — follow exactly:
1. Every new claim MUST cite its raw source: (raw/path/to/source.md)
2. Compilation is ADDITIVE. Never delete existing content. Add new information to the appropriate sections.
3. Preserve ALL existing content, frontmatter fields, and citations.
4. Increment update_count by 1 in frontmatter.
5. Update the "updated" date in frontmatter.
6. Update last_mentioned_in to point to the new raw source.
7. Add [[wikilinks]] for cross-references.
8. If new information CONTRADICTS existing claims, do NOT overwrite. Instead, include the contradiction in your response (see output format).
9. For new pages, use the provided template structure. Fill in ALL frontmatter fields.
10. Keep content factual and analytical. No filler, no hedging language.
11. Append to the Changelog section at the bottom of each page.

Use markdown formatting. Frontmatter must be valid YAML between --- delimiters."""


def build_compilation_prompt(
    extraction: dict[str, Any],
    affected_pages: list[dict[str, Any]],
) -> tuple[str, str]:
    """Build system and user prompts for Sonnet compilation.

    Returns (system_prompt, user_prompt).
    """
    # Collect unique templates needed
    needed_types = {p["page_type"] for p in affected_pages}
    template_sections = []
    for pt in sorted(needed_types):
        tmpl = load_template(pt)
        if tmpl:
            template_sections.append(f"### Template: {pt}\n\n{tmpl}")

    system = COMPILATION_SYSTEM
    if template_sections:
        system += "\n\n## PAGE TEMPLATES\n\n" + "\n\n".join(template_sections)

    # Build user prompt
    user_parts = []

    # Extraction data
    user_parts.append("## EXTRACTION DATA\n")
    user_parts.append(f"Source file: {extraction.get('source_file', 'unknown')}")
    user_parts.append(f"Source tier: {extraction.get('source_tier', 'C')}")
    user_parts.append(f"Triage verdict: {extraction.get('triage_verdict', 'unknown')}")
    user_parts.append(f"Extracted at: {extraction.get('extracted_at', 'unknown')}")
    user_parts.append(f"\nExtractions ({len(extraction.get('extractions', []))} items):")
    user_parts.append("```json")
    user_parts.append(json.dumps(extraction.get("extractions", []), indent=2))
    user_parts.append("```")

    # Affected pages
    user_parts.append("\n## AFFECTED WIKI PAGES\n")
    for page in affected_pages:
        path = page["wiki_path"]
        if page["exists"]:
            user_parts.append(f"### EXISTING PAGE: {path}")
            user_parts.append(f"(Update this page with new information from the extraction)\n")
            user_parts.append(page["current_content"])
            user_parts.append("")
        else:
            user_parts.append(f"### NEW PAGE NEEDED: {path}")
            user_parts.append(f"Page type: {page['page_type']}")
            user_parts.append("(Create this page using the template above. Fill all frontmatter fields.)\n")

    # Response format
    user_parts.append(RESPONSE_FORMAT_INSTRUCTION)

    user = "\n".join(user_parts)

    # Truncate if extremely long
    if len(user) > 80_000:
        user = user[:80_000] + "\n\n[... truncated for compilation ...]"

    return system, user


RESPONSE_FORMAT_INSTRUCTION = """
## RESPONSE FORMAT

Return a JSON object with this exact structure. Each page's full_content is the complete markdown file (frontmatter + body). Return ONLY the JSON, no commentary.

```
{
  "page_updates": [
    {
      "wiki_path": "wiki/entities/people/person-slug.md",
      "action": "update",
      "title": "Page Title",
      "page_type": "entity",
      "full_content": "---\\ntitle: ...\\n---\\n\\n## Section..."
    }
  ],
  "new_pages": [
    {
      "wiki_path": "wiki/themes/theme-slug.md",
      "action": "create",
      "title": "Page Title",
      "page_type": "theme",
      "full_content": "---\\ntitle: ...\\n---\\n\\n## Section..."
    }
  ],
  "contradictions_detected": [
    {
      "wiki_path": "wiki/contradictions/YYYY-MM-DD--slug.md",
      "title": "Contradiction Description",
      "full_content": "---\\ntitle: ...\\n---\\n\\n## The Disagreement..."
    }
  ]
}
```

Rules for the response:
- page_updates: pages that already exist and you're adding information to
- new_pages: pages that don't exist yet and need to be created
- contradictions_detected: only if new information directly contradicts existing wiki claims
- full_content must include complete valid YAML frontmatter between --- delimiters
- Every factual claim must cite (raw/path/to/source.md)
- If no pages need updating (extraction adds nothing new), return {"page_updates": [], "new_pages": [], "contradictions_detected": []}"""


# ---------------------------------------------------------------------------
# Sonnet interaction
# ---------------------------------------------------------------------------

async def call_sonnet(system: str, user: str) -> str | None:
    """Call Claude Sonnet and return the text response."""
    try:
        import anthropic
    except ImportError:
        logger.error("anthropic package not installed — run: pip install anthropic")
        return None

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY environment variable not set")
        return None

    client = anthropic.Anthropic(api_key=api_key)

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=[
                {
                    "type": "text",
                    "text": system,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user}],
        )
        return response.content[0].text
    except Exception as e:
        logger.error("Sonnet API error: %s", e)
        return None


def parse_compilation_response(raw_text: str) -> dict[str, Any] | None:
    """Parse Sonnet's JSON response, handling common formatting issues."""
    if not raw_text:
        return None

    text = raw_text.strip()

    # Strip markdown fences
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)

    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON object
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                result = json.loads(match.group())
            except json.JSONDecodeError:
                logger.error("Could not parse compilation response as JSON")
                return None
        else:
            logger.error("No JSON found in compilation response")
            return None

    # Validate structure
    if not isinstance(result, dict):
        return None
    if "page_updates" not in result and "new_pages" not in result:
        # Try wrapping if it looks like a single page
        return None

    result.setdefault("page_updates", [])
    result.setdefault("new_pages", [])
    result.setdefault("contradictions_detected", [])

    return result


# ---------------------------------------------------------------------------
# Content validation
# ---------------------------------------------------------------------------

def validate_page_content(content: str, wiki_path: str) -> list[str]:
    """Check that page content meets quality requirements.

    Returns list of issues (empty = valid).
    """
    issues = []

    # Must have frontmatter
    if not content.strip().startswith("---"):
        issues.append(f"{wiki_path}: Missing frontmatter (must start with ---)")
        return issues

    fm_match = re.match(r"^---\n(.*?)\n---", content, re.DOTALL)
    if not fm_match:
        issues.append(f"{wiki_path}: Malformed frontmatter (no closing ---)")
        return issues

    fm_text = fm_match.group(1)

    # Check required universal fields
    for field in ("title", "type", "created", "updated", "status"):
        if f"{field}:" not in fm_text:
            issues.append(f"{wiki_path}: Missing required field '{field}' in frontmatter")

    # Check for raw/ citations in body (skip for empty new pages)
    body = content[fm_match.end():]
    if body.strip() and "raw/" not in body and "Changelog" in body:
        issues.append(f"{wiki_path}: No raw/ citations found in body")

    return issues


# ---------------------------------------------------------------------------
# Core compilation pipeline
# ---------------------------------------------------------------------------

async def compile_extraction(
    extraction_path: str,
    use_llm: bool = True,
    dry_run: bool = False,
) -> dict[str, Any] | None:
    """Compile a single extraction into wiki page updates.

    Returns a summary dict, or None on failure.
    """
    logger.info("Compiling: %s", extraction_path)

    extraction = load_extraction(extraction_path)
    if extraction is None:
        logger.error("Could not load extraction: %s", extraction_path)
        return None

    # Determine affected pages
    affected = resolve_affected_pages(extraction)
    if not affected:
        logger.info("No affected pages for %s — skipping", extraction_path)
        mark_compiled(extraction_path)
        return {"extraction": extraction_path, "pages_updated": 0, "pages_created": 0}

    if dry_run:
        existing = [p["wiki_path"] for p in affected if p["exists"]]
        new = [p["wiki_path"] for p in affected if not p["exists"]]
        logger.info(
            "[DRY RUN] %s → update %d pages %s, create %d pages %s",
            extraction_path, len(existing), existing, len(new), new,
        )
        return {
            "extraction": extraction_path,
            "would_update": existing,
            "would_create": new,
        }

    if not use_llm:
        logger.info("[NO-LLM] Routing only for %s: %d affected pages", extraction_path, len(affected))
        return {
            "extraction": extraction_path,
            "affected_pages": [p["wiki_path"] for p in affected],
        }

    # Build prompt and call Sonnet
    system, user = build_compilation_prompt(extraction, affected)
    raw_response = await call_sonnet(system, user)
    result = parse_compilation_response(raw_response)

    if result is None:
        logger.error("Sonnet compilation failed for %s", extraction_path)
        append_log(f"COMPILE FAILED {extraction_path} | reason: Sonnet returned no parseable result")
        return None

    # Apply updates
    pages_updated = 0
    pages_created = 0
    contradictions_created = 0
    all_issues = []

    source_file = extraction.get("source_file", extraction_path)

    for page in result.get("page_updates", []):
        wiki_path = page.get("wiki_path", "")
        content = page.get("full_content", "")
        title = page.get("title", "Untitled")
        page_type = page.get("page_type", _infer_page_type(wiki_path))

        if not wiki_path or not content:
            continue

        issues = validate_page_content(content, wiki_path)
        if issues:
            all_issues.extend(issues)
            logger.warning("Validation issues in %s: %s", wiki_path, issues)
            # Write anyway — issues are warnings, not blockers
        write_wiki_page(wiki_path, content)
        update_index_entry(wiki_path, title, page_type, "active")
        pages_updated += 1

    for page in result.get("new_pages", []):
        wiki_path = page.get("wiki_path", "")
        content = page.get("full_content", "")
        title = page.get("title", "Untitled")
        page_type = page.get("page_type", _infer_page_type(wiki_path))

        if not wiki_path or not content:
            continue

        issues = validate_page_content(content, wiki_path)
        if issues:
            all_issues.extend(issues)
            logger.warning("Validation issues in new page %s: %s", wiki_path, issues)

        write_wiki_page(wiki_path, content)
        update_index_entry(wiki_path, title, page_type, "active")
        pages_created += 1

    for contra in result.get("contradictions_detected", []):
        wiki_path = contra.get("wiki_path", "")
        content = contra.get("full_content", "")
        title = contra.get("title", "Untitled Contradiction")

        if not wiki_path or not content:
            continue

        write_wiki_page(wiki_path, content)
        update_index_entry(wiki_path, title, "contradiction", "active")
        contradictions_created += 1

    # Track compilation
    mark_compiled(extraction_path)

    log_parts = [
        f"COMPILED from {source_file}",
        f"updated: {pages_updated}",
        f"created: {pages_created}",
    ]
    if contradictions_created:
        log_parts.append(f"contradictions: {contradictions_created}")
    if all_issues:
        log_parts.append(f"warnings: {len(all_issues)}")
    append_log(" | ".join(log_parts))

    logger.info(
        "Compiled %s: updated=%d, created=%d, contradictions=%d",
        extraction_path, pages_updated, pages_created, contradictions_created,
    )

    return {
        "extraction": extraction_path,
        "pages_updated": pages_updated,
        "pages_created": pages_created,
        "contradictions_created": contradictions_created,
        "validation_issues": all_issues,
    }


async def compile_all(
    file_filter: str | None = None,
    use_llm: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Compile all pending extractions (or a single file)."""
    if file_filter:
        files = [file_filter]
    else:
        files = find_uncompiled_extractions()

    if not files:
        logger.info("No uncompiled extractions found")
        return {"files_processed": 0}

    logger.info("Found %d extractions to compile", len(files))

    totals = {
        "files_processed": 0,
        "files_succeeded": 0,
        "files_failed": 0,
        "pages_updated": 0,
        "pages_created": 0,
        "contradictions_created": 0,
    }

    for filepath in files:
        result = await compile_extraction(filepath, use_llm=use_llm, dry_run=dry_run)
        totals["files_processed"] += 1

        if result and "pages_updated" in result:
            totals["files_succeeded"] += 1
            totals["pages_updated"] += result.get("pages_updated", 0)
            totals["pages_created"] += result.get("pages_created", 0)
            totals["contradictions_created"] += result.get("contradictions_created", 0)
        elif result:
            # dry_run or no-llm result
            totals["files_succeeded"] += 1
        else:
            totals["files_failed"] += 1

        # Rate limit between Sonnet calls
        if use_llm and not dry_run:
            await asyncio.sleep(1.0)

    if not dry_run:
        append_log(
            f"COMPILE BATCH complete | "
            f"files: {totals['files_processed']} | "
            f"succeeded: {totals['files_succeeded']} | "
            f"pages updated: {totals['pages_updated']} | "
            f"pages created: {totals['pages_created']} | "
            f"contradictions: {totals['contradictions_created']}"
        )

    return totals


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Compile extractions into wiki page updates")
    parser.add_argument("--file", help="Compile only this extraction file (relative path)")
    parser.add_argument("--no-llm", action="store_true", help="Validate routing only, no Sonnet calls")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be compiled")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    result = asyncio.run(
        compile_all(
            file_filter=args.file,
            use_llm=not args.no_llm,
            dry_run=args.dry_run,
        )
    )

    if result.get("files_failed", 0) > 0:
        logger.warning("%d files failed compilation", result["files_failed"])


if __name__ == "__main__":
    main()
