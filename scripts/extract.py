"""
extract.py — Haiku-powered structured extraction pipeline.

Reads raw markdown files, sends them to Claude Haiku for extraction,
scores extractions using configured signal weights, and applies triage
verdicts. Outputs structured JSON to extractions/.

The LLM extracts; the scoring is deterministic and locally tunable.

Usage:
    python scripts/extract.py                            # Process all unextracted files
    python scripts/extract.py --file raw/tweets/karpathy/2026-04-06.md
    python scripts/extract.py --dry-run                  # Show what would be extracted
    python scripts/extract.py --no-llm                   # Score-only (re-triage existing extractions)

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

from brain_io import append_log, brain_root, utcnow

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5-20251001"
MAX_TOKENS = 4096


# ---------------------------------------------------------------------------
# Extraction tracking
# ---------------------------------------------------------------------------

def _extracted_registry_path() -> Path:
    return brain_root() / "extractions" / ".extracted"


def load_extracted_set() -> set[str]:
    """Load set of already-extracted raw file paths."""
    path = _extracted_registry_path()
    if not path.exists():
        return set()
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }


def mark_extracted(relative_path: str) -> None:
    """Record that a raw file has been extracted."""
    path = _extracted_registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(relative_path + "\n")


# ---------------------------------------------------------------------------
# Raw file discovery
# ---------------------------------------------------------------------------

def find_unextracted_files() -> list[str]:
    """Find raw files that haven't been extracted yet.

    Returns list of paths relative to brain root.
    """
    already = load_extracted_set()
    raw = brain_root() / "raw"
    unextracted = []

    for md_file in sorted(raw.rglob("*.md")):
        rel = str(md_file.relative_to(brain_root()))
        if rel not in already:
            unextracted.append(rel)

    return unextracted


def parse_raw_frontmatter(filepath: str) -> dict[str, Any]:
    """Parse YAML frontmatter from a raw file. Returns dict of fields."""
    full_path = brain_root() / filepath
    content = full_path.read_text(encoding="utf-8")

    # Extract between --- delimiters
    match = re.match(r"^---\n(.*?)\n---", content, re.DOTALL)
    if not match:
        return {}

    frontmatter: dict[str, Any] = {}
    for line in match.group(1).splitlines():
        line = line.strip()
        if ":" not in line or line.startswith("#"):
            continue
        key, _, value = line.partition(":")
        key = key.strip()
        value = value.strip()

        # Simple type coercion
        if value in ("true", "True"):
            frontmatter[key] = True
        elif value in ("false", "False"):
            frontmatter[key] = False
        elif value in ("null", "None", ""):
            frontmatter[key] = None
        elif value.startswith("[") and value.endswith("]"):
            # Inline list
            items = [v.strip().strip("\"'") for v in value[1:-1].split(",") if v.strip()]
            frontmatter[key] = items
        else:
            # Try numeric
            try:
                frontmatter[key] = int(value)
            except ValueError:
                try:
                    frontmatter[key] = float(value)
                except ValueError:
                    frontmatter[key] = value.strip("\"'")

    return frontmatter


def read_raw_body(filepath: str) -> str:
    """Read the body content of a raw file (everything after frontmatter)."""
    full_path = brain_root() / filepath
    content = full_path.read_text(encoding="utf-8")

    # Find end of frontmatter
    match = re.match(r"^---\n.*?\n---\n*", content, re.DOTALL)
    if match:
        return content[match.end():]
    return content


# ---------------------------------------------------------------------------
# Haiku extraction prompt
# ---------------------------------------------------------------------------

EXTRACTION_SYSTEM_PROMPT = """You are an intelligence analyst extracting structured signals from raw source material for a market intelligence knowledge base.

Your job: read the source material and extract every distinct claim, prediction, event, data point, or notable opinion. Be thorough but precise — extract what's actually there, don't infer or editorialize.

For each extraction, classify it and assess its properties. Output ONLY valid JSON, no markdown fences, no commentary."""

EXTRACTION_USER_PROMPT = """Analyze this source material and extract all notable items.

Source metadata:
- File: {source_file}
- Source type: {source_type}
- Source tier: {source_tier} (S=outlier signal, A=high signal, B=useful, C=ambient)
- Domains: {domains}

Source content:
---
{content}
---

Return a JSON object with this exact structure:
{{
  "extractions": [
    {{
      "type": "claim" or "prediction" or "data_point" or "event" or "opinion",
      "content": "The actual claim or observation, paraphrased clearly",
      "confidence": "stated_as_fact" or "high_conviction" or "medium_conviction" or "speculative" or "hedged",
      "entities": ["Person", "Company", "Institution"],
      "themes": ["theme-slug-using-hyphens"],
      "sentiment": "bullish" or "bearish" or "neutral" or "mixed",
      "temporal": "current" or "forward_looking" or "historical",
      "falsifiable": true or false,
      "actionable": true or false
    }}
  ],
  "affected_wiki_pages": [
    "wiki/entities/people/person-slug.md",
    "wiki/themes/theme-slug.md",
    "wiki/entities/companies/company-slug.md"
  ]
}}

Rules:
- Extract ALL distinct claims, not just the most interesting one
- For predictions: include specific timeframes and resolution criteria when stated
- entities should be proper names (people, companies, institutions)
- themes should be kebab-case slugs describing macro trends or narratives
- affected_wiki_pages should list every wiki page this content would update
- Use the page path conventions: people in wiki/entities/people/, companies in wiki/entities/companies/, themes in wiki/themes/
- If the content is pure noise (no extractable signal), return {{"extractions": [], "affected_wiki_pages": []}}
- Return ONLY the JSON object, nothing else"""


def build_extraction_prompt(
    source_file: str,
    frontmatter: dict[str, Any],
    body: str,
) -> tuple[str, str]:
    """Build system and user prompts for Haiku extraction.

    Returns (system_prompt, user_prompt).
    """
    source_type = frontmatter.get("source", "unknown")
    tier = frontmatter.get("tier", "C")
    domains = frontmatter.get("domains", [])
    if isinstance(domains, list):
        domains_str = ", ".join(domains)
    else:
        domains_str = str(domains)

    # Truncate body if extremely long (transcripts can be huge)
    # Haiku has 200k context but we want to keep costs down
    max_chars = 50_000
    if len(body) > max_chars:
        body = body[:max_chars] + "\n\n[... truncated for extraction ...]"

    user = EXTRACTION_USER_PROMPT.format(
        source_file=source_file,
        source_type=source_type,
        source_tier=tier,
        domains=domains_str,
        content=body,
    )
    return EXTRACTION_SYSTEM_PROMPT, user


# ---------------------------------------------------------------------------
# LLM interaction
# ---------------------------------------------------------------------------

async def call_haiku(system: str, user: str) -> str | None:
    """Call Claude Haiku and return the text response."""
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
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        return response.content[0].text
    except Exception as e:
        logger.error("Haiku API error: %s", e)
        return None


def parse_llm_response(raw_text: str) -> dict[str, Any] | None:
    """Parse Haiku's JSON response, handling common formatting issues."""
    if not raw_text:
        return None

    text = raw_text.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        logger.warning("JSON parse failed: %s — attempting recovery", e)

        # Try to find JSON object in the text
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass

        logger.error("Could not parse LLM response as JSON")
        return None


# ---------------------------------------------------------------------------
# Triage scoring (deterministic — no LLM)
# ---------------------------------------------------------------------------

def load_signal_weights() -> dict[str, Any]:
    """Load signal-weights.json."""
    path = brain_root() / "config" / "signal-weights.json"
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


def load_thresholds() -> dict[str, Any]:
    """Load thresholds.json."""
    path = brain_root() / "config" / "thresholds.json"
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)


def score_extraction(
    extraction: dict[str, Any],
    source_tier: str,
    weights: dict[str, Any],
) -> float:
    """Compute a signal score for a single extraction item.

    Uses extraction type base weight × source tier multiplier.
    Returns 0.0–1.0.
    """
    ext_type = extraction.get("type", "opinion")
    type_config = weights.get("extraction", {}).get(ext_type, {})

    base = type_config.get("base_weight", 0.3)

    # Apply tier multiplier
    if type_config.get("tier_multiplier_applies", True) is not False:
        tier_key = f"{source_tier.lower()}_tier_multiplier"
        multiplier = type_config.get(tier_key, 1.0)
    else:
        multiplier = 1.0

    score = base * multiplier

    # Bonus for falsifiable claims (these are more useful)
    if extraction.get("falsifiable", False):
        score *= 1.15

    # Bonus for forward-looking (predictions are high value)
    if extraction.get("temporal") == "forward_looking":
        score *= 1.1

    # Bonus for actionable items
    if extraction.get("actionable", False):
        score *= 1.1

    # Confidence modifier
    confidence = extraction.get("confidence", "speculative")
    confidence_mods = {
        "stated_as_fact": 1.0,
        "high_conviction": 1.05,
        "medium_conviction": 0.95,
        "speculative": 0.85,
        "hedged": 0.8,
    }
    score *= confidence_mods.get(confidence, 0.9)

    return min(1.0, max(0.0, score))


def compute_triage_verdict(
    extractions: list[dict[str, Any]],
    source_tier: str,
    weights: dict[str, Any],
    thresholds: dict[str, Any],
) -> tuple[str, float]:
    """Compute triage verdict for a set of extractions from one source file.

    Returns (verdict, max_score).
    Verdict: "high_signal" | "medium_signal" | "noise"
    """
    if not extractions:
        return "noise", 0.0

    triage_config = thresholds.get("triage", {})
    high_min = triage_config.get("high_signal_min_score", 0.7)
    medium_min = triage_config.get("medium_signal_min_score", 0.4)

    # S and A tier always extract per config
    always_extract = False
    if source_tier in ("S", "A"):
        if triage_config.get(f"{source_tier.lower()}_tier_always_extract", True):
            always_extract = True

    # Score each extraction
    scores = []
    for ext in extractions:
        s = score_extraction(ext, source_tier, weights)
        ext["signal_strength"] = round(s, 3)
        scores.append(s)

    max_score = max(scores)

    # Apply verdict
    if max_score >= high_min or (always_extract and max_score >= medium_min * 0.7):
        return "high_signal", max_score
    elif max_score >= medium_min or always_extract:
        return "medium_signal", max_score
    else:
        return "noise", max_score


# ---------------------------------------------------------------------------
# Extraction output
# ---------------------------------------------------------------------------

def save_extraction(
    source_file: str,
    result: dict[str, Any],
) -> Path:
    """Save extraction result as JSON, mirroring raw/ structure under extractions/."""
    # raw/tweets/karpathy/2026-04-06.md → extractions/tweets/karpathy/2026-04-06.json
    rel = source_file
    if rel.startswith("raw/"):
        rel = rel[4:]
    json_path = brain_root() / "extractions" / Path(rel).with_suffix(".json")
    json_path.parent.mkdir(parents=True, exist_ok=True)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)

    return json_path


# ---------------------------------------------------------------------------
# Core extraction pipeline
# ---------------------------------------------------------------------------

async def extract_file(
    source_file: str,
    use_llm: bool = True,
    dry_run: bool = False,
) -> dict[str, Any] | None:
    """Run the full extraction pipeline on a single raw file.

    Returns the extraction result dict, or None on failure.
    """
    logger.info("Extracting: %s", source_file)

    # Read raw file
    full_path = brain_root() / source_file
    if not full_path.exists():
        logger.error("File not found: %s", source_file)
        return None

    frontmatter = parse_raw_frontmatter(source_file)
    body = read_raw_body(source_file)

    if not body.strip():
        logger.warning("Empty body in %s — skipping", source_file)
        return None

    source_tier = frontmatter.get("tier", "C")

    if dry_run:
        logger.info("[DRY RUN] Would extract %s (tier: %s, ~%d chars)", source_file, source_tier, len(body))
        return None

    # Call Haiku for extraction
    llm_result = None
    if use_llm:
        system, user = build_extraction_prompt(source_file, frontmatter, body)
        raw_response = await call_haiku(system, user)
        llm_result = parse_llm_response(raw_response) if raw_response else None

    if llm_result is None and use_llm:
        logger.error("LLM extraction failed for %s", source_file)
        append_log(f"EXTRACT FAILED {source_file} | reason: LLM returned no parseable result")
        return None

    if llm_result is None:
        # --no-llm mode: load existing extraction if available
        existing_path = brain_root() / "extractions" / Path(source_file.removeprefix("raw/")).with_suffix(".json")
        if existing_path.exists():
            with open(existing_path) as f:
                llm_result = json.load(f)
            logger.info("Re-scoring existing extraction: %s", source_file)
        else:
            logger.warning("No existing extraction for %s — skipping (use LLM to create)", source_file)
            return None

    extractions = llm_result.get("extractions", [])
    affected_pages = llm_result.get("affected_wiki_pages", [])

    # Score and triage (deterministic)
    weights = load_signal_weights()
    thresholds = load_thresholds()
    verdict, max_score = compute_triage_verdict(extractions, source_tier, weights, thresholds)

    # Assemble result
    result = {
        "source_file": source_file,
        "source_tier": source_tier,
        "extracted_at": utcnow().isoformat(),
        "model": MODEL if use_llm else "re-scored",
        "extraction_count": len(extractions),
        "extractions": extractions,
        "triage_verdict": verdict,
        "triage_max_score": round(max_score, 3),
        "affected_wiki_pages": affected_pages,
    }

    # Save
    json_path = save_extraction(source_file, result)
    mark_extracted(source_file)

    append_log(
        f"EXTRACT {source_file} | "
        f"items: {len(extractions)} | "
        f"verdict: {verdict} ({max_score:.2f}) | "
        f"pages: {len(affected_pages)}"
    )

    logger.info(
        "Extracted %s: %d items, verdict=%s (%.2f), %d affected pages",
        source_file, len(extractions), verdict, max_score, len(affected_pages),
    )

    return result


async def extract_all(
    file_filter: str | None = None,
    use_llm: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run extraction on all unextracted raw files (or a single file)."""
    if file_filter:
        files = [file_filter]
    else:
        files = find_unextracted_files()

    if not files:
        logger.info("No unextracted files found")
        return {"files_processed": 0}

    logger.info("Found %d files to extract", len(files))

    totals = {
        "files_processed": 0,
        "files_succeeded": 0,
        "files_failed": 0,
        "high_signal": 0,
        "medium_signal": 0,
        "noise": 0,
        "total_extractions": 0,
    }

    for filepath in files:
        result = await extract_file(filepath, use_llm=use_llm, dry_run=dry_run)
        totals["files_processed"] += 1

        if result:
            totals["files_succeeded"] += 1
            totals["total_extractions"] += result.get("extraction_count", 0)
            verdict = result.get("triage_verdict", "noise")
            totals[verdict] = totals.get(verdict, 0) + 1
        elif not dry_run:
            totals["files_failed"] += 1

        # Rate limit between LLM calls
        if use_llm and not dry_run:
            await asyncio.sleep(0.5)

    if not dry_run:
        append_log(
            f"EXTRACT BATCH complete | "
            f"files: {totals['files_processed']} | "
            f"succeeded: {totals['files_succeeded']} | "
            f"high: {totals['high_signal']} | "
            f"medium: {totals['medium_signal']} | "
            f"noise: {totals['noise']}"
        )

    return totals


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Extract structured signals from raw source files")
    parser.add_argument("--file", help="Extract only this file (relative path from brain root)")
    parser.add_argument("--no-llm", action="store_true", help="Re-score existing extractions without calling the LLM")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be extracted")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    result = asyncio.run(
        extract_all(
            file_filter=args.file,
            use_llm=not args.no_llm,
            dry_run=args.dry_run,
        )
    )
    if result.get("files_failed", 0) > 0:
        logger.warning("%d files failed extraction", result["files_failed"])


if __name__ == "__main__":
    main()
