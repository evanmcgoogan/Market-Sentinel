"""
synthesize.py — Intelligence synthesis pipeline.

Reads recent extractions and compiled wiki context, then calls Sonnet (or Opus
for monthly reviews) to generate synthesis documents: intraday briefs, daily
wraps, weekly deeps, and monthly reviews.

This is the brain's "voice" — the intelligence output layer that turns raw
signals into actionable perspective. Everything else in the pipeline feeds here.

Usage:
    python scripts/synthesize.py                              # Auto-select subtype based on time
    python scripts/synthesize.py --subtype intraday-brief     # Force a specific type
    python scripts/synthesize.py --subtype daily-wrap
    python scripts/synthesize.py --dry-run                    # Show what would be synthesized
    python scripts/synthesize.py --no-llm                     # Build prompt, skip LLM call
    python scripts/synthesize.py --since 2026-04-17T10:00:00Z # Override lookback window

Requires ANTHROPIC_API_KEY environment variable.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from brain_io import append_log, brain_root, format_frontmatter, today_str, utcnow

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

SONNET_MODEL = "claude-sonnet-4-6"
OPUS_MODEL = "claude-opus-4-7"

SUBTYPE_MODELS: dict[str, str] = {
    "intraday-brief": SONNET_MODEL,
    "daily-wrap": SONNET_MODEL,
    "weekly-deep": SONNET_MODEL,
    "monthly-review": OPUS_MODEL,
    "event-driven": SONNET_MODEL,
}

MAX_TOKENS_SONNET = 8192
MAX_TOKENS_OPUS = 16384

# Default lookback hours per subtype when no prior synthesis timestamp exists
DEFAULT_LOOKBACK_HOURS: dict[str, int] = {
    "intraday-brief": 4,
    "daily-wrap": 24,
    "weekly-deep": 7 * 24,
    "monthly-review": 30 * 24,
    "event-driven": 2,
}

# ---------------------------------------------------------------------------
# Synthesis state
# ---------------------------------------------------------------------------


def _state_path() -> Path:
    return brain_root() / "wiki" / "syntheses" / ".state"


def load_synthesis_state() -> dict[str, Any]:
    """Load synthesis tracking state from wiki/syntheses/.state (JSON)."""
    path = _state_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_synthesis_state(state: dict[str, Any]) -> None:
    """Persist synthesis state to disk."""
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")


def get_last_synthesis_time(subtype: str) -> datetime | None:
    """Return the timestamp of the last successful synthesis for this subtype."""
    state = load_synthesis_state()
    entry = state.get(subtype, {})
    ts = entry.get("last_run")
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, AttributeError):
        return None


def record_synthesis(subtype: str, path: str, timestamp: datetime) -> None:
    """Record a completed synthesis run to the state file."""
    state = load_synthesis_state()
    state[subtype] = {
        "last_run": timestamp.isoformat(),
        "last_path": path,
    }
    save_synthesis_state(state)


# ---------------------------------------------------------------------------
# Extraction discovery
# ---------------------------------------------------------------------------


def find_extractions_since(since: datetime) -> list[dict[str, Any]]:
    """Load all non-noise extractions with a timestamp newer than `since`."""
    ext_dir = brain_root() / "extractions"
    if not ext_dir.exists():
        return []

    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)

    result = []
    for json_file in sorted(ext_dir.rglob("*.json")):
        # Skip registry/dot files
        if json_file.name.startswith("."):
            continue
        try:
            with open(json_file) as f:
                data = json.load(f)

            # Skip noise
            if data.get("triage_verdict", "noise") == "noise":
                continue

            # Parse extraction timestamp
            raw_ts = data.get("extracted_at") or data.get("created_at") or ""
            if not raw_ts:
                continue
            try:
                ts = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

            if ts > since:
                result.append(data)

        except (json.JSONDecodeError, OSError):
            continue

    return result


def count_high_signal_in_window(window_hours: float = 2.0) -> int:
    """Count high_signal extractions in the last `window_hours` hours."""
    since = utcnow() - timedelta(hours=window_hours)
    extractions = find_extractions_since(since)
    return sum(1 for e in extractions if e.get("triage_verdict") == "high_signal")


def is_event_driven_trigger(threshold: int = 3, window_hours: float = 2.0) -> bool:
    """Return True if enough high_signal extractions have arrived to fire event synthesis."""
    return count_high_signal_in_window(window_hours) >= threshold


# ---------------------------------------------------------------------------
# Wiki context loading
# ---------------------------------------------------------------------------


def find_active_theses() -> list[str]:
    """Return relative paths of all active thesis wiki pages."""
    theses_dir = brain_root() / "wiki" / "theses"
    if not theses_dir.exists():
        return []
    result = []
    for md in sorted(theses_dir.glob("*.md")):
        content = md.read_text(encoding="utf-8")
        # Status line appears in frontmatter: "status: active"
        if re.search(r"^status:\s*active\s*$", content, re.MULTILINE):
            result.append(str(md.relative_to(brain_root())))
    return result


def read_wiki_page(wiki_path: str) -> str | None:
    """Read a wiki page by relative path. Returns None if not found."""
    full = brain_root() / wiki_path
    if not full.exists():
        return None
    return full.read_text(encoding="utf-8")


def find_prior_synthesis(subtype: str) -> str | None:
    """Find the most recent synthesis of the given subtype.

    Returns the full markdown content, or None if no prior synthesis exists.
    """
    # Check state file first — fastest path
    state = load_synthesis_state()
    entry = state.get(subtype, {})
    last_path = entry.get("last_path")
    if last_path:
        content = read_wiki_page(last_path)
        if content:
            return content

    # Fallback: scan the syntheses directory
    synth_dir = brain_root() / "wiki" / "syntheses"
    if not synth_dir.exists():
        return None
    candidates = sorted(synth_dir.glob(f"*--*-{subtype}.md"))
    if not candidates:
        return None
    return candidates[-1].read_text(encoding="utf-8")


def find_current_syntheses_of_type(subtype: str, since: datetime) -> list[str]:
    """Find current (non-superseded) synthesis pages of the given subtype since a datetime.

    Uses the filename timestamp (YYYY-MM-DD--HHMM-subtype.md) for precise time
    comparison, falling back to the `created` frontmatter field.

    Returns relative paths.
    """
    synth_dir = brain_root() / "wiki" / "syntheses"
    if not synth_dir.exists():
        return []

    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)

    results = []
    for md in sorted(synth_dir.glob(f"*--*-{subtype}.md")):
        content = md.read_text(encoding="utf-8")
        # Skip already superseded
        if re.search(r"^status:\s*superseded\s*$", content, re.MULTILINE):
            continue

        # Try to parse precise timestamp from filename: YYYY-MM-DD--HHMM-subtype.md
        file_ts: datetime | None = None
        fn_match = re.match(r"(\d{4}-\d{2}-\d{2})--(\d{4})-", md.stem)
        if fn_match:
            try:
                file_ts = datetime.strptime(
                    f"{fn_match.group(1)} {fn_match.group(2)}", "%Y-%m-%d %H%M"
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                pass

        # Fall back to `period_end` frontmatter if filename parse failed
        if file_ts is None:
            m = re.search(r"^period_end:\s*(.+)$", content, re.MULTILINE)
            if m:
                try:
                    file_ts = datetime.fromisoformat(
                        m.group(1).strip().replace("Z", "+00:00")
                    )
                    if file_ts.tzinfo is None:
                        file_ts = file_ts.replace(tzinfo=timezone.utc)
                except ValueError:
                    pass

        if file_ts is not None and file_ts < since:
            continue

        results.append(str(md.relative_to(brain_root())))

    return results


# ---------------------------------------------------------------------------
# Skip logic
# ---------------------------------------------------------------------------


def should_skip(
    subtype: str,
    extractions: list[dict[str, Any]],
) -> tuple[bool, str]:
    """Return (should_skip, reason). Synthesis is skipped when there's nothing to say."""
    if not extractions:
        return True, "no new extractions since last synthesis"

    non_noise = [e for e in extractions if e.get("triage_verdict") != "noise"]
    if not non_noise:
        return True, "all new extractions are noise"

    return False, ""


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_TEMPLATE = """\
You are the synthesis engine for a personal market intelligence brain. Your role is to reframe \
accumulated intelligence into a clear, actionable perspective for an investor pursuing financial \
independence through decision intelligence.

You are writing a {subtype} synthesis. This is NOT a summary — it is a structured intelligence \
reframe that surfaces what changed, what it means for active theses, and what requires attention.

## Output Requirements

Return a COMPLETE markdown document. Start immediately with the frontmatter block. Do NOT wrap \
the output in code fences. Use this structure:

---
[frontmatter fields]
---

[body sections]

## Frontmatter

The following fields are already determined — copy them exactly as shown:

{stub_frontmatter}

You MUST complete these additional fields (replace the 0 placeholders):
- themes_covered: list of [[wikilink]] format themes mentioned in body (e.g., [["[[AI Capex Boom]]"]])
- theses_covered: list of [[wikilink]] format theses mentioned in body
- wiki_pages_referenced: integer — count of wiki pages you drew on
- key_findings: integer — count of notable findings in this synthesis
- sources_referenced: integer — count of distinct raw source files cited

## Body Format

{body_template}

## Quality Rules

- Every factual claim needs a citation to the raw source path (e.g., `raw/tweets/karpathy/2026-04-17.md`)
- Use [[wikilinks]] for all cross-references to wiki pages
- Contradictions between sources are VALUABLE — surface them explicitly, never suppress
- Be direct and concise. No filler. This investor reads this to make decisions.
- Lead with what matters most for active theses and current positions.
"""

_BODY_TEMPLATES: dict[str, str] = {
    "intraday-brief": """\
## What Changed

[3-5 bullet points: the most important new intelligence since the last brief. Lead with what \
matters for active theses and positions.]

## Active Thesis Impact

[For each thesis affected by new intelligence, one-line impact assessment.]

- **[[Thesis Name]]**: [positive/negative/neutral] — [one sentence why, citing raw source]

## Developing Situations

[Fast-moving events that need monitoring. Skip section if none.]

## New Signals

[High-signal extractions that don't fit existing theses. Skip if none.]

## Contradictions Surfaced

[Any contradictions between sources or with existing wiki claims. Skip if none.]""",

    "daily-wrap": """\
## Executive Summary

[3-5 bullet points: the day's most important developments and portfolio implications.]

## Theme Updates

### [[Theme Name]]

- Direction change: [if any, else "none"]
- New evidence: [summary with raw citations]
- Confidence shift: [old] → [new] (or "stable")

## Thesis Health

### [[Thesis Name]]

- Today's impact: [positive / negative / neutral]
- New supporting evidence: [if any]
- New counter-evidence: [if any]
- Confidence: [current value]
- Action required: [none / review / reduce / exit]

## New Signals Worth Watching

[Signals that emerged today but need more data. Skip if none.]

## Source Highlights

[Which sources produced the most valuable intelligence today?]""",

    "weekly-deep": """\
## Executive Summary

[5-7 bullet points: the week's macro narrative and key shifts.]

## Theme Trajectories

### [[Theme Name]]

- Direction: [accelerating/stable/decelerating/reversing]
- Evidence weight this week: [high/medium/low]
- Confidence: [start] → [end]
- Key developments: [2-3 sentences]

## Thesis Scorecard

| Thesis | Start Confidence | End Confidence | Key Events | Action |
|--------|-----------------|----------------|------------|--------|
| [[Thesis Name]] | 0.X | 0.X | [summary] | [none/review/exit] |

## Source Performance

| Source | Extractions | High Signal | Notable |
|--------|------------|-------------|---------|

## Weak Signals

[Patterns appearing across 2+ sources that haven't become themes yet. Alpha lives here.]""",

    "monthly-review": """\
## Strategic Overview

[Opus-level structural analysis. What patterns did the incremental pipeline miss? What \
cross-domain connections exist?]

## Cross-Domain Connections

[Links between themes/entities/signals that intraday synthesis couldn't see.]

1. [Connection] — Links: [[Page A]], [[Page B]]

## Thesis Stress Tests

### [[Thesis Name]] — Counter-Case

[Most compelling evidence against this thesis. Be honest about uncertainty.]

## Source Accuracy Audit

| Source | Predictions | Resolved | Hit Rate | Recommendation |
|--------|------------|----------|----------|---------------|

## Blind Spot Analysis

[What topics are under-covered? What assumptions are untested? Where might the brain be wrong?]

## Structural Recommendations

[Changes to thresholds, tiers, themes, or architecture suggested by the month's data.]""",

    "event-driven": """\
## Trigger Context

[What high-signal events triggered this synthesis. How many high-signal extractions, from which \
sources.]

## What Happened

[Clear narrative of the events/signals that triggered this run.]

## Active Thesis Impact

[Immediate implications for each affected thesis.]

- **[[Thesis Name]]**: [impact] — [why]

## Time-Sensitive Actions

[Anything requiring attention within 24 hours. Skip if none.]

## Developing Situation

[What to watch. Next catalysts. Resolution timeline.]""",
}


def _build_stub_frontmatter(
    subtype: str,
    period_start: datetime,
    period_end: datetime,
    extraction_count: int,
    high_signal_count: int,
    model: str,
) -> dict[str, Any]:
    """Build the deterministic frontmatter fields for this synthesis run."""
    title_map = {
        "intraday-brief": period_end.strftime("%Y-%m-%d %H:%M Intraday Brief"),
        "daily-wrap": period_end.strftime("%Y-%m-%d Daily Wrap"),
        "weekly-deep": period_end.strftime("Week ending %Y-%m-%d Deep Review"),
        "monthly-review": period_end.strftime("%B %Y Monthly Review"),
        "event-driven": period_end.strftime("%Y-%m-%d %H:%M Event-Driven Brief"),
    }
    title = title_map.get(subtype, f"{today_str()} {subtype}")

    # Intraday types use full datetime; daily+ use date-only
    intraday_types = {"intraday-brief", "event-driven"}
    if subtype in intraday_types:
        ps = period_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        pe = period_end.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        ps = period_start.strftime("%Y-%m-%d")
        pe = period_end.strftime("%Y-%m-%d")

    model_short = "sonnet-4.6" if "sonnet" in model else "opus-4.7"

    return {
        "title": title,
        "type": "synthesis",
        "subtype": subtype,
        "period_start": ps,
        "period_end": pe,
        "model": model_short,
        "extraction_count": extraction_count,
        "high_signal_count": high_signal_count,
        # These are placeholders the LLM must fill
        "sources_referenced": 0,
        "wiki_pages_referenced": 0,
        "key_findings": 0,
        "themes_covered": [],
        "theses_covered": [],
        "created": today_str(),
        "updated": today_str(),
        "status": "current",
        "supersedes": [],
        "superseded_by": "null",
        "tags": ["synthesis", subtype],
    }


def build_synthesis_prompt(
    subtype: str,
    extractions: list[dict[str, Any]],
    active_theses: list[str],
    prior_synthesis: str | None,
    stub_frontmatter: dict[str, Any],
    max_extraction_chars: int = 60_000,
    max_context_chars: int = 20_000,
) -> tuple[list[dict[str, Any]], str]:
    """Build the (messages, system_content) tuple for the synthesis LLM call.

    System prompt carries the role + template (cached). User message carries
    time-sensitive context: new extractions, active theses, prior synthesis.
    """
    stub_yaml = format_frontmatter(stub_frontmatter)
    body_template = _BODY_TEMPLATES.get(subtype, _BODY_TEMPLATES["intraday-brief"])

    system_content = _SYSTEM_PROMPT_TEMPLATE.format(
        subtype=subtype,
        stub_frontmatter=stub_yaml,
        body_template=body_template,
    )

    # Build user message parts
    parts: list[str] = []

    # New extractions (primary signal)
    extractions_json = json.dumps(extractions, indent=2, default=str)
    if len(extractions_json) > max_extraction_chars:
        extractions_json = extractions_json[:max_extraction_chars] + "\n... [truncated]"
    parts.append(
        f"## New Extractions ({len(extractions)} items)\n\n"
        f"```json\n{extractions_json}\n```"
    )

    # Active theses context
    if active_theses:
        thesis_parts: list[str] = []
        total_chars = 0
        for path in active_theses:
            content = read_wiki_page(path)
            if not content:
                continue
            excerpt = content[:800]
            thesis_parts.append(f"### {path}\n\n{excerpt}")
            total_chars += len(excerpt)
            if total_chars > max_context_chars:
                thesis_parts.append("... [additional theses truncated for length]")
                break
        if thesis_parts:
            parts.append("## Active Theses\n\n" + "\n\n---\n\n".join(thesis_parts))

    # Prior synthesis for continuity
    if prior_synthesis:
        prior_excerpt = prior_synthesis[:3000]
        if len(prior_synthesis) > 3000:
            prior_excerpt += "\n... [truncated]"
        parts.append(f"## Prior Synthesis (for continuity)\n\n{prior_excerpt}")

    user_text = "\n\n---\n\n".join(parts)
    user_text += f"\n\n---\n\nPlease write the {subtype} synthesis now."

    messages = [
        {
            "role": "user",
            "content": [{"type": "text", "text": user_text}],
        }
    ]
    return messages, system_content


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------


def call_llm(
    messages: list[dict[str, Any]],
    system_content: str,
    model: str,
    max_tokens: int,
) -> str:
    """Call Claude with prompt caching on the system message."""
    import anthropic

    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=[
            {
                "type": "text",
                "text": system_content,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=messages,
    )
    return response.content[0].text


# ---------------------------------------------------------------------------
# Response parsing and validation
# ---------------------------------------------------------------------------


def parse_synthesis_response(raw: str) -> str:
    """Extract the markdown document from the LLM response.

    Strips outer code fences if the model wrapped the output.
    """
    if not raw:
        return ""
    stripped = raw.strip()
    # Strip markdown fences if the model wrapped its response
    fenced = re.match(r"^```(?:markdown|md)?\n(.*?)```\s*$", stripped, re.DOTALL)
    if fenced:
        return fenced.group(1).strip()
    return stripped


def _extract_frontmatter_dict(content: str) -> dict[str, Any]:
    """Extract and parse the YAML frontmatter from a markdown document."""
    import yaml

    m = re.match(r"^---\n(.*?)\n---", content, re.DOTALL)
    if not m:
        return {}
    try:
        return yaml.safe_load(m.group(1)) or {}
    except yaml.YAMLError:
        return {}


def validate_synthesis_content(content: str) -> list[str]:
    """Validate a synthesis document. Returns error strings (empty list = valid)."""
    errors: list[str] = []

    if not content.startswith("---"):
        errors.append("missing YAML frontmatter (document must start with ---)")
        return errors

    if content.count("---") < 2:
        errors.append("unclosed frontmatter (need two --- delimiters)")
        return errors

    fm = _extract_frontmatter_dict(content)
    if not fm:
        errors.append("could not parse frontmatter YAML")
        return errors

    required_fields = [
        "title", "type", "subtype", "period_start", "period_end",
        "model", "extraction_count", "status", "created", "updated",
    ]
    for field in required_fields:
        if field not in fm:
            errors.append(f"missing required frontmatter field: {field}")

    if fm.get("type") != "synthesis":
        errors.append(f"type must be 'synthesis', got: {fm.get('type')!r}")

    return errors


def merge_frontmatter(content: str, stub: dict[str, Any]) -> str:
    """Enforce deterministic stub fields in the content's frontmatter.

    For fields the code controls (title, type, subtype, period_*, model,
    extraction_count, high_signal_count, created, updated, tags), the stub
    wins over whatever the LLM wrote. For intelligence fields (themes_covered,
    theses_covered, key_findings, etc.), the LLM value is preserved.
    """
    import yaml

    m = re.match(r"^(---\n)(.*?)(\n---)(.*)", content, re.DOTALL)
    if not m:
        # No frontmatter found — prepend stub and return
        stub_yaml = format_frontmatter(stub)
        return stub_yaml + "\n\n" + content

    try:
        fm: dict[str, Any] = yaml.safe_load(m.group(2)) or {}
    except yaml.YAMLError:
        fm = {}

    # Deterministic fields — stub always wins
    deterministic = [
        "title", "type", "subtype", "period_start", "period_end",
        "model", "extraction_count", "high_signal_count",
        "created", "updated", "tags",
    ]
    for field in deterministic:
        if field in stub:
            fm[field] = stub[field]

    # Defaults for LLM-populated fields if the model omitted them
    fm.setdefault("wiki_pages_referenced", 0)
    fm.setdefault("sources_referenced", 0)
    fm.setdefault("key_findings", 0)
    fm.setdefault("themes_covered", [])
    fm.setdefault("theses_covered", [])
    fm.setdefault("status", "current")
    fm.setdefault("supersedes", [])
    fm.setdefault("superseded_by", None)

    new_fm = format_frontmatter(fm)
    body = m.group(4).lstrip("\n")
    return new_fm + "\n\n" + body


# ---------------------------------------------------------------------------
# Supersession
# ---------------------------------------------------------------------------


def mark_superseded(paths: list[str], superseded_by: str) -> None:
    """Update synthesis pages to status: superseded, recording superseded_by path."""
    import yaml

    for rel_path in paths:
        full = brain_root() / rel_path
        if not full.exists():
            continue

        content = full.read_text(encoding="utf-8")
        m = re.match(r"^(---\n)(.*?)(\n---)(.*)", content, re.DOTALL)
        if not m:
            continue

        try:
            fm: dict[str, Any] = yaml.safe_load(m.group(2)) or {}
        except yaml.YAMLError:
            continue

        fm["status"] = "superseded"
        fm["superseded_by"] = superseded_by
        fm["updated"] = today_str()

        new_fm = format_frontmatter(fm)
        body = m.group(4).lstrip("\n")
        full.write_text(new_fm + "\n\n" + body, encoding="utf-8")
        logger.info("Marked superseded: %s → %s", rel_path, superseded_by)


def _handle_supersession(subtype: str, new_path: str, since: datetime) -> None:
    """Mark older synthesis pages as superseded when appropriate.

    daily-wrap supersedes intraday-brief pages from the same day.
    weekly-deep supersedes daily-wrap pages from the same week.
    monthly-review supersedes weekly-deep pages from the same month.
    """
    supersession_map = {
        "daily-wrap": "intraday-brief",
        "weekly-deep": "daily-wrap",
        "monthly-review": "weekly-deep",
    }
    supersedes_type = supersession_map.get(subtype)
    if not supersedes_type:
        return

    to_supersede = find_current_syntheses_of_type(supersedes_type, since)
    if to_supersede:
        mark_superseded(to_supersede, new_path)
        logger.info(
            "%s supersedes %d %s page(s)",
            subtype,
            len(to_supersede),
            supersedes_type,
        )


# ---------------------------------------------------------------------------
# Index update
# ---------------------------------------------------------------------------


def _update_synthesis_index(wiki_path: str, title: str) -> None:
    """Add or update the synthesis entry in index.md under the Syntheses section."""
    index_path = brain_root() / "index.md"
    if not index_path.exists():
        return

    content = index_path.read_text(encoding="utf-8")
    entry_line = f"- [{title}]({wiki_path}) | synthesis | current | {today_str()}"

    # Update existing entry if it's already there
    pattern = re.compile(rf"^- \[.*?\]\({re.escape(wiki_path)}\).*$", re.MULTILINE)
    if pattern.search(content):
        content = pattern.sub(entry_line, content)
    else:
        # Insert under Syntheses section — replace _(No pages yet)_ if present
        section = "Syntheses"
        no_pages_pattern = re.compile(
            rf"(## {re.escape(section)}\n)\n_\(No pages yet\)_",
            re.MULTILINE,
        )
        match = no_pages_pattern.search(content)
        if match:
            content = (
                content[: match.start()]
                + match.group(1)
                + "\n"
                + entry_line
                + content[match.end() :]
            )
        else:
            # Section has entries — append after last entry in section
            sec_match = re.search(rf"## {re.escape(section)}\n", content)
            if sec_match:
                next_sec = re.search(r"\n## ", content[sec_match.end() :])
                if next_sec:
                    insert_pos = sec_match.end() + next_sec.start()
                else:
                    insert_pos = len(content)
                block = content[sec_match.end() : insert_pos].rstrip()
                content = (
                    content[: sec_match.end()]
                    + block
                    + "\n"
                    + entry_line
                    + "\n"
                    + content[insert_pos:]
                )

    index_path.write_text(content, encoding="utf-8")


# ---------------------------------------------------------------------------
# Output path
# ---------------------------------------------------------------------------


def synthesis_output_path(subtype: str, timestamp: datetime) -> str:
    """Generate the output path for a synthesis page.

    Format: wiki/syntheses/YYYY-MM-DD--HHMM-{subtype}.md
    Matches the naming convention in CLAUDE.md.
    """
    date_str = timestamp.strftime("%Y-%m-%d")
    time_str = timestamp.strftime("%H%M")
    return f"wiki/syntheses/{date_str}--{time_str}-{subtype}.md"


# ---------------------------------------------------------------------------
# Core synthesis function
# ---------------------------------------------------------------------------


def synthesize(
    subtype: str,
    since: datetime | None = None,
    dry_run: bool = False,
    no_llm: bool = False,
    verbose: bool = False,
) -> dict[str, Any]:
    """Run a synthesis of the given subtype.

    Returns a result dict with keys: skipped, reason, path, extraction_count,
    high_signal_count, model.
    """
    now = utcnow()

    # Determine lookback window
    if since is None:
        since = get_last_synthesis_time(subtype)
        if since is None:
            hours = DEFAULT_LOOKBACK_HOURS.get(subtype, 4)
            since = now - timedelta(hours=hours)

    logger.info("Synthesizing %s | since: %s", subtype, since.isoformat())

    # Gather extractions
    extractions = find_extractions_since(since)
    if verbose:
        logger.info("Found %d extractions since %s", len(extractions), since.isoformat())

    # Check skip conditions
    skip, reason = should_skip(subtype, extractions)
    if skip:
        logger.info("Skipping %s: %s", subtype, reason)
        return {"skipped": True, "reason": reason, "path": None, "extraction_count": 0}

    high_signal_count = sum(
        1 for e in extractions if e.get("triage_verdict") == "high_signal"
    )

    # Select model and token budget
    model = SUBTYPE_MODELS.get(subtype, SONNET_MODEL)
    max_tokens = MAX_TOKENS_OPUS if model == OPUS_MODEL else MAX_TOKENS_SONNET

    # Build stub frontmatter
    stub = _build_stub_frontmatter(
        subtype=subtype,
        period_start=since,
        period_end=now,
        extraction_count=len(extractions),
        high_signal_count=high_signal_count,
        model=model,
    )

    # Load wiki context
    active_theses = find_active_theses()
    prior_synthesis = find_prior_synthesis(subtype)

    if verbose or dry_run or no_llm:
        logger.info(
            "Context: %d extractions, %d active theses, prior synthesis: %s",
            len(extractions),
            len(active_theses),
            "yes" if prior_synthesis else "no",
        )

    if dry_run:
        return {
            "skipped": False,
            "reason": "dry_run",
            "path": None,
            "extraction_count": len(extractions),
            "high_signal_count": high_signal_count,
            "active_theses": len(active_theses),
            "model": model,
        }

    # Build prompt
    messages, system_content = build_synthesis_prompt(
        subtype=subtype,
        extractions=extractions,
        active_theses=active_theses,
        prior_synthesis=prior_synthesis,
        stub_frontmatter=stub,
    )

    if no_llm:
        logger.info("[no-llm] Would call %s with %d messages", model, len(messages))
        return {
            "skipped": False,
            "reason": "no_llm",
            "path": None,
            "extraction_count": len(extractions),
            "high_signal_count": high_signal_count,
            "model": model,
        }

    # Call LLM
    logger.info("Calling %s for %s synthesis...", model, subtype)
    try:
        raw = call_llm(messages, system_content, model, max_tokens)
    except Exception as e:
        logger.error("LLM call failed: %s", e)
        return {
            "skipped": True,
            "reason": f"llm_error: {e}",
            "path": None,
            "extraction_count": 0,
        }

    # Parse, validate, and merge
    content = parse_synthesis_response(raw)
    content = merge_frontmatter(content, stub)

    errors = validate_synthesis_content(content)
    if errors:
        logger.warning("Synthesis validation warnings: %s", errors)

    # Determine output path and write
    out_path = synthesis_output_path(subtype, now)
    full_out = brain_root() / out_path
    full_out.parent.mkdir(parents=True, exist_ok=True)
    full_out.write_text(content.rstrip() + "\n", encoding="utf-8")
    logger.info("Wrote synthesis: %s", out_path)

    # Supersession
    _handle_supersession(subtype, out_path, since)

    # Index
    fm = _extract_frontmatter_dict(content)
    title = fm.get("title", f"{subtype} {now.strftime('%Y-%m-%d %H:%M')}")
    _update_synthesis_index(out_path, title)

    # Record state and log
    record_synthesis(subtype, out_path, now)

    domains: list[str] = sorted(
        {d for e in extractions for d in e.get("domains", [])}
    )
    append_log(
        f"SYNTHESIZED {subtype} | extractions: {len(extractions)} | "
        f"high_signal: {high_signal_count} | model: {model} | "
        f"domains: {domains} | path: {out_path}"
    )

    return {
        "skipped": False,
        "reason": "ok",
        "path": out_path,
        "extraction_count": len(extractions),
        "high_signal_count": high_signal_count,
        "model": model,
    }


# ---------------------------------------------------------------------------
# Auto subtype selection
# ---------------------------------------------------------------------------


def auto_select_subtype() -> str:
    """Select the appropriate synthesis subtype based on current ET time."""
    from zoneinfo import ZoneInfo

    now_et = datetime.now(ZoneInfo("America/New_York"))

    # Monthly review: 1st Sunday of month at 05:00 ET
    if now_et.weekday() == 6 and now_et.day <= 7 and now_et.hour == 5:
        return "monthly-review"

    # Weekly deep: every Sunday at 04:00 ET
    if now_et.weekday() == 6 and now_et.hour == 4:
        return "weekly-deep"

    # Daily wrap: 21:00 ET any day
    if now_et.hour == 21:
        return "daily-wrap"

    # Event-driven: burst of high-signal extractions
    if is_event_driven_trigger():
        return "event-driven"

    # Default: intraday brief
    return "intraday-brief"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Intelligence synthesis pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--subtype",
        choices=list(SUBTYPE_MODELS.keys()),
        default=None,
        help="Force a specific synthesis type (default: auto-select based on time)",
    )
    parser.add_argument(
        "--since",
        default=None,
        metavar="ISO8601",
        help="Override lookback start time (e.g. 2026-04-17T10:00:00Z)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be synthesized without calling the LLM",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Build the prompt inputs but skip the LLM call",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose logging output",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stderr,
    )

    since_dt: datetime | None = None
    if args.since:
        try:
            since_dt = datetime.fromisoformat(args.since.replace("Z", "+00:00"))
        except ValueError:
            logger.error("Invalid --since format: %s (expected ISO 8601)", args.since)
            sys.exit(1)

    subtype = args.subtype or auto_select_subtype()
    logger.info("Running synthesis: %s", subtype)

    result = synthesize(
        subtype=subtype,
        since=since_dt,
        dry_run=args.dry_run,
        no_llm=args.no_llm,
        verbose=args.verbose,
    )

    if result.get("skipped"):
        print(f"SKIPPED: {result.get('reason')}")
        sys.exit(0)

    if args.dry_run:
        print("DRY RUN — would synthesize:")
        print(f"  subtype:        {subtype}")
        print(f"  extractions:    {result.get('extraction_count', 0)}")
        print(f"  high_signal:    {result.get('high_signal_count', 0)}")
        print(f"  active_theses:  {result.get('active_theses', 0)}")
        print(f"  model:          {result.get('model')}")
        sys.exit(0)

    if args.no_llm:
        print(
            f"NO-LLM: Would call {result.get('model')} with "
            f"{result.get('extraction_count', 0)} extractions"
        )
        sys.exit(0)

    print(f"OK: {result.get('path')}")
    print(f"  extractions:  {result.get('extraction_count', 0)}")
    print(f"  high_signal:  {result.get('high_signal_count', 0)}")
    print(f"  model:        {result.get('model')}")


if __name__ == "__main__":
    main()
