"""
brain_io.py — Shared I/O utilities for the Digital Brain ingestion layer.

Handles: config loading, raw file writing with frontmatter, deduplication,
log/index appending, and common path resolution. Every ingestion script
imports this module rather than re-implementing file I/O.
"""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

def brain_root() -> Path:
    """Return the repo root (parent of scripts/)."""
    return Path(__file__).resolve().parent.parent


def raw_dir() -> Path:
    return brain_root() / "raw"


def config_dir() -> Path:
    return brain_root() / "config"


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_sources_config() -> dict[str, Any]:
    """Load and return config/sources.yaml."""
    path = config_dir() / "sources.yaml"
    with open(path) as f:
        cfg = yaml.safe_load(f)
    return cfg or {}


# ---------------------------------------------------------------------------
# Frontmatter helpers
# ---------------------------------------------------------------------------

def format_frontmatter(fields: dict[str, Any]) -> str:
    """Render a dict as YAML frontmatter block (--- delimited).

    Handles simple types cleanly. Lists stay inline for short items,
    block-style for long items. No trailing whitespace.
    """
    lines = ["---"]
    for key, value in fields.items():
        lines.append(_yaml_line(key, value))
    lines.append("---")
    return "\n".join(lines)


def _yaml_line(key: str, value: Any) -> str:
    """Format a single key: value line for frontmatter."""
    if value is None:
        return f"{key}: null"
    if isinstance(value, bool):
        return f"{key}: {'true' if value else 'false'}"
    if isinstance(value, (int, float)):
        return f"{key}: {value}"
    if isinstance(value, datetime):
        return f"{key}: {value.isoformat()}"
    if isinstance(value, list):
        if not value:
            return f"{key}: []"
        # Short lists inline, long lists block-style
        if all(isinstance(v, str) and len(v) < 40 for v in value):
            items = ", ".join(f'"{v}"' if " " in str(v) else str(v) for v in value)
            return f"{key}: [{items}]"
        block = "\n".join(f"  - {v}" for v in value)
        return f"{key}:\n{block}"
    # String — quote if it contains special chars
    s = str(value)
    if any(c in s for c in ":{}[]#&*!|>'\"%@`"):
        escaped = s.replace('"', '\\"')
        return f'{key}: "{escaped}"'
    return f"{key}: {s}"


def write_raw_file(
    relative_path: str,
    frontmatter: dict[str, Any],
    body: str,
) -> Path:
    """Write a raw markdown file with frontmatter + body.

    Args:
        relative_path: Path relative to brain root (e.g. "raw/tweets/karpathy/2026-04-06.md")
        frontmatter: Dict of frontmatter fields
        body: Markdown body content

    Returns:
        Absolute path of the written file.
    """
    full_path = brain_root() / relative_path
    full_path.parent.mkdir(parents=True, exist_ok=True)

    content = format_frontmatter(frontmatter) + "\n\n" + body.rstrip() + "\n"
    full_path.write_text(content, encoding="utf-8")

    logger.info("Wrote raw file: %s", relative_path)
    return full_path


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

_HASHES_PATH: Path | None = None


def _hashes_path() -> Path:
    global _HASHES_PATH
    if _HASHES_PATH is None:
        _HASHES_PATH = raw_dir() / ".hashes"
    return _HASHES_PATH


def _load_hashes() -> set[str]:
    """Load existing hashes from raw/.hashes into a set."""
    path = _hashes_path()
    if not path.exists():
        return set()
    hashes = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            hashes.add(line)
    return hashes


# Module-level cache to avoid re-reading on every check
_hash_cache: set[str] | None = None


def is_duplicate(source_type: str, unique_id: str, content: str = "") -> bool:
    """Check if this content has already been ingested.

    Hash format: {source_type}:{unique_id}:{content_hash}
    The content_hash part uses the first 12 chars of SHA-256.
    """
    global _hash_cache
    if _hash_cache is None:
        _hash_cache = _load_hashes()

    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()[:12] if content else "none"
    entry = f"{source_type}:{unique_id}:{content_hash}"

    # Also check just source_type:unique_id prefix for ID-based dedup
    prefix = f"{source_type}:{unique_id}:"
    if entry in _hash_cache or any(h.startswith(prefix) for h in _hash_cache):
        return True
    return False


def record_hash(source_type: str, unique_id: str, content: str = "") -> None:
    """Append a new hash entry to raw/.hashes and the in-memory cache."""
    global _hash_cache
    if _hash_cache is None:
        _hash_cache = _load_hashes()

    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()[:12] if content else "none"
    entry = f"{source_type}:{unique_id}:{content_hash}"

    _hash_cache.add(entry)
    with open(_hashes_path(), "a", encoding="utf-8") as f:
        f.write(entry + "\n")


def reset_hash_cache() -> None:
    """Clear in-memory hash cache (useful for testing)."""
    global _hash_cache
    _hash_cache = None


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def append_log(message: str) -> None:
    """Append an operation entry to log.md."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    entry = f"[{timestamp}] {message}\n"

    log_path = brain_root() / "log.md"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(entry)


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def utcnow() -> datetime:
    """Current time in UTC with timezone info."""
    return datetime.now(timezone.utc)


def today_str() -> str:
    """Today's date as YYYY-MM-DD string."""
    return utcnow().strftime("%Y-%m-%d")


def slugify(text: str, max_length: int = 60) -> str:
    """Convert text to a URL/filename-safe slug.

    Lowercase, replace spaces/special chars with hyphens, collapse runs,
    strip leading/trailing hyphens, truncate to max_length.
    """
    import re
    slug = text.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    slug = slug.strip("-")
    if len(slug) > max_length:
        # Cut at last hyphen before max_length to avoid mid-word truncation
        cut = slug[:max_length].rfind("-")
        slug = slug[:cut] if cut > 20 else slug[:max_length]
    return slug
