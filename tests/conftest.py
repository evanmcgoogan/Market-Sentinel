"""Shared test fixtures for Digital Brain tests."""

import os
import sys
import tempfile
from pathlib import Path

import pytest

# Add scripts/ to path so we can import brain_io and ingestion modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))


@pytest.fixture
def tmp_brain(tmp_path, monkeypatch):
    """Create a temporary brain directory structure for testing.

    Patches brain_io.brain_root() to point to tmp_path so tests
    don't touch the real repo.
    """
    import brain_io

    # Create directory structure
    (tmp_path / "raw" / "tweets").mkdir(parents=True)
    (tmp_path / "raw" / "transcripts").mkdir(parents=True)
    (tmp_path / "raw" / "markets" / "polymarket").mkdir(parents=True)
    (tmp_path / "raw" / "markets" / "kalshi").mkdir(parents=True)
    (tmp_path / "raw" / "markets" / "price-feeds").mkdir(parents=True)
    (tmp_path / "raw" / "articles").mkdir(parents=True)
    (tmp_path / "config").mkdir(parents=True)

    # Create minimal files
    (tmp_path / "raw" / ".hashes").write_text("# Test hashes\n")
    (tmp_path / "log.md").write_text("# Test Log\n")

    # Write a test sources.yaml
    (tmp_path / "config" / "sources.yaml").write_text(
        """twitter:
  api: socialdata
  poll_interval_minutes: 10
  accounts:
    - handle: testuser
      tier: S
      domains: [ai, macro]
      notes: "Test account"

youtube:
  poll_interval_minutes: 30
  transcript_method: youtube-transcript-api
  channels:
    - name: "Test Channel"
      channel_id: "UC_test123"
      tier: S
      domains: [ai, technology]

markets:
  sources:
    - name: polymarket
      type: prediction-market
      poll: true
    - name: kalshi
      type: prediction-market
      poll: true
    - name: price-feeds
      type: asset-prices
      assets: [SPY, QQQ, BTC]
      poll: true
"""
    )

    # Patch brain_root to use tmp_path
    monkeypatch.setattr(brain_io, "brain_root", lambda: tmp_path)
    # Reset hash cache since we changed the root
    brain_io.reset_hash_cache()
    brain_io._HASHES_PATH = None

    return tmp_path
