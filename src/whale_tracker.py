"""
Whale tracker stub — preserved for import compatibility.

The old WhaleTracker class used Polygon RPC to scan on-chain trades.
That pipeline has been superseded by WhaleBrain in whale_intelligence.py,
which uses the Polymarket Data API directly (single HTTP call, always has data).

This stub keeps imports in main.py and signals.py working without changes.
"""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

# Preserved for import compatibility (used in tests and database).
GLOBAL_WHALE_MARKET_ID = "__global__"


class WhaleTracker:
    """No-op stub.  All whale intelligence now lives in WhaleBrain."""

    def __init__(self, *args: Any, **kwargs: Any):
        logger.debug("WhaleTracker stub initialised (no-op)")

    async def scan_recent_trades(self) -> None:
        """No-op — whale scanning is handled by WhaleBrain."""

    async def close(self) -> None:
        """No-op."""

    def get_recent_whale_activity(
        self, market_id: str, minutes: int = 60
    ) -> Dict[str, Any]:
        """Return an empty activity dict."""
        return {
            "has_whale_activity": False,
            "trade_count": 0,
            "total_volume": 0.0,
            "top_wallets": [],
            "smart_money_trades": 0,
        }
