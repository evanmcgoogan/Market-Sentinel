"""
Order book shape tracking and imbalance detection.
Monitors bid/ask depth, spread, and one-sided stacking.

Order book imbalance is a leading indicator:
- Heavy bids with light asks = someone knows price is going up
- Spread tightening = market maker confidence increasing
- One-sided stacking = directional pressure building
"""

import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple

from database import Database
from config import OrderBookConfig


logger = logging.getLogger(__name__)


class OrderBookAnalyzer:
    """
    Analyzes order book shape for anomalies.

    Key metrics:
    1. Bid/Ask depth ratio — imbalance shows directional pressure
    2. Spread — tightening means increasing confidence/activity
    3. Depth concentration — large orders at specific levels
    4. Shape changes — sudden shifts in book shape
    """

    def __init__(self, config: OrderBookConfig, db: Database):
        self.config = config
        self.db = db
        self._cycle_count = 0

    def should_fetch_this_cycle(self) -> bool:
        """Check if we should fetch order books this cycle (rate limiting)."""
        self._cycle_count += 1
        return self._cycle_count % self.config.fetch_every_n_cycles == 0

    def parse_polymarket_orderbook(
        self,
        orderbook_data: Dict,
        market_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Parse Polymarket CLOB API order book response.

        Expected format:
        {
            "bids": [{"price": "0.55", "size": "100"}, ...],
            "asks": [{"price": "0.60", "size": "50"}, ...],
        }
        """
        if not orderbook_data:
            return None

        bids = orderbook_data.get("bids", [])
        asks = orderbook_data.get("asks", [])

        if not bids and not asks:
            return None

        return self._analyze_book(bids, asks, market_id, "polymarket")

    def parse_kalshi_orderbook(
        self,
        orderbook_data: Dict,
        market_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Parse Kalshi order book response.

        Expected format:
        {
            "orderbook": {
                "yes": [[price_cents, quantity], ...],
                "no": [[price_cents, quantity], ...],
            }
        }
        """
        if not orderbook_data:
            return None

        ob = orderbook_data.get("orderbook", orderbook_data)
        yes_orders = ob.get("yes", [])
        no_orders = ob.get("no", [])

        # Convert to standard format
        bids = []
        for order in yes_orders:
            if isinstance(order, (list, tuple)) and len(order) >= 2:
                bids.append({"price": str(order[0] / 100), "size": str(order[1])})
            elif isinstance(order, dict):
                bids.append({
                    "price": str(order.get("price", 0) / 100),
                    "size": str(order.get("quantity", 0)),
                })

        asks = []
        for order in no_orders:
            if isinstance(order, (list, tuple)) and len(order) >= 2:
                # No orders are effectively asks on the yes token
                asks.append({"price": str(1 - order[0] / 100), "size": str(order[1])})
            elif isinstance(order, dict):
                asks.append({
                    "price": str(1 - order.get("price", 0) / 100),
                    "size": str(order.get("quantity", 0)),
                })

        if not bids and not asks:
            return None

        return self._analyze_book(bids, asks, market_id, "kalshi")

    def _analyze_book(
        self,
        bids: List[Dict],
        asks: List[Dict],
        market_id: str,
        platform: str,
    ) -> Optional[Dict[str, Any]]:
        """Compute order book metrics from bid/ask arrays."""
        try:
            # Parse bids and asks
            parsed_bids = []
            for b in bids:
                try:
                    price = float(b.get("price", 0))
                    size = float(b.get("size", 0))
                    if price > 0 and size > 0:
                        parsed_bids.append((price, size))
                except (ValueError, TypeError):
                    continue

            parsed_asks = []
            for a in asks:
                try:
                    price = float(a.get("price", 0))
                    size = float(a.get("size", 0))
                    if price > 0 and size > 0:
                        parsed_asks.append((price, size))
                except (ValueError, TypeError):
                    continue

            # Sort: bids descending, asks ascending
            parsed_bids.sort(key=lambda x: x[0], reverse=True)
            parsed_asks.sort(key=lambda x: x[0])

            # Compute metrics
            bid_depth = sum(p * s for p, s in parsed_bids)  # USD-weighted depth
            ask_depth = sum(p * s for p, s in parsed_asks)

            best_bid = parsed_bids[0][0] if parsed_bids else 0.0
            best_ask = parsed_asks[0][0] if parsed_asks else 1.0
            spread = best_ask - best_bid if best_ask > best_bid else 0.0

            # Bid/ask ratio (>1 means more buying pressure)
            bid_ask_ratio = bid_depth / ask_depth if ask_depth > 0 else float('inf')
            if bid_ask_ratio == float('inf'):
                bid_ask_ratio = 10.0  # Cap for storage

            # Top levels for detailed analysis
            top_levels = {
                "top_bids": [{"price": p, "size": s} for p, s in parsed_bids[:5]],
                "top_asks": [{"price": p, "size": s} for p, s in parsed_asks[:5]],
            }

            # Save to database
            self.db.save_orderbook_snapshot(
                platform=platform,
                market_id=market_id,
                bid_depth=bid_depth,
                ask_depth=ask_depth,
                spread=spread,
                best_bid=best_bid,
                best_ask=best_ask,
                bid_ask_ratio=bid_ask_ratio,
                top_levels=top_levels,
            )

            return {
                "bid_depth": bid_depth,
                "ask_depth": ask_depth,
                "spread": spread,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "bid_ask_ratio": bid_ask_ratio,
                "bid_levels": len(parsed_bids),
                "ask_levels": len(parsed_asks),
            }

        except Exception as e:
            logger.debug(f"Error analyzing order book for {market_id}: {e}")
            return None

    def detect_imbalance(
        self,
        platform: str,
        market_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Detect order book imbalance by comparing current state to baseline.

        Returns analysis dict if anomaly detected, None otherwise.
        """
        current = self.db.get_latest_orderbook(platform, market_id)
        if not current:
            return None

        baseline = self.db.get_orderbook_baseline(platform, market_id, hours=24)

        result = {
            "has_imbalance": False,
            "bid_ask_ratio": current.get("bid_ask_ratio", 1.0),
            "spread": current.get("spread", 0.0),
            "bid_depth": current.get("bid_depth", 0.0),
            "ask_depth": current.get("ask_depth", 0.0),
            "reasons": [],
        }

        ratio = current.get("bid_ask_ratio", 1.0)

        # Check bid/ask imbalance
        if ratio >= 2.0:  # Use fixed threshold
            result["has_imbalance"] = True
            result["reasons"].append(f"Heavy buy pressure (bid/ask ratio: {ratio:.1f}x)")
        elif ratio <= 0.5:
            result["has_imbalance"] = True
            result["reasons"].append(f"Heavy sell pressure (bid/ask ratio: {ratio:.1f}x)")

        # Check spread tightening vs baseline
        if baseline and baseline.get("avg_spread", 0) > 0:
            current_spread = current.get("spread", 0)
            avg_spread = baseline["avg_spread"]
            if avg_spread > 0:
                spread_change_pct = ((avg_spread - current_spread) / avg_spread) * 100
                if spread_change_pct >= 50:  # 50% tighter than usual
                    result["has_imbalance"] = True
                    result["reasons"].append(
                        f"Spread tightened {spread_change_pct:.0f}% "
                        f"({avg_spread:.4f} → {current_spread:.4f})"
                    )
                result["spread_change_pct"] = spread_change_pct

        # Check for sudden depth changes
        if baseline and baseline.get("sample_count", 0) >= 3:
            avg_bid = baseline.get("avg_bid_depth", 0)
            avg_ask = baseline.get("avg_ask_depth", 0)
            cur_bid = current.get("bid_depth", 0)
            cur_ask = current.get("ask_depth", 0)

            # One side surging while the other drops
            if avg_bid > 0 and avg_ask > 0:
                bid_change = (cur_bid - avg_bid) / avg_bid
                ask_change = (cur_ask - avg_ask) / avg_ask

                # Bids up, asks down = someone loading up
                if bid_change > 0.5 and ask_change < -0.3:
                    result["has_imbalance"] = True
                    result["reasons"].append(
                        f"One-sided stacking: bids +{bid_change*100:.0f}%, "
                        f"asks {ask_change*100:.0f}%"
                    )
                # Asks up, bids down = someone dumping
                elif ask_change > 0.5 and bid_change < -0.3:
                    result["has_imbalance"] = True
                    result["reasons"].append(
                        f"One-sided stacking: asks +{ask_change*100:.0f}%, "
                        f"bids {bid_change*100:.0f}%"
                    )

        return result
