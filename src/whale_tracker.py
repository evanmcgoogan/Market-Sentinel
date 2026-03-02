"""
Whale wallet tracker for Polymarket via Polygon blockchain.
Tracks large traders' on-chain activity to detect smart money movements.

Polymarket runs on Polygon (MATIC). Trades go through the CTF Exchange
contract. We monitor for large trades and build wallet profiles over time.

Uses free Polygon RPC — no API key needed.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

import aiohttp

from database import Database
from config import WhaleConfig


logger = logging.getLogger(__name__)

# Polymarket CTF Exchange OrderFilled event topic
# keccak256("OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)")
ORDER_FILLED_TOPIC = "0xd0a08e8c493f9c94f29311f9f468f3c1a2d6e680c6a2c1eb97b5a4e3de1c9090"

# USDC contract on Polygon
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# ERC20 Transfer event topic
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
POLYMARKET_DATA_API = "https://data-api.polymarket.com/trades"
GLOBAL_WHALE_MARKET_ID = "__global__"


class WhaleTracker:
    """
    Tracks large Polymarket traders via on-chain Polygon data.

    Strategy:
    1. Scan recent blocks for large USDC transfers to/from CTF Exchange
    2. Build wallet profiles (trade count, win rate, volume)
    3. Flag when known high-accuracy wallets enter new positions

    This is the "hunt the shark" feature — we're looking for wallets
    that consistently trade before events resolve correctly.
    """

    def __init__(self, config: WhaleConfig, db: Database):
        self.config = config
        self.db = db
        self._session: Optional[aiohttp.ClientSession] = None
        self._owns_session = False
        self._last_scan_block: Optional[int] = None
        self._last_scan_time = datetime.min.replace(tzinfo=timezone.utc)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
            )
            self._owns_session = True
        return self._session

    async def close(self):
        if self._owns_session and self._session:
            await self._session.close()
            self._session = None

    async def _rpc_call(self, method: str, params: list) -> Any:
        """Make a JSON-RPC call to Polygon."""
        session = await self._get_session()
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        }

        try:
            async with session.post(self.config.polygon_rpc_url, json=payload) as response:
                if response.status != 200:
                    logger.warning(f"Polygon RPC error: status {response.status}")
                    return None
                data = await response.json()
                if "error" in data:
                    logger.warning(f"Polygon RPC error: {data['error']}")
                    return None
                return data.get("result")
        except Exception as e:
            logger.debug(f"Polygon RPC call failed: {e}")
            return None

    async def _get_latest_block(self) -> Optional[int]:
        """Get the latest block number on Polygon."""
        result = await self._rpc_call("eth_blockNumber", [])
        if result:
            return int(result, 16)
        return None

    async def scan_recent_trades(self):
        """
        Scan recent Polygon blocks for large Polymarket trades.
        Called periodically from the main loop.
        """
        now = datetime.now(timezone.utc)
        interval_seconds = self.config.scan_interval_minutes * 60

        if (now - self._last_scan_time).total_seconds() < interval_seconds:
            return  # Too soon

        self._last_scan_time = now

        try:
            latest_block = await self._get_latest_block()
            if latest_block is None:
                logger.debug("Whale tracker: couldn't get latest block")
                return

            # Determine scan range
            if self._last_scan_block is None:
                # First scan: look back N blocks
                from_block = latest_block - self.config.blocks_per_scan
            else:
                from_block = self._last_scan_block + 1

            # Don't scan too many blocks at once
            to_block = min(latest_block, from_block + self.config.blocks_per_scan)

            if from_block > to_block:
                return

            # Scan for large USDC transfers to/from CTF Exchange
            trades = await self._get_large_trades(from_block, to_block)

            self._last_scan_block = to_block

            if trades:
                logger.info(f"Whale tracker: found {len(trades)} large trades in blocks {from_block}-{to_block}")
                await self._process_trades(trades)

        except Exception as e:
            logger.error(f"Whale tracker scan error: {e}")

    async def _get_large_trades(
        self,
        from_block: int,
        to_block: int,
    ) -> List[Dict[str, Any]]:
        """
        Get large USDC transfers involving the CTF Exchange contract.
        These represent Polymarket trades.
        """
        trades = []

        # Query USDC Transfer events to/from CTF Exchange
        ctf_address = self.config.ctf_exchange_address.lower()
        ctf_padded = "0x" + ctf_address[2:].zfill(64)

        # Transfers TO CTF Exchange (buying positions)
        buy_filter = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": USDC_ADDRESS,
            "topics": [
                TRANSFER_TOPIC,
                None,  # from: any
                ctf_padded,  # to: CTF Exchange
            ],
        }

        # Transfers FROM CTF Exchange (selling/settling)
        sell_filter = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": USDC_ADDRESS,
            "topics": [
                TRANSFER_TOPIC,
                ctf_padded,  # from: CTF Exchange
                None,  # to: any
            ],
        }

        for direction, log_filter in [("buy", buy_filter), ("sell", sell_filter)]:
            result = await self._rpc_call("eth_getLogs", [log_filter])
            if not result:
                continue

            for log in result:
                try:
                    # Decode USDC amount (6 decimals)
                    raw_amount = int(log["data"], 16)
                    amount_usdc = raw_amount / 1e6

                    if amount_usdc < self.config.min_trade_size_usdc:
                        continue

                    # Extract wallet address
                    topics = log.get("topics", [])
                    if direction == "buy" and len(topics) >= 2:
                        # Buyer is the 'from' address
                        wallet = "0x" + topics[1][-40:]
                    elif direction == "sell" and len(topics) >= 3:
                        # Seller is the 'to' address
                        wallet = "0x" + topics[2][-40:]
                    else:
                        continue

                    trades.append({
                        "address": wallet.lower(),
                        "direction": f"{direction}_yes",  # Simplified
                        "amount": amount_usdc,
                        "tx_hash": log.get("transactionHash", ""),
                        "block_number": int(log.get("blockNumber", "0x0"), 16),
                    })

                except (ValueError, IndexError, KeyError) as e:
                    logger.debug(f"Error parsing trade log: {e}")
                    continue

        return trades

    async def _process_trades(self, trades: List[Dict[str, Any]]):
        """Process discovered trades: save them and update wallet profiles."""
        enrichment = await self._fetch_trade_enrichment(
            [t.get("tx_hash", "") for t in trades if t.get("tx_hash")]
        )

        for trade in trades:
            address = trade["address"]
            tx_hash = trade["tx_hash"]
            enriched = enrichment.get(tx_hash.lower(), {})
            market_id = enriched.get("condition_id") or GLOBAL_WHALE_MARKET_ID

            # Save the trade
            self.db.save_whale_trade(
                address=address,
                market_id=market_id,
                market_name=enriched.get("market_name"),
                direction=enriched.get("direction", trade["direction"]),
                amount=trade["amount"],
                price=enriched.get("price", 0.0),
                tx_hash=tx_hash,
            )  # tx_hash uniqueness deduplicates overlapping scans

            # Update wallet profile
            await self._update_wallet_profile(address)

    async def _fetch_trade_enrichment(self, tx_hashes: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Enrich on-chain transfer events with Polymarket trade metadata.
        This resolves the market attribution bug where all trades were tagged
        as unknown.
        """
        if not tx_hashes:
            return {}

        tx_lookup = {tx.lower() for tx in tx_hashes if tx}
        session = await self._get_session()
        params = {"limit": 1000}

        try:
            async with session.get(POLYMARKET_DATA_API, params=params) as response:
                if response.status != 200:
                    return {}
                data = await response.json()
        except Exception as e:
            logger.debug(f"Whale enrichment fetch failed: {e}")
            return {}

        if not isinstance(data, list):
            return {}

        enriched: Dict[str, Dict[str, Any]] = {}
        for row in data:
            tx = (row.get("transactionHash") or "").lower()
            if not tx or tx not in tx_lookup:
                continue

            side = (row.get("side") or "").lower()
            outcome = str(row.get("outcome") or "yes").strip().lower()
            try:
                price = float(row.get("price") or 0.0)
            except (TypeError, ValueError):
                price = 0.0
            enriched[tx] = {
                "condition_id": row.get("conditionId") or "",
                "market_name": row.get("title") or "",
                "direction": f"{side}_yes" if side in {"buy", "sell"} else "buy_yes",
                "price": price,
            }
            if outcome == "no":
                enriched[tx]["direction"] = f"{side}_no" if side in {"buy", "sell"} else "buy_no"

        return enriched

    async def _update_wallet_profile(self, address: str):
        """Update aggregate stats for a wallet."""
        # Get all trades for this wallet from DB
        # For now, use a simple volume sum
        all_trades = self.db.get_recent_whale_trades(
            market_id=None,  # Get all
            minutes=60 * 24 * 30,  # Last 30 days
        )

        wallet_trades = [t for t in all_trades if t.get("address") == address]
        total_volume = sum(t.get("amount", 0) for t in wallet_trades)
        total_count = len(wallet_trades)

        is_whale = total_volume >= self.config.whale_volume_threshold

        self.db.upsert_whale_wallet(
            address=address,
            total_trades=total_count,
            winning_trades=0,  # Would need market resolution data
            total_volume=total_volume,
            is_whale=is_whale,
        )

    def get_recent_whale_activity(
        self,
        market_id: str,
        minutes: int = 60,
    ) -> Dict[str, Any]:
        """
        Check for recent whale activity on a specific market.

        Returns:
            {
                "has_whale_activity": bool,
                "trade_count": int,
                "total_volume": float,
                "top_wallets": [...],
                "smart_money_trades": int,  # Trades from high-winrate wallets
            }
        """
        trades = self.db.get_recent_whale_trades(
            market_id=market_id,
            minutes=minutes,
        )

        # Fallback: global whale flow so meaningful activity is not dropped
        # if market-level mapping is delayed.
        if not trades:
            trades = self.db.get_recent_whale_trades(
                market_id=GLOBAL_WHALE_MARKET_ID,
                minutes=minutes,
            )

        if not trades:
            return {
                "has_whale_activity": False,
                "trade_count": 0,
                "total_volume": 0.0,
                "top_wallets": [],
                "smart_money_trades": 0,
            }

        total_volume = sum(t.get("amount", 0) for t in trades)
        smart_money = [
            t for t in trades
            if t.get("win_rate", 0) >= 60.0
            and t.get("wallet_trades", 0) >= 10
        ]

        # Group by wallet
        wallets: Dict[str, float] = {}
        for t in trades:
            addr = t.get("address", "unknown")
            wallets[addr] = wallets.get(addr, 0) + t.get("amount", 0)

        top_wallets = sorted(wallets.items(), key=lambda x: x[1], reverse=True)[:5]

        return {
            "has_whale_activity": len(trades) > 0,
            "trade_count": len(trades),
            "total_volume": total_volume,
            "top_wallets": [{"address": addr, "volume": vol} for addr, vol in top_wallets],
            "smart_money_trades": len(smart_money),
        }
