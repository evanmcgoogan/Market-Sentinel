#!/usr/bin/env python3
"""
Market Sentinel - Prediction Market Early Warning System

Monitors Polymarket and Kalshi for abnormal market movements.
Sends SMS alerts when signals are detected.

Now with:
- Time-of-day anomaly detection
- Price acceleration (velocity of velocity)
- Order book shape tracking
- News cross-referencing ("no news" flag)
- Whale wallet tracking via Polygon
- Multi-signal correlation scoring

Run with: python main.py
Stop with: Ctrl+C
Test SMS: python main.py --test-sms
"""

import asyncio
import logging
import sys
import signal
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import load_config, Config
from database import Database
from models import Market, Platform, utcnow
from polymarket import PolymarketClient
from kalshi import KalshiClient
from filters import MarketFilter
from signals import SignalDetector, MarketMatcher
from alerts import AlertManager
from news_monitor import NewsMonitor
from whale_tracker import WhaleTracker
from orderbook import OrderBookAnalyzer


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger("sentinel")

# How often to run database cleanup (every 6 hours)
CLEANUP_INTERVAL = timedelta(hours=6)

# How often to log health stats (every 30 minutes)
STATS_INTERVAL = timedelta(minutes=30)

# How often to run performance labeling/metrics jobs
FEEDBACK_INTERVAL = timedelta(minutes=30)

# How often to run full DB compaction
COMPACTION_INTERVAL = timedelta(hours=24)


class MarketSentinel:
    """
    Main monitoring engine.
    Coordinates market fetching, signal detection, and alerting.
    Now integrates order book analysis, news monitoring, and whale tracking.
    """

    def __init__(self, config: Config):
        self.config = config

        # Initialize components
        self.db = Database(config.db_path)
        self.filter = MarketFilter(config.filters)
        self.matcher = MarketMatcher()
        self.alerter = AlertManager(config.twilio, config.alerts, self.db)

        # New components (initialized conditionally)
        self.news_monitor: Optional[NewsMonitor] = None
        self.whale_tracker: Optional[WhaleTracker] = None
        self.orderbook_analyzer: Optional[OrderBookAnalyzer] = None

        if config.news.enabled:
            self.news_monitor = NewsMonitor(config.news, self.db)
            logger.info("News monitor enabled")

        if config.whale.enabled:
            self.whale_tracker = WhaleTracker(config.whale, self.db)
            logger.info("Whale tracker enabled")

        if config.orderbook.enabled:
            self.orderbook_analyzer = OrderBookAnalyzer(config.orderbook, self.db)
            logger.info("Order book analyzer enabled")

        # Signal detector with new components wired in
        self.detector = SignalDetector(
            config=config.signals,
            db=self.db,
            news_monitor=self.news_monitor,
            whale_tracker=self.whale_tracker,
            orderbook_analyzer=self.orderbook_analyzer,
        )

        # API clients (initialized in run loop)
        self.polymarket: Optional[PolymarketClient] = None
        self.kalshi: Optional[KalshiClient] = None

        # State
        self._running = False
        self._last_full_refresh = datetime.min.replace(tzinfo=timezone.utc)
        self._last_cleanup = datetime.min.replace(tzinfo=timezone.utc)
        self._last_stats_log = datetime.min.replace(tzinfo=timezone.utc)
        self._last_feedback = datetime.min.replace(tzinfo=timezone.utc)
        self._last_autotune = datetime.min.replace(tzinfo=timezone.utc)
        self._last_compaction = datetime.min.replace(tzinfo=timezone.utc)
        self._cycle_count = 0
        self._cached_markets: Dict[str, List[Market]] = {
            "polymarket": [],
            "kalshi": [],
        }

        # Set up debug logging if enabled
        if config.debug:
            logging.getLogger().setLevel(logging.DEBUG)

    async def _init_clients(self):
        """Initialize API clients with config values."""
        api = self.config.api
        self.polymarket = PolymarketClient(
            batch_size=api.batch_size,
            max_markets=api.max_markets,
            inter_request_delay=api.inter_request_delay,
            max_retries=api.max_retries,
            retry_base_delay=api.retry_base_delay,
        )
        self.kalshi = KalshiClient(
            batch_size=api.batch_size,
            max_markets=api.max_markets,
            inter_request_delay=api.inter_request_delay,
            max_retries=api.max_retries,
            retry_base_delay=api.retry_base_delay,
        )

    async def _close_clients(self):
        """Clean up API clients and new components."""
        if self.polymarket:
            await self.polymarket.close()
        if self.kalshi:
            await self.kalshi.close()
        if self.news_monitor:
            await self.news_monitor.close()
        if self.whale_tracker:
            await self.whale_tracker.close()

    async def _fetch_markets(self, force_refresh: bool = False) -> Dict[str, List[Market]]:
        """
        Fetch markets from both platforms.
        Uses cached list unless refresh interval has passed.
        """
        now = utcnow()
        refresh_interval = timedelta(minutes=self.config.polling.full_refresh_interval_minutes)

        if not force_refresh and (now - self._last_full_refresh) < refresh_interval:
            return await self._update_prices()

        logger.info("Fetching full market lists...")

        try:
            polymarket_task = self.polymarket.fetch_markets()
            kalshi_task = self.kalshi.fetch_markets()

            polymarket_raw, kalshi_raw = await asyncio.gather(
                polymarket_task,
                kalshi_task,
                return_exceptions=True,
            )

            if isinstance(polymarket_raw, Exception):
                logger.error(f"Polymarket fetch failed: {polymarket_raw}")
                polymarket_raw = []
            if isinstance(kalshi_raw, Exception):
                logger.error(f"Kalshi fetch failed: {kalshi_raw}")
                kalshi_raw = []

            polymarket_filtered = self.filter.filter_markets(polymarket_raw)
            kalshi_filtered = self.filter.filter_markets(kalshi_raw)

            self._cached_markets = {
                "polymarket": polymarket_filtered,
                "kalshi": kalshi_filtered,
            }
            self._last_full_refresh = now

            logger.info(
                f"Monitoring {len(polymarket_filtered)} Polymarket + "
                f"{len(kalshi_filtered)} Kalshi markets"
            )

        except Exception as e:
            logger.error(f"Error fetching markets: {e}")

        return self._cached_markets

    async def _update_prices(self) -> Dict[str, List[Market]]:
        """
        Quick price update for cached markets.
        Runs between full refreshes so minute-level monitoring stays live.
        """
        try:
            poly_markets = self._cached_markets.get("polymarket", [])
            kalshi_markets = self._cached_markets.get("kalshi", [])

            tasks = []
            if self.polymarket and poly_markets:
                tasks.append(self.polymarket.refresh_market_prices(poly_markets))
            if self.kalshi and kalshi_markets:
                tasks.append(self.kalshi.refresh_market_prices(kalshi_markets))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.warning(f"Quick refresh failed, using cached prices: {e}")

        return self._cached_markets

    def _save_snapshots(self, markets: Dict[str, List[Market]]):
        """Save current market state to database."""
        for platform_markets in markets.values():
            for market in platform_markets:
                self.db.save_snapshot(
                    platform=market.platform_str,
                    market_id=market.market_id,
                    market_name=market.name,
                    probability=market.probability,
                    volume=market.volume_total,
                    volume_24h=market.volume_24h,
                    liquidity=market.liquidity,
                    end_date=market.end_date.isoformat() if market.end_date else None,
                    raw_data=market.raw_data,
                )

    def _load_previous_probabilities(self, markets: Dict[str, List[Market]]):
        """Load previous probability from last snapshot for each market."""
        for platform_markets in markets.values():
            for market in platform_markets:
                prev_snapshot = self.db.get_latest_snapshot(
                    market.platform_str,
                    market.market_id,
                )
                if prev_snapshot:
                    market.previous_probability = prev_snapshot["probability"]

    async def _fetch_order_books(self, markets: Dict[str, List[Market]]):
        """Fetch order books for monitored markets (rate-limited)."""
        if not self.orderbook_analyzer or not self.orderbook_analyzer.should_fetch_this_cycle():
            return

        fetched = 0
        max_fetch = self.config.orderbook.max_markets_per_cycle

        # Fetch Polymarket order books
        for market in markets.get("polymarket", [])[:max_fetch]:
            if fetched >= max_fetch:
                break

            # Need token ID for Polymarket CLOB
            tokens = market.raw_data.get("tokens", [])
            if not tokens:
                continue

            # Get the YES token
            token_id = None
            for t in tokens:
                if t.get("outcome") in ["Yes", "YES", "yes", True]:
                    token_id = t.get("token_id")
                    break

            if not token_id:
                token_id = tokens[0].get("token_id") if tokens else None

            if not token_id:
                continue

            try:
                book_data = await self.polymarket.get_order_book(token_id)
                if book_data:
                    self.orderbook_analyzer.parse_polymarket_orderbook(
                        book_data, market.market_id
                    )
                    fetched += 1
                    await asyncio.sleep(0.1)  # Rate limit
            except Exception as e:
                logger.debug(f"Error fetching Polymarket order book: {e}")

        # Fetch Kalshi order books
        for market in markets.get("kalshi", [])[:max_fetch - fetched]:
            if fetched >= max_fetch:
                break

            try:
                book_data = await self.kalshi.get_market_orderbook(market.market_id)
                if book_data:
                    self.orderbook_analyzer.parse_kalshi_orderbook(
                        book_data, market.market_id
                    )
                    fetched += 1
                    await asyncio.sleep(0.1)
            except Exception as e:
                logger.debug(f"Error fetching Kalshi order book: {e}")

        if fetched > 0:
            logger.info(f"Fetched {fetched} order books")

    async def _run_background_tasks(self):
        """Run background tasks: news fetching, whale scanning."""
        tasks = []

        if self.news_monitor:
            tasks.append(self.news_monitor.fetch_news())

        if self.whale_tracker:
            tasks.append(self.whale_tracker.scan_recent_trades())

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.debug(f"Background task error: {result}")

    def _analyze_and_alert(self, markets: Dict[str, List[Market]]):
        """Analyze all markets for signals and send alerts."""
        polymarket = markets.get("polymarket", [])
        kalshi = markets.get("kalshi", [])

        pairs = self.matcher.find_pairs(polymarket, kalshi)
        paired_markets: Dict[str, Market] = {}

        for pair in pairs:
            if pair.polymarket and pair.kalshi:
                paired_markets[pair.polymarket.market_id] = pair.kalshi
                paired_markets[pair.kalshi.market_id] = pair.polymarket

        all_markets = polymarket + kalshi
        alerts_sent = 0

        for market in all_markets:
            try:
                paired = paired_markets.get(market.market_id)
                signals = self.detector.detect_signals(market, paired)

                if not signals:
                    continue

                score = self.detector.calculate_signal_score(signals, market)

                if not self.detector.should_alert(score):
                    continue

                alert = self.detector.create_alert(market, signals, score, paired)

                if self.alerter.send_alert(alert):
                    alerts_sent += 1

            except Exception as e:
                logger.error(f"Error analyzing market {market.name[:30]}: {e}")

        if alerts_sent > 0:
            logger.info(f"Sent {alerts_sent} alerts")

    def _maybe_cleanup(self):
        """Run database cleanup if enough time has passed."""
        now = utcnow()
        if (now - self._last_cleanup) >= CLEANUP_INTERVAL:
            try:
                should_compact = (
                    (now - self._last_compaction) >= COMPACTION_INTERVAL
                    or self.db.get_db_size_bytes() > 1_000_000_000
                )
                self.db.cleanup_old_data(days=7, compact=should_compact)
                self._last_cleanup = now
                if should_compact:
                    self._last_compaction = now
                logger.info("Database cleanup completed")
            except Exception as e:
                logger.error(f"Database cleanup failed: {e}")

    def _maybe_feedback_loop(self):
        """Label outcomes, compute metrics, and optionally auto-tune thresholds."""
        now = utcnow()
        if (now - self._last_feedback) < FEEDBACK_INTERVAL:
            return
        self._last_feedback = now

        try:
            alert_labeling = self.db.label_alert_outcomes(
                horizon_minutes=180,
                success_move_pp=3.0,
                limit=2000,
            )
            move_detection = self.db.detect_market_move_events(
                window_minutes=60,
                min_change_pp=2.0,
                scan_minutes=360,
                per_market_cooldown_minutes=20,
            )
            move_labeling = self.db.label_market_move_outcomes(
                horizon_minutes=180,
                success_move_pp=2.5,
                limit=3000,
            )
            truth_report = self.db.get_truth_engine_report(
                lookback_days=self.config.autotune.lookback_days,
                min_samples=5,
                precision_target=self.config.autotune.target_precision,
                fixed_recall=max(0.20, min(0.80, self.config.autotune.min_recall + 0.10)),
            )
            # Keep compatibility for existing UI/consumers.
            self.db.set_state("signal_performance_metrics", truth_report.get("alerts", {}))
            self.db.set_state("truth_engine_report", truth_report)

            logger.info(
                "Feedback loop: alerts_labeled=%d moves_detected=%d moves_labeled=%d "
                "alert_samples=%d p@fixedR=%.3f ece=%.4f",
                alert_labeling.get("labeled", 0),
                move_detection.get("created", 0),
                move_labeling.get("labeled", 0),
                truth_report.get("alerts", {}).get("sample_size", 0),
                truth_report.get("alerts", {}).get("precision_at_fixed_recall", 0.0),
                truth_report.get("calibration", {}).get("ece", 0.0),
            )

            at = self.config.autotune
            if not at.enabled:
                return
            alert_metrics = truth_report.get("alerts", {})
            if alert_metrics.get("sample_size", 0) < at.min_samples:
                return
            interval = timedelta(minutes=at.interval_minutes)
            if (now - self._last_autotune) < interval:
                return

            updates = self.detector.auto_tune_thresholds(
                performance={
                    "precision": float(alert_metrics.get("precision_at_fixed_recall", 0.0)),
                    "recall": float(alert_metrics.get("recall_at_precision_target", 0.0)),
                },
                target_precision=at.target_precision,
                min_recall=at.min_recall,
                step_fraction=at.step_fraction,
                max_step_fraction=at.max_step_fraction,
            )
            if updates:
                self._last_autotune = now
                self.db.set_state("autotune_last_updates", {
                    "timestamp": now.isoformat(),
                    "updates": updates,
                })
        except Exception as e:
            logger.error(f"Feedback loop failed: {e}")

    def _maybe_log_stats(self):
        """Periodically log health stats."""
        now = utcnow()
        if (now - self._last_stats_log) >= STATS_INTERVAL:
            self._last_stats_log = now

            poly_count = len(self._cached_markets.get("polymarket", []))
            kalshi_count = len(self._cached_markets.get("kalshi", []))
            alert_stats = self.alerter.get_stats()
            recent_alerts_db = self.db.count_recent_alerts(minutes=60)
            news_count = self.db.count_recent_news(hours=4) if self.news_monitor else 0

            logger.info(
                f"Health: cycles={self._cycle_count} | "
                f"markets={poly_count}+{kalshi_count} | "
                f"alerts_this_hour={alert_stats['alerts_this_hour']} | "
                f"alerts_db_1h={recent_alerts_db} | "
                f"news_cached={news_count} | "
                f"twilio={'OK' if alert_stats['twilio_configured'] else 'OFF'}"
            )

    async def _run_cycle(self):
        """Run one monitoring cycle."""
        try:
            self._cycle_count += 1

            # Run background tasks (news, whale scanning)
            await self._run_background_tasks()

            # Fetch/update markets
            markets = await self._fetch_markets()

            if not any(markets.values()):
                logger.warning("No markets to monitor")
                return

            # Load previous probabilities before saving new snapshots
            self._load_previous_probabilities(markets)

            # Save snapshots for historical tracking
            self._save_snapshots(markets)

            # Fetch order books (rate-limited, not every cycle)
            await self._fetch_order_books(markets)

            # Analyze and alert
            self._analyze_and_alert(markets)

            # Periodic maintenance
            self._maybe_cleanup()
            self._maybe_log_stats()
            self._maybe_feedback_loop()

        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}")

    async def run(self):
        """
        Main monitoring loop.
        Runs until stopped with Ctrl+C.
        """
        self._running = True

        # Initialize clients
        await self._init_clients()

        logger.info("=" * 60)
        logger.info("Market Sentinel v2.0 starting...")
        logger.info(f"Poll interval: {self.config.polling.poll_interval_seconds}s")
        logger.info(f"Alert threshold: {self.config.signals.alert_threshold}")
        logger.info(f"News monitor: {'ON' if self.news_monitor else 'OFF'}")
        logger.info(f"Whale tracker: {'ON' if self.whale_tracker else 'OFF'}")
        logger.info(f"Order book analyzer: {'ON' if self.orderbook_analyzer else 'OFF'}")
        logger.info(f"Signals: velocity, volume, thin_liq, divergence, "
                     f"odd_hour, acceleration, orderbook, no_news, whale, correlation")
        logger.info("=" * 60)

        # Initial fetch
        await self._run_cycle()

        # Main loop
        while self._running:
            try:
                await asyncio.sleep(self.config.polling.poll_interval_seconds)
                await self._run_cycle()

            except asyncio.CancelledError:
                logger.info("Monitoring cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                await asyncio.sleep(5)

        # Cleanup
        await self._close_clients()
        logger.info("Market Sentinel stopped")

    def stop(self):
        """Signal the monitoring loop to stop."""
        self._running = False


async def send_test_sms(config: Config):
    """Send a test SMS to verify Twilio is working."""
    from twilio.rest import Client as TwilioClient

    tc = config.twilio
    if not all([tc.account_sid, tc.auth_token, tc.from_number, tc.to_number]):
        print("ERROR: Twilio not fully configured. Check config.json.")
        return False

    try:
        client = TwilioClient(tc.account_sid, tc.auth_token)
        message = client.messages.create(
            body=(
                "🟢 Market Sentinel v2.0 is ONLINE\n"
                "\n"
                "Active signals:\n"
                "• Price velocity\n"
                "• Volume shock\n"
                "• Thin liquidity\n"
                "• Cross-market divergence\n"
                "• Odd-hour activity (NEW)\n"
                "• Price acceleration (NEW)\n"
                "• Order book imbalance (NEW)\n"
                "• No-news flag (NEW)\n"
                "• Whale tracking (NEW)\n"
                "• Multi-signal correlation (NEW)\n"
                "\n"
                "Hunting the sharks. 🦈"
            ),
            from_=tc.from_number,
            to=tc.to_number,
        )
        print(f"✅ Test SMS sent! Message SID: {message.sid}")
        return True
    except Exception as e:
        print(f"❌ Test SMS failed: {e}")
        return False


def setup_signal_handlers(sentinel: MarketSentinel):
    """Set up graceful shutdown on Ctrl+C."""
    def handler(signum, frame):
        logger.info("\nShutdown requested...")
        sentinel.stop()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


async def main():
    """Entry point."""
    # Load configuration
    config = load_config()

    # Check for --test-sms flag
    if "--test-sms" in sys.argv:
        await send_test_sms(config)
        return

    # Create sentinel
    sentinel = MarketSentinel(config)

    # Set up signal handlers
    setup_signal_handlers(sentinel)

    # Run
    await sentinel.run()


if __name__ == "__main__":
    asyncio.run(main())
