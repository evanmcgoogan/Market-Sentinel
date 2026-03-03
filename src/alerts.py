"""
Alert management for Market Sentinel.
Logs alerts with rate limiting and records them to the database.
(SMS via Twilio has been removed; alerts are logged and stored in DB.)
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from models import Alert, utcnow_naive
from config import AlertConfig
from database import Database


logger = logging.getLogger(__name__)


class AlertManager:
    """
    Manages alert delivery with rate limiting and spam protection.
    Rate-limiting state is persisted to the database so it survives restarts.
    """

    def __init__(
        self,
        alert_config: AlertConfig,
        db: Database,
    ):
        self.alert_config = alert_config
        self.db = db
        self._load_rate_limit_state()

    def _load_rate_limit_state(self):
        """Load rate-limiting counters from database (survives restarts)."""
        state = self.db.get_state("alert_rate_limit", default=None)
        now = utcnow_naive()

        if state:
            try:
                self._hour_start = _parse_naive(state["hour_start"]) or now
                self._alerts_this_hour = state["alerts_this_hour"]
                last = state.get("last_alert_time")
                self._last_alert_time = _parse_naive(last) if last else None

                # Reset if the saved hour window has expired
                if (now - self._hour_start).total_seconds() >= 3600:
                    self._alerts_this_hour = 0
                    self._hour_start = now
            except (KeyError, ValueError):
                self._last_alert_time = None
                self._alerts_this_hour = 0
                self._hour_start = now
        else:
            self._last_alert_time = None
            self._alerts_this_hour = 0
            self._hour_start = now

    def _save_rate_limit_state(self):
        """Persist rate-limiting counters to database."""
        self.db.set_state("alert_rate_limit", {
            "hour_start": self._hour_start.isoformat(),
            "alerts_this_hour": self._alerts_this_hour,
            "last_alert_time": self._last_alert_time.isoformat() if self._last_alert_time else None,
        })

    def _check_rate_limits(self) -> bool:
        """Check if we can log an alert (rate limiting). Returns True if allowed."""
        now = utcnow_naive()

        if (now - self._hour_start).total_seconds() >= 3600:
            self._alerts_this_hour = 0
            self._hour_start = now

        if self._alerts_this_hour >= self.alert_config.max_alerts_per_hour:
            logger.warning("Hourly alert limit reached")
            return False

        if self._last_alert_time:
            elapsed = (now - self._last_alert_time).total_seconds()
            if elapsed < self.alert_config.min_seconds_between_alerts:
                logger.debug(f"Rate limit: {elapsed:.0f}s since last alert")
                return False

        return True

    def _check_market_cooldown(self, alert: Alert) -> bool:
        """Check if this specific market is in cooldown."""
        last_alert = self.db.get_last_alert_time(
            alert.market.platform_str,
            alert.market.market_id,
        )

        if last_alert is None:
            return True

        cooldown_minutes = self.alert_config.cooldown_per_market_minutes
        cooldown_end = last_alert + timedelta(minutes=cooldown_minutes)

        now = utcnow_naive()
        if now < cooldown_end:
            remaining = (cooldown_end - now).total_seconds() / 60
            logger.debug(
                f"Market cooldown: {alert.market.name[:30]} ({remaining:.1f}m remaining)"
            )
            return False

        return True

    def can_send_alert(self, alert: Alert) -> bool:
        """Full check if we can log this alert (rate limits + market cooldown)."""
        return self._check_rate_limits() and self._check_market_cooldown(alert)

    def send_alert(self, alert: Alert) -> bool:
        """
        Log alert and record it in the database.
        Returns True if the alert was processed (not rate-limited).
        """
        if not self.can_send_alert(alert):
            return False

        message = alert.format_message()

        logger.info(f"ALERT: {alert.market.name}")
        logger.info(f"Score: {alert.signal_score:.0f}")
        logger.info(f"Reasons: {', '.join(alert.reasons)}")
        print("\n" + "=" * 50)
        print(message)
        print("=" * 50 + "\n")

        # Record alert in database (feeds the dashboard story engine)
        self.db.record_alert(
            platform=alert.market.platform_str,
            market_id=alert.market.market_id,
            market_name=alert.market.name,
            signal_score=alert.signal_score,
            reasons=alert.reasons,
            old_probability=alert.old_probability,
            new_probability=alert.new_probability,
            signal_types=[s.signal_type for s in alert.signals],
            market_category=alert.market.category,
        )

        # Auto-link incoming alert to followed thesis thread, if applicable
        try:
            thesis_key = self.db.link_alert_to_followed_thesis(
                market_name=alert.market.name,
                category=alert.market.category,
                platform=alert.market.platform_str,
                market_id=alert.market.market_id,
                signal_score=alert.signal_score,
                signal_types=[s.signal_type for s in alert.signals],
            )
            if thesis_key:
                logger.info(f"Thesis auto-update applied: {thesis_key}")
        except Exception as e:
            logger.debug(f"Thesis auto-update skipped: {e}")

        self._last_alert_time = utcnow_naive()
        self._alerts_this_hour += 1
        self._save_rate_limit_state()

        return True

    def get_stats(self) -> dict:
        """Get alerting statistics."""
        return {
            "alerts_this_hour": self._alerts_this_hour,
            "max_per_hour": self.alert_config.max_alerts_per_hour,
            "last_alert": self._last_alert_time.isoformat() if self._last_alert_time else None,
        }


def _parse_naive(ts: Optional[str]) -> Optional[datetime]:
    """Parse a naive-UTC ISO string from DB state. Returns None on error."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return None
