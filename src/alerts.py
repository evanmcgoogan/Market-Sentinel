"""
SMS alerting via Twilio.
Sends text messages when signals are detected.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from twilio.rest import Client as TwilioClient
from twilio.base.exceptions import TwilioRestException

from models import Alert
from config import TwilioConfig, AlertConfig
from database import Database


logger = logging.getLogger(__name__)


class AlertManager:
    """
    Manages alert delivery with rate limiting and spam protection.
    Rate-limiting state is persisted to the database so it survives restarts.
    """

    def __init__(
        self,
        twilio_config: TwilioConfig,
        alert_config: AlertConfig,
        db: Database,
    ):
        self.twilio_config = twilio_config
        self.alert_config = alert_config
        self.db = db

        # Initialize Twilio client if configured
        self._twilio: Optional[TwilioClient] = None
        if self._is_twilio_configured():
            try:
                self._twilio = TwilioClient(
                    twilio_config.account_sid,
                    twilio_config.auth_token,
                )
                logger.info("Twilio client initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Twilio: {e}")

        # Load persisted rate-limiting state from DB
        self._load_rate_limit_state()

    def _load_rate_limit_state(self):
        """Load rate-limiting counters from database (survives restarts)."""
        state = self.db.get_state("alert_rate_limit", default=None)
        now = datetime.utcnow()

        if state:
            try:
                self._hour_start = datetime.fromisoformat(state["hour_start"])
                self._alerts_this_hour = state["alerts_this_hour"]
                last = state.get("last_alert_time")
                self._last_alert_time = datetime.fromisoformat(last) if last else None

                # Reset if the saved hour window has expired
                if (now - self._hour_start).total_seconds() >= 3600:
                    self._alerts_this_hour = 0
                    self._hour_start = now
            except (KeyError, ValueError):
                # Corrupted state, reset
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

    def _is_twilio_configured(self) -> bool:
        """Check if Twilio credentials are set."""
        return bool(
            self.twilio_config.account_sid and
            self.twilio_config.auth_token and
            self.twilio_config.from_number and
            self.twilio_config.to_number
        )

    def _check_rate_limits(self) -> bool:
        """
        Check if we can send an alert (rate limiting).
        Returns True if alert is allowed.
        """
        now = datetime.utcnow()

        # Reset hourly counter if needed
        if (now - self._hour_start).total_seconds() >= 3600:
            self._alerts_this_hour = 0
            self._hour_start = now

        # Check hourly limit
        if self._alerts_this_hour >= self.alert_config.max_alerts_per_hour:
            logger.warning("Hourly alert limit reached")
            return False

        # Check minimum time between alerts
        if self._last_alert_time:
            elapsed = (now - self._last_alert_time).total_seconds()
            if elapsed < self.alert_config.min_seconds_between_alerts:
                logger.debug(f"Rate limit: {elapsed:.0f}s since last alert")
                return False

        return True

    def _check_market_cooldown(self, alert: Alert) -> bool:
        """
        Check if this specific market is in cooldown.
        Prevents spamming about the same market.
        """
        last_alert = self.db.get_last_alert_time(
            alert.market.platform_str,
            alert.market.market_id,
        )

        if last_alert is None:
            return True

        cooldown_minutes = self.alert_config.cooldown_per_market_minutes
        cooldown_end = last_alert + timedelta(minutes=cooldown_minutes)

        if datetime.utcnow() < cooldown_end:
            remaining = (cooldown_end - datetime.utcnow()).total_seconds() / 60
            logger.debug(
                f"Market cooldown: {alert.market.name[:30]} "
                f"({remaining:.1f}m remaining)"
            )
            return False

        return True

    def can_send_alert(self, alert: Alert) -> bool:
        """
        Full check if we can send this alert.
        Combines rate limits and market cooldown.
        """
        if not self._check_rate_limits():
            return False

        if not self._check_market_cooldown(alert):
            return False

        return True

    def send_alert(self, alert: Alert) -> bool:
        """
        Send an SMS alert.
        Returns True if sent successfully.
        """
        if not self.can_send_alert(alert):
            return False

        message_body = alert.format_sms()

        # Log the alert (always, even if Twilio not configured)
        logger.info(f"ALERT: {alert.market.name}")
        logger.info(f"Score: {alert.signal_score:.0f}")
        logger.info(f"Reasons: {', '.join(alert.reasons)}")

        # Send SMS if Twilio is configured
        if self._twilio:
            try:
                message = self._twilio.messages.create(
                    body=message_body,
                    from_=self.twilio_config.from_number,
                    to=self.twilio_config.to_number,
                )
                logger.info(f"SMS sent: {message.sid}")

            except TwilioRestException as e:
                logger.error(f"Twilio error: {e}")
                # Still record the alert attempt
            except Exception as e:
                logger.error(f"Failed to send SMS: {e}")
        else:
            # Print to console if no Twilio
            print("\n" + "=" * 50)
            print(message_body)
            print("=" * 50 + "\n")

        # Record alert in database
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

        # Auto-link incoming alert to followed thesis thread, if applicable.
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

        # Update rate limiting state
        self._last_alert_time = datetime.utcnow()
        self._alerts_this_hour += 1

        # Persist to DB so state survives restart
        self._save_rate_limit_state()

        return True

    def get_stats(self) -> dict:
        """Get alerting statistics."""
        return {
            "alerts_this_hour": self._alerts_this_hour,
            "max_per_hour": self.alert_config.max_alerts_per_hour,
            "last_alert": self._last_alert_time.isoformat() if self._last_alert_time else None,
            "twilio_configured": self._is_twilio_configured(),
        }
