"""Tests for configuration management."""

import os
import sys
import json
import tempfile
import unittest

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import (
    Config, SignalThresholds, TwilioConfig, AlertConfig, APIConfig,
    PollingConfig, load_config, _validate_config,
)


class TestConfigValidation(unittest.TestCase):
    """Test configuration validation."""

    def test_default_config_is_valid(self):
        """Test that default config passes validation."""
        config = Config()
        warnings = _validate_config(config)
        self.assertEqual(len(warnings), 0)

    def test_warns_on_bad_alert_threshold(self):
        """Test warning for alert_threshold out of range."""
        config = Config()
        config.signals.alert_threshold = -5.0
        warnings = _validate_config(config)
        self.assertTrue(any("alert_threshold" in w for w in warnings))

    def test_warns_on_extreme_threshold(self):
        """Test warning for extreme values."""
        config = Config()
        config.signals.alert_threshold = 150.0
        warnings = _validate_config(config)
        self.assertTrue(any("alert_threshold" in w for w in warnings))

    def test_warns_on_low_poll_interval(self):
        """Test warning for very low poll interval."""
        config = Config()
        config.polling.poll_interval_seconds = 5
        warnings = _validate_config(config)
        self.assertTrue(any("poll_interval" in w for w in warnings))

    def test_warns_on_high_alerts_per_hour(self):
        """Test warning for high alerts per hour."""
        config = Config()
        config.alerts.max_alerts_per_hour = 100
        warnings = _validate_config(config)
        self.assertTrue(any("max_alerts_per_hour" in w for w in warnings))

    def test_warns_on_partial_twilio(self):
        """Test warning when Twilio is partially configured."""
        config = Config()
        config.twilio.account_sid = "AC123456"
        # Other fields empty
        warnings = _validate_config(config)
        self.assertTrue(any("Twilio" in w for w in warnings))

    def test_no_warn_on_full_twilio(self):
        """Test no warning when Twilio is fully configured."""
        config = Config()
        config.twilio = TwilioConfig(
            account_sid="AC123",
            auth_token="token",
            from_number="+1111",
            to_number="+2222",
        )
        warnings = _validate_config(config)
        self.assertFalse(any("Twilio" in w for w in warnings))

    def test_fixes_low_max_retries(self):
        """Test that max_retries < 1 gets corrected."""
        config = Config()
        config.api.max_retries = 0
        _validate_config(config)
        self.assertEqual(config.api.max_retries, 1)

    def test_warns_on_bad_volume_shock_multiplier(self):
        """Test warning for volume shock multiplier <= 1."""
        config = Config()
        config.signals.volume_shock_multiplier = 0.5
        warnings = _validate_config(config)
        self.assertTrue(any("volume_shock_multiplier" in w for w in warnings))


class TestConfigLoading(unittest.TestCase):
    """Test config loading from files."""

    def test_load_nonexistent_file_uses_defaults(self):
        """Test loading from nonexistent path uses defaults."""
        config = load_config("/tmp/nonexistent_sentinel_config.json")
        self.assertAlmostEqual(config.signals.alert_threshold, 40.0)
        self.assertEqual(config.polling.poll_interval_seconds, 60)

    def test_load_from_file(self):
        """Test loading config from a JSON file."""
        data = {
            "signals": {"alert_threshold": 75.0},
            "polling": {"poll_interval_seconds": 120},
            "api": {"max_retries": 5},
            "debug": True,
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(data, f)
            path = f.name

        try:
            config = load_config(path)
            self.assertAlmostEqual(config.signals.alert_threshold, 75.0)
            self.assertEqual(config.polling.poll_interval_seconds, 120)
            self.assertEqual(config.api.max_retries, 5)
            self.assertTrue(config.debug)
        finally:
            os.unlink(path)

    def test_env_overrides_twilio(self):
        """Test that environment variables override Twilio config."""
        os.environ["TWILIO_ACCOUNT_SID"] = "env_sid"
        try:
            config = load_config("/tmp/nonexistent.json")
            self.assertEqual(config.twilio.account_sid, "env_sid")
        finally:
            del os.environ["TWILIO_ACCOUNT_SID"]

    def test_api_config_defaults(self):
        """Test API config has sensible defaults."""
        config = Config()
        self.assertEqual(config.api.batch_size, 100)
        self.assertEqual(config.api.max_markets, 500)
        self.assertEqual(config.api.max_retries, 3)
        self.assertAlmostEqual(config.api.retry_base_delay, 5.0)


if __name__ == "__main__":
    unittest.main()
