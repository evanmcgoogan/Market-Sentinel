"""
Configuration management for Market Sentinel.
All thresholds and settings in one place for easy tuning.
"""

import json
import os
import logging
from dataclasses import dataclass, field
from typing import Optional, List
from pathlib import Path


logger = logging.getLogger(__name__)


@dataclass
class TwilioConfig:
    """Twilio SMS configuration."""
    account_sid: str = ""
    auth_token: str = ""
    from_number: str = ""  # Your Twilio phone number
    to_number: str = ""    # Your personal phone number


@dataclass
class SignalThresholds:
    """
    Thresholds for signal detection.
    Lower = more sensitive (noisier, earlier alerts).
    Higher = less sensitive (fewer alerts, may miss early signals).
    """
    # Price velocity: minimum probability change to trigger (0-100 scale)
    price_velocity_min_change: float = 5.0  # 5 percentage points
    price_velocity_time_window_minutes: int = 30

    # Volume shock: multiplier over baseline to trigger
    volume_shock_multiplier: float = 3.0  # 3x normal volume
    volume_baseline_hours: int = 24

    # Thin liquidity: max volume for "thin" classification
    thin_liquidity_max_volume: float = 10000.0  # dollars
    thin_liquidity_min_price_change: float = 3.0  # percentage points

    # Cross-market divergence: min difference between platforms
    cross_market_divergence_threshold: float = 8.0  # percentage points

    # Late-stage boost: days before resolution to start boosting
    late_stage_days_threshold: int = 7
    late_stage_multiplier: float = 1.5

    # Overall signal score threshold for alert (0-100)
    alert_threshold: float = 40.0  # Aggressive default

    # === NEW: Time-of-day anomaly detection ===
    # Multiplier over hourly baseline to flag odd-hour activity
    odd_hour_volume_multiplier: float = 3.0
    # Minimum samples needed before hourly baseline is reliable
    odd_hour_min_baseline_samples: int = 5
    # Hours considered "off-peak" for US political markets (UTC)
    # 3am-7am EST = 8-12 UTC
    off_peak_hours_utc: list = field(default_factory=lambda: [3, 4, 5, 6, 7, 8, 9, 10, 11])
    # Extra weight for activity during off-peak hours
    off_peak_bonus_multiplier: float = 1.5

    # === NEW: Acceleration (velocity of velocity) ===
    # Minimum acceleration (pp/min^2) to trigger
    acceleration_min_threshold: float = 0.5
    # Time window for measuring acceleration (needs 3+ snapshots)
    acceleration_window_minutes: int = 30

    # === NEW: Order book imbalance ===
    # Bid/ask ratio threshold for imbalance (>2.0 = 2x more bids than asks)
    orderbook_imbalance_threshold: float = 2.0
    # Minimum spread narrowing vs baseline to flag
    orderbook_spread_tightening_pct: float = 50.0  # 50% tighter than baseline
    # Max strength for orderbook signals
    orderbook_max_strength: float = 25.0

    # === NEW: No-news flag ===
    # Hours to look back for related news
    no_news_lookback_hours: int = 4
    # Minimum price change to care about "no news" (if market didn't move, no-news is irrelevant)
    no_news_min_price_change: float = 3.0
    # Strength bonus when significant move has zero news coverage
    no_news_strength: float = 20.0

    # === NEW: Whale activity ===
    # Minimum USD for a trade to be "whale-sized"
    whale_min_trade_usd: float = 5000.0
    # Minimum wallet win rate to flag as "smart money"
    whale_min_win_rate: float = 60.0
    # Minimum total trades for win rate to be meaningful
    whale_min_trades_for_winrate: int = 10
    # Strength for whale activity signal
    whale_signal_strength: float = 25.0

    # === NEW: Multi-signal correlation ===
    # Bonus multiplier when 3+ signals fire simultaneously
    correlation_3_signal_bonus: float = 1.3  # 30% boost
    # Bonus multiplier when 4+ signals fire simultaneously
    correlation_4_signal_bonus: float = 1.6  # 60% boost
    # Bonus multiplier when 5+ signals fire simultaneously
    correlation_5_signal_bonus: float = 2.0  # 100% boost (double)


@dataclass
class MarketFilterConfig:
    """Keywords and categories for filtering markets."""

    # Keywords that INCLUDE a market (case-insensitive)
    include_keywords: list = field(default_factory=lambda: [
        # Politics & Elections
        "election", "president", "congress", "senate", "house", "vote",
        "democrat", "republican", "biden", "trump", "governor", "poll",
        "primary", "nominee", "cabinet", "impeach", "legislation",

        # Geopolitics & International
        "china", "russia", "ukraine", "taiwan", "nato", "eu", "european",
        "iran", "israel", "palestine", "gaza", "korea", "japan", "india",
        "sanctions", "treaty", "diplomacy", "summit", "g7", "g20", "un",
        "united nations", "ambassador", "invasion", "annex",

        # Wars & Conflicts
        "war", "military", "invasion", "conflict", "attack", "missile",
        "nuclear", "troops", "army", "navy", "defense", "weapons",
        "ceasefire", "peace", "casualties", "drone", "strike",

        # AI & Tech
        "ai", "artificial intelligence", "openai", "anthropic", "deepmind",
        "google ai", "microsoft ai", "gpt", "llm", "agi", "regulation",
        "semiconductor", "chip", "nvidia", "tsmc", "tech", "crypto",
        "bitcoin", "ethereum", "sec", "ftc", "antitrust",

        # Markets & Economics
        "fed", "federal reserve", "interest rate", "inflation", "gdp",
        "recession", "stock", "s&p", "nasdaq", "dow", "treasury",
        "bond", "yield", "unemployment", "jobs", "economy", "tariff",
        "trade", "oil", "commodity", "dollar", "currency", "default",

        # Private Market Investing
        "ipo", "venture capital", "venture", "private equity", "spac",
        "merger", "acquisition", "m&a", "unicorn", "startup", "valuation",
        "fundraise", "fundraising", "series a", "series b", "series c",
        "pre-ipo", "secondary market", "carried interest", "buyout",
        "vc", "pe fund", "blackstone", "kkr", "apollo", "carlyle",
    ])

    # Keywords that EXCLUDE a market (case-insensitive)
    exclude_keywords: list = field(default_factory=lambda: [
        # Sports — leagues
        "nfl", "nba", "mlb", "nhl", "mls", "ufc", "wwe",
        "premier league", "la liga", "serie a", "bundesliga",
        "champions league", "europa league", "fa cup", "ligue 1",
        "super bowl", "world cup", "olympics", "formula 1", " f1 ",
        "grand prix", "wimbledon", "us open tennis",
        # Sports — generic terms
        "basketball", "baseball", "hockey", "tennis", "golf",
        "football game", "playoff", "championship game", "finals mvp",
        "espn", "sports", "athlete", "head coach", "score",
        # Sports — European clubs (most commonly appearing in prediction markets)
        "barcelona", "real madrid", "manchester city", "manchester united",
        "liverpool fc", "arsenal fc", "chelsea fc", "tottenham hotspur",
        "atletico madrid", "athletic bilbao", "mallorca", "sevilla",
        "juventus", "inter milan", "ac milan", "napoli", "roma",
        "bayern munich", "borussia dortmund", "bayer leverkusen",
        "paris saint-germain", "ajax", "psv eindhoven",
        "win the league", "win the cup", "win the title",
        "golden boot", "ballon d'or",

        # Entertainment
        "movie", "film", "oscar", "grammy", "emmy", "tony award",
        "box office", "netflix series", "disney plus", "streaming show",
        "tv show", "album release", "concert tour", "billboard chart",

        # Pop Culture & Celebrity
        "kardashian", "taylor swift", "beyonce", "drake", "kanye",
        "influencer", "viral", "reality tv", "bachelor",
        "love island", "survivor", "big brother",

        # Other exclusions
        "weather", "hurricane", "earthquake", "will it rain",
    ])

    # Category tags to include (platform-specific)
    include_categories: list = field(default_factory=lambda: [
        "politics", "geopolitics", "economics", "finance", "technology",
        "world", "us-politics", "crypto", "science", "business",
    ])

    # Category tags to exclude
    exclude_categories: list = field(default_factory=lambda: [
        "sports", "entertainment", "culture", "lifestyle", "gaming",
    ])


@dataclass
class PollingConfig:
    """How often to check markets."""
    poll_interval_seconds: int = 60  # Check every minute
    full_refresh_interval_minutes: int = 15  # Full market list refresh


@dataclass
class AlertConfig:
    """Alert rate limiting and formatting."""
    min_seconds_between_alerts: int = 60  # Basic spam protection
    max_alerts_per_hour: int = 20  # Hard cap
    cooldown_per_market_minutes: int = 10  # Don't re-alert same market


@dataclass
class APIConfig:
    """API client parameters."""
    batch_size: int = 100           # Markets per API request
    max_markets: int = 500          # Max total markets to fetch
    inter_request_delay: float = 0.2  # Seconds between paginated requests
    max_retries: int = 3            # Max retries on rate limit
    retry_base_delay: float = 5.0   # Base delay for retry backoff (seconds)


@dataclass
class OrderBookConfig:
    """Order book fetching configuration."""
    enabled: bool = True
    # Fetch order books every N cycles (not every cycle, to avoid rate limits)
    fetch_every_n_cycles: int = 5
    # Maximum markets to fetch order books for (prioritize high-signal markets)
    max_markets_per_cycle: int = 50


@dataclass
class NewsConfig:
    """News monitoring configuration."""
    enabled: bool = True
    # Free news sources (no API key needed)
    rss_feeds: list = field(default_factory=lambda: [
        # Major wire services and political news
        "https://feeds.reuters.com/reuters/topNews",
        "https://feeds.reuters.com/reuters/worldNews",
        "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
        "https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml",
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://www.cnbc.com/id/100003114/device/rss/rss.html",  # US news
        "https://feeds.skynews.com/feeds/rss/technology.xml",  # Tech
    ])
    # How often to refresh news (minutes)
    refresh_interval_minutes: int = 10
    # Optional: NewsAPI key for richer data (free tier = 100 req/day)
    newsapi_key: str = ""


@dataclass
class WhaleConfig:
    """Whale wallet tracking configuration."""
    enabled: bool = True
    # Polygon RPC endpoint (free public endpoint)
    polygon_rpc_url: str = "https://polygon-rpc.com"
    # Polymarket CTF Exchange contract on Polygon
    ctf_exchange_address: str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
    # How often to scan for new whale trades (minutes)
    scan_interval_minutes: int = 5
    # Minimum trade size in USDC to track
    min_trade_size_usdc: float = 5000.0
    # Number of recent blocks to scan per cycle
    blocks_per_scan: int = 100
    # Minimum total volume to classify as whale
    whale_volume_threshold: float = 50000.0


@dataclass
class AutoTuneConfig:
    """Adaptive threshold tuning from labeled alert outcomes."""
    enabled: bool = True
    interval_minutes: int = 180
    lookback_days: int = 14
    min_samples: int = 30
    target_precision: float = 0.60
    min_recall: float = 0.30
    step_fraction: float = 0.05
    max_step_fraction: float = 0.15


@dataclass
class Config:
    """Master configuration container."""
    twilio: TwilioConfig = field(default_factory=TwilioConfig)
    signals: SignalThresholds = field(default_factory=SignalThresholds)
    filters: MarketFilterConfig = field(default_factory=MarketFilterConfig)
    polling: PollingConfig = field(default_factory=PollingConfig)
    alerts: AlertConfig = field(default_factory=AlertConfig)
    api: APIConfig = field(default_factory=APIConfig)
    orderbook: OrderBookConfig = field(default_factory=OrderBookConfig)
    news: NewsConfig = field(default_factory=NewsConfig)
    whale: WhaleConfig = field(default_factory=WhaleConfig)
    autotune: AutoTuneConfig = field(default_factory=AutoTuneConfig)

    # Database path
    db_path: str = "market_sentinel.db"

    # Debug mode (more logging)
    debug: bool = False

    # Anthropic API key for Claude-powered headlines and intelligence notes
    anthropic_api_key: str = ""


def _validate_config(config: Config) -> List[str]:
    """
    Validate configuration values are sensible.
    Returns list of warning messages (empty if all OK).
    """
    warnings = []

    # Signal thresholds
    s = config.signals
    if not (0 < s.price_velocity_min_change <= 50):
        warnings.append(
            f"price_velocity_min_change={s.price_velocity_min_change} "
            f"outside recommended range (0, 50]"
        )
    if not (1 <= s.price_velocity_time_window_minutes <= 1440):
        warnings.append(
            f"price_velocity_time_window_minutes={s.price_velocity_time_window_minutes} "
            f"outside recommended range [1, 1440]"
        )
    if not (1.0 < s.volume_shock_multiplier <= 100):
        warnings.append(
            f"volume_shock_multiplier={s.volume_shock_multiplier} "
            f"outside recommended range (1, 100]"
        )
    if not (0 < s.alert_threshold <= 100):
        warnings.append(
            f"alert_threshold={s.alert_threshold} "
            f"outside valid range (0, 100]"
        )
    if not (1.0 <= s.late_stage_multiplier <= 5.0):
        warnings.append(
            f"late_stage_multiplier={s.late_stage_multiplier} "
            f"outside recommended range [1.0, 5.0]"
        )

    # New signal validations
    if not (1.0 <= s.odd_hour_volume_multiplier <= 50):
        warnings.append(
            f"odd_hour_volume_multiplier={s.odd_hour_volume_multiplier} "
            f"outside recommended range [1.0, 50]"
        )
    if not (0.1 <= s.acceleration_min_threshold <= 10):
        warnings.append(
            f"acceleration_min_threshold={s.acceleration_min_threshold} "
            f"outside recommended range [0.1, 10]"
        )
    if not (1.0 <= s.orderbook_imbalance_threshold <= 20):
        warnings.append(
            f"orderbook_imbalance_threshold={s.orderbook_imbalance_threshold} "
            f"outside recommended range [1.0, 20]"
        )
    if not (1.0 <= s.correlation_3_signal_bonus <= 5.0):
        warnings.append(
            f"correlation_3_signal_bonus={s.correlation_3_signal_bonus} "
            f"outside recommended range [1.0, 5.0]"
        )

    # Polling config
    p = config.polling
    if p.poll_interval_seconds < 10:
        warnings.append(
            f"poll_interval_seconds={p.poll_interval_seconds} is very low, "
            f"may hit API rate limits"
        )

    # Alert config
    a = config.alerts
    if a.max_alerts_per_hour > 60:
        warnings.append(
            f"max_alerts_per_hour={a.max_alerts_per_hour} is very high, "
            f"may cause SMS costs"
        )

    # API config
    api = config.api
    if api.max_retries < 1:
        warnings.append(f"max_retries={api.max_retries} is too low, setting to 1")
        api.max_retries = 1

    at = config.autotune
    if not (0.1 <= at.target_precision <= 0.95):
        warnings.append(
            f"target_precision={at.target_precision} outside recommended range [0.1, 0.95]"
        )
    if not (0.05 <= at.min_recall <= 0.9):
        warnings.append(
            f"min_recall={at.min_recall} outside recommended range [0.05, 0.9]"
        )
    if not (0.01 <= at.step_fraction <= at.max_step_fraction <= 0.25):
        warnings.append(
            "auto-tune step settings invalid: expected "
            "0.01 <= step_fraction <= max_step_fraction <= 0.25"
        )

    # Twilio: warn if partially configured
    t = config.twilio
    twilio_fields = [t.account_sid, t.auth_token, t.from_number, t.to_number]
    filled = sum(1 for f in twilio_fields if f and not f.startswith("YOUR_"))
    if 0 < filled < 4:
        warnings.append(
            "Twilio is partially configured — all 4 fields "
            "(account_sid, auth_token, from_number, to_number) are needed for SMS"
        )

    return warnings


def load_config(config_path: Optional[str] = None) -> Config:
    """
    Load configuration from JSON file.
    Falls back to defaults if file doesn't exist.
    Validates values and logs warnings for anything unusual.
    """
    if config_path is None:
        config_path = os.environ.get("SENTINEL_CONFIG", "config.json")

    config = Config()

    if Path(config_path).exists():
        with open(config_path, "r") as f:
            data = json.load(f)

        # Load Twilio config
        if "twilio" in data:
            config.twilio = TwilioConfig(**data["twilio"])

        # Load signal thresholds (merge with defaults for new fields)
        if "signals" in data:
            defaults = SignalThresholds()
            signal_data = data["signals"]
            # Only set fields that exist in the dataclass
            valid_fields = {f.name for f in defaults.__dataclass_fields__.values()}
            filtered = {k: v for k, v in signal_data.items() if k in valid_fields}
            config.signals = SignalThresholds(**filtered)

        # Load filter config
        if "filters" in data:
            fc = data["filters"]
            config.filters = MarketFilterConfig(
                include_keywords=fc.get("include_keywords", config.filters.include_keywords),
                exclude_keywords=fc.get("exclude_keywords", config.filters.exclude_keywords),
                include_categories=fc.get("include_categories", config.filters.include_categories),
                exclude_categories=fc.get("exclude_categories", config.filters.exclude_categories),
            )

        # Load polling config
        if "polling" in data:
            config.polling = PollingConfig(**data["polling"])

        # Load alert config
        if "alerts" in data:
            config.alerts = AlertConfig(**data["alerts"])

        # Load API config
        if "api" in data:
            config.api = APIConfig(**data["api"])

        # Load new configs
        if "orderbook" in data:
            config.orderbook = OrderBookConfig(**data["orderbook"])

        if "news" in data:
            news_data = data["news"]
            valid_fields = {f.name for f in NewsConfig.__dataclass_fields__.values()}
            filtered = {k: v for k, v in news_data.items() if k in valid_fields}
            config.news = NewsConfig(**filtered)

        if "whale" in data:
            whale_data = data["whale"]
            valid_fields = {f.name for f in WhaleConfig.__dataclass_fields__.values()}
            filtered = {k: v for k, v in whale_data.items() if k in valid_fields}
            config.whale = WhaleConfig(**filtered)

        if "autotune" in data:
            autotune_data = data["autotune"]
            valid_fields = {f.name for f in AutoTuneConfig.__dataclass_fields__.values()}
            filtered = {k: v for k, v in autotune_data.items() if k in valid_fields}
            config.autotune = AutoTuneConfig(**filtered)

        # Load top-level settings
        config.db_path = data.get("db_path", config.db_path)
        config.debug = data.get("debug", config.debug)
        config.anthropic_api_key = data.get("anthropic_api_key", "")

        logger.info(f"Loaded config from {config_path}")
    else:
        logger.info(f"No config file at {config_path}, using defaults")

    # Override Twilio from environment variables if set
    config.twilio.account_sid = os.environ.get("TWILIO_ACCOUNT_SID", config.twilio.account_sid)
    config.twilio.auth_token = os.environ.get("TWILIO_AUTH_TOKEN", config.twilio.auth_token)
    config.twilio.from_number = os.environ.get("TWILIO_FROM_NUMBER", config.twilio.from_number)
    config.twilio.to_number = os.environ.get("TWILIO_TO_NUMBER", config.twilio.to_number)

    # Override news API key from env
    config.news.newsapi_key = os.environ.get("NEWSAPI_KEY", config.news.newsapi_key)
    # Override DB path from env (useful for hosted deployments with mounted disks)
    config.db_path = os.environ.get("SENTINEL_DB_PATH", config.db_path)

    # Validate and log warnings
    warnings = _validate_config(config)
    for w in warnings:
        logger.warning(f"Config warning: {w}")

    return config


def save_default_config(path: str = "config.example.json"):
    """Save a default config file as an example."""
    config = Config()

    data = {
        "twilio": {
            "account_sid": "YOUR_TWILIO_ACCOUNT_SID",
            "auth_token": "YOUR_TWILIO_AUTH_TOKEN",
            "from_number": "+1234567890",
            "to_number": "+0987654321",
        },
        "signals": {
            "price_velocity_min_change": config.signals.price_velocity_min_change,
            "price_velocity_time_window_minutes": config.signals.price_velocity_time_window_minutes,
            "volume_shock_multiplier": config.signals.volume_shock_multiplier,
            "volume_baseline_hours": config.signals.volume_baseline_hours,
            "thin_liquidity_max_volume": config.signals.thin_liquidity_max_volume,
            "thin_liquidity_min_price_change": config.signals.thin_liquidity_min_price_change,
            "cross_market_divergence_threshold": config.signals.cross_market_divergence_threshold,
            "late_stage_days_threshold": config.signals.late_stage_days_threshold,
            "late_stage_multiplier": config.signals.late_stage_multiplier,
            "alert_threshold": config.signals.alert_threshold,
            "odd_hour_volume_multiplier": config.signals.odd_hour_volume_multiplier,
            "acceleration_min_threshold": config.signals.acceleration_min_threshold,
            "orderbook_imbalance_threshold": config.signals.orderbook_imbalance_threshold,
            "no_news_strength": config.signals.no_news_strength,
            "whale_signal_strength": config.signals.whale_signal_strength,
            "correlation_3_signal_bonus": config.signals.correlation_3_signal_bonus,
            "correlation_4_signal_bonus": config.signals.correlation_4_signal_bonus,
            "correlation_5_signal_bonus": config.signals.correlation_5_signal_bonus,
        },
        "polling": {
            "poll_interval_seconds": config.polling.poll_interval_seconds,
            "full_refresh_interval_minutes": config.polling.full_refresh_interval_minutes,
        },
        "alerts": {
            "min_seconds_between_alerts": config.alerts.min_seconds_between_alerts,
            "max_alerts_per_hour": config.alerts.max_alerts_per_hour,
            "cooldown_per_market_minutes": config.alerts.cooldown_per_market_minutes,
        },
        "api": {
            "batch_size": config.api.batch_size,
            "max_markets": config.api.max_markets,
            "inter_request_delay": config.api.inter_request_delay,
            "max_retries": config.api.max_retries,
            "retry_base_delay": config.api.retry_base_delay,
        },
        "orderbook": {
            "enabled": config.orderbook.enabled,
            "fetch_every_n_cycles": config.orderbook.fetch_every_n_cycles,
            "max_markets_per_cycle": config.orderbook.max_markets_per_cycle,
        },
        "news": {
            "enabled": config.news.enabled,
            "refresh_interval_minutes": config.news.refresh_interval_minutes,
            "newsapi_key": "",
        },
        "whale": {
            "enabled": config.whale.enabled,
            "min_trade_size_usdc": config.whale.min_trade_size_usdc,
            "scan_interval_minutes": config.whale.scan_interval_minutes,
            "whale_volume_threshold": config.whale.whale_volume_threshold,
        },
        "autotune": {
            "enabled": config.autotune.enabled,
            "interval_minutes": config.autotune.interval_minutes,
            "lookback_days": config.autotune.lookback_days,
            "min_samples": config.autotune.min_samples,
            "target_precision": config.autotune.target_precision,
            "min_recall": config.autotune.min_recall,
            "step_fraction": config.autotune.step_fraction,
            "max_step_fraction": config.autotune.max_step_fraction,
        },
        "db_path": config.db_path,
        "debug": config.debug,
    }

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    return path
