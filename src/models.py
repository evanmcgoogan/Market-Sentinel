"""
Data models for market data across platforms.
Provides a unified interface regardless of source (Polymarket/Kalshi).
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from enum import Enum


def utcnow() -> datetime:
    """Return timezone-aware UTC now."""
    return datetime.now(timezone.utc)


def utcnow_naive() -> datetime:
    """Return naive UTC datetime, format-compatible with existing DB strings."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def utcnow_str() -> str:
    """Return naive UTC ISO string for SQLite storage (matches existing DB format)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")


def ensure_aware(dt: datetime) -> datetime:
    """Ensure a datetime is timezone-aware (UTC). Handles naive datetimes by assuming UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


class Platform(Enum):
    """Supported prediction market platforms."""
    POLYMARKET = "polymarket"
    KALSHI = "kalshi"


@dataclass
class Market:
    """
    Unified market representation.
    Normalizes data from different platforms into a common format.
    """
    # Identifiers
    platform: Platform
    market_id: str
    slug: str  # URL-friendly identifier

    # Basic info
    name: str
    description: str = ""
    category: str = ""
    tags: List[str] = field(default_factory=list)

    # Current state (probability 0-100)
    probability: float = 50.0
    previous_probability: Optional[float] = None

    # Volume data (in USD)
    volume_total: float = 0.0
    volume_24h: float = 0.0
    liquidity: float = 0.0  # Order book depth

    # Timing
    end_date: Optional[datetime] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None

    # Platform-specific raw data
    raw_data: Dict[str, Any] = field(default_factory=dict)

    @property
    def platform_str(self) -> str:
        return self.platform.value

    @property
    def days_until_resolution(self) -> Optional[float]:
        """Days until market resolves, if end date is known."""
        if self.end_date is None:
            return None
        # Ensure both datetimes are timezone-aware to avoid naive/aware mismatch
        end = ensure_aware(self.end_date)
        now = utcnow()
        delta = end - now
        return max(0, delta.total_seconds() / 86400)

    @property
    def is_near_resolution(self) -> bool:
        """True if market resolves within 7 days."""
        days = self.days_until_resolution
        return days is not None and days <= 7

    @property
    def price_change(self) -> Optional[float]:
        """Change in probability from previous snapshot."""
        if self.previous_probability is None:
            return None
        return self.probability - self.previous_probability

    def __str__(self) -> str:
        return f"[{self.platform_str}] {self.name} @ {self.probability:.1f}%"


@dataclass
class Signal:
    """
    A detected signal/anomaly in market data.
    """
    # What triggered
    signal_type: str  # 'price_velocity', 'volume_shock', etc.
    description: str  # Human-readable explanation

    # Strength (0-100 contribution to final score)
    strength: float

    # Supporting data
    data: Dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.signal_type}: {self.description} (strength={self.strength:.1f})"


@dataclass
class Alert:
    """
    An alert to send to the user.
    Combines market info with signal analysis.
    """
    # Market info
    market: Market

    # Signal analysis
    signal_score: float  # 0-100 composite score
    signals: List[Signal] = field(default_factory=list)

    # Context
    old_probability: Optional[float] = None
    new_probability: Optional[float] = None
    time_delta_minutes: Optional[float] = None

    # Cross-platform context
    other_platform_probability: Optional[float] = None
    other_platform_name: Optional[str] = None

    @property
    def reasons(self) -> List[str]:
        """Human-readable list of why this alert fired."""
        return [s.description for s in self.signals]

    def format_message(self) -> str:
        """Format alert for logging/display."""
        lines = ["🚨 EARLY SIGNAL"]
        lines.append(f"{self.market.platform_str.upper()} — '{self.market.name}'")

        # Price change
        if self.old_probability is not None and self.new_probability is not None:
            time_str = ""
            if self.time_delta_minutes:
                if self.time_delta_minutes < 60:
                    time_str = f" in {int(self.time_delta_minutes)}m"
                else:
                    hours = self.time_delta_minutes / 60
                    time_str = f" in {hours:.1f}h"
            lines.append(f"{self.old_probability:.0f}% → {self.new_probability:.0f}%{time_str}")

        # Volume context
        if self.market.volume_24h > 0:
            vol_str = f"${self.market.volume_24h:,.0f}"
            if self.market.volume_24h < 10000:
                lines.append(f"Thin market ({vol_str} 24h vol)")
            else:
                lines.append(f"Vol: {vol_str}")

        # Reasons
        if self.signals:
            reason_strs = [s.description for s in self.signals[:3]]  # Top 3
            lines.append(" + ".join(reason_strs))

        # Cross-platform context
        if self.other_platform_probability is not None and self.other_platform_name:
            lines.append(f"{self.other_platform_name} @ {self.other_platform_probability:.0f}%")

        # Signal score
        lines.append(f"Score: {self.signal_score:.0f}/100")

        return "\n".join(lines)


@dataclass
class MarketPair:
    """
    A pair of markets from different platforms tracking the same event.
    Used for cross-market divergence detection.
    """
    polymarket: Optional[Market] = None
    kalshi: Optional[Market] = None

    @property
    def divergence(self) -> Optional[float]:
        """Absolute difference in probability between platforms."""
        if self.polymarket is None or self.kalshi is None:
            return None
        return abs(self.polymarket.probability - self.kalshi.probability)

    @property
    def has_both(self) -> bool:
        return self.polymarket is not None and self.kalshi is not None
