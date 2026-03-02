"""
Market filtering logic.
Determines which markets to monitor based on keywords, categories, and exclusions.
"""

import re
import logging
from typing import List, Set

from models import Market
from config import MarketFilterConfig


logger = logging.getLogger(__name__)


class MarketFilter:
    """
    Filters markets to focus on serious domains:
    - Politics & elections
    - Geopolitics / international relations
    - Wars / conflicts / security
    - AI policy, AI companies, frontier tech
    - Public markets, macro, investing, economics

    Excludes:
    - Sports
    - Entertainment
    - Pop culture
    - Celebrity-related markets
    """

    def __init__(self, config: MarketFilterConfig):
        self.config = config

        # Pre-compile patterns for efficiency
        self._include_patterns = self._compile_patterns(config.include_keywords)
        self._exclude_patterns = self._compile_patterns(config.exclude_keywords)

        # Category sets for fast lookup
        self._include_categories = set(c.lower() for c in config.include_categories)
        self._exclude_categories = set(c.lower() for c in config.exclude_categories)

    def _compile_patterns(self, keywords: List[str]) -> List[re.Pattern]:
        """Compile keywords into regex patterns for word boundary matching."""
        patterns = []
        for keyword in keywords:
            # Escape special regex chars, add word boundaries
            escaped = re.escape(keyword.lower())
            # Allow word boundaries or start/end of string
            pattern = re.compile(rf'\b{escaped}\b', re.IGNORECASE)
            patterns.append(pattern)
        return patterns

    def _text_matches_patterns(self, text: str, patterns: List[re.Pattern]) -> bool:
        """Check if text matches any of the patterns."""
        text_lower = text.lower()
        for pattern in patterns:
            if pattern.search(text_lower):
                return True
        return False

    def _get_searchable_text(self, market: Market) -> str:
        """Combine all market text fields for keyword matching."""
        parts = [
            market.name,
            market.description,
            market.category,
            market.slug,
        ]
        parts.extend(market.tags)

        return " ".join(filter(None, parts))

    def _check_category(self, market: Market) -> tuple[bool, bool]:
        """
        Check category-based inclusion/exclusion.
        Returns (should_include, should_exclude).
        """
        category = market.category.lower() if market.category else ""
        tags_lower = [t.lower() for t in market.tags]

        # Check explicit category inclusion
        include_match = False
        if category in self._include_categories:
            include_match = True
        for tag in tags_lower:
            if tag in self._include_categories:
                include_match = True
                break

        # Check explicit category exclusion
        exclude_match = False
        if category in self._exclude_categories:
            exclude_match = True
        for tag in tags_lower:
            if tag in self._exclude_categories:
                exclude_match = True
                break

        return include_match, exclude_match

    def should_monitor(self, market: Market) -> bool:
        """
        Determine if a market should be monitored.

        Logic:
        1. If market matches any EXCLUDE keyword/category -> REJECT
        2. If market matches any INCLUDE keyword/category -> ACCEPT
        3. Otherwise -> REJECT (conservative default)
        """
        searchable_text = self._get_searchable_text(market)

        # Step 1: Check exclusions first (they take priority)
        if self._text_matches_patterns(searchable_text, self._exclude_patterns):
            logger.debug(f"Excluding market (keyword): {market.name[:50]}")
            return False

        category_include, category_exclude = self._check_category(market)
        if category_exclude:
            logger.debug(f"Excluding market (category): {market.name[:50]}")
            return False

        # Step 2: Check inclusions
        if self._text_matches_patterns(searchable_text, self._include_patterns):
            return True

        if category_include:
            return True

        # Step 3: Default to exclude (conservative)
        logger.debug(f"Excluding market (no match): {market.name[:50]}")
        return False

    def filter_markets(self, markets: List[Market]) -> List[Market]:
        """
        Filter a list of markets, returning only those to monitor.
        """
        filtered = [m for m in markets if self.should_monitor(m)]

        logger.info(
            f"Filtered {len(markets)} markets -> {len(filtered)} to monitor "
            f"({len(markets) - len(filtered)} excluded)"
        )

        return filtered

    def get_match_reason(self, market: Market) -> str:
        """
        Get human-readable reason why a market was included.
        Useful for debugging filters.
        """
        searchable_text = self._get_searchable_text(market)

        # Find which keyword matched
        for i, pattern in enumerate(self._include_patterns):
            if pattern.search(searchable_text.lower()):
                return f"Keyword: '{self.config.include_keywords[i]}'"

        # Check category
        category = market.category.lower() if market.category else ""
        if category in self._include_categories:
            return f"Category: '{market.category}'"

        for tag in market.tags:
            if tag.lower() in self._include_categories:
                return f"Tag: '{tag}'"

        return "Unknown"


def create_default_filter() -> MarketFilter:
    """Create a filter with default configuration."""
    return MarketFilter(MarketFilterConfig())
