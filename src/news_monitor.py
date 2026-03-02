"""
News monitor for cross-referencing market moves with news coverage.
The "no news" flag is the strongest signal: a significant market move
with zero news coverage strongly suggests insider information flow.

Uses free RSS feeds (no API key) and optional NewsAPI (free tier).
"""

import asyncio
import re
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

import aiohttp

from database import Database
from config import NewsConfig


logger = logging.getLogger(__name__)


class NewsMonitor:
    """
    Monitors news sources and cross-references with market movements.

    Core insight: If a prediction market moves significantly and there's
    NO news coverage of the underlying event, that's a strong signal
    of private information flow (insiders, whales, or leaks).
    """

    def __init__(self, config: NewsConfig, db: Database):
        self.config = config
        self.db = db
        self._session: Optional[aiohttp.ClientSession] = None
        self._owns_session = False
        self._last_fetch = datetime.min.replace(tzinfo=timezone.utc)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
                headers={"User-Agent": "MarketSentinel/1.0 NewsBot"},
            )
            self._owns_session = True
        return self._session

    async def close(self):
        if self._owns_session and self._session:
            await self._session.close()
            self._session = None

    async def fetch_news(self):
        """
        Fetch news from all configured sources.
        Rate-limited by refresh_interval_minutes.
        """
        now = datetime.now(timezone.utc)
        interval_seconds = self.config.refresh_interval_minutes * 60

        if (now - self._last_fetch).total_seconds() < interval_seconds:
            return  # Too soon

        self._last_fetch = now
        articles_saved = 0

        # Fetch RSS feeds in parallel
        tasks = [self._fetch_rss_feed(url) for url in self.config.rss_feeds]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.debug(f"RSS fetch error: {result}")
                continue
            if isinstance(result, list):
                for article in result:
                    try:
                        self.db.save_news_article(
                            title=article["title"],
                            source=article.get("source", "rss"),
                            url=article.get("url", ""),
                            published_at=article.get("published_at"),
                            keywords=article.get("keywords", []),
                        )
                        articles_saved += 1
                    except Exception as e:
                        logger.debug(f"Error saving article: {e}")

        # Optional: fetch from NewsAPI if key is configured
        if self.config.newsapi_key:
            try:
                newsapi_articles = await self._fetch_newsapi()
                for article in newsapi_articles:
                    try:
                        self.db.save_news_article(
                            title=article["title"],
                            source=article.get("source", "newsapi"),
                            url=article.get("url", ""),
                            published_at=article.get("published_at"),
                            keywords=article.get("keywords", []),
                        )
                        articles_saved += 1
                    except Exception:
                        pass
            except Exception as e:
                logger.debug(f"NewsAPI fetch error: {e}")

        if articles_saved > 0:
            logger.info(f"News: cached {articles_saved} new articles")

    async def _fetch_rss_feed(self, feed_url: str) -> List[Dict[str, Any]]:
        """Parse an RSS feed and return articles."""
        session = await self._get_session()
        articles = []

        try:
            async with session.get(feed_url) as response:
                if response.status != 200:
                    return []
                text = await response.text()

            root = ET.fromstring(text)

            # Handle RSS 2.0
            for item in root.findall(".//item"):
                title = item.findtext("title", "")
                link = item.findtext("link", "")
                pub_date = item.findtext("pubDate", "")
                description = item.findtext("description", "")

                if not title:
                    continue

                # Extract keywords from title and description
                keywords = self._extract_keywords(title + " " + description)

                articles.append({
                    "title": title.strip(),
                    "url": link.strip(),
                    "source": self._domain_from_url(feed_url),
                    "published_at": pub_date,
                    "keywords": keywords,
                })

            # Handle Atom feeds
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            for entry in root.findall(".//atom:entry", ns):
                title = entry.findtext("atom:title", "", ns)
                link_elem = entry.find("atom:link", ns)
                link = link_elem.get("href", "") if link_elem is not None else ""
                published = entry.findtext("atom:published", "", ns) or entry.findtext("atom:updated", "", ns)

                if not title:
                    continue

                keywords = self._extract_keywords(title)
                articles.append({
                    "title": title.strip(),
                    "url": link.strip(),
                    "source": self._domain_from_url(feed_url),
                    "published_at": published,
                    "keywords": keywords,
                })

        except ET.ParseError:
            logger.debug(f"RSS parse error for {feed_url}")
        except Exception as e:
            logger.debug(f"RSS fetch error for {feed_url}: {e}")

        return articles

    async def _fetch_newsapi(self) -> List[Dict[str, Any]]:
        """Fetch from NewsAPI (free tier: 100 req/day)."""
        session = await self._get_session()
        articles = []

        url = "https://newsapi.org/v2/top-headlines"
        params = {
            "country": "us",
            "pageSize": 50,
            "apiKey": self.config.newsapi_key,
        }

        try:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    return []
                data = await response.json()

            for article in data.get("articles", []):
                title = article.get("title", "")
                if not title or title == "[Removed]":
                    continue

                keywords = self._extract_keywords(
                    title + " " + (article.get("description") or "")
                )

                articles.append({
                    "title": title,
                    "url": article.get("url", ""),
                    "source": article.get("source", {}).get("name", "newsapi"),
                    "published_at": article.get("publishedAt", ""),
                    "keywords": keywords,
                })
        except Exception as e:
            logger.debug(f"NewsAPI error: {e}")

        return articles

    def _extract_keywords(self, text: str) -> List[str]:
        """Extract meaningful keywords from text for matching."""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        # Lowercase and split
        words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())

        # Remove common stop words
        stop_words = {
            'the', 'and', 'for', 'are', 'but', 'not', 'you', 'all',
            'can', 'had', 'her', 'was', 'one', 'our', 'out', 'has',
            'have', 'been', 'some', 'them', 'than', 'its', 'over',
            'will', 'this', 'that', 'with', 'from', 'they', 'were',
            'says', 'said', 'about', 'would', 'could', 'should',
            'into', 'more', 'also', 'just', 'what', 'when', 'which',
            'how', 'who', 'new', 'may', 'after', 'before',
        }

        keywords = [w for w in words if w not in stop_words]
        # Return unique keywords, preserving order
        seen = set()
        unique = []
        for kw in keywords:
            if kw not in seen:
                seen.add(kw)
                unique.append(kw)
        return unique[:20]  # Cap at 20 keywords

    def _domain_from_url(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc.replace("www.", "")
        except Exception:
            return "unknown"

    def check_news_coverage(
        self,
        market_name: str,
        market_description: str = "",
        lookback_hours: int = 4,
    ) -> Dict[str, Any]:
        """
        Check if a market's topic has recent news coverage.

        Returns:
            {
                "has_news": bool,
                "article_count": int,
                "articles": [...],  # matching articles
                "search_terms": [...],  # what we searched for
            }
        """
        # Extract search terms from market name
        search_terms = self._extract_keywords(market_name + " " + market_description)

        # Take the most distinctive terms (first 5)
        search_terms = search_terms[:5]

        if not search_terms:
            return {
                "has_news": True,  # Can't determine, assume covered
                "article_count": 0,
                "articles": [],
                "search_terms": [],
            }

        # Search database
        matching_articles = self.db.search_recent_news(
            search_terms=search_terms,
            hours=lookback_hours,
        )

        return {
            "has_news": len(matching_articles) > 0,
            "article_count": len(matching_articles),
            "articles": matching_articles[:5],  # Return top 5
            "search_terms": search_terms,
        }
