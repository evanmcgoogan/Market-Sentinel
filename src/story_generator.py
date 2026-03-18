"""
Story Generator — converts raw signal data into readable news stories.

Transforms alert_history rows and market_snapshot movers into structured
Story objects the dashboard renders as a real-time publication feed.

Two layers on top of raw data:
  1. Clustering  — groups related markets (e.g. "deport 250k / 500k / 750k")
                   into a single story card with a probability ladder.
  2. Claude prose — rewrites template headlines/ledes with actual journalism
                    for the top-scoring stories. Falls back gracefully if no
                    API key is set.
"""

import json
import math
import os
import re
import hashlib
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any, Union

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Asset implications: keyword → watch-worthy instruments
# ---------------------------------------------------------------------------
ASSET_MAP: Dict[str, List[str]] = {
    "ukraine":         ["Brent Crude", "Natural Gas", "RTX", "LMT", "EUR/USD"],
    "russia":          ["Brent Crude", "Natural Gas", "USD/RUB"],
    "china":           ["FXI", "KWEB", "TSM", "SOXX", "USD/CNH"],
    "taiwan":          ["TSM", "SOXX", "QQQ", "Defense ETFs"],
    "iran":            ["Brent Crude", "GLD", "RTX", "LMT"],
    "israel":          ["GLD", "Brent Crude", "Defense stocks"],
    "gaza":            ["GLD", "Brent Crude"],
    "korea":           ["EWY", "SOXX", "Defense ETFs"],
    "nato":            ["RTX", "LMT", "NOC"],
    "india":           ["INDA", "USD/INR"],
    "japan":           ["EWJ", "USD/JPY"],
    "europe":          ["EZU", "EUR/USD"],
    "trump":           ["DXY", "BTC", "MXN short", "Tariff plays"],
    "election":        ["VIX", "Sector rotation"],
    "tariff":          ["FXI", "Industrials", "DXY"],
    "sanction":        ["USD strength", "Commodities"],
    "impeach":         ["VIX", "USD"],
    "fed":             ["2yr UST", "XLF", "QQQ", "GLD"],
    "federal reserve": ["2yr UST", "XLF", "QQQ", "GLD"],
    "interest rate":   ["TLT", "XLF", "QQQ", "2yr UST"],
    "inflation":       ["TIPS", "GLD", "XLE", "2yr UST"],
    "recession":       ["GLD", "TLT", "VIX", "XLU"],
    "gdp":             ["SPY", "EEM", "DXY"],
    "oil":             ["XOM", "CVX", "XLE", "Brent Crude"],
    "gold":            ["GLD", "GDX", "Silver"],
    "dollar":          ["DXY", "EUR/USD", "EEM"],
    "bitcoin":         ["MSTR", "COIN", "ETH-USD"],
    "crypto":          ["BTC-USD", "ETH-USD", "COIN"],
    "ethereum":        ["ETH-USD", "COIN"],
    "ai":              ["NVDA", "MSFT", "GOOGL", "AMD"],
    "nvidia":          ["NVDA", "AMD", "SOXX"],
    "openai":          ["MSFT", "NVDA"],
    "anthropic":       ["AMZN", "GOOGL"],
    "semiconductor":   ["SOXX", "TSM", "NVDA", "AMD"],
    "chip":            ["SOXX", "TSM", "NVDA"],
    "sec":             ["Crypto markets", "FinTech"],
    "antitrust":       ["GOOGL", "META", "AMZN", "MSFT"],
    "war":             ["GLD", "Brent Crude", "Defense stocks", "VIX"],
    "military":        ["RTX", "LMT", "NOC", "BA"],
    "nuclear":         ["CCJ", "URA", "Defense stocks"],
    "ceasefire":       ["Brent Crude", "Defense stocks", "EUR/USD"],
    "missile":         ["GLD", "Defense stocks", "Brent Crude"],
    "invasion":        ["GLD", "Brent Crude", "Defense stocks"],
    "ipo":             ["Renaissance IPO ETF", "Sector peers"],
    "merger":          ["Merger arb plays"],
    "acquisition":     ["Merger arb plays", "Sector ETFs"],
    "private equity":  ["BX", "KKR", "APO"],
    "venture":         ["ARKK", "Sector ETFs"],
    "startup":         ["ARKK", "Growth ETFs"],
    "spac":            ["IPOX", "Sector ETFs"],
}

CATEGORY_MAP = {
    # ── Sports: detected first, always excluded from the feed ──────────
    #    NOTE: markets with financial keywords (earnings, IPO, acquisition)
    #    are RESCUED by _is_noise_market() even if they match here.
    "SPORTS": [
        # Leagues & competitions
        "premier league", "la liga", "serie a", "bundesliga", "champions league",
        "europa league", "fa cup", "mls", "ligue 1", "eredivisie",
        "super bowl", "nfl", "nba", "mlb", "nhl", "nba finals",
        "world cup", "olympics", "formula 1", " f1 ", "grand prix",
        "wimbledon", "us open", "french open", "australian open",
        "march madness", "stanley cup", "world series", "ncaa",
        "college football", "college basketball", "cfp", "bowl game",
        "copa america", "euro 2024", "euro 2028", "african cup",
        "afc championship", "nfc championship", "conference finals",
        # European clubs
        "barcelona", "real madrid", "manchester city", "manchester united",
        "liverpool", "arsenal", "chelsea", "tottenham", "atletico",
        "athletic bilbao", "mallorca", "sevilla", "villarreal", "valencia",
        "juventus", "inter milan", "ac milan", "roma", "napoli", "lazio",
        "bayern munich", "borussia", "dortmund", "leipzig", "leverkusen",
        "paris saint", "psg", "marseille", "lyon", "monaco",
        "ajax", "psv", "porto", "benfica", "celtic", "rangers",
        # US franchises
        "lakers", "celtics", "warriors", "bulls", "knicks", "nets",
        "cowboys", "patriots", "chiefs", "eagles", "49ers", "ravens",
        "yankees", "dodgers", "red sox", "cubs", "astros", "braves",
        "steelers", "packers", "bears", "broncos", "chargers", "dolphins",
        "rockets", "spurs", "heat", "clippers", "nuggets", "bucks",
        "mets", "phillies", "padres", "giants", "cardinals", "mariners",
        "bruins", "penguins", "capitals", "oilers", "maple leafs",
        # Generic sports terms
        "league title", "league championship", "league winner",
        "win the league", "win the cup", "win the championship",
        "golden boot", "ballon d'or", "mvp award", "playoff bracket",
        "point spread", "over/under", "betting line", "moneyline",
        "spread", "prop bet", "parlay", "sportsbook",
        # Golf
        "masters tournament", "pga tour", "ryder cup", "open championship",
        "golf tournament", "golfer", "win the masters", "win the open",
        "schauffele", "mcilroy", "spieth", "woods", "koepka", "thomas",
        "detry", "fitzpatrick", "finau", "mccarthy", "rahm",
        # Tennis / combat sports / other
        "ufc ", " ufc", "bellator", "boxing match", "title fight",
        "wimbledon", "roland garros", "atp tour", "wta tour",
        "cricket", "rugby", "ipl", "t20", "ashes",
        # Racing
        "kentucky derby", "preakness", "belmont stakes", "triple crown",
        "nascar", "indycar", "le mans",
        # Generic individual sport matchup patterns
        "win the title", "win the trophy", "win the race",
        "win the medal", "make the cut",
    ],

    # ── Politics: domestic policy, law, governance, social issues ──────
    "POLITICS": [
        # Institutions & process
        "election", "president", "congress", "senate", "house", "vote",
        "democrat", "republican", "governor", "primary", "nominee",
        "cabinet", "impeach", "impeachment", "legislation", "poll",
        "approval", "executive order", "veto", "filibuster", "shutdown",
        "debt ceiling", "budget", "appropriation", "confirm", "confirmation",
        "ratify", "midterm", "ballot", "caucus", "electoral", "swing state",
        # People
        "trump", "biden", "harris", "desantis", "pence", "pelosi",
        "schumer", "mcconnell", "romney", "newsom", "abbott", "aoc",
        "ocasio", "rubio", "cruz", "sanders", "warren", "buttigieg",
        # Legal / criminal justice
        "sentence", "sentencing", "sentenced", "prison", "convicted",
        "conviction", "guilty", "verdict", "acquit", "acquittal",
        "trial", "court", "judge", "jury", "indicted", "indictment",
        "arrested", "arrest", "charged", "charges", "criminal", "crime",
        "justice", "lawsuit", "sue", "appeal", "parole", "bail", "plea",
        "testify", "testimony", "prosecutor", "subpoena", "contempt",
        "pardon", "commute", "clemency", "extradite", "extradition",
        "weinstein", "epstein", "ftx", "bankman",
        # Immigration
        "deport", "deportation", "immigration", "border", "migrant",
        "refugee", "asylum", "visa", "citizen", "citizenship",
        "undocumented", "daca", "ice agents", "customs",
        # Social policy
        "abortion", "reproductive", "roe", "planned parenthood",
        "lgbtq", "transgender", "gender", "same-sex", "marriage",
        "marijuana", "cannabis", "gun control", "firearm", "second amendment",
        # Law enforcement / intelligence
        "police", "fbi", "cia", "nsa", "doj", "department of justice",
        "attorney general", "investigation", "probe", "scandal",
        # Governance
        "resign", "resignation", "recall", "referendum", "term limit",
        "supreme court", "appeals court", "circuit court", "ruling",
    ],

    # ── Geopolitics: international relations, foreign policy ───────────
    "GEOPOLITICS": [
        # Major powers
        "china", "russia", "ukraine", "taiwan", "nato", "european union",
        "iran", "israel", "palestine", "gaza", "west bank", "hezbollah",
        "hamas", "korea", "north korea", "south korea", "japan", "india",
        # More countries
        "uk", "britain", "france", "germany", "brazil", "mexico",
        "pakistan", "saudi", "turkey", "poland", "italy", "spain",
        "australia", "canada", "venezuela", "cuba", "nicaragua",
        "afghanistan", "iraq", "syria", "lebanon", "libya", "sudan",
        "ethiopia", "nigeria", "myanmar", "indonesia", "philippines",
        "egypt", "algeria", "morocco", "kenya", "south africa",
        # Institutions
        "un security council", "un general assembly", "nato summit",
        "g7", "g20", "imf", "world bank", "wto", "opec", "asean",
        "brics", "iaea", "icc", "interpol",
        # Concepts
        "sanctions", "treaty", "diplomacy", "summit", "ambassador",
        "annexation", "annex", "sovereignty", "territorial",
        "export control", "embargo", "blockade", "occupation",
        "independence", "separatist", "secession", "alliance", "accord",
        "bilateral", "multilateral", "foreign policy", "geopolitical",
        # World leaders & key figures
        "netanyahu", "zelensky", "zelenskyy", "putin", "xi jinping",
        "modi", "macron", "scholz", "sunak", "starmer", "trudeau",
        "erdogan", "orban", "kim jong", "ayatollah", "khamenei",
        "pahlavi", "reza pahlavi", "maduro", "zelensky", "lula",
        "meloni", "mbs", "bin salman", "salman",
        # Iran-specific (high-volume markets right now)
        "iranian", "iranian regime", "supreme leader", "irgc",
        "revolutionary guard", "strait of hormuz", "hormuz",
        "nuclear deal", "jcpoa", "persian gulf",
        # Gulf region
        "gulf state", "uae", "dubai", "qatar", "bahrain", "kuwait",
        "riyadh", "doha", "abu dhabi",
        # Other active geopolitical themes
        "regime change", "regime fall", "regime collapse",
        "coup attempt", "civil war", "territorial", "occupied",
        "normalization", "abraham accords", "two-state",
    ],

    # ── Conflict: armed conflict, security, military operations ────────
    "CONFLICT": [
        "war", "military", "invasion", "conflict", "attack", "missile",
        "nuclear", "troops", "army", "navy", "air force", "marines",
        "defense", "weapons", "ceasefire", "peace deal", "peace talks",
        "casualties", "drone", "strike", "airstrike", "air strike",
        "bombing", "bomb", "explosion", "siege", "hostage",
        "prisoner of war", "pow", "offensive", "frontline", "artillery",
        "warship", "submarine", "fighter jet", "carrier group",
        "regiment", "battalion", "combat", "soldier", "veteran",
        "civilian casualty", "refugee crisis", "ethnic cleansing",
        "coup", "uprising", "insurgency", "terrorism", "isis", "houthi",
        "wagner", "special operation", "special forces", "pentagon",
        "department of defense", "dod",
        # Active conflict themes
        "nuclear strike", "ballistic missile", "hypersonic",
        "carrier group", "naval blockade", "amphibious",
        "ground offensive", "air campaign", "no-fly zone",
        "irgc", "revolutionary guard", "proxy war",
        "us strike", "air strike iran", "strike iran",
    ],

    # ── Technology: AI, crypto, biotech, space, cyber ─────────────────
    "TECHNOLOGY": [
        # AI / ML
        "ai", "artificial intelligence", "openai", "anthropic", "gpt",
        "llm", "agi", "machine learning", "deepmind", "gemini",
        "claude", "chatgpt", "copilot", "neural", "foundation model",
        # Hardware / chips
        "semiconductor", "chip", "nvidia", "tsmc", "amd", "intel",
        "arm holdings", "asml", "fab", "foundry",
        # Big Tech
        "apple", "google", "meta", "amazon", "microsoft", "tesla",
        "alphabet", "spacex", "palantir",
        # Crypto / blockchain
        "crypto", "bitcoin", "ethereum", "solana", "cardano",
        "blockchain", "defi", "nft", "stablecoin", "cbdc",
        "coinbase", "binance",
        # Emerging tech
        "robot", "robotics", "automation", "autonomous", "self-driving",
        "quantum", "5g", "6g", "satellite", "nasa", "space",
        # Biotech / health tech
        "biotech", "pharmaceutical", "fda", "drug approval",
        "clinical trial", "gene", "genome", "crispr", "mrna",
        # Cyber
        "cybersecurity", "cyber attack", "hack", "breach", "ransomware",
        "data breach",
        # Regulation
        "tech regulation", "antitrust", "sec", "ftc",
    ],

    # ── Markets: economics, finance, macro, commodities ────────────────
    "MARKETS": [
        # Monetary policy
        "fed", "federal reserve", "interest rate", "rate cut", "rate hike",
        "basis point", "quantitative easing", "quantitative tightening",
        "fomc", "jerome powell", "yellen", "fed chair", "fed nominee",
        "shelton", "judy shelton", "fed governor",
        # Macro indicators
        "inflation", "cpi", "ppi", "gdp", "recession", "unemployment",
        "payroll", "jobs report", "wage", "retail sales", "pmi",
        "consumer confidence", "housing", "manufacturing",
        # Markets
        "stock market", "s&p", "nasdaq", "dow", "russell",
        "treasury", "bond", "yield", "yield curve", "10-year",
        "vix", "volatility",
        # Assets
        "oil", "crude", "brent", "wti", "natural gas", "commodity",
        "gold", "silver", "copper", "iron ore",
        "dollar", "dxy", "currency", "forex", "exchange rate",
        "euro", "yen", "yuan", "pound",
        # Fiscal / revenue
        "revenue", "tax", "taxes", "tariff", "trade war", "trade deal",
        "import", "export", "collect", "collection", "customs duty",
        "deficit", "debt", "spending", "budget", "fiscal",
        "debt ceiling", "appropriation",
        # Corporate
        "ipo", "spac", "merger", "acquisition", "m&a", "buyout",
        "private equity", "venture capital", "unicorn", "valuation",
        "earnings", "profit", "revenue miss", "guidance",
        "bankruptcy", "default", "restructure",
        # Sectors
        "bank", "banking", "fintech", "insurance", "real estate",
        "energy sector", "renewable energy", "utilities",
        "economy", "economic",
    ],
}

CATEGORY_COLORS = {
    "POLITICS":    "#5C4A9B",
    "GEOPOLITICS": "#1A5276",
    "CONFLICT":    "#922B21",
    "TECHNOLOGY":  "#1A7A4A",
    "MARKETS":     "#784212",
    "SPORTS":      "#2E7D32",   # green — never shown, just for detection
    "OTHER":       "#5D6D7E",
}

# Words that don't carry topic meaning — stripped before clustering
STOP_WORDS = frozenset([
    'a', 'an', 'the', 'is', 'are', 'will', 'would', 'could', 'should',
    'in', 'on', 'at', 'to', 'for', 'of', 'and', 'or', 'but', 'by',
    'from', 'with', 'be', 'been', 'have', 'has', 'had', 'was', 'were',
    'this', 'that', 'which', 'who', 'what', 'when', 'where', 'if', 'than',
    'as', 'do', 'does', 'did', 'how', 'why', 'not', 'no', 'into',
    'more', 'most', 'least', 'between', 'before', 'after', 'during',
    'over', 'under', 'about', 'up', 'out', 'it', 'its', 'get', 'got',
])
_DIGIT = re.compile(r'\d')


def _content_words(text: str) -> frozenset:
    """Extract meaningful content words from a market name."""
    words = re.sub(r'[^\w\s]', ' ', text.lower()).split()
    return frozenset(
        w for w in words
        if w not in STOP_WORDS and not _DIGIT.search(w) and len(w) > 2
    )


def _word_similarity(w1: frozenset, w2: frozenset) -> float:
    if not w1 or not w2:
        return 0.0
    return len(w1 & w2) / min(len(w1), len(w2))


# ---------------------------------------------------------------------------
# Story dataclass
# ---------------------------------------------------------------------------
@dataclass
class Story:
    story_id: str
    market_id: str
    headline: str
    lede: str
    market_name: str
    platform: str
    probability: float
    old_probability: Optional[float]
    prob_change: Optional[float]
    direction: str
    signal_score: float
    signals: List[str]
    signal_types: List[str]
    category: str
    timestamp: datetime
    urgency: str
    watch_assets: List[str]
    volume_24h: Optional[float]
    is_radar: bool = False
    end_date: Optional[str] = None
    intelligence_value: float = 0.0

    def to_dict(self) -> dict:
        return {
            "id":              self.story_id,
            "headline":        self.headline,
            "lede":            self.lede,
            "market_id":       self.market_id,
            "market_name":     self.market_name,
            "platform":        self.platform,
            "probability":     round(self.probability, 1),
            "old_probability": round(self.old_probability, 1) if self.old_probability is not None else None,
            "prob_change":     round(self.prob_change, 1) if self.prob_change is not None else None,
            "direction":       self.direction,
            "signal_score":    round(self.signal_score, 1),
            "signals":         self.signals,
            "signal_types":    self.signal_types,
            "category":        self.category,
            "category_color":  CATEGORY_COLORS.get(self.category, CATEGORY_COLORS["OTHER"]),
            "timestamp":       self.timestamp.isoformat(),
            "relative_time":   self._relative_time(),
            "urgency":         self.urgency,
            "watch_assets":    self.watch_assets,
            "volume_24h":      self.volume_24h,
            "is_radar":        self.is_radar,
            "end_date":        self.end_date,
            "intelligence_value": round(self.intelligence_value, 1),
            "is_cluster":      False,
        }

    def _relative_time(self) -> str:
        now = datetime.now(timezone.utc)
        ts = self.timestamp.replace(tzinfo=timezone.utc) if self.timestamp.tzinfo is None else self.timestamp
        secs = max(0, int((now - ts).total_seconds()))
        if secs < 60:   return "just now"
        if secs < 3600: return f"{secs // 60}m ago"
        if secs < 86400: return f"{secs // 3600}h ago"
        return f"{secs // 86400}d ago"


# ---------------------------------------------------------------------------
# StoryCluster — a group of related markets rendered as one card
# ---------------------------------------------------------------------------
@dataclass
class StoryCluster:
    """Multiple prediction markets on the same topic, shown as one story card."""
    stories: List[Story]  # sorted by probability descending
    headline: str = ""
    lede: str = ""

    @property
    def story_id(self) -> str:
        return f"cluster-{self.stories[0].story_id}"

    @property
    def signal_score(self) -> float:
        return max(s.signal_score for s in self.stories)

    @property
    def category(self) -> str:
        return self.stories[0].category

    @property
    def urgency(self) -> str:
        order = {"breaking": 2, "developing": 1, "watch": 0}
        return max(self.stories, key=lambda s: order.get(s.urgency, 0)).urgency

    @property
    def timestamp(self) -> datetime:
        return self.stories[0].timestamp

    @property
    def watch_assets(self) -> List[str]:
        return self.stories[0].watch_assets

    @property
    def platform(self) -> str:
        return self.stories[0].platform

    @property
    def auto_headline(self) -> str:
        """Generate a cluster-level headline without Claude."""
        names   = [s.market_name for s in self.stories]
        topic   = _extract_topic(names)
        top     = self.stories[0]
        variant = _title_case(self._variant_label(top.market_name))  # already correct
        p_str   = f"{top.probability:.0f}%"
        if top.prob_change is not None and abs(top.prob_change) > 0.5:
            mag  = abs(top.prob_change)
            word = "Rising" if top.prob_change > 0 else "Falling"
            return f"{topic}: {p_str} on {variant} — {word} {mag:.1f}pp"
        return f"{topic}: {p_str} on {variant}"

    @property
    def auto_lede(self) -> str:
        """Generate a cluster-level lede without Claude."""
        names  = [s.market_name for s in self.stories]
        topic  = _extract_topic(names)
        top    = self.stories[0]
        plat   = top.platform.title()
        n      = len(self.stories)
        p_str  = f"{top.probability:.0f}%"

        top_label = self._variant_label(top.market_name)

        if n >= 3:
            second    = self.stories[1]
            sec_label = self._variant_label(second.market_name)
            intro = (
                f"{plat} has priced {n} discrete outcomes for {topic}. "
                f"The \u201c{top_label}\u201d scenario leads at {p_str}, "
                f"followed by \u201c{sec_label}\u201d at {second.probability:.0f}%."
            )
        else:
            second    = self.stories[1]
            sec_label = self._variant_label(second.market_name)
            intro = (
                f"{plat} is pricing two outcomes for {topic}: "
                f"\u201c{top_label}\u201d leads at {p_str} "
                f"vs \u201c{sec_label}\u201d at {second.probability:.0f}%."
            )

        # Add movement context
        max_change = max((abs(s.prob_change or 0) for s in self.stories), default=0)
        if max_change > 0.5:
            mover    = max(self.stories, key=lambda s: abs(s.prob_change or 0))
            mv_label = self._variant_label(mover.market_name)
            word     = "gained" if (mover.prob_change or 0) > 0 else "shed"
            move_ctx = (
                f" The \u201c{mv_label}\u201d line {word} "
                f"{abs(mover.prob_change or 0):.1f}pp in the latest session."
            )
        else:
            move_ctx = ""

        return intro + move_ctx

    def to_dict(self) -> dict:
        members = []
        for s in self.stories:
            label = self._variant_label(s.market_name)
            members.append({
                "label":        label,
                "market_name":  s.market_name,
                "probability":  round(s.probability, 1),
                "old_probability": round(s.old_probability, 1) if s.old_probability is not None else None,
                "prob_change":  round(s.prob_change, 1) if s.prob_change is not None else None,
                "direction":    s.direction,
                "signal_score": round(s.signal_score, 1),
                "platform":     s.platform,
            })

        rep = self.stories[0]
        return {
            "id":              self.story_id,
            "is_cluster":      True,
            "cluster_count":   len(self.stories),
            "cluster_markets": members,
            "headline":        self.headline or self.auto_headline,
            "lede":            self.lede or self.auto_lede,
            "category":        self.category,
            "category_color":  CATEGORY_COLORS.get(self.category, CATEGORY_COLORS["OTHER"]),
            "urgency":         self.urgency,
            "timestamp":       self.timestamp.isoformat(),
            "relative_time":   rep._relative_time(),
            "signal_score":    round(self.signal_score, 1),
            "watch_assets":    self.watch_assets,
            "platform":        self.platform,
            "is_radar":        False,
            # top-market probability fields for JS compatibility
            "probability":     round(rep.probability, 1),
            "market_id":       rep.market_id,
            "old_probability": round(rep.old_probability, 1) if rep.old_probability is not None else None,
            "prob_change":     round(rep.prob_change, 1) if rep.prob_change is not None else None,
            "direction":       rep.direction,
            "signals":         rep.signals,
            "signal_types":    rep.signal_types,
        }

    def _variant_label(self, market_name: str) -> str:
        """Strip common prefix/suffix to show only the distinguishing part."""
        names = [s.market_name for s in self.stories]
        if len(names) <= 1:
            return market_name[:60]

        # Find common prefix character by character
        common_prefix = os.path.commonprefix([n.lower() for n in names])
        # Walk back to word boundary
        if common_prefix and not common_prefix[-1].isalnum():
            pass
        else:
            common_prefix = common_prefix[:max(0, common_prefix.rfind(' '))]

        # Find common suffix
        rev_names = [n.lower()[::-1] for n in names]
        common_suffix = os.path.commonprefix(rev_names)[::-1]
        if common_suffix and not common_suffix[0].isalnum():
            pass
        else:
            idx = common_suffix.find(' ')
            common_suffix = common_suffix[idx:] if idx >= 0 else ""

        start = len(common_prefix)
        end   = len(market_name) - len(common_suffix)

        if start < end:
            variant = market_name[start:end].strip(" ,?-—–")
            if len(variant) > 2:
                return variant

        # Fallback: first 50 chars
        return market_name[:50]


# ---------------------------------------------------------------------------
# Claude headline generator
# ---------------------------------------------------------------------------
class ClaudeHeadlineGenerator:
    """
    Calls claude-haiku to write real headlines and ledes for top stories.
    Caches by (truncated market name + probability bucket) so we don't
    re-call Claude on every 30-second dashboard refresh.
    Falls back silently to template text if no key is configured.

    Cache is capped at _CACHE_MAX_SIZE entries (oldest-first eviction) to
    prevent unbounded memory growth on long-running Render deployments.

    The cache is persisted to the database (state table) so it survives
    process restarts — cold-start Claude calls are avoided after the first
    deployment day.
    """

    _CACHE_MAX_SIZE = 500   # entries before eviction kicks in
    _CACHE_EVICT_N  = 100   # how many oldest entries to drop at once
    _STATE_KEY      = "claude_headline_cache"

    SYSTEM = (
        "You are the editor-in-chief of Market Sentinel — an intelligence terminal that "
        "synthesizes prediction market signals into actionable news for investors, traders, "
        "and geopolitical analysts.\n\n"
        "MANDATE: Surface what is happening in the REAL WORLD and what it means for money. "
        "Prediction markets price in information before traditional media — your job is to "
        "decode WHY a market is moving, not describe THAT it moved.\n\n"
        "COVERAGE: geopolitics & armed conflict, US/global politics & policy, AI & frontier "
        "technology, macro & public markets, crypto (BTC/ETH only), M&A & private markets.\n\n"
        "RULES:\n"
        "- Headlines: Lead with the EVENT, not the market. 'Iran Nuclear Talks Collapse' not "
        "'Iran Market Surges'. Max 80 chars, present tense, active voice.\n"
        "- Ledes: 2 sentences. Sentence 1 = what is happening. Sentence 2 = why it matters "
        "to investors / what to watch next. Never start with 'The market' or 'Prediction markets'.\n"
        "- For settled markets (near 0% or 100%): State what ACTUALLY HAPPENED, then pivot to "
        "the next live question investors should be asking.\n"
        "- Never use filler phrases: 'interestingly', 'notably', 'it's worth noting', "
        "'in a significant development'. Just state the facts.\n"
        "- Be specific. Name countries, people, dollar amounts, dates when known.\n"
        "- Tone: Bloomberg terminal meets Stratfor intelligence brief. Authoritative, precise, "
        "zero fluff."
    )

    def __init__(self, api_key: str, db=None):
        import anthropic as _anthropic
        self._client = _anthropic.Anthropic(api_key=api_key)
        self._db = db
        self._cache: Dict[str, Dict[str, str]] = {}
        if db:
            try:
                saved = db.get_state(self._STATE_KEY, default=None)
                if saved and isinstance(saved, dict):
                    self._cache = saved
                    logger.info(f"ClaudeHeadlineGenerator: warmed cache with {len(saved)} entries from DB")
            except Exception as e:
                logger.debug(f"Failed to load headline cache from DB: {e}")

    def _persist_cache(self) -> None:
        """Evict oldest entries if at capacity, then save full cache to DB."""
        if len(self._cache) > self._CACHE_MAX_SIZE:
            keys_to_drop = list(self._cache.keys())[:self._CACHE_EVICT_N]
            for k in keys_to_drop:
                del self._cache[k]
        if self._db:
            try:
                self._db.set_state(self._STATE_KEY, self._cache)
            except Exception as e:
                logger.debug(f"Failed to save headline cache to DB: {e}")

    def _cache_key(self, name: str, prob: float) -> str:
        bucket = round(prob / 5) * 5  # snap to nearest 5pp
        raw = f"{name[:40]}|{bucket}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def enhance_story(self, story: Story) -> Dict[str, str]:
        key = self._cache_key(story.market_name, story.probability)
        if key in self._cache:
            return self._cache[key]

        result = self._call(
            market=story.market_name,
            platform=story.platform,
            prob=story.probability,
            old_prob=story.old_probability,
            change=story.prob_change,
            signals=story.signals[:3],
            score=story.signal_score,
        )
        self._cache[key] = result
        self._persist_cache()
        return result

    def enhance_cluster(self, cluster: StoryCluster) -> Dict[str, str]:
        names = [s.market_name for s in cluster.stories]
        key = hashlib.md5("|".join(sorted(n[:20] for n in names)).encode()).hexdigest()[:12]
        if key in self._cache:
            return self._cache[key]

        # Describe the cluster — these should be genuinely related threshold markets
        ladder = "\n".join(
            f'  - "{s.market_name}" → {s.probability:.0f}% ({"+" if (s.prob_change or 0) >= 0 else ""}{(s.prob_change or 0):.1f}pp)'
            for s in cluster.stories
        )
        prompt = (
            f"These {len(names)} prediction markets are the SAME question at different "
            f"thresholds (e.g. 250k vs 500k, or different candidates for one position):\n\n"
            f"{ladder}\n\n"
            f"Category: {cluster.category}\n\n"
            f"Write:\n"
            f"headline: A single news-style headline (max 80 chars) that captures the "
            f"underlying real-world situation. Lead with the EVENT, not odds. "
            f"E.g. 'Trump Deportation Push Stalls Below 500K Target' not 'Market Prices "
            f"Multiple Outcomes'.\n"
            f"lede: 2-3 sentences. What is happening, what the probability distribution "
            f"reveals about likely outcomes, and what investors should watch.\n\n"
            f"Return only valid JSON: {{\"headline\": \"...\", \"lede\": \"...\"}}"
        )
        result = self._call_raw(prompt)
        self._cache[key] = result
        self._persist_cache()
        return result

    def _call(self, market, platform, prob, old_prob, change, signals, score) -> Dict[str, str]:
        direction = "risen" if (change or 0) > 0 else "fallen"
        change_str = f"{abs(change):.1f}pp" if change is not None else "notably"
        old_str = f"{old_prob:.0f}%" if old_prob is not None else "previously"
        is_settled = prob >= 97.0 or prob <= 3.0

        if is_settled:
            outcome = "YES" if prob >= 97.0 else "NO"
            prompt = (
                f"RESOLVED EVENT:\n\n"
                f'Market question: "{market}"\n'
                f"Answer: {outcome} (final probability {prob:.0f}%)\n"
                f"Platform: {platform.title()}\n\n"
                f"Write:\n"
                f"headline: The real-world event that caused this (max 80 chars, declarative, "
                f"present tense). Lead with WHAT HAPPENED — not 'market resolves' or odds.\n"
                f"lede: 2 sentences. (1) What happened and its immediate significance. "
                f"(2) The next live question investors should be asking — name a specific "
                f"asset, sector, or upcoming event to watch.\n\n"
                f"Return only valid JSON: {{\"headline\": \"...\", \"lede\": \"...\"}}"
            )
        else:
            prompt = (
                f"EARLY SIGNAL:\n\n"
                f'Market: "{market}"\n'
                f"Probability: {old_str} → {prob:.0f}% ({direction} {change_str})\n"
                f"Signal triggers: {'; '.join(signals)}\n"
                f"Strength: {score:.0f}/100\n"
                f"Platform: {platform.title()}\n\n"
                f"Write:\n"
                f"headline: What real-world development is driving this move (max 80 chars, "
                f"present tense, active voice). Do NOT describe the probability change — "
                f"describe the EVENT causing it.\n"
                f"lede: 2 sentences. (1) What is happening in the real world — be specific, "
                f"name people/countries/amounts. Never start with 'The market' or "
                f"'Prediction markets'. (2) Why this matters to investors — name exposed "
                f"assets or sectors.\n\n"
                f"Return only valid JSON: {{\"headline\": \"...\", \"lede\": \"...\"}}"
            )
        return self._call_raw(prompt)

    def analyze_context(
        self,
        market_name: str,
        prob: float,
        change: Optional[float],
        platform: str,
        signals: List[str],
        news_articles: List[Dict],
    ) -> Dict[str, str]:
        """
        Generate an Intelligence Note for the drawer panel.
        Incorporates cached news headlines if available; falls back to
        Claude's trained knowledge if not. Cached per market+prob bucket.
        """
        key = self._cache_key(f"ctx:{market_name}", prob)
        if key in self._cache:
            return self._cache[key]

        # Build the news block
        news_block = ""
        if news_articles:
            lines = [
                f'  - "{a["title"]}" ({a.get("source","").replace("rss.","").replace("feeds.","")}, '
                f'{(a.get("published_at") or "")[:16]})'
                for a in news_articles[:5]
            ]
            news_block = "\n\nRelated news (last 48h):\n" + "\n".join(lines)

        change_str = ""
        if change is not None:
            word = "risen" if change > 0 else "fallen"
            change_str = f", has {word} {abs(change):.1f}pp in the last session"

        signals_str = ""
        clean_sigs = [s for s in (signals or []) if s and "below alert" not in s.lower()]
        if clean_sigs:
            signals_str = f"\nDetected signals: {'; '.join(clean_sigs[:3])}"

        is_settled = prob >= 97.0 or prob <= 3.0

        if is_settled:
            outcome_word = "resolved YES" if prob >= 97.0 else "resolved NO"
            prompt = (
                f'Prediction market "{market_name}" has {outcome_word} at {prob:.0f}%.\n'
                f"Platform: {platform.title()}"
                f"{signals_str}"
                f"{news_block}\n\n"
                f"Write a 3-sentence Intelligence Note for a sophisticated investor:\n"
                f"• Sentence 1: State the REAL-WORLD EVENT that caused this market to resolve "
                f"(use your knowledge of recent events — do NOT say 'the market settled' or "
                f"'this is a technical artifact'; tell the reader WHAT HAPPENED).\n"
                f"• Sentence 2: Explain the immediate implications — for regional stability, "
                f"financial markets, or geopolitics, whichever is most relevant.\n"
                f"• Sentence 3: Name the most important LIVE question that remains open for "
                f"investors — what active market or asset should they be watching right now.\n\n"
                f"Return only valid JSON: {{\"analysis\": \"...\"}}\n"
                f"Tone: authoritative, news-first, forward-looking. No market mechanics jargon."
            )
        else:
            prompt = (
                f'Prediction market: "{market_name}"\n'
                f"Platform: {platform.title()}\n"
                f"Current probability: {prob:.0f}%{change_str}"
                f"{signals_str}"
                f"{news_block}\n\n"
                f"Write a 3-sentence Intelligence Note for a sophisticated investor:\n"
                f"• Sentence 1: What real-world development is driving this market right now "
                f"(cite specific news if relevant; otherwise use your best informed analysis — "
                f"never say 'the market is moving' without explaining WHY).\n"
                f"• Sentence 2: What this probability level implies about near-term outcomes "
                f"and which assets or sectors are most exposed.\n"
                f"• Sentence 3: The single most important catalyst or data point to monitor next.\n\n"
                f"Return only valid JSON: {{\"analysis\": \"...\"}}\n"
                f"Tone: senior geopolitical/financial analyst. Precise, direct, zero fluff."
            )

        result = self._call_raw(prompt)
        self._cache[key] = result
        self._persist_cache()
        return result

    def _call_raw(self, prompt: str) -> Dict[str, str]:
        try:
            msg = self._client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=300,
                system=self.SYSTEM,
                messages=[{"role": "user", "content": prompt}],
            )
            text = msg.content[0].text.strip()
            # Strip markdown code fences if present
            text = re.sub(r'^```(?:json)?\s*', '', text)
            text = re.sub(r'\s*```$', '', text)
            return json.loads(text)
        except Exception as e:
            logger.debug(f"Claude generation failed: {e}")
            return {}


# ---------------------------------------------------------------------------
# StoryGenerator
# ---------------------------------------------------------------------------
class StoryGenerator:
    """
    Converts DB rows into Story / StoryCluster objects for the dashboard.

    Pass an api_key to enable Claude-powered headlines for top stories.
    Gracefully falls back to templates if key is absent or calls fail.
    """

    CLUSTER_THRESHOLD = 0.55   # word overlap ratio to group stories
    CLAUDE_TOP_N      = 6      # enrich this many top stories with Claude
    CLAUDE_TIMEOUT    = 10     # seconds to wait for parallel Claude calls

    def __init__(self, api_key: str = "", db=None):
        self._claude: Optional[ClaudeHeadlineGenerator] = None
        key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        if key:
            try:
                self._claude = ClaudeHeadlineGenerator(key, db=db)
                logger.info("Claude headline generation enabled (haiku)")
            except Exception as e:
                logger.warning(f"Claude init failed: {e}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_stories(self, db, hours: int = 24, limit: int = 40) -> List[Union[Story, StoryCluster]]:
        rows = db.get_recent_alerts_feed(hours=hours, limit=limit)
        raw  = [s for s in (self._row_to_story(r) for r in rows) if s]

        # ── Noise filter: drop sports, entertainment, crypto noise, etc.
        #    Sports markets with genuine financial significance are rescued.
        raw = [s for s in raw if not _is_noise_market(s.market_name)]

        # ── Also drop anything categorized as SPORTS that slipped through
        raw = [s for s in raw if s.category != "SPORTS"]

        # ── Suppress "OTHER" category if it's low signal (< 50) — these are
        #    uncategorized markets that add noise without editorial value.
        raw = [s for s in raw if s.category != "OTHER" or s.signal_score >= 50]

        # Mark and heavily penalize settled markets (prob ≥97% or ≤3%).
        # They are resolved events, not early signals. They stay in the feed
        # for context but rank far below live-moving markets.
        for s in raw:
            if s.probability >= 97.0 or s.probability <= 3.0:
                s.signal_score = s.signal_score * 0.25  # 75% penalty

        # ── Pass 1 dedup: exact market_name — keep highest-scored per market
        seen: Dict[str, Story] = {}
        for s in raw:
            if s.market_name not in seen or s.signal_score > seen[s.market_name].signal_score:
                seen[s.market_name] = s
        raw = list(seen.values())

        # ── Pass 2 dedup: question-stem — collapses time-variant duplicates
        #    "Bitcoin ≥$83k on March 17?" and "Bitcoin ≥$84k on March 18?"
        #    map to the same stem → only highest-intelligence-value survives.
        stem_best: Dict[str, Story] = {}
        for s in raw:
            stem = _question_stem(s.market_name)
            if stem not in stem_best or s.intelligence_value > stem_best[stem].intelligence_value:
                stem_best[stem] = s
        raw = list(stem_best.values())

        # ── Per-category rate limit: max 3 stories per category
        #    Prevents any single topic from dominating the feed.
        #    Sort by intelligence_value first so we keep the best ones.
        raw.sort(key=lambda s: s.intelligence_value, reverse=True)
        cat_counts: Dict[str, int] = {}
        rate_limited: List[Story] = []
        max_per_category = 3
        for s in raw:
            count = cat_counts.get(s.category, 0)
            if count < max_per_category:
                rate_limited.append(s)
                cat_counts[s.category] = count + 1
        raw = rate_limited

        clustered = self._cluster(raw)
        if self._claude:
            clustered = self._enrich_with_claude(clustered)

        return clustered

    def generate_radar(self, db, hours: int = 24, limit: int = 20) -> List[Story]:
        # Primary: markets with actual price movement
        mover_rows = db.get_recent_movers(hours=hours, min_change=1.0, limit=limit)

        # Secondary: top-volume markets (shows Iran/Fed/AI even with no delta yet)
        volume_rows = db.get_top_volume_markets(limit=40, hours=2)

        # Merge — deduplicate by market_id, movers take priority
        seen_ids = {r["market_id"] for r in mover_rows}
        combined = list(mover_rows)
        for r in volume_rows:
            if r["market_id"] not in seen_ids:
                seen_ids.add(r["market_id"])
                combined.append(r)

        # Convert to stories, apply comprehensive noise filter
        stories = [s for s in (self._mover_to_story(r) for r in combined) if s]
        stories = [s for s in stories if not _is_noise_market(s.market_name)]
        stories = [s for s in stories if s.category != "SPORTS"]
        stories = [s for s in stories if 3.0 < s.probability < 97.0]  # live markets only

        # Keep only editorially relevant categories — radar is not a general
        # prediction market index.  "OTHER" only if volume is significant.
        RADAR_CATEGORIES = {"GEOPOLITICS", "CONFLICT", "POLITICS", "MARKETS", "TECHNOLOGY"}
        stories = [
            s for s in stories
            if s.category in RADAR_CATEGORIES
            or (s.category == "OTHER" and (s.volume_24h or 0) >= 500_000)
        ]

        # ── Stem dedup: collapse time-variant duplicates in radar too
        stem_best: Dict[str, Story] = {}
        for s in stories:
            stem = _question_stem(s.market_name)
            if stem not in stem_best or s.intelligence_value > stem_best[stem].intelligence_value:
                stem_best[stem] = s
        stories = list(stem_best.values())

        # ── Sort by intelligence value, not raw volume
        stories.sort(key=lambda s: s.intelligence_value, reverse=True)
        return stories[:limit]

    def generate_resolved_context(self, db, limit: int = 6) -> List[Dict]:
        """
        Generate "Resolved Context" cards for the dedicated tab.

        Each card covers a recently settled high-volume market:
          - Claude explains WHAT HAPPENED (the real-world event)
          - Lists related LIVE markets the reader should watch next
          - Flags key assets/sectors exposed

        Returns a list of dicts ready for JSON serialization.
        """
        rows = db.get_resolved_context_markets(limit=limit * 2)
        if not rows:
            return []

        # Filter to editorial pillars — no sports, no entertainment, no noise
        rows = [r for r in rows if not _is_noise_market(r["market_name"])][:limit]

        # Pull the live radar for related-market surfacing
        live_radar = db.get_top_volume_markets(limit=60, hours=2)
        live_markets = [
            r for r in live_radar
            if 3.0 < r.get("latest_prob", 50) < 97.0
        ]

        cards = []
        for row in rows:
            market_name = row["market_name"]
            prob        = row["probability"]
            volume_24h  = row.get("volume_24h", 0) or 0
            platform    = row.get("platform", "polymarket")

            # Find live descendant markets (same topic keywords)
            topic_words = set(_content_words(market_name))
            related = []
            for lm in live_markets:
                lm_words = set(_content_words(lm["market_name"]))
                if len(topic_words & lm_words) >= 2 and lm["market_name"] != market_name:
                    related.append(lm)
                if len(related) >= 4:
                    break

            # Build asset implications
            name_lower = market_name.lower()
            assets = []
            for kw, asset_list in ASSET_MAP.items():
                if kw in name_lower:
                    assets.extend(asset_list)
            assets = list(dict.fromkeys(assets))[:4]  # dedup, cap at 4

            # Claude generates the event explanation + forward look
            outcome   = "YES" if prob >= 97.0 else "NO"
            category  = _detect_category(market_name)

            if self._claude:
                related_str = ""
                if related:
                    related_str = "\nActive related markets:\n" + "\n".join(
                        f'  - "{r["market_name"]}" → {r["latest_prob"]:.0f}%  (${r["volume_24h"]:,.0f}/day)'
                        for r in related
                    )

                prompt = (
                    f'Prediction market RESOLVED {outcome}: "{market_name}"\n'
                    f"Platform: {platform.title()}\n"
                    f"Final probability: {prob:.0f}%\n"
                    f"24h volume: ${volume_24h:,.0f}"
                    f"{related_str}\n\n"
                    f"Write a Resolved Context brief for sophisticated investors:\n"
                    f"1. headline: Declarative statement of what actually happened in the world "
                    f"(not 'market resolved' — tell us the real event, 85 chars max).\n"
                    f"2. event_summary: 2 sentences. What happened, when, and why it matters "
                    f"geopolitically or financially. Be specific — name people, places, numbers.\n"
                    f"3. forward_look: 2 sentences. What live questions remain open. What "
                    f"investors should watch next. Reference the related markets if provided.\n\n"
                    f"Return only valid JSON: "
                    f'{{\"headline\": \"...\", \"event_summary\": \"...\", \"forward_look\": \"...\"}}'
                )
                enrichment = self._claude._call_raw(prompt)
            else:
                enrichment = {}

            cards.append({
                "market_name": market_name,
                "platform":    platform,
                "probability": prob,
                "outcome":     outcome,
                "volume_24h":  volume_24h,
                "category":    category,
                "headline":    enrichment.get("headline", market_name),
                "event_summary": enrichment.get("event_summary", ""),
                "forward_look":  enrichment.get("forward_look", ""),
                "related_live":  [
                    {
                        "market_name": r["market_name"],
                        "probability": r.get("latest_prob", 0),
                        "volume_24h":  r.get("volume_24h", 0),
                    }
                    for r in related
                ],
                "assets": assets,
            })

        return cards

    # ------------------------------------------------------------------
    # Clustering
    # ------------------------------------------------------------------

    def _cluster(self, stories: List[Story]) -> List[Union[Story, StoryCluster]]:
        """
        Group stories that are the SAME question with different thresholds
        (e.g. "deport 250k / 500k / 1M") into StoryCluster objects.

        Uses question-stem matching: two markets cluster only if their
        stems are identical after stripping numbers, dates, and thresholds.
        This prevents "Trump tariffs" from merging with "Trump impeachment"
        just because they share the word "trump".
        """
        from collections import defaultdict

        stem_groups: Dict[str, List[int]] = defaultdict(list)
        for i, story in enumerate(stories):
            stem = _question_stem(story.market_name)
            stem_groups[stem].append(i)

        result: List[Union[Story, StoryCluster]] = []
        for stem, members in stem_groups.items():
            if len(members) == 1:
                result.append(stories[members[0]])
            else:
                cluster_stories = [stories[m] for m in members]
                # Sort cluster members by probability descending (most likely first)
                cluster_stories.sort(key=lambda s: s.probability, reverse=True)
                result.append(StoryCluster(stories=cluster_stories))

        return result

    # ------------------------------------------------------------------
    # Claude enrichment
    # ------------------------------------------------------------------

    def _enrich_with_claude(
        self,
        items: List[Union[Story, StoryCluster]],
    ) -> List[Union[Story, StoryCluster]]:
        """
        Enrich the top-N highest-scoring items with Claude-generated prose.
        Runs in parallel; falls back to template on timeout or error.
        """
        top = sorted(items, key=lambda x: x.signal_score, reverse=True)[:self.CLAUDE_TOP_N]

        def enrich(item):
            if isinstance(item, StoryCluster):
                return item, self._claude.enhance_cluster(item)
            else:
                return item, self._claude.enhance_story(item)

        # Use manual pool management so we can call shutdown(cancel_futures=True)
        # on timeout — the `with` statement's __exit__ calls shutdown(wait=True)
        # which blocks until ALL Claude calls complete even after as_completed
        # times out, turning the nominal 10s timeout into a 60s+ stall.
        pool = ThreadPoolExecutor(max_workers=self.CLAUDE_TOP_N)
        try:
            futures = {pool.submit(enrich, item): item for item in top}
            for future in as_completed(futures, timeout=self.CLAUDE_TIMEOUT):
                item, enhanced = future.result()
                if enhanced.get("headline"):
                    item.headline = enhanced["headline"]
                if enhanced.get("lede"):
                    item.lede = enhanced["lede"]
        except (TimeoutError, Exception) as e:
            logger.debug(f"Claude enrichment partial/failed: {e}")
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

        return items

    # ------------------------------------------------------------------
    # Row → Story conversion
    # ------------------------------------------------------------------

    def _row_to_story(self, row: dict) -> Optional[Story]:
        try:
            name     = row["market_name"]
            score    = float(row["signal_score"])
            old_prob = row.get("old_probability")
            new_prob = float(row.get("new_probability") or 50.0)
            reasons  = json.loads(row["reasons"]) if row.get("reasons") else []
            signal_types = json.loads(row.get("signal_types") or "[]")
            if not isinstance(signal_types, list):
                signal_types = []
            ts       = datetime.fromisoformat(row["timestamp"])

            change    = (new_prob - old_prob) if old_prob is not None else None
            direction = _direction(change)
            category  = _detect_category(name)

            headline = _make_headline(name, new_prob, change, direction, reasons)
            lede     = _make_lede(name, row["platform"], old_prob, new_prob, change, reasons, score)

            vol      = row.get("snapshot_volume_24h") or row.get("volume_24h")
            end_dt   = row.get("snapshot_end_date") or row.get("end_date")
            iv       = _intelligence_value(category, vol, score, end_dt)

            return Story(
                story_id=f"alert-{row['id']}",
                market_id=row.get("market_id", ""),
                headline=headline,
                lede=lede,
                market_name=name,
                platform=row["platform"],
                probability=new_prob,
                old_probability=old_prob,
                prob_change=change,
                direction=direction,
                signal_score=score,
                signals=reasons,
                signal_types=[str(s) for s in signal_types],
                category=category,
                timestamp=ts,
                urgency=_urgency(score, ts),
                watch_assets=_watch_assets(name),
                volume_24h=vol,
                is_radar=False,
                end_date=end_dt,
                intelligence_value=iv,
            )
        except Exception as e:
            logger.debug(f"Row parse error: {e}")
            return None

    def _mover_to_story(self, row: dict) -> Optional[Story]:
        try:
            name   = row["market_name"]
            latest = float(row["latest_prob"])
            oldest = float(row["oldest_prob"])
            change = float(row["change"])
            ts     = datetime.fromisoformat(row["latest_ts"])

            direction    = _direction(change)
            direction_w  = "Rising" if change > 0 else "Falling"
            category     = _detect_category(name)
            score        = min(38.0, abs(change) * 3.5)

            # Build radar headline — full clean title, probability at end
            q_bare  = re.sub(r'\?$', '', _short_name(name)).strip()
            headline = f"{q_bare} — Odds {direction_w} {abs(change):.1f}pp to {latest:.0f}%"

            vol    = row.get("volume_24h")
            end_dt = row.get("end_date")
            iv     = _intelligence_value(category, vol, score, end_dt)

            return Story(
                story_id=f"radar-{row['platform']}-{row['market_id']}",
                market_id=row.get("market_id", ""),
                headline=headline,
                lede=(
                    f"\u201c{name}\u201d has moved {abs(change):.1f}pp "
                    f"({oldest:.0f}% \u2192 {latest:.0f}%) in the past few hours on "
                    f"{row['platform'].title()} — below alert threshold but showing "
                    f"early momentum worth watching."
                ),
                market_name=name,
                platform=row["platform"],
                probability=latest,
                old_probability=oldest,
                prob_change=change,
                direction=direction,
                signal_score=score,
                signals=["Price moving — below alert threshold"],
                signal_types=["radar_momentum"],
                category=category,
                timestamp=ts,
                urgency="watch",
                watch_assets=_watch_assets(name),
                volume_24h=vol,
                is_radar=True,
                end_date=end_dt,
                intelligence_value=iv,
            )
        except Exception as e:
            logger.debug(f"Mover parse error: {e}")
            return None


# ---------------------------------------------------------------------------
# Pure helper functions (no class state needed)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Question-stem extraction for intelligent clustering
# ---------------------------------------------------------------------------

# Regex that matches numbers, dollar amounts, percentages, date fragments,
# and ordinal suffixes — these are the "variant" parts of threshold-style
# markets (e.g. 250k / 500k / $1M / 15% / Q1 2025).
_VARIANT_TOKENS = re.compile(
    r'\b\d[\d,]*(?:\.\d+)?[%kKmMbB]?\b'       # numbers + optional suffix
    r'|\$\d[\d,]*(?:\.\d+)?[kKmMbB]?'          # dollar amounts
    r'|\b(?:january|february|march|april|may|june|july|august|september'
    r'|october|november|december)\b'             # month names
    r'|\b20\d{2}\b'                              # years 20xx
    r'|\b(?:1st|2nd|3rd|\dth)\b',               # ordinals
    re.IGNORECASE,
)

# Leading question words stripped before stem comparison
_QUESTION_PREFIX = re.compile(
    r'^(will|who will|what will|when will|does|is|are|can|has|have|should)\s+',
    re.IGNORECASE,
)


def _question_stem(name: str) -> str:
    """
    Extract the question stem: the invariant part of a market name after
    stripping numbers, thresholds, dates, and question words.

    Only markets with *identical* stems should cluster — this means they're
    the same question asked at different thresholds (e.g. 250k / 500k / 1M).

    "Will Trump deport 250,000 immigrants before July 2025?"
    "Will Trump deport 500,000 immigrants before July 2025?"
    → both produce: "trump deport immigrants before" → CLUSTER ✓

    "Will Trump deport 250,000 immigrants?" vs "Will Trump be impeached?"
    → "trump deport immigrants" vs "trump be impeached" → DIFFERENT → separate ✓
    """
    s = name.lower().strip()
    s = re.sub(r'\?$', '', s).strip()               # drop trailing ?
    s = _QUESTION_PREFIX.sub('', s).strip()          # drop "will", "does", etc.
    s = _VARIANT_TOKENS.sub(' ', s)                  # strip numbers/dates/$
    s = re.sub(r'[^\w\s]', ' ', s)                   # strip punctuation
    s = ' '.join(s.split())                          # collapse whitespace
    # Remove stop words for a cleaner stem
    words = [w for w in s.split() if w not in STOP_WORDS and len(w) > 1]
    return ' '.join(words)


# ---------------------------------------------------------------------------
# Intelligence value scoring
# ---------------------------------------------------------------------------

_CATEGORY_PREMIUM = {
    "GEOPOLITICS": 2.0,  "CONFLICT": 2.0,
    "POLITICS": 1.5,     "MARKETS": 1.5,
    "TECHNOLOGY": 1.3,   "OTHER": 0.5,
    "SPORTS": 0.0,
}

_MIN_VOLUME_FOR_BOOST = 100_000      # $100k — below this, penalize
_MAX_VOLUME_FOR_BOOST = 50_000_000   # $50M — ceiling for log scaling


def _intelligence_value(
    category: str,
    volume_24h: Optional[float],
    signal_score: float,
    end_date: Optional[str] = None,
) -> float:
    """
    Rank markets by intelligence value — what matters for updating
    a mental model of the world in 10 seconds.

    Score = category_premium × volume_factor × horizon_factor × signal_score
    """
    cat_mult = _CATEGORY_PREMIUM.get(category, 0.5)

    # Volume factor: log-scaled, 1.0 at $100k, ~2.7 at $50M
    vol = volume_24h or 0.0
    if vol < _MIN_VOLUME_FOR_BOOST:
        vol_factor = 0.5  # low-volume markets are penalized
    else:
        vol_factor = 1.0 + math.log10(vol / _MIN_VOLUME_FOR_BOOST)

    # Horizon factor: markets resolving in >7 days = strategic signal,
    # markets resolving in <4 hours = noise (already caught by filters)
    horizon_factor = 1.0
    if end_date:
        try:
            end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            hours_to_resolve = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600
            if hours_to_resolve < 4:
                horizon_factor = 0.3   # imminent resolution = low signal
            elif hours_to_resolve < 24:
                horizon_factor = 0.7
            elif hours_to_resolve > 168:  # >1 week
                horizon_factor = 1.5   # strategic
        except (ValueError, TypeError):
            pass

    return cat_mult * vol_factor * horizon_factor * signal_score


# ---------------------------------------------------------------------------
# Question-stem deduplication
# ---------------------------------------------------------------------------

_STEM_STRIP_RE = re.compile(
    r'\$[\d,.]+[kKmMbB]?'                          # dollar amounts
    r'|\d+:\d+\s*(?:am|pm)?\s*(?:est|pst|utc|cst|mst|et|pt|ct|mt|gmt)?'  # times + tz (before day nums!)
    r'|[\d]{1,2}(?:st|nd|rd|th)?'                   # day numbers
    r'|\b\d{4}\b'                                    # years
    r'|(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*'  # months
    r'|\d+\.?\d*%?'                                  # bare numbers / percentages
    , re.IGNORECASE,
)


def _question_stem(name: str) -> str:
    """
    Strip numbers, dollar amounts, dates, and times to produce a
    canonical stem.  Two markets that differ only in threshold / date
    will collapse to the same stem.

    Example: "Bitcoin ≥$83k on March 17?" → "bitcoin ≥ on  ?"
    """
    return _STEM_STRIP_RE.sub('', name.lower()).strip()


# ---------------------------------------------------------------------------
# Noise / sports / entertainment filters
# ---------------------------------------------------------------------------

# Financial keywords that RESCUE a market from the sports/entertainment filter.
# If a sports-tagged market is actually about earnings, valuations, ownership,
# or financial events — it stays.
_FINANCIAL_RESCUE_KEYWORDS = frozenset([
    'earnings', 'revenue', 'profit', 'valuation', 'ipo', 'acquisition',
    'merger', 'buyout', 'bankruptcy', 'stock', 'share price', 'market cap',
    'quarterly', 'annual report', 'dividend', 'invest', 'ownership',
    'bought', 'sold', 'deal', 'sponsorship deal', 'broadcast rights',
    'salary cap', 'franchise value', 'pe ratio', 'sec filing',
])

# Expanded sports patterns — catches more variants than the CATEGORY_MAP list.
# These are substring checks (case-insensitive) applied ONLY after the
# financial rescue test fails.
_SPORTS_BLOCK_PATTERNS = [
    # Outcome language unique to sports
    "win the league", "win the cup", "win the title", "win the championship",
    "win the series", "win the match", "win the tournament", "win the race",
    "win the medal", "gold medal", "silver medal", "bronze medal",
    "qualify for", "relegat", "promoted to", "make the playoffs",
    "win mvp", "win rookie", "defensive player", "cy young", "heisman",
    "all-star", "all star", "home run", "touchdown", "goal scored",
    "hat trick", "free throw", "field goal", "three-pointer",
    "strikeout", "batting average", "yards", "assists",
    "clean sheet", "penalty kick", "red card", "yellow card",
    # Draft/transfer language
    "draft pick", "first overall", "traded to", "free agent sign",
    "transfer window", "transfer fee", "loan deal",
    # Season / match result language
    "regular season", "postseason", "game score", "final score",
    "match result", "halftime", "overtime", "extra time", "penalty shootout",
    "seed", "bracket", "round of", "quarterfinal", "semifinal",
    "group stage", "knockout stage",
]

# Crypto noise — high-frequency binary markets on daily price thresholds.
# These add zero signal value; users can get this from any price chart.
_CRYPTO_NOISE_PATTERNS = [
    "bitcoin above", "bitcoin below", "bitcoin over", "bitcoin under",
    "btc above", "btc below", "btc over", "btc under",
    "ethereum above", "ethereum below", "ethereum over", "ethereum under",
    "eth above", "eth below", "eth over", "eth under",
    "solana above", "solana below", "sol above", "sol below",
    "crypto above", "crypto below",
    "by end of day", "by end of week", "close above", "close below",
    "price of bitcoin be above", "price of bitcoin be below",
    "price of bitcoin above", "price of bitcoin below",
    "price of ethereum be above", "price of ethereum be below",
    "price of ethereum above", "price of ethereum below",
]

# Generic low-signal markets that add noise
_NOISE_PATTERNS = [
    "will it rain", "will it snow", "temperature above", "temperature below",
    "weather", "who will be eliminated", "reality tv", "love island",
    "bachelor", "bachelorette", "big brother", "survivor",
    "tiktok views", "youtube subscribers", "instagram followers",
    "twitter followers", "x followers",
]

# Esports / gaming — not financial signal
_ESPORTS_PATTERNS = [
    "counter-strike", "counter strike", "cs:", "cs2", "csgo",
    "league of legends", "lol:", "valorant", "dota",
    "overwatch", "call of duty", "cod:", "rainbow six",
    "esl pro", "blast premier", "iem ", "pgl ",
    " bo1", " bo3", " bo5",  # best-of series formats
    "esport", "e-sport",
]

# Hourly/daily binary crypto options — time-variant threshold markets.
# These are "$83k on March 18" style markets that evade the simpler
# "bitcoin above" patterns because they use ≥/≤ or specific timestamps.
_HOURLY_BINARY_PATTERNS = [
    "bitcoin ≥", "bitcoin ≤", "btc ≥", "btc ≤",
    "ethereum ≥", "ethereum ≤", "eth ≥", "eth ≤",
    "solana ≥", "solana ≤", "sol ≥", "sol ≤",
    "bitcoin price at", "btc price at", "eth price at",
    "bitcoin on march", "bitcoin on april", "bitcoin on may",
    "bitcoin on june", "bitcoin on july", "bitcoin on august",
    "btc on march", "btc on april", "btc on may",
    "crypto price at", "price at 5pm", "price at 12pm",
    "price at 5:00", "price at 12:00",
    "bitcoin be worth", "btc be worth", "eth be worth",
]

# Regex for time-resolution markets: crypto + dollar amount + date
_TIME_RESOLUTION_RE = re.compile(
    r'(?:bitcoin|btc|eth|ethereum|sol|solana).*'
    r'\$[\d,.]+[kKmMbB]?\s*'
    r'(?:on|by|before|after)\s+'
    r'(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)',
    re.IGNORECASE,
)

# Handicap / spread markets — sports betting dressed as prediction markets
_HANDICAP_SPREAD_PATTERNS = [
    "handicap", "spread", "over/under", "over under",
    "total points", "total goals", "total runs",
    "first half", "second half", "1st half", "2nd half",
    "moneyline", "money line", "point spread",
    "parlays", "teaser", "prop bet",
]

# Parlay/combo market detection: Kalshi combo markets have names like
# "yes Georgia Tech,yes SMU,yes Milwaukee" — these are sports parlays
# dressed up as prediction markets. Detect them by structure.
_COMBO_PATTERN = re.compile(
    r'(?:yes|no)\s+[A-Z].*?,\s*(?:yes|no)\s+[A-Z]',
    re.IGNORECASE,
)

# Sports-adjacent terms that appear in parlay/prop names
_PARLAY_SPORTS_TERMS = [
    "wins by over", "wins by under", "wins by more than",
    "points,", "rebounds,", "assists,",
    # "FC win" / "FC lose" patterns for football match markets
    " fc win", " fc lose", " fc draw",
    # Common in Kalshi daily match/game markets
    " win on 20",  # e.g. "Will Newcastle United FC win on 2026-03-04?"
]


def _is_noise_market(name: str) -> bool:
    """
    Return True if a market is noise that should be excluded from the feed.

    Multi-pass filter:
    0. Financial rescue — keep sports markets about money (earnings, IPO, etc.)
    1. Combo/parlay detection — "yes X, yes Y" format = sports parlay
    2. Sports entity + outcome language detection
    3. Esports / gaming detection
    4. Crypto daily price threshold noise
    5. Generic noise (weather, reality TV, social media)

    Sports markets with genuine financial significance are RESCUED in pass 0.
    """
    nl = name.lower()

    # ── Pass 0: financial rescue — if the market is about money/business,
    #    keep it even if it mentions a sports entity.
    if any(kw in nl for kw in _FINANCIAL_RESCUE_KEYWORDS):
        return False

    # ── Pass 1: combo/parlay detection ────────────────────────────────
    #    Kalshi parlays look like "yes Team1,yes Team2,yes Team3"
    if _COMBO_PATTERN.search(name):
        return True

    # ── Pass 1b: sports-adjacent parlay/prop terms ────────────────────
    if any(pat in nl for pat in _PARLAY_SPORTS_TERMS):
        return True

    # ── Pass 2: sports detection ──────────────────────────────────────
    # First check CATEGORY_MAP sports keywords (comprehensive entity list)
    is_sports = any(kw in nl for kw in CATEGORY_MAP["SPORTS"])

    # Also check sports outcome language
    if not is_sports:
        is_sports = any(pat in nl for pat in _SPORTS_BLOCK_PATTERNS)

    if is_sports:
        return True

    # ── Pass 3: esports / gaming ──────────────────────────────────────
    if any(pat in nl for pat in _ESPORTS_PATTERNS):
        return True

    # ── Pass 4: crypto price noise ────────────────────────────────────
    if any(pat in nl for pat in _CRYPTO_NOISE_PATTERNS):
        return True

    # ── Pass 4b: hourly binary crypto options ──────────────────────────
    if any(pat in nl for pat in _HOURLY_BINARY_PATTERNS):
        return True

    # ── Pass 4c: regex for time-variant crypto threshold markets ───────
    if _TIME_RESOLUTION_RE.search(name):
        return True

    # ── Pass 4d: handicap / spread / prop bet markets ──────────────────
    if any(pat in nl for pat in _HANDICAP_SPREAD_PATTERNS):
        return True

    # ── Pass 5: generic noise ─────────────────────────────────────────
    if any(pat in nl for pat in _NOISE_PATTERNS):
        return True

    return False


# Words kept lowercase in title case (unless first word)
_LOWER_WORDS = frozenset([
    'a', 'an', 'the', 'and', 'but', 'or', 'nor',
    'in', 'on', 'at', 'to', 'for', 'of', 'by', 'with',
    'vs', 'via', 'per', 'as',
])


def _title_case(text: str) -> str:
    """
    Smart title case:
    - Preserves dotted acronyms like U.S., U.K., E.U. (all-caps with dots)
    - Preserves plain acronyms like NATO, GDP, IPO
    - Keeps articles/prepositions lowercase after the first word
    """
    words = text.split()
    out = []
    for i, w in enumerate(words):
        # Already ALL-CAPS and more than one char → keep (NATO, GDP)
        if w.isupper() and len(w) > 1:
            out.append(w)
        # Dotted acronym like u.s. → U.S.
        elif re.match(r'^([a-zA-Z]\.){2,}$', w):
            out.append(w.upper())
        elif i == 0 or w.lower() not in _LOWER_WORDS:
            out.append(w[0].upper() + w[1:] if w else w)
        else:
            out.append(w.lower())
    return ' '.join(out)


def _short_name(name: str) -> str:
    """
    Return a clean, title-cased version of the market name for use in headlines.
    Keeps the 'Will…?' framing intact — reads naturally as a question headline.
    """
    name = name.strip()
    name = name[:80] + '…' if len(name) > 80 else name
    return _title_case(name)


# Auxiliary/copulative verbs — stop the subject-phrase extraction here
_VERB_STOP = re.compile(
    r'\s+(be|is|are|was|were|become|have|has|had|get|remain|stay)\b',
    re.IGNORECASE
)

# Action verb → noun-form substitution for cleaner topic labels
_VERB_TO_NOUN: Dict[str, str] = {
    'deport':    'Deportation',
    'collect':   'Collection',
    'win':       'Race',
    'elect':     'Election',
    'resign':    'Resignation',
    'impeach':   'Impeachment',
    'invade':    'Invasion',
    'attack':    'Attack',
    'sign':      'Signing',
    'pass':      'Passage',
    'acquire':   'Acquisition',
    'merge':     'Merger',
    'launch':    'Launch',
    'announce':  'Announcement',
    'approve':   'Approval',
    'reject':    'Rejection',
    'ban':       'Ban',
    'convict':   'Conviction',
    'sentence':  'Sentencing',
    'indict':    'Indictment',
    'arrest':    'Arrest',
    'nominate':  'Nomination',
    'appoint':   'Appointment',
    'raise':     'Rate Hike',
    'cut':       'Rate Cut',
}


def _extract_topic(names: List[str]) -> str:
    """
    Extract a concise topic label from a list of related market names.

    Strategy:
      1. Use the common prefix (stripped of leading "will" etc.).
         Stop at auxiliary/copulative verbs so we get the subject noun phrase,
         not the full predicate (e.g. "Harvey Weinstein" not "Harvey Weinstein be sentenced to").
      2. If the prefix is too short (<= 5 chars), try the common suffix
         (e.g. "2026 Texas Republican Primary" from two candidates' markets).
      3. Fall back to the first name, trimmed.
    """
    if not names:
        return "This Market"

    lowered = [n.lower() for n in names]

    # ── 1. Common prefix ────────────────────────────────────────────
    common = os.path.commonprefix(lowered)
    # Walk back to word boundary
    if common and common[-1] not in (' ', '?'):
        last_space = common.rfind(' ')
        common = common[:last_space] if last_space > 0 else ""
    common = common.strip()

    # Strip leading question starters
    topic = re.sub(
        r'^(will|who will|what will|when will|does|is|are|can|has|have)\s+',
        '', common, flags=re.IGNORECASE
    ).strip()

    # Stop at auxiliary/copulative verbs → keep only the subject phrase
    m = _VERB_STOP.search(topic)
    if m:
        topic = topic[:m.start()].strip()

    # Nominalize a trailing action verb (e.g. "trump deport" → "trump deportation")
    t_words = topic.split()
    if t_words and t_words[-1].lower() in _VERB_TO_NOUN:
        t_words[-1] = _VERB_TO_NOUN[t_words[-1].lower()]
        topic = ' '.join(t_words)

    # Strip leading article "The" / "A" when topic has more content
    topic = re.sub(r'^(the|a)\s+', '', topic, flags=re.IGNORECASE).strip()

    if len(topic) > 5:
        return _title_case(topic)

    # ── 2. Common suffix ────────────────────────────────────────────
    rev_lowered = [n.lower()[::-1] for n in names]
    rev_common  = os.path.commonprefix(rev_lowered)[::-1].strip(' ?')
    # Walk forward to word boundary
    if rev_common and rev_common[0] not in (' ', '?'):
        first_space = rev_common.find(' ')
        rev_common = rev_common[first_space:].strip() if first_space >= 0 else ""

    # Strip trailing question marks and "in 20XX" year suffixes
    suffix_topic = re.sub(r'\s+in\s+\d{4}$', '', rev_common.strip(' ?')).strip()
    # Strip leading prepositions/verbs from suffix topic
    suffix_topic = re.sub(r'^(the|a|an|in|at|for|of|on)\s+', '', suffix_topic, flags=re.IGNORECASE).strip()
    # Nominalize a leading verb in the suffix too
    s_words = suffix_topic.split()
    if s_words and s_words[0].lower() in _VERB_TO_NOUN:
        s_words[0] = _VERB_TO_NOUN[s_words[0].lower()]
        suffix_topic = ' '.join(s_words)

    if len(suffix_topic) > 5:
        return _title_case(suffix_topic)

    # ── 3. Fallback: first market name, stripped ────────────────────
    fb = re.sub(r'^(will|who will|what will|when will|does|is|are|can)\s+',
                '', names[0], flags=re.IGNORECASE)
    fb = re.sub(r'\?$', '', fb).strip()
    m2 = _VERB_STOP.search(fb.lower())
    if m2:
        fb = fb[:m2.start()].strip()
    return _title_case(fb[:40])


def _direction(change: Optional[float]) -> str:
    if change is None: return "flat"
    if change >  0.5:  return "up"
    if change < -0.5:  return "down"
    return "flat"


def _urgency(score: float, ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    age_mins = (datetime.now(timezone.utc) - ts).total_seconds() / 60
    if score >= 60 and age_mins < 45:  return "breaking"
    if score >= 40 or age_mins < 120:  return "developing"
    return "watch"


def _detect_category(name: str) -> str:
    name_lower = name.lower()

    # SPORTS always wins — these get filtered out of the feed entirely
    if any(kw in name_lower for kw in CATEGORY_MAP["SPORTS"]):
        return "SPORTS"

    scores = {
        cat: sum(1 for kw in kws if kw in name_lower)
        for cat, kws in CATEGORY_MAP.items()
        if cat != "SPORTS"
    }
    best = max(scores, key=lambda c: scores[c])
    return best if scores[best] > 0 else "OTHER"


def _watch_assets(name: str, max_assets: int = 5) -> List[str]:
    name_lower = name.lower()
    tally: Dict[str, int] = {}
    for kw, assets in ASSET_MAP.items():
        if kw in name_lower:
            for a in assets:
                tally[a] = tally.get(a, 0) + 1
    return sorted(tally, key=lambda a: -tally[a])[:max_assets]


def _make_headline(name: str, prob: float, change: Optional[float],
                   direction: str, reasons: List[str]) -> str:
    """
    Produce a short, journalistic headline that:
    - Keeps the market question in recognisable form (title-cased)
    - Includes the current probability
    - Describes the signal type with active-voice verbs
    """
    q       = _short_name(name)          # full title-cased question
    q_bare  = re.sub(r'\?$', '', q).strip()   # without trailing ?
    p_str   = f"{prob:.0f}%"
    primary = reasons[0].lower() if reasons else ""

    # ── Signal-specific patterns ──────────────────────────────────────
    if "whale" in primary or "smart money" in primary:
        return f"Smart Money Alert: {q_bare[:65]}"

    if "gap" in primary or "leading" in primary or "divergen" in primary:
        return f"Cross-Platform Divergence: {q_bare[:60]}"

    if "off-peak" in primary or "unusual hour" in primary:
        return f"Off-Hours Move: {q_bare[:60]} at {p_str}"

    if "thin market" in primary or "thin liquid" in primary:
        chg_str = f"{abs(change):.1f}pp " if change is not None else ""
        word = "Spike" if direction == "up" else "Drop"
        return f"Thin-Market {word}: {q_bare[:55]} — {chg_str}to {p_str}"

    # ── Directional move ─────────────────────────────────────────────
    if change is not None and abs(change) > 0.4:
        mag = abs(change)
        if direction == "up":
            verb = "Surges" if mag >= 10 else "Jumps" if mag >= 5 else "Rises"
        else:
            verb = "Plunges" if mag >= 10 else "Falls" if mag >= 5 else "Slips"

        if "accelerat" in primary:
            verb = "Accelerates " + ("Higher" if direction == "up" else "Lower")
            return f"{q_bare[:55]} {verb} — Now at {p_str}"

        return f"{q_bare[:55]} — Odds {verb} {mag:.1f}pp to {p_str}"

    # ── Flat / generic signal ─────────────────────────────────────────
    if "accelerat" in primary:
        word = "Building" if direction == "up" else "Fading"
        return f"{q_bare[:60]}: Momentum {word} at {p_str}"

    return f"{q_bare[:62]}: Market Signal at {p_str}"


def _make_cluster_headline(stories: List["Story"], topic: str) -> str:
    """
    Headline for a cluster card — describes the whole probability distribution.
    E.g. "Trump Deportation: 93% on 250K–500K Range — Falling 0.1pp"
    """
    if not stories:
        return topic

    top = stories[0]   # highest probability (sorted desc)

    # Build variant label: strip "Will / does / etc." AND common topic words
    top_raw   = re.sub(r'\?$', '', top.market_name).strip()
    top_clean = re.sub(
        r'^(will|who will|what will|when will|does|is|are|can|has|have)\s+',
        '', top_raw, flags=re.IGNORECASE
    ).strip().lower()

    # Remove words from the topic label one by one from the start of top_clean
    for word in topic.lower().split():
        top_clean = re.sub(
            r'^\s*' + re.escape(word) + r'\s+', '', top_clean, flags=re.IGNORECASE
        ).strip()

    variant = _title_case(top_clean.strip(" ,?-—–")) if len(top_clean.strip()) > 2 else top_raw[:50]
    p_str   = f"{top.probability:.0f}%"

    # Add movement context if significant
    if top.prob_change is not None and abs(top.prob_change) > 0.5:
        mag  = abs(top.prob_change)
        word = "Rising" if top.prob_change > 0 else "Falling"
        return f"{topic}: {p_str} on {variant} — {word} {mag:.1f}pp"

    return f"{topic}: {p_str} on {variant}"


def _make_lede(name: str, platform: str, old_prob: Optional[float],
               new_prob: float, change: Optional[float],
               reasons: List[str], score: float) -> str:
    """
    Two-sentence lede written in the style of a financial intelligence brief.
    Sentence 1: what moved and by how much.
    Sentence 2: what signals triggered / what it means.
    """
    plat = platform.title()
    parts: List[str] = []

    # ── Sentence 1: the move ─────────────────────────────────────────
    if old_prob is not None and change is not None and abs(change) > 0.3:
        if change > 0:
            verb  = "surged" if abs(change) >= 5 else "climbed"
            arrow = f"{old_prob:.0f}% → {new_prob:.0f}%"
            parts.append(
                f'Odds on \u201c{name}\u201d {verb} from {old_prob:.0f}% to '
                f'{new_prob:.0f}% on {plat}, a {abs(change):.1f}-point move.'
            )
        else:
            verb  = "plunged" if abs(change) >= 5 else "slipped"
            parts.append(
                f'Odds on \u201c{name}\u201d {verb} from {old_prob:.0f}% to '
                f'{new_prob:.0f}% on {plat}, shedding {abs(change):.1f} points.'
            )
    else:
        parts.append(
            f'The {plat} market on \u201c{name}\u201d is showing an '
            f'unusual signal at {new_prob:.0f}%.'
        )

    # ── Sentence 2: signal context ────────────────────────────────────
    if "whale" in ' '.join(reasons).lower() or "smart money" in ' '.join(reasons).lower():
        parts.append("Large-wallet activity has been detected, suggesting informed positioning.")
    elif "gap" in ' '.join(reasons).lower() or "divergen" in ' '.join(reasons).lower():
        parts.append("Polymarket and Kalshi are pricing this event differently — a potential arbitrage or information gap.")
    elif "accelerat" in ' '.join(reasons).lower():
        word = "upside" if change is not None and change > 0 else "downside"
        parts.append(f"The move is accelerating, with {word} momentum strengthening across multiple time frames.")
    elif "off-peak" in ' '.join(reasons).lower() or "unusual hour" in ' '.join(reasons).lower():
        parts.append("The activity is occurring outside normal trading hours, which can indicate informed pre-positioning.")
    elif len(reasons) >= 2:
        r1, r2 = reasons[0].lower().rstrip('.'), reasons[1].lower().rstrip('.')
        parts.append(f"Two independent signals corroborate the move: {r1} and {r2}.")
    elif reasons:
        parts.append(f"Trigger: {reasons[0].lower().rstrip('.')}.")

    if score >= 70:
        parts.append("High signal strength — warrants immediate attention.")

    return ' '.join(parts)


# ---------------------------------------------------------------------------
# OutlookGenerator — multi-asset price prediction dashboard
# ---------------------------------------------------------------------------

# The canonical asset list shown on the Outlook tab
OUTLOOK_ASSETS = [
    {"ticker": "SPY",   "name": "S&P 500",         "icon": "📈", "category": "EQUITY"},
    {"ticker": "QQQ",   "name": "Nasdaq 100",       "icon": "💻", "category": "EQUITY"},
    {"ticker": "VIX",   "name": "Volatility Index", "icon": "⚡", "category": "EQUITY",  "inverted": True},
    {"ticker": "GLD",   "name": "Gold",             "icon": "🥇", "category": "COMMODITY"},
    {"ticker": "SLV",   "name": "Silver",           "icon": "🥈", "category": "COMMODITY"},
    {"ticker": "WTI",   "name": "WTI Crude Oil",    "icon": "🛢", "category": "COMMODITY"},
    {"ticker": "COPX",  "name": "Copper",           "icon": "🔩", "category": "COMMODITY"},
    {"ticker": "DXY",   "name": "US Dollar Index",  "icon": "💵", "category": "FX"},
    {"ticker": "TLT",   "name": "20yr Treasuries",  "icon": "🏦", "category": "RATES"},
    {"ticker": "BTC",   "name": "Bitcoin",          "icon": "₿",  "category": "CRYPTO"},
    {"ticker": "ETH",   "name": "Ethereum",         "icon": "⟠",  "category": "CRYPTO"},
    {"ticker": "ITA",   "name": "Defense ETF",      "icon": "🛡",  "category": "SECTOR"},
]

MAGNITUDE_LABELS = {1: "SMALL", 2: "MODERATE", 3: "LARGE", 4: "MAJOR"}
CONFIDENCE_LABELS = {
    (0,  35): "LOW",
    (35, 60): "MEDIUM",
    (60, 80): "HIGH",
    (80, 101): "VERY HIGH",
}


def _confidence_label(score: int) -> str:
    for (lo, hi), label in CONFIDENCE_LABELS.items():
        if lo <= score < hi:
            return label
    return "MEDIUM"


class OutlookGenerator:
    """
    Synthesizes signals from prediction markets, news, and alerts into a
    forward-looking asset price outlook using Claude Sonnet.

    Produces directional predictions (UP/DOWN), magnitude (1-4), and
    confidence (0-100) for each asset in OUTLOOK_ASSETS over 24h and 48h.

    Cached for CACHE_TTL seconds to limit Sonnet API calls.
    """

    CACHE_TTL = 900  # 15 minutes — outlook changes slowly
    MODEL     = "claude-sonnet-4-6"

    def __init__(self, api_key: str = ""):
        key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        self._client = None
        if key:
            try:
                import anthropic as _anthropic
                self._client = _anthropic.Anthropic(api_key=key)
                logger.info("OutlookGenerator: Claude Sonnet enabled")
            except Exception as e:
                logger.warning(f"OutlookGenerator init failed: {e}")
        self._cache: Optional[Dict] = None
        self._cache_time: Optional[datetime] = None

    def load_from_db(self, db) -> None:
        """
        Warm the in-memory cache from the most recent DB prediction.
        Call once at startup to avoid a blocking Sonnet API call on the
        first /api/outlook request after a process restart.
        """
        try:
            row = db.get_latest_outlook_prediction()
            if not row:
                return
            generated_at_str = row.get("generated_at", "")
            if not generated_at_str:
                return
            ts = datetime.fromisoformat(generated_at_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - ts).total_seconds()
            if age < self.CACHE_TTL:
                self._cache      = row
                self._cache_time = ts.replace(tzinfo=None)  # naive UTC for comparison
                logger.info(f"OutlookGenerator: warmed cache from DB ({age:.0f}s old)")
        except Exception as e:
            logger.debug(f"OutlookGenerator.load_from_db failed: {e}")

    def generate(self, db) -> Dict:
        """Return a full outlook dict, using cache if fresh enough."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if (
            self._cache is not None
            and self._cache_time is not None
            and (now - self._cache_time).total_seconds() < self.CACHE_TTL
        ):
            return self._cache

        try:
            result = self._compute(db)
        except Exception as exc:
            logger.error(f"OutlookGenerator._compute error: {exc}", exc_info=True)
            result = self._fallback(
                reason="Forecast temporarily unavailable — retrying automatically."
            )
            result["_is_fallback"] = True
            # Don't cache failures — allow immediate retry on next request
            return result

        # Persist every fresh (non-fallback) prediction for future grading
        if result.get("assets") and result.get("market_regime") != "NEUTRAL" or result.get("session_id"):
            sid = result.get("session_id") or str(uuid.uuid4())
            result["session_id"] = sid
            try:
                db.save_outlook_prediction(
                    session_id       = sid,
                    generated_at     = result.get("generated_at", now.isoformat()),
                    market_regime    = result.get("market_regime", ""),
                    outlook_summary  = result.get("outlook_summary", ""),
                    dominant_themes_json = json.dumps(result.get("dominant_themes", [])),
                    assets_json      = json.dumps(result.get("assets", {})),
                )
            except Exception as persist_err:
                logger.warning(f"OutlookGenerator: failed to persist prediction: {persist_err}")

        self._cache      = result
        self._cache_time = now
        return result

    def invalidate(self):
        """Force next call to recompute."""
        self._cache = None
        self._cache_time = None

    # ── Internal ──────────────────────────────────────────────────────────

    def _compute(self, db) -> Dict:
        # 1. Gather intelligence from all available sources
        live_markets  = db.get_top_volume_markets(limit=25, hours=2)
        live_markets  = [m for m in live_markets if 3 < (m.get("latest_prob") or 50) < 97][:20]
        resolved      = db.get_resolved_context_markets(limit=6, min_volume_24h=500_000)
        alerts        = db.get_recent_alerts_feed(hours=12, limit=10)
        news          = db.get_all_recent_news(hours=12, limit=20)

        # 2. Build the prompt context blocks
        live_block = "\n".join(
            f'  • "{m["market_name"]}" — {m.get("latest_prob",50):.0f}%  '
            f'(${(m.get("volume_24h") or 0)/1e6:.2f}M/day)'
            for m in live_markets
        ) or "  (no live markets available)"

        resolved_block = "\n".join(
            f'  • [RESOLVED {r["probability"]:.0f}%] "{r["market_name"]}" — '
            f'${(r.get("volume_24h") or 0)/1e6:.2f}M/day'
            for r in resolved
        ) or "  (none)"

        alert_block = "\n".join(
            f'  • "{a["market_name"]}" moved {a.get("old_probability",0):.0f}%→'
            f'{a.get("new_probability",0):.0f}% — signals: {a.get("reasons","")}'
            for a in alerts
        ) or "  (none)"

        news_block = "\n".join(
            f'  • [{n.get("source","").replace("rss.","").replace("feeds.","")}] '
            f'"{n.get("title","")}"'
            for n in news
        ) or "  (no recent news)"

        tickers = [a["ticker"] for a in OUTLOOK_ASSETS]
        ticker_json = json.dumps(tickers)

        prompt = f"""You are Market Sentinel's chief strategist — a senior macro analyst who synthesizes prediction markets, geopolitical intelligence, and financial news into actionable asset price outlooks.

TODAY'S DATE: {datetime.now(timezone.utc).replace(tzinfo=None).strftime("%B %d, %Y")}

═══ LIVE PREDICTION MARKET SIGNALS (high-volume, unresolved) ═══
{live_block}

═══ RECENTLY RESOLVED MARKETS (what just happened) ═══
{resolved_block}

═══ RECENT MARKET SIGNAL ALERTS (notable moves) ═══
{alert_block}

═══ BREAKING NEWS HEADLINES ═══
{news_block}

═══ YOUR TASK ═══
Based on ALL of the above intelligence, produce a structured 24-hour and 48-hour price outlook for these assets: {ticker_json}

Asset notes:
- VIX: predict the VIX level itself (UP = fear rising = risk-off)
- DXY: predict USD index direction (UP = dollar strengthening)
- TLT: predict bond prices (UP = yields falling = flight to safety)
- ITA: Defense ETF (RTX, LMT, NOC, GD etc.)

For EACH asset and EACH horizon (24h AND 48h), provide:
- direction: "UP" or "DOWN"
- magnitude_score: integer 1-4 (1=<0.5%, 2=0.5-1.5%, 3=1.5-3%, 4=>3%)
- confidence: integer 0-100 (your conviction in this call)
- drivers: list of exactly 3 short strings (max 5 words each) — the key forces driving this prediction

Also provide:
- outlook_summary: 3 sentences. The dominant macro narrative right now, what it means for markets, and the single biggest risk to watch.
- market_regime: one of "RISK-OFF" | "RISK-ON" | "NEUTRAL" | "MIXED"
- dominant_themes: list of 4-6 theme strings (e.g. "Iran war escalation", "Fed on hold", "Safe haven bid")
- generated_note: one sentence explaining the #1 thing that changed your view most

Return ONLY valid compact JSON (no whitespace, no markdown fences) in exactly this schema:
{{
  "outlook_summary": "...",
  "market_regime": "RISK-OFF",
  "dominant_themes": ["...", "..."],
  "generated_note": "...",
  "assets": {{
    "SPY":  {{"24h": {{"direction": "DOWN", "magnitude_score": 2, "confidence": 72, "drivers": ["a","b","c"]}}, "48h": {{"direction": "DOWN", "magnitude_score": 3, "confidence": 61, "drivers": ["a","b","c"]}}}},
    "QQQ":  {{"24h": {{...}}, "48h": {{...}}}},
    "VIX":  {{"24h": {{...}}, "48h": {{...}}}},
    "GLD":  {{"24h": {{...}}, "48h": {{...}}}},
    "SLV":  {{"24h": {{...}}, "48h": {{...}}}},
    "WTI":  {{"24h": {{...}}, "48h": {{...}}}},
    "COPX": {{"24h": {{...}}, "48h": {{...}}}},
    "DXY":  {{"24h": {{...}}, "48h": {{...}}}},
    "TLT":  {{"24h": {{...}}, "48h": {{...}}}},
    "BTC":  {{"24h": {{...}}, "48h": {{...}}}},
    "ETH":  {{"24h": {{...}}, "48h": {{...}}}},
    "ITA":  {{"24h": {{...}}, "48h": {{...}}}}
  }}
}}"""

        if not self._client:
            fb = self._fallback()
            fb["_is_fallback"] = True
            return fb

        msg = self._client.messages.create(
            model=self.MODEL,
            max_tokens=4096,
            system=(
                "You are the chief market strategist at Market Sentinel. "
                "You synthesize geopolitical intelligence, prediction markets, and macro signals "
                "into authoritative asset price outlooks. Be direct, specific, and precise. "
                "Your predictions must be grounded in the actual signals provided — do not be generic. "
                "Return only valid compact JSON — no markdown, no commentary."
            ),
            messages=[{"role": "user", "content": prompt}],
        )

        raw = msg.content[0].text.strip()
        # Strip any markdown fences
        raw = re.sub(r'^```(?:json)?\s*', '', raw)
        raw = re.sub(r'\s*```\s*$', '', raw.strip())
        # Find the first JSON object start
        start = raw.find('{')
        if start == -1:
            raise ValueError("No JSON object found in Claude response")
        # Attempt 1: strict parse of first complete JSON object (handles trailing text)
        decoder = json.JSONDecoder()
        try:
            data, _ = decoder.raw_decode(raw, start)
        except json.JSONDecodeError:
            # Attempt 2: json_repair handles missing commas, truncated output, etc.
            try:
                from json_repair import repair_json
                repaired = repair_json(raw[start:])
                data = json.loads(repaired)
                logger.info("OutlookGenerator: used json_repair to recover malformed response")
            except Exception as repair_err:
                raise ValueError(f"JSON repair also failed: {repair_err}")

        # Enrich with static asset metadata and fill missing horizons
        asset_meta = {a["ticker"]: a for a in OUTLOOK_ASSETS}
        valid_tickers = set(asset_meta.keys())
        # Strip any rogue keys Claude may have added (e.g. "24h", "48h" leaked as ticker names)
        data["assets"] = {k: v for k, v in data.get("assets", {}).items() if k in valid_tickers}
        for ticker, horizons in data.get("assets", {}).items():
            meta = asset_meta.get(ticker, {})

            # Fill any missing horizon by mirroring the other, with lower confidence
            for h_primary, h_fallback in [("24h", "48h"), ("48h", "24h")]:
                if not isinstance(horizons.get(h_primary), dict) or not horizons[h_primary].get("direction"):
                    src = horizons.get(h_fallback) or {}
                    horizons[h_primary] = {
                        "direction":       src.get("direction", "—"),
                        "magnitude_score": max(1, (src.get("magnitude_score") or 1) - 1),
                        "confidence":      max(0, int((src.get("confidence") or 0) * 0.8)),
                        "drivers":         src.get("drivers", []),
                    }

            for h in ("24h", "48h"):
                pred = horizons[h]
                if not isinstance(pred, dict):
                    continue
                pred["magnitude_label"] = MAGNITUDE_LABELS.get(pred.get("magnitude_score", 1), "SMALL")
                pred["confidence_label"] = _confidence_label(pred.get("confidence", 50))

            horizons["ticker"]   = ticker
            horizons["name"]     = meta.get("name", ticker)
            horizons["category"] = meta.get("category", "OTHER")
            horizons["inverted"] = meta.get("inverted", False)

        data["generated_at"] = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        data["session_id"]   = str(uuid.uuid4())
        data["asset_order"]  = [a["ticker"] for a in OUTLOOK_ASSETS]
        return data

    def _fallback(self, reason: str = "") -> Dict:
        """Return a skeleton structure when the outlook can't be computed.

        Args:
            reason: human-readable explanation shown in the UI.
                    Defaults to a generic loading message.
        """
        if not reason:
            if not self._client:
                reason = "Outlook unavailable — Claude API not configured."
            else:
                reason = "Generating forecast — this may take up to 60 seconds on first load."
        assets = {}
        for a in OUTLOOK_ASSETS:
            assets[a["ticker"]] = {
                "ticker": a["ticker"], "name": a["name"],
                "category": a["category"], "inverted": a.get("inverted", False),
                "24h": {"direction": "—", "magnitude_score": 1, "magnitude_label": "SMALL",
                        "confidence": 0, "confidence_label": "LOW", "drivers": []},
                "48h": {"direction": "—", "magnitude_score": 1, "magnitude_label": "SMALL",
                        "confidence": 0, "confidence_label": "LOW", "drivers": []},
            }
        return {
            "outlook_summary": reason,
            "market_regime": "NEUTRAL",
            "dominant_themes": [],
            "generated_note": "",
            "assets": assets,
            "asset_order": [a["ticker"] for a in OUTLOOK_ASSETS],
            "generated_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        }


# ---------------------------------------------------------------------------
# Outlook Grader — grades past predictions vs actual price moves
# ---------------------------------------------------------------------------

# Yahoo Finance ticker mapping for each outlook asset
_YF_TICKERS: Dict[str, str] = {
    "SPY":  "SPY",
    "QQQ":  "QQQ",
    "VIX":  "^VIX",
    "GLD":  "GLD",
    "SLV":  "SLV",
    "WTI":  "CL=F",
    "COPX": "COPX",
    "DXY":  "DX-Y.NYB",
    "TLT":  "TLT",
    "BTC":  "BTC-USD",
    "ETH":  "ETH-USD",
    "ITA":  "ITA",
}


def _magnitude_tier(pct_change: float) -> int:
    """Convert absolute % change to magnitude tier 1-4."""
    a = abs(pct_change)
    if a < 0.5:  return 1
    if a < 1.5:  return 2
    if a < 3.0:  return 3
    return 4


def _direction_correct(predicted: str, actual_pct: float, inverted: bool = False) -> bool:
    """Return True if the predicted direction matches the actual move."""
    adj = -actual_pct if inverted else actual_pct
    if predicted == "UP":   return adj > 0.15
    if predicted == "DOWN": return adj < -0.15
    return False


class OutlookGrader:
    """
    Grades persisted Outlook predictions against actual price data.

    Workflow:
      1. `run_grading(db)` — finds ungraded predictions, fetches prices via
         yfinance, scores direction + magnitude, persists grades to DB.
      2. `generate_reflection(grades)` — calls Claude Haiku to write a short
         qualitative post-mortem on the most recent batch of grades.
      3. `get_track_record(db)` — returns the full payload for the UI.
    """

    HAIKU_MODEL = "claude-haiku-4-5"
    LIVE_CACHE_TTL = 120

    def __init__(self, api_key: str = ""):
        key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        self._client = None
        self._live_cache: Optional[Dict[str, Any]] = None
        self._live_cache_time: Optional[datetime] = None
        if key:
            try:
                import anthropic as _ant
                self._client = _ant.Anthropic(api_key=key)
                logger.info("OutlookGrader: Claude Haiku enabled for reflections")
            except Exception as e:
                logger.warning(f"OutlookGrader init error: {e}")

    # ── Price fetching ──────────────────────────────────────────────────

    @staticmethod
    def _fetch_prices(tickers: list, start_dt: datetime, end_dt: datetime) -> Dict[str, Dict[str, float]]:
        """
        Returns {ticker: {date_str: close_price}} for all requested tickers
        over the window [start_dt, end_dt].  Missing tickers return empty dicts.
        """
        try:
            import yfinance as yf
        except ImportError:
            logger.error("yfinance not installed — cannot grade outlook predictions")
            return {}

        result: Dict[str, Dict[str, float]] = {}
        # Fetch a slightly wider window to handle weekends / market closures
        fetch_start = (start_dt - timedelta(days=3)).strftime("%Y-%m-%d")
        fetch_end   = (end_dt   + timedelta(days=3)).strftime("%Y-%m-%d")

        for tkr in tickers:
            yf_sym = _YF_TICKERS.get(tkr, tkr)
            try:
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    df = yf.download(yf_sym, start=fetch_start, end=fetch_end,
                                     interval="1d", progress=False, auto_adjust=True)
                if df.empty:
                    result[tkr] = {}
                    continue
                day_map: Dict[str, float] = {}
                for idx, row in df.iterrows():
                    date_key = idx.strftime("%Y-%m-%d")
                    raw = row["Close"]
                    try:
                        day_map[date_key] = float(raw.iloc[0]) if hasattr(raw, "iloc") else float(raw)
                    except Exception:
                        pass
                result[tkr] = day_map
            except Exception as e:
                logger.warning(f"OutlookGrader: price fetch failed for {yf_sym}: {e}")
                result[tkr] = {}

        return result

    @staticmethod
    def _start_close(day_map: Dict[str, float], target_dt: datetime) -> Optional[float]:
        """
        Find the closing price AT or BEFORE target_dt (prior-close logic).
        Used for the prediction's start price.
        """
        for offset in [0, -1, -2, 1, -3, 2, -4, 3]:
            key = (target_dt + timedelta(days=offset)).strftime("%Y-%m-%d")
            if key in day_map:
                return day_map[key]
        return None

    @staticmethod
    def _end_close(day_map: Dict[str, float], target_dt: datetime) -> Optional[float]:
        """
        Find the closing price AT or AFTER target_dt (next-close logic).
        Used for the prediction's end price so weekends look forward to Monday.
        Returns None if no forward data is available yet (market hasn't closed).
        """
        for offset in [0, 1, 2, 3, -1, 4, -2]:
            key = (target_dt + timedelta(days=offset)).strftime("%Y-%m-%d")
            if key in day_map:
                return day_map[key]
        return None

    @staticmethod
    def _normalize_download_ts(ts: Any) -> Optional[datetime]:
        if ts is None:
            return None
        try:
            if hasattr(ts, "to_pydatetime"):
                dt = ts.to_pydatetime()
            elif isinstance(ts, datetime):
                dt = ts
            else:
                return None
        except Exception:
            return None
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    def get_live_price_snapshot(self) -> Dict[str, Any]:
        """
        Best-effort current price snapshot across all tracked Outlook assets.
        Used to verify that tracked asset prices are fresh and observable in UI.
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if (
            self._live_cache is not None
            and self._live_cache_time is not None
            and (now - self._live_cache_time).total_seconds() < self.LIVE_CACHE_TTL
        ):
            return self._live_cache

        try:
            import yfinance as yf
        except ImportError:
            logger.error("yfinance not installed — cannot build live price snapshot")
            payload = {
                "captured_at": now.isoformat(),
                "source": "yfinance",
                "assets": {},
                "summary": {"live": 0, "delayed": 0, "stale": 0, "missing": len(OUTLOOK_ASSETS)},
            }
            self._live_cache = payload
            self._live_cache_time = now
            return payload

        assets: Dict[str, Dict[str, Any]] = {}
        counts = {"live": 0, "delayed": 0, "stale": 0, "missing": 0}

        for asset in OUTLOOK_ASSETS:
            ticker = asset["ticker"]
            yf_sym = _YF_TICKERS.get(ticker, ticker)

            latest_price = None
            latest_dt = None
            source_interval = None

            try:
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    intraday = yf.download(
                        yf_sym,
                        period="2d",
                        interval="1m",
                        progress=False,
                        auto_adjust=False,
                        prepost=True,
                    )
                if intraday is not None and not intraday.empty:
                    close_series = intraday["Close"]
                    try:
                        close_series = close_series.dropna()
                    except Exception:
                        pass
                    if hasattr(close_series, "empty") and not close_series.empty:
                        raw_price = close_series.iloc[-1]
                        raw_ts = close_series.index[-1]
                        latest_price = float(raw_price.iloc[0]) if hasattr(raw_price, "iloc") else float(raw_price)
                        latest_dt = self._normalize_download_ts(raw_ts)
                        source_interval = "1m"
            except Exception as e:
                logger.debug(f"OutlookGrader live 1m fetch failed for {yf_sym}: {e}")

            if latest_price is None or latest_dt is None:
                try:
                    import warnings
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        daily = yf.download(
                            yf_sym,
                            period="10d",
                            interval="1d",
                            progress=False,
                            auto_adjust=False,
                        )
                    if daily is not None and not daily.empty:
                        close_series = daily["Close"]
                        try:
                            close_series = close_series.dropna()
                        except Exception:
                            pass
                        if hasattr(close_series, "empty") and not close_series.empty:
                            raw_price = close_series.iloc[-1]
                            raw_ts = close_series.index[-1]
                            latest_price = float(raw_price.iloc[0]) if hasattr(raw_price, "iloc") else float(raw_price)
                            latest_dt = self._normalize_download_ts(raw_ts)
                            source_interval = "1d"
                except Exception as e:
                    logger.debug(f"OutlookGrader live 1d fetch failed for {yf_sym}: {e}")

            if latest_price is None or latest_dt is None:
                counts["missing"] += 1
                assets[ticker] = {
                    "ticker": ticker,
                    "source_symbol": yf_sym,
                    "price": None,
                    "timestamp": None,
                    "age_minutes": None,
                    "freshness": "missing",
                    "source_interval": source_interval,
                }
                continue

            age_minutes = max(0.0, (now - latest_dt).total_seconds() / 60.0)
            if age_minutes <= 20:
                freshness = "live"
            elif age_minutes <= 360:
                freshness = "delayed"
            else:
                freshness = "stale"
            counts[freshness] += 1

            assets[ticker] = {
                "ticker": ticker,
                "source_symbol": yf_sym,
                "price": round(latest_price, 6),
                "timestamp": latest_dt.isoformat(),
                "age_minutes": round(age_minutes, 2),
                "freshness": freshness,
                "source_interval": source_interval,
            }

        payload = {
            "captured_at": now.isoformat(),
            "source": "yfinance",
            "assets": assets,
            "summary": counts,
        }
        self._live_cache = payload
        self._live_cache_time = now
        return payload

    # ── Grading ─────────────────────────────────────────────────────────

    def _grade_one(
        self,
        session_id: str,
        generated_at_str: str,
        assets_json_str: str,
        horizon: str,
    ) -> Optional[Dict]:
        """
        Grade a single prediction/horizon pair.
        Returns a grade payload dict or None if prices are unavailable.
        """
        generated_at  = datetime.fromisoformat(generated_at_str)
        horizon_hours = 24 if horizon == "24h" else 48
        end_dt        = generated_at + timedelta(hours=horizon_hours)

        if end_dt > datetime.now(timezone.utc).replace(tzinfo=None):
            return None   # Too early to grade

        assets = json.loads(assets_json_str)
        tickers = list(assets.keys())

        price_data = self._fetch_prices(tickers, generated_at, end_dt)
        if not price_data:
            return None

        asset_meta = {a["ticker"]: a for a in OUTLOOK_ASSETS}
        grades: Dict[str, Dict] = {}
        correct = total = 0

        for ticker, asset_info in assets.items():
            pred = asset_info.get(horizon, {})
            if not pred or pred.get("direction") in ("—", None, ""):
                continue

            day_map    = price_data.get(ticker, {})
            price_open = self._start_close(day_map, generated_at)
            price_end  = self._end_close(day_map, end_dt)

            # Skip if prices unavailable or identical (same-day stale data)
            if price_open is None or price_end is None or price_open == 0:
                continue
            if abs(price_end - price_open) < 1e-8 and price_open == price_end:
                continue  # identical → no market data for end window yet

            actual_pct   = (price_end - price_open) / price_open * 100
            pred_dir     = pred.get("direction", "")
            pred_mag     = pred.get("magnitude_score", 1)
            inverted     = asset_meta.get(ticker, {}).get("inverted", False)

            dir_ok       = _direction_correct(pred_dir, actual_pct, inverted)
            actual_mag   = _magnitude_tier(actual_pct)
            mag_diff     = abs(actual_mag - pred_mag)
            mag_score    = max(0.0, 1.0 - mag_diff * 0.4)
            composite    = round(dir_ok * 0.7 + mag_score * 0.3, 3)

            grades[ticker] = {
                "predicted_direction":       pred_dir,
                "actual_direction":          "UP" if actual_pct > 0 else "DOWN",
                "predicted_magnitude":       pred_mag,
                "predicted_magnitude_label": MAGNITUDE_LABELS.get(pred_mag, "SMALL"),
                "actual_magnitude":          actual_mag,
                "actual_magnitude_label":    MAGNITUDE_LABELS.get(actual_mag, "SMALL"),
                "actual_change_pct":         round(actual_pct, 2),
                "direction_correct":         dir_ok,
                "magnitude_score":           round(mag_score, 3),
                "composite_score":           composite,
                "price_start":               round(price_open, 4),
                "price_end":                 round(price_end, 4),
                "confidence":                pred.get("confidence", 0),
                "confidence_label":          pred.get("confidence_label", ""),
                "drivers":                   pred.get("drivers", []),
            }

            if dir_ok:
                correct += 1
            total += 1

        if not grades:
            return None

        dir_acc = round(correct / total, 3) if total else 0.0
        overall = round(sum(g["composite_score"] for g in grades.values()) / len(grades), 3)

        return {
            "session_id":         session_id,
            "horizon":            horizon,
            "direction_accuracy": dir_acc,
            "overall_score":      overall,
            "grades":             grades,
            "total_graded":       total,
            "total_correct":      correct,
        }

    def run_grading(self, db) -> int:
        """
        Check all pending predictions and grade any that are old enough.
        Returns the number of new grade rows written.
        """
        new_grades = 0
        for horizon in ("24h", "48h"):
            pending = db.get_ungraded_predictions(horizon)
            for pred in pending:
                try:
                    result = self._grade_one(
                        session_id       = pred["session_id"],
                        generated_at_str = pred["generated_at"],
                        assets_json_str  = pred["assets_json"],
                        horizon          = horizon,
                    )
                    if result:
                        db.save_outlook_grade(
                            session_id         = result["session_id"],
                            horizon            = horizon,
                            graded_at          = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
                            overall_score      = result["overall_score"],
                            direction_accuracy = result["direction_accuracy"],
                            grades_json        = json.dumps(result["grades"]),
                            reflection         = "",
                        )
                        logger.info(
                            f"OutlookGrader: graded {result['session_id'][:8]}…/{horizon} "
                            f"— {result['total_correct']}/{result['total_graded']} correct "
                            f"({result['direction_accuracy']*100:.0f}%)"
                        )
                        new_grades += 1
                except Exception as e:
                    logger.warning(f"OutlookGrader: grading error for {pred.get('session_id')}: {e}")

        # After grading, attach a fresh reflection to the most recent grade
        if new_grades > 0:
            self._refresh_reflection(db)

        return new_grades

    # ── Reflection ───────────────────────────────────────────────────────

    def _refresh_reflection(self, db):
        """Generate a Claude Haiku reflection and attach it to the latest grade."""
        recent = db.get_outlook_grades(limit=10)
        if not recent:
            return
        reflection = self._generate_reflection(recent)
        if reflection:
            db.update_outlook_grade_reflection(recent[0]["id"], reflection)

    def _generate_reflection(self, grades_list: list) -> str:
        """Call Claude Haiku for a 3-sentence post-mortem on recent grades."""
        if not self._client or not grades_list:
            return ""

        lines = []
        for g in grades_list[:6]:
            horizon = g.get("horizon", "?")
            acc     = (g.get("direction_accuracy") or 0) * 100
            try:
                grades = json.loads(g.get("grades_json") or "{}")
            except Exception:
                grades = {}
            wrong = [t for t, v in grades.items() if not v.get("direction_correct")]
            right = [t for t, v in grades.items() if v.get("direction_correct")]
            date  = (g.get("pred_generated_at") or "")[:10]
            regime = g.get("pred_regime") or "?"
            lines.append(
                f"• {date} {horizon} [{regime}]: {acc:.0f}% accuracy — "
                f"correct: {right or 'none'}, wrong: {wrong or 'none'}"
            )

        block = "\n".join(lines)
        try:
            msg = self._client.messages.create(
                model      = self.HAIKU_MODEL,
                max_tokens = 350,
                system     = (
                    "You are a senior quant analyst reviewing an AI prediction model's "
                    "track record on asset price direction calls. Be direct and specific."
                ),
                messages=[{"role": "user", "content": (
                    f"Here are recent prediction results:\n{block}\n\n"
                    "Write exactly 3 sentences:\n"
                    "1. Which assets / conditions the model predicted best and why.\n"
                    "2. Where it consistently fails and the likely cause.\n"
                    "3. One specific, actionable change to improve accuracy.\n"
                    "No preamble, no bullet points — just 3 plain sentences."
                )}],
            )
            return msg.content[0].text.strip()
        except Exception as e:
            logger.warning(f"OutlookGrader reflection error: {e}")
            return ""

    # ── Track record payload ─────────────────────────────────────────────

    def get_track_record(self, db) -> Dict:
        """
        Run pending grading then return the full track-record payload for
        the /api/outlook/track-record endpoint.
        """
        new = self.run_grading(db)

        stats       = db.get_outlook_track_record_stats()
        grades      = db.get_outlook_grades(limit=30)
        reflection  = db.get_latest_outlook_reflection()
        total_preds = db.count_outlook_predictions()

        # Parse grades_json back to dicts for the API response
        parsed_grades = []
        for g in grades:
            try:
                g = dict(g)
                g["grades"] = json.loads(g.pop("grades_json", "{}"))
            except Exception:
                g["grades"] = {}
            # Compute total_graded / total_correct from grades dict
            asset_grades = g["grades"]
            g["total_graded"]  = len(asset_grades)
            g["total_correct"] = sum(1 for v in asset_grades.values() if v.get("direction_correct"))
            # Parse dominant_themes
            try:
                g["dominant_themes"] = json.loads(g.get("dominant_themes") or "[]")
            except Exception:
                g["dominant_themes"] = []
            parsed_grades.append(g)

        return {
            "stats":              stats,
            "grades":             parsed_grades,
            "latest_reflection":  reflection,
            "total_predictions":  total_preds,
            "new_grades":         new,
            "server_time":        datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        }
