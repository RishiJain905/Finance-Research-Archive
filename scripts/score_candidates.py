"""Candidate scoring module for V2 shared candidate layer.

This module scores candidates based on positive and negative signals
to determine relevance and quality.
"""

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from scripts.candidate_utils import BASE_DIR, load_json


# Configuration paths
DOMAIN_TRUST_TIERS_PATH = BASE_DIR / "config" / "domain_trust_tiers.json"

# Positive URL keywords (add to url_score)
POSITIVE_URL_KEYWORDS = [
    "press",
    "statement",
    "report",
    "speech",
    "research",
    "analysis",
    "release",
    "announcement",
    "decision",
]

# Positive anchor/key terms (add to anchor_score)
POSITIVE_ANCHOR_TERMS = [
    "monetary policy",
    "inflation",
    "rates",
    "liquidity",
    "treasury",
    "repo",
    "labor market",
    "gdp",
    "pce",
    "volatility",
    "fed funds",
    "interest rate",
    "fomc",
    "central bank",
    "reserve",
    "balance sheet",
    "quantitative",
]

# Negative URL patterns (subtract from url_score)
NEGATIVE_URL_PATTERNS = [
    "about",
    "careers",
    "experts",
    "events",
    "category",
    "tag",
    "archive",
    "subscribe",
    "webinar",
    "podcast",
    "career",
    "jobs",
    "people",
    "team",
    "contact",
    "faq",
    "help",
    "search",
    "login",
    "signin",
    "register",
]

# Non-relevant terms that might appear in title/anchor (subtract from anchor_score)
NON_RELEVANT_TERMS = [
    "advertisement",
    "sponsored",
    "promoted",
    "newsletter",
    "subscribe",
    "donate",
    "support",
    "about us",
    "our team",
]

# Minimum content word count
MIN_CONTENT_WORDS = 500

# Freshness window (7 days)
FRESHNESS_WINDOW_DAYS = 7


def load_domain_trust_tiers() -> dict[str, list[str]]:
    """Load domain trust tiers configuration.

    Returns:
        Dict with 'high', 'medium', 'low' keys mapping to domain lists
    """
    if not DOMAIN_TRUST_TIERS_PATH.exists():
        return {"high": [], "medium": [], "low": []}

    with open(DOMAIN_TRUST_TIERS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def get_domain_trust_score(domain: str) -> tuple[int, str]:
    """Get trust score for a domain.

    Args:
        domain: Source domain

    Returns:
        Tuple of (trust_score, trust_tier) where trust_score is 0-100
    """
    if not domain:
        return 0, "low"

    # Normalize domain
    domain_lower = domain.lower()

    trust_tiers = load_domain_trust_tiers()

    # Check high trust
    if domain_lower in trust_tiers.get("high", []):
        return 100, "high"

    # Check medium trust
    if domain_lower in trust_tiers.get("medium", []):
        return 50, "medium"

    # Check low trust
    if domain_lower in trust_tiers.get("low", []):
        return 25, "low"

    # Default low for unknown domains
    return 10, "low"


def score_url(candidate: dict[str, Any]) -> int:
    """Score candidate based on URL characteristics.

    Args:
        candidate: Candidate dict with url field

    Returns:
        URL score (positive - negative, typically -10 to +30)
    """
    url = candidate.get("url", "").lower()
    if not url:
        return 0

    score = 0

    # Positive: URL contains relevant keywords
    for keyword in POSITIVE_URL_KEYWORDS:
        if keyword in url:
            score += 5

    # Negative: URL contains navigation/non-content patterns
    for pattern in NEGATIVE_URL_PATTERNS:
        if pattern in url:
            score -= 5

    # Check if URL path looks article-like
    # Articles typically have path segments like /2024/01/15/article-title
    # or end with .html, .htm, .aspx, etc.
    url_path_match = re.search(
        r"/([^/]+\.(html?|aspx?|php|htm))|/(\d{4}/\d{2}/\d{2})/", url
    )
    if url_path_match:
        score += 5

    return score


def score_anchor_text(candidate: dict[str, Any]) -> int:
    """Score candidate based on anchor text and title.

    Args:
        candidate: Candidate dict with anchor_text and title fields

    Returns:
        Anchor score (positive - negative, typically -10 to +30)
    """
    anchor = candidate.get("anchor_text", "").lower()
    title = candidate.get("title", "").lower()

    if not anchor and not title:
        return 0

    # Combine for scoring
    text_to_score = f"{anchor} {title}"
    score = 0

    # Positive: Contains relevant financial/macro terms
    for term in POSITIVE_ANCHOR_TERMS:
        if term in text_to_score:
            score += 5

    # Negative: Contains non-relevant terms
    for term in NON_RELEVANT_TERMS:
        if term in text_to_score:
            score -= 5

    return score


def score_freshness(candidate: dict[str, Any]) -> int:
    """Score candidate based on publication freshness.

    Args:
        candidate: Candidate dict with metadata.published_at or discovered_at

    Returns:
        Freshness score (0-20 based on age)
    """
    from datetime import timezone

    # Try published_at first
    published_at = candidate.get("metadata", {}).get("published_at")

    # Fall back to discovered_at
    if not published_at:
        published_at = candidate.get("discovered_at")

    if not published_at:
        return 0

    try:
        # Try parsing ISO format
        pub_date = None
        if isinstance(published_at, str):
            # Handle various ISO formats - remove Z suffix and parse
            clean_at = published_at.rstrip("Z")

            # Handle timezone offset if present (e.g., +00:00)
            tz_offset = None
            if "+" in clean_at or clean_at.endswith("-") and clean_at[-6:-5].isdigit():
                # Has timezone offset
                if clean_at[-6:-5] in ["+", "-"]:
                    tz_offset_str = clean_at[-6:]
                    clean_at = clean_at[:-6]
                    # Parse offset
                    tz_sign = 1 if tz_offset_str[0] == "+" else -1
                    tz_hours, tz_mins = int(tz_offset_str[1:3]), int(tz_offset_str[4:6])
                    tz_offset = timezone(
                        timedelta(hours=tz_sign * tz_hours, minutes=tz_sign * tz_mins)
                    )

            # Try parsing with various format strings
            for fmt in [
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d",
            ]:
                try:
                    pub_date = datetime.strptime(clean_at, fmt)
                    break
                except ValueError:
                    continue

            if pub_date:
                # Apply timezone offset or assume UTC
                if tz_offset:
                    pub_date = pub_date.replace(tzinfo=tz_offset)
                else:
                    pub_date = pub_date.replace(tzinfo=timezone.utc)
        else:
            pub_date = published_at

        if pub_date is None or pub_date.tzinfo is None:
            return 0

        # Calculate age using timezone-aware now
        now = datetime.now(timezone.utc)
        age = now - pub_date
        age_days = age.total_seconds() / (24 * 60 * 60)

        # Score based on freshness
        if age_days <= FRESHNESS_WINDOW_DAYS:
            return 20
        elif age_days <= 14:
            return 10
        elif age_days <= 30:
            return 5
        else:
            return 0

    except Exception:
        return 0


def score_content_length(candidate: dict[str, Any]) -> int:
    """Score based on content length.

    Args:
        candidate: Candidate dict with metadata.word_count or raw_text_path

    Returns:
        Content score (-10 to 0)
    """
    # Try word_count metadata first
    word_count = candidate.get("metadata", {}).get("word_count", 0)

    # Fall back to reading raw text
    if word_count == 0:
        raw_text_path = candidate.get("raw_text_path", "")
        if raw_text_path:
            text_path = Path(raw_text_path)
            if text_path.exists():
                try:
                    with open(text_path, "r", encoding="utf-8") as f:
                        content = f.read()
                    word_count = len(content.split())
                except Exception:
                    word_count = 0

    if word_count == 0:
        return -10
    elif word_count < MIN_CONTENT_WORDS:
        # Penalize short content
        return -5
    else:
        return 0


def score_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    """Score a candidate and return updated candidate with candidate_scores filled.

    Args:
        candidate: Candidate dict with all relevant fields

    Returns:
        Candidate with updated candidate_scores containing all component scores
            and total_score
    """
    # Ensure candidate_scores structure exists
    if "candidate_scores" not in candidate:
        candidate["candidate_scores"] = {}

    scores = candidate["candidate_scores"]

    # Calculate component scores
    domain = candidate.get("source", {}).get("domain", "")
    domain_trust_score, trust_tier = get_domain_trust_score(domain)

    url_score = score_url(candidate)
    anchor_score = score_anchor_text(candidate)
    freshness_score = score_freshness(candidate)
    content_length_score = score_content_length(candidate)

    # Keyword score is derived from URL + anchor positive matches
    keyword_score = sum(
        [5 for kw in POSITIVE_URL_KEYWORDS if kw in candidate.get("url", "").lower()]
    )

    # Calculate total
    total_score = (
        domain_trust_score
        + url_score
        + anchor_score
        + freshness_score
        + content_length_score
        + keyword_score
    )

    # Update candidate_scores
    scores["url_score"] = url_score
    scores["anchor_score"] = anchor_score
    scores["domain_trust_score"] = domain_trust_score
    scores["keyword_score"] = keyword_score
    scores["freshness_score"] = freshness_score
    scores["content_length_score"] = content_length_score
    scores["total_score"] = total_score
    scores["trust_tier"] = trust_tier

    # Also update source trust_tier
    if "source" in candidate:
        candidate["source"]["trust_tier"] = trust_tier

    return candidate


def filter_by_score(
    candidates: list[dict[str, Any]], threshold: float = 0.0
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Filter candidates by score threshold.

    Args:
        candidates: List of candidates (should have candidate_scores.total_score)
        threshold: Minimum score to pass filter (default 0.0)

    Returns:
        Tuple of (survivors, filtered_out) lists
    """
    survivors = []
    filtered_out = []

    for candidate in candidates:
        # Ensure candidate has been scored
        if (
            "candidate_scores" not in candidate
            or "total_score" not in candidate["candidate_scores"]
        ):
            candidate = score_candidate(candidate)

        total = candidate["candidate_scores"].get("total_score", 0)

        if total >= threshold:
            candidate["filter_status"] = "passed_score_threshold"
            survivors.append(candidate)
        else:
            candidate["filter_status"] = "failed_score_threshold"
            filtered_out.append(candidate)

    return survivors, filtered_out
