"""
Find quant records related to a given article record.

Provides:
- find_related_quant_records(): Find quant records related to an article
"""

from datetime import datetime, timedelta
from typing import Optional
import re


# Quant series IDs from config/quant_sources.json
QUANT_SERIES_IDS = frozenset(
    {
        "sofr",
        "fed_funds",
        "iorb",
        "2y_yield",
        "10y_yield",
        "treasury_auctions",
        "upcoming_treasury_auctions",
        "repo_operations",
    }
)

# Quant source types
QUANT_SOURCE_TYPES = frozenset({"quant_snapshot", "dataset_snapshot"})


def is_quant_record(record: dict) -> bool:
    """Check if a record is a quant record.

    Args:
        record: Record dict with id, source.source_type, etc.

    Returns:
        True if record is a quant record
    """
    # Check source_type
    source_type = record.get("source", {}).get("source_type", "")
    if source_type in QUANT_SOURCE_TYPES:
        return True

    # Check record_id against known quant series patterns
    record_id = record.get("id", "")
    for series_id in QUANT_SERIES_IDS:
        if record_id.startswith(series_id + "_") or record_id == series_id:
            return True

    return False


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse a date string in various formats.

    Supports: ISO 8601, "MM/DD/YYYY", "YYYY_MM_DD", "YYYY-MM-DD"

    Args:
        date_str: Date string to parse

    Returns:
        datetime object or None if parsing fails
    """
    if not date_str or not isinstance(date_str, str):
        return None

    date_str = date_str.strip()

    # Try ISO 8601 format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
    iso_formats = [
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in iso_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            # Normalize to naive datetime (strip timezone info)
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            return dt
        except ValueError:
            pass

    # Try MM/DD/YYYY
    try:
        return datetime.strptime(date_str, "%m/%d/%Y")
    except ValueError:
        pass

    # Try YYYY_MM_DD
    try:
        return datetime.strptime(date_str, "%Y_%m_%d")
    except ValueError:
        pass

    # Try with additional content after date (e.g., "3/18/2026 (FOMC statement)")
    match = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str)
    if match:
        month, day, year = match.groups()
        try:
            return datetime(int(year), int(month), int(day))
        except ValueError:
            pass

    return None


def is_business_day(date: datetime) -> bool:
    """Check if a date is a business day (Monday-Friday).

    Args:
        date: datetime object

    Returns:
        True if weekday (Mon=0, Sun=6), False if weekend
    """
    return date.weekday() < 5


def compute_time_proximity_score(date1_str: str, date2_str: str) -> float:
    """Compute time proximity score between two dates.

    Same day: 100, ±1 business day: 80, ±3 days: 60, ±7 days: 40, ±30 days: 20, beyond: 0

    Args:
        date1_str: First date string
        date2_str: Second date string

    Returns:
        Time proximity score (0-100)
    """
    date1 = parse_date(date1_str)
    date2 = parse_date(date2_str)

    if date1 is None or date2 is None:
        return 0.0

    # Normalize to date only (remove time component)
    date1 = date1.replace(hour=0, minute=0, second=0, microsecond=0)
    date2 = date2.replace(hour=0, minute=0, second=0, microsecond=0)

    delta_days = abs((date1 - date2).days)

    # Count business days between dates
    business_days = 0
    if delta_days > 0:
        # Iterate through days and count business days
        smaller_date = min(date1, date2)
        for i in range(delta_days):
            check_date = smaller_date + timedelta(days=i)
            if is_business_day(check_date):
                business_days += 1

    # Score based on business day distance
    if delta_days == 0:
        return 100.0
    elif business_days <= 1:
        return 80.0
    elif business_days <= 3:
        return 60.0
    elif business_days <= 7:
        return 40.0
    elif business_days <= 30:
        return 20.0
    else:
        return 0.0


def compute_topic_overlap_score(
    record1: dict, record2: dict
) -> tuple[float, list[str]]:
    """Compute topic overlap score and return shared topics.

    Exact topic field match: 50 points
    Tag overlap: (shared_tags / max(total_tags_1, total_tags_2)) * 30 points

    Args:
        record1: First record dict
        record2: Second record dict

    Returns:
        Tuple of (score, list of shared topics/tags)
    """
    score = 0.0
    shared = []

    # Exact topic match
    topic1 = (record1.get("topic") or "").strip().lower()
    topic2 = (record2.get("topic") or "").strip().lower()

    if topic1 and topic2 and topic1 == topic2:
        score += 50.0
        shared.append(record1.get("topic", ""))

    # Tag overlap
    tags1 = set(record1.get("tags") or [])
    tags2 = set(record2.get("tags") or [])

    shared_tags = tags1 & tags2
    if shared_tags:
        shared.extend(list(shared_tags))

    if tags1 and tags2:
        max_tags = max(len(tags1), len(tags2))
        if max_tags > 0:
            tag_score = (len(shared_tags) / max_tags) * 30.0
            score += tag_score

    return score, shared


def compute_compatibility_bonus(article_record: dict, quant_record: dict) -> float:
    """Compute source/event type compatibility bonus.

    policy_statement → rates snapshots: +15
    Treasury refunding → auction/funding snapshots: +15
    liquidity article → SOFR/repo/reserves: +15

    Args:
        article_record: Article record dict
        quant_record: Quant record dict

    Returns:
        Compatibility bonus score (0-15)
    """
    bonus = 0.0

    article_event_type = (article_record.get("event_type") or "").lower()
    article_topic = (article_record.get("topic") or "").lower()
    article_tags = set(article_record.get("tags") or [])

    quant_topic = (quant_record.get("topic") or "").lower()
    quant_tags = set(quant_record.get("tags") or [])
    quant_id = (quant_record.get("id") or "").lower()

    # Monetary policy article → rates quant records
    monetary_keywords = {"monetary", "policy", "fomc", "federal_reserve"}
    rates_keywords = {"rates", "fed_funds", "iorb", "macro catalysts"}

    article_is_monetary = (
        any(kw in article_event_type for kw in monetary_keywords)
        or any(kw in article_topic for kw in {"monetary", "policy"})
        or any(
            kw in article_tags for kw in {"monetary_policy", "fomc", "federal_reserve"}
        )
    )

    quant_is_rates = (
        quant_topic in rates_keywords
        or any(kw in quant_tags for kw in rates_keywords)
        or quant_id.startswith(("fed_funds_", "iorb_"))
    )

    if article_is_monetary and quant_is_rates:
        bonus += 15.0

    # Treasury article → auction/funding quant records
    treasury_keywords = {"treasury", "issuance", "funding", "refunding"}

    article_is_treasury = any(kw in article_topic for kw in treasury_keywords) or any(
        kw in article_tags
        for kw in {"treasury", "treasury_auction", "issuance", "funding"}
    )

    quant_is_auction = "treasury_auction" in quant_id or "upcoming_treasury" in quant_id

    if article_is_treasury and quant_is_auction:
        bonus += 15.0

    # Liquidity/market structure article → SOFR/repo/reserves quant records
    liquidity_keywords = {
        "liquidity",
        "repo",
        "market_structure",
        "funding",
        "reserves",
        "sofr",
    }

    article_is_liquidity = article_topic == "market structure" or any(
        kw in article_tags for kw in liquidity_keywords
    )

    quant_is_sofr = (
        quant_id.startswith(("sofr_", "repo_operations_"))
        or quant_topic == "market structure"
        or any(kw in quant_tags for kw in {"sofr", "repo", "liquidity"})
    )

    if article_is_liquidity and quant_is_sofr:
        bonus += 15.0

    return bonus


def compute_link_score(
    article_record: dict, quant_record: dict
) -> tuple[float, list[str], str]:
    """Compute combined link score and return (score, topic_overlap, reason).

    Args:
        article_record: Article record dict
        quant_record: Quant record dict

    Returns:
        Tuple of (total score, shared topics, human-readable reason)
    """
    # Time proximity score
    article_date = article_record.get("source", {}).get("published_at", "")
    quant_date = quant_record.get("source", {}).get("published_at", "")

    time_score = compute_time_proximity_score(article_date, quant_date)

    # Topic overlap score
    topic_score, shared_topics = compute_topic_overlap_score(
        article_record, quant_record
    )

    # Compatibility bonus
    compatibility_bonus = compute_compatibility_bonus(article_record, quant_record)

    # Total score
    total_score = time_score + topic_score + compatibility_bonus

    # Build reason string
    reason_parts = []
    if time_score >= 100:
        reason_parts.append("same-day")
    elif time_score >= 80:
        reason_parts.append("near-day")
    elif time_score >= 60:
        reason_parts.append("within 3 days")
    elif time_score >= 40:
        reason_parts.append("within a week")
    elif time_score >= 20:
        reason_parts.append("within a month")

    if topic_score >= 50:
        reason_parts.append("exact topic match")
    elif topic_score >= 15:
        reason_parts.append("topic overlap")

    if compatibility_bonus > 0:
        reason_parts.append("complementary context")

    reason = ", ".join(reason_parts) if reason_parts else "proximity match"

    return total_score, shared_topics, reason


def find_related_quant_records(
    article_record: dict,
    quant_records: list[dict],
    top_n: int = 3,
    min_score: float = 50.0,
) -> list[dict]:
    """Find quant records related to a given article record.

    Args:
        article_record: Article record dict to find related quants for
        quant_records: List of quant record dicts to search
        top_n: Maximum number of related quants to return
        min_score: Minimum link score threshold

    Returns:
        List of link entry dicts with keys:
        - record_id: str
        - relationship: str
        - reason: str
        - topic_overlap: list[str]
        - link_score: float
    """
    if not article_record or not quant_records:
        return []

    links = []

    for quant in quant_records:
        score, shared_topics, reason = compute_link_score(article_record, quant)

        if score >= min_score:
            # Determine relationship type based on time proximity
            article_date = article_record.get("source", {}).get("published_at", "")
            quant_date = quant.get("source", {}).get("published_at", "")
            time_score = compute_time_proximity_score(article_date, quant_date)

            if time_score >= 80:
                relationship = "nearest_relevant_quant_snapshot"
            else:
                relationship = "temporally_proximate_quant"

            links.append(
                {
                    "record_id": quant.get("id", ""),
                    "relationship": relationship,
                    "reason": reason,
                    "topic_overlap": shared_topics,
                    "link_score": round(score, 1),
                }
            )

    # Sort by score descending and take top_n
    links.sort(key=lambda x: x["link_score"], reverse=True)
    return links[:top_n]
