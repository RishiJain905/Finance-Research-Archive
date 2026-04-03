"""
Deterministic Watchlist Matcher for Phase 2.7 Part 3.

This module provides functions for matching research records and event clusters
against user-defined watchlists to identify relevant market themes and signals.
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def load_watchlists(config_path: str) -> list[dict]:
    """Load watchlists from config JSON file. Returns list of enabled watchlists.

    Args:
        config_path: Path to the watchlists JSON config file.

    Returns:
        List of enabled watchlist dictionaries.

    Raises:
        FileNotFoundError: If config file does not exist.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        all_watchlists = json.load(f)

    return [wl for wl in all_watchlists if wl.get("enabled", False)]


def extract_text_features(record_or_cluster: dict) -> dict:
    """Extract text fields from a record or cluster for matching.
    Returns dict with: title, summary, why_it_matters, tags, important_numbers, topic, event_type
    Handles both record schema and event_cluster schema.

    Args:
        record_or_cluster: Either a research record or event cluster dictionary.

    Returns:
        Dictionary with extracted text features for matching.
    """
    # Check if this is a cluster (has event_id) or record (has id)
    is_cluster = "event_id" in record_or_cluster

    if is_cluster:
        # Event cluster schema
        return {
            "title": record_or_cluster.get("title", ""),
            "summary": record_or_cluster.get("summary", ""),
            "why_it_matters": "",  # Clusters don't have why_it_matters
            "tags": record_or_cluster.get("keywords", []),  # clusters use keywords
            "important_numbers": record_or_cluster.get("important_numbers", []),
            "topic": record_or_cluster.get("topic", ""),
            "event_type": record_or_cluster.get("event_type", ""),
        }
    else:
        # Research record schema
        return {
            "title": record_or_cluster.get("title", ""),
            "summary": record_or_cluster.get("summary", ""),
            "why_it_matters": record_or_cluster.get("why_it_matters", ""),
            "tags": record_or_cluster.get("tags", []),
            "important_numbers": record_or_cluster.get("important_numbers", []),
            "topic": record_or_cluster.get("topic", ""),
            "event_type": record_or_cluster.get("event_type", ""),
        }


def tokenize(text: str) -> set[str]:
    """Tokenize text into lowercase terms. Extract alphanumeric tokens.

    Args:
        text: Input text to tokenize.

    Returns:
        Set of lowercase alphanumeric tokens.
    """
    if not text:
        return set()

    # Find all alphanumeric sequences (including numbers with decimals/percent)
    tokens = re.findall(r"[a-zA-Z0-9.]+%|[a-zA-Z0-9]+", text.lower())
    return set(tokens)


def compute_keyword_overlap(
    text_terms: set, watchlist: dict
) -> tuple[float, list[str]]:
    """Compute keyword overlap score and return (score, matched_terms).
    Score = matched_keywords / total_keywords (0-1).

    Args:
        text_terms: Set of tokenized terms from the text.
        watchlist: Watchlist dictionary with keywords list.

    Returns:
        Tuple of (overlap score 0-1, list of matched keywords).
    """
    keywords = watchlist.get("keywords", [])
    if not keywords:
        return 0.0, []

    # Lowercase text_terms for case-insensitive comparison
    text_terms_lower = {t.lower() for t in text_terms}

    # Tokenize keywords and find matches (case-insensitive)
    matched_terms = []
    for keyword in keywords:
        keyword_tokens = tokenize(keyword)
        # Check if any keyword token is in text_terms (both lowercased)
        if keyword_tokens and keyword_tokens.intersection(text_terms_lower):
            matched_terms.append(keyword)

    score = len(matched_terms) / len(keywords) if keywords else 0.0
    return score, matched_terms


def check_required_terms(text_terms: set, watchlist: dict) -> bool:
    """Check if all required_terms are present in text_terms.
    Returns True if no required terms or all present, False if any missing.

    Args:
        text_terms: Set of tokenized terms from the text.
        watchlist: Watchlist dictionary with required_terms list.

    Returns:
        True if all required terms are present or no required terms exist.
    """
    required_terms = watchlist.get("required_terms", [])
    if not required_terms:
        return True

    for term in required_terms:
        term_tokens = tokenize(term)
        # If any required term token is missing, fail
        if term_tokens and not term_tokens.intersection(text_terms):
            return False

    return True


def check_blocked_terms(text_terms: set, watchlist: dict) -> set[str]:
    """Return set of blocked terms found in text_terms.

    Args:
        text_terms: Set of tokenized terms from the text.
        watchlist: Watchlist dictionary with blocked_terms list.

    Returns:
        Set of blocked terms that were found in the text.
    """
    blocked_terms_config = watchlist.get("blocked_terms", [])
    if not blocked_terms_config:
        return set()

    found_blocked = set()
    for term in blocked_terms_config:
        term_tokens = tokenize(term)
        if term_tokens and term_tokens.intersection(text_terms):
            found_blocked.add(term)

    return found_blocked


def check_topic_compatibility(record_topic: str, watchlist_topic: str) -> bool:
    """Check if record topic is compatible with watchlist topic.
    Returns True if topics match or either is empty/missing.

    Args:
        record_topic: Topic string from the record/cluster.
        watchlist_topic: Topic string from the watchlist.

    Returns:
        True if topics are compatible (match or either empty).
    """
    # Treat None as empty
    if record_topic is None:
        record_topic = ""
    if watchlist_topic is None:
        watchlist_topic = ""

    # Empty topics are always compatible
    if not record_topic or not watchlist_topic:
        return True

    return record_topic == watchlist_topic


def check_event_type_compatibility(record_event_type: str, watchlist: dict) -> bool:
    """Check if record event_type is compatible with watchlist.
    For now, always returns True (event type compatibility is reserved for future).

    Args:
        record_event_type: Event type string from the record/cluster.
        watchlist: Watchlist dictionary (not used currently).

    Returns:
        Always True (reserved for future use).
    """
    return True


def determine_thesis_signal(
    matched_terms: list, blocked_terms: set, score: float
) -> str:
    """Determine thesis signal direction.
    - If blocked_terms found → "weakening"
    - If score > 0.5 and matched_terms → "strengthening"
    - Otherwise → "neutral"

    Args:
        matched_terms: List of keywords that matched.
        blocked_terms: Set of blocked terms found.
        score: Keyword overlap score.

    Returns:
        Thesis signal string: "strengthening", "weakening", or "neutral".
    """
    if blocked_terms:
        return "weakening"

    if score > 0.5 and matched_terms:
        return "strengthening"

    return "neutral"


def compute_match_score(
    keyword_score: float,
    required_pass: bool,
    blocked_count: int,
    topic_compat: bool,
    event_type_compat: bool,
) -> float:
    """Compute final match score (0-1).
    - If required_pass is False → 0
    - If topic_compat or event_type_compat is False → 0
    - Otherwise: keyword_score * (1 - 0.3 * min(blocked_count, 3))
    - Clamp to 0-1

    Args:
        keyword_score: Base keyword overlap score (0-1).
        required_pass: Whether required terms check passed.
        blocked_count: Number of blocked terms found.
        topic_compat: Whether topic is compatible.
        event_type_compat: Whether event type is compatible.

    Returns:
        Final match score clamped to 0-1.
    """
    # Fail conditions
    if not required_pass:
        return 0.0
    if not topic_compat:
        return 0.0
    if not event_type_compat:
        return 0.0

    # Apply blocked penalty: 0.3 per blocked term, capped at 3
    penalty = 0.3 * min(blocked_count, 3)
    score = keyword_score * (1 - penalty)

    # Clamp to 0-1
    return max(0.0, min(1.0, score))


def match_record_against_watchlists(record: dict, watchlists: list[dict]) -> list[dict]:
    """Match a single record against all enabled watchlists.
    Returns list of hit dicts with: watchlist_id, record_id, event_id, match_score, matched_terms, thesis_signal, created_at
    Only returns hits with match_score > 0.

    Args:
        record: Research record dictionary to match.
        watchlists: List of enabled watchlist dictionaries.

    Returns:
        List of hit dictionaries for matching watchlists with score > 0.
    """
    hits = []

    # Extract text features
    features = extract_text_features(record)
    record_id = record.get("id", "")

    # Build combined text for tokenization
    text_parts = [
        features["title"],
        features["summary"],
        features["why_it_matters"],
    ]
    # Add tags and important_numbers as strings
    if features["tags"]:
        text_parts.extend(str(t) for t in features["tags"])
    if features["important_numbers"]:
        text_parts.extend(str(n) for n in features["important_numbers"])

    text = " ".join(text_parts)
    text_terms = tokenize(text)

    for watchlist in watchlists:
        wl_id = watchlist.get("watchlist_id", "")

        # Check topic compatibility
        topic_compat = check_topic_compatibility(
            features["topic"], watchlist.get("topic", "")
        )

        # Check event type compatibility
        event_type_compat = check_event_type_compatibility(
            features["event_type"], watchlist
        )

        # Compute keyword overlap
        keyword_score, matched_terms = compute_keyword_overlap(text_terms, watchlist)

        # Check required terms
        required_pass = check_required_terms(text_terms, watchlist)

        # Check blocked terms
        blocked_terms = check_blocked_terms(text_terms, watchlist)

        # Compute final score
        match_score = compute_match_score(
            keyword_score=keyword_score,
            required_pass=required_pass,
            blocked_count=len(blocked_terms),
            topic_compat=topic_compat,
            event_type_compat=event_type_compat,
        )

        # Only include hits with score > 0
        if match_score > 0:
            thesis_signal = determine_thesis_signal(
                matched_terms, blocked_terms, keyword_score
            )
            hit = {
                "watchlist_id": wl_id,
                "record_id": record_id,
                "event_id": None,
                "match_score": round(match_score, 4),
                "matched_terms": matched_terms,
                "thesis_signal": thesis_signal,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            hits.append(hit)

    return hits


def match_cluster_against_watchlists(
    cluster: dict, watchlists: list[dict]
) -> list[dict]:
    """Match an event cluster against all enabled watchlists.
    Same as match_record_against_watchlists but sets event_id in hits.

    Args:
        cluster: Event cluster dictionary to match.
        watchlists: List of enabled watchlist dictionaries.

    Returns:
        List of hit dictionaries for matching watchlists with score > 0.
    """
    hits = []

    # Extract text features
    features = extract_text_features(cluster)
    event_id = cluster.get("event_id", "")

    # Build combined text for tokenization
    text_parts = [
        features["title"],
        features["summary"],
        features["why_it_matters"],
    ]
    # Add tags and important_numbers as strings
    if features["tags"]:
        text_parts.extend(str(t) for t in features["tags"])
    if features["important_numbers"]:
        text_parts.extend(str(n) for n in features["important_numbers"])

    text = " ".join(text_parts)
    text_terms = tokenize(text)

    for watchlist in watchlists:
        wl_id = watchlist.get("watchlist_id", "")

        # Check topic compatibility
        topic_compat = check_topic_compatibility(
            features["topic"], watchlist.get("topic", "")
        )

        # Check event type compatibility
        event_type_compat = check_event_type_compatibility(
            features["event_type"], watchlist
        )

        # Compute keyword overlap
        keyword_score, matched_terms = compute_keyword_overlap(text_terms, watchlist)

        # Check required terms
        required_pass = check_required_terms(text_terms, watchlist)

        # Check blocked terms
        blocked_terms = check_blocked_terms(text_terms, watchlist)

        # Compute final score
        match_score = compute_match_score(
            keyword_score=keyword_score,
            required_pass=required_pass,
            blocked_count=len(blocked_terms),
            topic_compat=topic_compat,
            event_type_compat=event_type_compat,
        )

        # Only include hits with score > 0
        if match_score > 0:
            thesis_signal = determine_thesis_signal(
                matched_terms, blocked_terms, keyword_score
            )
            hit = {
                "watchlist_id": wl_id,
                "record_id": None,  # clusters use event_id, not record_id
                "event_id": event_id,
                "match_score": round(match_score, 4),
                "matched_terms": matched_terms,
                "thesis_signal": thesis_signal,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            hits.append(hit)

    return hits


if __name__ == "__main__":
    # Simple smoke test
    import sys

    config_path = Path(__file__).parent.parent / "config" / "watchlists_v27.json"
    if config_path.exists():
        watchlists = load_watchlists(str(config_path))
        print(f"Loaded {len(watchlists)} enabled watchlists")
    else:
        print(f"Config not found at {config_path}")
        sys.exit(1)
