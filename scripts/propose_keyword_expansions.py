"""Propose Keyword Expansions Module.

Analyzes accepted candidates to propose new keywords for keyword bundles.
Part of Stream B - Adaptive Keyword Expansion.
"""

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from scripts.extract_candidate_features import extract_candidate_features

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent


def analyze_candidate_keywords(
    candidate: dict[str, Any],
    existing_positive_terms: list[str],
    existing_negative_terms: list[str],
) -> dict[str, list[str]]:
    """Analyze a candidate to extract new keyword candidates.

    Args:
        candidate: Accepted candidate dictionary with features
        existing_positive_terms: List of terms already in positive bundles
        existing_negative_terms: List of terms already in negative bundles

    Returns:
        Dictionary with 'new_positive_terms' and 'new_negative_terms' lists
    """
    # Combine title, anchor_text, and url for analysis
    title = candidate.get("title", "").lower()
    anchor_text = candidate.get("anchor_text", "").lower()
    url = candidate.get("url", "").lower()

    combined_text = f"{title} {anchor_text} {url}"

    # Extract potential keywords (simple tokenization)
    words = _tokenize_text(combined_text)

    # Filter out common stop words and short terms
    stop_words = {
        "the",
        "a",
        "an",
        "and",
        "or",
        "but",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "with",
        "by",
        "from",
        "as",
        "is",
        "was",
        "are",
        "were",
        "been",
        "be",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "will",
        "would",
        "could",
        "should",
        "may",
        "might",
        "must",
        "shall",
        "can",
        "need",
        "this",
        "that",
        "these",
        "those",
        "it",
        "its",
        "they",
        "them",
        "their",
        "what",
        "which",
        "who",
        "whom",
        "when",
        "where",
        "why",
        "how",
        "all",
        "each",
        "every",
        "both",
        "few",
        "more",
        "most",
        "other",
        "some",
        "such",
        "no",
        "nor",
        "not",
        "only",
        "own",
        "same",
        "so",
        "than",
        "too",
        "very",
        "just",
        "also",
        "now",
        "here",
        "there",
        "https",
        "http",
        "www",
        "com",
        "org",
        "edu",
        "gov",
        "net",
        "html",
    }

    filtered_words = [w for w in words if len(w) >= 4 and w not in stop_words]

    # Separate into potential positive and negative terms
    new_positive_terms = []
    new_negative_terms = []

    # High-value finance terms that should be positive
    positive_indicators = {
        "inflation",
        "monetary",
        "policy",
        "interest",
        "rate",
        "rates",
        "federal",
        "reserve",
        "fomc",
        "treasury",
        "yield",
        "market",
        "economic",
        "data",
        "gdp",
        "employment",
        "consumer",
        "price",
        "central",
        "bank",
        "ecb",
        "boe",
        "bankofengland",
        "imf",
        "liquidity",
        "volatility",
        "repo",
        "securities",
        "debt",
        "balance",
        "sheet",
        "quantitative",
        "easing",
        "tightening",
        "pce",
        "cpi",
        "ppi",
        "retail",
        "sales",
        "manufacturing",
        "pmi",
    }

    # Terms that typically indicate low-quality content
    negative_indicators = {
        "subscribe",
        "careers",
        "jobs",
        "advertisement",
        "sponsored",
        "newsletter",
        "donate",
        "about",
        "team",
        "event",
        "registration",
        "webinar",
        "podcast",
        "career",
        "people",
        "experts",
        "education",
        "programs",
        "museum",
        "archive",
        "category",
        "tag",
        "login",
        "signin",
        "register",
        "search",
        "faq",
        "help",
        "contact",
    }

    existing_positive_set = set(t.lower() for t in existing_positive_terms)
    existing_negative_set = set(t.lower() for t in existing_negative_terms)

    for word in filtered_words:
        word_lower = word.lower()

        # Skip if already in existing terms
        if word_lower in existing_positive_set or word_lower in existing_negative_set:
            continue

        # Check positive indicators
        if any(indicator in word_lower for indicator in positive_indicators):
            # Double check it's actually related to finance
            if any(indicator in combined_text for indicator in positive_indicators):
                new_positive_terms.append(word)

        # Check negative indicators
        if any(indicator in word_lower for indicator in negative_indicators):
            new_negative_terms.append(word)

    # Deduplicate and return
    return {
        "new_positive_terms": list(set(new_positive_terms)),
        "new_negative_terms": list(set(new_negative_terms)),
    }


def _tokenize_text(text: str) -> list[str]:
    """Tokenize text into words.

    Args:
        text: Input text

    Returns:
        List of word tokens
    """
    import re

    # Split on non-alphanumeric characters
    tokens = re.findall(r"\b[a-zA-Z0-9]+\b", text)
    return tokens


def calculate_term_significance(
    term: str,
    frequency: int,
    total_candidates: int,
    bundle_relevance: float = 1.0,
) -> float:
    """Calculate significance score for a proposed term.

    Args:
        term: The keyword term
        frequency: Number of times term appeared in accepted candidates
        total_candidates: Total number of accepted candidates analyzed
        bundle_relevance: Relevance multiplier for the target bundle (0-1)

    Returns:
        Significance score (0-100)
    """
    if total_candidates == 0:
        return 0.0

    # Base frequency score
    frequency_ratio = frequency / total_candidates
    frequency_score = min(100.0, frequency_ratio * 100 * 10)  # Scale up

    # Bundle relevance adjustment
    relevance_score = frequency_score * bundle_relevance

    return min(100.0, relevance_score)


def propose_bundle_expansions(
    accepted_candidates: list[dict[str, Any]],
    min_significance: float = 30.0,
) -> dict[str, Any]:
    """Propose keyword expansions for bundles based on accepted candidates.

    Args:
        accepted_candidates: List of accepted candidate dictionaries
        min_significance: Minimum significance score to propose a term

    Returns:
        Dictionary with proposed_expansions for each bundle_id
    """
    # Load existing keyword bundles
    from scripts.theme_memory_persistence import load_keyword_bundles

    bundles_config = load_keyword_bundles()
    bundles = bundles_config.get("bundles", {})

    # Collect existing terms
    existing_positive_terms = []
    existing_negative_terms = []

    for bundle in bundles.values():
        if bundle.get("is_negative", False):
            existing_negative_terms.extend(bundle.get("optional_terms", []))
            existing_negative_terms.extend(bundle.get("required_terms", []))
        else:
            existing_positive_terms.extend(bundle.get("optional_terms", []))
            existing_positive_terms.extend(bundle.get("required_terms", []))

    # Analyze all candidates
    all_new_positive = Counter()
    all_new_negative = Counter()

    for candidate in accepted_candidates:
        # Extract features if not already done
        if "source_domain" not in candidate:
            candidate = extract_candidate_features(candidate)

        result = analyze_candidate_keywords(
            candidate,
            existing_positive_terms,
            existing_negative_terms,
        )

        all_new_positive.update(result["new_positive_terms"])
        all_new_negative.update(result["new_negative_terms"])

    total = len(accepted_candidates)

    # Build proposals
    proposals = {}

    for term, count in all_new_positive.most_common(20):
        significance = calculate_term_significance(term, count, total)
        if significance >= min_significance:
            proposals[f"positive_{term}"] = {
                "term": term,
                "bundle": "positive",
                "frequency": count,
                "significance": significance,
                "proposed_action": "add_to_bundle",
            }

    for term, count in all_new_negative.most_common(10):
        significance = calculate_term_significance(term, count, total)
        if significance >= min_significance:
            proposals[f"negative_{term}"] = {
                "term": term,
                "bundle": "negative",
                "frequency": count,
                "significance": significance,
                "proposed_action": "add_to_negative_bundle",
            }

    return {
        "proposals": proposals,
        "analyzed_candidates": total,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_proposal_summary(proposals_result: dict[str, Any]) -> str:
    """Generate a human-readable summary of proposals.

    Args:
        proposals_result: Result from propose_bundle_expansions

    Returns:
        Summary string
    """
    proposals = proposals_result.get("proposals", {})
    analyzed = proposals_result.get("analyzed_candidates", 0)

    positive = [p for p in proposals.values() if p["bundle"] == "positive"]
    negative = [p for p in proposals.values() if p["bundle"] == "negative"]

    lines = [
        f"Keyword Expansion Proposals (analyzed {analyzed} candidates):",
        f"",
        f"Positive bundle proposals ({len(positive)}):",
    ]

    for p in sorted(positive, key=lambda x: x["significance"], reverse=True)[:10]:
        lines.append(
            f"  - {p['term']}: significance={p['significance']:.1f}, freq={p['frequency']}"
        )

    lines.append(f"")
    lines.append(f"Negative bundle proposals ({len(negative)}):")

    for p in sorted(negative, key=lambda x: x["significance"], reverse=True)[:5]:
        lines.append(
            f"  - {p['term']}: significance={p['significance']:.1f}, freq={p['frequency']}"
        )

    return "\n".join(lines)


# ============================================================================
# Theme Learning Functions
# ============================================================================


def analyze_candidate_for_themes(
    candidate: dict[str, Any],
    bundles: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Analyze a candidate to identify matched themes.

    Args:
        candidate: Candidate dictionary with features
        bundles: Keyword bundles configuration

    Returns:
        List of matched theme dictionaries
    """
    title = candidate.get("title", "").lower()
    anchor_text = candidate.get("anchor_text", "").lower()
    url = candidate.get("url", "").lower()

    combined_text = f"{title} {anchor_text} {url}"

    matched_themes = []

    for bundle_id, bundle in bundles.items():
        if bundle.get("is_negative", False):
            continue  # Skip negative bundles for theme matching

        required_terms = bundle.get("required_terms", [])
        optional_terms = bundle.get("optional_terms", [])

        # Check required terms
        required_matches = [
            term for term in required_terms if term.lower() in combined_text
        ]

        # Check optional terms
        optional_matches = [
            term for term in optional_terms if term.lower() in combined_text
        ]

        # Calculate match score
        required_score = (
            len(required_matches) / len(required_terms) if required_terms else 1.0
        )
        optional_score = (
            len(optional_matches) / len(optional_terms) if optional_terms else 0.0
        )

        # Bundle must have all required terms to be considered a match
        if required_terms and len(required_matches) < len(required_terms):
            continue

        match_strength = (required_score * 0.6) + (optional_score * 0.4)

        if match_strength > 0 or (not required_terms and not optional_terms):
            matched_themes.append(
                {
                    "bundle_id": bundle_id,
                    "bundle_name": bundle.get("name", bundle_id),
                    "required_matches": required_matches,
                    "optional_matches": optional_matches,
                    "match_strength": match_strength * 100,
                    "keywords": required_matches + optional_matches,
                }
            )

    return matched_themes


def propose_themes_from_candidates(
    accepted_candidates: list[dict[str, Any]],
    min_match_strength: float = 40.0,
) -> list[dict[str, Any]]:
    """Propose new themes from accepted candidates.

    Args:
        accepted_candidates: List of accepted candidate dictionaries
        min_match_strength: Minimum match strength to propose a theme

    Returns:
        List of proposed theme dictionaries
    """
    from scripts.theme_memory_persistence import (
        load_keyword_bundles,
        get_themes,
    )

    bundles_config = load_keyword_bundles()
    bundles = bundles_config.get("bundles", {})

    # Get existing themes to avoid duplicates
    existing_themes = get_themes()
    existing_bundle_ids = set(
        existing_themes.get(bid, {}).get("bundle_id") for bid in existing_themes
    )

    # Analyze candidates
    theme_matches = {}

    for candidate in accepted_candidates:
        matched_themes = analyze_candidate_for_themes(candidate, bundles)

        for match in matched_themes:
            bundle_id = match["bundle_id"]

            if bundle_id in existing_bundle_ids:
                continue  # Already have this theme

            if bundle_id not in theme_matches:
                theme_matches[bundle_id] = {
                    "bundle_id": bundle_id,
                    "bundle_name": match["bundle_name"],
                    "keywords": set(),
                    "total_match_strength": 0,
                    "match_count": 0,
                }

            theme_matches[bundle_id]["keywords"].update(match["keywords"])
            theme_matches[bundle_id]["total_match_strength"] += match["match_strength"]
            theme_matches[bundle_id]["match_count"] += 1

    # Build proposals
    proposals = []

    for bundle_id, data in theme_matches.items():
        avg_strength = (
            data["total_match_strength"] / data["match_count"]
            if data["match_count"] > 0
            else 0
        )

        if avg_strength >= min_match_strength:
            proposals.append(
                {
                    "bundle_id": data["bundle_id"],
                    "bundle_name": data["bundle_name"],
                    "keywords": list(data["keywords"]),
                    "priority": avg_strength,
                    "match_count": data["match_count"],
                    "avg_match_strength": avg_strength,
                }
            )

    # Sort by priority
    proposals.sort(key=lambda x: x["priority"], reverse=True)

    return proposals
