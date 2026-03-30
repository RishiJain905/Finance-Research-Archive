"""V2.5 Candidate Feature Extraction Module.

Extracts normalized features from raw candidates for scoring.
"""

import json
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent
SCORING_RULES_PATH = BASE_DIR / "config" / "scoring_rules.json"
CANDIDATE_INDEX_PATH = (
    BASE_DIR / "data" / "candidate_manifests" / "candidate_index.json"
)


def load_scoring_rules() -> dict[str, Any]:
    """Load scoring rules from config/scoring_rules.json.

    Returns:
        Parsed scoring rules dictionary
    """
    if not SCORING_RULES_PATH.exists():
        return {}

    with open(SCORING_RULES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_freshness_hours(candidate: dict[str, Any]) -> float:
    """Calculate freshness in hours (not days).

    Uses metadata.published_at or falls back to discovered_at.

    Args:
        candidate: Raw candidate dictionary

    Returns:
        Freshness age in hours (0 if no date available)
    """
    # Try published_at first
    published_at = candidate.get("metadata", {}).get("published_at")

    # Fall back to discovered_at
    if not published_at:
        published_at = candidate.get("discovered_at")

    if not published_at:
        return 0.0

    try:
        pub_date = None

        if isinstance(published_at, str):
            # Handle ISO format - remove Z suffix
            clean_at = published_at.rstrip("Z")

            # Handle timezone offset if present
            tz_offset = None
            if "+" in clean_at or (
                clean_at.endswith("-")
                and len(clean_at) > 10
                and clean_at[-6:-5].isdigit()
            ):
                if clean_at[-6:-5] in ["+", "-"]:
                    tz_offset_str = clean_at[-6:]
                    clean_at = clean_at[:-6]
                    tz_sign = 1 if tz_offset_str[0] == "+" else -1
                    tz_hours = int(tz_offset_str[1:3])
                    tz_mins = int(tz_offset_str[4:6])
                    tz_offset = timezone(
                        timedelta(hours=tz_sign * tz_hours, minutes=tz_sign * tz_mins)
                    )

            # Try parsing with various format strings
            for fmt in ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
                try:
                    pub_date = datetime.strptime(clean_at, fmt)
                    break
                except ValueError:
                    continue

            if pub_date:
                if tz_offset:
                    pub_date = pub_date.replace(tzinfo=tz_offset)
                else:
                    pub_date = pub_date.replace(tzinfo=timezone.utc)
        else:
            pub_date = published_at

        if pub_date is None or (
            hasattr(pub_date, "tzinfo") and pub_date.tzinfo is None
        ):
            return 0.0

        now = datetime.now(timezone.utc)
        age_delta = now - pub_date
        return age_delta.total_seconds() / 3600.0

    except Exception:
        return 0.0


def extract_url_quality_score(
    candidate: dict[str, Any], scoring_rules: dict[str, Any]
) -> float:
    """Score URL structure based on positive and negative hints.

    Positive hints add 10 points each (capped at 100),
    negative hints subtract 10 points each (floor at 0).

    Args:
        candidate: Raw candidate dictionary with url field
        scoring_rules: Scoring rules configuration

    Returns:
        URL quality score (0-100)
    """
    url = candidate.get("url", "").lower()
    if not url:
        return 0.0

    url_hints = scoring_rules.get("url_hints", {})
    positive_hints = url_hints.get("positive", [])
    negative_hints = url_hints.get("negative", [])

    score = 0

    # Positive hints add 10 points each
    for hint in positive_hints:
        if hint.lower() in url:
            score += 10

    # Negative hints subtract 10 points each
    for hint in negative_hints:
        if hint.lower() in url:
            score -= 10

    # Cap at 100, floor at 0
    return max(0.0, min(100.0, float(score)))


def extract_title_quality_score(
    candidate: dict[str, Any], scoring_rules: dict[str, Any]
) -> float:
    """Score title content based on positive and negative hints.

    Positive hints add 10 points each (capped at 100),
    negative hints subtract 10 points each (floor at 0).

    Args:
        candidate: Raw candidate dictionary with title field
        scoring_rules: Scoring rules configuration

    Returns:
        Title quality score (0-100)
    """
    title = candidate.get("title", "").lower()
    if not title:
        return 0.0

    title_hints = scoring_rules.get("title_hints", {})
    positive_hints = title_hints.get("positive", [])
    negative_hints = title_hints.get("negative", [])

    score = 0

    # Positive hints add 10 points each
    for hint in positive_hints:
        if hint.lower() in title:
            score += 10

    # Negative hints subtract 10 points each
    for hint in negative_hints:
        if hint.lower() in title:
            score -= 10

    # Cap at 100, floor at 0
    return max(0.0, min(100.0, float(score)))


def extract_keyword_match_score(
    candidate: dict[str, Any], scoring_rules: dict[str, Any]
) -> float:
    """Match candidate title/anchor against keyword bundles.

    Counts matches across title and anchor_text, normalizes to 0-100.

    Args:
        candidate: Raw candidate dictionary with title and/or anchor_text
        scoring_rules: Scoring rules configuration

    Returns:
        Keyword match score (0-100)
    """
    title = candidate.get("title", "").lower()
    anchor_text = candidate.get("anchor_text", "").lower()
    url = candidate.get("url", "").lower()

    # Combine text sources for matching
    combined_text = f"{title} {anchor_text} {url}"

    title_hints = scoring_rules.get("title_hints", {})
    positive_keywords = title_hints.get("positive", [])

    if not positive_keywords:
        return 0.0

    # Count matches
    match_count = 0
    for keyword in positive_keywords:
        if keyword.lower() in combined_text:
            match_count += 1

    # Normalize to 0-100 (percentage of matched keywords)
    # Assuming max meaningful matches is ~10 keywords
    normalized_score = (match_count / min(len(positive_keywords), 10)) * 100

    return min(100.0, normalized_score)


def extract_domain_trust_score(
    domain: str, scoring_rules: dict[str, Any]
) -> tuple[float, str]:
    """Look up domain in domain_trust_baselines.

    Returns score (high=100, medium=50, low=10) and tier.

    Args:
        domain: Source domain string
        scoring_rules: Scoring rules configuration

    Returns:
        Tuple of (trust_score, trust_tier)
    """
    if not domain:
        return 0.0, "low"

    domain_lower = domain.lower()
    baselines = scoring_rules.get("domain_trust_baselines", {})

    # Check high trust
    if domain_lower in baselines.get("high", []):
        return 100.0, "high"

    # Check medium trust
    if domain_lower in baselines.get("medium", []):
        return 50.0, "medium"

    # Low trust (default for unknown domains)
    return 10.0, "low"


def extract_lane_reliability_score(lane: str, scoring_rules: dict[str, Any]) -> float:
    """Look up lane in lane_reliability config.

    Args:
        lane: Lane name (trusted_sources, keyword_discovery, seed_crawl)
        scoring_rules: Scoring rules configuration

    Returns:
        Lane reliability score (0-100)
    """
    lane_reliability = scoring_rules.get("lane_reliability", {})

    # Default reliability scores if lane not found
    default_scores = {
        "trusted_sources": 100,
        "keyword_discovery": 50,
        "seed_crawl": 30,
    }

    return float(lane_reliability.get(lane, default_scores.get(lane, 0)))


def extract_domain_trust_score_with_memory(
    domain: str, scoring_rules: dict[str, Any]
) -> tuple[float, str]:
    """Look up domain trust, blending baseline with memory-based trust.

    1. Get baseline trust from scoring_rules.json (high=100, medium=50, low=10)
    2. Get memory-based trust from memory_manager.get_domain_trust(domain)
    3. Blend based on sample size in memory

    Returns (blended_trust_score, trust_tier)
    """
    # First get baseline trust and tier from existing function
    baseline_score, tier = extract_domain_trust_score(domain, scoring_rules)

    # Get memory-based trust
    from scripts.memory_manager import get_domain_trust

    memory_trust = get_domain_trust(domain)

    # If no memory exists, just use baseline
    if memory_trust == baseline_score:
        return baseline_score, tier

    # Get domain memory to check sample size
    from scripts.memory_persistence import get_domain_memory

    memory = get_domain_memory(domain)

    if memory is None:
        return baseline_score, tier

    total = memory.get("total_candidates", 0)

    # Load memory config for blending thresholds
    from scripts.memory_persistence import load_memory_config

    config = load_memory_config()
    cold_start = config.get("cold_start", {})
    min_samples = cold_start.get("min_samples_for_learning", 10)
    full_threshold = cold_start.get("full_learning_threshold", 25)

    if total < min_samples:
        # Not enough data, mostly use baseline
        return baseline_score, tier
    elif total >= full_threshold:
        # Enough data, mostly use memory
        # Blend: 10% baseline, 90% memory
        blended = baseline_score * 0.1 + memory_trust * 0.9
        return max(0, min(100, blended)), tier
    else:
        # Linear blend between min and full
        blend_ratio = (total - min_samples) / (full_threshold - min_samples)
        baseline_weight = 0.9 - (blend_ratio * 0.8)  # 0.9 → 0.1
        memory_weight = 0.1 + (blend_ratio * 0.8)  # 0.1 → 0.9
        blended = baseline_score * baseline_weight + memory_trust * memory_weight
        return max(0, min(100, blended)), tier


def extract_path_trust_score(
    domain: str, url: str, scoring_rules: dict[str, Any]
) -> float:
    """Extract path trust score from memory.

    Args:
        domain: Source domain
        url: Full URL
        scoring_rules: Scoring rules config

    Returns:
        Path trust score (0-100), defaults to 50 if no memory
    """
    from scripts.memory_manager import extract_path_pattern, get_path_trust

    path_pattern = extract_path_pattern(url)
    if not path_pattern:
        return 50.0  # Default for root path

    path_trust = get_path_trust(domain, path_pattern)
    return path_trust


def extract_source_quality_scores(source_id: str) -> tuple[float, float]:
    """Extract yield and noise scores from source memory.

    Args:
        source_id: Source identifier (often same as lane)

    Returns:
        Tuple of (yield_score, noise_score), defaults to (0.5, 0.5) if no memory
    """
    from scripts.memory_persistence import get_source_memory

    memory = get_source_memory(source_id)
    if memory is None:
        return (0.5, 0.5)  # Neutral default

    yield_score = memory.get("yield_score", 0.5)
    noise_score = memory.get("noise_score", 0.5)
    return (yield_score, noise_score)


def derive_source_type(candidate: dict[str, Any], scoring_rules: dict[str, Any]) -> str:
    """Derive source type for candidate.

    First checks source_type_map in scoring_rules by domain.
    If not found, auto-detects from URL patterns:
    - contains "press" or "release" → press_release
    - contains "speech" or "testimony" → speech
    - contains "research" or "analysis" → research
    - default → unknown

    Args:
        candidate: Raw candidate dictionary with url and source_domain
        scoring_rules: Scoring rules configuration

    Returns:
        Source type string
    """
    # First check source_type_map
    source_type_map = scoring_rules.get("source_type_map", {})
    domain = candidate.get("source_domain", "") or candidate.get("source", {}).get(
        "domain", ""
    )

    if domain:
        domain_lower = domain.lower()
        if domain_lower in source_type_map:
            return source_type_map[domain_lower]

    # Auto-detect from URL patterns
    url = candidate.get("url", "").lower()

    if "press" in url or "release" in url:
        return "press_release"
    if "speech" in url or "testimony" in url:
        return "speech"
    if "research" in url or "analysis" in url:
        return "research"

    return "unknown"


def extract_duplication_risk_score(
    candidate: dict[str, Any], candidate_index: dict[str, Any]
) -> float:
    """Check candidate_index for url_hash/title_hash matches.

    Higher risk (higher score) if duplicates exist. Return 0-100.

    Args:
        candidate: Raw candidate dictionary
        candidate_index: Candidate index with seen_url_hashes, seen_title_hashes

    Returns:
        Duplication risk score (0-100)
    """
    from scripts.candidate_utils import hash_url, hash_title

    risk_score = 0.0

    # Check URL hash
    url = candidate.get("url", "")
    if url:
        url_hash = hash_url(url)
        if url_hash in candidate_index.get("seen_url_hashes", {}):
            risk_score += 50.0

    # Check title hash
    title = candidate.get("title", "")
    if title:
        title_hash = hash_title(title)
        if title_hash in candidate_index.get("seen_title_hashes", {}):
            risk_score += 30.0

    # Check domain family (same domain multiple times suggests redundancy)
    domain = candidate.get("source_domain", "") or candidate.get("source", {}).get(
        "domain", ""
    )
    if domain:
        domain_hashes = candidate_index.get("seen_url_hashes", {})
        # Count URLs from same domain (rough heuristic)
        domain_count = sum(
            1 for h in domain_hashes.keys() if domain.lower() in str(domain_hashes[h])
        )
        if domain_count > 3:
            risk_score += 20.0

    return min(100.0, risk_score)


def extract_topic_hints(
    candidate: dict[str, Any], scoring_rules: dict[str, Any]
) -> list[str]:
    """Extract matched topic keywords from candidate.

    Args:
        candidate: Raw candidate dictionary
        scoring_rules: Scoring rules configuration

    Returns:
        List of matched topic keywords
    """
    title = candidate.get("title", "").lower()
    anchor_text = candidate.get("anchor_text", "").lower()
    url = candidate.get("url", "").lower()

    combined_text = f"{title} {anchor_text} {url}"

    title_hints = scoring_rules.get("title_hints", {})
    positive_keywords = title_hints.get("positive", [])

    matched = []
    for keyword in positive_keywords:
        if keyword.lower() in combined_text:
            matched.append(keyword)

    return matched


def calculate_bundle_match_score(
    candidate_text: str, keyword_bundles: dict[str, Any]
) -> float:
    """Calculate how well candidate matches keyword bundles.

    For each bundle, checks if all required_terms are present and
    scores based on optional_terms matched. Returns normalized score 0-100.

    Args:
        candidate_text: Combined text from candidate (title + anchor + url)
        keyword_bundles: Dictionary of bundle definitions

    Returns:
        Normalized bundle match score (0-100)
    """
    if not candidate_text or not keyword_bundles:
        return 0.0

    candidate_lower = candidate_text.lower()
    total_score = 0.0
    bundle_count = 0

    for bundle_id, bundle in keyword_bundles.items():
        if bundle.get("is_negative", False):
            continue  # Skip negative bundles in positive matching

        required_terms = bundle.get("required_terms", [])
        optional_terms = bundle.get("optional_terms", [])
        bundle_weight = bundle.get("weight", 1.0)

        # Check required terms
        required_matches = sum(
            1 for term in required_terms if term.lower() in candidate_lower
        )
        required_score = 0.0
        if required_terms:
            if len(required_terms) == required_matches:
                # All required matched - full points
                required_score = 20.0 * len(required_terms)
            else:
                # Partial match - no points
                required_score = 0.0

        # Check optional terms
        optional_matches = sum(
            1 for term in optional_terms if term.lower() in candidate_lower
        )
        optional_score = 10.0 * optional_matches  # 10 points per optional match

        bundle_score = (required_score + optional_score) * bundle_weight
        total_score += bundle_score
        bundle_count += 1

    if bundle_count == 0:
        return 0.0

    # Normalize to 0-100
    # Assuming max possible is roughly 100 per bundle (20 * 5 required + 10 * 5 optional)
    max_expected = bundle_count * 100.0
    normalized = (total_score / max_expected) * 100.0

    return min(100.0, max(0.0, normalized))


def extract_theme_match_features(
    candidate: dict[str, Any], themes: dict[str, Any], keyword_bundles: dict[str, Any]
) -> dict[str, Any]:
    """Extract theme-related features from candidate.

    Computes:
    - theme_match_count: Number of learned themes matched (0-10 scaled)
    - theme_match_score: Weighted score based on theme priority (0-100)
    - negative_bundle_match: Boolean if matches negative bundle
    - negative_bundle_penalty: Penalty score based on negative match strength (0-50)

    Args:
        candidate: Candidate dictionary with title, anchor_text, url
        themes: Dictionary of learned themes
        keyword_bundles: Dictionary of keyword bundle definitions

    Returns:
        Dictionary with theme-related features
    """
    # Combine text sources for matching
    title = candidate.get("title", "").lower()
    anchor_text = candidate.get("anchor_text", "").lower()
    url = candidate.get("url", "").lower()
    combined_text = f"{title} {anchor_text} {url}"

    theme_match_count = 0
    theme_match_score = 0.0
    negative_bundle_match = False
    negative_bundle_penalty = 0.0

    # Check each learned theme
    for theme_id, theme in themes.items():
        bundle_id = theme.get("bundle_id", "")
        keywords = theme.get("keywords", [])
        priority = theme.get("priority", 50.0)

        # Check if keywords match
        matched_keywords = [kw for kw in keywords if kw.lower() in combined_text]

        if matched_keywords:
            theme_match_count += 1
            # Weight by priority (0-100) scaled to contribution
            priority_contribution = (priority / 100.0) * 15.0  # Max 15 points per theme
            theme_match_score += priority_contribution

    # Check negative bundles
    negative_bundles = keyword_bundles.get("negative_bundles", {})
    if not negative_bundles:
        # Fall back to finding negative bundle in main bundles
        for bundle_id, bundle in keyword_bundles.items():
            if bundle.get("is_negative", False):
                negative_bundles[bundle_id] = bundle

    for neg_bundle_id, neg_bundle in negative_bundles.items():
        terms = neg_bundle.get("optional_terms", []) + neg_bundle.get(
            "required_terms", []
        )
        penalty_strength = neg_bundle.get("penalty_strength", 30.0)

        # Check if any negative terms match
        matched_negative_terms = [
            term for term in terms if term.lower() in combined_text
        ]

        if matched_negative_terms:
            negative_bundle_match = True
            # Scale penalty by how many negative terms matched
            match_ratio = len(matched_negative_terms) / max(len(terms), 1)
            negative_bundle_penalty = min(50.0, penalty_strength * match_ratio)
            break  # Only apply strongest negative bundle

    # Scale theme_match_count to 0-10 range
    theme_match_count_scaled = min(10.0, float(theme_match_count))

    # Scale theme_match_score to 0-100 range (cap at ~3 themes contributing max)
    theme_match_score = min(100.0, theme_match_score)

    return {
        "theme_match_count": theme_match_count_scaled,
        "theme_match_score": theme_match_score,
        "negative_bundle_match": negative_bundle_match,
        "negative_bundle_penalty": negative_bundle_penalty,
    }


def extract_candidate_features(candidate: dict[str, Any]) -> dict[str, Any]:
    """Main function to extract all features from a raw candidate.

    Takes raw candidate, extracts all features, returns candidate with new fields filled:
    - source_domain
    - freshness_hours
    - url_quality_score
    - title_quality_score
    - keyword_match_score
    - domain_trust_score
    - lane_reliability_score
    - duplication_risk_score
    - source_type
    - topic_hints (list of matched topic keywords)

    Args:
        candidate: Raw candidate dictionary

    Returns:
        Candidate with extracted features added
    """
    from scripts.candidate_utils import get_candidate_index

    # Load scoring rules and candidate index
    scoring_rules = load_scoring_rules()
    candidate_index = get_candidate_index()

    # Extract source domain
    source_domain = candidate.get("source_domain", "")
    if not source_domain:
        source_domain = candidate.get("source", {}).get("domain", "")

    # Extract freshness in hours
    freshness_hours = extract_freshness_hours(candidate)

    # URL quality score
    url_quality_score = extract_url_quality_score(candidate, scoring_rules)

    # Title quality score
    title_quality_score = extract_title_quality_score(candidate, scoring_rules)

    # Keyword match score
    keyword_match_score = extract_keyword_match_score(candidate, scoring_rules)

    # Domain trust score (with memory-based blending)
    domain_trust_score, trust_tier = extract_domain_trust_score_with_memory(
        source_domain, scoring_rules
    )

    # Lane reliability score
    lane = candidate.get("lane", "seed_crawl")
    lane_reliability_score = extract_lane_reliability_score(lane, scoring_rules)

    # Path trust score (from memory)
    url = candidate.get("url", "")
    path_trust_score = extract_path_trust_score(source_domain, url, scoring_rules)

    # Source quality scores (yield and noise from memory)
    source_id = candidate.get("source_id", lane)  # Default to lane as source_id
    source_yield_score, source_noise_score = extract_source_quality_scores(source_id)

    # Duplication risk score
    duplication_risk_score = extract_duplication_risk_score(candidate, candidate_index)

    # Source type
    source_type = derive_source_type(candidate, scoring_rules)

    # Topic hints
    topic_hints = extract_topic_hints(candidate, scoring_rules)

    # Add all extracted features to candidate
    candidate["source_domain"] = source_domain
    candidate["freshness_hours"] = freshness_hours
    candidate["url_quality_score"] = url_quality_score
    candidate["title_quality_score"] = title_quality_score
    candidate["keyword_match_score"] = keyword_match_score
    candidate["domain_trust_score"] = domain_trust_score
    candidate["domain_trust_tier"] = trust_tier
    candidate["lane_reliability_score"] = lane_reliability_score
    candidate["path_trust_score"] = path_trust_score
    candidate["source_yield_score"] = source_yield_score
    candidate["source_noise_score"] = source_noise_score
    candidate["duplication_risk_score"] = duplication_risk_score
    candidate["source_type"] = source_type
    candidate["topic_hints"] = topic_hints

    # Extract theme matching features
    themes = {}
    keyword_bundles = {}
    try:
        from scripts.theme_memory_persistence import get_themes, load_keyword_bundles

        themes = get_themes()
        keyword_bundles = load_keyword_bundles().get("bundles", {})
    except Exception:
        pass  # Theme memory not available

    if themes and keyword_bundles:
        theme_features = extract_theme_match_features(
            candidate, themes, keyword_bundles
        )
        candidate.update(theme_features)

    return candidate
