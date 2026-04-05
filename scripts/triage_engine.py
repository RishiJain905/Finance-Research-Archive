"""
V2.7 Triage and Prioritization Engine.

Scores and prioritizes candidates before expensive AI processing.
Sits between all ingestion lanes and the existing summarize/verify/archive backend.
"""

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

# Base directory is the project root
BASE_DIR = Path(__file__).resolve().parent.parent
CANDIDATES_DIR = BASE_DIR / "data" / "candidates"
TRIAGE_DIR = BASE_DIR / "data" / "triage"
DEFERRED_DIR = BASE_DIR / "data" / "deferred"

# Configuration paths
WEIGHTS_CONFIG_PATH = BASE_DIR / "config" / "triage_weights.json"
BUDGET_CONFIG_PATH = BASE_DIR / "config" / "triage_budget.json"


def load_json(path: Path) -> dict:
    """Load JSON file."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_candidates(candidates_dir: Path) -> list[dict]:
    """Load candidates from a directory of JSON files.

    Args:
        candidates_dir: Directory containing candidate JSON files

    Returns:
        List of candidate dictionaries
    """
    candidates = []
    if not candidates_dir.exists():
        return candidates

    for file_path in candidates_dir.glob("*.json"):
        if file_path.name.endswith(".json"):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    candidate = json.load(f)
                    if isinstance(candidate, dict):
                        candidates.append(candidate)
            except (json.JSONDecodeError, IOError):
                continue

    return candidates


def compute_source_trust(candidate: dict) -> float:
    """Compute source trust score component (0-100).

    Maps directly from domain_trust_score field.

    Args:
        candidate: Candidate dictionary

    Returns:
        Source trust score (0-100)
    """
    domain_trust = candidate.get("domain_trust_score", 0)
    return max(0.0, min(100.0, float(domain_trust)))


def compute_freshness(candidate: dict) -> float:
    """Compute freshness score component (0-100).

    Newer candidates score higher. Uses freshness_hours field.
    Full score for < 24 hours, decreases linearly to 0 at 168 hours (7 days).

    Args:
        candidate: Candidate dictionary

    Returns:
        Freshness score (0-100)
    """
    freshness_hours = candidate.get("freshness_hours", float("inf"))

    if freshness_hours <= 0:
        return 100.0  # Just discovered

    max_freshness_hours = 168.0  # 7 days

    if freshness_hours >= max_freshness_hours:
        return 0.0

    # Linear decay from 100 to 0
    freshness_score = 100.0 * (1.0 - (freshness_hours / max_freshness_hours))
    return max(0.0, min(100.0, freshness_score))


def compute_topic_relevance(candidate: dict) -> float:
    """Compute topic relevance score component (0-100).

    Combines keyword_match_score and topic_hints count.

    Args:
        candidate: Candidate dictionary

    Returns:
        Topic relevance score (0-100)
    """
    keyword_match = candidate.get("keyword_match_score", 0)
    topic_hints = candidate.get("topic_hints", [])

    # Base score from keyword match (0-80)
    keyword_score = max(0.0, min(80.0, float(keyword_match)))

    # Bonus from topic hints (0-20, 5 points per hint)
    hints_bonus = min(20.0, len(topic_hints) * 5.0)

    return min(100.0, keyword_score + hints_bonus)


def compute_title_quality(candidate: dict) -> float:
    """Compute title quality score component (0-100).

    Maps from title_quality_score field.

    Args:
        candidate: Candidate dictionary

    Returns:
        Title quality score (0-100)
    """
    title_quality = candidate.get("title_quality_score", 0)
    return max(0.0, min(100.0, float(title_quality)))


def compute_url_quality(candidate: dict) -> float:
    """Compute URL quality score component (0-100).

    Maps from url_quality_score field.

    Args:
        candidate: Candidate dictionary

    Returns:
        URL quality score (0-100)
    """
    url_quality = candidate.get("url_quality_score", 0)
    return max(0.0, min(100.0, float(url_quality)))


def compute_novelty(candidate: dict) -> float:
    """Compute novelty score component (0-100).

    Inverse of duplication risk. Novelty = 100 - duplicate_risk.

    Args:
        candidate: Candidate dictionary

    Returns:
        Novelty score (0-100)
    """
    duplicate_risk = candidate.get("duplication_risk_score", 0)
    novelty = 100.0 - float(duplicate_risk)
    return max(0.0, min(100.0, novelty))


def compute_quant_value(candidate: dict) -> float:
    """Compute quant value score component (0-100).

    Only meaningful for "quant" lane candidates.
    Based on data freshness for quant sources.

    Args:
        candidate: Candidate dictionary

    Returns:
        Quant value score (0-100)
    """
    lane = candidate.get("lane", "")

    # Non-quant lanes get 0
    if lane != "quant":
        return 0.0

    # For quant lane, score based on freshness
    freshness_hours = candidate.get("freshness_hours", float("inf"))

    if freshness_hours <= 0:
        return 100.0

    max_freshness_hours = 24.0  # Quant data should be fresh within a day

    if freshness_hours >= max_freshness_hours:
        return 0.0

    # Linear decay from 100 to 0
    quant_score = 100.0 * (1.0 - (freshness_hours / max_freshness_hours))
    return max(0.0, min(100.0, quant_score))


def compute_duplicate_risk(candidate: dict) -> float:
    """Compute duplicate risk score component (0-100).

    Maps from duplication_risk_score field.

    Args:
        candidate: Candidate dictionary

    Returns:
        Duplicate risk score (0-100)
    """
    duplicate_risk = candidate.get("duplication_risk_score", 0)
    return max(0.0, min(100.0, float(duplicate_risk)))


def calculate_weighted_score(scoring: dict, weights: dict) -> float:
    """Calculate weighted score from scoring components and weights.

    Args:
        scoring: Dictionary with 8 score components
        weights: Dictionary with weights for each component

    Returns:
        Weighted score (0-100)
    """
    positive_score = 0.0
    negative_score = 0.0
    positive_weight = 0.0
    negative_weight = 0.0

    for component, score in scoring.items():
        if component not in weights:
            continue

        weight = weights[component]

        if weight >= 0:
            positive_score += score * weight
            positive_weight += weight
        else:
            negative_score += score * abs(weight)
            negative_weight += abs(weight)

    if positive_weight + negative_weight == 0:
        return 0.0

    # Net score accounting for negative weights
    net_score = positive_score - negative_score

    # Normalize to 0-100
    return max(0.0, min(100.0, net_score))


def assign_priority_band(score: float, bands: dict) -> str:
    """Assign priority band based on score and band thresholds.

    Args:
        score: Priority score (0-100)
        bands: Dictionary with 'critical', 'high', 'medium', 'low' thresholds

    Returns:
        Priority band name: 'critical', 'high', 'medium', 'low', or 'discard'
    """
    critical = bands.get("critical", 85)
    high = bands.get("high", 70)
    medium = bands.get("medium", 50)
    low = bands.get("low", 30)

    if score >= critical:
        return "critical"
    elif score >= high:
        return "high"
    elif score >= medium:
        return "medium"
    elif score >= low:
        return "low"
    else:
        return "discard"


def generate_reasons(scoring: dict) -> list[str]:
    """Generate human-readable reasons for the scoring breakdown.

    Args:
        scoring: Dictionary with 8 score components and their values

    Returns:
        List of human-readable reason strings
    """
    reasons = []

    # Source trust
    if scoring.get("source_trust", 0) >= 80:
        reasons.append("high trust domain")
    elif scoring.get("source_trust", 0) < 30:
        reasons.append("low trust domain")

    # Freshness
    if scoring.get("freshness", 0) >= 80:
        reasons.append("very recent publication")
    elif scoring.get("freshness", 0) < 30:
        reasons.append("stale content")

    # Topic relevance
    if scoring.get("topic_relevance", 0) >= 70:
        reasons.append("strong topic match")
    elif scoring.get("topic_relevance", 0) < 30:
        reasons.append("weak topic relevance")

    # Title quality
    if scoring.get("title_quality", 0) >= 70:
        reasons.append("high quality title")
    elif scoring.get("title_quality", 0) < 30:
        reasons.append("poor title quality")

    # URL quality
    if scoring.get("url_quality", 0) >= 70:
        reasons.append("high quality URL structure")
    elif scoring.get("url_quality", 0) < 30:
        reasons.append("poor URL structure")

    # Novelty
    if scoring.get("novelty", 0) >= 80:
        reasons.append("highly novel content")
    elif scoring.get("novelty", 0) < 30:
        reasons.append("likely duplicate content")

    # Quant value
    if scoring.get("quant_value", 0) >= 70:
        reasons.append("fresh quant data")

    # Duplicate risk
    if scoring.get("duplicate_risk", 0) >= 50:
        reasons.append("high duplication risk")
    elif scoring.get("duplicate_risk", 0) < 20:
        reasons.append("low duplication risk")

    return reasons


def determine_action(band: str, budget_config: dict) -> str:
    """Determine action based on priority band and budget config.

    Args:
        band: Priority band name
        budget_config: Budget configuration dictionary

    Returns:
        Action: 'process_now', 'defer', or 'discard'
    """
    defer_medium = budget_config.get("defer_medium", True)

    if band in ("critical", "high"):
        return "process_now"
    elif band == "medium":
        if defer_medium:
            return "defer"
        else:
            return "process_now"
    elif band == "low":
        return "defer"
    else:  # discard
        return "discard"


def compute_triage_score(candidate: dict, weights: dict) -> dict:
    """Compute full triage score for a candidate.

    Args:
        candidate: Candidate dictionary
        weights: Weights configuration

    Returns:
        Triage result dictionary with scoring breakdown and final score
    """
    # Compute 8 component scores
    scoring = {
        "source_trust": compute_source_trust(candidate),
        "freshness": compute_freshness(candidate),
        "topic_relevance": compute_topic_relevance(candidate),
        "title_quality": compute_title_quality(candidate),
        "url_quality": compute_url_quality(candidate),
        "novelty": compute_novelty(candidate),
        "quant_value": compute_quant_value(candidate),
        "duplicate_risk": compute_duplicate_risk(candidate),
    }

    # Calculate weighted total
    priority_score = calculate_weighted_score(scoring, weights)

    return {
        "candidate_id": candidate.get("candidate_id", ""),
        "priority_score": priority_score,
        "scoring": scoring,
    }


def run_triage(
    candidates: list[dict], weights: dict, bands: dict, budget_config: dict
) -> tuple[list[dict], list[dict], list[dict]]:
    """Run the full triage pipeline on candidates.

    Args:
        candidates: List of candidate dictionaries
        weights: Weights configuration
        bands: Band thresholds configuration
        budget_config: Budget gate configuration

    Returns:
        Tuple of (process_now, defer, discard) lists
    """
    process_now = []
    defer = []
    discard = []

    # Score each candidate
    for candidate in candidates:
        triage_result = compute_triage_score(candidate, weights)
        priority_score = triage_result["priority_score"]
        scoring = triage_result["scoring"]

        # Assign band
        priority_band = assign_priority_band(priority_score, bands)

        # Determine action
        action = determine_action(priority_band, budget_config)

        # Generate reasons
        reasons = generate_reasons(scoring)

        # Build complete triage result
        triage = {
            "candidate_id": candidate.get("candidate_id", ""),
            "priority_score": priority_score,
            "priority_band": priority_band,
            "scoring": scoring,
            "reasons": reasons,
            "action": action,
        }

        # Attach triage result to candidate
        candidate["triage_result"] = triage

        # Route to appropriate list
        if action == "process_now":
            process_now.append(candidate)
        elif action == "defer":
            defer.append(candidate)
        else:  # discard
            discard.append(candidate)

    # Sort each list by priority score descending
    process_now.sort(
        key=lambda c: c.get("triage_result", {}).get("priority_score", 0), reverse=True
    )
    defer.sort(
        key=lambda c: c.get("triage_result", {}).get("priority_score", 0), reverse=True
    )
    discard.sort(
        key=lambda c: c.get("triage_result", {}).get("priority_score", 0), reverse=True
    )

    return process_now, defer, discard


def load_weights() -> dict:
    """Load triage weights configuration."""
    if WEIGHTS_CONFIG_PATH.exists():
        return load_json(WEIGHTS_CONFIG_PATH)
    return {
        "weights": {
            "source_trust": 0.20,
            "freshness": 0.15,
            "topic_relevance": 0.20,
            "title_quality": 0.10,
            "url_quality": 0.10,
            "novelty": 0.10,
            "quant_value": 0.10,
            "duplicate_risk": -0.15,
        },
        "bands": {
            "critical": 85,
            "high": 70,
            "medium": 50,
            "low": 30,
        },
    }


def load_budget_config() -> dict:
    """Load triage budget configuration."""
    if BUDGET_CONFIG_PATH.exists():
        return load_json(BUDGET_CONFIG_PATH)
    return {
        "article_process_limit": 25,
        "quant_process_limit": 10,
        "defer_medium": True,
    }
