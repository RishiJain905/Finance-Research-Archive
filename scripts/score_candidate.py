"""V2.5 Single Candidate Scoring Module.

Scores a single candidate using extracted features.
"""

import json
from pathlib import Path
from typing import Any

from scripts.candidate_utils import BASE_DIR

# Configuration paths
SCORING_RULES_PATH = BASE_DIR / "config" / "scoring_rules.json"


def load_scoring_rules() -> dict[str, Any]:
    """Load scoring rules from config/scoring_rules.json.

    Returns:
        Parsed scoring rules dictionary
    """
    if not SCORING_RULES_PATH.exists():
        return {}

    with open(SCORING_RULES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def normalize_score(value: float, min_val: float, max_val: float) -> float:
    """Normalize a score to 0-100 range.

    Args:
        value: The raw score value
        min_val: Minimum expected value (below this becomes 0)
        max_val: Maximum expected value (above this becomes 100)

    Returns:
        Normalized score (0-100)
    """
    if max_val <= min_val:
        return 0.0

    normalized = (value - min_val) / (max_val - min_val)
    return max(0.0, min(100.0, normalized * 100))


def compute_weighted_score(
    breakdown: dict[str, float], weights: dict[str, float]
) -> float:
    """Compute weighted sum from breakdown and weights.

    Args:
        breakdown: Dictionary of component scores
        weights: Dictionary of component weights

    Returns:
        Weighted score (0-100)
    """
    if not breakdown or not weights:
        return 0.0

    total_weight = sum(weights.values())
    if total_weight == 0:
        return 0.0

    weighted_sum = 0.0
    for component, score in breakdown.items():
        weight = weights.get(component, 0)
        weighted_sum += score * weight

    # Normalize by total weight
    return max(0.0, min(100.0, (weighted_sum / total_weight) * 100))


def score_candidate(
    candidate: dict[str, Any], scoring_rules: dict[str, Any] = None
) -> dict[str, Any]:
    """Score a single candidate using extracted features.

    Ensures candidate_score schema fields exist, normalizes all component scores
    to 0-100, applies weights to compute candidate_score, builds score_breakdown
    with normalized values and weights applied.

    Args:
        candidate: Candidate with extracted features
        scoring_rules: Optional pre-loaded scoring rules (loads if not provided)

    Returns:
        Updated candidate with candidate_score and score_breakdown filled
    """
    if scoring_rules is None:
        scoring_rules = load_scoring_rules()

    # Ensure candidate_score schema exists
    if "candidate_score" not in candidate:
        candidate["candidate_score"] = {}

    candidate_score = candidate["candidate_score"]
    weights = scoring_rules.get("weights", {})

    # Get freshness configuration for normalization
    freshness_config = scoring_rules.get("freshness", {})
    max_freshness_hours = freshness_config.get("max_hours_for_full_score", 168)

    # Normalize freshness (higher freshness_hours = lower freshness score)
    freshness_hours = candidate.get("freshness_hours", 0)
    # freshness_score = 100 if < max_freshness_hours, decays linearly to 0
    if freshness_hours < max_freshness_hours:
        freshness_normalized = 100.0 - (freshness_hours / max_freshness_hours * 100)
    else:
        freshness_normalized = 0.0

    # Component scores (already normalized to 0-100 in extraction)
    domain_trust_score = candidate.get("domain_trust_score", 0)
    url_quality_score = candidate.get("url_quality_score", 0)
    title_quality_score = candidate.get("title_quality_score", 0)
    keyword_match_score = candidate.get("keyword_match_score", 0)
    lane_reliability_score = candidate.get("lane_reliability_score", 0)
    duplication_risk_score = candidate.get("duplication_risk_score", 0)

    # Memory-based component scores (may be absent if no memory)
    path_trust_score = candidate.get("path_trust_score")
    source_yield_score = candidate.get("source_yield_score", 0.5)
    source_noise_score = candidate.get("source_noise_score", 0.5)

    # Duplication risk is negative signal (higher risk = lower final score)
    # We'll handle this by negating its weight contribution

    # Build breakdown with normalized values
    breakdown = {
        "domain_trust": domain_trust_score,
        "url_quality": url_quality_score,
        "title_quality": title_quality_score,
        "keyword_match": keyword_match_score,
        "freshness": freshness_normalized,
        "lane_reliability": lane_reliability_score,
        "duplication_risk": duplication_risk_score,
    }

    # Add memory-based components if available
    memory_weights = scoring_rules.get("memory_weights", {})
    if path_trust_score is not None:
        breakdown["path_trust"] = path_trust_score
    if source_yield_score is not None or source_noise_score is not None:
        # Convert yield/noise (0-1) to a quality score (0-100)
        # High yield (+), Low noise (+) = high quality
        # source_quality = (yield_score * 100) - (noise_score * 50)
        source_quality_score = (source_yield_score * 100) - (source_noise_score * 50)
        source_quality_score = max(0, min(100, source_quality_score))
        breakdown["source_quality"] = source_quality_score

    # Compute weighted score
    # Note: duplication_risk has negative weight, so we compute separately
    positive_weight_sum = sum(w for k, w in weights.items() if k != "duplication_risk")
    negative_weight_sum = abs(weights.get("duplication_risk", 0))

    positive_score = sum(
        breakdown[k] * weights.get(k, 0) for k in breakdown if k != "duplication_risk"
    )
    negative_score = breakdown.get("duplication_risk", 0) * abs(
        weights.get("duplication_risk", 0)
    )

    total_weight = positive_weight_sum + negative_weight_sum
    if total_weight > 0:
        final_score = (positive_score - negative_score) / total_weight
    else:
        final_score = 0.0

    final_score = max(0.0, min(100.0, final_score))

    # Build detailed score breakdown
    score_breakdown = {
        "domain_trust": {
            "raw": domain_trust_score,
            "normalized": domain_trust_score,
            "weight": weights.get("domain_trust", 0),
            "contribution": domain_trust_score * weights.get("domain_trust", 0),
        },
        "url_quality": {
            "raw": url_quality_score,
            "normalized": url_quality_score,
            "weight": weights.get("url_quality", 0),
            "contribution": url_quality_score * weights.get("url_quality", 0),
        },
        "title_quality": {
            "raw": title_quality_score,
            "normalized": title_quality_score,
            "weight": weights.get("title_quality", 0),
            "contribution": title_quality_score * weights.get("title_quality", 0),
        },
        "keyword_match": {
            "raw": keyword_match_score,
            "normalized": keyword_match_score,
            "weight": weights.get("keyword_match", 0),
            "contribution": keyword_match_score * weights.get("keyword_match", 0),
        },
        "freshness": {
            "raw": freshness_hours,
            "normalized": freshness_normalized,
            "max_hours": max_freshness_hours,
            "weight": weights.get("freshness", 0),
            "contribution": freshness_normalized * weights.get("freshness", 0),
        },
        "lane_reliability": {
            "raw": lane_reliability_score,
            "normalized": lane_reliability_score,
            "weight": weights.get("lane_reliability", 0),
            "contribution": lane_reliability_score * weights.get("lane_reliability", 0),
        },
        "duplication_risk": {
            "raw": duplication_risk_score,
            "normalized": duplication_risk_score,
            "weight": weights.get("duplication_risk", 0),
            "contribution": duplication_risk_score
            * abs(weights.get("duplication_risk", 0)),
        },
    }

    # Add memory-based breakdown components if available
    if path_trust_score is not None:
        path_trust_weight = memory_weights.get("path_trust_weight", 0.05)
        score_breakdown["path_trust"] = {
            "raw": path_trust_score,
            "normalized": path_trust_score,
            "weight": path_trust_weight,
            "contribution": path_trust_score * path_trust_weight,
        }

    if source_yield_score is not None or source_noise_score is not None:
        source_quality_weight = memory_weights.get("source_quality_weight", 0.05)
        score_breakdown["source_quality"] = {
            "raw": source_quality_score,
            "normalized": source_quality_score,
            "weight": source_quality_weight,
            "contribution": source_quality_score * source_quality_weight,
        }

    # Apply memory-based scoring adjustments
    # Get thresholds from memory_weights (with defaults)
    path_trust_penalty_threshold = memory_weights.get(
        "path_trust_penalty_threshold", 20
    )
    # Lowered yield threshold to 0.4 so yield=0.5 can trigger bonus
    # Noise threshold set to 0.25 to differentiate noise levels
    source_yield_bonus_threshold = memory_weights.get(
        "source_yield_bonus_threshold", 0.4
    )
    source_noise_penalty_threshold = memory_weights.get(
        "source_noise_penalty_threshold", 0.25
    )

    # If path_trust is very low (< threshold), apply penalty
    if path_trust_score is not None and path_trust_score < path_trust_penalty_threshold:
        final_score = final_score * 0.9  # 10% penalty
        score_breakdown["path_trust_penalty"] = {
            "applied": True,
            "threshold": path_trust_penalty_threshold,
            "actual": path_trust_score,
            "multiplier": 0.9,
        }

    # If yield is high and noise is low, apply bonus
    if (
        source_yield_score is not None
        and source_noise_score is not None
        and source_yield_score >= source_yield_bonus_threshold
        and source_noise_score <= source_noise_penalty_threshold
    ):
        final_score = final_score * 1.25  # 25% bonus for high yield, low noise
        # Cap at 100
        final_score = min(100.0, final_score)
        score_breakdown["source_quality_bonus"] = {
            "applied": True,
            "yield_threshold": source_yield_bonus_threshold,
            "yield_actual": source_yield_score,
            "noise_threshold": source_noise_penalty_threshold,
            "noise_actual": source_noise_score,
            "multiplier": 1.25,
        }

    # Update candidate_score
    candidate_score["candidate_score"] = final_score
    candidate_score["score_breakdown"] = score_breakdown
    candidate_score["weights_applied"] = weights
    candidate_score["scoring_version"] = "2.5"

    # Also ensure domain_trust_tier is preserved
    if "domain_trust_tier" in candidate:
        candidate_score["domain_trust_tier"] = candidate["domain_trust_tier"]

    return candidate
