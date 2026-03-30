"""Memory Manager Module.

V2.5 Part 2 Source and Domain Memory implementation.
Manages adaptive trust scoring based on outcome history with cold-start blending.

Dependencies:
- Uses scripts.memory_persistence for persistence operations
- Uses config/scoring_rules.json for baseline domain trust
- Uses config/memory_config.json for cold-start and weight configuration
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from scripts import memory_persistence
from scripts.candidate_utils import BASE_DIR
from scripts.extract_candidate_features import load_scoring_rules

# ============================================================================
# Configuration Loading
# ============================================================================


def _load_memory_config() -> dict[str, Any]:
    """Load memory configuration with fallback defaults.

    Returns:
        Memory configuration dictionary
    """
    try:
        return memory_persistence.load_memory_config()
    except Exception:
        return _default_memory_config()


def _default_memory_config() -> dict[str, Any]:
    """Return hardcoded default memory configuration.

    Returns:
        Default configuration dictionary
    """
    return {
        "cold_start": {
            "min_samples_for_learning": 10,
            "full_learning_threshold": 25,
            "blend_formula": "linear",
        },
        "weights": {
            "accepted_weight": 10,
            "rejected_weight": 5,
            "filtered_weight": 3,
            "review_weight": 0,
            "human_multiplier": 2.0,
        },
        "trust_score_bounds": {
            "min": 1,
            "max": 100,
        },
        "logging": {
            "log_dir": "logs/source_memory",
            "log_updates": True,
        },
    }


# ============================================================================
# Logging Setup
# ============================================================================


def _get_memory_logger() -> logging.Logger:
    """Get or create logger for memory updates.

    Returns:
        Logger instance for memory updates
    """
    config = _load_memory_config()
    log_dir = BASE_DIR / config.get("logging", {}).get("log_dir", "logs/source_memory")
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("memory_manager")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler(log_dir / "memory_updates.jsonl")
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)

    return logger


def _log_memory_update(
    domain: str,
    path_pattern: Optional[str],
    source_id: Optional[str],
    outcome: str,
    before_trust: float,
    after_trust: float,
    candidate_id: Optional[str] = None,
) -> None:
    """Log a memory update to the JSONL log file.

    Args:
        domain: Domain name
        path_pattern: Path pattern (if applicable)
        source_id: Source ID (if applicable)
        outcome: Outcome type
        before_trust: Trust score before update
        after_trust: Trust score after update
        candidate_id: Candidate ID (if available)
    """
    config = _load_memory_config()
    if not config.get("logging", {}).get("log_updates", True):
        return

    logger = _get_memory_logger()
    log_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "candidate_id": candidate_id,
        "domain": domain,
        "path_pattern": path_pattern,
        "source_id": source_id,
        "outcome": outcome,
        "before_trust": before_trust,
        "after_trust": after_trust,
    }
    logger.info(json.dumps(log_entry))


# ============================================================================
# Domain Trust Baseline Lookup
# ============================================================================


def _get_domain_baseline_trust(domain: str) -> float:
    """Look up baseline trust for a domain from scoring_rules.json.

    Args:
        domain: Domain name

    Returns:
        Baseline trust value (high=100, medium=50, low=10, default=10)
    """
    try:
        scoring_rules = load_scoring_rules()
        domain_lower = domain.lower()

        domain_trust_baselines = scoring_rules.get("domain_trust_baselines", {})

        # Check high trust domains
        high_domains = domain_trust_baselines.get("high", [])
        if domain_lower in [d.lower() for d in high_domains]:
            return 100.0

        # Check medium trust domains
        medium_domains = domain_trust_baselines.get("medium", [])
        if domain_lower in [d.lower() for d in medium_domains]:
            return 50.0

        # Low trust or unknown - default to 10
        return 10.0

    except Exception:
        # Default baseline if scoring_rules.json is missing or corrupted
        return 10.0


# ============================================================================
# Domain Memory Operations
# ============================================================================


def get_or_create_domain_memory(domain: str) -> dict[str, Any]:
    """Get existing domain memory or create new one with baseline trust.

    Looks up baseline trust from scoring_rules.json domain_trust_baselines:
    - high → 100
    - medium → 50
    - low → 10 (default)

    Args:
        domain: Domain name (e.g., 'brookings.edu')

    Returns:
        Initialized domain memory dictionary
    """
    # Try to get existing memory first
    existing = memory_persistence.get_domain_memory(domain)
    if existing is not None:
        return existing

    # Get baseline trust from scoring rules
    baseline_trust = _get_domain_baseline_trust(domain)

    # Initialize new memory
    memory = memory_persistence.initialize_domain_memory(
        domain=domain,
        baseline_trust=baseline_trust,
        trust_score=baseline_trust,
    )

    # Save and return
    memory_persistence.save_domain_memory(domain, memory)
    return memory


def update_domain_memory_on_outcome(
    domain: str,
    outcome: str,
    candidate_id: Optional[str] = None,
) -> dict[str, Any]:
    """Update domain memory counters based on outcome.

    Increments appropriate counters based on outcome type:
    - accepted: accepted_auto_count
    - accepted_human: accepted_human_count
    - rejected: rejected_auto_count
    - rejected_human: rejected_human_count
    - review: review_count
    - review_human: review_human_count
    - filtered_out: filtered_out_count

    After updating counters, recalculates trust_score and yield/noise scores.

    Args:
        domain: Domain name
        outcome: Outcome type (accepted, accepted_human, rejected, rejected_human,
                 review, review_human, filtered_out)
        candidate_id: Optional candidate ID for logging

    Returns:
        Updated domain memory dictionary
    """
    memory = get_or_create_domain_memory(domain)
    before_trust = memory.get("trust_score", 50.0)

    # Increment total candidates
    memory["total_candidates"] = memory.get("total_candidates", 0) + 1

    # Increment appropriate counter based on outcome
    if outcome == "accepted":
        memory["accepted_count"] = memory.get("accepted_count", 0) + 1
        memory["accepted_auto_count"] = memory.get("accepted_auto_count", 0) + 1
    elif outcome == "accepted_human":
        memory["accepted_count"] = memory.get("accepted_count", 0) + 1
        memory["accepted_human_count"] = memory.get("accepted_human_count", 0) + 1
    elif outcome == "rejected":
        memory["rejected_count"] = memory.get("rejected_count", 0) + 1
        memory["rejected_auto_count"] = memory.get("rejected_auto_count", 0) + 1
    elif outcome == "rejected_human":
        memory["rejected_count"] = memory.get("rejected_count", 0) + 1
        memory["rejected_human_count"] = memory.get("rejected_human_count", 0) + 1
    elif outcome == "review":
        memory["review_count"] = memory.get("review_count", 0) + 1
    elif outcome == "review_human":
        memory["review_count"] = memory.get("review_count", 0) + 1
        memory["review_human_count"] = memory.get("review_human_count", 0) + 1
    elif outcome == "filtered_out":
        memory["filtered_out_count"] = memory.get("filtered_out_count", 0) + 1

    # Recalculate trust score
    memory["trust_score"] = compute_trust_score(
        total=memory["total_candidates"],
        accepted=memory.get("accepted_count", 0),
        rejected=memory.get("rejected_count", 0),
        filtered_out=memory.get("filtered_out_count", 0),
        review=memory.get("review_count", 0),
        accepted_human=memory.get("accepted_human_count", 0),
        rejected_human=memory.get("rejected_human_count", 0),
        baseline_trust=memory.get("baseline_trust", 10.0),
    )

    # Recalculate yield and noise scores
    yield_score, noise_score = compute_yield_noise(memory)
    memory["yield_score"] = yield_score
    memory["noise_score"] = noise_score

    # Save updated memory
    memory_persistence.save_domain_memory(domain, memory)

    # Log the update
    _log_memory_update(
        domain=domain,
        path_pattern=None,
        source_id=None,
        outcome=outcome,
        before_trust=before_trust,
        after_trust=memory["trust_score"],
        candidate_id=candidate_id,
    )

    return memory


# ============================================================================
# Path Memory Operations
# ============================================================================


def extract_path_pattern(url: str) -> str:
    """Extract literal path segment from URL.

    Uses literal path segment extraction:
    - Input: "https://brookings.edu/research/economy/analysis"
    - Extract: "/research/" (first path segment after domain)

    Args:
        url: Full URL

    Returns:
        First path segment with leading/trailing slashes, or empty string if no path
    """
    try:
        # Handle URLs that may not have a scheme
        if not url.startswith(("http://", "https://")):
            url = "https://" + url

        from urllib.parse import urlparse

        parsed = urlparse(url)
        path = parsed.path

        if not path or path == "/":
            return ""

        # Split path and get first segment
        segments = [s for s in path.split("/") if s]
        if not segments:
            return ""

        # Return first segment with leading and trailing slashes
        return "/" + segments[0] + "/"

    except Exception:
        return ""


def get_or_create_path_memory(domain: str, path_pattern: str) -> dict[str, Any]:
    """Get existing path memory or create new one with baseline trust.

    Default baseline trust for paths is 10 (low) unless domain is high-trust.
    For high-trust domains (baseline >= 100), path baseline is 50.

    Args:
        domain: Domain name
        path_pattern: Path pattern (e.g., '/research/')

    Returns:
        Initialized path memory dictionary
    """
    # Try to get existing memory first
    existing = memory_persistence.get_path_memory(domain, path_pattern)
    if existing is not None:
        return existing

    # Determine baseline trust based on domain trust
    domain_baseline = _get_domain_baseline_trust(domain)
    if domain_baseline >= 100:
        # High-trust domain, path gets medium baseline
        path_baseline = 50.0
    else:
        path_baseline = 10.0

    # Initialize new memory
    memory = memory_persistence.initialize_path_memory(
        domain=domain,
        path_pattern=path_pattern,
        baseline_trust=path_baseline,
    )

    # Save and return
    memory_persistence.save_path_memory(domain, path_pattern, memory)
    return memory


def update_path_memory_on_outcome(
    domain: str,
    path_pattern: str,
    outcome: str,
    candidate_id: Optional[str] = None,
) -> dict[str, Any]:
    """Update path memory counters based on outcome.

    Similar to domain memory update but for path patterns.

    Args:
        domain: Domain name
        path_pattern: Path pattern (e.g., '/research/')
        outcome: Outcome type
        candidate_id: Optional candidate ID for logging

    Returns:
        Updated path memory dictionary
    """
    memory = get_or_create_path_memory(domain, path_pattern)
    before_trust = memory.get("trust_score", 50.0)

    # Increment total candidates
    memory["total_candidates"] = memory.get("total_candidates", 0) + 1

    # Increment appropriate counter based on outcome
    if outcome == "accepted":
        memory["accepted_count"] = memory.get("accepted_count", 0) + 1
    elif outcome == "accepted_human":
        memory["accepted_count"] = memory.get("accepted_count", 0) + 1
        memory["accepted_human_count"] = memory.get("accepted_human_count", 0) + 1
    elif outcome == "rejected":
        memory["rejected_count"] = memory.get("rejected_count", 0) + 1
    elif outcome == "rejected_human":
        memory["rejected_count"] = memory.get("rejected_count", 0) + 1
        memory["rejected_human_count"] = memory.get("rejected_human_count", 0) + 1
    elif outcome == "review":
        memory["review_count"] = memory.get("review_count", 0) + 1
    elif outcome == "review_human":
        memory["review_count"] = memory.get("review_count", 0) + 1
        memory["review_human_count"] = memory.get("review_human_count", 0) + 1
    elif outcome == "filtered_out":
        memory["filtered_out_count"] = memory.get("filtered_out_count", 0) + 1

    # Recalculate trust score
    memory["trust_score"] = compute_trust_score(
        total=memory["total_candidates"],
        accepted=memory.get("accepted_count", 0),
        rejected=memory.get("rejected_count", 0),
        filtered_out=memory.get("filtered_out_count", 0),
        review=memory.get("review_count", 0),
        accepted_human=memory.get("accepted_human_count", 0),
        rejected_human=memory.get("rejected_human_count", 0),
        baseline_trust=memory.get("baseline_trust", 10.0),
    )

    # Recalculate yield and noise scores
    yield_score, noise_score = compute_yield_noise(memory)
    memory["yield_score"] = yield_score
    memory["noise_score"] = noise_score

    # Save updated memory
    memory_persistence.save_path_memory(domain, path_pattern, memory)

    # Log the update
    _log_memory_update(
        domain=domain,
        path_pattern=path_pattern,
        source_id=None,
        outcome=outcome,
        before_trust=before_trust,
        after_trust=memory["trust_score"],
        candidate_id=candidate_id,
    )

    return memory


# ============================================================================
# Source Memory Operations
# ============================================================================


def get_or_create_source_memory(
    source_id: str,
    source_type: str = "manual",
    source_domain: Optional[str] = None,
) -> dict[str, Any]:
    """Get existing source memory or create new one.

    Default baseline trust is 50 for unknown sources.

    Args:
        source_id: Source identifier
        source_type: Type of source (manual, trusted_sources, keyword_discovery, seed_crawl)
        source_domain: Primary domain associated with this source

    Returns:
        Initialized source memory dictionary
    """
    # Try to get existing memory first
    existing = memory_persistence.get_source_memory(source_id)
    if existing is not None:
        return existing

    # Determine baseline trust based on source type
    if source_type == "trusted_sources":
        baseline_trust = 100.0
    elif source_type == "keyword_discovery":
        baseline_trust = 50.0
    elif source_type == "seed_crawl":
        baseline_trust = 30.0
    else:
        # manual or unknown
        baseline_trust = 50.0

    # Initialize new memory
    memory = memory_persistence.initialize_source_memory(
        source_id=source_id,
        source_type=source_type,
        source_domain=source_domain,
        baseline_trust=baseline_trust,
    )

    # Save and return
    memory_persistence.save_source_memory(source_id, memory)
    return memory


def update_source_memory_on_outcome(
    source_id: str,
    outcome: str,
    candidate_id: Optional[str] = None,
) -> dict[str, Any]:
    """Update source memory counters based on outcome.

    Args:
        source_id: Source identifier
        outcome: Outcome type
        candidate_id: Optional candidate ID for logging

    Returns:
        Updated source memory dictionary
    """
    memory = memory_persistence.get_source_memory(source_id)
    if memory is None:
        # Source memory doesn't exist, create with defaults
        memory = get_or_create_source_memory(source_id)

    before_trust = memory.get("trust_score", 50.0)

    # Increment total candidates
    memory["total_candidates"] = memory.get("total_candidates", 0) + 1

    # Increment appropriate counter based on outcome
    if outcome == "accepted":
        memory["accepted_count"] = memory.get("accepted_count", 0) + 1
    elif outcome == "accepted_human":
        memory["accepted_count"] = memory.get("accepted_count", 0) + 1
        memory["accepted_human_count"] = memory.get("accepted_human_count", 0) + 1
    elif outcome == "rejected":
        memory["rejected_count"] = memory.get("rejected_count", 0) + 1
    elif outcome == "rejected_human":
        memory["rejected_count"] = memory.get("rejected_count", 0) + 1
        memory["rejected_human_count"] = memory.get("rejected_human_count", 0) + 1
    elif outcome == "review":
        memory["review_count"] = memory.get("review_count", 0) + 1
    elif outcome == "review_human":
        memory["review_count"] = memory.get("review_count", 0) + 1
        memory["review_human_count"] = memory.get("review_human_count", 0) + 1
    elif outcome == "filtered_out":
        memory["filtered_out_count"] = memory.get("filtered_out_count", 0) + 1

    # Recalculate trust score
    memory["trust_score"] = compute_trust_score(
        total=memory["total_candidates"],
        accepted=memory.get("accepted_count", 0),
        rejected=memory.get("rejected_count", 0),
        filtered_out=memory.get("filtered_out_count", 0),
        review=memory.get("review_count", 0),
        accepted_human=memory.get("accepted_human_count", 0),
        rejected_human=memory.get("rejected_human_count", 0),
        baseline_trust=memory.get("baseline_trust", 50.0),
    )

    # Recalculate yield and noise scores
    yield_score, noise_score = compute_yield_noise(memory)
    memory["yield_score"] = yield_score
    memory["noise_score"] = noise_score

    # Save updated memory
    memory_persistence.save_source_memory(source_id, memory)

    # Log the update
    _log_memory_update(
        domain=memory.get("source_domain", ""),
        path_pattern=None,
        source_id=source_id,
        outcome=outcome,
        before_trust=before_trust,
        after_trust=memory["trust_score"],
        candidate_id=candidate_id,
    )

    return memory


# ============================================================================
# Trust Score Computation
# ============================================================================


def compute_trust_score(
    total: int,
    accepted: int,
    rejected: int,
    filtered_out: int,
    review: int,
    accepted_human: int = 0,
    rejected_human: int = 0,
    baseline_trust: float = 10.0,
) -> float:
    """Compute trust score based on outcomes and cold-start blending.

    Formula:
        trust_score = baseline_trust + adjustment

    Where adjustment is based on:
    - accepted_weight * accepted_count (positive)
    - rejected_weight * rejected_count (negative)
    - filtered_weight * filtered_out_count (negative)
    - review_weight * review_count (neutral/partial)
    - Human decisions get human_multiplier applied

    Cold-start blending:
    - Before min_samples: mostly baseline (e.g., 90% baseline, 10% learned)
    - Between min and full: linear blend
    - After full_learning_threshold: mostly learned (e.g., 10% baseline, 90% learned)

    Config comes from memory_config.json:
    - cold_start.min_samples_for_learning = 10
    - cold_start.full_learning_threshold = 25
    - weights.accepted_weight = 10
    - weights.rejected_weight = 5
    - weights.filtered_weight = 3
    - weights.human_multiplier = 2.0
    - trust_score_bounds.min = 1 (prevent zero-trust)

    Args:
        total: Total number of candidates
        accepted: Number of accepted candidates
        rejected: Number of rejected candidates
        filtered_out: Number of filtered out candidates
        review: Number of review queue candidates
        accepted_human: Number of human-accepted candidates
        rejected_human: Number of human-rejected candidates
        baseline_trust: Initial baseline trust score

    Returns:
        Trust score clamped to min/max bounds
    """
    config = _load_memory_config()

    # Get cold-start configuration
    cold_start = config.get("cold_start", {})
    min_samples = cold_start.get("min_samples_for_learning", 10)
    full_threshold = cold_start.get("full_learning_threshold", 25)

    # Get weights
    weights = config.get("weights", {})
    accepted_weight = weights.get("accepted_weight", 10)
    rejected_weight = weights.get("rejected_weight", 5)
    filtered_weight = weights.get("filtered_weight", 3)
    review_weight = weights.get("review_weight", 0)
    human_multiplier = weights.get("human_multiplier", 2.0)

    # Get bounds
    bounds = config.get("trust_score_bounds", {})
    min_trust = bounds.get("min", 1)
    max_trust = bounds.get("max", 100)

    # Calculate learned adjustment (before cold-start blending)
    # Human decisions get multiplied
    # Human contributions: human decisions count human_multiplier times
    # Auto contributions: auto decisions count once
    human_accepted_contribution = accepted_human * accepted_weight * human_multiplier
    auto_accepted_contribution = accepted * accepted_weight
    total_accepted_contribution = (
        human_accepted_contribution + auto_accepted_contribution
    )

    human_rejected_contribution = rejected_human * rejected_weight * human_multiplier
    auto_rejected_contribution = rejected * rejected_weight
    total_rejected_contribution = (
        human_rejected_contribution + auto_rejected_contribution
    )

    # Calculate net adjustment from all outcomes
    adjustment = (
        total_accepted_contribution
        - total_rejected_contribution
        - (filtered_out * filtered_weight)
        + (review * review_weight)
    )

    # Calculate learned trust (what the score would be based purely on outcomes)
    learned_trust = baseline_trust + adjustment

    # Apply cold-start blending
    if total <= 0:
        # No data yet, use baseline
        trust_score = baseline_trust
    elif total < min_samples:
        # Before minimum samples: heavily weight baseline
        # Blend factor: 0.9 baseline, 0.1 learned at min_samples=0 → 0.5 baseline, 0.5 learned at min_samples
        blend_factor = total / min_samples if min_samples > 0 else 0
        baseline_weight = 1.0 - (blend_factor * 0.1)  # Goes from 0.9 to 1.0
        learned_weight = blend_factor * 0.1  # Goes from 0.0 to 0.1
        trust_score = (baseline_weight * baseline_trust) + (
            learned_weight * learned_trust
        )
    elif total >= full_threshold:
        # After full learning threshold: heavily weight learned
        # Blend factor: 0.1 baseline, 0.9 learned at full_threshold and beyond
        baseline_weight = 0.1
        learned_weight = 0.9
        trust_score = (baseline_weight * baseline_trust) + (
            learned_weight * learned_trust
        )
    else:
        # Between min and full: linear blend
        # At min_samples: 0.9 baseline, 0.1 learned
        # At full_threshold: 0.1 baseline, 0.9 learned
        blend_ratio = (total - min_samples) / (full_threshold - min_samples)
        baseline_weight = 1.0 - 0.1 - (blend_ratio * 0.8)  # Goes from 0.9 to 0.1
        learned_weight = 0.1 + (blend_ratio * 0.8)  # Goes from 0.1 to 0.9
        trust_score = (baseline_weight * baseline_trust) + (
            learned_weight * learned_trust
        )

    # Clamp to bounds (prevent zero-trust or exceeding max)
    return max(min_trust, min(max_trust, trust_score))


def compute_yield_noise(memory: dict[str, Any]) -> tuple[float, float]:
    """Compute yield_score and noise_score.

    yield_score = accepted_count / total_candidates
    noise_score = (filtered_out_count + rejected_count) / total_candidates

    Args:
        memory: Memory dictionary with counters

    Returns:
        Tuple of (yield_score, noise_score)
    """
    total = memory.get("total_candidates", 0)
    if total <= 0:
        return (0.0, 0.0)

    accepted = memory.get("accepted_count", 0)
    filtered_out = memory.get("filtered_out_count", 0)
    rejected = memory.get("rejected_count", 0)

    yield_score = accepted / total
    noise_score = (filtered_out + rejected) / total

    return (yield_score, noise_score)


# ============================================================================
# Combined Update Function
# ============================================================================


def update_all_memory_on_outcome(
    domain: str,
    outcome: str,
    source_id: Optional[str] = None,
    source_type: str = "manual",
    url: Optional[str] = None,
    candidate_id: Optional[str] = None,
) -> dict[str, Any]:
    """Update all relevant memory records after a candidate outcome.

    This is the main entry point called after routing.

    1. Extract path_pattern from URL if provided
    2. Update domain memory
    3. Update path memory if path_pattern exists
    4. Update source memory if source_id provided
    5. Return summary of updates

    Args:
        domain: Domain name
        outcome: Outcome type (accepted, accepted_human, rejected, rejected_human,
                review, review_human, filtered_out)
        source_id: Optional source identifier
        source_type: Type of source (manual, trusted_sources, keyword_discovery, seed_crawl)
        url: Full URL for path pattern extraction
        candidate_id: Optional candidate ID for logging

    Returns:
        Dictionary summarizing all memory updates
    """
    # Extract path pattern from URL if provided
    path_pattern = None
    if url:
        path_pattern = extract_path_pattern(url)

    # Update domain memory
    domain_memory = update_domain_memory_on_outcome(domain, outcome, candidate_id)

    # Initialize result summary
    result = {
        "domain_memory": domain_memory,
        "path_memory": None,
        "source_memory": None,
        "path_pattern": path_pattern,
    }

    # Update path memory if path pattern exists
    if path_pattern:
        path_memory = update_path_memory_on_outcome(
            domain, path_pattern, outcome, candidate_id
        )
        result["path_memory"] = path_memory

    # Update source memory if source_id provided
    if source_id:
        source_memory = get_or_create_source_memory(source_id, source_type, domain)
        source_memory = update_source_memory_on_outcome(
            source_id, outcome, candidate_id
        )
        result["source_memory"] = source_memory

    return result


# ============================================================================
# Convenience Functions
# ============================================================================


def get_domain_trust(domain: str) -> float:
    """Get current trust score for a domain.

    Args:
        domain: Domain name

    Returns:
        Current trust score, or baseline if no memory exists
    """
    memory = memory_persistence.get_domain_memory(domain)
    if memory is None:
        return _get_domain_baseline_trust(domain)
    return memory.get("trust_score", _get_domain_baseline_trust(domain))


def get_path_trust(domain: str, path_pattern: str) -> float:
    """Get current trust score for a path pattern.

    Args:
        domain: Domain name
        path_pattern: Path pattern

    Returns:
        Current trust score, or baseline if no memory exists
    """
    memory = memory_persistence.get_path_memory(domain, path_pattern)
    if memory is None:
        domain_baseline = _get_domain_baseline_trust(domain)
        if domain_baseline >= 100:
            return 50.0
        return 10.0
    return memory.get("trust_score", 10.0)


def get_source_trust(source_id: str) -> float:
    """Get current trust score for a source.

    Args:
        source_id: Source identifier

    Returns:
        Current trust score, or 50.0 (default) if no memory exists
    """
    memory = memory_persistence.get_source_memory(source_id)
    if memory is None:
        return 50.0
    return memory.get("trust_score", 50.0)


def get_all_domain_trust() -> dict[str, float]:
    """Get trust scores for all domains.

    Returns:
        Dictionary mapping domain to trust score
    """
    all_memory = memory_persistence.get_all_domain_memory()
    return {domain: mem.get("trust_score", 10.0) for domain, mem in all_memory.items()}


def get_all_source_trust() -> dict[str, float]:
    """Get trust scores for all sources.

    Returns:
        Dictionary mapping source_id to trust score
    """
    all_memory = memory_persistence.get_all_source_memory()
    return {
        source_id: mem.get("trust_score", 50.0) for source_id, mem in all_memory.items()
    }
