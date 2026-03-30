"""V2.5 Batch Candidate Scoring Module.

Batch scoring with priority routing and JSONL logging.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.candidate_utils import BASE_DIR

# Configuration paths
SCORING_RULES_PATH = BASE_DIR / "config" / "scoring_rules.json"
SCORING_LOG_DIR = BASE_DIR / "logs" / "candidate_scoring"


def load_scoring_rules() -> dict[str, Any]:
    """Load scoring rules from config/scoring_rules.json.

    Returns:
        Parsed scoring rules dictionary
    """
    if not SCORING_RULES_PATH.exists():
        return {}

    with open(SCORING_RULES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def route_to_priority_bucket(
    candidate_score: float, thresholds: dict[str, float]
) -> str:
    """Route candidate to priority bucket based on score.

    Args:
        candidate_score: The candidate's final score (0-100)
        thresholds: Dictionary with high, medium, low threshold values

    Returns:
        Bucket name: "high", "medium", "low", or "skip"
    """
    high_threshold = thresholds.get("high", 75)
    medium_threshold = thresholds.get("medium", 60)
    low_threshold = thresholds.get("low", 45)

    if candidate_score >= high_threshold:
        return "high"
    elif candidate_score >= medium_threshold:
        return "medium"
    elif candidate_score >= low_threshold:
        return "low"
    else:
        return "skip"


def determine_process_decision(bucket: str, priority_buckets: dict[str, str]) -> str:
    """Determine process decision based on priority bucket.

    Args:
        bucket: Priority bucket name ("high", "medium", "low", "skip")
        priority_buckets: Mapping of bucket to decision

    Returns:
        Process decision: "process", "defer", or "skip"
    """
    return priority_buckets.get(bucket, "skip")


def write_scoring_log(candidates: list[dict[str, Any]], output_dir: Path) -> Path:
    """Write scoring results to JSONL log file.

    Creates logs/candidate_scoring/YYYY-MM-DD_HH-MM-SS.jsonl with one record
    per candidate containing: candidate_id, lane, domain, title, candidate_score,
    score_breakdown, priority_bucket, process_decision, timestamp.

    Args:
        candidates: List of scored candidates with scoring metadata
        output_dir: Directory to write log file to

    Returns:
        Path to the created log file
    """
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate timestamp-based filename
    timestamp = datetime.now(timezone.utc)
    filename = timestamp.strftime("%Y-%m-%d_%H-%M-%S") + ".jsonl"
    log_path = output_dir / filename

    # Write JSONL file
    with open(log_path, "w", encoding="utf-8") as f:
        for candidate in candidates:
            # Extract required fields for log
            log_record = {
                "candidate_id": candidate.get("candidate_id", ""),
                "lane": candidate.get("lane", ""),
                "domain": candidate.get("source_domain", ""),
                "title": candidate.get("title", ""),
                "candidate_score": candidate.get("candidate_score", {}).get(
                    "candidate_score", 0
                ),
                "score_breakdown": candidate.get("candidate_score", {}).get(
                    "score_breakdown", {}
                ),
                "priority_bucket": candidate.get("priority_bucket", "skip"),
                "process_decision": candidate.get("process_decision", "skip"),
                "timestamp": timestamp.isoformat(),
            }
            f.write(json.dumps(log_record, ensure_ascii=False) + "\n")

    return log_path


def score_candidates_batch(
    candidates: list[dict[str, Any]], scoring_rules: dict[str, Any] = None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Score candidates in batch with priority routing.

    Scores each candidate using score_candidate, routes each to priority bucket,
    determines process decision, writes JSONL log, returns tuple of lists.

    Args:
        candidates: List of raw candidates to score
        scoring_rules: Optional pre-loaded scoring rules

    Returns:
        Tuple of (process_list, defer_list, skip_list) - each containing
        scored candidates with priority_bucket and process_decision added
    """
    # Import here to avoid circular dependency
    from scripts.extract_candidate_features import extract_candidate_features
    from scripts.score_candidate import score_candidate as score_single_candidate

    if scoring_rules is None:
        scoring_rules = load_scoring_rules()

    thresholds = scoring_rules.get("thresholds", {})
    priority_buckets = scoring_rules.get("priority_buckets", {})

    process_list = []
    defer_list = []
    skip_list = []

    for candidate in candidates:
        # Extract features
        candidate = extract_candidate_features(candidate)

        # Score candidate
        candidate = score_single_candidate(candidate, scoring_rules)

        # Get final score
        final_score = candidate.get("candidate_score", {}).get("candidate_score", 0)

        # Route to priority bucket
        priority_bucket = route_to_priority_bucket(final_score, thresholds)
        candidate["priority_bucket"] = priority_bucket

        # Determine process decision
        process_decision = determine_process_decision(priority_bucket, priority_buckets)
        candidate["process_decision"] = process_decision

        # Add to appropriate list
        if process_decision == "process":
            process_list.append(candidate)
        elif process_decision == "defer":
            defer_list.append(candidate)
        else:  # skip
            skip_list.append(candidate)

    # Write scoring log
    if candidates:
        all_candidates = process_list + defer_list + skip_list
        write_scoring_log(all_candidates, SCORING_LOG_DIR)

    return process_list, defer_list, skip_list
