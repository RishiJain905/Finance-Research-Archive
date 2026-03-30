"""V2.5 Candidate Scoring Log Utilities.

Helpers for working with JSONL scoring logs.
"""

import json
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
SCORING_LOG_DIR = BASE_DIR / "logs" / "candidate_scoring"

# Ensure log directory exists on module load
ensure_scoring_log_dir()


def ensure_scoring_log_dir() -> Path:
    """Create and return path to logs/candidate_scoring/ directory."""
    SCORING_LOG_DIR.mkdir(parents=True, exist_ok=True)
    return SCORING_LOG_DIR


def get_latest_scoring_log() -> Path | None:
    """Find the most recent JSONL log file in logs/candidate_scoring/.

    Returns:
        Path to the most recent log file, or None if no logs exist.
    """
    if not SCORING_LOG_DIR.exists():
        return None

    jsonl_files = list(SCORING_LOG_DIR.glob("*.jsonl"))
    if not jsonl_files:
        return None

    return max(jsonl_files, key=lambda p: p.stat().st_mtime)


def get_scoring_logs(limit: int = 10) -> list[Path]:
    """Get N most recent log files sorted by modification time (newest first).

    Args:
        limit: Maximum number of log files to return.

    Returns:
        List of Path objects sorted by modification time (newest first).
    """
    if not SCORING_LOG_DIR.exists():
        return []

    jsonl_files = list(SCORING_LOG_DIR.glob("*.jsonl"))
    jsonl_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return jsonl_files[:limit]


def parse_scoring_log(log_path: Path) -> list[dict]:
    """Parse a JSONL log file and return list of records.

    Handles malformed lines gracefully by skipping them with a warning.

    Args:
        log_path: Path to the JSONL log file.

    Returns:
        List of parsed record dictionaries.
    """
    records = []
    with open(log_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                warnings.warn(f"Skipping malformed line {line_num} in {log_path}: {e}")
    return records


def summarize_scoring_log(log_path: Path) -> dict:
    """Return summary statistics for a scoring log file.

    Args:
        log_path: Path to the JSONL log file.

    Returns:
        Dictionary containing summary statistics:
        - total_candidates: int
        - process_count: int
        - defer_count: int
        - skip_count: int
        - avg_score: float (all candidates)
        - avg_score_process: float (process only)
        - bucket_counts: dict (high, medium, low, skip)
        - lane_counts: dict (trusted_sources, keyword_discovery, seed_crawl)
    """
    records = parse_scoring_log(log_path)

    total_candidates = len(records)
    process_count = 0
    defer_count = 0
    skip_count = 0

    total_score = 0.0
    process_score = 0.0
    process_count_for_avg = 0

    bucket_counts = {"high": 0, "medium": 0, "low": 0, "skip": 0}
    lane_counts = {"trusted_sources": 0, "keyword_discovery": 0, "seed_crawl": 0}

    for record in records:
        decision = record.get("process_decision", "")
        if decision == "process":
            process_count += 1
        elif decision == "defer":
            defer_count += 1
        elif decision == "skip":
            skip_count += 1

        score = record.get("final_score")
        if score is not None:
            total_score += score
            if decision == "process":
                process_score += score
                process_count_for_avg += 1

        bucket = record.get("priority_bucket", "")
        if bucket in bucket_counts:
            bucket_counts[bucket] += 1

        lane = record.get("lane", "")
        if lane in lane_counts:
            lane_counts[lane] += 1

    avg_score = total_score / total_candidates if total_candidates > 0 else 0.0
    avg_score_process = (
        process_score / process_count_for_avg if process_count_for_avg > 0 else 0.0
    )

    return {
        "total_candidates": total_candidates,
        "process_count": process_count,
        "defer_count": defer_count,
        "skip_count": skip_count,
        "avg_score": round(avg_score, 4),
        "avg_score_process": round(avg_score_process, 4),
        "bucket_counts": bucket_counts,
        "lane_counts": lane_counts,
    }


def filter_log_by_decision(log_path: Path, decision: str) -> list[dict]:
    """Filter log records by process_decision.

    Args:
        log_path: Path to the JSONL log file.
        decision: Decision value to filter by ("process", "defer", "skip").

    Returns:
        List of records matching the specified decision.
    """
    records = parse_scoring_log(log_path)
    return [r for r in records if r.get("process_decision") == decision]


def filter_log_by_bucket(log_path: Path, bucket: str) -> list[dict]:
    """Filter log records by priority_bucket.

    Args:
        log_path: Path to the JSONL log file.
        bucket: Bucket value to filter by ("high", "medium", "low", "skip").

    Returns:
        List of records matching the specified priority_bucket.
    """
    records = parse_scoring_log(log_path)
    return [r for r in records if r.get("priority_bucket") == bucket]


def filter_log_by_lane(log_path: Path, lane: str) -> list[dict]:
    """Filter log records by lane.

    Args:
        log_path: Path to the JSONL log file.
        lane: Lane value to filter by ("trusted_sources", "keyword_discovery", "seed_crawl").

    Returns:
        List of records matching the specified lane.
    """
    records = parse_scoring_log(log_path)
    return [r for r in records if r.get("lane") == lane]
