"""
Triage Metrics Module.

Tracks and reports triage metrics for analysis.
"""

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
TRIAGE_DIR = BASE_DIR / "data" / "triage"

# Thread lock for atomic file operations
_lock = threading.Lock()


def _get_metrics_path() -> Path:
    """Get the metrics file path dynamically."""
    return TRIAGE_DIR / "metrics.json"


def _load_metrics() -> dict:
    """Load metrics from disk."""
    with _lock:
        metrics_path = _get_metrics_path()
        if not metrics_path.exists():
            return {"lanes": {}, "last_updated": None}
        try:
            with open(metrics_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {"lanes": {}, "last_updated": None}


def _save_metrics(metrics: dict) -> None:
    """Save metrics to disk atomically."""
    with _lock:
        metrics_path = _get_metrics_path()
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = metrics_path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, ensure_ascii=False)
        temp_path.replace(metrics_path)


def _get_lane_data(lane: str) -> dict:
    """Get or initialize lane data."""
    metrics = _load_metrics()
    if lane not in metrics.get("lanes", {}):
        metrics["lanes"][lane] = {
            "total_candidates": 0,
            "processed": 0,
            "deferred": 0,
            "discarded": 0,
            "priority_scores": [],
        }
    return metrics["lanes"][lane]


def record_triage_metric(candidate: dict, action: str) -> None:
    """Record a triage metric for a candidate.

    Args:
        candidate: Candidate dictionary with triage_result
        action: Action taken (process_now, defer, discard)
    """
    metrics = _load_metrics()
    lane = candidate.get("lane", "unknown")

    if lane not in metrics["lanes"]:
        metrics["lanes"][lane] = {
            "total_candidates": 0,
            "processed": 0,
            "deferred": 0,
            "discarded": 0,
            "priority_scores": [],
        }

    lane_data = metrics["lanes"][lane]
    lane_data["total_candidates"] += 1

    triage = candidate.get("triage_result", {})
    priority_score = triage.get("priority_score", 0)
    lane_data["priority_scores"].append(priority_score)

    if action == "process_now":
        lane_data["processed"] += 1
    elif action == "defer":
        lane_data["deferred"] += 1
    elif action == "discard":
        lane_data["discarded"] += 1

    metrics["last_updated"] = datetime.now(timezone.utc).isoformat()
    _save_metrics(metrics)


def get_lane_metrics(lane: str) -> dict:
    """Get metrics for a specific lane.

    Args:
        lane: Lane name

    Returns:
        Metrics dictionary for the lane with calculated fields
    """
    metrics = _load_metrics()
    lane_data = metrics.get("lanes", {}).get(
        lane,
        {
            "total_candidates": 0,
            "processed": 0,
            "deferred": 0,
            "discarded": 0,
            "priority_scores": [],
        },
    )

    # Calculate avg_priority_score
    scores = lane_data.get("priority_scores", [])
    if scores:
        avg_priority_score = sum(scores) / len(scores)
    else:
        avg_priority_score = 0.0

    # Calculate accepted_ratio (processed / total)
    total = lane_data.get("total_candidates", 0)
    processed = lane_data.get("processed", 0)
    if total > 0:
        accepted_ratio = processed / total
    else:
        accepted_ratio = 0.0

    return {
        "total_candidates": lane_data.get("total_candidates", 0),
        "processed": lane_data.get("processed", 0),
        "deferred": lane_data.get("deferred", 0),
        "discarded": lane_data.get("discarded", 0),
        "avg_priority_score": avg_priority_score,
        "accepted_ratio": accepted_ratio,
    }


def generate_triage_report() -> dict:
    """Generate a full triage report.

    Returns:
        Report dictionary with all lanes and summary
    """
    metrics = _load_metrics()
    lanes = metrics.get("lanes", {})

    report_lanes = {}
    total_candidates = 0
    total_processed = 0
    total_deferred = 0
    total_discarded = 0

    for lane_name in lanes:
        lane_metrics = get_lane_metrics(lane_name)
        report_lanes[lane_name] = lane_metrics
        total_candidates += lane_metrics["total_candidates"]
        total_processed += lane_metrics["processed"]
        total_deferred += lane_metrics["deferred"]
        total_discarded += lane_metrics["discarded"]

    # Calculate overall avg_priority_score
    all_scores = []
    for lane_data in lanes.values():
        all_scores.extend(lane_data.get("priority_scores", []))

    if all_scores:
        overall_avg_score = sum(all_scores) / len(all_scores)
    else:
        overall_avg_score = 0.0

    # Overall accepted ratio
    if total_candidates > 0:
        overall_accepted_ratio = total_processed / total_candidates
    else:
        overall_accepted_ratio = 0.0

    return {
        "lanes": report_lanes,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_candidates": total_candidates,
            "processed": total_processed,
            "deferred": total_deferred,
            "discarded": total_discarded,
            "avg_priority_score": overall_avg_score,
            "accepted_ratio": overall_accepted_ratio,
        },
    }


def reset_metrics() -> None:
    """Reset all metrics to initial state."""
    _save_metrics({"lanes": {}, "last_updated": datetime.now(timezone.utc).isoformat()})
