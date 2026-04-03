"""Watchlist Hit Persistence Layer for V2.7 Part 3.

Provides functions for generating, saving, loading, and querying watchlist hits.
"""

import json
import os
import random
import string
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def generate_hit_id(
    watchlist_id: str, record_id: str, timestamp: Optional[str] = None
) -> str:
    """Generate a unique hit ID from watchlist_id + record_id + timestamp.

    Format: {watchlist_id}_{record_id}_{timestamp}
    If timestamp not provided, use current UTC time in YYYYMMDD_HHMMSS format.

    Args:
        watchlist_id: The ID of the watchlist
        record_id: The ID of the matched record
        timestamp: Optional timestamp in YYYYMMDD_HHMMSS format. If not provided,
                   current UTC time is used.

    Returns:
        A unique hit ID string
    """
    if timestamp is None:
        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y%m%d_%H%M%S")
    return f"{watchlist_id}_{record_id}_{timestamp}"


def save_watchlist_hit(hit: dict, hits_dir: str) -> Path:
    """Atomically save a watchlist hit to hits_dir/{hit_id}.json.

    Uses temp file + rename for atomicity.
    Creates directory if it doesn't exist.
    Returns path where hit was saved.

    Args:
        hit: The hit dictionary to save
        hits_dir: The directory to save hits in

    Returns:
        Path to the saved hit file
    """
    # Generate hit_id from the hit data
    timestamp_str = hit.get("created_at", "")
    # Convert ISO format to YYYYMMDD_HHMMSS if needed
    if timestamp_str:
        try:
            dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            timestamp_str = dt.strftime("%Y%m%d_%H%M%S")
        except ValueError:
            # If already in correct format or can't parse, use as-is
            pass

    hit_id = generate_hit_id(hit["watchlist_id"], hit["record_id"], timestamp_str)

    # Ensure directory exists
    os.makedirs(hits_dir, exist_ok=True)

    # Create temp file and atomically rename
    temp_fd, temp_path = tempfile.mkstemp(dir=hits_dir, suffix=".tmp", prefix="")
    try:
        with os.fdopen(temp_fd, "w") as f:
            json.dump(hit, f, indent=2)
        final_path = Path(hits_dir) / f"{hit_id}.json"
        os.replace(temp_path, final_path)
        return final_path
    except Exception:
        # Clean up temp file on failure
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise


def load_watchlist_hit(hit_id: str, hits_dir: str) -> dict:
    """Load a watchlist hit by ID from hits_dir.

    Args:
        hit_id: The hit ID (without .json extension)
        hits_dir: The directory containing hit files

    Returns:
        The hit dictionary

    Raises:
        FileNotFoundError: If the hit file doesn't exist
    """
    hit_path = Path(hits_dir) / f"{hit_id}.json"
    if not hit_path.exists():
        raise FileNotFoundError(f"Hit not found: {hit_id}")
    with open(hit_path, "r") as f:
        return json.load(f)


def list_hits_by_watchlist(watchlist_id: str, hits_dir: str) -> list[dict]:
    """List all hits for a given watchlist_id.

    Returns list of hit dicts sorted by created_at descending.

    Args:
        watchlist_id: The watchlist ID to filter by
        hits_dir: The directory containing hit files

    Returns:
        List of hit dictionaries for the specified watchlist
    """
    hits = []
    hits_path = Path(hits_dir)
    if not hits_path.exists():
        return hits

    for hit_file in hits_path.glob("*.json"):
        try:
            with open(hit_file, "r") as f:
                hit = json.load(f)
            if hit.get("watchlist_id") == watchlist_id:
                hits.append(hit)
        except (json.JSONDecodeError, IOError):
            continue

    # Sort by created_at descending
    hits.sort(key=lambda h: h.get("created_at", ""), reverse=True)
    return hits


def list_hits_by_record(record_id: str, hits_dir: str) -> list[dict]:
    """List all hits for a given record_id.

    Returns list of hit dicts sorted by created_at descending.

    Args:
        record_id: The record ID to filter by
        hits_dir: The directory containing hit files

    Returns:
        List of hit dictionaries for the specified record
    """
    hits = []
    hits_path = Path(hits_dir)
    if not hits_path.exists():
        return hits

    for hit_file in hits_path.glob("*.json"):
        try:
            with open(hit_file, "r") as f:
                hit = json.load(f)
            if hit.get("record_id") == record_id:
                hits.append(hit)
        except (json.JSONDecodeError, IOError):
            continue

    # Sort by created_at descending
    hits.sort(key=lambda h: h.get("created_at", ""), reverse=True)
    return hits


def list_hits_by_date_range(
    start_date: str, end_date: str, hits_dir: str
) -> list[dict]:
    """List all hits within a date range (ISO-8601 dates).

    Returns list of hit dicts sorted by created_at descending.

    Args:
        start_date: Start date in ISO-8601 format (YYYY-MM-DD)
        end_date: End date in ISO-8601 format (YYYY-MM-DD)
        hits_dir: The directory containing hit files

    Returns:
        List of hit dictionaries within the date range
    """
    hits = []
    hits_path = Path(hits_dir)
    if not hits_path.exists():
        return hits

    # Parse dates for comparison
    start_dt = datetime.fromisoformat(start_date)
    end_dt = datetime.fromisoformat(end_date)
    # Set end_dt to end of day
    end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

    for hit_file in hits_path.glob("*.json"):
        try:
            with open(hit_file, "r") as f:
                hit = json.load(f)
            created_at_str = hit.get("created_at", "")
            if created_at_str:
                # Parse ISO format and strip timezone for comparison
                created_at_str = created_at_str.replace("Z", "+00:00")
                created_dt = datetime.fromisoformat(created_at_str)
                # Normalize to UTC date for comparison
                if (
                    start_dt
                    <= created_dt.replace(tzinfo=None)
                    <= end_dt.replace(tzinfo=None)
                ):
                    hits.append(hit)
        except (json.JSONDecodeError, IOError, ValueError):
            continue

    # Sort by created_at descending
    hits.sort(key=lambda h: h.get("created_at", ""), reverse=True)
    return hits


def get_watchlist_metrics(
    watchlist_id: str, hits_dir: str, records_dir: Optional[str] = None
) -> dict:
    """Compute metrics for a watchlist.

    Args:
        watchlist_id: The watchlist ID to compute metrics for
        hits_dir: The directory containing hit files
        records_dir: Optional directory containing record files for status lookup

    Returns:
        Dictionary with metrics:
        - total_hits: int
        - accepted_hits: int (hits where record status is 'accepted')
        - review_hits: int (hits where record status is 'review_queue')
        - rejected_hits: int (hits where record status is 'rejected')
        - hits_per_day: dict of {date: count}
        - source_domains: list of unique source domains (requires records_dir)
        - latest_hit_at: str or None
    """
    hits = list_hits_by_watchlist(watchlist_id, hits_dir)

    metrics = {
        "total_hits": len(hits),
        "accepted_hits": 0,
        "review_hits": 0,
        "rejected_hits": 0,
        "hits_per_day": {},
        "source_domains": [],
        "latest_hit_at": None,
    }

    if not hits:
        return metrics

    # Get record statuses if records_dir provided
    record_statuses = {}
    source_domains = set()
    if records_dir and os.path.exists(records_dir):
        records_path = Path(records_dir)
        for hit in hits:
            record_id = hit.get("record_id")
            if record_id:
                record_path = records_path / f"{record_id}.json"
                if record_path.exists():
                    try:
                        with open(record_path, "r") as f:
                            record = json.load(f)
                        record_statuses[record_id] = record.get("status", "")
                        source_domain = record.get("source_domain", "")
                        if source_domain:
                            source_domains.add(source_domain)
                    except (json.JSONDecodeError, IOError):
                        pass

    # Compute metrics
    for hit in hits:
        record_id = hit.get("record_id", "")
        status = record_statuses.get(record_id, "")

        if status == "accepted":
            metrics["accepted_hits"] += 1
        elif status == "review_queue":
            metrics["review_hits"] += 1
        elif status == "rejected":
            metrics["rejected_hits"] += 1

        # Compute hits per day
        created_at = hit.get("created_at", "")
        if created_at:
            date_str = created_at.split("T")[0]  # Get YYYY-MM-DD part
            metrics["hits_per_day"][date_str] = (
                metrics["hits_per_day"].get(date_str, 0) + 1
            )

    metrics["source_domains"] = sorted(list(source_domains))

    # Get latest hit timestamp
    if hits:
        metrics["latest_hit_at"] = max(
            (h.get("created_at") for h in hits if h.get("created_at")), default=None
        )

    return metrics


def ensure_dirs_exist(*dirs: str) -> None:
    """Ensure all specified directories exist. Create if needed.

    Args:
        *dirs: Variable number of directory paths to ensure exist
    """
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)
