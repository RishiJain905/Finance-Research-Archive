"""Thesis State Updater for V2.7 Part 3.

Provides functions for managing thesis state for watchlists based on signals
extracted from watchlist hits.
"""

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

DEFAULT_THESES_DIR = "data/theses"
STRENGTHENING_THRESHOLD = 0.6  # If >60% of signals are strengthening → strengthening
WEAKENING_THRESHOLD = 0.6  # If >60% of signals are weakening → weakening
LOOKBACK_DAYS = 7  # Consider hits from last 7 days


def load_thesis_state(watchlist_id: str, theses_dir: str = DEFAULT_THESES_DIR) -> dict:
    """Load thesis state for a watchlist.

    Returns dict with: watchlist_id, current_state, signals_history, last_updated
    If no state exists, returns default neutral state.

    Args:
        watchlist_id: The ID of the watchlist
        theses_dir: The directory containing thesis state files

    Returns:
        Dictionary with thesis state information
    """
    state_path = Path(theses_dir) / f"{watchlist_id}_thesis.json"

    if state_path.exists():
        try:
            with open(state_path, "r") as f:
                state = json.load(f)
            return state
        except (json.JSONDecodeError, IOError):
            pass

    # Return default neutral state
    now = datetime.now(timezone.utc).isoformat()
    return {
        "watchlist_id": watchlist_id,
        "current_state": "neutral",
        "signals_history": [],
        "last_updated": now,
        "state_changes": 0,
    }


def save_thesis_state(state: dict, theses_dir: str = DEFAULT_THESES_DIR) -> Path:
    """Save thesis state to theses_dir/{watchlist_id}_thesis.json.

    Uses atomic write (temp + rename).
    Returns path where state was saved.

    Args:
        state: The thesis state dictionary to save
        theses_dir: The directory to save thesis state in

    Returns:
        Path to the saved state file
    """
    watchlist_id = state["watchlist_id"]
    os.makedirs(theses_dir, exist_ok=True)

    # Create temp file and atomically rename
    temp_fd, temp_path = tempfile.mkstemp(dir=theses_dir, suffix=".tmp", prefix="")
    try:
        with os.fdopen(temp_fd, "w") as f:
            json.dump(state, f, indent=2)
        final_path = Path(theses_dir) / f"{watchlist_id}_thesis.json"
        os.replace(temp_path, final_path)
        return final_path
    except Exception:
        # Clean up temp file on failure
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise


def compute_thesis_from_hits(
    watchlist_id: str, hits: list[dict], lookback_days: int = LOOKBACK_DAYS
) -> str:
    """Compute thesis state from recent hits.

    - Count strengthening, weakening, neutral signals in lookback window
    - If strengthening > weakening by threshold → "strengthening"
    - If weakening > strengthening by threshold → "weakening"
    - Otherwise → "neutral"

    Args:
        watchlist_id: The watchlist ID (for compatibility, not used)
        hits: List of hit dictionaries with thesis_signal and created_at fields
        lookback_days: Number of days to look back for hits

    Returns:
        Thesis state string: "strengthening", "weakening", or "neutral"
    """
    if not hits:
        return "neutral"

    # Calculate cutoff date (naive UTC for comparison)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=lookback_days)
    cutoff_naive = cutoff.replace(tzinfo=None)

    # Filter hits within lookback window
    recent_hits = []
    for hit in hits:
        created_at_str = hit.get("created_at", "")
        if created_at_str:
            try:
                # Parse ISO format
                created_at_str = created_at_str.replace("Z", "+00:00")
                created_at = datetime.fromisoformat(created_at_str)
                # Remove timezone for comparison with cutoff
                if created_at.replace(tzinfo=None) >= cutoff_naive:
                    recent_hits.append(hit)
            except ValueError:
                continue

    if not recent_hits:
        return "neutral"

    # Count signals
    strengthening_count = sum(
        1 for h in recent_hits if h.get("thesis_signal") == "strengthening"
    )
    weakening_count = sum(
        1 for h in recent_hits if h.get("thesis_signal") == "weakening"
    )
    neutral_count = sum(1 for h in recent_hits if h.get("thesis_signal") == "neutral")

    total_signals = len(recent_hits)

    # Check for strengthening threshold (>60%)
    if (
        strengthening_count > 0
        and strengthening_count / total_signals > STRENGTHENING_THRESHOLD
    ):
        return "strengthening"

    # Check for weakening threshold (>60%)
    if weakening_count > 0 and weakening_count / total_signals > WEAKENING_THRESHOLD:
        return "weakening"

    return "neutral"


def update_thesis_state(
    watchlist_id: str, new_signal: str, theses_dir: str = DEFAULT_THESES_DIR
) -> dict:
    """Update thesis state for a watchlist with a new signal.

    Loads current state, adds new signal to history, computes new thesis, saves.

    Args:
        watchlist_id: The ID of the watchlist
        new_signal: The new signal to add ("strengthening", "weakening", or "neutral")
        theses_dir: The directory containing thesis state files

    Returns:
        The updated state dictionary
    """
    # Load current state
    state = load_thesis_state(watchlist_id, theses_dir)
    now = datetime.now(timezone.utc)

    # Add new signal to history
    state["signals_history"].append(
        {
            "signal": new_signal,
            "timestamp": now.isoformat(),
        }
    )
    state["last_updated"] = now.isoformat()

    # Determine new thesis state
    # If new_signal is neutral, keep current state
    # Otherwise, use the new signal as the state
    if new_signal != "neutral":
        new_thesis = new_signal
    else:
        new_thesis = state["current_state"]

    # Check if state changed (only count transitions between non-neutral states)
    old_state = state["current_state"]
    if old_state != "neutral" and new_thesis != "neutral" and old_state != new_thesis:
        state["state_changes"] = state.get("state_changes", 0) + 1

    state["current_state"] = new_thesis

    # Save updated state
    save_thesis_state(state, theses_dir)

    return state


def _compute_thesis_from_signals(signals_history: list[dict]) -> str:
    """Compute thesis state from signals history.

    Args:
        signals_history: List of signal dicts with "signal" and "timestamp"

    Returns:
        Thesis state string
    """
    if not signals_history:
        return "neutral"

    # Use last 7 days of signals
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    cutoff_naive = cutoff.replace(tzinfo=None)

    recent_signals = []
    for sig in signals_history:
        ts_str = sig.get("timestamp", "")
        if ts_str:
            try:
                ts_str = ts_str.replace("Z", "+00:00")
                ts = datetime.fromisoformat(ts_str)
                if ts.replace(tzinfo=None) >= cutoff_naive:
                    recent_signals.append(sig)
            except ValueError:
                continue

    if not recent_signals:
        return "neutral"

    strengthening_count = sum(
        1 for s in recent_signals if s.get("signal") == "strengthening"
    )
    weakening_count = sum(1 for s in recent_signals if s.get("signal") == "weakening")
    total_signals = len(recent_signals)

    if (
        strengthening_count > 0
        and strengthening_count / total_signals > STRENGTHENING_THRESHOLD
    ):
        return "strengthening"

    if weakening_count > 0 and weakening_count / total_signals > WEAKENING_THRESHOLD:
        return "weakening"

    return "neutral"


def get_thesis_history(
    watchlist_id: str, theses_dir: str = DEFAULT_THESES_DIR
) -> list[dict]:
    """Get the full signal history for a watchlist.

    Returns list of {signal, timestamp} dicts sorted by timestamp descending.

    Args:
        watchlist_id: The ID of the watchlist
        theses_dir: The directory containing thesis state files

    Returns:
        List of signal history dictionaries sorted newest first
    """
    state = load_thesis_state(watchlist_id, theses_dir)

    history = state.get("signals_history", [])

    # Sort by timestamp descending (newest first)
    history.sort(key=lambda h: h.get("timestamp", ""), reverse=True)

    return history


def get_thesis_metrics(
    watchlist_id: str, hits_dir: str, theses_dir: str = DEFAULT_THESES_DIR
) -> dict:
    """Compute comprehensive metrics for a watchlist's thesis tracking.

    Returns dict with:
    - current_state: str
    - total_signals: int
    - strengthening_count: int
    - weakening_count: int
    - neutral_count: int
    - signals_per_day: dict
    - thesis_state_changes: int (number of times state changed)
    - latest_signal: str or None

    Args:
        watchlist_id: The ID of the watchlist
        hits_dir: The directory containing hit files
        theses_dir: The directory containing thesis state files

    Returns:
        Dictionary with comprehensive thesis metrics
    """
    state = load_thesis_state(watchlist_id, theses_dir)
    history = state.get("signals_history", [])

    metrics = {
        "current_state": state.get("current_state", "neutral"),
        "total_signals": len(history),
        "strengthening_count": 0,
        "weakening_count": 0,
        "neutral_count": 0,
        "signals_per_day": {},
        "thesis_state_changes": state.get("state_changes", 0),
        "latest_signal": None,
    }

    for sig in history:
        signal_type = sig.get("signal", "neutral")
        if signal_type == "strengthening":
            metrics["strengthening_count"] += 1
        elif signal_type == "weakening":
            metrics["weakening_count"] += 1
        else:
            metrics["neutral_count"] += 1

        # Compute signals per day
        ts = sig.get("timestamp", "")
        if ts:
            date_str = ts.split("T")[0]  # Get YYYY-MM-DD part
            metrics["signals_per_day"][date_str] = (
                metrics["signals_per_day"].get(date_str, 0) + 1
            )

    # Get latest signal
    if history:
        # History is already sorted newest first in load_thesis_state
        sorted_history = sorted(
            history, key=lambda h: h.get("timestamp", ""), reverse=True
        )
        metrics["latest_signal"] = sorted_history[0].get("signal")

    return metrics
