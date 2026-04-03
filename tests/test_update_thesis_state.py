"""Tests for update_thesis_state module."""

import json
import os
import shutil
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from scripts.update_thesis_state import (
    DEFAULT_THESES_DIR,
    STRENGTHENING_THRESHOLD,
    WEAKENING_THRESHOLD,
    LOOKBACK_DAYS,
    compute_thesis_from_hits,
    get_thesis_history,
    get_thesis_metrics,
    load_thesis_state,
    save_thesis_state,
    update_thesis_state,
)


class TestLoadThesisState:
    """Tests for load_thesis_state function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_loads_existing_state(self):
        """loads existing thesis state for a watchlist"""
        # Create a thesis state file
        state_data = {
            "watchlist_id": "wl_test",
            "current_state": "strengthening",
            "signals_history": [
                {"signal": "strengthening", "timestamp": "2026-04-02T10:30:00Z"},
                {"signal": "neutral", "timestamp": "2026-04-01T08:00:00Z"},
            ],
            "last_updated": "2026-04-02T10:30:00Z",
            "state_changes": 1,
        }
        state_path = Path(self.test_dir) / "wl_test_thesis.json"
        with open(state_path, "w") as f:
            json.dump(state_data, f)

        state = load_thesis_state("wl_test", self.test_dir)
        assert state["watchlist_id"] == "wl_test"
        assert state["current_state"] == "strengthening"
        assert len(state["signals_history"]) == 2
        assert state["state_changes"] == 1

    def test_returns_default_for_missing(self):
        """returns default neutral state when no state file exists"""
        state = load_thesis_state("wl_nonexistent", self.test_dir)
        assert state["watchlist_id"] == "wl_nonexistent"
        assert state["current_state"] == "neutral"
        assert state["signals_history"] == []
        assert state["state_changes"] == 0
        assert state["last_updated"] is not None


class TestSaveThesisState:
    """Tests for save_thesis_state function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_saves_state_correctly(self):
        """saves thesis state to correct file path"""
        state = {
            "watchlist_id": "wl_save",
            "current_state": "weakening",
            "signals_history": [
                {"signal": "weakening", "timestamp": "2026-04-02T12:00:00Z"},
            ],
            "last_updated": "2026-04-02T12:00:00Z",
            "state_changes": 1,
        }
        path = save_thesis_state(state, self.test_dir)

        assert path.name == "wl_save_thesis.json"
        assert os.path.exists(path)

        with open(path, "r") as f:
            loaded = json.load(f)
        assert loaded["watchlist_id"] == "wl_save"
        assert loaded["current_state"] == "weakening"

    def test_atomic_write(self):
        """uses temp file + rename for atomicity"""
        state = {
            "watchlist_id": "wl_atomic",
            "current_state": "neutral",
            "signals_history": [],
            "last_updated": "2026-04-02T12:00:00Z",
            "state_changes": 0,
        }
        path = save_thesis_state(state, self.test_dir)

        # Check that no temp files remain
        assert path.exists()
        assert not any(f.endswith(".tmp") for f in os.listdir(self.test_dir))

    def test_overwrites_existing_state(self):
        """overwrites existing state file"""
        state1 = {
            "watchlist_id": "wl_overwrite",
            "current_state": "neutral",
            "signals_history": [],
            "last_updated": "2026-04-01T10:00:00Z",
            "state_changes": 0,
        }
        save_thesis_state(state1, self.test_dir)

        state2 = {
            "watchlist_id": "wl_overwrite",
            "current_state": "strengthening",
            "signals_history": [
                {"signal": "strengthening", "timestamp": "2026-04-02T10:00:00Z"}
            ],
            "last_updated": "2026-04-02T10:00:00Z",
            "state_changes": 1,
        }
        save_thesis_state(state2, self.test_dir)

        with open(Path(self.test_dir) / "wl_overwrite_thesis.json", "r") as f:
            loaded = json.load(f)
        assert loaded["current_state"] == "strengthening"
        assert loaded["state_changes"] == 1


class TestComputeThesisFromHits:
    """Tests for compute_thesis_from_hits function."""

    def test_strengthening_when_gt_60_percent_strengthening(self):
        """returns 'strengthening' when >60% of signals are strengthening"""
        hits = [
            {"thesis_signal": "strengthening", "created_at": "2026-04-02T10:00:00Z"},
            {"thesis_signal": "strengthening", "created_at": "2026-04-02T11:00:00Z"},
            {"thesis_signal": "strengthening", "created_at": "2026-04-02T12:00:00Z"},
            {"thesis_signal": "neutral", "created_at": "2026-04-02T13:00:00Z"},
            {"thesis_signal": "neutral", "created_at": "2026-04-02T14:00:00Z"},
        ]
        # 3/5 = 60% exactly - not > 60%, should be neutral
        result = compute_thesis_from_hits("wl_test", hits)
        assert result == "neutral"

        # 4/5 = 80% > 60%, should be strengthening
        hits.append(
            {"thesis_signal": "strengthening", "created_at": "2026-04-02T15:00:00Z"}
        )
        result = compute_thesis_from_hits("wl_test", hits)
        assert result == "strengthening"

    def test_weakening_when_gt_60_percent_weakening(self):
        """returns 'weakening' when >60% of signals are weakening"""
        hits = [
            {"thesis_signal": "weakening", "created_at": "2026-04-02T10:00:00Z"},
            {"thesis_signal": "weakening", "created_at": "2026-04-02T11:00:00Z"},
            {"thesis_signal": "weakening", "created_at": "2026-04-02T12:00:00Z"},
            {"thesis_signal": "neutral", "created_at": "2026-04-02T13:00:00Z"},
            {"thesis_signal": "neutral", "created_at": "2026-04-02T14:00:00Z"},
        ]
        # 3/5 = 60% exactly - not > 60%, should be neutral
        result = compute_thesis_from_hits("wl_test", hits)
        assert result == "neutral"

        # 4/5 = 80% > 60%, should be weakening
        hits.append(
            {"thesis_signal": "weakening", "created_at": "2026-04-02T15:00:00Z"}
        )
        result = compute_thesis_from_hits("wl_test", hits)
        assert result == "weakening"

    def test_neutral_when_no_clear_majority(self):
        """returns 'neutral' when there's no clear majority"""
        hits = [
            {"thesis_signal": "strengthening", "created_at": "2026-04-02T10:00:00Z"},
            {"thesis_signal": "weakening", "created_at": "2026-04-02T11:00:00Z"},
            {"thesis_signal": "neutral", "created_at": "2026-04-02T12:00:00Z"},
        ]
        result = compute_thesis_from_hits("wl_test", hits)
        assert result == "neutral"

    def test_empty_hits_returns_neutral(self):
        """returns 'neutral' for empty hits list"""
        result = compute_thesis_from_hits("wl_test", [])
        assert result == "neutral"

    def test_respects_lookback_days(self):
        """only considers hits within lookback_days window"""
        # Create hits with varying ages
        today = datetime.now(timezone.utc)
        recent_date = (today - timedelta(days=3)).strftime("%Y-%m-%dT10:00:00Z")
        old_date = (today - timedelta(days=10)).strftime("%Y-%m-%dT10:00:00Z")

        hits = [
            # Recent strengthening hits (within 7 days)
            {"thesis_signal": "strengthening", "created_at": recent_date},
            {"thesis_signal": "strengthening", "created_at": recent_date},
            # Old weakening hits (outside 7 days)
            {"thesis_signal": "weakening", "created_at": old_date},
            {"thesis_signal": "weakening", "created_at": old_date},
            {"thesis_signal": "weakening", "created_at": old_date},
        ]
        # Only recent hits should count (2 strengthening)
        # 2/2 = 100% > 60%, so strengthening
        result = compute_thesis_from_hits("wl_test", hits, lookback_days=7)
        assert result == "strengthening"

        # With shorter lookback, still strengthening
        result = compute_thesis_from_hits("wl_test", hits, lookback_days=5)
        assert result == "strengthening"


class TestUpdateThesisState:
    """Tests for update_thesis_state function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_creates_initial_state(self):
        """creates initial state when no state exists"""
        state = update_thesis_state("wl_new", "strengthening", self.test_dir)

        assert state["watchlist_id"] == "wl_new"
        assert state["current_state"] == "strengthening"
        assert len(state["signals_history"]) == 1
        assert state["signals_history"][0]["signal"] == "strengthening"
        assert state["state_changes"] == 0

    def test_updates_existing_state(self):
        """updates existing state with new signal"""
        # First create initial state
        initial_state = {
            "watchlist_id": "wl_existing",
            "current_state": "neutral",
            "signals_history": [],
            "last_updated": "2026-04-01T10:00:00Z",
            "state_changes": 0,
        }
        save_thesis_state(initial_state, self.test_dir)

        # Update with a new signal
        state = update_thesis_state("wl_existing", "strengthening", self.test_dir)

        assert state["watchlist_id"] == "wl_existing"
        assert state["current_state"] == "strengthening"
        assert len(state["signals_history"]) == 1
        assert (
            state["state_changes"] == 0
        )  # neutral -> strengthening is first state, not a change

    def test_increments_state_changes_on_transition(self):
        """increments state_changes when state actually transitions"""
        # Start with strengthening state
        initial_state = {
            "watchlist_id": "wl_transition",
            "current_state": "strengthening",
            "signals_history": [
                {"signal": "strengthening", "timestamp": "2026-04-01T10:00:00Z"},
            ],
            "last_updated": "2026-04-01T10:00:00Z",
            "state_changes": 0,
        }
        save_thesis_state(initial_state, self.test_dir)

        # Transition to weakening should increment state_changes
        state = update_thesis_state("wl_transition", "weakening", self.test_dir)
        assert state["current_state"] == "weakening"
        assert state["state_changes"] == 1

        # Transition back to strengthening should increment again
        state = update_thesis_state("wl_transition", "strengthening", self.test_dir)
        assert state["current_state"] == "strengthening"
        assert state["state_changes"] == 2

    def test_no_state_change_when_same_signal(self):
        """does not increment state_changes when state doesn't change"""
        initial_state = {
            "watchlist_id": "wl_same",
            "current_state": "strengthening",
            "signals_history": [
                {"signal": "strengthening", "timestamp": "2026-04-01T10:00:00Z"},
            ],
            "last_updated": "2026-04-01T10:00:00Z",
            "state_changes": 0,
        }
        save_thesis_state(initial_state, self.test_dir)

        # Same state should not increment state_changes
        state = update_thesis_state("wl_same", "strengthening", self.test_dir)
        assert state["current_state"] == "strengthening"
        assert state["state_changes"] == 0  # No change!


class TestGetThesisHistory:
    """Tests for get_thesis_history function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_returns_sorted_history_descending(self):
        """returns history sorted by timestamp descending"""
        state = {
            "watchlist_id": "wl_history",
            "current_state": "weakening",
            "signals_history": [
                {"signal": "neutral", "timestamp": "2026-04-01T08:00:00Z"},
                {"signal": "strengthening", "timestamp": "2026-04-02T10:00:00Z"},
                {"signal": "weakening", "timestamp": "2026-04-03T14:00:00Z"},
            ],
            "last_updated": "2026-04-03T14:00:00Z",
            "state_changes": 2,
        }
        save_thesis_state(state, self.test_dir)

        history = get_thesis_history("wl_history", self.test_dir)

        assert len(history) == 3
        # Should be sorted descending (newest first)
        assert history[0]["signal"] == "weakening"
        assert history[1]["signal"] == "strengthening"
        assert history[2]["signal"] == "neutral"

    def test_returns_empty_for_no_history(self):
        """returns empty list when no thesis history exists"""
        # Create directory with no state file
        history = get_thesis_history("wl_empty", self.test_dir)
        assert history == []


class TestGetThesisMetrics:
    """Tests for get_thesis_metrics function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.hits_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        if os.path.exists(self.hits_dir):
            shutil.rmtree(self.hits_dir)

    def test_returns_all_metrics_correctly(self):
        """returns comprehensive thesis metrics"""
        # Create thesis state
        state = {
            "watchlist_id": "wl_metrics",
            "current_state": "strengthening",
            "signals_history": [
                {"signal": "strengthening", "timestamp": "2026-04-02T10:00:00Z"},
                {"signal": "neutral", "timestamp": "2026-04-02T09:00:00Z"},
                {"signal": "weakening", "timestamp": "2026-04-01T14:00:00Z"},
                {"signal": "strengthening", "timestamp": "2026-04-01T10:00:00Z"},
            ],
            "last_updated": "2026-04-02T10:00:00Z",
            "state_changes": 2,
        }
        save_thesis_state(state, self.test_dir)

        metrics = get_thesis_metrics("wl_metrics", self.hits_dir, self.test_dir)

        assert metrics["current_state"] == "strengthening"
        assert metrics["total_signals"] == 4
        assert metrics["strengthening_count"] == 2
        assert metrics["weakening_count"] == 1
        assert metrics["neutral_count"] == 1
        assert metrics["thesis_state_changes"] == 2
        assert metrics["latest_signal"] == "strengthening"

    def test_handles_missing_thesis_file(self):
        """handles watchlist with no thesis state file"""
        metrics = get_thesis_metrics("wl_missing", self.hits_dir, self.test_dir)

        assert metrics["current_state"] == "neutral"
        assert metrics["total_signals"] == 0
        assert metrics["strengthening_count"] == 0
        assert metrics["weakening_count"] == 0
        assert metrics["neutral_count"] == 0
        assert metrics["thesis_state_changes"] == 0
        assert metrics["latest_signal"] is None
