"""Tests for watchlist_hit_persistence module."""

import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.watchlist_hit_persistence import (
    ensure_dirs_exist,
    generate_hit_id,
    get_watchlist_metrics,
    list_hits_by_date_range,
    list_hits_by_record,
    list_hits_by_watchlist,
    load_watchlist_hit,
    save_watchlist_hit,
)


class TestGenerateHitId:
    """Tests for generate_hit_id function."""

    def test_generates_consistent_id(self):
        """generates consistent ID from watchlist_id + record_id + timestamp"""
        hit_id = generate_hit_id(
            "wl_repo_stress", "rec_20260402_001", "20260402_123456"
        )
        assert hit_id == "wl_repo_stress_rec_20260402_001_20260402_123456"

    def test_uses_provided_timestamp(self):
        """uses provided timestamp in the ID"""
        hit_id = generate_hit_id("wl_macro", "rec_abc", "20260315_090000")
        assert hit_id == "wl_macro_rec_abc_20260315_090000"

    def test_generates_timestamp_if_not_provided(self):
        """generates timestamp if not provided (uses current UTC time)"""
        hit_id = generate_hit_id("wl_test", "rec_xyz")
        # The format is {watchlist_id}_{record_id}_{YYYYMMDD}_{HHMMSS}
        # With IDs like "wl_test" and "rec_xyz", we get:
        # "wl_test_rec_xyz_20260403_033702" (6 parts when split by _)
        parts = hit_id.split("_")
        # Last two parts should be YYYYMMDD and HHMMSS
        assert len(parts) >= 4
        timestamp_date = parts[-2]
        timestamp_time = parts[-1]
        assert len(timestamp_date) == 8
        assert timestamp_date.isdigit()
        assert len(timestamp_time) == 6
        assert timestamp_time.isdigit()


class TestSaveWatchlistHit:
    """Tests for save_watchlist_hit function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_saves_file_correctly(self):
        """saves hit file correctly with proper naming"""
        hit = {
            "watchlist_id": "wl_repo_stress",
            "record_id": "rec_20260402_001",
            "match_score": 0.85,
            "matched_terms": ["repo", "stress", "liquidity"],
            "thesis_signal": "strengthening",
            "created_at": "2026-04-02T12:34:56Z",
        }
        hit_id = "wl_repo_stress_rec_20260402_001_20260402_123456"
        path = save_watchlist_hit(hit, self.test_dir)

        assert path.name == f"{hit_id}.json"
        assert os.path.exists(path)

        with open(path, "r") as f:
            loaded = json.load(f)
        assert loaded == hit

    def test_creates_directory_if_missing(self):
        """creates directory if it doesn't exist"""
        subdir = os.path.join(self.test_dir, "subdir", "nested")
        hit = {
            "watchlist_id": "wl_test",
            "record_id": "rec_001",
            "match_score": 0.5,
            "matched_terms": ["test"],
            "thesis_signal": "neutral",
            "created_at": "2026-04-02T12:00:00Z",
        }
        hit_id = "wl_test_rec_001_20260402_120000"
        path = save_watchlist_hit(hit, subdir)

        assert os.path.exists(subdir)
        assert os.path.exists(path)

    def test_atomic_write(self):
        """uses temp file + rename for atomicity"""
        hit = {
            "watchlist_id": "wl_atomic",
            "record_id": "rec_test",
            "match_score": 0.9,
            "matched_terms": ["atomic"],
            "thesis_signal": "neutral",
            "created_at": "2026-04-02T12:00:00Z",
        }
        hit_id = "wl_atomic_rec_test_20260402_120000"
        path = save_watchlist_hit(hit, self.test_dir)

        # Check that the final file exists (not a temp file)
        assert path.exists()
        assert not any(f.endswith(".tmp") for f in os.listdir(self.test_dir))

    def test_valid_json(self):
        """produces valid JSON file"""
        hit = {
            "watchlist_id": "wl_json",
            "record_id": "rec_json",
            "match_score": 0.75,
            "matched_terms": ["json", "test"],
            "thesis_signal": "weakening",
            "created_at": "2026-04-02T12:00:00Z",
        }
        hit_id = "wl_json_rec_json_20260402_120000"
        path = save_watchlist_hit(hit, self.test_dir)

        with open(path, "r") as f:
            loaded = json.load(f)
        # Verify it's valid JSON and contains expected data
        assert loaded["watchlist_id"] == "wl_json"
        assert loaded["record_id"] == "rec_json"


class TestLoadWatchlistHit:
    """Tests for load_watchlist_hit function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_loads_correct_hit(self):
        """loads correct hit by ID from hits_dir"""
        hit = {
            "watchlist_id": "wl_load_test",
            "record_id": "rec_load",
            "match_score": 0.88,
            "matched_terms": ["load", "test"],
            "thesis_signal": "strengthening",
            "created_at": "2026-04-02T14:30:00Z",
        }
        hit_id = "wl_load_test_rec_load_20260402_143000"
        save_watchlist_hit(hit, self.test_dir)

        loaded = load_watchlist_hit(hit_id, self.test_dir)
        assert loaded == hit

    def test_raises_file_not_found_for_missing(self):
        """raises FileNotFoundError for missing hit"""
        with pytest.raises(FileNotFoundError):
            load_watchlist_hit("nonexistent_hit_123", self.test_dir)


class TestListHitsByWatchlist:
    """Tests for list_hits_by_watchlist function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_filters_correctly(self):
        """filters hits by watchlist_id correctly"""
        # Create hits for different watchlists
        hit1 = {
            "watchlist_id": "wl_alpha",
            "record_id": "rec_001",
            "match_score": 0.9,
            "matched_terms": ["alpha"],
            "thesis_signal": "strengthening",
            "created_at": "2026-04-02T10:00:00Z",
        }
        hit2 = {
            "watchlist_id": "wl_beta",
            "record_id": "rec_002",
            "match_score": 0.8,
            "matched_terms": ["beta"],
            "thesis_signal": "weakening",
            "created_at": "2026-04-02T11:00:00Z",
        }
        hit3 = {
            "watchlist_id": "wl_alpha",
            "record_id": "rec_003",
            "match_score": 0.85,
            "matched_terms": ["alpha"],
            "thesis_signal": "neutral",
            "created_at": "2026-04-02T12:00:00Z",
        }

        save_watchlist_hit(hit1, self.test_dir)
        save_watchlist_hit(hit2, self.test_dir)
        save_watchlist_hit(hit3, self.test_dir)

        alpha_hits = list_hits_by_watchlist("wl_alpha", self.test_dir)
        assert len(alpha_hits) == 2
        assert all(h["watchlist_id"] == "wl_alpha" for h in alpha_hits)

    def test_returns_empty_for_no_hits(self):
        """returns empty list for watchlist with no hits"""
        hits = list_hits_by_watchlist("wl_nonexistent", self.test_dir)
        assert hits == []

    def test_sorted_by_date_descending(self):
        """returns hits sorted by created_at descending"""
        hits_data = [
            ("2026-04-02T08:00:00Z", "rec_morning"),
            ("2026-04-02T14:00:00Z", "rec_afternoon"),
            ("2026-04-02T10:00:00Z", "rec_late_morning"),
        ]
        for created_at, record_id in hits_data:
            hit = {
                "watchlist_id": "wl_sorted",
                "record_id": record_id,
                "match_score": 0.8,
                "matched_terms": ["sorted"],
                "thesis_signal": "neutral",
                "created_at": created_at,
            }
            save_watchlist_hit(hit, self.test_dir)

        hits = list_hits_by_watchlist("wl_sorted", self.test_dir)
        created_times = [h["created_at"] for h in hits]
        assert created_times == sorted(created_times, reverse=True)


class TestListHitsByRecord:
    """Tests for list_hits_by_record function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_filters_correctly(self):
        """filters hits by record_id correctly"""
        hit1 = {
            "watchlist_id": "wl_1",
            "record_id": "rec_shared",
            "match_score": 0.9,
            "matched_terms": ["shared"],
            "thesis_signal": "strengthening",
            "created_at": "2026-04-02T10:00:00Z",
        }
        hit2 = {
            "watchlist_id": "wl_2",
            "record_id": "rec_shared",
            "match_score": 0.85,
            "matched_terms": ["shared"],
            "thesis_signal": "weakening",
            "created_at": "2026-04-02T11:00:00Z",
        }
        hit3 = {
            "watchlist_id": "wl_1",
            "record_id": "rec_other",
            "match_score": 0.8,
            "matched_terms": ["other"],
            "thesis_signal": "neutral",
            "created_at": "2026-04-02T12:00:00Z",
        }

        save_watchlist_hit(hit1, self.test_dir)
        save_watchlist_hit(hit2, self.test_dir)
        save_watchlist_hit(hit3, self.test_dir)

        shared_hits = list_hits_by_record("rec_shared", self.test_dir)
        assert len(shared_hits) == 2
        assert all(h["record_id"] == "rec_shared" for h in shared_hits)

    def test_returns_empty_for_no_hits(self):
        """returns empty list for record with no hits"""
        hits = list_hits_by_record("rec_nonexistent", self.test_dir)
        assert hits == []


class TestListHitsByDateRange:
    """Tests for list_hits_by_date_range function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_filters_by_date_range(self):
        """filters hits within date range correctly"""
        hits_data = [
            ("2026-04-01T10:00:00Z", "rec_01"),
            ("2026-04-02T10:00:00Z", "rec_02"),
            ("2026-04-03T10:00:00Z", "rec_03"),
            ("2026-04-04T10:00:00Z", "rec_04"),
        ]
        for created_at, record_id in hits_data:
            hit = {
                "watchlist_id": "wl_range",
                "record_id": record_id,
                "match_score": 0.8,
                "matched_terms": ["range"],
                "thesis_signal": "neutral",
                "created_at": created_at,
            }
            save_watchlist_hit(hit, self.test_dir)

        # Get hits from April 2 to April 3 (inclusive)
        hits = list_hits_by_date_range("2026-04-02", "2026-04-03", self.test_dir)
        assert len(hits) == 2

    def test_handles_edge_cases(self):
        """handles edge cases like same start/end date"""
        hit = {
            "watchlist_id": "wl_edge",
            "record_id": "rec_edge",
            "match_score": 0.8,
            "matched_terms": ["edge"],
            "thesis_signal": "neutral",
            "created_at": "2026-04-02T12:00:00Z",
        }
        save_watchlist_hit(hit, self.test_dir)

        # Same start and end - should include hits on that day
        hits = list_hits_by_date_range("2026-04-02", "2026-04-02", self.test_dir)
        assert len(hits) == 1
        assert hits[0]["record_id"] == "rec_edge"


class TestGetWatchlistMetrics:
    """Tests for get_watchlist_metrics function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()
        self.records_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        if os.path.exists(self.records_dir):
            shutil.rmtree(self.records_dir)

    def test_returns_correct_counts(self):
        """returns correct hit counts by status"""
        hits_data = [
            ("accepted", "rec_01"),
            ("review_queue", "rec_02"),
            ("rejected", "rec_03"),
            ("accepted", "rec_04"),
            ("review_queue", "rec_05"),
        ]
        for status, record_id in hits_data:
            # Create a record with status
            record = {
                "record_id": record_id,
                "status": status,
                "source_url": "https://example.com/article",
                "source_domain": "example.com",
            }
            record_path = Path(self.records_dir) / f"{record_id}.json"
            with open(record_path, "w") as f:
                json.dump(record, f)

            # Create a hit referencing this record
            hit = {
                "watchlist_id": "wl_metrics",
                "record_id": record_id,
                "match_score": 0.8,
                "matched_terms": ["metrics"],
                "thesis_signal": "neutral",
                "created_at": "2026-04-02T12:00:00Z",
            }
            save_watchlist_hit(hit, self.test_dir)

        metrics = get_watchlist_metrics("wl_metrics", self.test_dir, self.records_dir)
        assert metrics["total_hits"] == 5
        assert metrics["accepted_hits"] == 2
        assert metrics["review_hits"] == 2
        assert metrics["rejected_hits"] == 1

    def test_handles_no_hits(self):
        """handles watchlist with no hits"""
        metrics = get_watchlist_metrics("wl_empty", self.test_dir, self.records_dir)
        assert metrics["total_hits"] == 0
        assert metrics["accepted_hits"] == 0
        assert metrics["review_hits"] == 0
        assert metrics["rejected_hits"] == 0

    def test_hits_per_day(self):
        """computes hits_per_day correctly"""
        hits_data = [
            ("2026-04-01T10:00:00Z", "rec_01"),
            ("2026-04-01T14:00:00Z", "rec_02"),
            ("2026-04-02T10:00:00Z", "rec_03"),
        ]
        for created_at, record_id in hits_data:
            hit = {
                "watchlist_id": "wl_daily",
                "record_id": record_id,
                "match_score": 0.8,
                "matched_terms": ["daily"],
                "thesis_signal": "neutral",
                "created_at": created_at,
            }
            save_watchlist_hit(hit, self.test_dir)

            # Also save a minimal record
            record = {
                "record_id": record_id,
                "status": "accepted",
                "source_url": "https://example.com",
                "source_domain": "example.com",
            }
            record_path = Path(self.records_dir) / f"{record_id}.json"
            with open(record_path, "w") as f:
                json.dump(record, f)

        metrics = get_watchlist_metrics("wl_daily", self.test_dir, self.records_dir)
        assert "2026-04-01" in metrics["hits_per_day"]
        assert "2026-04-02" in metrics["hits_per_day"]
        assert metrics["hits_per_day"]["2026-04-01"] == 2
        assert metrics["hits_per_day"]["2026-04-02"] == 1


class TestEnsureDirsExist:
    """Tests for ensure_dirs_exist function."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_creates_missing_dirs(self):
        """creates missing directories"""
        new_dir = os.path.join(self.test_dir, "new", "nested", "dir")
        ensure_dirs_exist(new_dir)
        assert os.path.exists(new_dir)
        assert os.path.isdir(new_dir)

    def test_skips_existing_dirs(self):
        """does not fail for existing directories"""
        existing = os.path.join(self.test_dir, "existing")
        os.makedirs(existing)
        # Should not raise
        ensure_dirs_exist(existing)
        assert os.path.exists(existing)

    def test_creates_multiple_dirs(self):
        """creates multiple directories at once"""
        dir1 = os.path.join(self.test_dir, "dir1")
        dir2 = os.path.join(self.test_dir, "dir2", "nested")
        dir3 = os.path.join(self.test_dir, "dir3")
        ensure_dirs_exist(dir1, dir2, dir3)
        assert os.path.exists(dir1)
        assert os.path.exists(dir2)
        assert os.path.exists(dir3)
