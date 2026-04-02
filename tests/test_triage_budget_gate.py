"""
Tests for Stream 3: Budget Gate and Metrics (triage_budget_gate.py, triage_metrics.py)

Following TDD (Red-Green-Refactor), these tests will initially FAIL
because the implementation files do not exist yet. Once the implementations
are created, these tests should pass.

Tests verify:
1. Budget enforcement per lane
2. Defer medium priority behavior
3. Metrics tracking per lane
4. Triage report generation
"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Import the modules under test - this will fail until the files are created
from scripts.triage_budget_gate import apply_budget_gate
from scripts.triage_metrics import (
    record_triage_metric,
    get_lane_metrics,
    generate_triage_report,
    reset_metrics,
)


# =============================================================================
# Test Fixtures / Helper Data
# =============================================================================


def create_mock_candidate(
    candidate_id="test_001",
    lane="article",
    priority_score=85,
    priority_band="high",
    action="process_now",
):
    """Factory to create mock candidate data with triage result."""
    return {
        "candidate_id": candidate_id,
        "lane": lane,
        "triage_result": {
            "priority_score": priority_score,
            "priority_band": priority_band,
            "action": action,
        },
    }


# =============================================================================
# TestApplyBudgetGate
# =============================================================================


class TestApplyBudgetGate(unittest.TestCase):
    """Tests for apply_budget_gate function."""

    def setUp(self):
        """Set up budget configuration and test data directories."""
        self.temp_dir = tempfile.mkdtemp()
        self.triage_dir = Path(self.temp_dir) / "triage"
        self.triage_dir.mkdir(parents=True, exist_ok=True)

        self.budget_config = {
            "article_process_limit": 25,
            "quant_process_limit": 10,
            "defer_medium": True,
        }

        # Mock the TRIAGE_DIR path for metrics
        self.metrics_path = self.triage_dir / "metrics.json"

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_apply_budget_gate_returns_three_lists(self):
        """apply_budget_gate returns tuple of (process_list, defer_list, discard_list)."""
        process_list = [create_mock_candidate(candidate_id="c1")]
        defer_list = []
        discard_list = []

        result = apply_budget_gate(
            process_list, defer_list, discard_list, self.budget_config, "article"
        )

        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 3)

    def test_article_lane_respects_article_process_limit(self):
        """Article lane should limit processed candidates to article_process_limit."""
        # Create 30 candidates (over the 25 limit)
        process_list = [
            create_mock_candidate(candidate_id=f"c{i}", priority_score=100 - i)
            for i in range(30)
        ]
        defer_list = []
        discard_list = []

        with patch("scripts.triage_budget_gate.TRIAGE_DIR", self.triage_dir):
            result_process, result_defer, result_discard = apply_budget_gate(
                process_list, defer_list, discard_list, self.budget_config, "article"
            )

        # Should have exactly 25 in process list
        self.assertEqual(len(result_process), 25)
        # 5 should be moved to defer
        self.assertEqual(len(result_defer), 5)

    def test_quant_lane_respects_quant_process_limit(self):
        """Quant lane should limit processed candidates to quant_process_limit."""
        # Create 15 candidates (over the 10 limit)
        process_list = [
            create_mock_candidate(
                candidate_id=f"q{i}", lane="quant", priority_score=100 - i
            )
            for i in range(15)
        ]
        defer_list = []
        discard_list = []

        with patch("scripts.triage_budget_gate.TRIAGE_DIR", self.triage_dir):
            result_process, result_defer, result_discard = apply_budget_gate(
                process_list, defer_list, discard_list, self.budget_config, "quant"
            )

        # Should have exactly 10 in process list
        self.assertEqual(len(result_process), 10)
        # 5 should be moved to defer
        self.assertEqual(len(result_defer), 5)

    def test_under_budget_keeps_all_in_process_list(self):
        """When candidates are under budget, all stay in process_list."""
        # Create 10 candidates (under the 25 limit for article)
        process_list = [
            create_mock_candidate(candidate_id=f"c{i}", priority_score=100 - i)
            for i in range(10)
        ]
        defer_list = []
        discard_list = []

        with patch("scripts.triage_budget_gate.TRIAGE_DIR", self.triage_dir):
            result_process, result_defer, result_discard = apply_budget_gate(
                process_list, defer_list, discard_list, self.budget_config, "article"
            )

        self.assertEqual(len(result_process), 10)
        self.assertEqual(len(result_defer), 0)

    def test_excess_candidates_move_to_defer_list(self):
        """When budget exceeded, excess candidates move from process_list to defer_list."""
        # Create exactly 26 candidates (1 over the 25 limit)
        process_list = [
            create_mock_candidate(candidate_id=f"c{i}", priority_score=100 - i)
            for i in range(26)
        ]
        defer_list = []
        discard_list = []

        with patch("scripts.triage_budget_gate.TRIAGE_DIR", self.triage_dir):
            result_process, result_defer, result_discard = apply_budget_gate(
                process_list, defer_list, discard_list, self.budget_config, "article"
            )

        self.assertEqual(len(result_process), 25)
        self.assertEqual(len(result_defer), 1)
        # The deferred candidate should be the one with lowest priority
        self.assertEqual(result_defer[0]["candidate_id"], "c25")

    def test_defer_medium_true_defers_medium_priority(self):
        """When defer_medium=true, medium priority candidates are deferred even if under budget."""
        budget_config = {
            "article_process_limit": 30,
            "quant_process_limit": 10,
            "defer_medium": True,
        }

        # Create 20 high priority and 10 medium priority candidates
        process_list = [
            create_mock_candidate(
                candidate_id=f"high{i}", priority_band="high", priority_score=80 - i
            )
            for i in range(20)
        ] + [
            create_mock_candidate(
                candidate_id=f"med{i}", priority_band="medium", priority_score=60 - i
            )
            for i in range(10)
        ]
        defer_list = []
        discard_list = []

        with patch("scripts.triage_budget_gate.TRIAGE_DIR", self.triage_dir):
            result_process, result_defer, result_discard = apply_budget_gate(
                process_list, defer_list, discard_list, budget_config, "article"
            )

        # All medium priority should be deferred
        medium_candidates = [
            c for c in result_defer if c["triage_result"]["priority_band"] == "medium"
        ]
        self.assertEqual(len(medium_candidates), 10)

    def test_defer_medium_false_processes_medium_priority(self):
        """When defer_medium=false, medium priority candidates are processed if under budget."""
        budget_config = {
            "article_process_limit": 30,
            "quant_process_limit": 10,
            "defer_medium": False,
        }

        # Create 20 high priority and 10 medium priority candidates
        process_list = [
            create_mock_candidate(
                candidate_id=f"high{i}", priority_band="high", priority_score=80 - i
            )
            for i in range(20)
        ] + [
            create_mock_candidate(
                candidate_id=f"med{i}", priority_band="medium", priority_score=60 - i
            )
            for i in range(10)
        ]
        defer_list = []
        discard_list = []

        with patch("scripts.triage_budget_gate.TRIAGE_DIR", self.triage_dir):
            result_process, result_defer, result_discard = apply_budget_gate(
                process_list, defer_list, discard_list, budget_config, "article"
            )

        # Medium priority should NOT be deferred when defer_medium=False
        medium_candidates = [
            c for c in result_defer if c["triage_result"]["priority_band"] == "medium"
        ]
        self.assertEqual(len(medium_candidates), 0)
        # All 30 should be in process list
        self.assertEqual(len(result_process), 30)

    def test_discard_list_unchanged_after_budget_gate(self):
        """Discard list should pass through unchanged."""
        process_list = [create_mock_candidate(candidate_id="c1")]
        defer_list = []
        discard_list = [
            create_mock_candidate(candidate_id="d1", action="discard"),
            create_mock_candidate(candidate_id="d2", action="discard"),
        ]

        with patch("scripts.triage_budget_gate.TRIAGE_DIR", self.triage_dir):
            result_process, result_defer, result_discard = apply_budget_gate(
                process_list, defer_list, discard_list, self.budget_config, "article"
            )

        self.assertEqual(len(result_discard), 2)
        self.assertEqual(result_discard[0]["candidate_id"], "d1")
        self.assertEqual(result_discard[1]["candidate_id"], "d2")

    def test_existing_defer_list_preserved(self):
        """Existing defer candidates should be preserved."""
        process_list = [create_mock_candidate(candidate_id="c1")]
        defer_list = [create_mock_candidate(candidate_id="existing_defer")]
        discard_list = []

        with patch("scripts.triage_budget_gate.TRIAGE_DIR", self.triage_dir):
            result_process, result_defer, result_discard = apply_budget_gate(
                process_list, defer_list, discard_list, self.budget_config, "article"
            )

        # Existing defer should still be there
        defer_ids = [c["candidate_id"] for c in result_defer]
        self.assertIn("existing_defer", defer_ids)

    def test_empty_process_list(self):
        """Empty process list should return empty lists."""
        process_list = []
        defer_list = []
        discard_list = []

        with patch("scripts.triage_budget_gate.TRIAGE_DIR", self.triage_dir):
            result_process, result_defer, result_discard = apply_budget_gate(
                process_list, defer_list, discard_list, self.budget_config, "article"
            )

        self.assertEqual(result_process, [])
        self.assertEqual(result_defer, [])
        self.assertEqual(result_discard, [])

    def test_process_list_sorted_by_priority_score(self):
        """The process_list should remain sorted by priority_score descending."""
        # Create candidates with explicit scores
        process_list = [
            create_mock_candidate(candidate_id="low", priority_score=30),
            create_mock_candidate(candidate_id="high", priority_score=90),
            create_mock_candidate(candidate_id="medium", priority_score=60),
        ]
        defer_list = []
        discard_list = []

        with patch("scripts.triage_budget_gate.TRIAGE_DIR", self.triage_dir):
            result_process, result_defer, result_discard = apply_budget_gate(
                process_list, defer_list, discard_list, self.budget_config, "article"
            )

        # Should be sorted descending
        scores = [c["triage_result"]["priority_score"] for c in result_process]
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_unknown_lane_uses_default_limits(self):
        """Unknown lane should use article_process_limit as default."""
        # Create 30 candidates for unknown lane
        process_list = [
            create_mock_candidate(
                candidate_id=f"u{i}", lane="unknown", priority_score=100 - i
            )
            for i in range(30)
        ]
        defer_list = []
        discard_list = []

        with patch("scripts.triage_budget_gate.TRIAGE_DIR", self.triage_dir):
            result_process, result_defer, result_discard = apply_budget_gate(
                process_list,
                defer_list,
                discard_list,
                self.budget_config,
                "unknown_lane",
            )

        # Should respect article limit (25)
        self.assertEqual(len(result_process), 25)
        self.assertEqual(len(result_defer), 5)


# =============================================================================
# TestRecordTriageMetric
# =============================================================================


class TestRecordTriageMetric(unittest.TestCase):
    """Tests for record_triage_metric function."""

    def setUp(self):
        """Set up temporary metrics file."""
        self.temp_dir = tempfile.mkdtemp()
        self.triage_dir = Path(self.temp_dir) / "triage"
        self.triage_dir.mkdir(parents=True, exist_ok=True)
        self.metrics_path = self.triage_dir / "metrics.json"

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_record_triage_metric_increments_total_candidates(self):
        """Recording a metric should increment total_candidates for the lane."""
        candidate = create_mock_candidate(candidate_id="test_001", lane="article")

        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            record_triage_metric(candidate, "process_now")
            record_triage_metric(candidate, "process_now")
            metrics = get_lane_metrics("article")

        self.assertEqual(metrics["total_candidates"], 2)

    def test_record_triage_metric_tracks_processed_action(self):
        """Recording process_now action should increment processed count."""
        candidate = create_mock_candidate(candidate_id="test_001", lane="article")

        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            record_triage_metric(candidate, "process_now")
            metrics = get_lane_metrics("article")

        self.assertEqual(metrics["processed"], 1)

    def test_record_triage_metric_tracks_deferred_action(self):
        """Recording defer action should increment deferred count."""
        candidate = create_mock_candidate(candidate_id="test_001", lane="article")

        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            record_triage_metric(candidate, "defer")
            metrics = get_lane_metrics("article")

        self.assertEqual(metrics["deferred"], 1)

    def test_record_triage_metric_tracks_discarded_action(self):
        """Recording discard action should increment discarded count."""
        candidate = create_mock_candidate(candidate_id="test_001", lane="article")

        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            record_triage_metric(candidate, "discard")
            metrics = get_lane_metrics("article")

        self.assertEqual(metrics["discarded"], 1)

    def test_record_triage_metric_tracks_avg_priority_score(self):
        """Recording metrics should track average priority_score."""
        candidate1 = create_mock_candidate(
            candidate_id="c1", lane="article", priority_score=80
        )
        candidate2 = create_mock_candidate(
            candidate_id="c2", lane="article", priority_score=60
        )

        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            record_triage_metric(candidate1, "process_now")
            record_triage_metric(candidate2, "process_now")
            metrics = get_lane_metrics("article")

        self.assertEqual(metrics["avg_priority_score"], 70.0)

    def test_record_triage_metric_separate_lanes(self):
        """Metrics should be tracked separately per lane."""
        article_candidate = create_mock_candidate(
            candidate_id="a1", lane="article", priority_score=80
        )
        quant_candidate = create_mock_candidate(
            candidate_id="q1", lane="quant", priority_score=70
        )

        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            record_triage_metric(article_candidate, "process_now")
            record_triage_metric(quant_candidate, "process_now")
            article_metrics = get_lane_metrics("article")
            quant_metrics = get_lane_metrics("quant")

        self.assertEqual(article_metrics["total_candidates"], 1)
        self.assertEqual(quant_metrics["total_candidates"], 1)


# =============================================================================
# TestGetLaneMetrics
# =============================================================================


class TestGetLaneMetrics(unittest.TestCase):
    """Tests for get_lane_metrics function."""

    def setUp(self):
        """Set up temporary metrics file."""
        self.temp_dir = tempfile.mkdtemp()
        self.triage_dir = Path(self.temp_dir) / "triage"
        self.triage_dir.mkdir(parents=True, exist_ok=True)
        self.metrics_path = self.triage_dir / "metrics.json"

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_get_lane_metrics_returns_dict(self):
        """get_lane_metrics should return a dictionary."""
        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            metrics = get_lane_metrics("article")

        self.assertIsInstance(metrics, dict)

    def test_get_lane_metrics_has_total_candidates(self):
        """Metrics should include total_candidates field."""
        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            metrics = get_lane_metrics("article")

        self.assertIn("total_candidates", metrics)

    def test_get_lane_metrics_has_processed(self):
        """Metrics should include processed field."""
        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            metrics = get_lane_metrics("article")

        self.assertIn("processed", metrics)

    def test_get_lane_metrics_has_deferred(self):
        """Metrics should include deferred field."""
        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            metrics = get_lane_metrics("article")

        self.assertIn("deferred", metrics)

    def test_get_lane_metrics_has_discarded(self):
        """Metrics should include discarded field."""
        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            metrics = get_lane_metrics("article")

        self.assertIn("discarded", metrics)

    def test_get_lane_metrics_has_avg_priority_score(self):
        """Metrics should include avg_priority_score field."""
        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            metrics = get_lane_metrics("article")

        self.assertIn("avg_priority_score", metrics)

    def test_get_lane_metrics_has_accepted_ratio(self):
        """Metrics should include accepted_ratio field."""
        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            metrics = get_lane_metrics("article")

        self.assertIn("accepted_ratio", metrics)

    def test_get_lane_metrics_unknown_lane_returns_defaults(self):
        """Unknown lane should return default metrics structure."""
        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            metrics = get_lane_metrics("nonexistent_lane")

        self.assertEqual(metrics["total_candidates"], 0)
        self.assertEqual(metrics["processed"], 0)
        self.assertEqual(metrics["deferred"], 0)
        self.assertEqual(metrics["discarded"], 0)


# =============================================================================
# TestGenerateTriageReport
# =============================================================================


class TestGenerateTriageReport(unittest.TestCase):
    """Tests for generate_triage_report function."""

    def setUp(self):
        """Set up temporary metrics file."""
        self.temp_dir = tempfile.mkdtemp()
        self.triage_dir = Path(self.temp_dir) / "triage"
        self.triage_dir.mkdir(parents=True, exist_ok=True)
        self.metrics_path = self.triage_dir / "metrics.json"

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_generate_triage_report_returns_dict(self):
        """generate_triage_report should return a dictionary."""
        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            report = generate_triage_report()

        self.assertIsInstance(report, dict)

    def test_generate_triage_report_includes_lanes(self):
        """Report should include lane metrics."""
        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            report = generate_triage_report()

        self.assertIn("lanes", report)

    def test_generate_triage_report_includes_timestamp(self):
        """Report should include timestamp field."""
        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            report = generate_triage_report()

        self.assertIn("timestamp", report)

    def test_generate_triage_report_includes_summary(self):
        """Report should include summary section."""
        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            report = generate_triage_report()

        self.assertIn("summary", report)

    def test_generate_triage_report_aggregates_all_lanes(self):
        """Report should aggregate metrics from all lanes."""
        article_candidate = create_mock_candidate(
            candidate_id="a1", lane="article", priority_score=80
        )
        quant_candidate = create_mock_candidate(
            candidate_id="q1", lane="quant", priority_score=70
        )

        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            record_triage_metric(article_candidate, "process_now")
            record_triage_metric(quant_candidate, "process_now")
            report = generate_triage_report()

        self.assertIn("article", report["lanes"])
        self.assertIn("quant", report["lanes"])


# =============================================================================
# TestResetMetrics
# =============================================================================


class TestResetMetrics(unittest.TestCase):
    """Tests for reset_metrics function."""

    def setUp(self):
        """Set up temporary metrics file."""
        self.temp_dir = tempfile.mkdtemp()
        self.triage_dir = Path(self.temp_dir) / "triage"
        self.triage_dir.mkdir(parents=True, exist_ok=True)
        self.metrics_path = self.triage_dir / "metrics.json"

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_reset_metrics_clears_all_metrics(self):
        """reset_metrics should clear all lane metrics."""
        candidate = create_mock_candidate(candidate_id="test_001", lane="article")

        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            record_triage_metric(candidate, "process_now")
            reset_metrics()
            metrics = get_lane_metrics("article")

        self.assertEqual(metrics["total_candidates"], 0)
        self.assertEqual(metrics["processed"], 0)


# =============================================================================
# TestMetricsIntegration
# =============================================================================


class TestMetricsIntegration(unittest.TestCase):
    """Integration tests for metrics with budget gate."""

    def setUp(self):
        """Set up temporary metrics file."""
        self.temp_dir = tempfile.mkdtemp()
        self.triage_dir = Path(self.temp_dir) / "triage"
        self.triage_dir.mkdir(parents=True, exist_ok=True)

        self.budget_config = {
            "article_process_limit": 25,
            "quant_process_limit": 10,
            "defer_medium": True,
        }

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_budget_gate_integration_records_metrics(self):
        """Budget gate should record metrics for all processed candidates."""
        # Create 30 candidates for article lane
        process_list = [
            create_mock_candidate(
                candidate_id=f"c{i}", lane="article", priority_score=100 - i
            )
            for i in range(30)
        ]
        defer_list = []
        discard_list = []

        with (
            patch("scripts.triage_budget_gate.TRIAGE_DIR", self.triage_dir),
            patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir),
        ):
            # Apply budget gate (this should record metrics internally)
            result_process, result_defer, result_discard = apply_budget_gate(
                process_list, defer_list, discard_list, self.budget_config, "article"
            )

            # Verify metrics were recorded
            metrics = get_lane_metrics("article")
            # Should have metrics for all 30 candidates
            self.assertEqual(metrics["total_candidates"], 30)

    def test_accepted_ratio_calculation(self):
        """accepted_ratio should be processed / total_candidates."""
        candidate = create_mock_candidate(
            candidate_id="test_001", lane="article", priority_score=80
        )

        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            record_triage_metric(candidate, "process_now")
            metrics = get_lane_metrics("article")

        # processed=1, total=1, ratio=1.0
        self.assertEqual(metrics["accepted_ratio"], 1.0)

    def test_partial_accepted_ratio_calculation(self):
        """accepted_ratio should be correct when some are deferred/discarded."""
        c1 = create_mock_candidate(candidate_id="c1", lane="article", priority_score=80)
        c2 = create_mock_candidate(candidate_id="c2", lane="article", priority_score=60)

        with patch("scripts.triage_metrics.TRIAGE_DIR", self.triage_dir):
            record_triage_metric(c1, "process_now")
            record_triage_metric(c2, "defer")
            metrics = get_lane_metrics("article")

        # processed=1, total=2, ratio=0.5
        self.assertEqual(metrics["accepted_ratio"], 0.5)


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    unittest.main()
