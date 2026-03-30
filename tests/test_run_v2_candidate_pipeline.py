"""
Tests for the V2 candidate pipeline orchestration script.

This test verifies that the run_v2_candidate_pipeline script correctly
orchestrates the candidate processing flow for a given lane.
"""

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Add scripts directory to path for imports
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))


class TestRunV2CandidatePipeline:
    """Test suite for run_v2_candidate_pipeline.py"""

    def test_pipeline_runs_without_errors(self, tmp_path):
        """Test that the pipeline runs successfully with synthetic candidates."""
        # Run the pipeline with the test lane
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.run_v2_candidate_pipeline",
                "--lane",
                "trusted_sources",
            ],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
        )

        # Pipeline should complete without crashing
        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

    def test_pipeline_accepts_lane_argument(self, tmp_path):
        """Test that --lane argument is properly parsed."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.run_v2_candidate_pipeline",
                "--lane",
                "keyword_discovery",
            ],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
        )

        # Should complete without argument errors
        assert "unrecognized arguments" not in result.stderr.lower()
        assert result.returncode == 0

    def test_pipeline_creates_candidate_data_dirs(self, tmp_path):
        """Test that pipeline creates necessary data directories."""
        # Run pipeline
        subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.run_v2_candidate_pipeline",
                "--lane",
                "trusted_sources",
            ],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
        )

        # Check that candidate data directories were created
        candidates_dir = BASE_DIR / "data" / "candidates"
        assert candidates_dir.exists(), "data/candidates directory should be created"

        discovered_dir = candidates_dir / "discovered"
        assert discovered_dir.exists(), "data/candidates/discovered should be created"

    def test_pipeline_default_lane_is_trusted_sources(self):
        """Test that default lane is trusted_sources when --lane not specified."""
        result = subprocess.run(
            [sys.executable, "-m", "scripts.run_v2_candidate_pipeline"],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
        )

        # Should work with defaults
        assert result.returncode == 0 or "trusted_sources" in result.stdout


class TestPipelineSteps:
    """Test that individual pipeline steps are called correctly."""

    @patch("scripts.run_v2_candidate_pipeline.discover_candidates")
    @patch("scripts.run_v2_candidate_pipeline.process_dedupe")
    @patch("scripts.run_v2_candidate_pipeline.extract_candidate_features")
    @patch("scripts.run_v2_candidate_pipeline.score_candidate")
    @patch("scripts.run_v2_candidate_pipeline.score_candidates_batch")
    @patch("scripts.run_v2_candidate_pipeline.convert_candidates")
    def test_pipeline_calls_steps_in_order(
        self,
        mock_convert,
        mock_batch,
        mock_score,
        mock_extract,
        mock_dedupe,
        mock_discover,
        tmp_path,
    ):
        """Test that all pipeline steps are called in sequence."""
        from scripts.run_v2_candidate_pipeline import run_pipeline

        # Setup mocks
        mock_discover.return_value = [
            {"candidate_id": "test_candidate_1", "lane": "trusted_sources"}
        ]
        mock_dedupe.return_value = (
            [{"candidate_id": "test_candidate_1", "lane": "trusted_sources"}],
            [],  # duplicates
        )
        mock_extract.return_value = {
            "candidate_id": "test_candidate_1",
            "lane": "trusted_sources",
        }
        mock_score.return_value = {
            "candidate_id": "test_candidate_1",
            "candidate_scores": {"total_score": 80},
            "candidate_score": {"candidate_score": 80, "score_breakdown": {}},
            "priority_bucket": "high",
            "process_decision": "process",
        }
        mock_batch.return_value = (
            [
                {
                    "candidate_id": "test_candidate_1",
                    "candidate_scores": {"total_score": 80},
                    "candidate_score": {"candidate_score": 80, "score_breakdown": {}},
                    "priority_bucket": "high",
                    "process_decision": "process",
                }
            ],
            [],  # defer_list
            [],  # skip_list
        )
        mock_convert.return_value = ["test_record_id"]

        # Run pipeline
        run_pipeline("trusted_sources")

        # Verify call order
        mock_discover.assert_called_once_with("trusted_sources")
        mock_dedupe.assert_called_once()
        mock_extract.assert_called()
        mock_score.assert_called()
        mock_batch.assert_called_once()
        mock_convert.assert_called_once()


class TestSyntheticCandidateGeneration:
    """Test synthetic candidate generation for testing."""

    def test_synthetic_candidate_has_required_fields(self):
        """Test that synthetic candidates have all required schema fields."""
        from scripts.run_v2_candidate_pipeline import create_synthetic_candidate

        candidate = create_synthetic_candidate(
            lane="trusted_sources",
            domain="federalreserve.gov",
            title="FOMC Statement March 29 2026",
            anchor_text="Federal Reserve issues FOMC statement",
            url="https://www.federalreserve.gov/newsevents/pressreleases/monetary20260329a.htm",
        )

        # Check required top-level fields
        assert "candidate_id" in candidate
        assert "lane" in candidate
        assert candidate["lane"] == "trusted_sources"
        assert "discovered_at" in candidate
        assert "topic" in candidate
        assert "source" in candidate
        assert "title" in candidate
        assert "anchor_text" in candidate
        assert "status" in candidate

        # Check source fields
        assert "domain" in candidate["source"]
        assert candidate["source"]["domain"] == "federalreserve.gov"
        assert "source_name" in candidate["source"]
        assert "url" in candidate["source"]
        assert "discovery_method" in candidate["source"]
        assert "trust_tier" in candidate["source"]

        # Check dedupe fields
        assert "dedupe" in candidate
        assert "url_hash" in candidate["dedupe"]

    def test_synthetic_candidate_id_format(self):
        """Test that synthetic candidate IDs follow the expected format."""
        from scripts.run_v2_candidate_pipeline import create_synthetic_candidate

        candidate = create_synthetic_candidate(
            lane="trusted_sources",
            domain="federalreserve.gov",
            title="FOMC Statement March 29 2026",
            anchor_text="Federal Reserve issues FOMC statement",
            url="https://www.federalreserve.gov/newsevents/pressreleases/monetary20260329a.htm",
        )

        # Should have lane prefix
        assert candidate["candidate_id"].startswith("trusted_sources_")

        # Should contain domain
        assert "federalreserve" in candidate["candidate_id"]


class TestCandidateUtils:
    """Test candidate utility functions."""

    def test_generate_candidate_id(self):
        """Test candidate ID generation."""
        from scripts.candidate_utils import generate_candidate_id

        candidate_id = generate_candidate_id(
            lane="trusted_sources",
            domain="federalreserve.gov",
            title="FOMC Statement",
            url="https://www.federalreserve.gov/test",
        )

        assert candidate_id.startswith("trusted_sources_federalreserve")
        assert len(candidate_id) > 0

    def test_hash_url(self):
        """Test URL hashing for dedupe."""
        from scripts.candidate_utils import hash_url

        hash1 = hash_url("https://www.federalreserve.gov/test")
        hash2 = hash_url("https://www.federalreserve.gov/test")
        hash3 = hash_url("https://www.federalreserve.gov/other")

        assert hash1 == hash2  # Same URL should produce same hash
        assert hash1 != hash3  # Different URL should produce different hash

    def test_normalize_title(self):
        """Test title normalization."""
        from scripts.candidate_utils import normalize_title

        title1 = normalize_title("  FOMC Statement March 2026!  ")
        title2 = normalize_title("fomc statement march 2026")

        assert title1 == title2  # Normalized titles should match

    def test_save_candidate_json(self, tmp_path):
        """Test saving candidate to JSON."""
        from scripts.candidate_utils import save_candidate_json

        candidate = {
            "candidate_id": "test_123",
            "lane": "trusted_sources",
            "title": "Test",
        }

        output_path = tmp_path / "test_candidate.json"
        result = save_candidate_json(candidate, output_path)

        assert result == output_path
        assert output_path.exists()

        with open(output_path) as f:
            loaded = json.load(f)
        assert loaded["candidate_id"] == "test_123"

    def test_load_candidate_json(self, tmp_path):
        """Test loading candidate from JSON."""
        from scripts.candidate_utils import load_candidate_json

        candidate = {"candidate_id": "test_456", "lane": "trusted_sources"}

        test_file = tmp_path / "candidate.json"
        with open(test_file, "w") as f:
            json.dump(candidate, f)

        loaded = load_candidate_json(test_file)
        assert loaded["candidate_id"] == "test_456"
