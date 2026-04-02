"""
Tests for Stream 4 & 5: Pipeline Integration and Metadata Persistence

Following TDD (Red-Green-Refactor), these tests will initially FAIL
because the implementation files do not exist yet. Once the implementations
are created, these tests should pass.

Tests verify:
1. Candidate Creation from Raw Records (convert_raw_to_candidate.py)
2. Pipeline Integration (run_ingest_and_process.py modifications)
3. Triage Metadata Persistence (process_record.py modifications)

Key functions under test:
- convert_raw_record_to_candidate() - converts a raw record to candidate format
- convert_batch_raw_to_candidates() - batch converts raw records to candidates
- Pipeline: ingest → filter → create candidates → triage → budget gate → process
- Triage metadata persistence in processed records
"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Import modules under test - will fail until implementation is created
from scripts.convert_raw_to_candidate import (
    convert_raw_record_to_candidate,
    convert_batch_raw_to_candidates,
    parse_raw_record,
)
from scripts.run_ingest_and_process import (
    create_candidates_from_raw_records,
    run_triage_on_candidates,
    apply_budget_gate_to_triage_results,
)
from scripts.process_record import (
    persist_triage_metadata,
    load_triage_metadata_for_record,
)


# =============================================================================
# Test Fixtures / Helper Data
# =============================================================================


def create_mock_raw_record(
    record_id="test_001",
    lane="trusted_sources",
    topic="macro catalysts",
    title="Federal Reserve Press Release",
    url="https://federalreserve.gov/press",
    domain_trust_tier="high",
    body_content="This is the body content of the article.",
    discovery_method="monitor",
):
    """Factory to create mock raw record content matching V1 format."""
    header_lines = [
        f"TARGET: {topic}",
        f"TOPIC: {topic}",
        f"TITLE: {title}",
        f"URL: {url}",
        f"LANE: {lane}",
        f"DISCOVERY_METHOD: {discovery_method}",
        f"DOMAIN_TRUST_TIER: {domain_trust_tier}",
    ]
    header = "\n".join(header_lines)
    return f"{header}\n\n{body_content}"


def create_mock_candidate(
    candidate_id="test_001",
    lane="trusted_sources",
    source_domain="federalreserve.gov",
    title="Federal Reserve Press Release",
    source_url="https://federalreserve.gov/press",
    discovered_at="2026-03-29T12:00:00Z",
    source_type="press_release",
    topic="macro catalysts",
):
    """Factory to create mock candidate data for triage."""
    return {
        "candidate_id": candidate_id,
        "lane": lane,
        "source_name": source_domain,
        "source_domain": source_domain,
        "source_url": source_url,
        "discovered_at": discovered_at,
        "topic": topic,
        "title": title,
        "anchor_text": title,  # Usually same as title for article discovery
        "raw_path": f"data/raw/{candidate_id}.txt",
        "source_type": source_type,
        "discovery_context": {
            "query": None,
            "seed_domain": None,
            "parent_url": None,
        },
        # Scoring fields (would be computed by scoring pipeline)
        "domain_trust_score": 100,
        "freshness_hours": 2.0,
        "keyword_match_score": 80,
        "title_quality_score": 70,
        "url_quality_score": 60,
        "duplication_risk_score": 10,
        "topic_hints": ["inflation", "rates"],
    }


# =============================================================================
# TestRawRecordParsing
# =============================================================================


class TestRawRecordParsing(unittest.TestCase):
    """Tests for parse_raw_record function."""

    def setUp(self):
        """Set up temporary raw record directory."""
        self.temp_dir = tempfile.mkdtemp()
        self.raw_dir = Path(self.temp_dir) / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_parse_raw_record_extracts_all_fields(self):
        """parse_raw_record extracts all required fields from raw record format."""
        record_content = create_mock_raw_record(
            record_id="parse_test_001",
            lane="trusted_sources",
            topic="inflation",
            title="FOMC Statement",
            url="https://federalreserve.gov/statement",
            domain_trust_tier="high",
        )
        record_path = self.raw_dir / "parse_test_001.txt"
        record_path.write_text(record_content, encoding="utf-8")

        result = parse_raw_record("parse_test_001", self.raw_dir)

        self.assertEqual(result["record_id"], "parse_test_001")
        self.assertEqual(result["lane"], "trusted_sources")
        self.assertEqual(result["topic"], "inflation")
        self.assertEqual(result["title"], "FOMC Statement")
        self.assertEqual(result["url"], "https://federalreserve.gov/statement")
        self.assertEqual(result["domain_trust_tier"], "high")
        self.assertIn("body_content", result)

    def test_parse_raw_record_handles_missing_optional_fields(self):
        """parse_raw_record handles records with missing optional fields."""
        minimal_content = "TITLE: Minimal Record\nURL: https://example.com\n"
        record_path = self.raw_dir / "minimal_001.txt"
        record_path.write_text(minimal_content, encoding="utf-8")

        result = parse_raw_record("minimal_001", self.raw_dir)

        self.assertEqual(result["record_id"], "minimal_001")
        self.assertEqual(result["title"], "Minimal Record")
        self.assertEqual(result["url"], "https://example.com")

    def test_parse_raw_record_returns_none_for_nonexistent(self):
        """parse_raw_record returns None for nonexistent record."""
        result = parse_raw_record("nonexistent_record", self.raw_dir)
        self.assertIsNone(result)


# =============================================================================
# TestConvertRawRecordToCandidate
# =============================================================================


class TestConvertRawRecordToCandidate(unittest.TestCase):
    """Tests for convert_raw_record_to_candidate function."""

    def setUp(self):
        """Set up temporary directories."""
        self.temp_dir = tempfile.mkdtemp()
        self.raw_dir = Path(self.temp_dir) / "raw"
        self.candidates_dir = Path(self.temp_dir) / "candidates" / "discovered"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.candidates_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_convert_raw_record_to_candidate_returns_dict(self):
        """convert_raw_record_to_candidate returns a dictionary."""
        record_content = create_mock_raw_record(record_id="conv_001")
        record_path = self.raw_dir / "conv_001.txt"
        record_path.write_text(record_content, encoding="utf-8")

        result = convert_raw_record_to_candidate("conv_001", lane="trusted_sources")

        self.assertIsInstance(result, dict)

    def test_converted_candidate_has_all_required_fields(self):
        """Converted candidate has all required fields per schemas/candidate.json."""
        record_content = create_mock_raw_record(
            record_id="full_001",
            lane="trusted_sources",
            topic="macro catalysts",
            title="Fed Announces Rate Decision",
            url="https://federalreserve.gov/announcement",
        )
        record_path = self.raw_dir / "full_001.txt"
        record_path.write_text(record_content, encoding="utf-8")

        result = convert_raw_record_to_candidate("full_001", lane="trusted_sources")

        # Required fields per schemas/candidate.json
        required_fields = [
            "candidate_id",
            "lane",
            "source_name",
            "source_domain",
            "source_url",
            "discovered_at",
            "topic",
            "title",
            "anchor_text",
            "raw_path",
            "source_type",
            "discovery_context",
        ]
        for field in required_fields:
            self.assertIn(field, result, f"Missing required field: {field}")

    def test_candidate_lane_is_article_for_trusted_sources(self):
        """Lane 'trusted_sources' maps to article-like source_type."""
        record_content = create_mock_raw_record(
            record_id="lane_article_001",
            lane="trusted_sources",
        )
        record_path = self.raw_dir / "lane_article_001.txt"
        record_path.write_text(record_content, encoding="utf-8")

        result = convert_raw_record_to_candidate(
            "lane_article_001", lane="trusted_sources"
        )

        # Lane should be preserved
        self.assertEqual(result["lane"], "trusted_sources")
        # Source type should be article-like
        self.assertEqual(result["source_type"], "article")

    def test_candidate_lane_is_quant_for_quant_lane(self):
        """Lane 'quant' candidates get quant source_type."""
        record_content = create_mock_raw_record(
            record_id="lane_quant_001",
            lane="quant",
            title="Daily Treasury Snapshot",
        )
        record_path = self.raw_dir / "lane_quant_001.txt"
        record_path.write_text(record_content, encoding="utf-8")

        result = convert_raw_record_to_candidate("lane_quant_001", lane="quant")

        # Lane should be quant
        self.assertEqual(result["lane"], "quant")
        # Source type should be quant_snapshot
        self.assertEqual(result["source_type"], "quant_snapshot")

    def test_candidate_id_is_deterministic(self):
        """Same input produces same candidate_id."""
        record_content = create_mock_raw_record(record_id="det_001")
        record_path = self.raw_dir / "det_001.txt"
        record_path.write_text(record_content, encoding="utf-8")

        result1 = convert_raw_record_to_candidate("det_001", lane="trusted_sources")
        result2 = convert_raw_record_to_candidate("det_001", lane="trusted_sources")

        self.assertEqual(result1["candidate_id"], result2["candidate_id"])

    def test_convert_raw_record_to_candidate_saves_to_disk(self):
        """convert_raw_record_to_candidate saves candidate JSON to data/candidates/discovered/."""
        record_content = create_mock_raw_record(record_id="save_001")
        record_path = self.raw_dir / "save_001.txt"
        record_path.write_text(record_content, encoding="utf-8")

        with patch(
            "scripts.convert_raw_to_candidate.CANDIDATES_DIR", self.candidates_dir
        ):
            result = convert_raw_record_to_candidate("save_001", lane="trusted_sources")

        # Check file was created
        candidate_path = self.candidates_dir / f"{result['candidate_id']}.json"
        self.assertTrue(
            candidate_path.exists(), f"Expected candidate file at {candidate_path}"
        )

        # Verify saved content matches
        with open(candidate_path, "r", encoding="utf-8") as f:
            saved_data = json.load(f)
        self.assertEqual(saved_data["candidate_id"], result["candidate_id"])


# =============================================================================
# TestConvertBatchRawToCandidates
# =============================================================================


class TestConvertBatchRawToCandidates(unittest.TestCase):
    """Tests for convert_batch_raw_to_candidates function."""

    def setUp(self):
        """Set up temporary directories."""
        self.temp_dir = tempfile.mkdtemp()
        self.raw_dir = Path(self.temp_dir) / "raw"
        self.candidates_dir = Path(self.temp_dir) / "candidates" / "discovered"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.candidates_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_convert_batch_returns_list(self):
        """convert_batch_raw_to_candidates returns a list."""
        record_ids = ["batch_001", "batch_002"]

        with patch(
            "scripts.convert_raw_to_candidate.CANDIDATES_DIR", self.candidates_dir
        ):
            result = convert_batch_raw_to_candidates(record_ids, lane="trusted_sources")

        self.assertIsInstance(result, list)

    def test_convert_batch_processes_all_records(self):
        """convert_batch_raw_to_candidates processes all input record IDs."""
        for i in range(3):
            record_content = create_mock_raw_record(record_id=f"batch_{i}")
            (self.raw_dir / f"batch_{i}.txt").write_text(
                record_content, encoding="utf-8"
            )

        with patch(
            "scripts.convert_raw_to_candidate.CANDIDATES_DIR", self.candidates_dir
        ):
            result = convert_batch_raw_to_candidates(
                ["batch_0", "batch_1", "batch_2"], lane="trusted_sources"
            )

        self.assertEqual(len(result), 3)

    def test_convert_batch_handles_missing_records(self):
        """convert_batch_raw_to_candidates handles missing records gracefully."""
        # Only create 1 of 3 records
        record_content = create_mock_raw_record(record_id="exists_001")
        (self.raw_dir / "exists_001.txt").write_text(record_content, encoding="utf-8")

        with patch(
            "scripts.convert_raw_to_candidate.CANDIDATES_DIR", self.candidates_dir
        ):
            result = convert_batch_raw_to_candidates(
                ["exists_001", "missing_001", "missing_002"], lane="trusted_sources"
            )

        # Should only return successfully converted candidates
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["record_id"], "exists_001")


# =============================================================================
# TestPipelineIntegration
# =============================================================================


class TestPipelineIntegration(unittest.TestCase):
    """Tests for pipeline integration: ingest → filter → create candidates → triage → budget gate."""

    def setUp(self):
        """Set up temporary directories for pipeline test."""
        self.temp_dir = tempfile.mkdtemp()
        self.raw_dir = Path(self.temp_dir) / "raw"
        self.candidates_dir = Path(self.temp_dir) / "candidates" / "discovered"
        self.triage_dir = Path(self.temp_dir) / "triage"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.candidates_dir.mkdir(parents=True, exist_ok=True)
        self.triage_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_create_candidates_from_raw_records_exists(self):
        """run_ingest_and_process.create_candidates_from_raw_records function exists."""
        # This tests that the function has been added to the pipeline
        self.assertTrue(
            hasattr(
                __import__(
                    "scripts.run_ingest_and_process",
                    fromlist=["create_candidates_from_raw_records"],
                ),
                "create_candidates_from_raw_records",
            ),
            "create_candidates_from_raw_records not found in run_ingest_and_process",
        )

    def test_run_triage_on_candidates_exists(self):
        """run_ingest_and_process.run_triage_on_candidates function exists."""
        self.assertTrue(
            hasattr(
                __import__(
                    "scripts.run_ingest_and_process",
                    fromlist=["run_triage_on_candidates"],
                ),
                "run_triage_on_candidates",
            ),
            "run_triage_on_candidates not found in run_ingest_and_process",
        )

    def test_apply_budget_gate_to_triage_results_exists(self):
        """run_ingest_and_process.apply_budget_gate_to_triage_results function exists."""
        self.assertTrue(
            hasattr(
                __import__(
                    "scripts.run_ingest_and_process",
                    fromlist=["apply_budget_gate_to_triage_results"],
                ),
                "apply_budget_gate_to_triage_results",
            ),
            "apply_budget_gate_to_triage_results not found in run_ingest_and_process",
        )

    def test_pipeline_creates_candidates_from_raw(self):
        """Pipeline step: creates candidates from filtered raw records."""
        # Create some raw records
        for i in range(3):
            record_content = create_mock_raw_record(record_id=f"pipeline_001_{i}")
            (self.raw_dir / f"pipeline_001_{i}.txt").write_text(
                record_content, encoding="utf-8"
            )

        with patch(
            "scripts.run_ingest_and_process.CANDIDATES_DIR", self.candidates_dir
        ):
            result = create_candidates_from_raw_records(
                raw_dir=self.raw_dir, lane="trusted_sources"
            )

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 3)

    def test_pipeline_triage_engine_is_called(self):
        """Pipeline step: triage engine is called with candidates."""
        candidates = [
            create_mock_candidate(
                candidate_id=f"triage_cand_{i}", lane="trusted_sources"
            )
            for i in range(3)
        ]

        weights = {
            "source_trust": 0.23,
            "freshness": 0.17,
            "topic_relevance": 0.23,
            "title_quality": 0.12,
            "url_quality": 0.12,
            "novelty": 0.12,
            "quant_value": 0.16,
            "duplicate_risk": -0.15,
        }
        bands = {"critical": 85, "high": 70, "medium": 50, "low": 30}

        with patch("scripts.run_ingest_and_process.TRIAGE_DIR", self.triage_dir):
            result = run_triage_on_candidates(candidates, weights, bands)

        # Should return triage results
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 3)  # (process_now, defer, discard)

        process_now, defer, discard = result
        # All candidates should be in some list
        total = len(process_now) + len(defer) + len(discard)
        self.assertEqual(total, 3)

    def test_pipeline_budget_gate_limits_are_applied(self):
        """Pipeline step: budget gate limits are applied to triage results."""
        # Create 30 candidates (over article limit of 25)
        candidates = [
            create_mock_candidate(
                candidate_id=f"budget_cand_{i}",
                lane="trusted_sources",
                domain_trust_score=100 - i,  # Decreasing priority
            )
            for i in range(30)
        ]

        budget_config = {
            "article_process_limit": 25,
            "quant_process_limit": 10,
            "defer_medium": True,
        }

        with patch("scripts.run_ingest_and_process.TRIAGE_DIR", self.triage_dir):
            result = apply_budget_gate_to_triage_results(
                triage_results=([c for c in candidates], [], []),  # All in process_now
                budget_config=budget_config,
                lane="trusted_sources",
            )

        process_list = result[0]
        # Should be limited to 25
        self.assertLessEqual(len(process_list), 25)

    def test_pipeline_only_selected_candidates_are_processed(self):
        """Pipeline step: only selected (under budget) candidates are marked for processing."""
        # Create 30 candidates
        candidates = [
            create_mock_candidate(
                candidate_id=f"select_cand_{i}",
                lane="trusted_sources",
                domain_trust_score=100 - i,
            )
            for i in range(30)
        ]

        budget_config = {
            "article_process_limit": 25,
            "quant_process_limit": 10,
            "defer_medium": True,
        }

        with patch("scripts.run_ingest_and_process.TRIAGE_DIR", self.triage_dir):
            process_list, defer_list, discard_list = (
                apply_budget_gate_to_triage_results(
                    triage_results=([c for c in candidates], [], []),
                    budget_config=budget_config,
                    lane="trusted_sources",
                )
            )

        # Process list should contain candidates with highest scores
        # First candidate should be cand_0 (highest score 100)
        self.assertEqual(process_list[0]["candidate_id"], "select_cand_0")
        # Last candidate should be cand_24 (25th highest)
        self.assertEqual(process_list[-1]["candidate_id"], "select_cand_24")

    def test_pipeline_metrics_are_recorded(self):
        """Pipeline step: metrics are recorded for all triage actions."""
        candidates = [
            create_mock_candidate(
                candidate_id=f"metrics_cand_{i}", lane="trusted_sources"
            )
            for i in range(5)
        ]

        weights = {
            "source_trust": 0.23,
            "freshness": 0.17,
            "topic_relevance": 0.23,
            "title_quality": 0.12,
            "url_quality": 0.12,
            "novelty": 0.12,
            "quant_value": 0.16,
            "duplicate_risk": -0.15,
        }
        bands = {"critical": 85, "high": 70, "medium": 50, "low": 30}
        budget_config = {
            "article_process_limit": 25,
            "quant_process_limit": 10,
            "defer_medium": True,
        }

        with patch("scripts.run_ingest_and_process.TRIAGE_DIR", self.triage_dir):
            # Run triage
            triage_result = run_triage_on_candidates(candidates, weights, bands)
            # Apply budget gate (which should record metrics)
            apply_budget_gate_to_triage_results(
                triage_result, budget_config, "trusted_sources"
            )

        # Check metrics file was created
        metrics_path = self.triage_dir / "metrics.json"
        self.assertTrue(metrics_path.exists(), "Metrics file should be created")


# =============================================================================
# TestTriageMetadataPersistence
# =============================================================================


class TestTriageMetadataPersistence(unittest.TestCase):
    """Tests for triage metadata persistence in processed records."""

    def setUp(self):
        """Set up temporary directories."""
        self.temp_dir = tempfile.mkdtemp()
        self.triage_dir = Path(self.temp_dir) / "triage"
        self.accepted_dir = Path(self.temp_dir) / "accepted"
        self.rejected_dir = Path(self.temp_dir) / "rejected"
        self.review_queue_dir = Path(self.temp_dir) / "review_queue"
        self.triage_dir.mkdir(parents=True, exist_ok=True)
        self.accepted_dir.mkdir(parents=True, exist_ok=True)
        self.rejected_dir.mkdir(parents=True, exist_ok=True)
        self.review_queue_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_persist_triage_metadata_exists(self):
        """process_record.persist_triage_metadata function exists."""
        self.assertTrue(
            hasattr(
                __import__(
                    "scripts.process_record", fromlist=["persist_triage_metadata"]
                ),
                "persist_triage_metadata",
            ),
            "persist_triage_metadata not found in process_record",
        )

    def test_load_triage_metadata_for_record_exists(self):
        """process_record.load_triage_metadata_for_record function exists."""
        self.assertTrue(
            hasattr(
                __import__(
                    "scripts.process_record",
                    fromlist=["load_triage_metadata_for_record"],
                ),
                "load_triage_metadata_for_record",
            ),
            "load_triage_metadata_for_record not found in process_record",
        )

    def test_triage_metadata_appears_in_processed_record(self):
        """Triage metadata appears in accepted/processed records."""
        candidate = create_mock_candidate(candidate_id="meta_001")
        candidate["triage_result"] = {
            "priority_score": 85.5,
            "priority_band": "high",
            "scoring": {
                "source_trust": 100.0,
                "freshness": 90.0,
                "topic_relevance": 85.0,
            },
            "reasons": ["high trust domain", "very recent publication"],
            "action": "process_now",
        }

        with patch("scripts.process_record.TRIAGE_DIR", self.triage_dir):
            persist_triage_metadata("meta_001", candidate, "accepted")

        # Check triage metadata file was created
        triage_path = self.triage_dir / "meta_001.json"
        self.assertTrue(
            triage_path.exists(), f"Triage metadata should be saved to {triage_path}"
        )

        # Verify content
        with open(triage_path, "r", encoding="utf-8") as f:
            saved_metadata = json.load(f)

        self.assertIn("priority_score", saved_metadata)
        self.assertIn("priority_band", saved_metadata)
        self.assertIn("lane", saved_metadata)
        self.assertIn("reasons", saved_metadata)

    def test_triage_metadata_includes_priority_score(self):
        """Triage metadata includes priority_score field."""
        candidate = create_mock_candidate(candidate_id="score_001")
        candidate["triage_result"] = {
            "priority_score": 92.3,
            "priority_band": "critical",
            "action": "process_now",
        }

        with patch("scripts.process_record.TRIAGE_DIR", self.triage_dir):
            persist_triage_metadata("score_001", candidate, "accepted")

        triage_path = self.triage_dir / "score_001.json"
        with open(triage_path, "r", encoding="utf-8") as f:
            saved_metadata = json.load(f)

        self.assertEqual(saved_metadata["priority_score"], 92.3)

    def test_triage_metadata_includes_priority_band(self):
        """Triage metadata includes priority_band field."""
        candidate = create_mock_candidate(candidate_id="band_001")
        candidate["triage_result"] = {
            "priority_score": 75.0,
            "priority_band": "high",
            "action": "process_now",
        }

        with patch("scripts.process_record.TRIAGE_DIR", self.triage_dir):
            persist_triage_metadata("band_001", candidate, "accepted")

        triage_path = self.triage_dir / "band_001.json"
        with open(triage_path, "r", encoding="utf-8") as f:
            saved_metadata = json.load(f)

        self.assertEqual(saved_metadata["priority_band"], "high")

    def test_triage_metadata_includes_lane(self):
        """Triage metadata includes lane field."""
        candidate = create_mock_candidate(candidate_id="lane_001", lane="quant")
        candidate["triage_result"] = {
            "priority_score": 88.0,
            "priority_band": "critical",
            "action": "process_now",
        }

        with patch("scripts.process_record.TRIAGE_DIR", self.triage_dir):
            persist_triage_metadata("lane_001", candidate, "accepted")

        triage_path = self.triage_dir / "lane_001.json"
        with open(triage_path, "r", encoding="utf-8") as f:
            saved_metadata = json.load(f)

        self.assertEqual(saved_metadata["lane"], "quant")

    def test_triage_metadata_includes_reasons(self):
        """Triage metadata includes reasons field."""
        candidate = create_mock_candidate(candidate_id="reasons_001")
        candidate["triage_result"] = {
            "priority_score": 80.0,
            "priority_band": "high",
            "reasons": ["high trust domain", "strong topic match"],
            "action": "process_now",
        }

        with patch("scripts.process_record.TRIAGE_DIR", self.triage_dir):
            persist_triage_metadata("reasons_001", candidate, "accepted")

        triage_path = self.triage_dir / "reasons_001.json"
        with open(triage_path, "r", encoding="utf-8") as f:
            saved_metadata = json.load(f)

        self.assertIn("reasons", saved_metadata)
        self.assertIsInstance(saved_metadata["reasons"], list)
        self.assertEqual(len(saved_metadata["reasons"]), 2)

    def test_load_triage_metadata_retrieves_saved_data(self):
        """load_triage_metadata_for_record retrieves previously saved triage metadata."""
        candidate = create_mock_candidate(candidate_id="load_001")
        candidate["triage_result"] = {
            "priority_score": 78.5,
            "priority_band": "high",
            "reasons": ["high trust domain"],
            "action": "process_now",
        }

        with patch("scripts.process_record.TRIAGE_DIR", self.triage_dir):
            persist_triage_metadata("load_001", candidate, "accepted")
            loaded_metadata = load_triage_metadata_for_record("load_001")

        self.assertIsNotNone(loaded_metadata)
        self.assertEqual(loaded_metadata["priority_score"], 78.5)
        self.assertEqual(loaded_metadata["priority_band"], "high")
        self.assertEqual(loaded_metadata["lane"], "trusted_sources")

    def test_load_triage_metadata_returns_none_for_nonexistent(self):
        """load_triage_metadata_for_record returns None for nonexistent record."""
        with patch("scripts.process_record.TRIAGE_DIR", self.triage_dir):
            result = load_triage_metadata_for_record("nonexistent_record")

        self.assertIsNone(result)

    def test_triage_metadata_persists_for_different_destinations(self):
        """Triage metadata is persisted for accepted, rejected, and review_queue records."""
        destinations = ["accepted", "rejected", "review_queue"]

        for destination in destinations:
            candidate = create_mock_candidate(candidate_id=f"dest_{destination}")
            candidate["triage_result"] = {
                "priority_score": 65.0,
                "priority_band": "medium",
                "action": "process_now" if destination == "accepted" else "defer",
            }

            with patch("scripts.process_record.TRIAGE_DIR", self.triage_dir):
                persist_triage_metadata(f"dest_{destination}", candidate, destination)

            triage_path = self.triage_dir / f"dest_{destination}.json"
            self.assertTrue(
                triage_path.exists(),
                f"Triage metadata should be saved for {destination}",
            )


# =============================================================================
# TestPipelineFlowIntegration
# =============================================================================


class TestPipelineFlowIntegration(unittest.TestCase):
    """Integration tests for the complete pipeline flow."""

    def setUp(self):
        """Set up temporary directories for full pipeline test."""
        self.temp_dir = tempfile.mkdtemp()
        self.raw_dir = Path(self.temp_dir) / "raw"
        self.candidates_dir = Path(self.temp_dir) / "candidates" / "discovered"
        self.triage_dir = Path(self.temp_dir) / "triage"
        self.accepted_dir = Path(self.temp_dir) / "accepted"
        self.rejected_dir = Path(self.temp_dir) / "rejected"
        self.review_queue_dir = Path(self.temp_dir) / "review_queue"

        for dir_path in [
            self.raw_dir,
            self.candidates_dir,
            self.triage_dir,
            self.accepted_dir,
            self.rejected_dir,
            self.review_queue_dir,
        ]:
            dir_path.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_full_pipeline_flow_raw_to_candidate_to_triage_to_metadata(self):
        """Full flow: raw record → candidate → triage → budget gate → metadata persistence."""
        # Step 1: Create raw records
        raw_record_content = create_mock_raw_record(
            record_id="full_flow_001",
            lane="trusted_sources",
            topic="inflation",
            title="CPI Report Released",
        )
        (self.raw_dir / "full_flow_001.txt").write_text(
            raw_record_content, encoding="utf-8"
        )

        # Step 2: Convert raw to candidate
        with patch(
            "scripts.convert_raw_to_candidate.CANDIDATES_DIR", self.candidates_dir
        ):
            candidate = convert_raw_record_to_candidate(
                "full_flow_001", lane="trusted_sources"
            )

        self.assertIsNotNone(candidate)
        self.assertIn("candidate_id", candidate)

        # Step 3: Run triage on candidate
        weights = {
            "source_trust": 0.23,
            "freshness": 0.17,
            "topic_relevance": 0.23,
            "title_quality": 0.12,
            "url_quality": 0.12,
            "novelty": 0.12,
            "quant_value": 0.16,
            "duplicate_risk": -0.15,
        }
        bands = {"critical": 85, "high": 70, "medium": 50, "low": 30}
        budget_config = {
            "article_process_limit": 25,
            "quant_process_limit": 10,
            "defer_medium": True,
        }

        with patch("scripts.run_ingest_and_process.TRIAGE_DIR", self.triage_dir):
            process_list, defer_list, discard_list = run_triage_on_candidates(
                [candidate], weights, bands
            )

        # Step 4: Apply budget gate
        with patch("scripts.run_ingest_and_process.TRIAGE_DIR", self.triage_dir):
            final_process, final_defer, final_discard = (
                apply_budget_gate_to_triage_results(
                    (process_list, defer_list, discard_list),
                    budget_config,
                    "trusted_sources",
                )
            )

        # Step 5: Persist triage metadata
        if final_process:
            with patch("scripts.process_record.TRIAGE_DIR", self.triage_dir):
                persist_triage_metadata(
                    final_process[0]["candidate_id"], final_process[0], "accepted"
                )

            # Verify metadata was saved
            triage_path = self.triage_dir / f"{final_process[0]['candidate_id']}.json"
            self.assertTrue(triage_path.exists())

            with open(triage_path, "r", encoding="utf-8") as f:
                saved_metadata = json.load(f)

            self.assertIn("priority_score", saved_metadata)
            self.assertIn("priority_band", saved_metadata)
            self.assertIn("lane", saved_metadata)
            self.assertIn("reasons", saved_metadata)

    def test_pipeline_quant_lane_separation(self):
        """Pipeline correctly separates quant lane from article lanes."""
        # Create mixed lane records
        article_content = create_mock_raw_record(
            record_id="article_001", lane="trusted_sources"
        )
        quant_content = create_mock_raw_record(record_id="quant_001", lane="quant")

        (self.raw_dir / "article_001.txt").write_text(article_content, encoding="utf-8")
        (self.raw_dir / "quant_001.txt").write_text(quant_content, encoding="utf-8")

        # Convert to candidates
        with patch(
            "scripts.convert_raw_to_candidate.CANDIDATES_DIR", self.candidates_dir
        ):
            article_candidate = convert_raw_record_to_candidate(
                "article_001", lane="trusted_sources"
            )
            quant_candidate = convert_raw_record_to_candidate("quant_001", lane="quant")

        self.assertEqual(article_candidate["lane"], "trusted_sources")
        self.assertEqual(article_candidate["source_type"], "article")
        self.assertEqual(quant_candidate["lane"], "quant")
        self.assertEqual(quant_candidate["source_type"], "quant_snapshot")

    def test_pipeline_respects_quant_budget_limit(self):
        """Pipeline applies different budget limits for quant vs article lanes."""
        # Create 15 quant candidates (over limit of 10)
        candidates = [
            create_mock_candidate(
                candidate_id=f"quant_budget_{i}",
                lane="quant",
                domain_trust_score=100 - i,
            )
            for i in range(15)
        ]

        budget_config = {
            "article_process_limit": 25,
            "quant_process_limit": 10,
            "defer_medium": True,
        }

        with patch("scripts.run_ingest_and_process.TRIAGE_DIR", self.triage_dir):
            triage_result = run_triage_on_candidates(
                candidates,
                weights={
                    "source_trust": 0.23,
                    "freshness": 0.17,
                    "topic_relevance": 0.23,
                    "title_quality": 0.12,
                    "url_quality": 0.12,
                    "novelty": 0.12,
                    "quant_value": 0.16,
                    "duplicate_risk": -0.15,
                },
                bands={"critical": 85, "high": 70, "medium": 50, "low": 30},
            )

            process_list, defer_list, discard_list = (
                apply_budget_gate_to_triage_results(
                    triage_result, budget_config, "quant"
                )
            )

        # Quant lane should be limited to 10
        self.assertLessEqual(len(process_list), 10)


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    unittest.main()
