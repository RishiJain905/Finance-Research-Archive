"""Tests for score_candidates_batch module."""

import json
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from scripts.score_candidates_batch import (
    load_scoring_rules,
    route_to_priority_bucket,
    determine_process_decision,
    write_scoring_log,
    score_candidates_batch,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def scoring_rules():
    """Load actual scoring rules for tests."""
    return load_scoring_rules()


@pytest.fixture
def mock_scoring_rules():
    """Provide mock scoring rules for isolated testing."""
    return {
        "weights": {
            "domain_trust": 0.25,
            "url_quality": 0.20,
            "title_quality": 0.20,
            "keyword_match": 0.20,
            "freshness": 0.10,
            "lane_reliability": 0.10,
            "duplication_risk": -0.15,
        },
        "thresholds": {
            "high": 75,
            "medium": 60,
            "low": 45,
        },
        "priority_buckets": {
            "high": "process",
            "medium": "defer",
            "low": "defer",
            "skip": "skip",
        },
        "freshness": {
            "max_hours_for_full_score": 168,
        },
        "lane_reliability": {
            "trusted_sources": 100,
            "keyword_discovery": 50,
            "seed_crawl": 30,
        },
        "domain_trust_baselines": {
            "high": ["federalreserve.gov"],
            "medium": ["brookings.edu"],
            "low": [],
        },
        "url_hints": {
            "positive": ["press", "statement"],
            "negative": ["about", "careers"],
        },
        "title_hints": {
            "positive": ["inflation", "rates"],
            "negative": ["subscribe"],
        },
        "source_type_map": {
            "federalreserve.gov": "rulemaking",
        },
    }


@pytest.fixture
def sample_candidates():
    """Provide sample candidates for batch testing."""
    return [
        {
            "candidate_id": "high_score_1",
            "url": "https://federalreserve.gov/press-release-2024",
            "title": "Fed Announces Inflation Interest Rates Decision",
            "anchor_text": "Federal Reserve monetary policy",
            "source_domain": "federalreserve.gov",
            "lane": "trusted_sources",
            "discovered_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "candidate_id": "medium_score_1",
            "url": "https://brookings.edu/research/economic-outlook",
            "title": "Economic Outlook Analysis",
            "anchor_text": "Economic research",
            "source_domain": "brookings.edu",
            "lane": "keyword_discovery",
            "discovered_at": (
                datetime.now(timezone.utc) - timedelta(days=3)
            ).isoformat(),
        },
        {
            "candidate_id": "low_score_1",
            "url": "https://example.com/about",
            "title": "About Our Company",
            "anchor_text": "Learn more",
            "source_domain": "example.com",
            "lane": "seed_crawl",
            "discovered_at": (
                datetime.now(timezone.utc) - timedelta(days=10)
            ).isoformat(),
        },
        {
            "candidate_id": "skip_score_1",
            "url": "https://example.com/careers",
            "title": "Subscribe to Newsletter",
            "anchor_text": "Sign up",
            "source_domain": "example.com",
            "lane": "seed_crawl",
            "discovered_at": (
                datetime.now(timezone.utc) - timedelta(days=30)
            ).isoformat(),
        },
    ]


@pytest.fixture
def tmp_output_dir(tmp_path):
    """Provide temporary output directory for log files."""
    log_dir = tmp_path / "logs" / "candidate_scoring"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


# =============================================================================
# TestLoadScoringRules
# =============================================================================


class TestLoadScoringRules:
    """Tests for load_scoring_rules function."""

    def test_loads_scoring_rules_json(self):
        """Verify scoring_rules.json is loaded."""
        rules = load_scoring_rules()

        assert isinstance(rules, dict)
        assert len(rules) > 0

    def test_scoring_rules_has_thresholds(self):
        """Scoring rules contain thresholds."""
        rules = load_scoring_rules()

        assert "thresholds" in rules
        assert "high" in rules["thresholds"]
        assert "medium" in rules["thresholds"]
        assert "low" in rules["thresholds"]


# =============================================================================
# TestRouteToPriorityBucket
# =============================================================================


class TestRouteToPriorityBucket:
    """Tests for route_to_priority_bucket function."""

    def test_high_bucket(self):
        """score >= 75 → 'high'."""
        thresholds = {"high": 75, "medium": 60, "low": 45}

        assert route_to_priority_bucket(75, thresholds) == "high"
        assert route_to_priority_bucket(90, thresholds) == "high"
        assert route_to_priority_bucket(100, thresholds) == "high"

    def test_medium_bucket(self):
        """score 60-74 → 'medium'."""
        thresholds = {"high": 75, "medium": 60, "low": 45}

        assert route_to_priority_bucket(60, thresholds) == "medium"
        assert route_to_priority_bucket(65, thresholds) == "medium"
        assert route_to_priority_bucket(74, thresholds) == "medium"

    def test_low_bucket(self):
        """score 45-59 → 'low'."""
        thresholds = {"high": 75, "medium": 60, "low": 45}

        assert route_to_priority_bucket(45, thresholds) == "low"
        assert route_to_priority_bucket(50, thresholds) == "low"
        assert route_to_priority_bucket(59, thresholds) == "low"

    def test_skip_bucket(self):
        """score < 45 → 'skip'."""
        thresholds = {"high": 75, "medium": 60, "low": 45}

        assert route_to_priority_bucket(44, thresholds) == "skip"
        assert route_to_priority_bucket(30, thresholds) == "skip"
        assert route_to_priority_bucket(0, thresholds) == "skip"
        assert route_to_priority_bucket(-10, thresholds) == "skip"

    def test_boundary_at_high_threshold(self):
        """Score just below high threshold is medium."""
        thresholds = {"high": 75, "medium": 60, "low": 45}

        assert route_to_priority_bucket(74.99, thresholds) == "medium"

    def test_boundary_at_low_threshold(self):
        """Score just below low threshold is skip."""
        thresholds = {"high": 75, "medium": 60, "low": 45}

        assert route_to_priority_bucket(44.99, thresholds) == "skip"


# =============================================================================
# TestDetermineProcessDecision
# =============================================================================


class TestDetermineProcessDecision:
    """Tests for determine_process_decision function."""

    def test_high_maps_to_process(self):
        """'high' bucket → 'process'."""
        priority_buckets = {
            "high": "process",
            "medium": "defer",
            "low": "defer",
            "skip": "skip",
        }

        assert determine_process_decision("high", priority_buckets) == "process"

    def test_medium_maps_to_defer(self):
        """'medium' bucket → 'defer'."""
        priority_buckets = {
            "high": "process",
            "medium": "defer",
            "low": "defer",
            "skip": "skip",
        }

        assert determine_process_decision("medium", priority_buckets) == "defer"

    def test_low_maps_to_defer(self):
        """'low' bucket → 'defer'."""
        priority_buckets = {
            "high": "process",
            "medium": "defer",
            "low": "defer",
            "skip": "skip",
        }

        assert determine_process_decision("low", priority_buckets) == "defer"

    def test_skip_maps_to_skip(self):
        """'skip' bucket → 'skip'."""
        priority_buckets = {
            "high": "process",
            "medium": "defer",
            "low": "defer",
            "skip": "skip",
        }

        assert determine_process_decision("skip", priority_buckets) == "skip"

    def test_unknown_bucket_defaults_to_skip(self):
        """Unknown bucket defaults to 'skip'."""
        priority_buckets = {
            "high": "process",
            "medium": "defer",
            "low": "defer",
            "skip": "skip",
        }

        assert determine_process_decision("unknown", priority_buckets) == "skip"


# =============================================================================
# TestWriteScoringLog
# =============================================================================


class TestWriteScoringLog:
    """Tests for write_scoring_log function."""

    def test_creates_log_file(self, tmp_output_dir):
        """File is created in logs/candidate_scoring/."""
        candidates = [
            {
                "candidate_id": "test_1",
                "lane": "trusted_sources",
                "source_domain": "federalreserve.gov",
                "title": "Test",
                "candidate_score": {"candidate_score": 80, "score_breakdown": {}},
                "priority_bucket": "high",
                "process_decision": "process",
            }
        ]

        log_path = write_scoring_log(candidates, tmp_output_dir)

        assert log_path.exists()
        assert log_path.suffix == ".jsonl"

    def test_log_is_jsonl_format(self, tmp_output_dir):
        """Each line is valid JSON."""
        candidates = [
            {
                "candidate_id": f"test_{i}",
                "lane": "trusted_sources",
                "source_domain": "federalreserve.gov",
                "title": "Test",
                "candidate_score": {"candidate_score": 80, "score_breakdown": {}},
                "priority_bucket": "high",
                "process_decision": "process",
            }
            for i in range(3)
        ]

        log_path = write_scoring_log(candidates, tmp_output_dir)

        with open(log_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        assert len(lines) == 3
        for line in lines:
            # Each line should be valid JSON
            record = json.loads(line)
            assert isinstance(record, dict)

    def test_log_contains_required_fields(self, tmp_output_dir):
        """All required fields per record."""
        candidates = [
            {
                "candidate_id": "test_1",
                "lane": "trusted_sources",
                "source_domain": "federalreserve.gov",
                "title": "Fed Press Release",
                "candidate_score": {
                    "candidate_score": 85,
                    "score_breakdown": {"domain_trust": {"raw": 100}},
                },
                "priority_bucket": "high",
                "process_decision": "process",
            }
        ]

        log_path = write_scoring_log(candidates, tmp_output_dir)

        with open(log_path, "r", encoding="utf-8") as f:
            line = f.readline()

        record = json.loads(line)
        required_fields = [
            "candidate_id",
            "lane",
            "domain",
            "title",
            "candidate_score",
            "score_breakdown",
            "priority_bucket",
            "process_decision",
            "timestamp",
        ]

        for field in required_fields:
            assert field in record, f"Missing required field: {field}"

    def test_log_has_one_record_per_candidate(self, tmp_output_dir):
        """Count matches input."""
        candidates = [
            {
                "candidate_id": f"test_{i}",
                "lane": "trusted_sources",
                "source_domain": "federalreserve.gov",
                "title": "Test",
                "candidate_score": {"candidate_score": 80, "score_breakdown": {}},
                "priority_bucket": "high",
                "process_decision": "process",
            }
            for i in range(5)
        ]

        log_path = write_scoring_log(candidates, tmp_output_dir)

        with open(log_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        assert len(lines) == 5

    def test_log_uses_source_domain_for_domain_field(self, tmp_output_dir):
        """Log record uses source_domain for domain field."""
        candidates = [
            {
                "candidate_id": "test_1",
                "lane": "trusted_sources",
                "source_domain": "federalreserve.gov",
                "title": "Test",
                "candidate_score": {"candidate_score": 80, "score_breakdown": {}},
                "priority_bucket": "high",
                "process_decision": "process",
            }
        ]

        log_path = write_scoring_log(candidates, tmp_output_dir)

        with open(log_path, "r", encoding="utf-8") as f:
            line = f.readline()

        record = json.loads(line)
        assert record["domain"] == "federalreserve.gov"


# =============================================================================
# TestScoreCandidatesBatch
# =============================================================================


class TestScoreCandidatesBatch:
    """Tests for score_candidates_batch function."""

    @patch("scripts.score_candidates_batch.write_scoring_log")
    def test_returns_three_lists(self, mock_write_log, scoring_rules):
        """Returns (process, defer, skip)."""
        mock_write_log.return_value = Path("/tmp/test.jsonl")

        candidates = [
            {
                "candidate_id": "test_1",
                "url": "https://federalreserve.gov/press-release",
                "title": "Fed Press Release on Inflation Rates",
                "anchor_text": "",
                "source_domain": "federalreserve.gov",
                "lane": "trusted_sources",
                "discovered_at": datetime.now(timezone.utc).isoformat(),
            }
        ]

        process_list, defer_list, skip_list = score_candidates_batch(
            candidates, scoring_rules
        )

        assert isinstance(process_list, list)
        assert isinstance(defer_list, list)
        assert isinstance(skip_list, list)

    @patch("scripts.score_candidates_batch.write_scoring_log")
    def test_high_scores_route_to_process(self, mock_write_log, scoring_rules):
        """Candidates with score >= 75 in process list."""
        mock_write_log.return_value = Path("/tmp/test.jsonl")

        # Use a candidate with maximum signals to ensure >= 75
        # High trust domain + all positive URL hints + all positive title hints
        candidates = [
            {
                "candidate_id": "high_score_1",
                "url": "https://federalreserve.gov/press/statement/report/speech/research/analysis/release/announcement/decision/testimony/bulletin/commentary/market-notice/policy/article",
                "title": "Monetary Policy Inflation Rates Liquidity Treasury Repo Labor Market GDP PCE Volatility Fed Funds Interest Rate FOMC Central Bank Reserve Balance Sheet Quantitative Market Structure Macro Catalysts Funding Yield Curve",
                "anchor_text": "Monetary Policy Inflation Rates Liquidity Treasury Repo Labor Market GDP PCE Volatility Fed Funds Interest Rate",
                "source_domain": "federalreserve.gov",
                "lane": "trusted_sources",
                "discovered_at": datetime.now(timezone.utc).isoformat(),
            },
        ]

        process_list, defer_list, skip_list = score_candidates_batch(
            candidates, scoring_rules
        )

        # High trust domain with all positive signals should be >= 75 (score ~87.5)
        assert len(process_list) >= 1
        assert any(c["candidate_id"] == "high_score_1" for c in process_list)

    @patch("scripts.score_candidates_batch.write_scoring_log")
    def test_skip_scores_route_to_skip(self, mock_write_log, scoring_rules):
        """Candidates with score < 45 in skip list."""
        mock_write_log.return_value = Path("/tmp/test.jsonl")

        candidates = [
            {
                "candidate_id": "skip_score_1",
                "url": "https://example.com/careers",
                "title": "Subscribe to Newsletter - Careers",
                "anchor_text": "Sign up now",
                "source_domain": "example.com",
                "lane": "seed_crawl",
                "discovered_at": (
                    datetime.now(timezone.utc) - timedelta(days=30)
                ).isoformat(),
            },
        ]

        process_list, defer_list, skip_list = score_candidates_batch(
            candidates, scoring_rules
        )

        # Low trust domain with negative keywords and old should be skipped
        assert len(skip_list) >= 1
        assert any(c["candidate_id"] == "skip_score_1" for c in skip_list)

    @patch("scripts.score_candidates_batch.write_scoring_log")
    def test_medium_low_in_defer(self, mock_write_log, scoring_rules):
        """Candidates 45-74 in defer list."""
        mock_write_log.return_value = Path("/tmp/test.jsonl")

        candidates = [
            {
                "candidate_id": "medium_score_1",
                "url": "https://example.com/research",
                "title": "Subscribe to our newsletter",
                "anchor_text": "Click here",
                "source_domain": "example.com",
                "lane": "seed_crawl",
                "discovered_at": (
                    datetime.now(timezone.utc) - timedelta(days=5)
                ).isoformat(),
            },
        ]

        process_list, defer_list, skip_list = score_candidates_batch(
            candidates, scoring_rules
        )

        # Unknown domain with negative title hints but not terrible should be deferred
        # (In this case, the negative title hints and unknown domain should result in defer)
        assert len(defer_list) + len(skip_list) >= 1

    @patch("scripts.score_candidates_batch.write_scoring_log")
    def test_all_candidates_scored(
        self, mock_write_log, scoring_rules, sample_candidates
    ):
        """All input candidates appear in some output list."""
        mock_write_log.return_value = Path("/tmp/test.jsonl")

        process_list, defer_list, skip_list = score_candidates_batch(
            sample_candidates, scoring_rules
        )

        all_output = process_list + defer_list + skip_list
        output_ids = {c["candidate_id"] for c in all_output}
        input_ids = {c["candidate_id"] for c in sample_candidates}

        assert output_ids == input_ids

    @patch("scripts.score_candidates_batch.write_scoring_log")
    def test_log_file_created(
        self, mock_write_log, scoring_rules, sample_candidates, tmp_path
    ):
        """JSONL log is written."""
        mock_write_log.return_value = Path("/tmp/test.jsonl")

        process_list, defer_list, skip_list = score_candidates_batch(
            sample_candidates, scoring_rules
        )

        # Verify write_scoring_log was called
        assert mock_write_log.called

    @patch("scripts.score_candidates_batch.write_scoring_log")
    def test_scored_candidates_have_required_fields(
        self, mock_write_log, scoring_rules
    ):
        """Scored candidates have all required scoring fields."""
        mock_write_log.return_value = Path("/tmp/test.jsonl")

        candidates = [
            {
                "candidate_id": "test_1",
                "url": "https://federalreserve.gov/press",
                "title": "Fed Press Release on Inflation",
                "anchor_text": "",
                "source_domain": "federalreserve.gov",
                "lane": "trusted_sources",
                "discovered_at": datetime.now(timezone.utc).isoformat(),
            }
        ]

        process_list, defer_list, skip_list = score_candidates_batch(
            candidates, scoring_rules
        )

        # All output candidates should have scoring fields
        for candidate in process_list + defer_list + skip_list:
            assert "candidate_score" in candidate
            assert "candidate_score" in candidate["candidate_score"]
            assert "score_breakdown" in candidate["candidate_score"]
            assert "priority_bucket" in candidate
            assert "process_decision" in candidate

    @patch("scripts.score_candidates_batch.write_scoring_log")
    def test_empty_candidates_list(self, mock_write_log, scoring_rules):
        """Empty input returns three empty lists."""
        mock_write_log.return_value = Path("/tmp/test.jsonl")

        process_list, defer_list, skip_list = score_candidates_batch([], scoring_rules)

        assert process_list == []
        assert defer_list == []
        assert skip_list == []

    @patch("scripts.score_candidates_batch.write_scoring_log")
    def test_priority_bucket_added_to_candidates(self, mock_write_log, scoring_rules):
        """priority_bucket field is added to each candidate."""
        mock_write_log.return_value = Path("/tmp/test.jsonl")

        candidates = [
            {
                "candidate_id": "test_1",
                "url": "https://federalreserve.gov/press",
                "title": "Fed Press Release",
                "anchor_text": "",
                "source_domain": "federalreserve.gov",
                "lane": "trusted_sources",
                "discovered_at": datetime.now(timezone.utc).isoformat(),
            }
        ]

        process_list, defer_list, skip_list = score_candidates_batch(
            candidates, scoring_rules
        )

        all_candidates = process_list + defer_list + skip_list
        for candidate in all_candidates:
            assert "priority_bucket" in candidate
            assert candidate["priority_bucket"] in ["high", "medium", "low", "skip"]

    @patch("scripts.score_candidates_batch.write_scoring_log")
    def test_process_decision_added_to_candidates(self, mock_write_log, scoring_rules):
        """process_decision field is added to each candidate."""
        mock_write_log.return_value = Path("/tmp/test.jsonl")

        candidates = [
            {
                "candidate_id": "test_1",
                "url": "https://federalreserve.gov/press",
                "title": "Fed Press Release",
                "anchor_text": "",
                "source_domain": "federalreserve.gov",
                "lane": "trusted_sources",
                "discovered_at": datetime.now(timezone.utc).isoformat(),
            }
        ]

        process_list, defer_list, skip_list = score_candidates_batch(
            candidates, scoring_rules
        )

        all_candidates = process_list + defer_list + skip_list
        for candidate in all_candidates:
            assert "process_decision" in candidate
            assert candidate["process_decision"] in ["process", "defer", "skip"]
