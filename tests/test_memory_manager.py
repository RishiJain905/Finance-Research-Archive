"""Tests for memory_manager module.

Tests adaptive trust scoring, cold-start blending, and memory operations.
Uses mocks for memory_persistence and load_scoring_rules to isolate unit under test.
"""

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Import the module under test
from scripts import memory_manager
from scripts import memory_persistence


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def temp_memory_dir(tmp_path):
    """Create a temporary memory directory for testing."""
    memory_dir = tmp_path / "source_memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    return memory_dir


@pytest.fixture
def temp_domain_memory_file(temp_memory_dir):
    """Path to temporary domain memory file."""
    return temp_memory_dir / "domain_memory.json"


@pytest.fixture
def temp_path_memory_file(temp_memory_dir):
    """Path to temporary path memory file."""
    return temp_memory_dir / "path_memory.json"


@pytest.fixture
def temp_source_memory_file(temp_memory_dir):
    """Path to temporary source memory file."""
    return temp_memory_dir / "source_memory.json"


@pytest.fixture
def mock_memory_paths(temp_memory_dir):
    """Mock the memory file paths to use temp directory."""
    domain_path = temp_memory_dir / "domain_memory.json"
    path_path = temp_memory_dir / "path_memory.json"
    source_path = temp_memory_dir / "source_memory.json"

    with (
        patch.object(memory_persistence, "DOMAIN_MEMORY_PATH", domain_path),
        patch.object(memory_persistence, "PATH_MEMORY_PATH", path_path),
        patch.object(memory_persistence, "SOURCE_MEMORY_PATH", source_path),
    ):
        yield {
            "domain": domain_path,
            "path": path_path,
            "source": source_path,
        }


@pytest.fixture
def mock_scoring_rules():
    """Mock load_scoring_rules to return test baseline trust values."""
    mock_rules = {
        "domain_trust_baselines": {
            "high": [
                "federalreserve.gov",
                "www.federalreserve.gov",
                "newyorkfed.org",
                "www.newyorkfed.org",
            ],
            "medium": [
                "brookings.edu",
                "www.brookings.edu",
                "imf.org",
                "www.imf.org",
            ],
            "low": [],
        },
    }
    with patch("scripts.memory_manager.load_scoring_rules", return_value=mock_rules):
        yield mock_rules


@pytest.fixture
def mock_memory_config():
    """Provide test memory configuration."""
    return {
        "cold_start": {
            "min_samples_for_learning": 10,
            "full_learning_threshold": 25,
            "blend_formula": "linear",
        },
        "weights": {
            "accepted_weight": 10,
            "rejected_weight": 5,
            "filtered_weight": 3,
            "review_weight": 0,
            "human_multiplier": 2.0,
        },
        "trust_score_bounds": {
            "min": 1,
            "max": 100,
        },
        "logging": {
            "log_dir": "logs/source_memory",
            "log_updates": True,
        },
    }


@pytest.fixture
def mock_memory_persistence(temp_memory_dir):
    """Mock all memory_persistence operations to use temp directory."""
    domain_path = temp_memory_dir / "domain_memory.json"
    path_path = temp_memory_dir / "path_memory.json"
    source_path = temp_memory_dir / "source_memory.json"

    with (
        patch.object(memory_persistence, "DOMAIN_MEMORY_PATH", domain_path),
        patch.object(memory_persistence, "PATH_MEMORY_PATH", path_path),
        patch.object(memory_persistence, "SOURCE_MEMORY_PATH", source_path),
        patch.object(memory_persistence, "_ensure_memory_dir"),
    ):
        yield {
            "domain": domain_path,
            "path": path_path,
            "source": source_path,
        }


@pytest.fixture
def sample_domain_memory():
    """Provide a sample domain memory record."""
    return {
        "domain": "federalreserve.gov",
        "trust_score": 85.0,
        "baseline_trust": 100.0,
        "total_candidates": 50,
        "accepted_count": 25,
        "accepted_human_count": 10,
        "accepted_auto_count": 15,
        "review_count": 10,
        "review_human_count": 5,
        "rejected_count": 8,
        "rejected_human_count": 3,
        "rejected_auto_count": 5,
        "filtered_out_count": 7,
        "yield_score": 0.5,
        "noise_score": 0.3,
        "first_seen": datetime.now(timezone.utc).isoformat(),
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# TestDomainBaselineLookup
# =============================================================================


class TestDomainBaselineLookup:
    """Tests for _get_domain_baseline_trust function."""

    def test_high_trust_domain_returns_100(self, mock_scoring_rules):
        """_get_domain_baseline_trust returns 100 for high trust domains."""
        result = memory_manager._get_domain_baseline_trust("federalreserve.gov")
        assert result == 100.0

    def test_high_trust_domain_www_variant(self, mock_scoring_rules):
        """_get_domain_baseline_trust returns 100 for www variant of high trust."""
        result = memory_manager._get_domain_baseline_trust("www.federalreserve.gov")
        assert result == 100.0

    def test_medium_trust_domain_returns_50(self, mock_scoring_rules):
        """_get_domain_baseline_trust returns 50 for medium trust domains."""
        result = memory_manager._get_domain_baseline_trust("brookings.edu")
        assert result == 50.0

    def test_medium_trust_domain_www_variant(self, mock_scoring_rules):
        """_get_domain_baseline_trust returns 50 for www variant of medium trust."""
        result = memory_manager._get_domain_baseline_trust("www.brookings.edu")
        assert result == 50.0

    def test_unknown_domain_returns_10(self, mock_scoring_rules):
        """_get_domain_baseline_trust returns 10 for unknown domains."""
        result = memory_manager._get_domain_baseline_trust("unknown.com")
        assert result == 10.0

    def test_case_insensitive_lookup(self, mock_scoring_rules):
        """_get_domain_baseline_trust is case-insensitive."""
        result1 = memory_manager._get_domain_baseline_trust("FEDERALRESERVE.GOV")
        result2 = memory_manager._get_domain_baseline_trust("FederalReserve.gov")
        assert result1 == 100.0
        assert result2 == 100.0

    def test_fallback_on_scoring_rules_error(self):
        """_get_domain_baseline_trust returns 10 when load_scoring_rules fails."""
        with patch(
            "scripts.memory_manager.load_scoring_rules",
            side_effect=Exception("File not found"),
        ):
            result = memory_manager._get_domain_baseline_trust("anydomain.com")
        assert result == 10.0


# =============================================================================
# TestPathPatternExtraction
# =============================================================================


class TestPathPatternExtraction:
    """Tests for extract_path_pattern function."""

    def test_extracts_first_segment_with_slashes(self):
        """extract_path_pattern returns first segment with leading/trailing slashes."""
        url = "https://brookings.edu/research/economy/analysis"
        result = memory_manager.extract_path_pattern(url)
        assert result == "/research/"

    def test_handles_url_without_path(self):
        """extract_path_pattern returns empty string for root path."""
        url = "https://newyorkfed.org/"
        result = memory_manager.extract_path_pattern(url)
        assert result == ""

    def test_handles_url_with_single_segment(self):
        """extract_path_pattern works with single path segment."""
        url = "https://federalreserve.gov/press-release-2024"
        result = memory_manager.extract_path_pattern(url)
        assert result == "/press-release-2024/"

    def test_handles_url_without_scheme(self):
        """extract_path_pattern adds https:// if missing scheme."""
        url = "brookings.edu/research/economy"
        result = memory_manager.extract_path_pattern(url)
        assert result == "/research/"

    def test_invalid_url_returns_empty_string(self):
        """extract_path_pattern returns empty string for invalid URL."""
        result = memory_manager.extract_path_pattern("invalid-url")
        assert result == ""

    def test_empty_string_returns_empty_string(self):
        """extract_path_pattern returns empty string for empty input."""
        result = memory_manager.extract_path_pattern("")
        assert result == ""

    def test_url_with_query_params(self):
        """extract_path_pattern ignores query parameters."""
        url = "https://brookings.edu/research/?page=1"
        result = memory_manager.extract_path_pattern(url)
        assert result == "/research/"


# =============================================================================
# TestTrustScoreComputation
# =============================================================================


class TestTrustScoreComputation:
    """Tests for compute_trust_score function."""

    def test_cold_start_before_min_samples_heavily_weights_baseline(
        self, mock_memory_config
    ):
        """Before min_samples (10), trust score is mostly baseline."""
        with patch(
            "scripts.memory_manager._load_memory_config",
            return_value=mock_memory_config,
        ):
            # At 5 samples (half of min_samples), baseline weight should be ~0.95
            score = memory_manager.compute_trust_score(
                total=5,
                accepted=0,
                rejected=0,
                filtered_out=0,
                review=0,
                baseline_trust=50.0,
            )
            # Should be close to baseline (50) since learned weight is very small
            assert 49.0 <= score <= 51.0

    def test_cold_start_at_min_samples_blends_linearly(self, mock_memory_config):
        """At min_samples (10), blend is ~90% baseline, ~10% learned."""
        with patch(
            "scripts.memory_manager._load_memory_config",
            return_value=mock_memory_config,
        ):
            score = memory_manager.compute_trust_score(
                total=10,
                accepted=0,
                rejected=0,
                filtered_out=0,
                review=0,
                baseline_trust=50.0,
            )
            # At exactly min_samples, baseline_weight should be 0.9
            assert 49.0 <= score <= 51.0

    def test_cold_start_at_full_threshold_heavily_weights_learned(
        self, mock_memory_config
    ):
        """At full_learning_threshold (25), ~10% baseline, ~90% learned."""
        with patch(
            "scripts.memory_manager._load_memory_config",
            return_value=mock_memory_config,
        ):
            score = memory_manager.compute_trust_score(
                total=25,
                accepted=0,
                rejected=0,
                filtered_out=0,
                review=0,
                baseline_trust=50.0,
            )
            # With no outcomes, learned_trust = baseline, so result = baseline
            assert score == 50.0

    def test_after_full_threshold_heavily_weights_learned(self, mock_memory_config):
        """After full_learning_threshold, ~10% baseline, ~90% learned."""
        with patch(
            "scripts.memory_manager._load_memory_config",
            return_value=mock_memory_config,
        ):
            # 30 samples, some accepted
            score = memory_manager.compute_trust_score(
                total=30,
                accepted=20,
                rejected=5,
                filtered_out=3,
                review=2,
                baseline_trust=50.0,
            )
            # Should be higher than baseline due to positive outcomes
            assert score > 60.0

    def test_human_multiplier_doubles_human_decisions(self, mock_memory_config):
        """Human decisions count 2x (human_multiplier=2.0)."""
        with patch(
            "scripts.memory_manager._load_memory_config",
            return_value=mock_memory_config,
        ):
            # Use 15 samples (between min=10 and full=25) to avoid bounds hitting
            # At 15 samples: blend_ratio = 0.333, learned_weight ≈ 0.366
            auto_score = memory_manager.compute_trust_score(
                total=15,
                accepted=5,
                rejected=0,
                filtered_out=0,
                review=0,
                accepted_human=0,
                baseline_trust=50.0,
            )
            human_score = memory_manager.compute_trust_score(
                total=15,
                accepted=0,
                rejected=0,
                filtered_out=0,
                review=0,
                accepted_human=5,
                baseline_trust=50.0,
            )
            # Human score should be higher due to 2x multiplier
            assert human_score > auto_score

    def test_positive_outcomes_increase_score(self, mock_memory_config):
        """Accepted outcomes increase trust score."""
        with patch(
            "scripts.memory_manager._load_memory_config",
            return_value=mock_memory_config,
        ):
            baseline_score = memory_manager.compute_trust_score(
                total=30,
                accepted=0,
                rejected=0,
                filtered_out=0,
                review=0,
                baseline_trust=50.0,
            )
            positive_score = memory_manager.compute_trust_score(
                total=30,
                accepted=20,
                rejected=5,
                filtered_out=3,
                review=2,
                baseline_trust=50.0,
            )
            assert positive_score > baseline_score

    def test_negative_outcomes_decrease_score(self, mock_memory_config):
        """Rejected/filtered outcomes decrease trust score."""
        with patch(
            "scripts.memory_manager._load_memory_config",
            return_value=mock_memory_config,
        ):
            baseline_score = memory_manager.compute_trust_score(
                total=30,
                accepted=0,
                rejected=0,
                filtered_out=0,
                review=0,
                baseline_trust=50.0,
            )
            negative_score = memory_manager.compute_trust_score(
                total=30,
                accepted=5,
                rejected=15,
                filtered_out=8,
                review=2,
                baseline_trust=50.0,
            )
            assert negative_score < baseline_score

    def test_score_stays_above_min_bound(self, mock_memory_config):
        """Trust score never goes below min bound (1)."""
        with patch(
            "scripts.memory_manager._load_memory_config",
            return_value=mock_memory_config,
        ):
            score = memory_manager.compute_trust_score(
                total=100,
                accepted=0,
                rejected=50,
                filtered_out=50,
                review=0,
                baseline_trust=10.0,
            )
            assert score >= 1.0

    def test_score_stays_below_max_bound(self, mock_memory_config):
        """Trust score never exceeds max bound (100)."""
        with patch(
            "scripts.memory_manager._load_memory_config",
            return_value=mock_memory_config,
        ):
            score = memory_manager.compute_trust_score(
                total=100,
                accepted=100,
                rejected=0,
                filtered_out=0,
                review=0,
                baseline_trust=10.0,
            )
            assert score <= 100.0

    def test_zero_total_returns_baseline(self, mock_memory_config):
        """With zero total, returns baseline trust."""
        with patch(
            "scripts.memory_manager._load_memory_config",
            return_value=mock_memory_config,
        ):
            score = memory_manager.compute_trust_score(
                total=0,
                accepted=0,
                rejected=0,
                filtered_out=0,
                review=0,
                baseline_trust=75.0,
            )
            assert score == 75.0

    def test_review_weight_is_neutral(self, mock_memory_config):
        """Review outcomes have weight 0 (neutral impact)."""
        with patch(
            "scripts.memory_manager._load_memory_config",
            return_value=mock_memory_config,
        ):
            score_no_review = memory_manager.compute_trust_score(
                total=30,
                accepted=10,
                rejected=5,
                filtered_out=5,
                review=0,
                baseline_trust=50.0,
            )
            score_with_review = memory_manager.compute_trust_score(
                total=30,
                accepted=10,
                rejected=5,
                filtered_out=5,
                review=10,
                baseline_trust=50.0,
            )
            # review_weight = 0, so scores should be equal
            assert score_no_review == score_with_review


# =============================================================================
# TestYieldNoiseComputation
# =============================================================================


class TestYieldNoiseComputation:
    """Tests for compute_yield_noise function."""

    def test_yield_score_calculation(self):
        """yield_score = accepted / total."""
        memory = {
            "total_candidates": 100,
            "accepted_count": 25,
            "rejected_count": 10,
            "filtered_out_count": 5,
        }
        yield_score, _ = memory_manager.compute_yield_noise(memory)
        assert yield_score == 0.25

    def test_noise_score_calculation(self):
        """noise_score = (filtered_out + rejected) / total."""
        memory = {
            "total_candidates": 100,
            "accepted_count": 25,
            "rejected_count": 10,
            "filtered_out_count": 5,
        }
        _, noise_score = memory_manager.compute_yield_noise(memory)
        assert noise_score == 0.15

    def test_zero_total_returns_zeros(self):
        """When total=0, returns (0.0, 0.0)."""
        memory = {
            "total_candidates": 0,
            "accepted_count": 0,
            "rejected_count": 0,
            "filtered_out_count": 0,
        }
        yield_score, noise_score = memory_manager.compute_yield_noise(memory)
        assert yield_score == 0.0
        assert noise_score == 0.0

    def test_all_accepted_yields_one(self):
        """All accepted gives yield_score of 1.0."""
        memory = {
            "total_candidates": 50,
            "accepted_count": 50,
            "rejected_count": 0,
            "filtered_out_count": 0,
        }
        yield_score, noise_score = memory_manager.compute_yield_noise(memory)
        assert yield_score == 1.0
        assert noise_score == 0.0

    def test_all_rejected_noise_is_one(self):
        """All rejected gives noise_score of 1.0."""
        memory = {
            "total_candidates": 50,
            "accepted_count": 0,
            "rejected_count": 50,
            "filtered_out_count": 0,
        }
        yield_score, noise_score = memory_manager.compute_yield_noise(memory)
        assert yield_score == 0.0
        assert noise_score == 1.0


# =============================================================================
# TestDomainMemoryOperations
# =============================================================================


class TestDomainMemoryOperations:
    """Tests for domain memory operations."""

    def test_get_or_create_domain_memory_creates_with_baseline(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """get_or_create_domain_memory creates new memory with baseline trust."""
        memory = memory_manager.get_or_create_domain_memory("brookings.edu")

        assert memory["domain"] == "brookings.edu"
        assert memory["baseline_trust"] == 50.0
        assert memory["trust_score"] == 50.0
        assert memory["total_candidates"] == 0

    def test_get_or_create_domain_memory_returns_existing(
        self, mock_memory_persistence, sample_domain_memory
    ):
        """get_or_create_domain_memory returns existing memory if present."""
        # Pre-populate with existing memory
        domain_path = mock_memory_persistence["domain"]
        data = {"federalreserve.gov": sample_domain_memory.copy()}
        domain_path.write_text(json.dumps(data), encoding="utf-8")

        memory = memory_manager.get_or_create_domain_memory("federalreserve.gov")

        assert memory["trust_score"] == 85.0
        assert memory["total_candidates"] == 50

    def test_update_domain_memory_on_outcome_increments_counters(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """update_domain_memory_on_outcome increments appropriate counters."""
        # Create initial memory
        memory_manager.get_or_create_domain_memory("testdomain.com")

        # Update with accepted outcome
        updated = memory_manager.update_domain_memory_on_outcome(
            "testdomain.com", "accepted"
        )

        assert updated["total_candidates"] == 1
        assert updated["accepted_count"] == 1
        assert updated["accepted_auto_count"] == 1

    def test_update_domain_memory_on_outcome_accepted_human(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """update_domain_memory_on_outcome handles accepted_human outcome."""
        memory_manager.get_or_create_domain_memory("testdomain.com")

        updated = memory_manager.update_domain_memory_on_outcome(
            "testdomain.com", "accepted_human"
        )

        assert updated["accepted_human_count"] == 1

    def test_update_domain_memory_on_outcome_rejected(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """update_domain_memory_on_outcome handles rejected outcome."""
        memory_manager.get_or_create_domain_memory("testdomain.com")

        updated = memory_manager.update_domain_memory_on_outcome(
            "testdomain.com", "rejected"
        )

        assert updated["rejected_count"] == 1
        assert updated["rejected_auto_count"] == 1

    def test_update_domain_memory_on_outcome_rejected_human(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """update_domain_memory_on_outcome handles rejected_human outcome."""
        memory_manager.get_or_create_domain_memory("testdomain.com")

        updated = memory_manager.update_domain_memory_on_outcome(
            "testdomain.com", "rejected_human"
        )

        assert updated["rejected_human_count"] == 1

    def test_update_domain_memory_on_outcome_review(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """update_domain_memory_on_outcome handles review outcome."""
        memory_manager.get_or_create_domain_memory("testdomain.com")

        updated = memory_manager.update_domain_memory_on_outcome(
            "testdomain.com", "review"
        )

        assert updated["review_count"] == 1

    def test_update_domain_memory_on_outcome_filtered_out(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """update_domain_memory_on_outcome handles filtered_out outcome."""
        memory_manager.get_or_create_domain_memory("testdomain.com")

        updated = memory_manager.update_domain_memory_on_outcome(
            "testdomain.com", "filtered_out"
        )

        assert updated["filtered_out_count"] == 1

    def test_update_domain_memory_recalculates_trust_score(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """update_domain_memory_on_outcome recalculates trust_score."""
        memory_manager.get_or_create_domain_memory("testdomain.com")

        # Add many accepted outcomes
        for _ in range(20):
            memory_manager.update_domain_memory_on_outcome("testdomain.com", "accepted")

        memory = memory_manager.get_or_create_domain_memory("testdomain.com")
        # With 20 accepted out of 20, score should be high
        assert memory["trust_score"] > 50.0


# =============================================================================
# TestPathMemoryOperations
# =============================================================================


class TestPathMemoryOperations:
    """Tests for path memory operations."""

    def test_get_or_create_path_memory_high_trust_domain(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """get_or_create_path_memory gets 50 baseline for high-trust domain paths."""
        memory = memory_manager.get_or_create_path_memory(
            "federalreserve.gov", "/research/"
        )

        assert memory["domain"] == "federalreserve.gov"
        assert memory["path_pattern"] == "/research/"
        assert memory["baseline_trust"] == 50.0  # High-trust domain → medium path

    def test_get_or_create_path_memory_low_trust_domain(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """get_or_create_path_memory gets 10 baseline for low-trust domain paths."""
        memory = memory_manager.get_or_create_path_memory("unknown.com", "/articles/")

        assert memory["baseline_trust"] == 10.0

    def test_get_or_create_path_memory_medium_trust_domain(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """get_or_create_path_memory gets 10 baseline for medium-trust domain paths."""
        memory = memory_manager.get_or_create_path_memory("brookings.edu", "/research/")

        assert memory["baseline_trust"] == 10.0  # Not high-trust, so low

    def test_update_path_memory_on_outcome(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """update_path_memory_on_outcome updates counters correctly."""
        memory_manager.get_or_create_path_memory("test.com", "/test/")

        updated = memory_manager.update_path_memory_on_outcome(
            "test.com", "/test/", "accepted"
        )

        assert updated["total_candidates"] == 1
        assert updated["accepted_count"] == 1


# =============================================================================
# TestSourceMemoryOperations
# =============================================================================


class TestSourceMemoryOperations:
    """Tests for source memory operations."""

    def test_get_or_create_source_memory_trusted_sources(self, mock_memory_persistence):
        """get_or_create_source_memory gives 100 baseline for trusted_sources."""
        with (
            patch.object(memory_persistence, "get_source_memory", return_value=None),
            patch.object(memory_persistence, "save_source_memory") as mock_save,
        ):
            memory = memory_manager.get_or_create_source_memory(
                "fed_monitor", source_type="trusted_sources"
            )

            assert memory["source_id"] == "fed_monitor"
            assert memory["source_type"] == "trusted_sources"
            assert memory["baseline_trust"] == 100.0
            mock_save.assert_called_once()

    def test_get_or_create_source_memory_keyword_discovery(
        self, mock_memory_persistence
    ):
        """get_or_create_source_memory gives 50 baseline for keyword_discovery."""
        with (
            patch.object(memory_persistence, "get_source_memory", return_value=None),
            patch.object(memory_persistence, "save_source_memory") as mock_save,
        ):
            memory = memory_manager.get_or_create_source_memory(
                "keyword_src", source_type="keyword_discovery"
            )

            assert memory["baseline_trust"] == 50.0
            mock_save.assert_called_once()

    def test_get_or_create_source_memory_seed_crawl(self, mock_memory_persistence):
        """get_or_create_source_memory gives 30 baseline for seed_crawl."""
        with (
            patch.object(memory_persistence, "get_source_memory", return_value=None),
            patch.object(memory_persistence, "save_source_memory") as mock_save,
        ):
            memory = memory_manager.get_or_create_source_memory(
                "seed_src", source_type="seed_crawl"
            )

            assert memory["baseline_trust"] == 30.0
            mock_save.assert_called_once()

    def test_get_or_create_source_memory_manual(self, mock_memory_persistence):
        """get_or_create_source_memory gives 50 baseline for manual (default)."""
        with (
            patch.object(memory_persistence, "get_source_memory", return_value=None),
            patch.object(memory_persistence, "save_source_memory") as mock_save,
        ):
            memory = memory_manager.get_or_create_source_memory("manual_src")

            assert memory["baseline_trust"] == 50.0
            assert memory["source_type"] == "manual"
            mock_save.assert_called_once()

    def test_update_source_memory_on_outcome(self, mock_memory_persistence):
        """update_source_memory_on_outcome updates counters correctly."""
        existing_memory = {
            "source_id": "test_source",
            "source_type": "manual",
            "trust_score": 50.0,
            "baseline_trust": 50.0,
            "total_candidates": 0,
            "accepted_count": 0,
            "rejected_count": 0,
            "filtered_out_count": 0,
            "review_count": 0,
            "accepted_human_count": 0,
            "rejected_human_count": 0,
        }
        with (
            patch.object(
                memory_persistence,
                "get_source_memory",
                return_value=existing_memory,
            ),
            patch.object(memory_persistence, "save_source_memory"),
        ):
            updated = memory_manager.update_source_memory_on_outcome(
                "test_source", "accepted"
            )

            assert updated["total_candidates"] == 1
            assert updated["accepted_count"] == 1


# =============================================================================
# TestCombinedUpdate
# =============================================================================


class TestCombinedUpdate:
    """Tests for update_all_memory_on_outcome function."""

    def test_update_all_memory_updates_domain(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """update_all_memory_on_outcome updates domain memory."""
        result = memory_manager.update_all_memory_on_outcome(
            domain="test.com",
            outcome="accepted",
            url="https://test.com/articles/test",
        )

        assert result["domain_memory"] is not None
        assert result["domain_memory"]["accepted_count"] == 1

    def test_update_all_memory_extracts_path(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """update_all_memory_on_outcome extracts path pattern from URL."""
        result = memory_manager.update_all_memory_on_outcome(
            domain="test.com",
            outcome="accepted",
            url="https://test.com/research/article",
        )

        assert result["path_pattern"] == "/research/"
        assert result["path_memory"] is not None

    def test_update_all_memory_no_path_when_root(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """update_all_memory_on_outcome skips path memory for root URLs."""
        result = memory_manager.update_all_memory_on_outcome(
            domain="test.com",
            outcome="accepted",
            url="https://test.com/",
        )

        assert result["path_pattern"] == ""
        assert result["path_memory"] is None

    def test_update_all_memory_updates_source(self, mock_memory_persistence):
        """update_all_memory_on_outcome updates source memory when source_id given."""
        result = memory_manager.update_all_memory_on_outcome(
            domain="test.com",
            outcome="accepted",
            source_id="test_source",
            source_type="keyword_discovery",
        )

        assert result["source_memory"] is not None
        assert result["source_memory"]["source_id"] == "test_source"

    def test_update_all_memory_returns_summary(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """update_all_memory_on_outcome returns complete summary."""
        result = memory_manager.update_all_memory_on_outcome(
            domain="test.com",
            outcome="rejected",
            source_id="src1",
            source_type="manual",
            url="https://test.com/blog/post",
            candidate_id="cand_123",
        )

        assert "domain_memory" in result
        assert "path_memory" in result
        assert "source_memory" in result
        assert "path_pattern" in result


# =============================================================================
# TestConvenienceFunctions
# =============================================================================


class TestConvenienceFunctions:
    """Tests for convenience getter functions."""

    def test_get_domain_trust_returns_baseline_when_no_memory(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """get_domain_trust returns baseline when no memory exists."""
        trust = memory_manager.get_domain_trust("unknown.com")
        assert trust == 10.0

    def test_get_domain_trust_returns_memory_score(
        self, mock_memory_persistence, sample_domain_memory
    ):
        """get_domain_trust returns trust_score from memory."""
        domain_path = mock_memory_persistence["domain"]
        data = {"federalreserve.gov": sample_domain_memory.copy()}
        domain_path.write_text(json.dumps(data), encoding="utf-8")

        trust = memory_manager.get_domain_trust("federalreserve.gov")
        assert trust == 85.0

    def test_get_path_trust_returns_baseline_for_unknown(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """get_path_trust returns baseline when no memory exists."""
        trust = memory_manager.get_path_trust("unknown.com", "/test/")
        assert trust == 10.0

    def test_get_path_trust_returns_50_for_high_trust_domain(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """get_path_trust returns 50 for unknown path on high-trust domain."""
        trust = memory_manager.get_path_trust("federalreserve.gov", "/unknown/")
        assert trust == 50.0

    def test_get_source_trust_returns_default(self, mock_memory_persistence):
        """get_source_trust returns 50.0 when no memory exists."""
        trust = memory_manager.get_source_trust("nonexistent_source")
        assert trust == 50.0

    def test_get_all_domain_trust_returns_all(
        self, mock_memory_persistence, sample_domain_memory
    ):
        """get_all_domain_trust returns all domain trust scores."""
        domain_path = mock_memory_persistence["domain"]
        data = {
            "fed.gov": {
                **sample_domain_memory,
                "domain": "fed.gov",
                "trust_score": 90.0,
            },
            "imf.org": {
                **sample_domain_memory,
                "domain": "imf.org",
                "trust_score": 70.0,
            },
        }
        domain_path.write_text(json.dumps(data), encoding="utf-8")

        trusts = memory_manager.get_all_domain_trust()

        assert len(trusts) == 2
        assert trusts["fed.gov"] == 90.0
        assert trusts["imf.org"] == 70.0

    def test_get_all_source_trust_returns_all(self, mock_memory_persistence):
        """get_all_source_trust returns all source trust scores."""
        source_path = mock_memory_persistence["source"]
        data = {
            "source1": {"source_id": "source1", "trust_score": 85.0},
            "source2": {"source_id": "source2", "trust_score": 60.0},
        }
        source_path.write_text(json.dumps(data), encoding="utf-8")

        trusts = memory_manager.get_all_source_trust()

        assert len(trusts) == 2
        assert trusts["source1"] == 85.0
        assert trusts["source2"] == 60.0


# =============================================================================
# TestDefaultMemoryConfig
# =============================================================================


class TestDefaultMemoryConfig:
    """Tests for _default_memory_config function."""

    def test_returns_valid_structure(self):
        """_default_memory_config returns a valid dict."""
        config = memory_manager._default_memory_config()

        assert isinstance(config, dict)
        assert "cold_start" in config
        assert "weights" in config
        assert "trust_score_bounds" in config

    def test_cold_start_has_required_fields(self):
        """_default_memory_config cold_start section has required fields."""
        config = memory_manager._default_memory_config()
        cold_start = config.get("cold_start", {})

        assert "min_samples_for_learning" in cold_start
        assert "full_learning_threshold" in cold_start
        assert cold_start["min_samples_for_learning"] == 10
        assert cold_start["full_learning_threshold"] == 25

    def test_weights_have_required_fields(self):
        """_default_memory_config weights section has required fields."""
        config = memory_manager._default_memory_config()
        weights = config.get("weights", {})

        assert weights["accepted_weight"] == 10
        assert weights["rejected_weight"] == 5
        assert weights["filtered_weight"] == 3
        assert weights["review_weight"] == 0
        assert weights["human_multiplier"] == 2.0

    def test_bounds_are_1_to_100(self):
        """_default_memory_config trust_score_bounds are 1 to 100."""
        config = memory_manager._default_memory_config()
        bounds = config.get("trust_score_bounds", {})

        assert bounds["min"] == 1
        assert bounds["max"] == 100


# =============================================================================
# TestMemoryConfigLoading
# =============================================================================


class TestMemoryConfigLoading:
    """Tests for _load_memory_config function."""

    def test_load_memory_config_returns_config(self, tmp_path):
        """_load_memory_config returns configuration dict from memory_persistence."""
        # Test that the function delegates to memory_persistence.load_memory_config
        mock_config = {
            "cold_start": {
                "min_samples_for_learning": 10,
                "full_learning_threshold": 25,
            },
            "weights": {},
        }
        with patch.object(
            memory_persistence, "load_memory_config", return_value=mock_config
        ):
            config = memory_manager._load_memory_config()

        assert isinstance(config, dict)
        assert "cold_start" in config

    def test_load_memory_config_falls_back_to_default_on_error(self, tmp_path):
        """_load_memory_config returns default config on error."""
        # Mock load_memory_config to raise an exception
        with patch.object(
            memory_persistence,
            "load_memory_config",
            side_effect=Exception("File not found"),
        ):
            config = memory_manager._load_memory_config()

        # Should return default config
        assert config["cold_start"]["min_samples_for_learning"] == 10
        assert config["cold_start"]["full_learning_threshold"] == 25


# =============================================================================
# TestEdgeCases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_extract_path_pattern_with_multiple_slashes(self):
        """extract_path_pattern handles URLs with multiple slashes in path."""
        url = "https://example.com/a/b/c/d"
        result = memory_manager.extract_path_pattern(url)
        assert result == "/a/"

    def test_extract_path_pattern_root_path(self):
        """extract_path_pattern handles path that is just /."""
        url = "https://example.com/"
        result = memory_manager.extract_path_pattern(url)
        assert result == ""

    def test_compute_trust_score_with_only_review_outcomes(self, mock_memory_config):
        """compute_trust_score handles review-only outcomes (weight=0)."""
        with patch(
            "scripts.memory_manager._load_memory_config",
            return_value=mock_memory_config,
        ):
            score = memory_manager.compute_trust_score(
                total=10,
                accepted=0,
                rejected=0,
                filtered_out=0,
                review=10,
                baseline_trust=50.0,
            )
            # review_weight = 0, so score should equal baseline
            assert score == 50.0

    def test_domain_memory_unknown_domain_gets_low_baseline(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """get_or_create_domain_memory gives low baseline to unknown domains."""
        memory = memory_manager.get_or_create_domain_memory("never-seen-before.com")

        assert memory["baseline_trust"] == 10.0

    def test_update_all_memory_without_url(self, mock_memory_persistence):
        """update_all_memory_on_outcome works without URL (no path extraction)."""
        result = memory_manager.update_all_memory_on_outcome(
            domain="test.com",
            outcome="accepted",
        )

        assert result["path_pattern"] is None
        assert result["path_memory"] is None
        assert result["domain_memory"] is not None

    def test_update_all_memory_without_source_id(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """update_all_memory_on_outcome works without source_id."""
        result = memory_manager.update_all_memory_on_outcome(
            domain="test.com",
            outcome="accepted",
            url="https://test.com/blog/post",
        )

        assert result["source_memory"] is None
        assert result["domain_memory"] is not None
        assert result["path_memory"] is not None

    def test_memory_update_recalculates_yield_and_noise(
        self, mock_memory_persistence, mock_scoring_rules
    ):
        """Memory updates recalculate yield_score and noise_score."""
        memory_manager.get_or_create_domain_memory("test.com")

        for _ in range(10):
            memory_manager.update_domain_memory_on_outcome("test.com", "accepted")
        for _ in range(5):
            memory_manager.update_domain_memory_on_outcome("test.com", "rejected")

        memory = memory_manager.get_or_create_domain_memory("test.com")

        assert memory["yield_score"] == 10 / 15
        assert memory["noise_score"] == 5 / 15
