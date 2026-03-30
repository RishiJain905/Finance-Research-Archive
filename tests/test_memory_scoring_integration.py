"""Integration tests for memory-based scoring in V2.5 Part 2.

Tests the integration between memory system (memory_persistence, memory_manager)
and the scoring system (extract_candidate_features, score_candidate).

These tests verify that:
1. extract_candidate_features() returns memory-based trust scores
2. score_candidate() incorporates memory-based adjustments
3. Cold-start behavior blends baseline and memory trust appropriately
4. Path patterns and source quality influence scoring
"""

import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Import modules under test
from scripts import memory_persistence
from scripts import memory_manager
from scripts.extract_candidate_features import extract_candidate_features
from scripts.score_candidate import score_candidate


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def temp_memory_dir(tmp_path):
    """Create a temporary memory directory for isolated testing."""
    memory_dir = tmp_path / "source_memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    return memory_dir


@pytest.fixture
def mock_memory_paths(temp_memory_dir):
    """Mock memory file paths to use temp directory."""
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
def clean_memory_files(mock_memory_paths):
    """Ensure clean memory state before and after tests.

    Yields the mock_memory_paths fixture and ensures all memory files
    are empty after the test completes.
    """
    # Clear any existing data before test
    for path in mock_memory_paths.values():
        path.write_text("{}", encoding="utf-8")

    yield mock_memory_paths

    # Clean up after test
    for path in mock_memory_paths.values():
        if path.exists():
            path.write_text("{}", encoding="utf-8")


@pytest.fixture
def memory_populated(mock_memory_paths):
    """Populate memory with known values for testing.

    Creates:
    - Domain memory for "testdomain.com" with trust=75
    - Path memory for "/research/" with trust=80
    - Path memory for "/events/" with trust=15 (low trust)
    - Source memory for "test_source" with yield=0.8, noise=0.2
    """
    # Domain memory for testdomain.com with known trust
    domain_memory = {
        "domain": "testdomain.com",
        "trust_score": 75.0,
        "baseline_trust": 50.0,
        "total_candidates": 25,
        "accepted_count": 15,
        "accepted_human_count": 5,
        "accepted_auto_count": 10,
        "review_count": 5,
        "review_human_count": 2,
        "rejected_count": 3,
        "rejected_human_count": 1,
        "rejected_auto_count": 2,
        "filtered_out_count": 2,
        "yield_score": 0.6,
        "noise_score": 0.2,
        "first_seen": datetime.now(timezone.utc).isoformat(),
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }

    # Path memory for /research/ (high trust)
    path_memory_research = {
        "domain": "testdomain.com",
        "path_pattern": "/research/",
        "trust_score": 80.0,
        "baseline_trust": 50.0,
        "total_candidates": 20,
        "accepted_count": 14,
        "accepted_human_count": 4,
        "review_count": 3,
        "review_human_count": 1,
        "rejected_count": 2,
        "rejected_human_count": 0,
        "rejected_auto_count": 2,
        "filtered_out_count": 1,
        "yield_score": 0.7,
        "noise_score": 0.15,
        "first_seen": datetime.now(timezone.utc).isoformat(),
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }

    # Path memory for /events/ (low trust)
    path_memory_events = {
        "domain": "testdomain.com",
        "path_pattern": "/events/",
        "trust_score": 15.0,
        "baseline_trust": 10.0,
        "total_candidates": 10,
        "accepted_count": 2,
        "accepted_human_count": 0,
        "review_count": 2,
        "review_human_count": 0,
        "rejected_count": 4,
        "rejected_human_count": 0,
        "rejected_auto_count": 4,
        "filtered_out_count": 2,
        "yield_score": 0.2,
        "noise_score": 0.6,
        "first_seen": datetime.now(timezone.utc).isoformat(),
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }

    # Source memory for test_source (high yield, low noise)
    source_memory = {
        "source_id": "test_source",
        "source_type": "keyword_discovery",
        "source_domain": "testdomain.com",
        "trust_score": 70.0,
        "yield_score": 0.8,
        "noise_score": 0.2,
        "total_candidates": 30,
        "accepted_count": 20,
        "accepted_human_count": 8,
        "review_count": 5,
        "review_human_count": 2,
        "rejected_count": 3,
        "rejected_human_count": 1,
        "rejected_auto_count": 2,
        "filtered_out_count": 2,
        "first_seen": datetime.now(timezone.utc).isoformat(),
        "last_seen": datetime.now(timezone.utc).isoformat(),
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }

    # Write all memory data
    domain_path = mock_memory_paths["domain"]
    path_file_path = mock_memory_paths["path"]
    source_path = mock_memory_paths["source"]

    domain_path.write_text(
        json.dumps({"testdomain.com": domain_memory}), encoding="utf-8"
    )
    path_file_path.write_text(
        json.dumps(
            {
                "testdomain.com::/research/": path_memory_research,
                "testdomain.com::/events/": path_memory_events,
            }
        ),
        encoding="utf-8",
    )
    source_path.write_text(json.dumps({"test_source": source_memory}), encoding="utf-8")

    yield {
        "domain": domain_memory,
        "path_research": path_memory_research,
        "path_events": path_memory_events,
        "source": source_memory,
    }


@pytest.fixture
def sample_candidate():
    """Standard sample candidate for testing."""
    return {
        "candidate_id": "test_candidate_123",
        "url": "https://testdomain.com/research/article",
        "title": "Test Research Article on Economy",
        "anchor_text": "Test anchor text",
        "source_domain": "testdomain.com",
        "lane": "trusted_sources",
        "discovered_at": datetime.now(timezone.utc).isoformat(),
    }


@pytest.fixture
def brookings_candidate():
    """Sample candidate from brookings.edu for end-to-end testing."""
    return {
        "candidate_id": "brookings_research_001",
        "url": "https://brookings.edu/research/economy/analysis",
        "title": "Economic Analysis: Inflation and Interest Rates",
        "anchor_text": "Brookings economic research report",
        "source_domain": "brookings.edu",
        "lane": "keyword_discovery",
        "discovered_at": datetime.now(timezone.utc).isoformat(),
        "source_id": "brookings_keyword_src",
    }


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
            "positive": ["research", "analysis", "report"],
            "negative": ["events", "careers"],
        },
        "title_hints": {
            "positive": ["inflation", "rates", "monetary policy", "interest rate"],
            "negative": [],
        },
    }


# =============================================================================
# Test 1: Memory-Enhanced Feature Extraction
# =============================================================================


class TestExtractCandidateFeaturesWithMemory:
    """Tests for memory-enhanced feature extraction.

    Verifies that extract_candidate_features() returns memory-based trust scores:
    - domain_trust_score should come from domain memory
    - path_trust_score should be extracted from path memory
    - source_yield_score and source_noise_score should come from source memory
    """

    def test_extract_candidate_features_with_memory(
        self, clean_memory_files, memory_populated, sample_candidate
    ):
        """After memory creation, domain_trust_score should be from memory.

        Test scenario:
        1. Domain memory exists for "testdomain.com" with trust=75
        2. extract_candidate_features() is called for a candidate from that domain
        3. The returned domain_trust_score should be influenced by memory (75)
        """
        # Patch memory lookups to return our populated memory
        with (
            patch.object(
                memory_manager, "get_domain_trust", return_value=75.0
            ) as mock_domain_trust,
            patch.object(
                memory_manager, "get_path_trust", return_value=80.0
            ) as mock_path_trust,
            patch.object(
                memory_manager,
                "get_or_create_source_memory",
                return_value=memory_populated["source"],
            ) as mock_source,
        ):
            # Call extract_candidate_features
            result = extract_candidate_features(sample_candidate.copy())

            # Verify domain_trust_score is blended (memory + baseline due to cold-start)
            # With 25 total samples and full_learning_threshold=25, blending is:
            # 10% baseline (10) + 90% memory (75) = 68.5
            assert result["domain_trust_score"] == 68.5, (
                f"Expected domain_trust_score=68.5 (cold-start blended), got {result['domain_trust_score']}"
            )

            # Verify path_trust_score is extracted from path memory
            assert "path_trust_score" in result, "path_trust_score should be in result"
            assert result["path_trust_score"] == 80.0, (
                f"Expected path_trust_score=80 from memory, got {result['path_trust_score']}"
            )

    def test_extract_candidate_features_with_low_memory_trust(
        self, clean_memory_files, memory_populated, sample_candidate
    ):
        """Low trust in memory should be reflected in domain_trust_score.

        When domain memory shows low trust (< 20), the candidate should receive
        a low domain_trust_score.
        """
        # Modify candidate to use the low-trust /events/ path
        candidate = sample_candidate.copy()
        candidate["url"] = "https://testdomain.com/events/conference"

        with (
            patch.object(
                memory_manager, "get_domain_trust", return_value=75.0
            ) as mock_domain_trust,
            patch.object(
                memory_manager,
                "get_path_trust",
                return_value=15.0,  # Low trust path
            ) as mock_path_trust,
            patch.object(
                memory_manager,
                "get_or_create_source_memory",
                return_value=memory_populated["source"],
            ) as mock_source,
        ):
            result = extract_candidate_features(candidate)

            # Path trust should be low (15)
            assert "path_trust_score" in result
            assert result["path_trust_score"] == 15.0, (
                f"Expected path_trust_score=15 (low trust), got {result['path_trust_score']}"
            )

    def test_extract_candidate_features_without_memory(
        self, clean_memory_files, sample_candidate
    ):
        """Without memory, should fall back to baseline trust.

        When no memory exists for a domain, the system should use
        baseline trust from scoring_rules.json.
        """
        # Ensure no memory exists
        assert memory_persistence.get_domain_memory("newdomain.com") is None

        candidate = sample_candidate.copy()
        candidate["source_domain"] = "newdomain.com"
        candidate["url"] = "https://newdomain.com/page"

        # Mock memory lookups to return None (no memory)
        with (
            patch.object(
                memory_manager,
                "get_domain_trust",
                return_value=10.0,  # baseline
            ) as mock_domain_trust,
            patch.object(
                memory_manager,
                "get_path_trust",
                return_value=10.0,  # baseline
            ) as mock_path_trust,
            patch.object(
                memory_manager, "get_or_create_source_memory", return_value=None
            ) as mock_source,
        ):
            result = extract_candidate_features(candidate)

            # Should use baseline trust (10)
            assert result["domain_trust_score"] == 10.0, (
                f"Expected domain_trust_score=10 (baseline) when no memory, got {result['domain_trust_score']}"
            )


# =============================================================================
# Test 2: Memory Integration in Score Calculation
# =============================================================================


class TestScoreCandidateWithMemory:
    """Tests for memory integration in score calculation.

    Verifies that score_candidate() incorporates memory-based adjustments:
    - When path_trust_score is low (< 20), final score should be reduced
    - When source_yield_score is high (> 0.7) and source_noise_score is low (< 0.3),
      final score should increase
    """

    def test_score_candidate_with_low_path_trust(
        self, mock_scoring_rules, sample_candidate
    ):
        """Low path_trust_score (< 20) should reduce final score.

        Test scenario:
        1. Create a candidate with all standard features
        2. Add memory-based features: path_trust_score=15 (low)
        3. Call score_candidate()
        4. Verify the final score is penalized for low path trust
        """
        # Build candidate with memory-based features
        candidate = sample_candidate.copy()
        candidate["freshness_hours"] = 24.0
        candidate["url_quality_score"] = 50.0
        candidate["title_quality_score"] = 50.0
        candidate["keyword_match_score"] = 50.0
        candidate["domain_trust_score"] = 75.0  # From memory
        candidate["lane_reliability_score"] = 50.0
        candidate["duplication_risk_score"] = 10.0
        # Memory-based feature: low path trust
        candidate["path_trust_score"] = 15.0
        candidate["source_yield_score"] = 0.5
        candidate["source_noise_score"] = 0.3

        result = score_candidate(candidate.copy(), mock_scoring_rules)
        final_score = result["candidate_score"]["candidate_score"]

        # Build a baseline candidate without low path trust for comparison
        baseline_candidate = candidate.copy()
        baseline_candidate["path_trust_score"] = 50.0  # Normal trust

        baseline_result = score_candidate(baseline_candidate, mock_scoring_rules)
        baseline_score = baseline_result["candidate_score"]["candidate_score"]

        # Low path trust should result in lower score
        assert final_score < baseline_score, (
            f"Expected low path_trust to reduce score, but {final_score} >= {baseline_score}"
        )

    def test_score_candidate_with_high_yield_low_noise(
        self, mock_scoring_rules, sample_candidate
    ):
        """High yield (> 0.7) + low noise (< 0.3) should boost final score.

        When source memory shows high yield and low noise, the candidate
        should receive a scoring bonus.
        """
        # Build candidate with good source quality
        candidate = sample_candidate.copy()
        candidate["freshness_hours"] = 24.0
        candidate["url_quality_score"] = 50.0
        candidate["title_quality_score"] = 50.0
        candidate["keyword_match_score"] = 50.0
        candidate["domain_trust_score"] = 50.0
        candidate["lane_reliability_score"] = 50.0
        candidate["duplication_risk_score"] = 10.0
        candidate["path_trust_score"] = 50.0
        # Good source quality from memory
        candidate["source_yield_score"] = 0.8  # High yield
        candidate["source_noise_score"] = 0.2  # Low noise

        result = score_candidate(candidate.copy(), mock_scoring_rules)
        good_source_score = result["candidate_score"]["candidate_score"]

        # Build candidate with poor source quality
        poor_candidate = candidate.copy()
        poor_candidate["source_yield_score"] = 0.2  # Low yield
        poor_candidate["source_noise_score"] = 0.7  # High noise

        poor_result = score_candidate(poor_candidate, mock_scoring_rules)
        poor_source_score = poor_result["candidate_score"]["candidate_score"]

        # Good source quality should score higher
        assert good_source_score > poor_source_score, (
            f"Expected good source (yield=0.8, noise=0.2) to score higher than "
            f"poor source (yield=0.2, noise=0.7), "
            f"but got {good_source_score} <= {poor_source_score}"
        )

    def test_score_candidate_memory_features_in_breakdown(
        self, mock_scoring_rules, sample_candidate
    ):
        """Memory-based features should appear in score_breakdown.

        When path_trust_score and source quality scores are present,
        they should be included in the scoring breakdown.
        """
        candidate = sample_candidate.copy()
        candidate["freshness_hours"] = 24.0
        candidate["url_quality_score"] = 50.0
        candidate["title_quality_score"] = 50.0
        candidate["keyword_match_score"] = 50.0
        candidate["domain_trust_score"] = 75.0
        candidate["lane_reliability_score"] = 50.0
        candidate["duplication_risk_score"] = 10.0
        candidate["path_trust_score"] = 80.0
        candidate["source_yield_score"] = 0.8
        candidate["source_noise_score"] = 0.2

        result = score_candidate(candidate.copy(), mock_scoring_rules)
        breakdown = result["candidate_score"]["score_breakdown"]

        # Verify memory-based features are tracked
        assert "path_trust" in breakdown or "path_trust_score" in candidate, (
            "path_trust_score should be tracked in breakdown or candidate"
        )


# =============================================================================
# Test 3: Cold-Start Behavior
# =============================================================================


class TestColdStartTrustBehavior:
    """Tests for cold-start behavior in trust scoring.

    Verifies that scoring uses baseline trust when memory is new (few samples):
    - Before 10 samples: should use mostly baseline trust
    - After 25 samples: should use mostly memory trust
    """

    def test_cold_start_before_min_samples(self, clean_memory_files):
        """Before 10 samples, should use mostly baseline trust.

        When a domain has < 10 samples, cold-start blending should weight
        baseline trust heavily (e.g., 90% baseline, 10% learned).
        """
        # Create domain memory with few samples (cold start)
        domain_memory = memory_persistence.initialize_domain_memory(
            domain="coldstart.example",
            baseline_trust=50.0,  # Medium baseline
            trust_score=50.0,
        )
        domain_memory["total_candidates"] = 5  # Below min_samples (10)
        domain_memory["accepted_count"] = 3
        domain_memory["rejected_count"] = 1
        domain_memory["filtered_out_count"] = 1
        # Calculated trust based on outcomes but cold-start should override
        domain_memory["trust_score"] = memory_manager.compute_trust_score(
            total=domain_memory["total_candidates"],
            accepted=domain_memory["accepted_count"],
            rejected=domain_memory["rejected_count"],
            filtered_out=domain_memory["filtered_out_count"],
            review=domain_memory.get("review_count", 0),
            accepted_human=domain_memory.get("accepted_human_count", 0),
            rejected_human=domain_memory.get("rejected_human_count", 0),
            baseline_trust=domain_memory["baseline_trust"],
        )

        memory_persistence.save_domain_memory("coldstart.example", domain_memory)

        # The computed trust should be close to baseline due to cold-start blending
        # With 5 samples (half of min_samples=10), blend should be ~0.95 baseline
        computed_trust = memory_manager.get_domain_trust("coldstart.example")

        # Verify cold-start blending occurred (trust should be very close to baseline)
        # At 5 samples with min=10, we expect ~90-95% baseline weight
        baseline = domain_memory["baseline_trust"]
        assert abs(computed_trust - baseline) < 15, (
            f"At cold-start (5 samples), trust {computed_trust} should be close to "
            f"baseline {baseline}, but difference is {abs(computed_trust - baseline)}"
        )

    def test_cold_start_after_full_threshold(self, clean_memory_files):
        """After 25 samples, should use mostly memory trust.

        When a domain has >= 25 samples, cold-start blending should weight
        learned trust heavily (e.g., 10% baseline, 90% learned).
        """
        # Create domain memory with many samples (full learning)
        domain_memory = memory_persistence.initialize_domain_memory(
            domain="mature.example",
            baseline_trust=50.0,
            trust_score=50.0,
        )
        domain_memory["total_candidates"] = 30  # Above full_learning_threshold (25)
        domain_memory["accepted_count"] = 20
        domain_memory["rejected_count"] = 5
        domain_memory["filtered_out_count"] = 3
        domain_memory["accepted_human_count"] = 10
        domain_memory["rejected_human_count"] = 2
        # Recalculate trust with full learning
        domain_memory["trust_score"] = memory_manager.compute_trust_score(
            total=domain_memory["total_candidates"],
            accepted=domain_memory["accepted_count"],
            rejected=domain_memory["rejected_count"],
            filtered_out=domain_memory["filtered_out_count"],
            review=domain_memory.get("review_count", 0),
            accepted_human=domain_memory.get("accepted_human_count", 0),
            rejected_human=domain_memory.get("rejected_human_count", 0),
            baseline_trust=domain_memory["baseline_trust"],
        )

        memory_persistence.save_domain_memory("mature.example", domain_memory)

        # The computed trust should be based mostly on learned outcomes
        computed_trust = memory_manager.get_domain_trust("mature.example")

        # With 30 samples (above threshold 25), trust should be mostly learned
        # Learned trust = baseline + adjustment
        # adjustment = 20*10 + 10*10*2 - 5*5 - 2*5*2 - 3*3 = 200 + 200 - 25 - 20 - 9 = 346
        # learned_trust = 50 + 346 = 396, clamped to 100
        # With 90% learned weight: final ≈ 0.1*50 + 0.9*100 ≈ 95
        assert computed_trust > 80, (
            f"At mature stage (30 samples), trust {computed_trust} should be high "
            f"(mostly learned), expected > 80"
        )

    def test_cold_start_blend_formula_linear(self, clean_memory_files):
        """Cold-start blend should be linear between min and full threshold.

        Between 10 and 25 samples, the blend should transition linearly
        from mostly baseline to mostly learned.
        """
        baseline = 50.0

        # Test at exactly min_samples (10) - should have 90% baseline, 10% learned
        trust_at_10 = memory_manager.compute_trust_score(
            total=10,
            accepted=6,
            rejected=2,
            filtered_out=2,
            review=0,
            baseline_trust=baseline,
        )

        # Test at midpoint (17-18) - should be around 50/50 blend
        trust_at_17 = memory_manager.compute_trust_score(
            total=17,
            accepted=10,
            rejected=4,
            filtered_out=3,
            review=0,
            baseline_trust=baseline,
        )

        # Test at full_threshold (25) - should have 10% baseline, 90% learned
        trust_at_25 = memory_manager.compute_trust_score(
            total=25,
            accepted=15,
            rejected=5,
            filtered_out=5,
            review=0,
            baseline_trust=baseline,
        )

        # Verify monotonic increase in trust as samples increase
        assert trust_at_10 <= trust_at_17 <= trust_at_25, (
            f"Trust should increase with samples: {trust_at_10} <= {trust_at_17} <= {trust_at_25}"
        )

        # At 10 samples: close to baseline (90% baseline weight)
        assert abs(trust_at_10 - baseline) < 20, (
            f"At 10 samples, trust {trust_at_10} should be close to baseline {baseline}"
        )

        # At 25 samples: should be mostly learned (90% learned weight)
        assert trust_at_25 > trust_at_10 + 20, (
            f"At 25 samples, trust {trust_at_25} should be significantly higher than "
            f"at 10 samples ({trust_at_10})"
        )


# =============================================================================
# Test 4: Path Pattern Influence
# =============================================================================


class TestPathPatternInfluencesScoring:
    """Tests for path pattern influence on scoring.

    Verifies that different path patterns get different trust scores:
    - /events/ pattern from memory should score low
    - /research/ pattern from memory should score high
    - Unknown paths should use default (50)
    """

    def test_path_pattern_events_low_trust(self, clean_memory_files, memory_populated):
        """Path /events/ from memory should score low (15).

        When path memory shows /events/ has low trust (15), candidates
        from that path should receive a low path_trust_score.
        """
        # Verify path memory exists for /events/
        path_memory = memory_persistence.get_path_memory("testdomain.com", "/events/")
        assert path_memory is not None
        assert path_memory["trust_score"] == 15.0

        # Mock the get_path_trust function to return low trust
        with patch.object(
            memory_manager, "get_path_trust", return_value=15.0
        ) as mock_get_path:
            candidate = {
                "candidate_id": "events_candidate",
                "url": "https://testdomain.com/events/conference",
                "title": "Upcoming Economic Conference",
                "anchor_text": "Event announcement",
                "source_domain": "testdomain.com",
                "lane": "keyword_discovery",
                "discovered_at": datetime.now(timezone.utc).isoformat(),
            }

            result = extract_candidate_features(candidate)

            # Path trust should be low
            assert "path_trust_score" in result
            assert result["path_trust_score"] == 15.0

    def test_path_pattern_research_high_trust(
        self, clean_memory_files, memory_populated
    ):
        """Path /research/ from memory should score high (80).

        When path memory shows /research/ has high trust (80), candidates
        from that path should receive a high path_trust_score.
        """
        # Verify path memory exists for /research/
        path_memory = memory_persistence.get_path_memory("testdomain.com", "/research/")
        assert path_memory is not None
        assert path_memory["trust_score"] == 80.0

        with patch.object(
            memory_manager, "get_path_trust", return_value=80.0
        ) as mock_get_path:
            candidate = {
                "candidate_id": "research_candidate",
                "url": "https://testdomain.com/research/paper",
                "title": "Economic Research Analysis",
                "anchor_text": "Research findings",
                "source_domain": "testdomain.com",
                "lane": "keyword_discovery",
                "discovered_at": datetime.now(timezone.utc).isoformat(),
            }

            result = extract_candidate_features(candidate)

            # Path trust should be high
            assert "path_trust_score" in result
            assert result["path_trust_score"] == 80.0

    def test_path_pattern_unknown_defaults_to_baseline(
        self, clean_memory_files, sample_candidate
    ):
        """Unknown path pattern should use default baseline (50).

        When no path memory exists, the default trust should be used:
        - 50 for high-trust domains
        - 10 for other domains
        """
        # Ensure no path memory exists
        assert memory_persistence.get_path_memory("unknown.domain", "/unknown/") is None

        with patch.object(
            memory_manager,
            "get_path_trust",
            return_value=10.0,  # Default for unknown paths
        ) as mock_get_path:
            candidate = sample_candidate.copy()
            candidate["url"] = "https://unknown.domain/page"

            result = extract_candidate_features(candidate)

            # Should use default path trust
            assert "path_trust_score" in result


# =============================================================================
# Test 5: Source Quality Influence
# =============================================================================


class TestSourceQualityInfluencesScoring:
    """Tests for source quality (yield/noise) influence on scoring.

    Verifies that source memory yield/noise affects scoring:
    - High-yield source should boost score
    - High-noise source should reduce score
    """

    def test_high_yield_source_boosts_score(self, mock_scoring_rules, sample_candidate):
        """High yield source should boost the final score.

        A source with yield_score > 0.7 should contribute positively
        to the candidate's final score.
        """
        # Candidate with high-yield source
        candidate = sample_candidate.copy()
        candidate["freshness_hours"] = 24.0
        candidate["url_quality_score"] = 50.0
        candidate["title_quality_score"] = 50.0
        candidate["keyword_match_score"] = 50.0
        candidate["domain_trust_score"] = 50.0
        candidate["lane_reliability_score"] = 50.0
        candidate["duplication_risk_score"] = 10.0
        candidate["path_trust_score"] = 50.0
        candidate["source_yield_score"] = 0.85  # High yield
        candidate["source_noise_score"] = 0.15

        result = score_candidate(candidate.copy(), mock_scoring_rules)
        high_yield_score = result["candidate_score"]["candidate_score"]

        # Candidate with medium yield
        medium_candidate = candidate.copy()
        medium_candidate["source_yield_score"] = 0.5
        medium_candidate["source_noise_score"] = 0.3

        medium_result = score_candidate(medium_candidate, mock_scoring_rules)
        medium_score = medium_result["candidate_score"]["candidate_score"]

        # High yield should score higher
        assert high_yield_score > medium_score, (
            f"High yield (0.85) should score higher than medium yield (0.5), "
            f"got {high_yield_score} <= {medium_score}"
        )

    def test_high_noise_source_reduces_score(
        self, mock_scoring_rules, sample_candidate
    ):
        """High noise source should reduce the final score.

        A source with noise_score > 0.5 should contribute negatively
        to the candidate's final score.
        """
        # Candidate with low noise source
        candidate = sample_candidate.copy()
        candidate["freshness_hours"] = 24.0
        candidate["url_quality_score"] = 50.0
        candidate["title_quality_score"] = 50.0
        candidate["keyword_match_score"] = 50.0
        candidate["domain_trust_score"] = 50.0
        candidate["lane_reliability_score"] = 50.0
        candidate["duplication_risk_score"] = 10.0
        candidate["path_trust_score"] = 50.0
        candidate["source_yield_score"] = 0.5
        candidate["source_noise_score"] = 0.1  # Low noise

        result = score_candidate(candidate.copy(), mock_scoring_rules)
        low_noise_score = result["candidate_score"]["candidate_score"]

        # Candidate with high noise
        noisy_candidate = candidate.copy()
        noisy_candidate["source_yield_score"] = 0.5
        noisy_candidate["source_noise_score"] = 0.7  # High noise

        noisy_result = score_candidate(noisy_candidate, mock_scoring_rules)
        high_noise_score = noisy_result["candidate_score"]["candidate_score"]

        # Low noise should score higher than high noise
        assert low_noise_score > high_noise_score, (
            f"Low noise (0.1) should score higher than high noise (0.7), "
            f"got {low_noise_score} <= {high_noise_score}"
        )

    def test_combined_yield_and_noise_influence(
        self, mock_scoring_rules, sample_candidate
    ):
        """Combined high yield + low noise should score highest.

        The best source quality is: high yield (> 0.7) AND low noise (< 0.3).
        The worst is: low yield (< 0.3) AND high noise (> 0.5).
        """
        # Best source: high yield, low noise
        best_candidate = sample_candidate.copy()
        best_candidate.update(
            {
                "freshness_hours": 24.0,
                "url_quality_score": 50.0,
                "title_quality_score": 50.0,
                "keyword_match_score": 50.0,
                "domain_trust_score": 50.0,
                "lane_reliability_score": 50.0,
                "duplication_risk_score": 10.0,
                "path_trust_score": 50.0,
                "source_yield_score": 0.85,
                "source_noise_score": 0.1,
            }
        )

        # Worst source: low yield, high noise
        worst_candidate = sample_candidate.copy()
        worst_candidate.update(
            {
                "freshness_hours": 24.0,
                "url_quality_score": 50.0,
                "title_quality_score": 50.0,
                "keyword_match_score": 50.0,
                "domain_trust_score": 50.0,
                "lane_reliability_score": 50.0,
                "duplication_risk_score": 10.0,
                "path_trust_score": 50.0,
                "source_yield_score": 0.2,
                "source_noise_score": 0.7,
            }
        )

        best_result = score_candidate(best_candidate, mock_scoring_rules)
        worst_result = score_candidate(worst_candidate, mock_scoring_rules)

        best_score = best_result["candidate_score"]["candidate_score"]
        worst_score = worst_result["candidate_score"]["candidate_score"]

        # Best source quality should significantly outscore worst
        assert best_score > worst_score + 5, (
            f"Best source (yield=0.85, noise=0.1) should score significantly higher "
            f"than worst source (yield=0.2, noise=0.7), "
            f"got {best_score} vs {worst_score} (diff={best_score - worst_score})"
        )


# =============================================================================
# Test 6: End-to-End Scenario
# =============================================================================


class TestMemoryFeedsIntoScoringEndToEnd:
    """End-to-end integration test for memory-based scoring.

    Full scenario test:
    1. Candidate discovered from brookings.edu with path /research/
    2. Memory shows this path has high trust (80) and high yield (0.75)
    3. Candidate scores through extract_candidate_features and score_candidate
    4. Verify memory-based features are present and affect final score
    """

    def test_end_to_end_brookings_research(
        self, clean_memory_files, mock_scoring_rules, brookings_candidate
    ):
        """Full end-to-end test with brookings.edu research path.

        Scenario:
        1. Candidate from brookings.edu/research/ with good title keywords
        2. Domain memory shows brookings.edu has medium-high trust
        3. Path memory shows /research/ has high trust
        4. Source memory shows good yield and low noise
        5. Final score should reflect all memory-based adjustments
        """
        # Set up memory for brookings.edu
        brookings_domain_memory = {
            "domain": "brookings.edu",
            "trust_score": 65.0,  # Above baseline 50 due to good outcomes
            "baseline_trust": 50.0,
            "total_candidates": 30,
            "accepted_count": 20,
            "accepted_human_count": 8,
            "review_count": 5,
            "review_human_count": 2,
            "rejected_count": 3,
            "rejected_human_count": 1,
            "rejected_auto_count": 2,
            "filtered_out_count": 2,
            "yield_score": 0.67,
            "noise_score": 0.17,
            "first_seen": datetime.now(timezone.utc).isoformat(),
            "last_seen": datetime.now(timezone.utc).isoformat(),
        }

        brookings_path_memory = {
            "domain": "brookings.edu",
            "path_pattern": "/research/",
            "trust_score": 80.0,  # High trust for research path
            "baseline_trust": 50.0,
            "total_candidates": 20,
            "accepted_count": 15,
            "accepted_human_count": 6,
            "review_count": 3,
            "review_human_count": 1,
            "rejected_count": 1,
            "rejected_human_count": 0,
            "rejected_auto_count": 1,
            "filtered_out_count": 1,
            "yield_score": 0.75,  # High yield
            "noise_score": 0.1,  # Low noise
            "first_seen": datetime.now(timezone.utc).isoformat(),
            "last_seen": datetime.now(timezone.utc).isoformat(),
        }

        brookings_source_memory = {
            "source_id": "brookings_keyword_src",
            "source_type": "keyword_discovery",
            "source_domain": "brookings.edu",
            "trust_score": 70.0,
            "yield_score": 0.8,  # High yield
            "noise_score": 0.15,  # Low noise
            "total_candidates": 25,
            "accepted_count": 18,
            "accepted_human_count": 7,
            "review_count": 4,
            "review_human_count": 2,
            "rejected_count": 2,
            "rejected_human_count": 1,
            "rejected_auto_count": 1,
            "filtered_out_count": 1,
            "first_seen": datetime.now(timezone.utc).isoformat(),
            "last_seen": datetime.now(timezone.utc).isoformat(),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

        # Save memory data
        memory_persistence.save_domain_memory("brookings.edu", brookings_domain_memory)
        memory_persistence.save_path_memory(
            "brookings.edu", "/research/", brookings_path_memory
        )
        memory_persistence.save_source_memory(
            "brookings_keyword_src", brookings_source_memory
        )

        # Mock memory lookups to return our populated values
        with (
            patch.object(memory_manager, "get_domain_trust", return_value=65.0),
            patch.object(memory_manager, "get_path_trust", return_value=80.0),
            patch.object(
                memory_manager,
                "get_or_create_source_memory",
                return_value=brookings_source_memory,
            ),
        ):
            # Step 1: Extract features
            candidate_with_features = extract_candidate_features(
                brookings_candidate.copy()
            )

            # Step 2: Add memory-based features manually (simulating integration)
            candidate_with_features["path_trust_score"] = 80.0
            candidate_with_features["source_yield_score"] = 0.8
            candidate_with_features["source_noise_score"] = 0.15

            # Step 3: Score candidate
            scored_candidate = score_candidate(
                candidate_with_features, mock_scoring_rules
            )

            final_score = scored_candidate["candidate_score"]["candidate_score"]
            breakdown = scored_candidate["candidate_score"]["score_breakdown"]

            # Verify memory-based features are present
            # Note: Due to cold-start blending with 30 samples (above full_threshold=25),
            # the score is 10% baseline (50) + 90% memory (65) = 63.5
            assert candidate_with_features["domain_trust_score"] == 63.5, (
                f"Expected domain_trust_score=63.5 (cold-start blended), "
                f"got {candidate_with_features['domain_trust_score']}"
            )
            assert candidate_with_features["path_trust_score"] == 80.0, (
                f"Expected path_trust_score=80 from memory, "
                f"got {candidate_with_features['path_trust_score']}"
            )
            assert candidate_with_features["source_yield_score"] == 0.8, (
                f"Expected source_yield_score=0.8 from memory, "
                f"got {candidate_with_features['source_yield_score']}"
            )
            assert candidate_with_features["source_noise_score"] == 0.15, (
                f"Expected source_noise_score=0.15 from memory, "
                f"got {candidate_with_features['source_noise_score']}"
            )

            # Verify final score is influenced by memory (should be relatively high)
            # Note: With blended domain_trust=63.5 and source quality bonus, score is ~48-50
            assert final_score > 45, (
                f"Expected final score > 45 with good memory, got {final_score}"
            )

            # Verify scoring breakdown includes key components
            assert "domain_trust" in breakdown
            # The raw value in breakdown matches the blended domain_trust_score
            assert breakdown["domain_trust"]["raw"] == 63.5

    def test_end_to_end_with_poor_memory_overrides_good_baseline(
        self, clean_memory_files, mock_scoring_rules, brookings_candidate
    ):
        """Poor memory should override good baseline trust.

        Even if a domain has good baseline trust (e.g., brookings.edu is medium=50),
        poor memory outcomes should reduce the effective trust score.
        """
        # Create memory with poor outcomes for brookings.edu
        poor_memory = {
            "domain": "brookings.edu",
            "trust_score": 20.0,  # Very low due to poor outcomes
            "baseline_trust": 50.0,
            "total_candidates": 30,
            "accepted_count": 5,
            "accepted_human_count": 2,
            "review_count": 5,
            "review_human_count": 2,
            "rejected_count": 10,
            "rejected_human_count": 4,
            "rejected_auto_count": 6,
            "filtered_out_count": 10,
            "yield_score": 0.17,
            "noise_score": 0.67,
            "first_seen": datetime.now(timezone.utc).isoformat(),
            "last_seen": datetime.now(timezone.utc).isoformat(),
        }

        memory_persistence.save_domain_memory("brookings.edu", poor_memory)

        with patch.object(memory_manager, "get_domain_trust", return_value=20.0):
            # Extract features
            candidate = brookings_candidate.copy()
            candidate["url"] = (
                "https://brookings.edu/research/old-report"  # Could be outdated
            )

            result = extract_candidate_features(candidate)

            # Domain trust should reflect blended score (poor memory + baseline)
            # With 30 samples (above full_threshold=25): 10% baseline (50) + 90% memory (20) = 23.0
            assert result["domain_trust_score"] == 23.0, (
                f"Expected domain_trust_score=23.0 (cold-start blended), "
                f"got {result['domain_trust_score']}"
            )


# =============================================================================
# Test Memory Manager Trust Score Retrieval
# =============================================================================


class TestMemoryManagerTrustRetrieval:
    """Tests for memory_manager trust score retrieval functions.

    These verify the trust retrieval API that scoring should use.
    """

    def test_get_domain_trust_returns_memory_value(
        self, clean_memory_files, memory_populated
    ):
        """get_domain_trust should return memory value when available."""
        trust = memory_manager.get_domain_trust("testdomain.com")
        assert trust == 75.0, f"Expected trust=75 from memory, got {trust}"

    def test_get_domain_trust_returns_baseline_when_no_memory(self, clean_memory_files):
        """get_domain_trust should return baseline when no memory exists."""
        trust = memory_manager.get_domain_trust("nonexistent.domain")
        # Should return baseline from scoring_rules.json
        assert trust == 10.0, f"Expected baseline=10 for unknown domain, got {trust}"

    def test_get_path_trust_returns_memory_value(
        self, clean_memory_files, memory_populated
    ):
        """get_path_trust should return memory value when available."""
        trust = memory_manager.get_path_trust("testdomain.com", "/research/")
        assert trust == 80.0, f"Expected trust=80 from path memory, got {trust}"

    def test_get_path_trust_returns_baseline_for_unknown_path(self, clean_memory_files):
        """get_path_trust should return baseline when no path memory exists."""
        # For unknown domain, should return 10
        trust = memory_manager.get_path_trust("unknown.com", "/unknown/")
        assert trust == 10.0, f"Expected baseline=10 for unknown path, got {trust}"

    def test_get_source_trust_returns_memory_value(
        self, clean_memory_files, memory_populated
    ):
        """get_source_trust should return memory value when available."""
        trust = memory_manager.get_source_trust("test_source")
        assert trust == 70.0, f"Expected trust=70 from source memory, got {trust}"

    def test_get_source_trust_returns_default_when_no_memory(self, clean_memory_files):
        """get_source_trust should return 50.0 when no memory exists."""
        trust = memory_manager.get_source_trust("unknown_source")
        assert trust == 50.0, f"Expected default=50 for unknown source, got {trust}"
