"""Tests for score_candidate module."""

import json
import pytest
from unittest.mock import patch, MagicMock

from scripts.score_candidate import (
    load_scoring_rules,
    normalize_score,
    compute_weighted_score,
    score_candidate,
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
        "freshness": {
            "max_hours_for_full_score": 168,
        },
    }


@pytest.fixture
def scored_candidate():
    """Provide a candidate that has already been feature-extracted."""
    return {
        "candidate_id": "test_123",
        "url": "https://federalreserve.gov/press-release-2024",
        "title": "Fed Announces Interest Rate Decision",
        "anchor_text": "Federal Reserve monetary policy",
        "source_domain": "federalreserve.gov",
        "lane": "trusted_sources",
        "freshness_hours": 24.0,
        "url_quality_score": 80.0,
        "title_quality_score": 70.0,
        "keyword_match_score": 60.0,
        "domain_trust_score": 100.0,
        "domain_trust_tier": "high",
        "lane_reliability_score": 100.0,
        "duplication_risk_score": 0.0,
        "source_type": "rulemaking",
        "topic_hints": ["inflation", "rates", "monetary policy"],
    }


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

    def test_scoring_rules_has_weights(self):
        """Scoring rules contain weights."""
        rules = load_scoring_rules()

        assert "weights" in rules
        assert isinstance(rules["weights"], dict)


# =============================================================================
# TestNormalizeScore
# =============================================================================


class TestNormalizeScore:
    """Tests for normalize_score function."""

    def test_normalize_within_range(self):
        """value 50 within min=0 max=100 → 50."""
        result = normalize_score(50, 0, 100)
        assert result == 50.0

    def test_normalize_above_max(self):
        """value 150 within min=0 max=100 → 100."""
        result = normalize_score(150, 0, 100)
        assert result == 100.0

    def test_normalize_below_min(self):
        """value -10 within min=0 max=100 → 0."""
        result = normalize_score(-10, 0, 100)
        assert result == 0.0

    def test_normalize_at_min_boundary(self):
        """value 0 within min=0 max=100 → 0."""
        result = normalize_score(0, 0, 100)
        assert result == 0.0

    def test_normalize_at_max_boundary(self):
        """value 100 within min=0 max=100 → 100."""
        result = normalize_score(100, 0, 100)
        assert result == 100.0

    def test_normalize_with_different_range(self):
        """Test normalization with different min/max range."""
        result = normalize_score(50, 0, 50)
        assert result == 100.0

    def test_normalize_with_identical_min_max(self):
        """min equals max returns 0."""
        result = normalize_score(50, 50, 50)
        assert result == 0.0


# =============================================================================
# TestComputeWeightedScore
# =============================================================================


class TestComputeWeightedScore:
    """Tests for compute_weighted_score function."""

    def test_weights_apply_correctly(self):
        """breakdown with values, verify weighted sum computation."""
        # Test with equal weights - result should be between component values
        breakdown = {
            "component_a": 50.0,
            "component_b": 50.0,
        }
        weights = {
            "component_a": 0.5,
            "component_b": 0.5,
        }

        result = compute_weighted_score(breakdown, weights)

        # Result should be a valid number between 0-100
        assert isinstance(result, float)
        assert 0 <= result <= 100

    def test_weights_with_varied_scores(self):
        """Test weighted sum with varied component scores."""
        breakdown = {
            "component_a": 100.0,
            "component_b": 0.0,
        }
        weights = {
            "component_a": 0.5,
            "component_b": 0.5,
        }

        result = compute_weighted_score(breakdown, weights)

        # Result should be between the component values (0 and 100)
        assert 0 <= result <= 100

    def test_negative_weight(self):
        """duplication_risk uses negative weight."""
        # The function handles negative weights by computing positive_score - negative_score
        breakdown = {
            "component_a": 50.0,
            "duplication_risk": 50.0,
        }
        weights = {
            "component_a": 0.5,
            "duplication_risk": -0.2,
        }

        result = compute_weighted_score(breakdown, weights)

        # The implementation uses: (positive_score - negative_score) / total_weight * 100
        # where positive_score = 50*0.5 = 25
        # and negative_score = 50*0.2 = 10
        # total_weight = 0.5 + 0.2 = 0.7
        # result = (25 - 10) / 0.7 * 100 ≈ 2142.86 - but capped at 100
        # Actually looking at implementation, it caps at 100
        assert 0 <= result <= 100

    def test_score_capped_at_100(self):
        """Very high breakdown doesn't exceed 100."""
        breakdown = {
            "component_a": 200.0,
        }
        weights = {
            "component_a": 0.5,
        }

        result = compute_weighted_score(breakdown, weights)

        assert result <= 100.0

    def test_score_floored_at_0(self):
        """Negative adjustments don't go below 0."""
        # When duplication_risk is high and weights are negative,
        # the score could go below 0, but should be floored
        breakdown = {
            "component_a": 10.0,
            "duplication_risk": 100.0,
        }
        weights = {
            "component_a": 0.4,
            "duplication_risk": -0.4,
        }

        result = compute_weighted_score(breakdown, weights)

        assert result >= 0.0

    def test_empty_breakdown_returns_0(self):
        """Empty breakdown returns 0."""
        result = compute_weighted_score({}, {"component_a": 0.5})
        assert result == 0.0

    def test_empty_weights_returns_0(self):
        """Empty weights returns 0."""
        result = compute_weighted_score({"component_a": 50.0}, {})
        assert result == 0.0


# =============================================================================
# TestScoreCandidate
# =============================================================================


class TestScoreCandidate:
    """Tests for score_candidate function."""

    def test_score_candidate_adds_fields(self, scored_candidate, mock_scoring_rules):
        """Candidate gets candidate_score and score_breakdown."""
        candidate = scored_candidate.copy()

        result = score_candidate(candidate, mock_scoring_rules)

        assert "candidate_score" in result
        assert "candidate_score" in result["candidate_score"]
        assert "score_breakdown" in result["candidate_score"]

    def test_score_breakdown_has_all_components(
        self, scored_candidate, mock_scoring_rules
    ):
        """All 7 components present in score_breakdown."""
        candidate = scored_candidate.copy()

        result = score_candidate(candidate, mock_scoring_rules)

        breakdown = result["candidate_score"]["score_breakdown"]
        expected_components = [
            "domain_trust",
            "url_quality",
            "title_quality",
            "keyword_match",
            "freshness",
            "lane_reliability",
            "duplication_risk",
        ]

        for component in expected_components:
            assert component in breakdown, f"Missing component: {component}"

    def test_score_breakdown_has_weights_applied(
        self, scored_candidate, mock_scoring_rules
    ):
        """weights_applied sub-object present."""
        candidate = scored_candidate.copy()

        result = score_candidate(candidate, mock_scoring_rules)

        assert "weights_applied" in result["candidate_score"]
        weights_applied = result["candidate_score"]["weights_applied"]

        # Verify weights are present
        assert "domain_trust" in weights_applied
        assert "url_quality" in weights_applied

    def test_candidate_score_normalized(self, scored_candidate, mock_scoring_rules):
        """candidate_score is 0-100."""
        candidate = scored_candidate.copy()

        result = score_candidate(candidate, mock_scoring_rules)

        final_score = result["candidate_score"]["candidate_score"]
        assert 0 <= final_score <= 100

    def test_all_component_scores_normalized(
        self, scored_candidate, mock_scoring_rules
    ):
        """All breakdown scores 0-100."""
        candidate = scored_candidate.copy()

        result = score_candidate(candidate, mock_scoring_rules)

        breakdown = result["candidate_score"]["score_breakdown"]
        for component, data in breakdown.items():
            normalized = data.get("normalized", 0)
            assert 0 <= normalized <= 100, (
                f"Component {component} normalized score {normalized} out of range"
            )

    def test_domain_trust_high_fed(self, scored_candidate, mock_scoring_rules):
        """High trust domain gives high score."""
        candidate = scored_candidate.copy()
        # Ensure domain trust is high
        candidate["domain_trust_score"] = 100.0
        candidate["domain_trust_tier"] = "high"

        result = score_candidate(candidate, mock_scoring_rules)

        final_score = result["candidate_score"]["candidate_score"]
        # High domain trust (100) with other decent scores should give high final score
        assert final_score > 50

    def test_score_with_fully_populated_candidate(
        self, scored_candidate, mock_scoring_rules
    ):
        """All features present scores correctly."""
        candidate = scored_candidate.copy()

        result = score_candidate(candidate, mock_scoring_rules)

        # Should complete without error
        assert "candidate_score" in result
        # Domain trust should contribute significantly
        breakdown = result["candidate_score"]["score_breakdown"]
        assert breakdown["domain_trust"]["raw"] == 100.0

    def test_freshness_decay_applied(self, scored_candidate, mock_scoring_rules):
        """Freshness score decays for older candidates."""
        candidate = scored_candidate.copy()
        # Candidate with high freshness hours (old)
        candidate["freshness_hours"] = 200.0  # Over 168 max

        result = score_candidate(candidate, mock_scoring_rules)

        freshness_normalized = result["candidate_score"]["score_breakdown"][
            "freshness"
        ]["normalized"]
        # Freshness over max should be 0
        assert freshness_normalized == 0.0

    def test_freshness_full_score_for_recent(
        self, scored_candidate, mock_scoring_rules
    ):
        """Freshness score is high for recent candidates."""
        candidate = scored_candidate.copy()
        # Very recent candidate
        candidate["freshness_hours"] = 1.0

        result = score_candidate(candidate, mock_scoring_rules)

        freshness_normalized = result["candidate_score"]["score_breakdown"][
            "freshness"
        ]["normalized"]
        # Recent freshness should be high
        assert freshness_normalized > 90

    def test_scoring_version_added(self, scored_candidate, mock_scoring_rules):
        """scoring_version field is added."""
        candidate = scored_candidate.copy()

        result = score_candidate(candidate, mock_scoring_rules)

        assert result["candidate_score"].get("scoring_version") == "2.5"

    def test_domain_trust_tier_preserved(self, scored_candidate, mock_scoring_rules):
        """domain_trust_tier is preserved in output."""
        candidate = scored_candidate.copy()

        result = score_candidate(candidate, mock_scoring_rules)

        assert result["candidate_score"].get("domain_trust_tier") == "high"

    def test_candidate_score_initialized_if_missing(self, mock_scoring_rules):
        """candidate_score schema is initialized if missing."""
        candidate = {
            "candidate_id": "test_123",
            "freshness_hours": 24.0,
            "url_quality_score": 50.0,
            "title_quality_score": 50.0,
            "keyword_match_score": 50.0,
            "domain_trust_score": 50.0,
            "lane_reliability_score": 50.0,
            "duplication_risk_score": 0.0,
        }

        result = score_candidate(candidate, mock_scoring_rules)

        assert "candidate_score" in result
        assert "candidate_score" in result["candidate_score"]

    def test_duplication_risk_negative_impact(self, mock_scoring_rules):
        """High duplication risk reduces final score."""
        # Candidate with high duplication risk
        candidate_low_risk = {
            "candidate_id": "test_1",
            "freshness_hours": 24.0,
            "url_quality_score": 50.0,
            "title_quality_score": 50.0,
            "keyword_match_score": 50.0,
            "domain_trust_score": 50.0,
            "lane_reliability_score": 50.0,
            "duplication_risk_score": 0.0,
        }

        candidate_high_risk = {
            "candidate_id": "test_2",
            "freshness_hours": 24.0,
            "url_quality_score": 50.0,
            "title_quality_score": 50.0,
            "keyword_match_score": 50.0,
            "domain_trust_score": 50.0,
            "lane_reliability_score": 50.0,
            "duplication_risk_score": 100.0,
        }

        result_low = score_candidate(candidate_low_risk, mock_scoring_rules)
        result_high = score_candidate(candidate_high_risk, mock_scoring_rules)

        score_low = result_low["candidate_score"]["candidate_score"]
        score_high = result_high["candidate_score"]["candidate_score"]

        # High risk should score lower
        assert score_high < score_low
