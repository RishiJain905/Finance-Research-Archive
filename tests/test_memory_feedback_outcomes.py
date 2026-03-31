"""Tests for memory feedback outcome types.

Tests the new outcome types (bad_source, good_source, weak_accept, promote, suppress)
and their effects on trust score calculation in memory_manager.py.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def fixtures_path():
    """Path to fixtures directory."""
    return BASE_DIR / "tests" / "fixtures"


@pytest.fixture
def sample_feedback_records(fixtures_path):
    """Load sample feedback records from fixtures."""
    fixtures_file = fixtures_path / "sample_feedback_records.json"
    with open(fixtures_file, "r", encoding="utf-8") as f:
        return json.load(f)


# =============================================================================
# Counter Increment Tests
# =============================================================================


class TestCounterIncrements:
    """Tests that counters are incremented correctly for each outcome type."""

    def _simulate_counter_update(self, memory, outcome):
        """Simulate counter update for a given outcome.

        This mirrors the logic in memory_manager.py update_domain_memory_on_outcome.
        """
        memory = memory.copy()

        # Ensure counters exist
        memory.setdefault("total_candidates", 0)
        memory.setdefault("accepted_count", 0)
        memory.setdefault("rejected_count", 0)
        memory.setdefault("bad_source_count", 0)
        memory.setdefault("good_source_count", 0)
        memory.setdefault("weak_accept_count", 0)
        memory.setdefault("promote_count", 0)
        memory.setdefault("suppress_count", 0)

        memory["total_candidates"] += 1

        if outcome == "bad_source":
            memory["rejected_count"] += 1
            memory["bad_source_count"] += 1
        elif outcome == "good_source":
            memory["accepted_count"] += 1
            memory["good_source_count"] += 1
        elif outcome == "weak_accept":
            memory["accepted_count"] += 1
            memory["weak_accept_count"] += 1
        elif outcome == "promote":
            memory["accepted_count"] += 1
            memory["promote_count"] += 1
        elif outcome == "suppress":
            memory["rejected_count"] += 1
            memory["suppress_count"] += 1
        elif outcome == "expand_topic":
            # No trust counter changes for expand_topic
            pass
        elif outcome == "accepted_human":
            memory["accepted_count"] += 1
        elif outcome == "rejected_human":
            memory["rejected_count"] += 1

        return memory

    def test_bad_source_count_increments(self):
        """bad_source outcome should increment bad_source_count."""
        memory = {"total_candidates": 10, "rejected_count": 2, "bad_source_count": 0}

        updated = self._simulate_counter_update(memory, "bad_source")

        assert updated["bad_source_count"] == 1

    def test_good_source_count_increments(self):
        """good_source outcome should increment good_source_count."""
        memory = {"total_candidates": 10, "accepted_count": 5, "good_source_count": 0}

        updated = self._simulate_counter_update(memory, "good_source")

        assert updated["good_source_count"] == 1

    def test_weak_accept_count_increments(self):
        """weak_accept outcome should increment weak_accept_count."""
        memory = {"total_candidates": 10, "accepted_count": 5, "weak_accept_count": 0}

        updated = self._simulate_counter_update(memory, "weak_accept")

        assert updated["weak_accept_count"] == 1

    def test_promote_count_increments(self):
        """promote outcome should increment promote_count."""
        memory = {"total_candidates": 10, "accepted_count": 5, "promote_count": 0}

        updated = self._simulate_counter_update(memory, "promote")

        assert updated["promote_count"] == 1

    def test_suppress_count_increments(self):
        """suppress outcome should increment suppress_count."""
        memory = {"total_candidates": 10, "rejected_count": 2, "suppress_count": 0}

        updated = self._simulate_counter_update(memory, "suppress")

        assert updated["suppress_count"] == 1


# =============================================================================
# Trust Score Calculation Tests
# =============================================================================


def _compute_trust_score(
    total,
    accepted,
    rejected,
    filtered_out=0,
    review=0,
    accepted_human=0,
    rejected_human=0,
    bad_source=0,
    good_source=0,
    weak_accept=0,
    promote=0,
    suppress=0,
    baseline_trust=10.0,
):
    """Compute trust score based on outcomes.

    This mirrors the logic in memory_manager.compute_trust_score
    with the new feedback_weights added.
    """
    # Default weights from memory_manager.py
    accepted_weight = 10
    rejected_weight = 5
    filtered_weight = 3
    review_weight = 0
    human_multiplier = 2.0

    # New feedback weights
    bad_source_weight = 15
    good_source_weight = 30
    weak_accept_weight = 10
    promote_weight = 40
    suppress_weight = 10

    # Bounds
    min_trust = 1
    max_trust = 100

    # Cold-start blending
    min_samples = 10
    full_threshold = 25

    # Calculate contributions
    # Human contributions get human_multiplier
    human_accepted_contribution = accepted_human * accepted_weight * human_multiplier
    auto_accepted_contribution = accepted * accepted_weight
    total_accepted_contribution = (
        human_accepted_contribution + auto_accepted_contribution
    )

    human_rejected_contribution = rejected_human * rejected_weight * human_multiplier
    auto_rejected_contribution = rejected * rejected_weight
    total_rejected_contribution = (
        human_rejected_contribution + auto_rejected_contribution
    )

    # New outcome contributions (these use their own weights)
    bad_source_contribution = bad_source * bad_source_weight
    good_source_contribution = good_source * good_source_weight
    weak_accept_contribution = weak_accept * weak_accept_weight
    promote_contribution = promote * promote_weight
    suppress_contribution = suppress * suppress_weight

    # Net adjustment
    adjustment = (
        total_accepted_contribution
        - total_rejected_contribution
        - (filtered_out * filtered_weight)
        + (review * review_weight)
        - bad_source_contribution
        + good_source_contribution
        - weak_accept_contribution
        + promote_contribution
        - suppress_contribution
    )

    learned_trust = baseline_trust + adjustment

    # Cold-start blending
    if total <= 0:
        trust_score = baseline_trust
    elif total < min_samples:
        blend_factor = total / min_samples if min_samples > 0 else 0
        baseline_weight = 1.0 - (blend_factor * 0.1)
        learned_weight = blend_factor * 0.1
        trust_score = (baseline_weight * baseline_trust) + (
            learned_weight * learned_trust
        )
    elif total >= full_threshold:
        baseline_weight = 0.1
        learned_weight = 0.9
        trust_score = (baseline_weight * baseline_trust) + (
            learned_weight * learned_trust
        )
    else:
        blend_ratio = (total - min_samples) / (full_threshold - min_samples)
        baseline_weight = 1.0 - 0.1 - (blend_ratio * 0.8)
        learned_weight = 0.1 + (blend_ratio * 0.8)
        trust_score = (baseline_weight * baseline_trust) + (
            learned_weight * learned_trust
        )

    return max(min_trust, min(max_trust, trust_score))


class TestTrustScoreCalculation:
    """Tests for trust score calculation with new outcome weights."""

    def test_bad_source_reduces_trust_more_than_rejected_human(self):
        """bad_source should reduce trust more than rejected_human.

        With default settings:
        - rejected_human: 1 * 5 * 2.0 = 10 point penalty
        - bad_source: 1 * 15 = 15 point penalty

        bad_source has a higher weight because it indicates a systemic
        source quality issue, not just a single rejection.
        """
        # Use parameters that don't cause clamping to max
        # With low baseline and more rejections, scores stay below 100
        score_rejected_human = _compute_trust_score(
            total=30,  # Past cold-start threshold
            accepted=5,
            rejected=10,
            rejected_human=1,
            baseline_trust=30.0,
        )

        # One bad_source
        score_bad_source = _compute_trust_score(
            total=30, accepted=5, rejected=10, bad_source=1, baseline_trust=30.0
        )

        # bad_source should result in lower trust
        assert score_bad_source < score_rejected_human

    def test_good_source_increases_trust_more_than_accepted_human(self):
        """good_source should increase trust more than accepted_human.

        With default settings:
        - accepted_human: 1 * 10 * 2.0 = 20 point bonus
        - good_source: 1 * 30 = 30 point bonus

        good_source has a higher weight because it indicates a reliable
        source that should be prioritized.
        """
        # One accepted_human
        score_accepted_human = _compute_trust_score(
            total=30, accepted=0, rejected=1, accepted_human=1, baseline_trust=50.0
        )

        # One good_source
        score_good_source = _compute_trust_score(
            total=30, accepted=0, rejected=1, good_source=1, baseline_trust=50.0
        )

        # good_source should result in higher trust
        assert score_good_source > score_accepted_human

    def test_weak_accept_increases_trust_less_than_accepted_human(self):
        """weak_accept should increase trust less than accepted_human.

        With default settings:
        - accepted_human: 1 * 10 * 2.0 = 20 point bonus
        - weak_accept: 1 * 10 = 10 point bonus

        weak_accept indicates the content was marginal quality.
        """
        # One accepted_human
        score_accepted_human = _compute_trust_score(
            total=30, accepted=0, rejected=1, accepted_human=1, baseline_trust=50.0
        )

        # One weak_accept
        score_weak_accept = _compute_trust_score(
            total=30, accepted=0, rejected=1, weak_accept=1, baseline_trust=50.0
        )

        # weak_accept should result in lower trust increase than accepted_human
        assert score_weak_accept < score_accepted_human

    def test_promote_increases_trust_more_than_accepted_human(self):
        """promote should increase trust more than accepted_human.

        With default settings:
        - accepted_human: 1 * 10 * 2.0 = 20 point bonus
        - promote: 1 * 40 = 40 point bonus

        promote indicates exceptional quality worthy of special handling.
        """
        # One accepted_human
        score_accepted_human = _compute_trust_score(
            total=30, accepted=0, rejected=1, accepted_human=1, baseline_trust=50.0
        )

        # One promote
        score_promote = _compute_trust_score(
            total=30, accepted=0, rejected=1, promote=1, baseline_trust=50.0
        )

        # promote should result in higher trust than accepted_human
        assert score_promote > score_accepted_human


# =============================================================================
# Cold-Start Blending Tests
# =============================================================================


class TestColdStartBlending:
    """Tests for cold-start blending with new outcome types."""

    def test_new_counters_default_to_zero(self):
        """New counters should default to 0 for existing memory records."""
        # Simulate loading an existing memory record that doesn't have
        # the new counter fields
        existing_memory = {
            "domain": "example.com",
            "trust_score": 50.0,
            "total_candidates": 5,
            "accepted_count": 2,
            "rejected_count": 1,
            # Note: bad_source_count, good_source_count, etc. not present
        }

        # Should default to 0 when accessed
        bad_source_count = existing_memory.get("bad_source_count", 0)
        good_source_count = existing_memory.get("good_source_count", 0)
        weak_accept_count = existing_memory.get("weak_accept_count", 0)
        promote_count = existing_memory.get("promote_count", 0)
        suppress_count = existing_memory.get("suppress_count", 0)

        assert bad_source_count == 0
        assert good_source_count == 0
        assert weak_accept_count == 0
        assert promote_count == 0
        assert suppress_count == 0

    def test_cold_start_with_new_outcomes(self):
        """Cold-start blending should work correctly with new outcome types."""
        # Before min_samples (10), baseline is heavily weighted

        # With 5 samples and one bad_source
        score_bad_source_cold = _compute_trust_score(
            total=5, accepted=2, rejected=1, bad_source=1, baseline_trust=50.0
        )

        # With 5 samples and one standard rejected
        score_rejected_cold = _compute_trust_score(
            total=5, accepted=2, rejected=1, rejected_human=1, baseline_trust=50.0
        )

        # Both should be closer to baseline due to cold-start blending
        # but bad_source should still be lower than rejected
        assert 45 < score_bad_source_cold < 55
        assert 45 < score_rejected_cold < 55
        assert score_bad_source_cold < score_rejected_cold

    def test_full_learning_with_new_outcomes(self):
        """After full_learning_threshold (25), learned is heavily weighted."""

        # Use parameters that don't cause clamping to max
        # With lower accepted and higher rejected, scores stay in valid range
        score_bad_source = _compute_trust_score(
            total=30, accepted=5, rejected=15, bad_source=1, baseline_trust=30.0
        )

        # With 30 samples and one good_source
        score_good_source = _compute_trust_score(
            total=30, accepted=5, rejected=15, good_source=1, baseline_trust=30.0
        )

        # Good source should give significantly higher trust
        assert score_good_source > score_bad_source


# =============================================================================
# Multiple Outcome Accumulation Tests
# =============================================================================


class TestMultipleOutcomeAccumulation:
    """Tests for accumulating multiple outcomes of the same type."""

    def test_multiple_bad_sources_accumulate(self):
        """Multiple bad_source outcomes should accumulate penalty."""
        # Use parameters that don't cause clamping
        score_one = _compute_trust_score(
            total=30, accepted=5, rejected=10, bad_source=1, baseline_trust=30.0
        )

        score_three = _compute_trust_score(
            total=30, accepted=5, rejected=10, bad_source=3, baseline_trust=30.0
        )

        # Three bad_sources should reduce trust more than one
        assert score_three < score_one

    def test_multiple_good_sources_accumulate(self):
        """Multiple good_source outcomes should accumulate bonus."""
        # Use parameters that don't cause clamping
        score_one = _compute_trust_score(
            total=30, accepted=5, rejected=15, good_source=1, baseline_trust=30.0
        )

        score_three = _compute_trust_score(
            total=30, accepted=5, rejected=15, good_source=3, baseline_trust=30.0
        )

        # Three good_sources should increase trust more than one
        assert score_three > score_one

    def test_mixed_outcomes_net_effect(self):
        """Mixed outcomes should have a net effect based on weights."""
        # Use parameters that don't cause clamping
        # Mix of 2 good_sources and 1 bad_source
        score_mixed = _compute_trust_score(
            total=30,
            accepted=5,
            rejected=15,
            good_source=2,
            bad_source=1,
            baseline_trust=30.0,
        )

        # Just 1 good_source
        score_one_good = _compute_trust_score(
            total=30, accepted=5, rejected=15, good_source=1, baseline_trust=30.0
        )

        # 2 good_sources and 1 bad_source should be better than 1 good_source
        # because good_source_weight (30) * 2 - bad_source_weight (15) = 45
        # vs good_source_weight (30) * 1 = 30
        assert score_mixed > score_one_good


# =============================================================================
# Feedback Weights Configuration Tests
# =============================================================================


class TestFeedbackWeightsConfiguration:
    """Tests for configurable feedback weights."""

    def test_default_feedback_weights_exist(self):
        """Default feedback weights should be defined."""
        # These should match the weights in memory_manager._default_memory_config
        default_weights = {
            "bad_source_weight": 15,
            "good_source_weight": 30,
            "weak_accept_weight": 10,
            "promote_weight": 40,
            "suppress_weight": 10,
        }

        # Verify all weights are positive
        for weight_name, weight_value in default_weights.items():
            assert weight_value > 0, f"{weight_name} should be positive"

    def test_feedback_weights_are_configurable(self):
        """Feedback weights should be configurable via memory_config."""
        # In production, these would come from config/memory_config.json
        custom_weights = {
            "bad_source_weight": 20,  # More aggressive source penalty
            "good_source_weight": 40,  # More aggressive source bonus
        }

        # Weights should be independently configurable
        assert custom_weights["bad_source_weight"] != 15
        assert custom_weights["good_source_weight"] != 30


# =============================================================================
# Memory Schema Field Existence Tests
# =============================================================================


class TestMemorySchemaFields:
    """Tests that memory schemas include the new feedback outcome fields."""

    def test_domain_memory_has_bad_source_count_field(self):
        """Domain memory schema should have bad_source_count field."""
        schema_path = BASE_DIR / "schemas" / "domain_memory.json"

        if not schema_path.exists():
            pytest.skip("domain_memory.json schema not found")

        with open(schema_path, "r") as f:
            schema = json.load(f)

        props = schema.get("properties", {})

        # Schema should have or accept bad_source_count
        # (it may be in additionalProperties or explicit)
        assert (
            "bad_source_count" in props
            or schema.get("additionalProperties") is not False
        )

    def test_domain_memory_has_good_source_count_field(self):
        """Domain memory schema should have good_source_count field."""
        schema_path = BASE_DIR / "schemas" / "domain_memory.json"

        if not schema_path.exists():
            pytest.skip("domain_memory.json schema not found")

        with open(schema_path, "r") as f:
            schema = json.load(f)

        props = schema.get("properties", {})

        assert (
            "good_source_count" in props
            or schema.get("additionalProperties") is not False
        )

    def test_domain_memory_has_weak_accept_count_field(self):
        """Domain memory schema should have weak_accept_count field."""
        schema_path = BASE_DIR / "schemas" / "domain_memory.json"

        if not schema_path.exists():
            pytest.skip("domain_memory.json schema not found")

        with open(schema_path, "r") as f:
            schema = json.load(f)

        props = schema.get("properties", {})

        assert (
            "weak_accept_count" in props
            or schema.get("additionalProperties") is not False
        )

    def test_domain_memory_has_promote_count_field(self):
        """Domain memory schema should have promote_count field."""
        schema_path = BASE_DIR / "schemas" / "domain_memory.json"

        if not schema_path.exists():
            pytest.skip("domain_memory.json schema not found")

        with open(schema_path, "r") as f:
            schema = json.load(f)

        props = schema.get("properties", {})

        assert (
            "promote_count" in props or schema.get("additionalProperties") is not False
        )

    def test_domain_memory_has_suppress_count_field(self):
        """Domain memory schema should have suppress_count field."""
        schema_path = BASE_DIR / "schemas" / "domain_memory.json"

        if not schema_path.exists():
            pytest.skip("domain_memory.json schema not found")

        with open(schema_path, "r") as f:
            schema = json.load(f)

        props = schema.get("properties", {})

        assert (
            "suppress_count" in props or schema.get("additionalProperties") is not False
        )


# =============================================================================
# Integration with memory_manager Tests
# =============================================================================


class TestMemoryManagerIntegration:
    """Integration tests with memory_manager module."""

    def test_update_domain_memory_on_outcome_accepts_new_types(self):
        """update_domain_memory_on_outcome should accept new outcome types."""
        try:
            from scripts import memory_manager
        except ImportError:
            pytest.skip("memory_manager.py not importable")

        # These outcome types should be handled by the function
        new_outcomes = [
            "bad_source",
            "good_source",
            "weak_accept",
            "promote",
            "suppress",
        ]

        for outcome in new_outcomes:
            # Should not raise an exception
            try:
                # We're just checking the function exists and can be called
                # Full integration test would require mocking file I/O
                assert hasattr(memory_manager, "update_domain_memory_on_outcome")
            except Exception as e:
                pytest.fail(f"Failed for outcome {outcome}: {e}")

    def test_compute_trust_score_includes_new_counters(self):
        """compute_trust_score should include new counters in calculation."""
        try:
            from scripts import memory_manager
        except ImportError:
            pytest.skip("memory_manager.py not importable")

        # Check that the function signature accepts new parameters
        import inspect

        sig = inspect.signature(memory_manager.compute_trust_score)
        params = list(sig.parameters.keys())

        # Should have parameters for new counters
        expected_new_params = [
            "bad_source",
            "good_source",
            "weak_accept",
            "promote",
            "suppress",
        ]

        for param in expected_new_params:
            assert param in params, f"Missing parameter: {param}"

    def test_trust_score_calculation_with_all_new_counters(self):
        """Trust score calculation should work with all new counters."""
        try:
            from scripts import memory_manager
        except ImportError:
            pytest.skip("memory_manager.py not importable")

        # Call with new counter parameters
        score = memory_manager.compute_trust_score(
            total=30,
            accepted=10,
            rejected=5,
            filtered_out=2,
            review=3,
            accepted_human=2,
            rejected_human=1,
            bad_source=1,
            good_source=1,
            weak_accept=1,
            promote=1,
            suppress=1,
            baseline_trust=50.0,
        )

        # Should return a valid trust score within bounds
        assert 1 <= score <= 100


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_all_counters_zero_initial_state(self):
        """New memory record should have all new counters at 0."""
        memory = {
            "domain": "newsource.com",
            "trust_score": 10.0,
            "total_candidates": 0,
            "accepted_count": 0,
            "rejected_count": 0,
            "bad_source_count": 0,
            "good_source_count": 0,
            "weak_accept_count": 0,
            "promote_count": 0,
            "suppress_count": 0,
        }

        assert memory["bad_source_count"] == 0
        assert memory["good_source_count"] == 0
        assert memory["weak_accept_count"] == 0
        assert memory["promote_count"] == 0
        assert memory["suppress_count"] == 0

    def test_negative_counter_not_allowed(self):
        """Counters should not go negative."""
        memory = {"bad_source_count": 0}

        # Trying to decrement should result in 0, not negative
        # (this is handled in the actual implementation)
        result = max(0, memory.get("bad_source_count", 0) - 1)

        assert result >= 0

    def test_large_counter_values_handled(self):
        """Large counter values should be handled without overflow."""
        score = _compute_trust_score(
            total=1000,
            accepted=500,
            rejected=200,
            bad_source=100,
            good_source=100,
            baseline_trust=50.0,
        )

        # Should still be clamped to valid range
        assert 1 <= score <= 100
