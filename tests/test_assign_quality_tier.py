"""
Comprehensive tests for the assign_quality_tier module.
"""

import json
import tempfile
import unittest
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
import sys

sys.path.insert(0, str(BASE_DIR))

from scripts.assign_quality_tier import (
    assign_quality_tier,
    compute_tier_score,
    extract_feature_values,
    generate_reasoning,
    get_tier_label,
    load_config,
    load_domain_trust,
    normalize,
    process_batch,
    process_record_file,
)


class NormalizeFunctionTests(unittest.TestCase):
    """Test the normalize function."""

    def test_normalize_mid_value(self):
        """Normalize 5 on scale [0, 10] should return 0.5."""
        result = normalize(5, [0, 10])
        self.assertEqual(result, 0.5)

    def test_normalize_min_value(self):
        """Normalize min value should return 0.0."""
        result = normalize(0, [0, 10])
        self.assertEqual(result, 0.0)

    def test_normalize_max_value(self):
        """Normalize max value should return 1.0."""
        result = normalize(10, [0, 10])
        self.assertEqual(result, 1.0)

    def test_normalize_out_of_range_high(self):
        """Normalize value above max should clamp to 1.0."""
        result = normalize(15, [0, 10])
        self.assertEqual(result, 1.0)

    def test_normalize_out_of_range_low(self):
        """Normalize value below min should clamp to 0.0."""
        result = normalize(-5, [0, 10])
        self.assertEqual(result, 0.0)

    def test_normalize_min_equals_max_returns_zero(self):
        """Normalize when min equals max should return 0.0 (avoid division by zero)."""
        result = normalize(5, [5, 5])
        self.assertEqual(result, 0.0)

    def test_normalize_with_different_scale(self):
        """Normalize on different scale [0, 100]."""
        result = normalize(50, [0, 100])
        self.assertEqual(result, 0.5)

    def test_normalize_negative_scale(self):
        """Normalize with negative scale values."""
        result = normalize(-5, [-10, 0])
        self.assertEqual(result, 0.5)


class ExtractFeatureValuesTests(unittest.TestCase):
    """Test the extract_feature_values function."""

    def setUp(self):
        self.config = load_config()
        self.domain_trust = load_domain_trust()

    def test_extract_with_complete_record(self):
        """Extract features from a complete record."""
        record = {
            "llm_review": {
                "verification_confidence": 9,
                "issues_found": ["minor issue 1", "minor issue 2"],
            },
            "human_review": {"decision": "approved_by_human"},
            "source": {"domain": "federalreserve.gov"},
            "why_it_matters": "This matters because it explains Fed policy direction.",
            "important_numbers": [
                {"value": "5.25%", "description": "federal funds rate"}
            ],
            "topic": "monetary policy",
            "event_type": "fed_policy",
        }

        features = extract_feature_values(record, self.domain_trust)

        self.assertEqual(features["verification_confidence"], 9)
        self.assertTrue(features["human_approved"])
        self.assertEqual(features["issues_found_count"], 2)
        self.assertEqual(features["why_it_matters_quality"], 1.0)
        self.assertTrue(features["has_structured_numbers"])
        self.assertEqual(features["source_trust_score"], 100)
        self.assertEqual(features["topic"], "monetary policy")
        self.assertEqual(features["event_type"], "fed_policy")

    def test_extract_with_minimal_record(self):
        """Extract features from minimal/legacy record (missing llm_review, human_review, source)."""
        record = {
            "why_it_matters": "Short explanation.",
            "important_numbers": [],
            "topic": "market structure",
            "event_type": "yield_curve",
        }

        features = extract_feature_values(record, self.domain_trust)

        self.assertEqual(features["verification_confidence"], 0)
        self.assertFalse(features["human_approved"])
        self.assertEqual(features["issues_found_count"], 0)
        self.assertEqual(features["why_it_matters_quality"], 0.5)  # Short but not empty
        self.assertFalse(features["has_structured_numbers"])
        self.assertEqual(features["source_trust_score"], 10)  # Default low trust
        self.assertEqual(features["topic"], "market structure")
        self.assertEqual(features["event_type"], "yield_curve")

    def test_extract_with_empty_why_it_matters(self):
        """Extract features with empty why_it_matters."""
        record = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "",
            "important_numbers": [],
            "topic": "macro data",
            "event_type": "data_release",
        }

        features = extract_feature_values(record, self.domain_trust)
        self.assertEqual(features["why_it_matters_quality"], 0.0)

    def test_extract_with_whitespace_only_why_it_matters(self):
        """Extract features with whitespace-only why_it_matters."""
        record = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "   ",
            "important_numbers": [],
            "topic": "macro data",
            "event_type": "data_release",
        }

        features = extract_feature_values(record, self.domain_trust)
        self.assertEqual(features["why_it_matters_quality"], 0.0)

    def test_extract_with_short_why_it_matters(self):
        """Extract features with short why_it_matters (<=30 chars)."""
        record = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "Short explanation.",
            "important_numbers": [],
            "topic": "macro data",
            "event_type": "data_release",
        }

        features = extract_feature_values(record, self.domain_trust)
        self.assertEqual(features["why_it_matters_quality"], 0.5)

    def test_extract_with_long_why_it_matters(self):
        """Extract features with long why_it_matters (>30 chars)."""
        record = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "This is a longer explanation that clearly states why this matters for markets.",
            "important_numbers": [],
            "topic": "macro data",
            "event_type": "data_release",
        }

        features = extract_feature_values(record, self.domain_trust)
        self.assertEqual(features["why_it_matters_quality"], 1.0)

    def test_extract_with_empty_important_numbers(self):
        """Extract features with empty important_numbers."""
        record = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "Test explanation that is definitely longer than thirty characters.",
            "important_numbers": [],
            "topic": "regulation",
            "event_type": "other",
        }

        features = extract_feature_values(record, self.domain_trust)
        self.assertFalse(features["has_structured_numbers"])

    def test_extract_with_important_numbers(self):
        """Extract features with non-empty important_numbers."""
        record = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "Test explanation that is definitely longer than thirty characters.",
            "important_numbers": [{"value": "2.5%", "description": "GDP growth"}],
            "topic": "regulation",
            "event_type": "other",
        }

        features = extract_feature_values(record, self.domain_trust)
        self.assertTrue(features["has_structured_numbers"])

    def test_extract_issues_capped_at_10(self):
        """Issues count should be capped at 10."""
        record = {
            "llm_review": {
                "verification_confidence": 5,
                "issues_found": [f"issue_{i}" for i in range(15)],
            },
            "human_review": {},
            "source": {},
            "why_it_matters": "",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        features = extract_feature_values(record, self.domain_trust)
        self.assertEqual(features["issues_found_count"], 10)

    def test_extract_human_approved_alternatives(self):
        """Test human_approved recognizes 'approved' in addition to 'approved_by_human'."""
        record_approved = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {"decision": "approved"},
            "source": {},
            "why_it_matters": "",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        features = extract_feature_values(record_approved, self.domain_trust)
        self.assertTrue(features["human_approved"])

    def test_extract_domain_trust_mapping(self):
        """Test that domain trust score is correctly mapped."""
        record_high = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {},
            "source": {"domain": "federalreserve.gov"},
            "why_it_matters": "",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        record_medium = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {},
            "source": {"domain": "imf.org"},
            "why_it_matters": "",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        record_low = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {},
            "source": {"domain": "unknownsite.com"},
            "why_it_matters": "",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        features_high = extract_feature_values(record_high, self.domain_trust)
        features_medium = extract_feature_values(record_medium, self.domain_trust)
        features_low = extract_feature_values(record_low, self.domain_trust)

        self.assertEqual(features_high["source_trust_score"], 100)
        self.assertEqual(features_medium["source_trust_score"], 50)
        self.assertEqual(features_low["source_trust_score"], 10)  # default


class ComputeTierScoreTests(unittest.TestCase):
    """Test the compute_tier_score function."""

    def setUp(self):
        self.config = load_config()
        self.domain_trust = load_domain_trust()

    def test_score_returns_value_between_0_and_100(self):
        """Score should always be between 0 and 100."""
        low_record = {
            "llm_review": {"verification_confidence": 0, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        high_record = {
            "llm_review": {"verification_confidence": 10, "issues_found": []},
            "human_review": {"decision": "approved_by_human"},
            "source": {"domain": "federalreserve.gov"},
            "why_it_matters": "This is a very important explanation that clearly matters for monetary policy.",
            "important_numbers": [{"value": "5.25%", "description": "rate"}],
            "topic": "monetary policy",
            "event_type": "fed_policy",
        }

        low_features = extract_feature_values(low_record, self.domain_trust)
        high_features = extract_feature_values(high_record, self.domain_trust)

        low_score = compute_tier_score(low_features, self.config)
        high_score = compute_tier_score(high_features, self.config)

        self.assertGreaterEqual(low_score, 0)
        self.assertLessEqual(low_score, 100)
        self.assertGreaterEqual(high_score, 0)
        self.assertLessEqual(high_score, 100)

    def test_verification_confidence_contributes_to_score(self):
        """Higher verification confidence should result in higher score."""
        low_conf_record = {
            "llm_review": {"verification_confidence": 2, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        high_conf_record = {
            "llm_review": {"verification_confidence": 9, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        low_features = extract_feature_values(low_conf_record, self.domain_trust)
        high_features = extract_feature_values(high_conf_record, self.domain_trust)

        low_score = compute_tier_score(low_features, self.config)
        high_score = compute_tier_score(high_features, self.config)

        self.assertGreater(high_score, low_score)

    def test_human_approval_boosts_score(self):
        """Human approval should add to the score."""
        no_approval_record = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        with_approval_record = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {"decision": "approved_by_human"},
            "source": {},
            "why_it_matters": "",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        no_approval_features = extract_feature_values(
            no_approval_record, self.domain_trust
        )
        with_approval_features = extract_feature_values(
            with_approval_record, self.domain_trust
        )

        no_approval_score = compute_tier_score(no_approval_features, self.config)
        with_approval_score = compute_tier_score(with_approval_features, self.config)

        # The boost is: boost (10) * weight (0.15) = 1.5
        self.assertGreater(with_approval_score, no_approval_score)

    def test_inverted_issues_scoring(self):
        """Fewer issues should result in higher score."""
        many_issues_record = {
            "llm_review": {
                "verification_confidence": 5,
                "issues_found": ["i1", "i2", "i3", "i4", "i5"],
            },
            "human_review": {},
            "source": {},
            "why_it_matters": "",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        few_issues_record = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        many_features = extract_feature_values(many_issues_record, self.domain_trust)
        few_features = extract_feature_values(few_issues_record, self.domain_trust)

        many_score = compute_tier_score(many_features, self.config)
        few_score = compute_tier_score(few_features, self.config)

        self.assertGreater(few_score, many_score)


class TierLabelTests(unittest.TestCase):
    """Test the get_tier_label function."""

    def setUp(self):
        self.config = load_config()

    def test_score_0_returns_tier_3(self):
        """Score of 0 should return tier_3."""
        tier = get_tier_label(0, self.config)
        self.assertEqual(tier, "tier_3")

    def test_score_50_returns_tier_3(self):
        """Score of 50 should return tier_3."""
        tier = get_tier_label(50, self.config)
        self.assertEqual(tier, "tier_3")

    def test_score_54_returns_tier_3(self):
        """Score of 54 should return tier_3 (below tier_2 threshold)."""
        tier = get_tier_label(54, self.config)
        self.assertEqual(tier, "tier_3")

    def test_score_55_returns_tier_2(self):
        """Score of 55 should return tier_2 (at tier_2 threshold)."""
        tier = get_tier_label(55, self.config)
        self.assertEqual(tier, "tier_2")

    def test_score_60_returns_tier_2(self):
        """Score of 60 should return tier_2."""
        tier = get_tier_label(60, self.config)
        self.assertEqual(tier, "tier_2")

    def test_score_79_returns_tier_2(self):
        """Score of 79 should return tier_2 (below tier_1 threshold)."""
        tier = get_tier_label(79, self.config)
        self.assertEqual(tier, "tier_2")

    def test_score_80_returns_tier_1(self):
        """Score of 80 should return tier_1 (at tier_1 threshold)."""
        tier = get_tier_label(80, self.config)
        self.assertEqual(tier, "tier_1")

    def test_score_90_returns_tier_1(self):
        """Score of 90 should return tier_1."""
        tier = get_tier_label(90, self.config)
        self.assertEqual(tier, "tier_1")

    def test_score_100_returns_tier_1(self):
        """Score of 100 should return tier_1."""
        tier = get_tier_label(100, self.config)
        self.assertEqual(tier, "tier_1")

    def test_fallback_to_lowest_tier(self):
        """With empty tiers config, should fallback to tier_3."""
        empty_config = {"tiers": {}}
        tier = get_tier_label(50, empty_config)
        self.assertEqual(tier, "tier_3")


class GenerateReasoningTests(unittest.TestCase):
    """Test the generate_reasoning function."""

    def setUp(self):
        self.config = load_config()
        self.domain_trust = load_domain_trust()

    def test_reasoning_produces_at_least_one_reason(self):
        """generate_reasoning should produce at least 1 reason."""
        record = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "Test.",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        features = extract_feature_values(record, self.domain_trust)
        score = compute_tier_score(features, self.config)
        tier = get_tier_label(score, self.config)
        reasoning = generate_reasoning(record, features, score, tier, self.config)

        self.assertGreater(len(reasoning), 0)

    def test_reasoning_includes_score_summary(self):
        """First reasoning item should include score summary."""
        record = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "Test.",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        features = extract_feature_values(record, self.domain_trust)
        score = compute_tier_score(features, self.config)
        tier = get_tier_label(score, self.config)
        reasoning = generate_reasoning(record, features, score, tier, self.config)

        first_reason = reasoning[0]
        self.assertIn("quality score:", first_reason)
        self.assertIn(str(score), first_reason)

    def test_reasoning_reflects_high_confidence(self):
        """High verification confidence should be reflected in reasoning."""
        record = {
            "llm_review": {"verification_confidence": 9, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "Test explanation that is longer than thirty characters.",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        features = extract_feature_values(record, self.domain_trust)
        score = compute_tier_score(features, self.config)
        tier = get_tier_label(score, self.config)
        reasoning = generate_reasoning(record, features, score, tier, self.config)

        reasoning_text = " ".join(reasoning)
        self.assertIn("high verification confidence", reasoning_text)

    def test_reasoning_reflects_human_approval(self):
        """Human approval should be reflected in reasoning."""
        record = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {"decision": "approved_by_human"},
            "source": {},
            "why_it_matters": "Test explanation that is longer than thirty characters.",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        features = extract_feature_values(record, self.domain_trust)
        score = compute_tier_score(features, self.config)
        tier = get_tier_label(score, self.config)
        reasoning = generate_reasoning(record, features, score, tier, self.config)

        self.assertIn("human-approved", reasoning)

    def test_reasoning_reflects_no_issues(self):
        """No issues should be reflected in reasoning."""
        record = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "Test explanation that is longer than thirty characters.",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        features = extract_feature_values(record, self.domain_trust)
        score = compute_tier_score(features, self.config)
        tier = get_tier_label(score, self.config)
        reasoning = generate_reasoning(record, features, score, tier, self.config)

        self.assertIn("no issues found", " ".join(reasoning))

    def test_reasoning_reflects_minor_issues(self):
        """Minor issues (1-2) should be reflected in reasoning."""
        record = {
            "llm_review": {
                "verification_confidence": 5,
                "issues_found": ["issue1", "issue2"],
            },
            "human_review": {},
            "source": {},
            "why_it_matters": "Test explanation that is longer than thirty characters.",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        features = extract_feature_values(record, self.domain_trust)
        score = compute_tier_score(features, self.config)
        tier = get_tier_label(score, self.config)
        reasoning = generate_reasoning(record, features, score, tier, self.config)

        reasoning_text = " ".join(reasoning)
        self.assertIn("minor issues", reasoning_text)


class TierAssignmentIntegrationTests(unittest.TestCase):
    """Integration tests for tier assignment."""

    def setUp(self):
        self.config = load_config()
        self.domain_trust = load_domain_trust()

    def test_tier_1_assignment_high_quality_record(self):
        """Record with high quality attributes should be assigned tier_1."""
        record = {
            "llm_review": {"verification_confidence": 9, "issues_found": []},
            "human_review": {"decision": "approved_by_human"},
            "source": {"domain": "federalreserve.gov"},
            "why_it_matters": "Fed policy changes directly impact borrowing costs and market conditions across all asset classes.",
            "important_numbers": [
                {"value": "5.25%", "description": "federal funds rate"},
                {"value": "0.75%", "description": "tariff rate"},
            ],
            "topic": "monetary policy",
            "event_type": "fed_policy",
        }

        result = assign_quality_tier(
            record, self.config, domain_trust=self.domain_trust
        )
        quality_tier = result["quality_tier"]

        self.assertEqual(quality_tier["tier"], "tier_1")
        self.assertGreaterEqual(quality_tier["score"], 80)

    def test_tier_1_boundary_score(self):
        """Record scoring exactly at tier_1 boundary (80) should be tier_1."""
        record = {
            "llm_review": {"verification_confidence": 8, "issues_found": []},
            "human_review": {"decision": "approved_by_human"},
            "source": {"domain": "federalreserve.gov"},
            "why_it_matters": "Fed policy directly affects interest rates and market conditions broadly.",
            "important_numbers": [{"value": "5.25%", "description": "rate"}],
            "topic": "monetary policy",
            "event_type": "fed_policy",
        }

        result = assign_quality_tier(
            record, self.config, domain_trust=self.domain_trust
        )
        quality_tier = result["quality_tier"]

        self.assertEqual(quality_tier["tier"], "tier_1")
        self.assertGreaterEqual(quality_tier["score"], 80)

    def test_tier_2_assignment_moderate_record(self):
        """Record with moderate quality attributes should be assigned tier_2."""
        record = {
            "llm_review": {
                "verification_confidence": 6,
                "issues_found": ["minor issue 1"],
            },
            "human_review": {},
            "source": {"domain": "imf.org"},
            "why_it_matters": "This matters for economic analysis.",
            "important_numbers": [],
            "topic": "macro data",
            "event_type": "data_release",
        }

        result = assign_quality_tier(
            record, self.config, domain_trust=self.domain_trust
        )
        quality_tier = result["quality_tier"]

        self.assertEqual(quality_tier["tier"], "tier_2")
        self.assertGreaterEqual(quality_tier["score"], 55)
        self.assertLess(quality_tier["score"], 80)

    def test_tier_2_range_score(self):
        """Record scoring in tier_2 range (55-79) should be tier_2."""
        # This record is configured to score in the tier_2 range:
        # - Higher verification confidence (7)
        # - Some issues (2)
        # - Short why_it_matters (0.5)
        # - Has numbers (0.10 boost)
        # - Medium trust source
        # - Topic centrality 0.7
        # - Event significance 0.7
        record = {
            "llm_review": {
                "verification_confidence": 7,
                "issues_found": ["minor", "also minor"],
            },
            "human_review": {},
            "source": {"domain": "imf.org"},
            "why_it_matters": "Economic data provides context.",
            "important_numbers": [{"value": "2.5%", "description": "GDP growth"}],
            "topic": "market structure",
            "event_type": "liquidity",
        }

        result = assign_quality_tier(
            record, self.config, domain_trust=self.domain_trust
        )
        quality_tier = result["quality_tier"]

        self.assertGreaterEqual(quality_tier["score"], 55)
        self.assertLess(quality_tier["score"], 80)
        self.assertEqual(quality_tier["tier"], "tier_2")

    def test_tier_3_assignment_low_quality_record(self):
        """Record with low quality attributes should be assigned tier_3."""
        record = {
            "llm_review": {
                "verification_confidence": 3,
                "issues_found": ["issue1", "issue2", "issue3"],
            },
            "human_review": {},
            "source": {"domain": "exampleblog.com"},
            "why_it_matters": "",
            "important_numbers": [],
            "topic": "other",
            "event_type": "other",
        }

        result = assign_quality_tier(
            record, self.config, domain_trust=self.domain_trust
        )
        quality_tier = result["quality_tier"]

        self.assertEqual(quality_tier["tier"], "tier_3")
        self.assertLess(quality_tier["score"], 55)

    def test_tier_3_boundary_score(self):
        """Record scoring below 55 should be tier_3."""
        record = {
            "llm_review": {"verification_confidence": 2, "issues_found": ["issue1"]},
            "human_review": {},
            "source": {},
            "why_it_matters": "Short.",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        result = assign_quality_tier(
            record, self.config, domain_trust=self.domain_trust
        )
        quality_tier = result["quality_tier"]

        self.assertEqual(quality_tier["tier"], "tier_3")
        self.assertLess(quality_tier["score"], 55)

    def test_quality_tier_block_structure(self):
        """Quality tier block should have correct structure."""
        record = {
            "llm_review": {"verification_confidence": 5, "issues_found": []},
            "human_review": {},
            "source": {},
            "why_it_matters": "Test explanation.",
            "important_numbers": [],
            "topic": "",
            "event_type": "",
        }

        result = assign_quality_tier(
            record, self.config, domain_trust=self.domain_trust
        )
        quality_tier = result["quality_tier"]

        self.assertIn("tier", quality_tier)
        self.assertIn("score", quality_tier)
        self.assertIn("reasoning", quality_tier)
        self.assertIsInstance(quality_tier["reasoning"], list)
        self.assertGreater(len(quality_tier["reasoning"]), 0)


class ProcessRecordFileTests(unittest.TestCase):
    """Test process_record_file function."""

    def setUp(self):
        self.config = load_config()
        self.domain_trust = load_domain_trust()

    def test_dry_run_does_not_modify_file(self):
        """dry_run=True should not modify the file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            record = {
                "id": "test_record",
                "llm_review": {"verification_confidence": 5, "issues_found": []},
                "human_review": {},
                "source": {},
                "why_it_matters": "Test explanation.",
                "important_numbers": [],
                "topic": "",
                "event_type": "",
            }

            file_path = Path(tmpdir) / "test_record.json"
            with file_path.open("w", encoding="utf-8") as f:
                json.dump(record, f)

            # Read original content
            with file_path.open("r", encoding="utf-8") as f:
                original_content = f.read()

            # Process with dry_run=True
            result = process_record_file(file_path, self.config, dry_run=True)

            # File should not be modified
            with file_path.open("r", encoding="utf-8") as f:
                new_content = f.read()

            self.assertEqual(original_content, new_content)
            self.assertTrue(result["success"])
            self.assertIsNotNone(result["new_tier"])

    def test_skips_already_tiered_records(self):
        """Records with existing quality_tier should be skipped."""
        with tempfile.TemporaryDirectory() as tmpdir:
            record = {
                "id": "test_record",
                "llm_review": {"verification_confidence": 5, "issues_found": []},
                "human_review": {},
                "source": {},
                "why_it_matters": "Test.",
                "important_numbers": [],
                "topic": "",
                "event_type": "",
                "quality_tier": {
                    "tier": "tier_1",
                    "score": 85.0,
                    "reasoning": ["existing reasoning"],
                },
            }

            file_path = Path(tmpdir) / "test_record.json"
            with file_path.open("w", encoding="utf-8") as f:
                json.dump(record, f)

            result = process_record_file(file_path, self.config, dry_run=False)

            self.assertTrue(result["skipped"])
            self.assertTrue(result["success"])
            self.assertEqual(result["old_tier"], "tier_1")
            self.assertIsNone(result["new_tier"])

    def test_handles_missing_file(self):
        """Should handle missing files gracefully."""
        result = process_record_file(
            Path("/nonexistent/path/record.json"), self.config, dry_run=False
        )

        self.assertFalse(result["success"])
        self.assertIn("error", result)

    def test_handles_invalid_json(self):
        """Should handle invalid JSON gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "invalid_record.json"
            file_path.write_text("{ invalid json }", encoding="utf-8")

            result = process_record_file(file_path, self.config, dry_run=False)

            self.assertFalse(result["success"])
            self.assertIn("error", result)

    def test_assigns_tier_to_valid_record(self):
        """Should assign tier to a valid record without existing quality_tier."""
        with tempfile.TemporaryDirectory() as tmpdir:
            record = {
                "id": "test_record",
                "llm_review": {"verification_confidence": 7, "issues_found": []},
                "human_review": {},
                "source": {"domain": "federalreserve.gov"},
                "why_it_matters": "Fed policy impacts all markets significantly.",
                "important_numbers": [],
                "topic": "monetary policy",
                "event_type": "fed_policy",
            }

            file_path = Path(tmpdir) / "test_record.json"
            with file_path.open("w", encoding="utf-8") as f:
                json.dump(record, f)

            result = process_record_file(file_path, self.config, dry_run=False)

            self.assertTrue(result["success"])
            self.assertFalse(result["skipped"])
            self.assertIsNotNone(result["new_tier"])
            self.assertIsNotNone(result["score"])

            # Verify file was updated
            with file_path.open("r", encoding="utf-8") as f:
                updated_record = json.load(f)

            self.assertIn("quality_tier", updated_record)
            self.assertEqual(updated_record["quality_tier"]["tier"], result["new_tier"])


class ProcessBatchTests(unittest.TestCase):
    """Test process_batch function."""

    def setUp(self):
        self.config = load_config()

    def test_processes_all_json_files(self):
        """Should process all JSON files in directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create multiple test records
            for i in range(3):
                record = {
                    "id": f"test_record_{i}",
                    "llm_review": {
                        "verification_confidence": 5 + i,
                        "issues_found": [],
                    },
                    "human_review": {},
                    "source": {},
                    "why_it_matters": f"Test explanation {i}.",
                    "important_numbers": [],
                    "topic": "",
                    "event_type": "",
                }
                file_path = Path(tmpdir) / f"record_{i}.json"
                with file_path.open("w", encoding="utf-8") as f:
                    json.dump(record, f)

            # Create a non-JSON file that should be ignored
            non_json_path = Path(tmpdir) / "readme.txt"
            non_json_path.write_text("This is not a JSON file", encoding="utf-8")

            results = process_batch(Path(tmpdir), self.config, dry_run=True)

            self.assertEqual(len(results), 3)
            for result in results:
                self.assertTrue(result["success"])

    def test_returns_correct_counts(self):
        """Should return correct counts for assigned/skipped/failed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create one valid record
            record = {
                "id": "test_record",
                "llm_review": {"verification_confidence": 5, "issues_found": []},
                "human_review": {},
                "source": {},
                "why_it_matters": "Test.",
                "important_numbers": [],
                "topic": "",
                "event_type": "",
            }
            file_path = Path(tmpdir) / "record.json"
            with file_path.open("w", encoding="utf-8") as f:
                json.dump(record, f)

            results = process_batch(Path(tmpdir), self.config, dry_run=True)

            self.assertEqual(len(results), 1)
            assigned = sum(1 for r in results if r["success"] and not r["skipped"])
            self.assertEqual(assigned, 1)

    def test_handles_empty_directory(self):
        """Should handle empty directory gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            results = process_batch(Path(tmpdir), self.config, dry_run=True)
            self.assertEqual(len(results), 0)


if __name__ == "__main__":
    unittest.main()
