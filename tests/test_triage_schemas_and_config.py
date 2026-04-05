"""
Tests for Stream 1: Triage schemas, config, and directory structure.
These tests verify the existence and structure of:
- schemas/candidate.json
- schemas/triage_result.json
- config/triage_weights.json
- config/triage_budget.json
- data/candidates/ directory
- data/triage/ directory
- data/deferred/ directory
"""

import json
import unittest
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


class CandidateSchemaTests(unittest.TestCase):
    """Test candidate.json schema structure and required fields."""

    def setUp(self):
        schema_path = BASE_DIR / "schemas" / "candidate.json"
        with schema_path.open("r", encoding="utf-8") as f:
            self.schema = json.load(f)

    def test_schema_is_valid_json(self):
        """Schema should be parseable JSON."""
        self.assertIsInstance(self.schema, dict)

    def test_schema_has_required_fields(self):
        """Schema should have all required fields for a candidate record."""
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
            self.assertIn(
                field,
                self.schema.get("properties", {}),
                f"Missing required field: {field}",
            )

    def test_candidate_id_field_exists(self):
        """candidate_id field should exist."""
        self.assertIn("candidate_id", self.schema.get("properties", {}))

    def test_lane_field_exists(self):
        """lane field should exist."""
        self.assertIn("lane", self.schema.get("properties", {}))

    def test_source_name_field_exists(self):
        """source_name field should exist."""
        self.assertIn("source_name", self.schema.get("properties", {}))

    def test_source_domain_field_exists(self):
        """source_domain field should exist."""
        self.assertIn("source_domain", self.schema.get("properties", {}))

    def test_source_url_field_exists(self):
        """source_url field should exist."""
        self.assertIn("source_url", self.schema.get("properties", {}))

    def test_discovered_at_field_exists(self):
        """discovered_at field should exist."""
        self.assertIn("discovered_at", self.schema.get("properties", {}))

    def test_topic_field_exists(self):
        """topic field should exist."""
        self.assertIn("topic", self.schema.get("properties", {}))

    def test_title_field_exists(self):
        """title field should exist."""
        self.assertIn("title", self.schema.get("properties", {}))

    def test_anchor_text_field_exists(self):
        """anchor_text field should exist."""
        self.assertIn("anchor_text", self.schema.get("properties", {}))

    def test_raw_path_field_exists(self):
        """raw_path field should exist."""
        self.assertIn("raw_path", self.schema.get("properties", {}))

    def test_source_type_field_exists(self):
        """source_type field should exist."""
        self.assertIn("source_type", self.schema.get("properties", {}))

    def test_discovery_context_field_exists(self):
        """discovery_context field should exist."""
        self.assertIn("discovery_context", self.schema.get("properties", {}))


class TriageResultSchemaTests(unittest.TestCase):
    """Test triage_result.json schema structure and required fields."""

    def setUp(self):
        schema_path = BASE_DIR / "schemas" / "triage_result.json"
        with schema_path.open("r", encoding="utf-8") as f:
            self.schema = json.load(f)

    def test_schema_is_valid_json(self):
        """Schema should be parseable JSON."""
        self.assertIsInstance(self.schema, dict)

    def test_schema_has_candidate_id(self):
        """Schema should have candidate_id field."""
        self.assertIn("candidate_id", self.schema.get("properties", {}))

    def test_schema_has_priority_score(self):
        """Schema should have priority_score field."""
        self.assertIn("priority_score", self.schema.get("properties", {}))

    def test_schema_has_priority_band(self):
        """Schema should have priority_band field."""
        self.assertIn("priority_band", self.schema.get("properties", {}))

    def test_schema_has_scoring_field(self):
        """Schema should have scoring field."""
        self.assertIn("scoring", self.schema.get("properties", {}))

    def test_schema_has_reasons_field(self):
        """Schema should have reasons field."""
        self.assertIn("reasons", self.schema.get("properties", {}))

    def test_schema_has_action_field(self):
        """Schema should have action field."""
        self.assertIn("action", self.schema.get("properties", {}))

    def test_scoring_has_source_trust(self):
        """scoring should have source_trust sub-field."""
        scoring_props = (
            self.schema.get("properties", {}).get("scoring", {}).get("properties", {})
        )
        self.assertIn("source_trust", scoring_props)

    def test_scoring_has_freshness(self):
        """scoring should have freshness sub-field."""
        scoring_props = (
            self.schema.get("properties", {}).get("scoring", {}).get("properties", {})
        )
        self.assertIn("freshness", scoring_props)

    def test_scoring_has_topic_relevance(self):
        """scoring should have topic_relevance sub-field."""
        scoring_props = (
            self.schema.get("properties", {}).get("scoring", {}).get("properties", {})
        )
        self.assertIn("topic_relevance", scoring_props)

    def test_scoring_has_title_quality(self):
        """scoring should have title_quality sub-field."""
        scoring_props = (
            self.schema.get("properties", {}).get("scoring", {}).get("properties", {})
        )
        self.assertIn("title_quality", scoring_props)

    def test_scoring_has_url_quality(self):
        """scoring should have url_quality sub-field."""
        scoring_props = (
            self.schema.get("properties", {}).get("scoring", {}).get("properties", {})
        )
        self.assertIn("url_quality", scoring_props)

    def test_scoring_has_novelty(self):
        """scoring should have novelty sub-field."""
        scoring_props = (
            self.schema.get("properties", {}).get("scoring", {}).get("properties", {})
        )
        self.assertIn("novelty", scoring_props)

    def test_scoring_has_quant_value(self):
        """scoring should have quant_value sub-field."""
        scoring_props = (
            self.schema.get("properties", {}).get("scoring", {}).get("properties", {})
        )
        self.assertIn("quant_value", scoring_props)

    def test_scoring_has_duplicate_risk(self):
        """scoring should have duplicate_risk sub-field."""
        scoring_props = (
            self.schema.get("properties", {}).get("scoring", {}).get("properties", {})
        )
        self.assertIn("duplicate_risk", scoring_props)

    def test_scoring_has_all_eight_sub_fields(self):
        """scoring should have all 8 sub-fields."""
        expected_sub_fields = [
            "source_trust",
            "freshness",
            "topic_relevance",
            "title_quality",
            "url_quality",
            "novelty",
            "quant_value",
            "duplicate_risk",
        ]
        scoring_props = (
            self.schema.get("properties", {}).get("scoring", {}).get("properties", {})
        )
        actual_sub_fields = list(scoring_props.keys())
        self.assertEqual(
            set(expected_sub_fields),
            set(actual_sub_fields),
            f"scoring should have exactly {expected_sub_fields}, got {actual_sub_fields}",
        )


class TriageWeightsConfigTests(unittest.TestCase):
    """Test triage_weights.json configuration structure."""

    def setUp(self):
        config_path = BASE_DIR / "config" / "triage_weights.json"
        with config_path.open("r", encoding="utf-8") as f:
            self.config = json.load(f)

    def test_config_is_valid_json(self):
        """Config should be parseable JSON."""
        self.assertIsInstance(self.config, dict)

    def test_config_has_weights_object(self):
        """Config should have weights object with all 8 categories."""
        self.assertIn("weights", self.config)
        weights = self.config["weights"]
        expected_categories = [
            "source_trust",
            "freshness",
            "topic_relevance",
            "title_quality",
            "url_quality",
            "novelty",
            "quant_value",
            "duplicate_risk",
        ]
        for category in expected_categories:
            self.assertIn(category, weights, f"Missing weight category: {category}")

    def test_weights_has_all_eight_categories(self):
        """weights should have all 8 categories."""
        expected_categories = [
            "source_trust",
            "freshness",
            "topic_relevance",
            "title_quality",
            "url_quality",
            "novelty",
            "quant_value",
            "duplicate_risk",
        ]
        actual_categories = list(self.config["weights"].keys())
        self.assertEqual(
            set(expected_categories),
            set(actual_categories),
            f"weights should have exactly {expected_categories}, got {actual_categories}",
        )

    def test_config_has_bands_object(self):
        """Config should have bands object."""
        self.assertIn("bands", self.config)

    def test_bands_has_critical(self):
        """bands should have critical threshold."""
        self.assertIn("critical", self.config["bands"])

    def test_bands_has_high(self):
        """bands should have high threshold."""
        self.assertIn("high", self.config["bands"])

    def test_bands_has_medium(self):
        """bands should have medium threshold."""
        self.assertIn("medium", self.config["bands"])

    def test_bands_has_low(self):
        """bands should have low threshold."""
        self.assertIn("low", self.config["bands"])

    def test_bands_has_all_four_levels(self):
        """bands should have all four priority levels."""
        expected_bands = ["critical", "high", "medium", "low"]
        actual_bands = list(self.config["bands"].keys())
        self.assertEqual(
            set(expected_bands),
            set(actual_bands),
            f"bands should have exactly {expected_bands}, got {actual_bands}",
        )


class TriageWeightsValuesTests(unittest.TestCase):
    """Test triage_weights.json weight values and band thresholds."""

    def setUp(self):
        config_path = BASE_DIR / "config" / "triage_weights.json"
        with config_path.open("r", encoding="utf-8") as f:
            self.config = json.load(f)

    def test_weight_values_sum_approximately_to_one(self):
        """Weight values should sum approximately to 1.0 (accounting for negative duplicate_risk)."""
        weights = self.config["weights"]
        total = sum(weights.values())
        self.assertAlmostEqual(
            total,
            1.0,
            places=2,
            msg=f"Weights sum to {total}, expected approximately 1.0",
        )

    def test_band_thresholds_are_in_descending_order(self):
        """Band thresholds should be in descending order: critical > high > medium > low."""
        bands = self.config["bands"]
        critical = bands["critical"]
        high = bands["high"]
        medium = bands["medium"]
        low = bands["low"]

        self.assertGreater(
            critical,
            high,
            f"critical ({critical}) should be greater than high ({high})",
        )
        self.assertGreater(
            high, medium, f"high ({high}) should be greater than medium ({medium})"
        )
        self.assertGreater(
            medium, low, f"medium ({medium}) should be greater than low ({low})"
        )


class TriageBudgetConfigTests(unittest.TestCase):
    """Test triage_budget.json configuration structure."""

    def setUp(self):
        config_path = BASE_DIR / "config" / "triage_budget.json"
        with config_path.open("r", encoding="utf-8") as f:
            self.config = json.load(f)

    def test_config_is_valid_json(self):
        """Config should be parseable JSON."""
        self.assertIsInstance(self.config, dict)

    def test_config_has_article_process_limit(self):
        """Config should have article_process_limit field."""
        self.assertIn("article_process_limit", self.config)

    def test_config_has_quant_process_limit(self):
        """Config should have quant_process_limit field."""
        self.assertIn("quant_process_limit", self.config)

    def test_config_has_defer_medium(self):
        """Config should have defer_medium field."""
        self.assertIn("defer_medium", self.config)

    def test_article_process_limit_is_numeric(self):
        """article_process_limit should be a number."""
        limit = self.config["article_process_limit"]
        self.assertIsInstance(limit, (int, float))

    def test_quant_process_limit_is_numeric(self):
        """quant_process_limit should be a number."""
        limit = self.config["quant_process_limit"]
        self.assertIsInstance(limit, (int, float))

    def test_defer_medium_is_boolean(self):
        """defer_medium should be a boolean."""
        defer = self.config["defer_medium"]
        self.assertIsInstance(defer, bool)


class DirectoryStructureTests(unittest.TestCase):
    """Test that required directories exist."""

    def test_data_candidates_directory_exists(self):
        """data/candidates/ directory should exist."""
        candidates_dir = BASE_DIR / "data" / "candidates"
        self.assertTrue(
            candidates_dir.exists(), f"Directory {candidates_dir} does not exist"
        )

    def test_data_candidates_is_directory(self):
        """data/candidates/ should be a directory."""
        candidates_dir = BASE_DIR / "data" / "candidates"
        self.assertTrue(
            candidates_dir.is_dir(), f"{candidates_dir} exists but is not a directory"
        )

    def test_data_triage_directory_exists(self):
        """data/triage/ directory should exist."""
        triage_dir = BASE_DIR / "data" / "triage"
        self.assertTrue(triage_dir.exists(), f"Directory {triage_dir} does not exist")

    def test_data_triage_is_directory(self):
        """data/triage/ should be a directory."""
        triage_dir = BASE_DIR / "data" / "triage"
        self.assertTrue(
            triage_dir.is_dir(), f"{triage_dir} exists but is not a directory"
        )

    def test_data_deferred_directory_exists(self):
        """data/deferred/ directory should exist."""
        deferred_dir = BASE_DIR / "data" / "deferred"
        self.assertTrue(
            deferred_dir.exists(), f"Directory {deferred_dir} does not exist"
        )

    def test_data_deferred_is_directory(self):
        """data/deferred/ should be a directory."""
        deferred_dir = BASE_DIR / "data" / "deferred"
        self.assertTrue(
            deferred_dir.is_dir(), f"{deferred_dir} exists but is not a directory"
        )


if __name__ == "__main__":
    unittest.main()
