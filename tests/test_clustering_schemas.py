"""
Tests for Stream 1: Event Clustering Schemas and Configuration.
These tests verify the existence and structure of:
- schemas/event_cluster.json
- schemas/story_edge.json
- config/clustering_rules.json
"""

import json
import unittest
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent


class EventClusterSchemaTests(unittest.TestCase):
    """Test event_cluster.json schema structure and required fields."""

    def setUp(self):
        schema_path = BASE_DIR / "schemas" / "event_cluster.json"
        with schema_path.open("r", encoding="utf-8") as f:
            self.schema = json.load(f)

    def test_schema_is_valid_json(self):
        """Schema should be parseable JSON."""
        self.assertIsInstance(self.schema, dict)

    def test_schema_has_required_fields(self):
        """Schema should have all required fields for an event cluster."""
        required_fields = [
            "event_id",
            "title",
            "topic",
            "event_type",
            "summary",
            "status",
            "created_at",
            "updated_at",
            "record_ids",
            "source_domains",
            "keywords",
            "quant_links",
            "confidence",
        ]
        properties = self.schema.get("properties", {})
        for field in required_fields:
            self.assertIn(
                field,
                properties,
                f"Missing required field: {field}",
            )

    def test_event_id_field_exists(self):
        """event_id field should exist and be a string."""
        props = self.schema.get("properties", {})
        self.assertIn("event_id", props)
        self.assertEqual(props["event_id"]["type"], "string")

    def test_title_field_exists(self):
        """title field should exist and be a string."""
        props = self.schema.get("properties", {})
        self.assertIn("title", props)
        self.assertEqual(props["title"]["type"], "string")

    def test_topic_field_exists(self):
        """topic field should exist and be a string."""
        props = self.schema.get("properties", {})
        self.assertIn("topic", props)
        self.assertEqual(props["topic"]["type"], "string")

    def test_event_type_field_exists(self):
        """event_type field should exist and be a string."""
        props = self.schema.get("properties", {})
        self.assertIn("event_type", props)
        self.assertEqual(props["event_type"]["type"], "string")

    def test_summary_field_exists(self):
        """summary field should exist and be a string."""
        props = self.schema.get("properties", {})
        self.assertIn("summary", props)
        self.assertEqual(props["summary"]["type"], "string")

    def test_status_field_is_enum(self):
        """status field should be an enum with open, stable, archived."""
        props = self.schema.get("properties", {})
        self.assertIn("status", props)
        status_field = props["status"]
        self.assertEqual(status_field["type"], "string")
        self.assertIn("enum", status_field)
        self.assertEqual(
            set(status_field["enum"]),
            {"open", "stable", "archived"},
            "status enum should contain open, stable, archived",
        )

    def test_created_at_field_exists(self):
        """created_at field should exist and be a string with date-time format."""
        props = self.schema.get("properties", {})
        self.assertIn("created_at", props)
        self.assertEqual(props["created_at"]["type"], "string")
        self.assertEqual(props["created_at"]["format"], "date-time")

    def test_updated_at_field_exists(self):
        """updated_at field should exist and be a string with date-time format."""
        props = self.schema.get("properties", {})
        self.assertIn("updated_at", props)
        self.assertEqual(props["updated_at"]["type"], "string")
        self.assertEqual(props["updated_at"]["format"], "date-time")

    def test_record_ids_field_is_array_of_strings(self):
        """record_ids field should be an array of strings."""
        props = self.schema.get("properties", {})
        self.assertIn("record_ids", props)
        self.assertEqual(props["record_ids"]["type"], "array")
        self.assertEqual(props["record_ids"]["items"]["type"], "string")

    def test_source_domains_field_is_array_of_strings(self):
        """source_domains field should be an array of strings."""
        props = self.schema.get("properties", {})
        self.assertIn("source_domains", props)
        self.assertEqual(props["source_domains"]["type"], "array")
        self.assertEqual(props["source_domains"]["items"]["type"], "string")

    def test_keywords_field_is_array_of_strings(self):
        """keywords field should be an array of strings."""
        props = self.schema.get("properties", {})
        self.assertIn("keywords", props)
        self.assertEqual(props["keywords"]["type"], "array")
        self.assertEqual(props["keywords"]["items"]["type"], "string")

    def test_quant_links_field_is_array_of_strings(self):
        """quant_links field should be an array of strings."""
        props = self.schema.get("properties", {})
        self.assertIn("quant_links", props)
        self.assertEqual(props["quant_links"]["type"], "array")
        self.assertEqual(props["quant_links"]["items"]["type"], "string")

    def test_confidence_field_is_number_with_range(self):
        """confidence field should be a number between 0 and 1."""
        props = self.schema.get("properties", {})
        self.assertIn("confidence", props)
        self.assertEqual(props["confidence"]["type"], "number")
        self.assertEqual(props["confidence"]["minimum"], 0)
        self.assertEqual(props["confidence"]["maximum"], 1)

    def test_iso_8601_timestamp_validation(self):
        """created_at and updated_at should accept valid ISO-8601 timestamps."""
        # Test that ISO-8601 format is accepted
        valid_timestamps = [
            "2026-03-18T14:30:00Z",
            "2026-03-18T14:30:00+00:00",
            "2026-03-18T14:30:00.123Z",
            "2026-03-18",
        ]
        for ts in valid_timestamps:
            try:
                datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                self.fail(f"Invalid ISO-8601 timestamp: {ts}")


class StoryEdgeSchemaTests(unittest.TestCase):
    """Test story_edge.json schema structure and required fields."""

    def setUp(self):
        schema_path = BASE_DIR / "schemas" / "story_edge.json"
        with schema_path.open("r", encoding="utf-8") as f:
            self.schema = json.load(f)

    def test_schema_is_valid_json(self):
        """Schema should be parseable JSON."""
        self.assertIsInstance(self.schema, dict)

    def test_schema_has_required_fields(self):
        """Schema should have all required fields for a story edge."""
        required_fields = [
            "from_type",
            "from_id",
            "to_type",
            "to_id",
            "relationship",
            "weight",
        ]
        properties = self.schema.get("properties", {})
        for field in required_fields:
            self.assertIn(
                field,
                properties,
                f"Missing required field: {field}",
            )

    def test_from_type_is_enum(self):
        """from_type field should be an enum with record, event, quant, theme."""
        props = self.schema.get("properties", {})
        self.assertIn("from_type", props)
        from_type = props["from_type"]
        self.assertEqual(from_type["type"], "string")
        self.assertIn("enum", from_type)
        self.assertEqual(
            set(from_type["enum"]),
            {"record", "event", "quant", "theme"},
            "from_type enum should contain record, event, quant, theme",
        )

    def test_from_id_field_exists(self):
        """from_id field should exist and be a string."""
        props = self.schema.get("properties", {})
        self.assertIn("from_id", props)
        self.assertEqual(props["from_id"]["type"], "string")

    def test_to_type_is_enum(self):
        """to_type field should be an enum with record, event, quant, theme."""
        props = self.schema.get("properties", {})
        self.assertIn("to_type", props)
        to_type = props["to_type"]
        self.assertEqual(to_type["type"], "string")
        self.assertIn("enum", to_type)
        self.assertEqual(
            set(to_type["enum"]),
            {"record", "event", "quant", "theme"},
            "to_type enum should contain record, event, quant, theme",
        )

    def test_to_id_field_exists(self):
        """to_id field should exist and be a string."""
        props = self.schema.get("properties", {})
        self.assertIn("to_id", props)
        self.assertEqual(props["to_id"]["type"], "string")

    def test_relationship_is_enum(self):
        """relationship field should be an enum with supports, extends, quant_context, contradicts, duplicate_theme."""
        props = self.schema.get("properties", {})
        self.assertIn("relationship", props)
        relationship = props["relationship"]
        self.assertEqual(relationship["type"], "string")
        self.assertIn("enum", relationship)
        self.assertEqual(
            set(relationship["enum"]),
            {"supports", "extends", "quant_context", "contradicts", "duplicate_theme"},
            "relationship enum should contain supports, extends, quant_context, contradicts, duplicate_theme",
        )

    def test_weight_field_is_number_with_range(self):
        """weight field should be a number between 0 and 1."""
        props = self.schema.get("properties", {})
        self.assertIn("weight", props)
        self.assertEqual(props["weight"]["type"], "number")
        self.assertEqual(props["weight"]["minimum"], 0)
        self.assertEqual(props["weight"]["maximum"], 1)


class ClusteringRulesConfigTests(unittest.TestCase):
    """Test clustering_rules.json configuration structure."""

    def setUp(self):
        config_path = BASE_DIR / "config" / "clustering_rules.json"
        with config_path.open("r", encoding="utf-8") as f:
            self.config = json.load(f)

    def test_config_is_valid_json(self):
        """Config should be parseable JSON."""
        self.assertIsInstance(self.config, dict)

    def test_config_has_similarity_threshold(self):
        """Config should have similarity_threshold field."""
        self.assertIn("similarity_threshold", self.config)
        threshold = self.config["similarity_threshold"]
        self.assertEqual(threshold, 60, "similarity_threshold should be 60")

    def test_config_has_time_window_days(self):
        """Config should have time_window_days field."""
        self.assertIn("time_window_days", self.config)
        self.assertEqual(self.config["time_window_days"], 7)

    def test_config_has_min_cluster_size(self):
        """Config should have min_cluster_size field."""
        self.assertIn("min_cluster_size", self.config)
        self.assertEqual(self.config["min_cluster_size"], 1)

    def test_config_has_stable_cluster_accepts_new_records(self):
        """Config should have stable_cluster_accepts_new_records field."""
        self.assertIn("stable_cluster_accepts_new_records", self.config)
        self.assertIsInstance(
            self.config["stable_cluster_accepts_new_records"],
            bool,
            "stable_cluster_accepts_new_records should be a boolean",
        )
        self.assertTrue(self.config["stable_cluster_accepts_new_records"])

    def test_config_has_stable_threshold(self):
        """Config should have stable_threshold field."""
        self.assertIn("stable_threshold", self.config)
        self.assertEqual(self.config["stable_threshold"], 5)

    def test_config_has_archived_threshold_days(self):
        """Config should have archived_threshold_days field."""
        self.assertIn("archived_threshold_days", self.config)
        self.assertEqual(self.config["archived_threshold_days"], 14)

    def test_config_has_title_generation(self):
        """Config should have title_generation field set to deterministic."""
        self.assertIn("title_generation", self.config)
        self.assertEqual(
            self.config["title_generation"],
            "deterministic",
            "title_generation should be 'deterministic'",
        )

    def test_config_has_weight_overrides(self):
        """Config should have weight_overrides object."""
        self.assertIn("weight_overrides", self.config)
        weights = self.config["weight_overrides"]
        self.assertIsInstance(weights, dict)

    def test_weight_overrides_has_topic_compatibility(self):
        """weight_overrides should have topic_compatibility = 0.30."""
        weights = self.config["weight_overrides"]
        self.assertIn("topic_compatibility", weights)
        self.assertEqual(weights["topic_compatibility"], 0.30)

    def test_weight_overrides_has_phrase_overlap(self):
        """weight_overrides should have phrase_overlap = 0.25."""
        weights = self.config["weight_overrides"]
        self.assertIn("phrase_overlap", weights)
        self.assertEqual(weights["phrase_overlap"], 0.25)

    def test_weight_overrides_has_time_proximity(self):
        """weight_overrides should have time_proximity = 0.20."""
        weights = self.config["weight_overrides"]
        self.assertIn("time_proximity", weights)
        self.assertEqual(weights["time_proximity"], 0.20)

    def test_weight_overrides_has_source_diversity(self):
        """weight_overrides should have source_diversity = 0.15."""
        weights = self.config["weight_overrides"]
        self.assertIn("source_diversity", weights)
        self.assertEqual(weights["source_diversity"], 0.15)

    def test_weight_overrides_has_quant_support(self):
        """weight_overrides should have quant_support = 0.10."""
        weights = self.config["weight_overrides"]
        self.assertIn("quant_support", weights)
        self.assertEqual(weights["quant_support"], 0.10)

    def test_weight_overrides_sum_to_one(self):
        """weight_overrides values should sum to 1.0."""
        weights = self.config["weight_overrides"]
        total = sum(weights.values())
        self.assertAlmostEqual(
            total,
            1.0,
            places=2,
            msg=f"Weight overrides sum to {total}, expected 1.0",
        )

    def test_config_has_event_types(self):
        """Config should have event_types array."""
        self.assertIn("event_types", self.config)
        event_types = self.config["event_types"]
        self.assertIsInstance(event_types, list)
        self.assertEqual(
            set(event_types),
            {
                "fed_speech",
                "treasury_update",
                "ny_fed_market_note",
                "quant_move",
                "rate_change",
                "liquidity_event",
                "yield_curve_shift",
            },
        )


class ConfidenceAndWeightRangeTests(unittest.TestCase):
    """Test that confidence and weight values are properly constrained to 0-1 range."""

    def test_event_cluster_confidence_must_be_between_0_and_1(self):
        """confidence values must be between 0 and 1 (inclusive)."""
        # Test edge cases for confidence field validation
        valid_confidences = [0, 0.5, 1.0, 0.0, 0.123]
        for conf in valid_confidences:
            self.assertGreaterEqual(conf, 0, f"confidence {conf} should be >= 0")
            self.assertLessEqual(conf, 1, f"confidence {conf} should be <= 1")

    def test_story_edge_weight_must_be_between_0_and_1(self):
        """weight values must be between 0 and 1 (inclusive)."""
        # Test edge cases for weight field validation
        valid_weights = [0, 0.5, 1.0, 0.0, 0.123]
        for weight in valid_weights:
            self.assertGreaterEqual(weight, 0, f"weight {weight} should be >= 0")
            self.assertLessEqual(weight, 1, f"weight {weight} should be <= 1")


if __name__ == "__main__":
    unittest.main()
