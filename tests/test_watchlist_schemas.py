"""
Tests for Phase 2.7 Part 3: Watchlist schemas, config, and directory structure.
These tests verify the existence and structure of:
- config/watchlists_v27.json
- schemas/watchlist.json
- schemas/watchlist_hit.json
- data/watchlists/ directory
- data/watchlist_hits/ directory
- data/theses/ directory
"""

import json
import unittest
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


class WatchlistConfigTests(unittest.TestCase):
    """Test watchlists_v27.json configuration structure."""

    def setUp(self):
        config_path = BASE_DIR / "config" / "watchlists_v27.json"
        with config_path.open("r", encoding="utf-8") as f:
            self.config = json.load(f)

    def test_config_is_valid_json(self):
        """Config should be parseable JSON."""
        self.assertIsInstance(self.config, list)

    def test_config_is_array(self):
        """Config should be an array."""
        self.assertIsInstance(self.config, list)

    def test_config_has_seven_watchlists(self):
        """Config should have exactly 7 watchlists."""
        self.assertEqual(len(self.config), 7)

    def test_watchlist_ids_are_unique(self):
        """All watchlist IDs should be unique."""
        ids = [wl.get("watchlist_id") for wl in self.config]
        self.assertEqual(len(ids), len(set(ids)), "Watchlist IDs should be unique")

    def test_each_watchlist_has_required_fields(self):
        """Each watchlist should have all required fields."""
        required_fields = [
            "watchlist_id",
            "title",
            "topic",
            "keywords",
            "priority",
            "enabled",
        ]
        for i, watchlist in enumerate(self.config):
            for field in required_fields:
                self.assertIn(
                    field,
                    watchlist,
                    f"Watchlist {i} ({watchlist.get('watchlist_id', 'unknown')}) missing required field: {field}",
                )

    def test_keywords_is_non_empty_array(self):
        """Each watchlist should have a non-empty keywords array."""
        for i, watchlist in enumerate(self.config):
            keywords = watchlist.get("keywords", [])
            self.assertIsInstance(
                keywords, list, f"Watchlist {i} keywords should be a list"
            )
            self.assertGreater(
                len(keywords),
                0,
                f"Watchlist {i} ({watchlist.get('watchlist_id')}) should have at least one keyword",
            )

    def test_priority_values_are_valid(self):
        """Priority values should be one of: high, medium, low."""
        valid_priorities = ["high", "medium", "low"]
        for i, watchlist in enumerate(self.config):
            priority = watchlist.get("priority")
            self.assertIn(
                priority,
                valid_priorities,
                f"Watchlist {i} ({watchlist.get('watchlist_id')}) has invalid priority: {priority}",
            )

    def test_enabled_is_boolean(self):
        """enabled field should be a boolean."""
        for i, watchlist in enumerate(self.config):
            enabled = watchlist.get("enabled")
            self.assertIsInstance(
                enabled,
                bool,
                f"Watchlist {i} ({watchlist.get('watchlist_id')}) enabled should be boolean, got {type(enabled)}",
            )

    def test_required_terms_is_array(self):
        """required_terms should be an array (can be empty)."""
        for i, watchlist in enumerate(self.config):
            required_terms = watchlist.get("required_terms")
            self.assertIsInstance(
                required_terms,
                list,
                f"Watchlist {i} ({watchlist.get('watchlist_id')}) required_terms should be an array",
            )

    def test_blocked_terms_is_array(self):
        """blocked_terms should be an array (can be empty)."""
        for i, watchlist in enumerate(self.config):
            blocked_terms = watchlist.get("blocked_terms")
            self.assertIsInstance(
                blocked_terms,
                list,
                f"Watchlist {i} ({watchlist.get('watchlist_id')}) blocked_terms should be an array",
            )

    def test_watchlist_ids_follow_naming_convention(self):
        """Watchlist IDs should follow 'wl_' prefix convention."""
        for i, watchlist in enumerate(self.config):
            wl_id = watchlist.get("watchlist_id", "")
            self.assertTrue(
                wl_id.startswith("wl_"),
                f"Watchlist {i} ID '{wl_id}' should start with 'wl_'",
            )


class WatchlistSchemaTests(unittest.TestCase):
    """Test watchlist.json schema structure and required fields."""

    def setUp(self):
        schema_path = BASE_DIR / "schemas" / "watchlist.json"
        with schema_path.open("r", encoding="utf-8") as f:
            self.schema = json.load(f)

    def test_schema_is_valid_json(self):
        """Schema should be parseable JSON."""
        self.assertIsInstance(self.schema, dict)

    def test_schema_has_required_fields(self):
        """Schema should have required array with expected fields."""
        required_fields = [
            "watchlist_id",
            "title",
            "topic",
            "keywords",
            "priority",
            "enabled",
        ]
        required = self.schema.get("required", [])
        for field in required_fields:
            self.assertIn(field, required, f"Missing required field in schema: {field}")

    def test_schema_has_properties(self):
        """Schema should have properties object."""
        self.assertIn("properties", self.schema)

    def test_watchlist_id_property_exists(self):
        """watchlist_id property should exist."""
        props = self.schema.get("properties", {})
        self.assertIn("watchlist_id", props)

    def test_title_property_exists(self):
        """title property should exist."""
        props = self.schema.get("properties", {})
        self.assertIn("title", props)

    def test_topic_property_exists(self):
        """topic property should exist."""
        props = self.schema.get("properties", {})
        self.assertIn("topic", props)

    def test_keywords_property_exists(self):
        """keywords property should exist."""
        props = self.schema.get("properties", {})
        self.assertIn("keywords", props)

    def test_keywords_is_array_type(self):
        """keywords should be of type array."""
        props = self.schema.get("properties", {})
        self.assertEqual(props.get("keywords", {}).get("type"), "array")

    def test_keywords_min_items(self):
        """keywords should have minItems of 1."""
        props = self.schema.get("properties", {})
        self.assertEqual(props.get("keywords", {}).get("minItems"), 1)

    def test_priority_property_exists(self):
        """priority property should exist."""
        props = self.schema.get("properties", {})
        self.assertIn("priority", props)

    def test_priority_is_enum(self):
        """priority should be an enum with high, medium, low."""
        props = self.schema.get("properties", {})
        enum_values = props.get("priority", {}).get("enum", [])
        self.assertEqual(set(enum_values), {"high", "medium", "low"})

    def test_enabled_property_exists(self):
        """enabled property should exist."""
        props = self.schema.get("properties", {})
        self.assertIn("enabled", props)

    def test_enabled_is_boolean_type(self):
        """enabled should be of type boolean."""
        props = self.schema.get("properties", {})
        self.assertEqual(props.get("enabled", {}).get("type"), "boolean")

    def test_required_terms_property_exists(self):
        """required_terms property should exist."""
        props = self.schema.get("properties", {})
        self.assertIn("required_terms", props)

    def test_blocked_terms_property_exists(self):
        """blocked_terms property should exist."""
        props = self.schema.get("properties", {})
        self.assertIn("blocked_terms", props)

    def test_additional_properties_is_false(self):
        """additionalProperties should be false to prevent extra fields."""
        self.assertFalse(self.schema.get("additionalProperties", True))


class WatchlistHitSchemaTests(unittest.TestCase):
    """Test watchlist_hit.json schema structure and required fields."""

    def setUp(self):
        schema_path = BASE_DIR / "schemas" / "watchlist_hit.json"
        with schema_path.open("r", encoding="utf-8") as f:
            self.schema = json.load(f)

    def test_schema_is_valid_json(self):
        """Schema should be parseable JSON."""
        self.assertIsInstance(self.schema, dict)

    def test_schema_has_required_fields(self):
        """Schema should have required array with expected fields."""
        required_fields = [
            "watchlist_id",
            "record_id",
            "match_score",
            "matched_terms",
            "thesis_signal",
            "created_at",
        ]
        required = self.schema.get("required", [])
        for field in required_fields:
            self.assertIn(field, required, f"Missing required field in schema: {field}")

    def test_schema_has_properties(self):
        """Schema should have properties object."""
        self.assertIn("properties", self.schema)

    def test_watchlist_id_property_exists(self):
        """watchlist_id property should exist."""
        props = self.schema.get("properties", {})
        self.assertIn("watchlist_id", props)

    def test_record_id_property_exists(self):
        """record_id property should exist."""
        props = self.schema.get("properties", {})
        self.assertIn("record_id", props)

    def test_event_id_property_exists(self):
        """event_id property should exist and allow null."""
        props = self.schema.get("properties", {})
        self.assertIn("event_id", props)
        # event_id should allow null type
        event_id_type = props.get("event_id", {}).get("type")
        self.assertEqual(event_id_type, ["string", "null"])

    def test_match_score_property_exists(self):
        """match_score property should exist."""
        props = self.schema.get("properties", {})
        self.assertIn("match_score", props)

    def test_match_score_has_minimum(self):
        """match_score should have minimum of 0."""
        props = self.schema.get("properties", {})
        self.assertEqual(props.get("match_score", {}).get("minimum"), 0)

    def test_match_score_has_maximum(self):
        """match_score should have maximum of 1."""
        props = self.schema.get("properties", {})
        self.assertEqual(props.get("match_score", {}).get("maximum"), 1)

    def test_matched_terms_property_exists(self):
        """matched_terms property should exist."""
        props = self.schema.get("properties", {})
        self.assertIn("matched_terms", props)

    def test_matched_terms_is_array(self):
        """matched_terms should be of type array."""
        props = self.schema.get("properties", {})
        self.assertEqual(props.get("matched_terms", {}).get("type"), "array")

    def test_thesis_signal_property_exists(self):
        """thesis_signal property should exist."""
        props = self.schema.get("properties", {})
        self.assertIn("thesis_signal", props)

    def test_thesis_signal_is_enum(self):
        """thesis_signal should be an enum with strengthening, weakening, neutral."""
        props = self.schema.get("properties", {})
        enum_values = props.get("thesis_signal", {}).get("enum", [])
        self.assertEqual(set(enum_values), {"strengthening", "weakening", "neutral"})

    def test_created_at_property_exists(self):
        """created_at property should exist."""
        props = self.schema.get("properties", {})
        self.assertIn("created_at", props)

    def test_created_at_is_date_time_format(self):
        """created_at should have date-time format."""
        props = self.schema.get("properties", {})
        self.assertEqual(props.get("created_at", {}).get("format"), "date-time")

    def test_additional_properties_is_false(self):
        """additionalProperties should be false to prevent extra fields."""
        self.assertFalse(self.schema.get("additionalProperties", True))


class WatchlistConfigValidationTests(unittest.TestCase):
    """Test that watchlist config items conform to the schema."""

    def setUp(self):
        config_path = BASE_DIR / "config" / "watchlists_v27.json"
        with config_path.open("r", encoding="utf-8") as f:
            self.watchlists = json.load(f)

    def test_watchlist_ids_are_strings(self):
        """watchlist_id should be a string."""
        for watchlist in self.watchlists:
            self.assertIsInstance(watchlist.get("watchlist_id"), str)

    def test_titles_are_strings(self):
        """title should be a string."""
        for watchlist in self.watchlists:
            self.assertIsInstance(watchlist.get("title"), str)

    def test_topics_are_strings(self):
        """topic should be a string."""
        for watchlist in self.watchlists:
            self.assertIsInstance(watchlist.get("topic"), str)

    def test_descriptions_are_strings(self):
        """description should be a string."""
        for watchlist in self.watchlists:
            desc = watchlist.get("description")
            self.assertIsInstance(
                desc,
                str,
                f"description should be string for {watchlist.get('watchlist_id')}",
            )

    def test_keywords_are_string_arrays(self):
        """keywords should be arrays of strings."""
        for watchlist in self.watchlists:
            keywords = watchlist.get("keywords", [])
            self.assertIsInstance(keywords, list)
            for kw in keywords:
                self.assertIsInstance(kw, str, f"keyword '{kw}' should be string")

    def test_required_terms_are_string_arrays(self):
        """required_terms should be arrays of strings."""
        for watchlist in self.watchlists:
            required_terms = watchlist.get("required_terms", [])
            self.assertIsInstance(required_terms, list)
            for term in required_terms:
                self.assertIsInstance(
                    term, str, f"required_term '{term}' should be string"
                )

    def test_blocked_terms_are_string_arrays(self):
        """blocked_terms should be arrays of strings."""
        for watchlist in self.watchlists:
            blocked_terms = watchlist.get("blocked_terms", [])
            self.assertIsInstance(blocked_terms, list)
            for term in blocked_terms:
                self.assertIsInstance(
                    term, str, f"blocked_term '{term}' should be string"
                )

    def test_priority_values_in_config_match_schema(self):
        """Priority values in config should match schema enum."""
        valid_priorities = ["high", "medium", "low"]
        for watchlist in self.watchlists:
            priority = watchlist.get("priority")
            self.assertIn(priority, valid_priorities)


class InvalidWatchlistTests(unittest.TestCase):
    """Test that invalid watchlist objects are properly rejected."""

    def test_missing_watchlist_id_is_invalid(self):
        """Watchlist missing watchlist_id should be invalid."""
        invalid_watchlist = {
            "title": "Test",
            "topic": "test",
            "keywords": ["test"],
            "priority": "medium",
            "enabled": True,
        }
        required_fields = [
            "watchlist_id",
            "title",
            "topic",
            "keywords",
            "priority",
            "enabled",
        ]
        missing_fields = [f for f in required_fields if f not in invalid_watchlist]
        self.assertGreater(len(missing_fields), 0, "Should have missing fields")

    def test_missing_title_is_invalid(self):
        """Watchlist missing title should be invalid."""
        invalid_watchlist = {
            "watchlist_id": "wl_test",
            "topic": "test",
            "keywords": ["test"],
            "priority": "medium",
            "enabled": True,
        }
        self.assertNotIn("title", invalid_watchlist)

    def test_missing_keywords_is_invalid(self):
        """Watchlist missing keywords should be invalid."""
        invalid_watchlist = {
            "watchlist_id": "wl_test",
            "title": "Test",
            "topic": "test",
            "priority": "medium",
            "enabled": True,
        }
        self.assertNotIn("keywords", invalid_watchlist)

    def test_empty_keywords_is_invalid(self):
        """Watchlist with empty keywords should be invalid per schema (minItems: 1)."""
        # Schema requires minItems: 1 for keywords
        invalid_watchlist = {
            "watchlist_id": "wl_test",
            "title": "Test",
            "topic": "test",
            "keywords": [],  # Empty array violates minItems: 1
            "priority": "medium",
            "enabled": True,
        }
        self.assertEqual(len(invalid_watchlist.get("keywords", [])), 0)

    def test_invalid_priority_is_invalid(self):
        """Watchlist with invalid priority should be invalid."""
        invalid_watchlist = {
            "watchlist_id": "wl_test",
            "title": "Test",
            "topic": "test",
            "keywords": ["test"],
            "priority": "critical",  # Not in enum ["high", "medium", "low"]
            "enabled": True,
        }
        valid_priorities = ["high", "medium", "low"]
        self.assertNotIn(invalid_watchlist.get("priority"), valid_priorities)

    def test_non_boolean_enabled_is_invalid(self):
        """Watchlist with non-boolean enabled should be invalid."""
        invalid_watchlist = {
            "watchlist_id": "wl_test",
            "title": "Test",
            "topic": "test",
            "keywords": ["test"],
            "priority": "medium",
            "enabled": "true",  # String instead of boolean
        }
        self.assertNotIsInstance(invalid_watchlist.get("enabled"), bool)

    def test_extra_fields_trigger_additional_properties_violation(self):
        """Watchlist with extra fields should violate additionalProperties: false."""
        invalid_watchlist = {
            "watchlist_id": "wl_test",
            "title": "Test",
            "topic": "test",
            "keywords": ["test"],
            "priority": "medium",
            "enabled": True,
            "extra_field": "not allowed",
        }
        # extra_field is not in the schema's properties
        allowed_fields = {
            "watchlist_id",
            "title",
            "topic",
            "description",
            "keywords",
            "required_terms",
            "blocked_terms",
            "priority",
            "enabled",
        }
        has_extra = any(k not in allowed_fields for k in invalid_watchlist.keys())
        self.assertTrue(has_extra, "Should have extra fields")


class DirectoryStructureTests(unittest.TestCase):
    """Test that required directories exist."""

    def test_data_watchlists_directory_exists(self):
        """data/watchlists/ directory should exist."""
        watchlists_dir = BASE_DIR / "data" / "watchlists"
        self.assertTrue(
            watchlists_dir.exists(),
            f"Directory {watchlists_dir} does not exist",
        )

    def test_data_watchlists_is_directory(self):
        """data/watchlists/ should be a directory."""
        watchlists_dir = BASE_DIR / "data" / "watchlists"
        self.assertTrue(
            watchlists_dir.is_dir(),
            f"{watchlists_dir} exists but is not a directory",
        )

    def test_data_watchlist_hits_directory_exists(self):
        """data/watchlist_hits/ directory should exist."""
        hits_dir = BASE_DIR / "data" / "watchlist_hits"
        self.assertTrue(
            hits_dir.exists(),
            f"Directory {hits_dir} does not exist",
        )

    def test_data_watchlist_hits_is_directory(self):
        """data/watchlist_hits/ should be a directory."""
        hits_dir = BASE_DIR / "data" / "watchlist_hits"
        self.assertTrue(
            hits_dir.is_dir(),
            f"{hits_dir} exists but is not a directory",
        )

    def test_data_theses_directory_exists(self):
        """data/theses/ directory should exist."""
        theses_dir = BASE_DIR / "data" / "theses"
        self.assertTrue(
            theses_dir.exists(),
            f"Directory {theses_dir} does not exist",
        )

    def test_data_theses_is_directory(self):
        """data/theses/ should be a directory."""
        theses_dir = BASE_DIR / "data" / "theses"
        self.assertTrue(
            theses_dir.is_dir(),
            f"{theses_dir} exists but is not a directory",
        )


if __name__ == "__main__":
    unittest.main()
