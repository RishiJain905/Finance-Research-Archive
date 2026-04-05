"""
Phase 1 Tests for Article-Quant Link Schema and Configuration (V2.7)

These tests validate:
1. Schema file exists and is valid JSON Schema draft-07
2. All required fields are present
3. Relationship enum validation
4. Score range (0-100)
5. Dimension enum validation
6. Config structure (topic_to_series, keyword_to_series keys present)
7. Config values are correct types
"""

import json
import os
import pytest
from jsonschema import Draft7Validator, ValidationError


# Paths
SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "schemas", "article_quant_link.json"
)
CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "config", "quant_linking_rules.json"
)
DATA_LINKS_DIR = os.path.join(
    os.path.dirname(__file__), "..", "data", "article_quant_links"
)


class TestSchemaFile:
    """Test that the schema file exists and is valid JSON Schema draft-07."""

    def test_schema_file_exists(self):
        """Schema file should exist at the expected path."""
        assert os.path.exists(SCHEMA_PATH), f"Schema file not found at {SCHEMA_PATH}"

    def test_schema_is_valid_json(self):
        """Schema file should be valid JSON."""
        with open(SCHEMA_PATH, "r") as f:
            schema = json.load(f)
        assert isinstance(schema, dict), "Schema should be a JSON object"

    def test_schema_is_draft_07(self):
        """Schema should declare JSON Schema draft-07."""
        with open(SCHEMA_PATH, "r") as f:
            schema = json.load(f)
        assert schema.get("$schema") == "http://json-schema.org/draft-07/schema#", (
            "Schema must be draft-07"
        )

    def test_schema_title_and_description(self):
        """Schema should have title and description."""
        with open(SCHEMA_PATH, "r") as f:
            schema = json.load(f)
        assert "title" in schema, "Schema should have a title"
        assert "description" in schema, "Schema should have a description"


class TestSchemaRequiredFields:
    """Test that all required fields are present in the schema."""

    @pytest.fixture
    def schema(self):
        with open(SCHEMA_PATH, "r") as f:
            return json.load(f)

    def test_required_fields_defined(self, schema):
        """Schema should define required fields."""
        assert "required" in schema, "Schema should define required fields"
        required = schema["required"]
        expected_required = [
            "link_id",
            "article_record_id",
            "quant_record_id",
            "relationship",
            "score",
            "matched_dimensions",
            "created_at",
        ]
        assert set(required) == set(expected_required), (
            f"Required fields should be {expected_required}, got {required}"
        )


class TestSchemaRelationshipEnum:
    """Test relationship field enum validation."""

    @pytest.fixture
    def schema(self):
        with open(SCHEMA_PATH, "r") as f:
            return json.load(f)

    def test_relationship_enum_exists(self, schema):
        """Relationship field should have enum validation."""
        props = schema.get("properties", {})
        assert "relationship" in props, "relationship field should be defined"
        assert "enum" in props["relationship"], "relationship should have enum"

    def test_relationship_enum_values(self, schema):
        """Relationship enum should contain correct values."""
        props = schema["properties"]
        enum_values = props["relationship"]["enum"]
        expected = ["supports", "context", "confirms", "weak_context"]
        assert set(enum_values) == set(expected), (
            f"Relationship enum should be {expected}, got {enum_values}"
        )


class TestSchemaScoreRange:
    """Test score field range validation."""

    @pytest.fixture
    def schema(self):
        with open(SCHEMA_PATH, "r") as f:
            return json.load(f)

    def test_score_has_minimum(self, schema):
        """Score should have minimum of 0."""
        props = schema.get("properties", {})
        assert "score" in props, "score field should be defined"
        assert "minimum" in props["score"], "score should have minimum"
        assert props["score"]["minimum"] == 0, "score minimum should be 0"

    def test_score_has_maximum(self, schema):
        """Score should have maximum of 100."""
        props = schema.get("properties", {})
        assert "score" in props, "score field should be defined"
        assert "maximum" in props["score"], "score should have maximum"
        assert props["score"]["maximum"] == 100, "score maximum should be 100"

    def test_score_type_is_number(self, schema):
        """Score type should be number."""
        props = schema.get("properties", {})
        assert props["score"].get("type") == "number", "score type should be number"


class TestSchemaMatchedDimensionsEnum:
    """Test matched_dimensions field enum validation."""

    @pytest.fixture
    def schema(self):
        with open(SCHEMA_PATH, "r") as f:
            return json.load(f)

    def test_matched_dimensions_enum_exists(self, schema):
        """matched_dimensions should have items with enum validation."""
        props = schema.get("properties", {})
        assert "matched_dimensions" in props, (
            "matched_dimensions field should be defined"
        )
        assert "items" in props["matched_dimensions"], (
            "matched_dimensions should have items"
        )
        assert "enum" in props["matched_dimensions"]["items"], (
            "matched_dimensions items should have enum"
        )

    def test_matched_dimensions_enum_values(self, schema):
        """matched_dimensions enum should contain correct values."""
        props = schema["properties"]
        enum_values = props["matched_dimensions"]["items"]["enum"]
        expected = ["topic", "time_window", "keyword_overlap", "event_alignment"]
        assert set(enum_values) == set(expected), (
            f"matched_dimensions enum should be {expected}, got {enum_values}"
        )


class TestSchemaFieldTypes:
    """Test that field types are correctly defined."""

    @pytest.fixture
    def schema(self):
        with open(SCHEMA_PATH, "r") as f:
            return json.load(f)

    def test_link_id_is_string(self, schema):
        """link_id should be a string type."""
        assert schema["properties"]["link_id"].get("type") == "string"

    def test_article_record_id_is_string(self, schema):
        """article_record_id should be a string type."""
        assert schema["properties"]["article_record_id"].get("type") == "string"

    def test_quant_record_id_is_string(self, schema):
        """quant_record_id should be a string type."""
        assert schema["properties"]["quant_record_id"].get("type") == "string"

    def test_event_id_is_string(self, schema):
        """event_id should be a string type (optional field)."""
        assert schema["properties"]["event_id"].get("type") == "string"

    def test_relationship_is_string(self, schema):
        """relationship should be a string type."""
        assert schema["properties"]["relationship"].get("type") == "string"

    def test_matched_dimensions_is_array(self, schema):
        """matched_dimensions should be an array type."""
        assert schema["properties"]["matched_dimensions"].get("type") == "array"

    def test_created_at_is_date_time_format(self, schema):
        """created_at should be a string with date-time format."""
        assert schema["properties"]["created_at"].get("type") == "string"
        assert schema["properties"]["created_at"].get("format") == "date-time"


class TestConfigFile:
    """Test that the config file exists and is valid."""

    def test_config_file_exists(self):
        """Config file should exist at the expected path."""
        assert os.path.exists(CONFIG_PATH), f"Config file not found at {CONFIG_PATH}"

    def test_config_is_valid_json(self):
        """Config file should be valid JSON."""
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)
        assert isinstance(config, dict), "Config should be a JSON object"


class TestConfigStructure:
    """Test that config has required top-level keys."""

    @pytest.fixture
    def config(self):
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)

    def test_config_has_topic_to_series(self, config):
        """Config should have topic_to_series key."""
        assert "topic_to_series" in config, "Config should have topic_to_series"

    def test_config_has_keyword_to_series(self, config):
        """Config should have keyword_to_series key."""
        assert "keyword_to_series" in config, "Config should have keyword_to_series"

    def test_config_has_scoring_bands(self, config):
        """Config should have scoring_bands key."""
        assert "scoring_bands" in config, "Config should have scoring_bands"

    def test_config_has_dimension_weights(self, config):
        """Config should have dimension_weights key."""
        assert "dimension_weights" in config, "Config should have dimension_weights"

    def test_config_has_time_window_days(self, config):
        """Config should have time_window_days key."""
        assert "time_window_days" in config, "Config should have time_window_days"


class TestConfigValues:
    """Test that config values are correct types."""

    @pytest.fixture
    def config(self):
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)

    def test_topic_to_series_is_dict(self, config):
        """topic_to_series should be a dictionary."""
        assert isinstance(config["topic_to_series"], dict), (
            "topic_to_series should be a dict"
        )

    def test_keyword_to_series_is_dict(self, config):
        """keyword_to_series should be a dictionary."""
        assert isinstance(config["keyword_to_series"], dict), (
            "keyword_to_series should be a dict"
        )

    def test_topic_to_series_values_are_lists(self, config):
        """topic_to_series values should be lists."""
        for key, value in config["topic_to_series"].items():
            assert isinstance(value, list), (
                f"topic_to_series['{key}'] should be a list, got {type(value)}"
            )

    def test_keyword_to_series_values_are_lists(self, config):
        """keyword_to_series values should be lists."""
        for key, value in config["keyword_to_series"].items():
            assert isinstance(value, list), (
                f"keyword_to_series['{key}'] should be a list, got {type(value)}"
            )

    def test_scoring_bands_is_dict(self, config):
        """scoring_bands should be a dictionary."""
        assert isinstance(config["scoring_bands"], dict), (
            "scoring_bands should be a dict"
        )

    def test_dimension_weights_is_dict(self, config):
        """dimension_weights should be a dictionary."""
        assert isinstance(config["dimension_weights"], dict), (
            "dimension_weights should be a dict"
        )

    def test_dimension_weights_values_are_numbers(self, config):
        """dimension_weights values should be numbers."""
        for key, value in config["dimension_weights"].items():
            assert isinstance(value, (int, float)), (
                f"dimension_weights['{key}'] should be a number, got {type(value)}"
            )

    def test_time_window_days_is_number(self, config):
        """time_window_days should be a number."""
        assert isinstance(config["time_window_days"], (int, float)), (
            f"time_window_days should be a number, got {type(config['time_window_days'])}"
        )


class TestConfigScoringBands:
    """Test scoring_bands structure and values."""

    @pytest.fixture
    def config(self):
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)

    def test_scoring_bands_has_strong(self, config):
        """scoring_bands should have 'strong' key."""
        assert "strong" in config["scoring_bands"]

    def test_scoring_bands_has_contextual(self, config):
        """scoring_bands should have 'contextual' key."""
        assert "contextual" in config["scoring_bands"]

    def test_scoring_bands_has_weak(self, config):
        """scoring_bands should have 'weak' key."""
        assert "weak" in config["scoring_bands"]

    def test_scoring_bands_has_ignore_below(self, config):
        """scoring_bands should have 'ignore_below' key."""
        assert "ignore_below" in config["scoring_bands"]

    def test_scoring_band_min_values_are_numbers(self, config):
        """Scoring band min values should be numbers."""
        for band_name in ["strong", "contextual", "weak"]:
            band = config["scoring_bands"][band_name]
            assert "min" in band, f"{band_name} should have 'min'"
            assert isinstance(band["min"], (int, float)), (
                f"{band_name}.min should be a number"
            )

    def test_scoring_band_relationships_are_valid(self, config):
        """Scoring band relationship values should match schema enum."""
        valid_relationships = ["supports", "context", "confirms", "weak_context"]
        for band_name in ["strong", "contextual", "weak"]:
            band = config["scoring_bands"][band_name]
            assert "relationship" in band, f"{band_name} should have 'relationship'"
            assert band["relationship"] in valid_relationships, (
                f"{band_name}.relationship should be one of {valid_relationships}"
            )


class TestDataDirectory:
    """Test that the data directory exists."""

    def test_data_links_directory_exists(self):
        """data/article_quant_links directory should exist."""
        assert os.path.exists(DATA_LINKS_DIR), (
            f"Data directory not found at {DATA_LINKS_DIR}"
        )

    def test_data_links_directory_is_directory(self):
        """data/article_quant_links should be a directory."""
        assert os.path.isdir(DATA_LINKS_DIR), f"{DATA_LINKS_DIR} should be a directory"


class TestSchemaValidationIntegration:
    """Integration test: validate sample data against schema."""

    @pytest.fixture
    def schema(self):
        with open(SCHEMA_PATH, "r") as f:
            return json.load(f)

    def test_valid_sample_passes_validation(self, schema):
        """A valid link object should pass schema validation."""
        validator = Draft7Validator(schema)

        valid_link = {
            "link_id": "link_001",
            "article_record_id": "art_001",
            "quant_record_id": "quant_001",
            "relationship": "supports",
            "score": 85.0,
            "matched_dimensions": ["topic", "time_window"],
            "created_at": "2026-04-03T10:00:00Z",
        }

        errors = list(validator.iter_errors(valid_link))
        assert len(errors) == 0, f"Valid link should pass: {errors}"

    def test_invalid_relationship_fails_validation(self, schema):
        """Invalid relationship value should fail validation."""
        validator = Draft7Validator(schema)

        invalid_link = {
            "link_id": "link_001",
            "article_record_id": "art_001",
            "quant_record_id": "quant_001",
            "relationship": "invalid_value",
            "score": 85.0,
            "matched_dimensions": ["topic"],
            "created_at": "2026-04-03T10:00:00Z",
        }

        errors = list(validator.iter_errors(invalid_link))
        assert len(errors) > 0, "Invalid relationship should fail validation"

    def test_score_below_zero_fails_validation(self, schema):
        """Score below 0 should fail validation."""
        validator = Draft7Validator(schema)

        invalid_link = {
            "link_id": "link_001",
            "article_record_id": "art_001",
            "quant_record_id": "quant_001",
            "relationship": "supports",
            "score": -5.0,
            "matched_dimensions": ["topic"],
            "created_at": "2026-04-03T10:00:00Z",
        }

        errors = list(validator.iter_errors(invalid_link))
        assert len(errors) > 0, "Score below 0 should fail validation"

    def test_score_above_100_fails_validation(self, schema):
        """Score above 100 should fail validation."""
        validator = Draft7Validator(schema)

        invalid_link = {
            "link_id": "link_001",
            "article_record_id": "art_001",
            "quant_record_id": "quant_001",
            "relationship": "supports",
            "score": 105.0,
            "matched_dimensions": ["topic"],
            "created_at": "2026-04-03T10:00:00Z",
        }

        errors = list(validator.iter_errors(invalid_link))
        assert len(errors) > 0, "Score above 100 should fail validation"

    def test_invalid_dimension_fails_validation(self, schema):
        """Invalid dimension value should fail validation."""
        validator = Draft7Validator(schema)

        invalid_link = {
            "link_id": "link_001",
            "article_record_id": "art_001",
            "quant_record_id": "quant_001",
            "relationship": "supports",
            "score": 85.0,
            "matched_dimensions": ["invalid_dimension"],
            "created_at": "2026-04-03T10:00:00Z",
        }

        errors = list(validator.iter_errors(invalid_link))
        assert len(errors) > 0, "Invalid dimension should fail validation"

    def test_missing_required_field_fails_validation(self, schema):
        """Missing required field should fail validation."""
        validator = Draft7Validator(schema)

        invalid_link = {
            "link_id": "link_001",
            "article_record_id": "art_001",
            # missing quant_record_id
            "relationship": "supports",
            "score": 85.0,
            "matched_dimensions": ["topic"],
            "created_at": "2026-04-03T10:00:00Z",
        }

        errors = list(validator.iter_errors(invalid_link))
        assert len(errors) > 0, "Missing required field should fail validation"
