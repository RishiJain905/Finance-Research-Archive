"""Tests for memory_config.json.

Validates that the memory configuration schema file has correct default values
and structure as specified in the requirements.

Note: memory_config.json is a JSON Schema file that defines the configuration
structure and default values. The defaults are specified using the "default"
keyword in each property definition.
"""

import json
from pathlib import Path

import pytest

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "memory_config.json"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def memory_config_schema():
    """Load memory configuration schema file."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# =============================================================================
# TestConfigFileExistence
# =============================================================================


class TestConfigFileExistence:
    """Tests that config file exists and is valid JSON."""

    def test_config_file_exists(self):
        """memory_config.json file exists."""
        assert CONFIG_PATH.exists(), "memory_config.json not found"

    def test_config_file_is_valid_json(self, memory_config_schema):
        """Config file is valid JSON."""
        assert isinstance(memory_config_schema, dict)
        assert len(memory_config_schema) > 0


# =============================================================================
# TestColdStartDefaults (in Schema)
# =============================================================================


class TestColdStartDefaults:
    """Tests for cold_start section default values in schema."""

    def test_cold_start_section_exists(self, memory_config_schema):
        """cold_start section exists in schema properties."""
        props = memory_config_schema.get("properties", {})
        assert "cold_start" in props

    def test_cold_start_subschema_exists(self, memory_config_schema):
        """cold_start has a subschema with properties."""
        cold_start = memory_config_schema.get("properties", {}).get("cold_start", {})
        assert "properties" in cold_start

    def test_min_samples_for_learning_default(self, memory_config_schema):
        """min_samples_for_learning default is 10 in schema."""
        cold_start = memory_config_schema.get("properties", {}).get("cold_start", {})
        min_samples_prop = cold_start.get("properties", {}).get(
            "min_samples_for_learning", {}
        )
        assert min_samples_prop.get("default") == 10, (
            f"Expected default 10, got {min_samples_prop.get('default')}"
        )

    def test_full_learning_threshold_default(self, memory_config_schema):
        """full_learning_threshold default is 25 in schema."""
        cold_start = memory_config_schema.get("properties", {}).get("cold_start", {})
        threshold_prop = cold_start.get("properties", {}).get(
            "full_learning_threshold", {}
        )
        assert threshold_prop.get("default") == 25, (
            f"Expected default 25, got {threshold_prop.get('default')}"
        )

    def test_min_samples_for_learning_is_integer(self, memory_config_schema):
        """min_samples_for_learning is an integer in schema."""
        cold_start = memory_config_schema.get("properties", {}).get("cold_start", {})
        min_samples_prop = cold_start.get("properties", {}).get(
            "min_samples_for_learning", {}
        )
        assert min_samples_prop.get("type") == "integer"

    def test_full_learning_threshold_is_integer(self, memory_config_schema):
        """full_learning_threshold is an integer in schema."""
        cold_start = memory_config_schema.get("properties", {}).get("cold_start", {})
        threshold_prop = cold_start.get("properties", {}).get(
            "full_learning_threshold", {}
        )
        assert threshold_prop.get("type") == "integer"


# =============================================================================
# TestWeightsDefaults (in Schema)
# =============================================================================


class TestWeightsDefaults:
    """Tests for weights section default values in schema."""

    def test_weights_section_exists(self, memory_config_schema):
        """weights section exists in schema properties."""
        props = memory_config_schema.get("properties", {})
        assert "weights" in props

    def test_weights_subschema_exists(self, memory_config_schema):
        """weights has a subschema with properties."""
        weights = memory_config_schema.get("properties", {}).get("weights", {})
        assert "properties" in weights

    def test_accepted_weight_default(self, memory_config_schema):
        """accepted_weight default is 10 in schema."""
        weights = memory_config_schema.get("properties", {}).get("weights", {})
        weight_prop = weights.get("properties", {}).get("accepted_weight", {})
        assert weight_prop.get("default") == 10, (
            f"Expected default 10, got {weight_prop.get('default')}"
        )

    def test_rejected_weight_default(self, memory_config_schema):
        """rejected_weight default is 5 in schema."""
        weights = memory_config_schema.get("properties", {}).get("weights", {})
        weight_prop = weights.get("properties", {}).get("rejected_weight", {})
        assert weight_prop.get("default") == 5, (
            f"Expected default 5, got {weight_prop.get('default')}"
        )

    def test_filtered_weight_default(self, memory_config_schema):
        """filtered_weight default is 3 in schema."""
        weights = memory_config_schema.get("properties", {}).get("weights", {})
        weight_prop = weights.get("properties", {}).get("filtered_weight", {})
        assert weight_prop.get("default") == 3, (
            f"Expected default 3, got {weight_prop.get('default')}"
        )

    def test_review_weight_default(self, memory_config_schema):
        """review_weight default is 0 in schema."""
        weights = memory_config_schema.get("properties", {}).get("weights", {})
        weight_prop = weights.get("properties", {}).get("review_weight", {})
        assert weight_prop.get("default") == 0, (
            f"Expected default 0, got {weight_prop.get('default')}"
        )

    def test_human_multiplier_default(self, memory_config_schema):
        """human_multiplier default is 2.0 in schema."""
        weights = memory_config_schema.get("properties", {}).get("weights", {})
        multiplier_prop = weights.get("properties", {}).get("human_multiplier", {})
        assert multiplier_prop.get("default") == 2.0, (
            f"Expected default 2.0, got {multiplier_prop.get('default')}"
        )

    def test_human_multiplier_is_number(self, memory_config_schema):
        """human_multiplier is a number type in schema."""
        weights = memory_config_schema.get("properties", {}).get("weights", {})
        multiplier_prop = weights.get("properties", {}).get("human_multiplier", {})
        assert multiplier_prop.get("type") == "number"


# =============================================================================
# TestDecayDefaults (in Schema)
# =============================================================================


class TestDecayDefaults:
    """Tests for decay section default values in schema."""

    def test_decay_section_exists(self, memory_config_schema):
        """decay section exists in schema properties."""
        props = memory_config_schema.get("properties", {})
        assert "decay" in props

    def test_decay_enabled_default(self, memory_config_schema):
        """decay.enabled default is false in schema."""
        decay = memory_config_schema.get("properties", {}).get("decay", {})
        enabled_prop = decay.get("properties", {}).get("enabled", {})
        assert enabled_prop.get("default") is False, (
            f"Expected default False, got {enabled_prop.get('default')}"
        )

    def test_decay_enabled_is_boolean(self, memory_config_schema):
        """decay.enabled is boolean type in schema."""
        decay = memory_config_schema.get("properties", {}).get("decay", {})
        enabled_prop = decay.get("properties", {}).get("enabled", {})
        assert enabled_prop.get("type") == "boolean"

    def test_half_life_days_default(self, memory_config_schema):
        """half_life_days has default value in schema."""
        decay = memory_config_schema.get("properties", {}).get("decay", {})
        half_life_prop = decay.get("properties", {}).get("half_life_days", {})
        assert "default" in half_life_prop
        assert isinstance(half_life_prop.get("default"), int)
        assert half_life_prop.get("default") > 0

    def test_min_age_days_default(self, memory_config_schema):
        """min_age_days has default value in schema."""
        decay = memory_config_schema.get("properties", {}).get("decay", {})
        min_age_prop = decay.get("properties", {}).get("min_age_days", {})
        assert "default" in min_age_prop
        assert isinstance(min_age_prop.get("default"), int)
        assert min_age_prop.get("default") > 0


# =============================================================================
# TestLoggingDefaults (in Schema)
# =============================================================================


class TestLoggingDefaults:
    """Tests for logging section default values in schema."""

    def test_logging_section_exists(self, memory_config_schema):
        """logging section exists in schema properties."""
        props = memory_config_schema.get("properties", {})
        assert "logging" in props

    def test_log_updates_default(self, memory_config_schema):
        """log_updates default is true in schema."""
        logging = memory_config_schema.get("properties", {}).get("logging", {})
        log_updates_prop = logging.get("properties", {}).get("log_updates", {})
        assert log_updates_prop.get("default") is True, (
            f"Expected default True, got {log_updates_prop.get('default')}"
        )

    def test_log_updates_is_boolean(self, memory_config_schema):
        """log_updates is boolean type in schema."""
        logging = memory_config_schema.get("properties", {}).get("logging", {})
        log_updates_prop = logging.get("properties", {}).get("log_updates", {})
        assert log_updates_prop.get("type") == "boolean"

    def test_log_dir_default(self, memory_config_schema):
        """log_dir has default value in schema."""
        logging = memory_config_schema.get("properties", {}).get("logging", {})
        log_dir_prop = logging.get("properties", {}).get("log_dir", {})
        assert "default" in log_dir_prop
        assert log_dir_prop.get("default") == "logs/source_memory"


# =============================================================================
# TestTrustScoreBounds (in Schema)
# =============================================================================


class TestTrustScoreBounds:
    """Tests for trust_score_bounds section in schema."""

    def test_trust_score_bounds_section_exists(self, memory_config_schema):
        """trust_score_bounds section exists."""
        props = memory_config_schema.get("properties", {})
        assert "trust_score_bounds" in props

    def test_min_bound_default(self, memory_config_schema):
        """trust_score_bounds.min default is 1 in schema."""
        bounds = memory_config_schema.get("properties", {}).get(
            "trust_score_bounds", {}
        )
        min_prop = bounds.get("properties", {}).get("min", {})
        assert min_prop.get("default") == 1, (
            f"Expected default 1, got {min_prop.get('default')}"
        )

    def test_max_bound_default(self, memory_config_schema):
        """trust_score_bounds.max default is 100 in schema."""
        bounds = memory_config_schema.get("properties", {}).get(
            "trust_score_bounds", {}
        )
        max_prop = bounds.get("properties", {}).get("max", {})
        assert max_prop.get("default") == 100, (
            f"Expected default 100, got {max_prop.get('default')}"
        )


# =============================================================================
# TestConfigStructure
# =============================================================================


class TestConfigStructure:
    """Tests for overall config schema structure."""

    def test_has_required_top_level_keys(self, memory_config_schema):
        """Config schema has all required top-level keys."""
        props = memory_config_schema.get("properties", {})
        required_keys = [
            "cold_start",
            "weights",
            "decay",
            "logging",
        ]
        for key in required_keys:
            assert key in props, f"Missing top-level key: {key}"

    def test_config_is_not_empty(self, memory_config_schema):
        """Config schema dictionary is not empty."""
        assert len(memory_config_schema) > 0

    def test_all_sections_are_objects(self, memory_config_schema):
        """All top-level sections are objects in schema."""
        props = memory_config_schema.get("properties", {})
        sections = ["cold_start", "weights", "decay", "logging"]
        for section in sections:
            section_def = props.get(section, {})
            assert section_def.get("type") == "object", (
                f"{section} should be an object type"
            )


# =============================================================================
# TestConfigValues
# =============================================================================


class TestConfigValues:
    """Tests for specific configuration values in schema."""

    def test_blend_formula_is_valid_option(self, memory_config_schema):
        """blend_formula enum includes valid options."""
        cold_start = memory_config_schema.get("properties", {}).get("cold_start", {})
        blend_prop = cold_start.get("properties", {}).get("blend_formula", {})
        enum = blend_prop.get("enum", [])
        assert "linear" in enum
        assert "exponential" in enum
        assert blend_prop.get("default") == "linear"


# =============================================================================
# TestConfigSchema
# =============================================================================


class TestConfigSchema:
    """Tests for JSON schema compliance."""

    def test_config_has_schema_field(self, memory_config_schema):
        """Config has $schema field."""
        assert "$schema" in memory_config_schema

    def test_config_schema_is_draft_07(self, memory_config_schema):
        """Config uses JSON Schema draft-07."""
        schema = memory_config_schema.get("$schema", "")
        assert "draft-07" in schema

    def test_config_has_title(self, memory_config_schema):
        """Config has title."""
        assert "title" in memory_config_schema

    def test_config_has_description(self, memory_config_schema):
        """Config has description."""
        assert "description" in memory_config_schema

    def test_config_type_is_object(self, memory_config_schema):
        """Config type is object."""
        assert memory_config_schema.get("type") == "object"


# =============================================================================
# TestEdgeCases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases."""

    def test_all_default_values_are_json_compatible(self, memory_config_schema):
        """All default values in schema are JSON-compatible."""

        def check_defaults(obj):
            if isinstance(obj, dict):
                if "default" in obj:
                    # Verify the default is JSON serializable
                    default = obj["default"]
                    if default is not None:
                        json.dumps(default)  # Will raise if not serializable
                for value in obj.values():
                    check_defaults(value)
            elif isinstance(obj, list):
                for item in obj:
                    check_defaults(item)

        check_defaults(memory_config_schema)

    def test_config_can_be_parsed_multiple_times(self, memory_config_schema):
        """Config file can be loaded multiple times without issues."""
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            first_load = json.load(f)

        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            second_load = json.load(f)

        assert first_load == second_load

    def test_cold_start_min_less_than_full_learning(self, memory_config_schema):
        """min_samples_for_learning < full_learning_threshold in defaults."""
        cold_start = memory_config_schema.get("properties", {}).get("cold_start", {})
        min_samples = (
            cold_start.get("properties", {})
            .get("min_samples_for_learning", {})
            .get("default")
        )
        full_threshold = (
            cold_start.get("properties", {})
            .get("full_learning_threshold", {})
            .get("default")
        )

        assert min_samples < full_threshold, (
            f"min_samples ({min_samples}) should be less than "
            f"full_learning_threshold ({full_threshold})"
        )
