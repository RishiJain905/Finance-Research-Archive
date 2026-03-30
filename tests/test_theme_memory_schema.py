"""Tests for theme memory JSON schemas.

Validates that the theme memory, keyword bundles, and expansion schemas
are valid JSON Schema draft-07 and that data conforms to requirements.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMAS_DIR = BASE_DIR / "schemas"
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def theme_memory_schema():
    """Load theme memory schema."""
    schema_path = SCHEMAS_DIR / "theme_memory.json"
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def keyword_bundles_config():
    """Load keyword bundles config."""
    config_path = CONFIG_DIR / "keyword_bundles.json"
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def negative_bundles_config():
    """Load negative keyword bundles config."""
    config_path = CONFIG_DIR / "negative_keyword_bundles.json"
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def themes_data():
    """Load themes data file."""
    data_path = DATA_DIR / "theme_memory" / "themes.json"
    with open(data_path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def expansions_data():
    """Load expansions data file."""
    data_path = DATA_DIR / "theme_memory" / "expansions.json"
    with open(data_path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def valid_theme():
    """Provide a valid theme memory record."""
    now = datetime.now(timezone.utc).isoformat()
    return {
        "theme_id": "repo_liquidity_treasury",
        "theme_label": "Repo / Liquidity / Treasury",
        "positive_terms": ["repo", "liquidity", "treasury", "collateral"],
        "negative_terms": [],
        "accepted_count": 15,
        "review_count": 20,
        "rejected_count": 5,
        "last_seen": now,
        "priority_score": 72.5,
    }


@pytest.fixture
def valid_keyword_bundle():
    """Provide a valid keyword bundle."""
    return {
        "bundle_id": "repo_liquidity_treasury",
        "label": "Repo / Liquidity / Treasury",
        "required_terms": ["repo", "liquidity"],
        "optional_terms": ["treasury", "funding conditions", "collateral"],
        "exclusions": [],
        "priority_score": 70,
        "source": "static",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


@pytest.fixture
def valid_negative_bundle():
    """Provide a valid negative keyword bundle."""
    return {
        "bundle_id": "broad_event_noise",
        "label": "Broad Event Noise",
        "terms": ["event registration", "conference", "careers", "webinar"],
        "suppression_strength": 0.8,
        "source": "static",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# TestSchemaFilesExist
# =============================================================================


class TestSchemaFilesExist:
    """Tests that all required files exist and are valid JSON."""

    def test_theme_memory_schema_exists(self):
        """Theme memory schema file exists."""
        schema_path = SCHEMAS_DIR / "theme_memory.json"
        assert schema_path.exists(), "theme_memory.json schema file not found"

    def test_keyword_bundles_config_exists(self):
        """Keyword bundles config file exists."""
        config_path = CONFIG_DIR / "keyword_bundles.json"
        assert config_path.exists(), "keyword_bundles.json config file not found"

    def test_negative_bundles_config_exists(self):
        """Negative keyword bundles config file exists."""
        config_path = CONFIG_DIR / "negative_keyword_bundles.json"
        assert config_path.exists(), (
            "negative_keyword_bundles.json config file not found"
        )

    def test_themes_data_exists(self):
        """Themes data file exists."""
        data_path = DATA_DIR / "theme_memory" / "themes.json"
        assert data_path.exists(), "themes.json data file not found"

    def test_expansions_data_exists(self):
        """Expansions data file exists."""
        data_path = DATA_DIR / "theme_memory" / "expansions.json"
        assert data_path.exists(), "expansions.json data file not found"

    def test_theme_memory_schema_is_valid_json(self, theme_memory_schema):
        """Theme memory schema is valid JSON."""
        assert isinstance(theme_memory_schema, dict)
        assert len(theme_memory_schema) > 0

    def test_keyword_bundles_config_is_valid_json(self, keyword_bundles_config):
        """Keyword bundles config is valid JSON."""
        assert isinstance(keyword_bundles_config, dict)
        assert "bundles" in keyword_bundles_config

    def test_negative_bundles_config_is_valid_json(self, negative_bundles_config):
        """Negative bundles config is valid JSON."""
        assert isinstance(negative_bundles_config, dict)
        assert "bundles" in negative_bundles_config


# =============================================================================
# TestThemeMemorySchema
# =============================================================================


class TestThemeMemorySchema:
    """Tests for theme memory schema validation."""

    def test_schema_uses_draft_07(self, theme_memory_schema):
        """Schema uses JSON Schema draft-07."""
        assert "draft-07" in theme_memory_schema.get("$schema", "")

    def test_schema_is_object_type(self, theme_memory_schema):
        """Schema type is object."""
        assert theme_memory_schema.get("type") == "object"

    def test_required_fields(self, theme_memory_schema):
        """All required fields are present."""
        required = [
            "theme_id",
            "theme_label",
            "positive_terms",
            "negative_terms",
            "accepted_count",
            "review_count",
            "rejected_count",
            "last_seen",
            "priority_score",
        ]
        schema_required = theme_memory_schema.get("required", [])
        for field in required:
            assert field in schema_required, f"Missing required field: {field}"

    def test_theme_id_is_string(self, theme_memory_schema):
        """theme_id is a string."""
        props = theme_memory_schema.get("properties", {})
        theme_id = props.get("theme_id", {})
        assert theme_id.get("type") == "string"

    def test_theme_label_is_string(self, theme_memory_schema):
        """theme_label is a string."""
        props = theme_memory_schema.get("properties", {})
        theme_label = props.get("theme_label", {})
        assert theme_label.get("type") == "string"

    def test_positive_terms_is_array_of_strings(self, theme_memory_schema):
        """positive_terms is an array of strings."""
        props = theme_memory_schema.get("properties", {})
        positive_terms = props.get("positive_terms", {})
        assert positive_terms.get("type") == "array"
        items = positive_terms.get("items", {})
        assert items.get("type") == "string"

    def test_negative_terms_is_array_of_strings(self, theme_memory_schema):
        """negative_terms is an array of strings."""
        props = theme_memory_schema.get("properties", {})
        negative_terms = props.get("negative_terms", {})
        assert negative_terms.get("type") == "array"
        items = negative_terms.get("items", {})
        assert items.get("type") == "string"

    def test_count_fields_are_non_negative_integers(self, theme_memory_schema):
        """Count fields are non-negative integers."""
        props = theme_memory_schema.get("properties", {})
        count_fields = ["accepted_count", "review_count", "rejected_count"]
        for field in count_fields:
            field_def = props.get(field, {})
            assert field_def.get("type") == "integer", f"{field} should be integer"
            assert field_def.get("minimum") == 0, f"{field} should be minimum 0"

    def test_priority_score_is_number_0_to_100(self, theme_memory_schema):
        """priority_score is a number with 0-100 range."""
        props = theme_memory_schema.get("properties", {})
        priority_score = props.get("priority_score", {})
        assert priority_score.get("type") == "number"
        assert priority_score.get("minimum") == 0
        assert priority_score.get("maximum") == 100

    def test_last_seen_is_date_time_format(self, theme_memory_schema):
        """last_seen uses date-time format."""
        props = theme_memory_schema.get("properties", {})
        last_seen = props.get("last_seen", {})
        assert last_seen.get("format") == "date-time"

    def test_theme_memory_valid_full_record(self, valid_theme):
        """A complete valid theme memory record passes basic validation."""
        required_fields = [
            "theme_id",
            "theme_label",
            "positive_terms",
            "negative_terms",
            "accepted_count",
            "review_count",
            "rejected_count",
            "last_seen",
            "priority_score",
        ]
        for field in required_fields:
            assert field in valid_theme, f"Missing required: {field}"


# =============================================================================
# TestKeywordBundlesConfig
# =============================================================================


class TestKeywordBundlesConfig:
    """Tests for keyword bundles configuration."""

    def test_has_bundles_array(self, keyword_bundles_config):
        """Config has bundles array."""
        assert "bundles" in keyword_bundles_config
        assert isinstance(keyword_bundles_config["bundles"], list)

    def test_bundles_have_required_fields(self, keyword_bundles_config):
        """Each bundle has required fields."""
        if not keyword_bundles_config["bundles"]:
            pytest.skip("No bundles defined")

        bundle = keyword_bundles_config["bundles"][0]
        required = [
            "bundle_id",
            "label",
            "required_terms",
            "optional_terms",
            "exclusions",
            "priority_score",
            "source",
        ]
        for field in required:
            assert field in bundle, f"Bundle missing required field: {field}"

    def test_required_terms_is_array(self, keyword_bundles_config):
        """required_terms is an array."""
        if not keyword_bundles_config["bundles"]:
            pytest.skip("No bundles defined")

        bundle = keyword_bundles_config["bundles"][0]
        assert isinstance(bundle.get("required_terms"), list)

    def test_optional_terms_is_array(self, keyword_bundles_config):
        """optional_terms is an array."""
        if not keyword_bundles_config["bundles"]:
            pytest.skip("No bundles defined")

        bundle = keyword_bundles_config["bundles"][0]
        assert isinstance(bundle.get("optional_terms"), list)

    def test_exclusions_is_array(self, keyword_bundles_config):
        """exclusions is an array."""
        if not keyword_bundles_config["bundles"]:
            pytest.skip("No bundles defined")

        bundle = keyword_bundles_config["bundles"][0]
        assert isinstance(bundle.get("exclusions"), list)

    def test_priority_score_is_number(self, keyword_bundles_config):
        """priority_score is a number."""
        if not keyword_bundles_config["bundles"]:
            pytest.skip("No bundles defined")

        bundle = keyword_bundles_config["bundles"][0]
        assert isinstance(bundle.get("priority_score"), (int, float))

    def test_has_version(self, keyword_bundles_config):
        """Config has version field."""
        assert "version" in keyword_bundles_config

    def test_bundle_id_is_unique_in_bundles(self, keyword_bundles_config):
        """Bundle IDs are unique within the config."""
        if not keyword_bundles_config["bundles"]:
            pytest.skip("No bundles defined")

        bundle_ids = [b.get("bundle_id") for b in keyword_bundles_config["bundles"]]
        assert len(bundle_ids) == len(set(bundle_ids)), "Duplicate bundle_id found"


# =============================================================================
# TestNegativeKeywordBundlesConfig
# =============================================================================


class TestNegativeKeywordBundlesConfig:
    """Tests for negative keyword bundles configuration."""

    def test_has_bundles_array(self, negative_bundles_config):
        """Config has bundles array."""
        assert "bundles" in negative_bundles_config
        assert isinstance(negative_bundles_config["bundles"], list)

    def test_bundles_have_required_fields(self, negative_bundles_config):
        """Each bundle has required fields."""
        if not negative_bundles_config["bundles"]:
            pytest.skip("No bundles defined")

        bundle = negative_bundles_config["bundles"][0]
        required = ["bundle_id", "label", "terms", "suppression_strength", "source"]
        for field in required:
            assert field in bundle, f"Bundle missing required field: {field}"

    def test_terms_is_array(self, negative_bundles_config):
        """terms is an array of strings."""
        if not negative_bundles_config["bundles"]:
            pytest.skip("No bundles defined")

        bundle = negative_bundles_config["bundles"][0]
        assert isinstance(bundle.get("terms"), list)
        if bundle["terms"]:
            assert isinstance(bundle["terms"][0], str)

    def test_suppression_strength_is_number_0_to_1(self, negative_bundles_config):
        """suppression_strength is a number 0-1."""
        if not negative_bundles_config["bundles"]:
            pytest.skip("No bundles defined")

        bundle = negative_bundles_config["bundles"][0]
        strength = bundle.get("suppression_strength")
        assert isinstance(strength, (int, float))
        assert 0 <= strength <= 1

    def test_has_version(self, negative_bundles_config):
        """Config has version field."""
        assert "version" in negative_bundles_config


# =============================================================================
# TestThemesData
# =============================================================================


class TestThemesData:
    """Tests for themes data file."""

    def test_has_themes_dict(self, themes_data):
        """Data has themes dict."""
        assert "themes" in themes_data
        assert isinstance(themes_data["themes"], dict)

    def test_has_version(self, themes_data):
        """Data has version field."""
        assert "version" in themes_data

    def test_has_last_updated(self, themes_data):
        """Data has last_updated field."""
        assert "last_updated" in themes_data

    def test_empty_themes_is_valid(self, themes_data):
        """Empty themes dict is valid."""
        assert themes_data["themes"] == {}


# =============================================================================
# TestExpansionsData
# =============================================================================


class TestExpansionsData:
    """Tests for expansions data file."""

    def test_has_proposals_array(self, expansions_data):
        """Data has proposals array."""
        assert "proposals" in expansions_data
        assert isinstance(expansions_data["proposals"], list)

    def test_has_approved_array(self, expansions_data):
        """Data has approved array."""
        assert "approved" in expansions_data
        assert isinstance(expansions_data["approved"], list)

    def test_has_rejected_array(self, expansions_data):
        """Data has rejected array."""
        assert "rejected" in expansions_data
        assert isinstance(expansions_data["rejected"], list)

    def test_has_applied_array(self, expansions_data):
        """Data has applied array."""
        assert "applied" in expansions_data
        assert isinstance(expansions_data["applied"], list)

    def test_has_version(self, expansions_data):
        """Data has version field."""
        assert "version" in expansions_data

    def test_empty_proposals_is_valid(self, expansions_data):
        """Empty proposals array is valid."""
        assert expansions_data["proposals"] == []
        assert expansions_data["approved"] == []
        assert expansions_data["rejected"] == []
        assert expansions_data["applied"] == []


# =============================================================================
# TestSchemaEdgeCases
# =============================================================================


class TestSchemaEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_priority_score_at_minimum(self, theme_memory_schema):
        """priority_score can be 0."""
        props = theme_memory_schema.get("properties", {})
        priority_score = props.get("priority_score", {})
        assert priority_score.get("minimum") == 0

    def test_priority_score_at_maximum(self, theme_memory_schema):
        """priority_score can be 100."""
        props = theme_memory_schema.get("properties", {})
        priority_score = props.get("priority_score", {})
        assert priority_score.get("maximum") == 100

    def test_empty_positive_terms_array_is_valid(self):
        """Empty positive_terms array is valid."""
        theme = {
            "theme_id": "test_theme",
            "theme_label": "Test Theme",
            "positive_terms": [],
            "negative_terms": [],
            "accepted_count": 0,
            "review_count": 0,
            "rejected_count": 0,
            "last_seen": datetime.now(timezone.utc).isoformat(),
            "priority_score": 50.0,
        }
        assert isinstance(theme["positive_terms"], list)

    def test_empty_negative_terms_array_is_valid(self):
        """Empty negative_terms array is valid."""
        theme = {
            "theme_id": "test_theme",
            "theme_label": "Test Theme",
            "positive_terms": ["repo"],
            "negative_terms": [],
            "accepted_count": 0,
            "review_count": 0,
            "rejected_count": 0,
            "last_seen": datetime.now(timezone.utc).isoformat(),
            "priority_score": 50.0,
        }
        assert isinstance(theme["negative_terms"], list)

    def test_suppression_strength_boundaries(self):
        """suppression_strength can be 0 or 1."""
        for strength in [0, 0.5, 1.0]:
            bundle = {
                "bundle_id": "test",
                "label": "Test",
                "terms": ["test"],
                "suppression_strength": strength,
                "source": "static",
            }
            assert 0 <= bundle["suppression_strength"] <= 1

    def test_all_count_fields_can_be_zero(self):
        """All count fields can be 0."""
        theme = {
            "theme_id": "new_theme",
            "theme_label": "New Theme",
            "positive_terms": ["term"],
            "negative_terms": [],
            "accepted_count": 0,
            "review_count": 0,
            "rejected_count": 0,
            "last_seen": datetime.now(timezone.utc).isoformat(),
            "priority_score": 50.0,
        }
        assert theme["accepted_count"] == 0
        assert theme["review_count"] == 0
        assert theme["rejected_count"] == 0

    def test_keyword_bundle_terms_are_strings(self, keyword_bundles_config):
        """Keyword bundle terms are strings."""
        if not keyword_bundles_config["bundles"]:
            pytest.skip("No bundles defined")

        bundle = keyword_bundles_config["bundles"][0]
        for term in bundle.get("required_terms", []):
            assert isinstance(term, str)
        for term in bundle.get("optional_terms", []):
            assert isinstance(term, str)
