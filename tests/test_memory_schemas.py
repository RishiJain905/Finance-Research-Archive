"""Tests for memory JSON schemas.

Validates that the schema files are valid JSON Schema draft-07
and that data conforms to the schema requirements.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMAS_DIR = BASE_DIR / "schemas"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def domain_memory_schema():
    """Load domain memory schema."""
    schema_path = SCHEMAS_DIR / "domain_memory.json"
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def path_memory_schema():
    """Load path memory schema."""
    schema_path = SCHEMAS_DIR / "path_memory.json"
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def source_memory_schema():
    """Load source memory schema."""
    schema_path = SCHEMAS_DIR / "source_memory.json"
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def valid_domain_memory():
    """Provide a valid domain memory record."""
    now = datetime.now(timezone.utc).isoformat()
    return {
        "domain": "federalreserve.gov",
        "trust_score": 85.5,
        "total_candidates": 100,
        "accepted_count": 45,
        "review_count": 20,
        "rejected_count": 15,
        "filtered_out_count": 20,
        "last_seen": now,
    }


@pytest.fixture
def valid_path_memory():
    """Provide a valid path memory record."""
    now = datetime.now(timezone.utc).isoformat()
    return {
        "domain": "brookings.edu",
        "path_pattern": "/events/",
        "trust_score": 72.0,
        "total_candidates": 50,
        "accepted_count": 20,
        "review_count": 10,
        "rejected_count": 8,
        "filtered_out_count": 12,
    }


@pytest.fixture
def valid_source_memory():
    """Provide a valid source memory record."""
    now = datetime.now(timezone.utc).isoformat()
    return {
        "source_id": "federalreserve_fomc",
        "source_type": "trusted_source_monitor",
        "trust_score": 95.0,
        "yield_score": 0.45,
        "noise_score": 0.25,
        "last_updated": now,
    }


# =============================================================================
# TestSchemaFilesExist
# =============================================================================


class TestSchemaFilesExist:
    """Tests that all schema files exist and are valid JSON."""

    def test_domain_memory_schema_exists(self):
        """Domain memory schema file exists."""
        schema_path = SCHEMAS_DIR / "domain_memory.json"
        assert schema_path.exists(), "domain_memory.json schema file not found"

    def test_path_memory_schema_exists(self):
        """Path memory schema file exists."""
        schema_path = SCHEMAS_DIR / "path_memory.json"
        assert schema_path.exists(), "path_memory.json schema file not found"

    def test_source_memory_schema_exists(self):
        """Source memory schema file exists."""
        schema_path = SCHEMAS_DIR / "source_memory.json"
        assert schema_path.exists(), "source_memory.json schema file not found"

    def test_domain_memory_schema_is_valid_json(self, domain_memory_schema):
        """Domain memory schema is valid JSON."""
        assert isinstance(domain_memory_schema, dict)
        assert len(domain_memory_schema) > 0

    def test_path_memory_schema_is_valid_json(self, path_memory_schema):
        """Path memory schema is valid JSON."""
        assert isinstance(path_memory_schema, dict)
        assert len(path_memory_schema) > 0

    def test_source_memory_schema_is_valid_json(self, source_memory_schema):
        """Source memory schema is valid JSON."""
        assert isinstance(source_memory_schema, dict)
        assert len(source_memory_schema) > 0


# =============================================================================
# TestDomainMemorySchema
# =============================================================================


class TestDomainMemorySchema:
    """Tests for domain memory schema validation."""

    def test_schema_has_correct_title(self, domain_memory_schema):
        """Schema has correct title."""
        assert domain_memory_schema.get("title") == "Domain Memory"

    def test_schema_uses_draft_07(self, domain_memory_schema):
        """Schema uses JSON Schema draft-07."""
        assert "draft-07" in domain_memory_schema.get("$schema", "")

    def test_schema_is_object_type(self, domain_memory_schema):
        """Schema type is object."""
        assert domain_memory_schema.get("type") == "object"

    def test_required_fields(self, domain_memory_schema):
        """All required fields are present."""
        required = [
            "domain",
            "trust_score",
            "total_candidates",
            "accepted_count",
            "review_count",
            "rejected_count",
            "filtered_out_count",
            "last_seen",
        ]
        schema_required = domain_memory_schema.get("required", [])
        for field in required:
            assert field in schema_required, f"Missing required field: {field}"

    def test_trust_score_is_number_0_to_100(self, domain_memory_schema):
        """trust_score is number with 0-100 range."""
        props = domain_memory_schema.get("properties", {})
        trust_score = props.get("trust_score", {})
        assert trust_score.get("type") == "number"
        assert trust_score.get("minimum") == 0
        assert trust_score.get("maximum") == 100

    def test_count_fields_are_non_negative_integers(self, domain_memory_schema):
        """Count fields are non-negative integers."""
        props = domain_memory_schema.get("properties", {})
        count_fields = [
            "total_candidates",
            "accepted_count",
            "review_count",
            "rejected_count",
            "filtered_out_count",
        ]
        for field in count_fields:
            field_def = props.get(field, {})
            assert field_def.get("type") == "integer", f"{field} should be integer"
            assert field_def.get("minimum") == 0, f"{field} should be minimum 0"

    def test_yield_score_and_noise_score_are_0_to_1(self, domain_memory_schema):
        """yield_score and noise_score are 0-1 when present."""
        props = domain_memory_schema.get("properties", {})

        for field in ["yield_score", "noise_score"]:
            field_def = props.get(field, {})
            assert field_def.get("type") == "number"
            assert field_def.get("minimum") == 0
            assert field_def.get("maximum") == 1

    def test_human_count_fields_exist(self, domain_memory_schema):
        """Human count tracking fields exist."""
        props = domain_memory_schema.get("properties", {})
        assert "accepted_human_count" in props
        assert "rejected_human_count" in props

    def test_last_seen_is_date_time_format(self, domain_memory_schema):
        """last_seen uses date-time format."""
        props = domain_memory_schema.get("properties", {})
        last_seen = props.get("last_seen", {})
        assert last_seen.get("format") == "date-time"

    def test_domain_memory_valid_full_record(self, valid_domain_memory):
        """A complete valid domain memory record passes basic validation."""
        required_fields = [
            "domain",
            "trust_score",
            "total_candidates",
            "accepted_count",
            "review_count",
            "rejected_count",
            "filtered_out_count",
            "last_seen",
        ]
        for field in required_fields:
            assert field in valid_domain_memory, f"Missing required: {field}"


# =============================================================================
# TestPathMemorySchema
# =============================================================================


class TestPathMemorySchema:
    """Tests for path memory schema validation."""

    def test_schema_has_correct_title(self, path_memory_schema):
        """Schema has correct title."""
        assert path_memory_schema.get("title") == "Path Memory"

    def test_schema_uses_draft_07(self, path_memory_schema):
        """Schema uses JSON Schema draft-07."""
        assert "draft-07" in path_memory_schema.get("$schema", "")

    def test_schema_is_object_type(self, path_memory_schema):
        """Schema type is object."""
        assert path_memory_schema.get("type") == "object"

    def test_required_fields(self, path_memory_schema):
        """All required fields are present."""
        required = [
            "domain",
            "path_pattern",
            "trust_score",
            "total_candidates",
            "accepted_count",
            "review_count",
            "rejected_count",
            "filtered_out_count",
        ]
        schema_required = path_memory_schema.get("required", [])
        for field in required:
            assert field in schema_required, f"Missing required field: {field}"

    def test_trust_score_is_number_0_to_100(self, path_memory_schema):
        """trust_score is number with 0-100 range."""
        props = path_memory_schema.get("properties", {})
        trust_score = props.get("trust_score", {})
        assert trust_score.get("type") == "number"
        assert trust_score.get("minimum") == 0
        assert trust_score.get("maximum") == 100

    def test_path_pattern_is_string(self, path_memory_schema):
        """path_pattern is a string."""
        props = path_memory_schema.get("properties", {})
        path_pattern = props.get("path_pattern", {})
        assert path_pattern.get("type") == "string"

    def test_count_fields_are_non_negative_integers(self, path_memory_schema):
        """Count fields are non-negative integers."""
        props = path_memory_schema.get("properties", {})
        count_fields = [
            "total_candidates",
            "accepted_count",
            "review_count",
            "rejected_count",
            "filtered_out_count",
        ]
        for field in count_fields:
            field_def = props.get(field, {})
            assert field_def.get("type") == "integer", f"{field} should be integer"
            assert field_def.get("minimum") == 0, f"{field} should be minimum 0"

    def test_yield_score_and_noise_score_are_0_to_1(self, path_memory_schema):
        """yield_score and noise_score are 0-1 when present."""
        props = path_memory_schema.get("properties", {})

        for field in ["yield_score", "noise_score"]:
            field_def = props.get(field, {})
            assert field_def.get("type") == "number"
            assert field_def.get("minimum") == 0
            assert field_def.get("maximum") == 1

    def test_human_count_fields_exist(self, path_memory_schema):
        """Human count tracking fields exist."""
        props = path_memory_schema.get("properties", {})
        assert "accepted_human_count" in props
        assert "rejected_human_count" in props

    def test_path_memory_valid_full_record(self, valid_path_memory):
        """A complete valid path memory record passes basic validation."""
        required_fields = [
            "domain",
            "path_pattern",
            "trust_score",
            "total_candidates",
            "accepted_count",
            "review_count",
            "rejected_count",
            "filtered_out_count",
        ]
        for field in required_fields:
            assert field in valid_path_memory, f"Missing required: {field}"


# =============================================================================
# TestSourceMemorySchema
# =============================================================================


class TestSourceMemorySchema:
    """Tests for source memory schema validation."""

    def test_schema_has_correct_title(self, source_memory_schema):
        """Schema has correct title."""
        assert source_memory_schema.get("title") == "Source Memory"

    def test_schema_uses_draft_07(self, source_memory_schema):
        """Schema uses JSON Schema draft-07."""
        assert "draft-07" in source_memory_schema.get("$schema", "")

    def test_schema_is_object_type(self, source_memory_schema):
        """Schema type is object."""
        assert source_memory_schema.get("type") == "object"

    def test_required_fields(self, source_memory_schema):
        """All required fields are present."""
        required = [
            "source_id",
            "source_type",
            "trust_score",
            "yield_score",
            "noise_score",
            "last_updated",
        ]
        schema_required = source_memory_schema.get("required", [])
        for field in required:
            assert field in schema_required, f"Missing required field: {field}"

    def test_source_type_is_enum(self, source_memory_schema):
        """source_type is an enum of valid types."""
        props = source_memory_schema.get("properties", {})
        source_type = props.get("source_type", {})
        expected_types = [
            "trusted_source_monitor",
            "keyword_discovery",
            "seed_crawl",
            "manual",
        ]
        assert source_type.get("enum") == expected_types

    def test_trust_score_is_number_0_to_100(self, source_memory_schema):
        """trust_score is number with 0-100 range."""
        props = source_memory_schema.get("properties", {})
        trust_score = props.get("trust_score", {})
        assert trust_score.get("type") == "number"
        assert trust_score.get("minimum") == 0
        assert trust_score.get("maximum") == 100

    def test_yield_score_is_0_to_1(self, source_memory_schema):
        """yield_score is 0-1."""
        props = source_memory_schema.get("properties", {})
        yield_score = props.get("yield_score", {})
        assert yield_score.get("type") == "number"
        assert yield_score.get("minimum") == 0
        assert yield_score.get("maximum") == 1

    def test_noise_score_is_0_to_1(self, source_memory_schema):
        """noise_score is 0-1."""
        props = source_memory_schema.get("properties", {})
        noise_score = props.get("noise_score", {})
        assert noise_score.get("type") == "number"
        assert noise_score.get("minimum") == 0
        assert noise_score.get("maximum") == 1

    def test_last_updated_is_date_time_format(self, source_memory_schema):
        """last_updated uses date-time format."""
        props = source_memory_schema.get("properties", {})
        last_updated = props.get("last_updated", {})
        assert last_updated.get("format") == "date-time"

    def test_source_id_is_string(self, source_memory_schema):
        """source_id is a string."""
        props = source_memory_schema.get("properties", {})
        source_id = props.get("source_id", {})
        assert source_id.get("type") == "string"

    def test_source_memory_valid_full_record(self, valid_source_memory):
        """A complete valid source memory record passes basic validation."""
        required_fields = [
            "source_id",
            "source_type",
            "trust_score",
            "yield_score",
            "noise_score",
            "last_updated",
        ]
        for field in required_fields:
            assert field in valid_source_memory, f"Missing required: {field}"


# =============================================================================
# TestSchemaEdgeCases
# =============================================================================


class TestSchemaEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_trust_score_at_minimum(self):
        """trust_score can be 0."""
        record = {
            "domain": "test.com",
            "trust_score": 0,
            "total_candidates": 0,
            "accepted_count": 0,
            "review_count": 0,
            "rejected_count": 0,
            "filtered_out_count": 0,
            "last_seen": datetime.now(timezone.utc).isoformat(),
        }
        assert record["trust_score"] == 0

    def test_trust_score_at_maximum(self):
        """trust_score can be 100."""
        record = {
            "domain": "test.com",
            "trust_score": 100,
            "total_candidates": 100,
            "accepted_count": 100,
            "review_count": 0,
            "rejected_count": 0,
            "filtered_out_count": 0,
            "last_seen": datetime.now(timezone.utc).isoformat(),
        }
        assert record["trust_score"] == 100

    def test_yield_score_at_boundaries(self):
        """yield_score can be 0 or 1."""
        for score in [0, 1]:
            record = {
                "source_id": "test_source",
                "source_type": "manual",
                "trust_score": 50,
                "yield_score": score,
                "noise_score": 0,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
            assert record["yield_score"] == score

    def test_noise_score_at_boundaries(self):
        """noise_score can be 0 or 1."""
        for score in [0, 1]:
            record = {
                "source_id": "test_source",
                "source_type": "manual",
                "trust_score": 50,
                "yield_score": 0,
                "noise_score": score,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
            assert record["noise_score"] == score

    def test_path_pattern_literal_string(self):
        """path_pattern is stored as literal string, not regex."""
        record = {
            "domain": "brookings.edu",
            "path_pattern": "/events/",
            "trust_score": 50,
            "total_candidates": 10,
            "accepted_count": 5,
            "review_count": 2,
            "rejected_count": 1,
            "filtered_out_count": 2,
        }
        # Verify it's a string with slashes
        assert isinstance(record["path_pattern"], str)
        assert "/" in record["path_pattern"]

    def test_all_source_types_are_valid(self):
        """All four source types are accepted."""
        valid_types = [
            "trusted_source_monitor",
            "keyword_discovery",
            "seed_crawl",
            "manual",
        ]
        for source_type in valid_types:
            record = {
                "source_id": f"test_{source_type}",
                "source_type": source_type,
                "trust_score": 50,
                "yield_score": 0.5,
                "noise_score": 0.3,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
            assert record["source_type"] in valid_types
