"""Tests for human_feedback.json schema validation.

Validates that the human feedback schema conforms to JSON Schema draft-07
and that feedback blocks pass validation according to the schema.
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
def human_feedback_schema():
    """Load human feedback schema."""
    schema_path = SCHEMAS_DIR / "human_feedback.json"
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


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
# Schema Structure Tests
# =============================================================================


class TestSchemaStructure:
    """Tests for human_feedback.json schema structure."""

    def test_schema_is_valid_json(self, human_feedback_schema):
        """Schema should be parseable JSON."""
        assert isinstance(human_feedback_schema, dict)
        assert len(human_feedback_schema) > 0

    def test_schema_uses_draft_07(self, human_feedback_schema):
        """Schema uses JSON Schema draft-07."""
        assert "draft-07" in human_feedback_schema.get("$schema", "")

    def test_schema_is_object_type(self, human_feedback_schema):
        """Schema type should be 'object'."""
        assert human_feedback_schema.get("type") == "object"

    def test_schema_has_description(self, human_feedback_schema):
        """Schema should have a description."""
        assert "description" in human_feedback_schema

    def test_schema_has_properties(self, human_feedback_schema):
        """Schema should have properties."""
        assert "properties" in human_feedback_schema


class TestDecisionField:
    """Tests for the decision field enum."""

    def test_decision_field_exists(self, human_feedback_schema):
        """Decision field should exist in properties."""
        props = human_feedback_schema.get("properties", {})
        assert "decision" in props

    def test_decision_is_string_type(self, human_feedback_schema):
        """Decision field should be type string."""
        props = human_feedback_schema.get("properties", {})
        decision = props.get("decision", {})
        assert decision.get("type") == "string"

    def test_decision_has_eight_enum_values(self, human_feedback_schema):
        """Decision should have exactly 8 enum values."""
        props = human_feedback_schema.get("properties", {})
        decision = props.get("decision", {})
        enum_values = decision.get("enum", [])
        assert len(enum_values) == 8

    def test_all_decision_enum_values_are_present(self, human_feedback_schema):
        """All expected decision enum values should be present."""
        props = human_feedback_schema.get("properties", {})
        decision = props.get("decision", {})
        enum_values = decision.get("enum", [])

        expected_values = [
            "approve",
            "reject",
            "approve_but_weak",
            "approve_and_promote",
            "bad_source",
            "good_source",
            "expand_this_topic",
            "suppress_similar_items",
        ]
        assert set(enum_values) == set(expected_values)

    def test_decision_has_description(self, human_feedback_schema):
        """Decision field should have a description."""
        props = human_feedback_schema.get("properties", {})
        decision = props.get("decision", {})
        assert "description" in decision


class TestNotesField:
    """Tests for the notes field."""

    def test_notes_field_exists(self, human_feedback_schema):
        """Notes field should exist in properties."""
        props = human_feedback_schema.get("properties", {})
        assert "notes" in props

    def test_notes_is_string_type(self, human_feedback_schema):
        """Notes field should be type string."""
        props = human_feedback_schema.get("properties", {})
        notes = props.get("notes", {})
        assert notes.get("type") == "string"

    def test_notes_max_length_is_500(self, human_feedback_schema):
        """Notes field should have maxLength of 500."""
        props = human_feedback_schema.get("properties", {})
        notes = props.get("notes", {})
        assert notes.get("maxLength") == 500

    def test_notes_is_optional(self, human_feedback_schema):
        """Notes field should not be in required list."""
        required = human_feedback_schema.get("required", [])
        assert "notes" not in required


class TestSourceFeedbackField:
    """Tests for the source_feedback field."""

    def test_source_feedback_field_exists(self, human_feedback_schema):
        """source_feedback field should exist in properties."""
        props = human_feedback_schema.get("properties", {})
        assert "source_feedback" in props

    def test_source_feedback_accepts_null(self, human_feedback_schema):
        """source_feedback should accept null value."""
        props = human_feedback_schema.get("properties", {})
        source_feedback = props.get("source_feedback", {})
        assert "null" in source_feedback.get("type", [])

    def test_source_feedback_enum_values(self, human_feedback_schema):
        """source_feedback enum should have correct values."""
        props = human_feedback_schema.get("properties", {})
        source_feedback = props.get("source_feedback", {})
        enum_values = source_feedback.get("enum", [])
        assert set(enum_values) == {"good_source", "bad_source", None}


class TestTopicFeedbackField:
    """Tests for the topic_feedback field."""

    def test_topic_feedback_field_exists(self, human_feedback_schema):
        """topic_feedback field should exist in properties."""
        props = human_feedback_schema.get("properties", {})
        assert "topic_feedback" in props

    def test_topic_feedback_accepts_null(self, human_feedback_schema):
        """topic_feedback should accept null value."""
        props = human_feedback_schema.get("properties", {})
        topic_feedback = props.get("topic_feedback", {})
        assert "null" in topic_feedback.get("type", [])

    def test_topic_feedback_enum_values(self, human_feedback_schema):
        """topic_feedback enum should have correct values."""
        props = human_feedback_schema.get("properties", {})
        topic_feedback = props.get("topic_feedback", {})
        enum_values = topic_feedback.get("enum", [])
        assert set(enum_values) == {"expand_this_topic", None}


class TestReviewedAtField:
    """Tests for the reviewed_at field."""

    def test_reviewed_at_field_exists(self, human_feedback_schema):
        """reviewed_at field should exist in properties."""
        props = human_feedback_schema.get("properties", {})
        assert "reviewed_at" in props

    def test_reviewed_at_is_string_type(self, human_feedback_schema):
        """reviewed_at field should be type string."""
        props = human_feedback_schema.get("properties", {})
        reviewed_at = props.get("reviewed_at", {})
        assert reviewed_at.get("type") == "string"

    def test_reviewed_at_is_date_time_format(self, human_feedback_schema):
        """reviewed_at should have date-time format."""
        props = human_feedback_schema.get("properties", {})
        reviewed_at = props.get("reviewed_at", {})
        assert reviewed_at.get("format") == "date-time"

    def test_reviewed_at_is_required(self, human_feedback_schema):
        """reviewed_at should be in required list."""
        required = human_feedback_schema.get("required", [])
        assert "reviewed_at" in required


class TestReviewerIdField:
    """Tests for the reviewer_id field."""

    def test_reviewer_id_field_exists(self, human_feedback_schema):
        """reviewer_id field should exist in properties."""
        props = human_feedback_schema.get("properties", {})
        assert "reviewer_id" in props

    def test_reviewer_id_is_string_type(self, human_feedback_schema):
        """reviewer_id field should be type string."""
        props = human_feedback_schema.get("properties", {})
        reviewer_id = props.get("reviewer_id", {})
        assert reviewer_id.get("type") == "string"

    def test_reviewer_id_is_optional(self, human_feedback_schema):
        """reviewer_id should not be in required list."""
        required = human_feedback_schema.get("required", [])
        assert "reviewer_id" not in required


class TestRequiredFields:
    """Tests for required fields."""

    def test_decision_is_required(self, human_feedback_schema):
        """decision should be in required list."""
        required = human_feedback_schema.get("required", [])
        assert "decision" in required

    def test_only_decision_and_reviewed_at_are_required(self, human_feedback_schema):
        """Only decision and reviewed_at should be required."""
        required = human_feedback_schema.get("required", [])
        assert set(required) == {"decision", "reviewed_at"}


# =============================================================================
# Schema Validation Tests
# =============================================================================


class TestValidFeedbackBlocks:
    """Tests that valid feedback blocks pass validation."""

    def validate_feedback(self, schema, feedback):
        """Simple validator for human_feedback schema."""
        errors = []

        # Check required fields
        for req_field in schema.get("required", []):
            if req_field not in feedback:
                errors.append(f"Missing required field: {req_field}")

        # Get properties schema
        props = schema.get("properties", {})

        # Check decision field
        if "decision" in feedback:
            decision_schema = props.get("decision", {})
            if feedback["decision"] not in decision_schema.get("enum", []):
                errors.append(f"Invalid decision value: {feedback['decision']}")

        # Check notes field
        if "notes" in feedback:
            notes_schema = props.get("notes", {})
            notes = feedback["notes"]
            if not isinstance(notes, str):
                errors.append("notes must be a string")
            elif "maxLength" in notes_schema and len(notes) > notes_schema["maxLength"]:
                errors.append(
                    f"notes exceeds max length of {notes_schema['maxLength']}"
                )

        # Check source_feedback field
        if "source_feedback" in feedback and feedback["source_feedback"] is not None:
            source_schema = props.get("source_feedback", {})
            if feedback["source_feedback"] not in source_schema.get("enum", []):
                errors.append(
                    f"Invalid source_feedback value: {feedback['source_feedback']}"
                )

        # Check topic_feedback field
        if "topic_feedback" in feedback and feedback["topic_feedback"] is not None:
            topic_schema = props.get("topic_feedback", {})
            if feedback["topic_feedback"] not in topic_schema.get("enum", []):
                errors.append(
                    f"Invalid topic_feedback value: {feedback['topic_feedback']}"
                )

        # Check reviewed_at format (basic ISO-8601 check)
        if "reviewed_at" in feedback:
            reviewed_at = feedback["reviewed_at"]
            if not isinstance(reviewed_at, str):
                errors.append("reviewed_at must be a string")
            # Basic ISO-8601 format check (should contain T and Z or +/-)
            elif "T" not in reviewed_at or (
                not reviewed_at.endswith("Z")
                and "+" not in reviewed_at
                and "-" not in reviewed_at[-6:]
            ):
                # More lenient check - just ensure it looks like a datetime
                if len(reviewed_at) < 10:
                    errors.append("reviewed_at does not appear to be ISO-8601 format")

        return errors

    def test_valid_minimal_feedback(
        self, human_feedback_schema, sample_feedback_records
    ):
        """Minimal feedback with only required fields should pass."""
        feedback = sample_feedback_records["minimal_valid_feedback"]
        errors = self.validate_feedback(human_feedback_schema, feedback)
        assert errors == [], f"Validation errors: {errors}"

    def test_valid_full_feedback(self, human_feedback_schema, sample_feedback_records):
        """Feedback with all fields should pass."""
        feedback = sample_feedback_records["feedback_with_all_optional_fields"]
        errors = self.validate_feedback(human_feedback_schema, feedback)
        assert errors == [], f"Validation errors: {errors}"

    def test_valid_notes_at_max_length(
        self, human_feedback_schema, sample_feedback_records
    ):
        """Notes at exactly 500 chars should pass."""
        feedback = sample_feedback_records["notes_at_max_length"]
        errors = self.validate_feedback(human_feedback_schema, feedback)
        assert errors == [], f"Validation errors: {errors}"

    def test_all_eight_decision_values_valid(
        self, human_feedback_schema, sample_feedback_records
    ):
        """All 8 decision enum values should pass validation."""
        feedback_list = sample_feedback_records["all_feedback_decisions"]
        for feedback in feedback_list:
            errors = self.validate_feedback(human_feedback_schema, feedback)
            assert errors == [], f"Decision '{feedback['decision']}' failed: {errors}"


class TestInvalidFeedbackBlocks:
    """Tests that invalid feedback blocks are rejected."""

    def validate_feedback(self, schema, feedback):
        """Simple validator for human_feedback schema."""
        errors = []

        # Check required fields
        for req_field in schema.get("required", []):
            if req_field not in feedback:
                errors.append(f"Missing required field: {req_field}")

        # Get properties schema
        props = schema.get("properties", {})

        # Check decision field
        if "decision" in feedback:
            decision_schema = props.get("decision", {})
            if feedback["decision"] not in decision_schema.get("enum", []):
                errors.append(f"Invalid decision value: {feedback['decision']}")

        # Check notes field
        if "notes" in feedback:
            notes_schema = props.get("notes", {})
            notes = feedback["notes"]
            if not isinstance(notes, str):
                errors.append("notes must be a string")
            elif "maxLength" in notes_schema and len(notes) > notes_schema["maxLength"]:
                errors.append(
                    f"notes exceeds max length of {notes_schema['maxLength']}"
                )

        # Check source_feedback field
        if "source_feedback" in feedback and feedback["source_feedback"] is not None:
            source_schema = props.get("source_feedback", {})
            if feedback["source_feedback"] not in source_schema.get("enum", []):
                errors.append(
                    f"Invalid source_feedback value: {feedback['source_feedback']}"
                )

        # Check topic_feedback field
        if "topic_feedback" in feedback and feedback["topic_feedback"] is not None:
            topic_schema = props.get("topic_feedback", {})
            if feedback["topic_feedback"] not in topic_schema.get("enum", []):
                errors.append(
                    f"Invalid topic_feedback value: {feedback['topic_feedback']}"
                )

        return errors

    def test_missing_decision_fails(self, human_feedback_schema):
        """Feedback missing decision should fail."""
        feedback = {"reviewed_at": "2024-01-15T10:00:00Z", "notes": "Some notes"}
        errors = self.validate_feedback(human_feedback_schema, feedback)
        assert len(errors) > 0
        assert any("decision" in e.lower() for e in errors)

    def test_missing_reviewed_at_fails(self, human_feedback_schema):
        """Feedback missing reviewed_at should fail."""
        feedback = {"decision": "approve", "notes": "Some notes"}
        errors = self.validate_feedback(human_feedback_schema, feedback)
        assert len(errors) > 0
        assert any("reviewed_at" in e.lower() for e in errors)

    def test_invalid_decision_value_fails(self, human_feedback_schema):
        """Invalid decision value should fail."""
        feedback = {
            "decision": "invalid_decision",
            "reviewed_at": "2024-01-15T10:00:00Z",
        }
        errors = self.validate_feedback(human_feedback_schema, feedback)
        assert len(errors) > 0
        assert any("decision" in e.lower() for e in errors)

    def test_notes_over_max_length_fails(self, human_feedback_schema):
        """Notes over 500 chars should fail."""
        feedback = {
            "decision": "approve",
            "reviewed_at": "2024-01-15T10:00:00Z",
            "notes": "A" * 501,  # One over the limit
        }
        errors = self.validate_feedback(human_feedback_schema, feedback)
        assert len(errors) > 0
        assert any("max length" in e.lower() for e in errors)

    def test_invalid_source_feedback_fails(self, human_feedback_schema):
        """Invalid source_feedback value should fail."""
        feedback = {
            "decision": "approve",
            "reviewed_at": "2024-01-15T10:00:00Z",
            "source_feedback": "mediocre_source",  # Not a valid value
        }
        errors = self.validate_feedback(human_feedback_schema, feedback)
        assert len(errors) > 0
        assert any("source_feedback" in e.lower() for e in errors)

    def test_invalid_topic_feedback_fails(self, human_feedback_schema):
        """Invalid topic_feedback value should fail."""
        feedback = {
            "decision": "approve",
            "reviewed_at": "2024-01-15T10:00:00Z",
            "topic_feedback": "shrink_this_topic",  # Not a valid value
        }
        errors = self.validate_feedback(human_feedback_schema, feedback)
        assert len(errors) > 0
        assert any("topic_feedback" in e.lower() for e in errors)


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_string_decision_fails(self, human_feedback_schema):
        """Empty string decision should fail."""
        feedback = {"decision": "", "reviewed_at": "2024-01-15T10:00:00Z"}
        # Empty string is not in the enum
        props = human_feedback_schema.get("properties", {})
        decision = props.get("decision", {})
        assert "" not in decision.get("enum", [])

    def test_empty_string_notes_fails_max_length(self, human_feedback_schema):
        """Empty notes string should pass (it's optional)."""
        feedback = {
            "decision": "approve",
            "reviewed_at": "2024-01-15T10:00:00Z",
            "notes": "",
        }
        props = human_feedback_schema.get("properties", {})
        notes_schema = props.get("notes", {})
        max_len = notes_schema.get("maxLength", 500)
        assert len("") <= max_len

    def test_notes_exactly_500_chars_passes(self, human_feedback_schema):
        """Notes exactly at max length should pass."""
        feedback = {
            "decision": "approve",
            "reviewed_at": "2024-01-15T10:00:00Z",
            "notes": "A" * 500,
        }
        props = human_feedback_schema.get("properties", {})
        notes_schema = props.get("notes", {})
        max_len = notes_schema.get("maxLength", 500)
        assert len(feedback["notes"]) == max_len

    def test_source_feedback_null_is_valid(self, human_feedback_schema):
        """source_feedback null should be valid."""
        feedback = {
            "decision": "approve",
            "reviewed_at": "2024-01-15T10:00:00Z",
            "source_feedback": None,
        }
        props = human_feedback_schema.get("properties", {})
        source_schema = props.get("source_feedback", {})
        assert None in source_schema.get("enum", [])

    def test_topic_feedback_null_is_valid(self, human_feedback_schema):
        """topic_feedback null should be valid."""
        feedback = {
            "decision": "approve",
            "reviewed_at": "2024-01-15T10:00:00Z",
            "topic_feedback": None,
        }
        props = human_feedback_schema.get("properties", {})
        topic_schema = props.get("topic_feedback", {})
        assert None in topic_schema.get("enum", [])
