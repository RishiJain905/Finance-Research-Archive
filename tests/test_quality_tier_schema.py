"""
Tests for the quality_tier.json schema validation.
"""

import json
import unittest
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


class QualityTierSchemaTests(unittest.TestCase):
    """Test quality_tier.json schema structure and validation rules."""

    def setUp(self):
        schema_path = BASE_DIR / "schemas" / "quality_tier.json"
        with schema_path.open("r", encoding="utf-8") as f:
            self.schema = json.load(f)

    def test_schema_is_valid_json(self):
        """Schema should be parseable JSON."""
        self.assertIsInstance(self.schema, dict)

    def test_schema_has_required_fields(self):
        """Schema should have required fields: type, properties, required."""
        self.assertIn("type", self.schema)
        self.assertIn("properties", self.schema)
        self.assertIn("required", self.schema)

    def test_schema_type_is_object(self):
        """Schema type should be 'object'."""
        self.assertEqual(self.schema["type"], "object")

    def test_required_fields_include_tier_and_reasoning(self):
        """Required fields must include 'tier' and 'reasoning'."""
        required = self.schema.get("required", [])
        self.assertIn("tier", required)
        self.assertIn("reasoning", required)

    def test_tier_enum_only_accepts_tier_1_tier_2_tier_3(self):
        """Tier field enum must only accept tier_1, tier_2, tier_3."""
        tier_props = self.schema["properties"]["tier"]
        self.assertEqual(tier_props["type"], "string")
        self.assertIn("enum", tier_props)
        enum_values = tier_props["enum"]
        self.assertEqual(set(enum_values), {"tier_1", "tier_2", "tier_3"})

    def test_tier_enum_rejects_other_values(self):
        """Tier field should reject values not in enum."""
        tier_props = self.schema["properties"]["tier"]
        invalid_values = ["tier_0", "tier_4", "high", "medium", "low", ""]
        for val in invalid_values:
            self.assertNotIn(val, tier_props["enum"])

    def test_score_field_exists_and_has_range(self):
        """Score field should exist with minimum 0 and maximum 100."""
        self.assertIn("score", self.schema["properties"])
        score_props = self.schema["properties"]["score"]
        self.assertEqual(score_props["type"], "number")
        self.assertEqual(score_props["minimum"], 0)
        self.assertEqual(score_props["maximum"], 100)

    def test_score_is_optional(self):
        """Score field should not be in required list."""
        required = self.schema.get("required", [])
        self.assertNotIn("score", required)

    def test_reasoning_field_is_array_with_string_items(self):
        """Reasoning field should be array of strings with minItems 1."""
        reasoning_props = self.schema["properties"]["reasoning"]
        self.assertEqual(reasoning_props["type"], "array")
        self.assertEqual(reasoning_props["items"]["type"], "string")
        self.assertEqual(reasoning_props["minItems"], 1)

    def test_additional_properties_is_false(self):
        """additionalProperties must be false to prevent extra fields."""
        self.assertIn("additionalProperties", self.schema)
        self.assertFalse(self.schema["additionalProperties"])

    def test_additional_properties_false_rejects_extra_fields(self):
        """Verifying additionalProperties: false rejects unknown fields."""
        allowed_props = set(self.schema["properties"].keys())
        extra_props = {"tier", "score", "reasoning"}  # These are allowed
        self.assertEqual(allowed_props, extra_props)

    def test_schema_description_exists(self):
        """Schema should have a description."""
        self.assertIn("description", self.schema)

    def test_tier_description_exists(self):
        """Tier field should have a description."""
        self.assertIn("description", self.schema["properties"]["tier"])

    def test_reasoning_description_exists(self):
        """Reasoning field should have a description."""
        self.assertIn("description", self.schema["properties"]["reasoning"])


class QualityTierSchemaValidationTests(unittest.TestCase):
    """Test that instance data validates against the schema."""

    def test_valid_tier_1_block(self):
        """A valid tier_1 quality_tier block should pass schema."""
        schema_path = BASE_DIR / "schemas" / "quality_tier.json"
        with schema_path.open("r", encoding="utf-8") as f:
            schema = json.load(f)

        valid_block = {
            "tier": "tier_1",
            "score": 85.5,
            "reasoning": [
                "quality score: 85.5/100 (highly_reusable)",
                "high verification confidence",
            ],
        }
        self._validate_schema(schema, valid_block)

    def test_valid_tier_2_block(self):
        """A valid tier_2 quality_tier block should pass schema."""
        schema_path = BASE_DIR / "schemas" / "quality_tier.json"
        with schema_path.open("r", encoding="utf-8") as f:
            schema = json.load(f)

        valid_block = {
            "tier": "tier_2",
            "score": 65.0,
            "reasoning": [
                "quality score: 65.0/100 (useful_context)",
                "moderate verification confidence",
            ],
        }
        self._validate_schema(schema, valid_block)

    def test_valid_tier_3_block(self):
        """A valid tier_3 quality_tier block should pass schema."""
        schema_path = BASE_DIR / "schemas" / "quality_tier.json"
        with schema_path.open("r", encoding="utf-8") as f:
            schema = json.load(f)

        valid_block = {
            "tier": "tier_3",
            "score": 30.0,
            "reasoning": [
                "quality score: 30.0/100 (acceptable)",
                "low verification confidence",
            ],
        }
        self._validate_schema(schema, valid_block)

    def test_missing_tier_fails_validation(self):
        """Block missing 'tier' should fail validation."""
        schema_path = BASE_DIR / "schemas" / "quality_tier.json"
        with schema_path.open("r", encoding="utf-8") as f:
            schema = json.load(f)

        invalid_block = {"score": 85.0, "reasoning": ["some reasoning"]}
        self.assertFalse(self._validate_schema(schema, invalid_block))

    def test_missing_reasoning_fails_validation(self):
        """Block missing 'reasoning' should fail validation."""
        schema_path = BASE_DIR / "schemas" / "quality_tier.json"
        with schema_path.open("r", encoding="utf-8") as f:
            schema = json.load(f)

        invalid_block = {"tier": "tier_1", "score": 85.0}
        self.assertFalse(self._validate_schema(schema, invalid_block))

    def test_invalid_tier_value_fails_validation(self):
        """Block with invalid tier value should fail validation."""
        schema_path = BASE_DIR / "schemas" / "quality_tier.json"
        with schema_path.open("r", encoding="utf-8") as f:
            schema = json.load(f)

        invalid_block = {
            "tier": "tier_4",
            "score": 85.0,
            "reasoning": ["some reasoning"],
        }
        self.assertFalse(self._validate_schema(schema, invalid_block))

    def test_score_below_minimum_fails_validation(self):
        """Score below 0 should fail validation."""
        schema_path = BASE_DIR / "schemas" / "quality_tier.json"
        with schema_path.open("r", encoding="utf-8") as f:
            schema = json.load(f)

        invalid_block = {
            "tier": "tier_3",
            "score": -5.0,
            "reasoning": ["some reasoning"],
        }
        self.assertFalse(self._validate_schema(schema, invalid_block))

    def test_score_above_maximum_fails_validation(self):
        """Score above 100 should fail validation."""
        schema_path = BASE_DIR / "schemas" / "quality_tier.json"
        with schema_path.open("r", encoding="utf-8") as f:
            schema = json.load(f)

        invalid_block = {
            "tier": "tier_1",
            "score": 105.0,
            "reasoning": ["some reasoning"],
        }
        self.assertFalse(self._validate_schema(schema, invalid_block))

    def test_empty_reasoning_array_fails_validation(self):
        """Reasoning array with 0 items should fail minItems constraint."""
        schema_path = BASE_DIR / "schemas" / "quality_tier.json"
        with schema_path.open("r", encoding="utf-8") as f:
            schema = json.load(f)

        invalid_block = {"tier": "tier_2", "score": 60.0, "reasoning": []}
        self.assertFalse(self._validate_schema(schema, invalid_block))

    def test_extra_field_fails_validation(self):
        """Block with extra fields should fail due to additionalProperties: false."""
        schema_path = BASE_DIR / "schemas" / "quality_tier.json"
        with schema_path.open("r", encoding="utf-8") as f:
            schema = json.load(f)

        invalid_block = {
            "tier": "tier_1",
            "score": 85.0,
            "reasoning": ["some reasoning"],
            "extra_field": "not allowed",
        }
        self.assertFalse(self._validate_schema(schema, invalid_block))

    def test_reasoning_with_non_string_fails_validation(self):
        """Reasoning array with non-string items should fail."""
        schema_path = BASE_DIR / "schemas" / "quality_tier.json"
        with schema_path.open("r", encoding="utf-8") as f:
            schema = json.load(f)

        invalid_block = {
            "tier": "tier_1",
            "score": 85.0,
            "reasoning": ["valid string", 123, "another string"],
        }
        self.assertFalse(self._validate_schema(schema, invalid_block))

    def _validate_schema(self, schema, instance):
        """Simple JSON Schema validator for testing purposes."""
        try:
            return self._validate_recursive(schema, instance, path="")
        except Exception:
            return False

    def _validate_recursive(self, schema, instance, path):
        """Recursively validate instance against schema."""
        if schema.get("type") == "object":
            if not isinstance(instance, dict):
                raise ValueError(f"{path}: expected object, got {type(instance)}")

            # Check required fields
            for req in schema.get("required", []):
                if req not in instance:
                    raise ValueError(f"{path}: missing required field '{req}'")

            # Check properties
            for key, value in instance.items():
                if key not in schema.get("properties", {}):
                    if schema.get("additionalProperties") is False:
                        raise ValueError(f"{path}.{key}: unexpected field")
                else:
                    prop_schema = schema["properties"][key]
                    self._validate_recursive(prop_schema, value, f"{path}.{key}")

            # Check additionalProperties
            if schema.get("additionalProperties") is False:
                allowed = set(schema.get("properties", {}).keys())
                present = set(instance.keys())
                extra = present - allowed
                if extra:
                    raise ValueError(f"{path}: unexpected fields {extra}")

        elif schema.get("type") == "array":
            if not isinstance(instance, list):
                raise ValueError(f"{path}: expected array, got {type(instance)}")

            min_items = schema.get("minItems", 0)
            if len(instance) < min_items:
                raise ValueError(
                    f"{path}: array has {len(instance)} items, minimum is {min_items}"
                )

            items_schema = schema.get("items", {})
            for i, item in enumerate(instance):
                self._validate_recursive(items_schema, item, f"{path}[{i}]")

        elif schema.get("type") == "number":
            if not isinstance(instance, (int, float)):
                raise ValueError(f"{path}: expected number, got {type(instance)}")

            if "minimum" in schema and instance < schema["minimum"]:
                raise ValueError(
                    f"{path}: {instance} is less than minimum {schema['minimum']}"
                )

            if "maximum" in schema and instance > schema["maximum"]:
                raise ValueError(
                    f"{path}: {instance} is greater than maximum {schema['maximum']}"
                )

        elif schema.get("type") == "string":
            if not isinstance(instance, str):
                raise ValueError(f"{path}: expected string, got {type(instance)}")

            if "enum" in schema and instance not in schema["enum"]:
                raise ValueError(f"{path}: '{instance}' not in enum {schema['enum']}")

        return True


if __name__ == "__main__":
    unittest.main()
