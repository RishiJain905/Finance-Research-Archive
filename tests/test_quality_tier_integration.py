"""
Integration tests for quality tier assignment with other project components.
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
    load_config,
    load_domain_trust,
    process_record_file,
)


class RouteRecordContextTests(unittest.TestCase):
    """Test quality tier assignment in route_record context."""

    def test_assign_quality_tier_imported_from_route_record_context(self):
        """assign_quality_tier should be callable in route_record context."""
        # This tests that the module can be imported and used as it would
        # be in route_record.py
        try:
            from scripts import assign_quality_tier as aqt

            self.assertIsNotNone(aqt.assign_quality_tier)
            self.assertIsNotNone(aqt.load_config)
        except ImportError as e:
            self.fail(f"Failed to import assign_quality_tier: {e}")

    def test_assign_quality_tier_with_route_record_style_record(self):
        """Should work with records as they would come from route_record."""
        config = load_config()
        domain_trust = load_domain_trust()

        # Simulate a record that came through route_record.py
        record = {
            "id": "route_record_001",
            "status": "accepted",
            "topic": "monetary policy",
            "event_type": "fed_policy",
            "title": "Fed signals rate cuts",
            "summary": "Federal Reserve officials indicated rate reductions may be appropriate.",
            "why_it_matters": "Changes in Fed policy rates directly impact borrowing costs and market conditions.",
            "market_structure_context": "Rate expectations affect yield curve and risk appetite.",
            "macro_context": "Fed signaling shapes market expectations for policy path.",
            "tags": ["fed", "monetary policy", "interest rates"],
            "source": {
                "name": "Federal Reserve Press Releases",
                "url": "https://www.federalreserve.gov/newsevents/pressreleases/2026-test.htm",
                "published_at": "2026-03-15",
                "source_type": "press_release",
                "domain": "federalreserve.gov",
            },
            "llm_review": {
                "verification_confidence": 9,
                "verdict": "accept",
                "issues_found": [],
            },
            "human_review": {
                "required": False,
                "decision": "approved_by_human",
                "notes": "",
            },
            "important_numbers": [
                {"value": "5.25%", "description": "federal funds rate"},
                {"value": "2.5%", "description": "core PCE inflation"},
            ],
        }

        result = assign_quality_tier(record, config, domain_trust=domain_trust)
        quality_tier = result["quality_tier"]

        # Verify structure
        self.assertIn("tier", quality_tier)
        self.assertIn("score", quality_tier)
        self.assertIn("reasoning", quality_tier)

        # Should be high quality due to:
        # - High verification confidence (9)
        # - Human approved
        # - No issues
        # - Strong why_it_matters
        # - Has numbers
        # - High-trust source
        # - Core topic (monetary policy)
        # - High-significance event (fed_policy)
        self.assertEqual(quality_tier["tier"], "tier_1")
        self.assertGreaterEqual(quality_tier["score"], 80)


class FinalizeReviewContextTests(unittest.TestCase):
    """Test quality tier assignment in finalize_review context."""

    def test_assign_quality_tier_from_finalize_review_context(self):
        """assign_quality_tier should be callable from finalize_review context."""
        # This simulates how finalize_review.py might call assign_quality_tier
        config = load_config()
        domain_trust = load_domain_trust()

        # Record after finalization decision
        record = {
            "id": "finalized_record_001",
            "status": "accepted",
            "topic": "fiscal policy",
            "event_type": "fiscal_policy",
            "title": "Treasury announces new bond issuance",
            "summary": "Treasury plans to increase bond issuance to fund deficit.",
            "why_it_matters": "Changes in Treasury issuance affect yield curve dynamics and funding costs.",
            "llm_review": {
                "verification_confidence": 8,
                "verdict": "accept",
                "issues_found": ["Minor date discrepancy in paragraph 2"],
            },
            "human_review": {
                "required": True,
                "decision": "approved_by_human",
                "notes": "Verified content, minor formatting issue acceptable.",
            },
            "source": {
                "name": "U.S. Treasury",
                "url": "https://home.treasury.gov/news/press-releases/2026-test",
                "domain": "treasury.gov",
            },
            "important_numbers": [
                {"value": "$500B", "description": "quarterly issuance increase"}
            ],
        }

        result = assign_quality_tier(record, config, domain_trust=domain_trust)
        quality_tier = result["quality_tier"]

        self.assertIn("tier", quality_tier)
        self.assertIsInstance(quality_tier["score"], float)
        self.assertGreater(len(quality_tier["reasoning"]), 0)


class EndToEndTests(unittest.TestCase):
    """End-to-end integration tests."""

    def setUp(self):
        self.config = load_config()
        self.domain_trust = load_domain_trust()

    def test_end_to_end_create_temp_record_assign_tier(self):
        """Create a temp record, assign tier, verify tier block structure."""
        # Create a realistic record
        record = {
            "id": "e2e_test_record",
            "status": "accepted",
            "topic": "market structure",
            "event_type": "liquidity",
            "title": "Treasury market liquidity metrics",
            "summary": "Bid-ask spreads in Treasury markets have widened.",
            "why_it_matters": "Treasury market liquidity is critical for price discovery.",
            "llm_review": {
                "verification_confidence": 7,
                "verdict": "accept",
                "issues_found": [],
            },
            "human_review": {"required": False, "decision": "", "notes": ""},
            "source": {
                "name": "Federal Reserve Bank of New York",
                "url": "https://www.newyorkfed.org/markets/reference-rates",
                "domain": "newyorkfed.org",
            },
            "important_numbers": [
                {"value": "0.15%", "description": "bid-ask spread widening"}
            ],
        }

        # Assign tier
        result = assign_quality_tier(
            record, self.config, domain_trust=self.domain_trust
        )

        # Verify tier block structure
        quality_tier = result["quality_tier"]
        self.assertIn("tier", quality_tier)
        self.assertIn("score", quality_tier)
        self.assertIn("reasoning", quality_tier)

        # Verify tier is valid
        self.assertIn(quality_tier["tier"], ["tier_1", "tier_2", "tier_3"])

        # Verify score is numeric and in range
        self.assertIsInstance(quality_tier["score"], float)
        self.assertGreaterEqual(quality_tier["score"], 0)
        self.assertLessEqual(quality_tier["score"], 100)

        # Verify reasoning is a list with at least one item
        self.assertIsInstance(quality_tier["reasoning"], list)
        self.assertGreater(len(quality_tier["reasoning"]), 0)

        # Verify all reasoning items are strings
        for reason in quality_tier["reasoning"]:
            self.assertIsInstance(reason, str)

    def test_tier_assignment_does_not_modify_other_fields(self):
        """Assigning tier should not modify other record fields."""
        original_record = {
            "id": "unchanged_fields_test",
            "status": "accepted",
            "topic": "monetary policy",
            "event_type": "fed_policy",
            "title": "Fed maintains policy stance",
            "why_it_matters": "Fed policy affects all market segments.",
            "llm_review": {
                "verification_confidence": 8,
                "verdict": "accept",
                "issues_found": [],
            },
            "human_review": {"decision": "approved"},
            "source": {"domain": "federalreserve.gov"},
            "important_numbers": [{"value": "5.25%", "description": "rate"}],
            "custom_field": "this should not be modified",
        }

        # Make a deep copy for comparison
        record_copy = json.loads(json.dumps(original_record))

        # Assign tier
        result = assign_quality_tier(
            original_record, self.config, domain_trust=self.domain_trust
        )

        # Verify original fields are unchanged
        self.assertEqual(original_record["id"], record_copy["id"])
        self.assertEqual(original_record["status"], record_copy["status"])
        self.assertEqual(original_record["topic"], record_copy["topic"])
        self.assertEqual(original_record["event_type"], record_copy["event_type"])
        self.assertEqual(original_record["title"], record_copy["title"])
        self.assertEqual(
            original_record["why_it_matters"], record_copy["why_it_matters"]
        )
        self.assertEqual(original_record["custom_field"], record_copy["custom_field"])

        # Verify quality_tier was added (not modifying existing fields)
        self.assertIn("quality_tier", result)
        self.assertNotIn(
            "quality_tier", original_record
        )  # Original should be unchanged

    def test_full_pipeline_file_based(self):
        """Test full pipeline with file-based operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a record
            record = {
                "id": "pipeline_test",
                "status": "accepted",
                "topic": "monetary policy",
                "event_type": "fed_policy",
                "title": "FOMC statement release",
                "why_it_matters": "FOMC statements directly impact market expectations for policy.",
                "llm_review": {
                    "verification_confidence": 9,
                    "verdict": "accept",
                    "issues_found": [],
                },
                "human_review": {"decision": "approved_by_human"},
                "source": {
                    "name": "Federal Reserve",
                    "url": "https://www.federalreserve.gov/test",
                    "domain": "federalreserve.gov",
                },
                "important_numbers": [
                    {"value": "5.25%", "description": "fed funds rate"}
                ],
            }

            file_path = Path(tmpdir) / "pipeline_test.json"
            with file_path.open("w", encoding="utf-8") as f:
                json.dump(record, f)

            # Process the file (not dry run)
            result = process_record_file(file_path, self.config, dry_run=False)

            self.assertTrue(result["success"])
            self.assertFalse(result["skipped"])
            self.assertEqual(result["new_tier"], "tier_1")

            # Read back and verify
            with file_path.open("r", encoding="utf-8") as f:
                updated_record = json.load(f)

            self.assertIn("quality_tier", updated_record)
            qt = updated_record["quality_tier"]
            self.assertEqual(qt["tier"], "tier_1")
            self.assertGreaterEqual(qt["score"], 80)
            self.assertIsInstance(qt["reasoning"], list)
            self.assertGreater(len(qt["reasoning"]), 0)

    def test_batch_with_mixed_records(self):
        """Test batch processing with mixed quality records."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # High quality record
            high_quality = {
                "id": "high_quality",
                "llm_review": {"verification_confidence": 9, "issues_found": []},
                "human_review": {"decision": "approved_by_human"},
                "source": {"domain": "federalreserve.gov"},
                "why_it_matters": "Fed policy directly affects all borrowing costs and market conditions.",
                "important_numbers": [{"value": "5.25%", "description": "rate"}],
                "topic": "monetary policy",
                "event_type": "fed_policy",
            }

            # Medium quality record - designed to score in tier_2 range (55-79)
            # Higher verification, has numbers, medium trust source
            medium_quality = {
                "id": "medium_quality",
                "llm_review": {
                    "verification_confidence": 7,
                    "issues_found": ["minor issue"],
                },
                "human_review": {},
                "source": {"domain": "imf.org"},
                "why_it_matters": "This analysis provides useful economic context.",
                "important_numbers": [{"value": "2.5%", "description": "GDP growth"}],
                "topic": "market structure",
                "event_type": "liquidity",
            }

            # Low quality record
            low_quality = {
                "id": "low_quality",
                "llm_review": {
                    "verification_confidence": 2,
                    "issues_found": ["i1", "i2", "i3"],
                },
                "human_review": {},
                "source": {"domain": "randomblog.com"},
                "why_it_matters": "",
                "important_numbers": [],
                "topic": "other",
                "event_type": "other",
            }

            # Write files
            for record in [high_quality, medium_quality, low_quality]:
                file_path = Path(tmpdir) / f"{record['id']}.json"
                with file_path.open("w", encoding="utf-8") as f:
                    json.dump(record, f)

            # Process batch
            from scripts.assign_quality_tier import process_batch

            results = process_batch(Path(tmpdir), self.config, dry_run=True)

            self.assertEqual(len(results), 3)

            # Find each result by file path
            high_result = next(r for r in results if "high_quality" in r["file"])
            medium_result = next(r for r in results if "medium_quality" in r["file"])
            low_result = next(r for r in results if "low_quality" in r["file"])

            # Verify tiers
            self.assertEqual(high_result["new_tier"], "tier_1")
            self.assertEqual(medium_result["new_tier"], "tier_2")
            self.assertEqual(low_result["new_tier"], "tier_3")

            # Verify scores are in correct ranges
            self.assertGreaterEqual(high_result["score"], 80)
            self.assertGreaterEqual(medium_result["score"], 55)
            self.assertLess(medium_result["score"], 80)
            self.assertLess(low_result["score"], 55)


class SchemaComplianceTests(unittest.TestCase):
    """Test that output complies with quality_tier schema."""

    def test_output_complies_with_schema(self):
        """Generated quality_tier should comply with quality_tier.json schema."""
        schema_path = BASE_DIR / "schemas" / "quality_tier.json"
        with schema_path.open("r", encoding="utf-8") as f:
            schema = json.load(f)

        config = load_config()
        domain_trust = load_domain_trust()

        # Test various record types
        test_records = [
            {
                "id": "schema_test_1",
                "llm_review": {"verification_confidence": 9, "issues_found": []},
                "human_review": {"decision": "approved_by_human"},
                "source": {"domain": "federalreserve.gov"},
                "why_it_matters": "Strong explanation that exceeds thirty characters easily.",
                "important_numbers": [{"value": "5.25%", "description": "rate"}],
                "topic": "monetary policy",
                "event_type": "fed_policy",
            },
            {
                "id": "schema_test_2",
                "llm_review": {
                    "verification_confidence": 3,
                    "issues_found": ["i1", "i2"],
                },
                "human_review": {},
                "source": {},
                "why_it_matters": "",
                "important_numbers": [],
                "topic": "",
                "event_type": "",
            },
            {
                "id": "schema_test_3",
                "llm_review": {"verification_confidence": 5, "issues_found": []},
                "human_review": {},
                "source": {"domain": "imf.org"},
                "why_it_matters": "Medium length explanation here.",
                "important_numbers": [],
                "topic": "macro data",
                "event_type": "data_release",
            },
        ]

        for record in test_records:
            result = assign_quality_tier(record, config, domain_trust=domain_trust)
            qt = result["quality_tier"]

            # Verify tier enum
            self.assertIn(qt["tier"], schema["properties"]["tier"]["enum"])

            # Verify score range
            self.assertGreaterEqual(
                qt["score"], schema["properties"]["score"]["minimum"]
            )
            self.assertLessEqual(qt["score"], schema["properties"]["score"]["maximum"])

            # Verify reasoning
            self.assertIsInstance(qt["reasoning"], list)
            self.assertGreaterEqual(
                len(qt["reasoning"]), schema["properties"]["reasoning"]["minItems"]
            )
            for reason in qt["reasoning"]:
                self.assertIsInstance(reason, str)


if __name__ == "__main__":
    unittest.main()
