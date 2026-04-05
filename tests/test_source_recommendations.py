"""
Tests for V2.7 Part 5: Source Recommendations Engine (source_recommendations.py)

Comprehensive test suite covering:
- Rule loading and defaults
- Rule evaluation functions
- Reason generation
- Recommendation generation
- Batch recommendation generation
- Persistence and loading
- End-to-end pipeline
"""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from scripts import source_recommendations


# =============================================================================
# Test Fixtures / Helper Data
# =============================================================================


def make_stats(
    source_name="Test Source",
    source_domain="test.com",
    records_seen=10,
    accepted_count=5,
    review_count=2,
    rejected_count=1,
    filtered_out_count=2,
    accepted_ratio=0.5,
    review_ratio=0.2,
    rejected_ratio=0.1,
    filtered_ratio=0.2,
    avg_priority_score=75.0,
    avg_verification_confidence=7.5,
    last_seen_at="2026-04-01T00:00:00+00:00",
):
    """Factory to create mock source stats."""
    return {
        "source_name": source_name,
        "source_domain": source_domain,
        "records_seen": records_seen,
        "accepted_count": accepted_count,
        "review_count": review_count,
        "rejected_count": rejected_count,
        "filtered_out_count": filtered_out_count,
        "accepted_ratio": accepted_ratio,
        "review_ratio": review_ratio,
        "rejected_ratio": rejected_ratio,
        "filtered_ratio": filtered_ratio,
        "avg_priority_score": avg_priority_score,
        "avg_verification_confidence": avg_verification_confidence,
        "last_seen_at": last_seen_at,
    }


def make_rules(
    disable=None,
    tighten=None,
    lower_max_links=None,
    investigate=None,
):
    """Factory to create mock rules with optional overrides."""
    rules = {
        "disable": {
            "min_records_seen": 20,
            "max_accepted_ratio": 0.05,
            "min_filtered_ratio": 0.70,
        },
        "lower_max_links": {
            "min_accepted_ratio": 0.10,
            "min_filtered_ratio": 0.50,
            "min_records_seen": 10,
        },
        "tighten": {
            "min_accepted_ratio": 0.05,
            "min_review_ratio": 0.30,
            "min_records_seen": 10,
        },
        "investigate": {
            "min_records_seen": 5,
            "max_accepted_ratio": 0.50,
            "min_review_ratio": 0.20,
        },
    }

    if disable is not None:
        rules["disable"].update(disable)
    if tighten is not None:
        rules["tighten"].update(tighten)
    if lower_max_links is not None:
        rules["lower_max_links"].update(lower_max_links)
    if investigate is not None:
        rules["investigate"].update(investigate)

    return rules


# =============================================================================
# TestLoadRecommendationRules
# =============================================================================


class TestLoadRecommendationRules(unittest.TestCase):
    """Test rule loading and defaults."""

    def test_returns_default_rules(self):
        """Should return default rules when no custom file provided."""
        rules = source_recommendations.load_recommendation_rules()

        self.assertIn("disable", rules)
        self.assertIn("tighten", rules)
        self.assertIn("lower_max_links", rules)
        self.assertIn("investigate", rules)

        # Check disable rule structure
        self.assertIn("min_records_seen", rules["disable"])
        self.assertIn("max_accepted_ratio", rules["disable"])
        self.assertIn("min_filtered_ratio", rules["disable"])

    def test_loads_custom_rules_file(self):
        """Should load rules from custom JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_rules = {
                "disable": {
                    "min_records_seen": 100,
                    "max_accepted_ratio": 0.01,
                    "min_filtered_ratio": 0.90,
                }
            }
            rules_path = Path(tmpdir) / "custom_rules.json"
            rules_path.write_text(json.dumps(custom_rules), encoding="utf-8")

            rules = source_recommendations.load_recommendation_rules(rules_path)

            # Custom values should be loaded
            self.assertEqual(rules["disable"]["min_records_seen"], 100)
            self.assertEqual(rules["disable"]["max_accepted_ratio"], 0.01)

    def test_merges_with_defaults(self):
        """Should merge custom rules with defaults for missing keys.

        Note: The implementation replaces top-level rule keys entirely,
        so custom rules should provide complete rule definitions.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_rules = {
                "disable": {
                    "min_records_seen": 50,
                    "max_accepted_ratio": 0.03,  # Override this
                    "min_filtered_ratio": 0.80,  # Override this too
                }
            }
            rules_path = Path(tmpdir) / "custom_rules.json"
            rules_path.write_text(json.dumps(custom_rules), encoding="utf-8")

            rules = source_recommendations.load_recommendation_rules(rules_path)

            # Custom values - all three should be present since custom_rules is complete
            self.assertEqual(rules["disable"]["min_records_seen"], 50)
            self.assertEqual(rules["disable"]["max_accepted_ratio"], 0.03)
            self.assertEqual(rules["disable"]["min_filtered_ratio"], 0.80)
            # Other rules should use defaults
            self.assertEqual(rules["tighten"]["min_records_seen"], 10)


# =============================================================================
# TestDisableRule
# =============================================================================


class TestDisableRule(unittest.TestCase):
    """Test disable threshold evaluation."""

    def test_triggers_on_low_accepted_high_filtered(self):
        """Should trigger disable when conditions met: 20+ records, <5% accepted, >70% filtered."""
        stats = make_stats(
            records_seen=20,
            accepted_ratio=0.04,  # Below 0.05
            filtered_ratio=0.75,  # Above 0.70
        )
        rules = make_rules()

        result = source_recommendations.evaluate_disable_rule(stats, rules)

        self.assertTrue(result)

    def test_does_not_trigger_with_high_accepted(self):
        """Should not trigger disable when accepted ratio is high."""
        stats = make_stats(
            records_seen=20,
            accepted_ratio=0.10,  # Above 0.05
            filtered_ratio=0.75,
        )
        rules = make_rules()

        result = source_recommendations.evaluate_disable_rule(stats, rules)

        self.assertFalse(result)

    def test_does_not_trigger_below_min_records(self):
        """Should not trigger disable below minimum records threshold."""
        stats = make_stats(
            records_seen=19,  # One below 20
            accepted_ratio=0.04,
            filtered_ratio=0.75,
        )
        rules = make_rules()

        result = source_recommendations.evaluate_disable_rule(stats, rules)

        self.assertFalse(result)


# =============================================================================
# TestTightenRule
# =============================================================================


class TestTightenRule(unittest.TestCase):
    """Test tighten threshold evaluation."""

    def test_triggers_on_high_review_ratio(self):
        """Should trigger tighten when: >=5% accepted, >=30% review, 10+ records."""
        stats = make_stats(
            records_seen=10,
            accepted_ratio=0.10,  # Above 0.05
            review_ratio=0.40,  # Above 0.30
            filtered_ratio=0.3,
        )
        rules = make_rules()

        result = source_recommendations.evaluate_tighten_rule(stats, rules)

        self.assertTrue(result)

    def test_does_not_trigger_with_low_review_ratio(self):
        """Should not trigger when review ratio is below threshold."""
        stats = make_stats(
            records_seen=10,
            accepted_ratio=0.10,
            review_ratio=0.20,  # Below 0.30
            filtered_ratio=0.3,
        )
        rules = make_rules()

        result = source_recommendations.evaluate_tighten_rule(stats, rules)

        self.assertFalse(result)

    def test_does_not_trigger_below_min_records(self):
        """Should not trigger below minimum records threshold."""
        stats = make_stats(
            records_seen=9,  # Below 10
            accepted_ratio=0.10,
            review_ratio=0.40,
            filtered_ratio=0.3,
        )
        rules = make_rules()

        result = source_recommendations.evaluate_tighten_rule(stats, rules)

        self.assertFalse(result)


# =============================================================================
# TestLowerMaxLinksRule
# =============================================================================


class TestLowerMaxLinksRule(unittest.TestCase):
    """Test lower max links threshold evaluation."""

    def test_triggers_on_moderate_accepted_high_filtered(self):
        """Should trigger when: >=10% accepted, >=50% filtered, 10+ records."""
        stats = make_stats(
            records_seen=10,
            accepted_ratio=0.15,  # Above 0.10
            filtered_ratio=0.60,  # Above 0.50
        )
        rules = make_rules()

        result = source_recommendations.evaluate_lower_max_links_rule(stats, rules)

        self.assertTrue(result)

    def test_does_not_trigger_with_low_filtered(self):
        """Should not trigger when filtered ratio is below threshold."""
        stats = make_stats(
            records_seen=10,
            accepted_ratio=0.15,
            filtered_ratio=0.40,  # Below 0.50
        )
        rules = make_rules()

        result = source_recommendations.evaluate_lower_max_links_rule(stats, rules)

        self.assertFalse(result)


# =============================================================================
# TestInvestigateRule
# =============================================================================


class TestInvestigateRule(unittest.TestCase):
    """Test investigate threshold evaluation."""

    def test_triggers_on_mixed_results(self):
        """Should trigger when: 5+ records, <50% accepted, >=20% review."""
        stats = make_stats(
            records_seen=5,
            accepted_ratio=0.30,  # Below 0.50
            review_ratio=0.40,  # Above 0.20
            filtered_ratio=0.3,
        )
        rules = make_rules()

        result = source_recommendations.evaluate_investigate_rule(stats, rules)

        self.assertTrue(result)

    def test_does_not_trigger_with_clear_results(self):
        """Should not trigger when results are clear (high accepted ratio)."""
        stats = make_stats(
            records_seen=5,
            accepted_ratio=0.60,  # Above 0.50
            review_ratio=0.20,
            filtered_ratio=0.2,
        )
        rules = make_rules()

        result = source_recommendations.evaluate_investigate_rule(stats, rules)

        self.assertFalse(result)


# =============================================================================
# TestBuildReasons
# =============================================================================


class TestBuildReasons(unittest.TestCase):
    """Test human-readable reason generation."""

    def test_disable_reasons_are_descriptive(self):
        """Disable reasons should mention low accepted and high filtered ratios."""
        stats = make_stats(
            records_seen=25,
            accepted_count=1,
            accepted_ratio=0.04,
            filtered_ratio=0.80,
        )
        rules = make_rules()

        reasons = source_recommendations.build_reasons("disable", stats, rules)

        self.assertIsInstance(reasons, list)
        self.assertGreater(len(reasons), 0)
        # Should mention the low accepted ratio
        self.assertTrue(any("4.0%" in r or "accepted" in r.lower() for r in reasons))

    def test_tighten_reasons_mention_review_ratio(self):
        """Tighten reasons should mention review ratio."""
        stats = make_stats(
            records_seen=15,
            accepted_count=2,
            accepted_ratio=0.10,
            review_ratio=0.40,
            filtered_ratio=0.5,
        )
        rules = make_rules()

        reasons = source_recommendations.build_reasons("tighten", stats, rules)

        self.assertTrue(any("review" in r.lower() for r in reasons))

    def test_keep_reasons_are_positive(self):
        """Keep reasons should be positive/neutral."""
        stats = make_stats(
            records_seen=10,
            accepted_ratio=0.5,
            filtered_ratio=0.2,
            review_ratio=0.2,
        )
        rules = make_rules()

        reasons = source_recommendations.build_reasons("keep", stats, rules)

        self.assertIsInstance(reasons, list)
        self.assertGreater(len(reasons), 0)

    def test_investigate_reasons_mention_zero_accepted(self):
        """Investigate reasons should mention zero accepted records."""
        stats = make_stats(
            records_seen=10,
            accepted_count=0,
            accepted_ratio=0.0,
            filtered_ratio=0.6,
            review_ratio=0.4,
        )
        rules = make_rules()

        reasons = source_recommendations.build_reasons("investigate", stats, rules)

        self.assertTrue(
            any("zero" in r.lower() or "accepted" in r.lower() for r in reasons)
        )


# =============================================================================
# TestGenerateRecommendation
# =============================================================================


class TestGenerateRecommendation(unittest.TestCase):
    """Test single recommendation generation."""

    def test_generates_disable_recommendation(self):
        """Should generate 'disable' recommendation when conditions met."""
        stats = make_stats(
            records_seen=20,
            accepted_ratio=0.04,
            filtered_ratio=0.75,
        )
        rules = make_rules()

        rec = source_recommendations.generate_recommendation(stats, rules)

        self.assertEqual(rec["recommended_action"], "disable")

    def test_generates_keep_recommendation(self):
        """Should generate 'keep' recommendation for good sources."""
        stats = make_stats(
            records_seen=10,
            accepted_ratio=0.6,
            filtered_ratio=0.1,
            review_ratio=0.2,
        )
        rules = make_rules()

        rec = source_recommendations.generate_recommendation(stats, rules)

        self.assertEqual(rec["recommended_action"], "keep")

    def test_generates_tighten_recommendation(self):
        """Should generate 'tighten' recommendation when high review ratio.

        Must NOT meet lower_max_links threshold (filtered_ratio < 0.50).
        """
        stats = make_stats(
            records_seen=10,
            accepted_ratio=0.10,
            review_ratio=0.40,
            filtered_ratio=0.30,  # Below 0.50 to avoid lower_max_links
        )
        rules = make_rules()

        rec = source_recommendations.generate_recommendation(stats, rules)

        self.assertEqual(rec["recommended_action"], "tighten")

    def test_generates_investigate_for_zero_accepted(self):
        """Should generate 'investigate' for sources with zero accepted records."""
        stats = make_stats(
            records_seen=8,
            accepted_count=0,
            accepted_ratio=0.0,
            filtered_ratio=0.6,
            review_ratio=0.4,
        )
        rules = make_rules()

        rec = source_recommendations.generate_recommendation(stats, rules)

        self.assertEqual(rec["recommended_action"], "investigate")

    def test_recommendation_has_required_fields(self):
        """Recommendation should have all required fields."""
        stats = make_stats()
        rules = make_rules()

        rec = source_recommendations.generate_recommendation(stats, rules)

        required_fields = {
            "source_name",
            "recommended_action",
            "reasons",
            "metrics_snapshot",
            "created_at",
        }
        self.assertEqual(required_fields, set(rec.keys()))

    def test_recommendation_created_at_is_iso8601(self):
        """created_at should be valid ISO-8601 format."""
        stats = make_stats()
        rules = make_rules()

        rec = source_recommendations.generate_recommendation(stats, rules)

        # Should be parseable as ISO datetime
        self.assertIsNotNone(rec["created_at"])
        # Should contain T for ISO format
        self.assertIn("T", rec["created_at"])


# =============================================================================
# TestGenerateAllRecommendations
# =============================================================================


class TestGenerateAllRecommendations(unittest.TestCase):
    """Test batch recommendation generation."""

    def setUp(self):
        """Create temp directory with mock stats files."""
        self.temp_dir = tempfile.mkdtemp()
        self.stats_dir = Path(self.temp_dir)

    def tearDown(self):
        """Clean up temp directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_generates_for_all_sources(self):
        """Should generate recommendation for each source stats file."""
        # Create stats files
        stats1 = make_stats(source_name="Source A", source_domain="source-a.com")
        stats2 = make_stats(source_name="Source B", source_domain="source-b.com")

        (self.stats_dir / "source-a.com.json").write_text(
            json.dumps(stats1), encoding="utf-8"
        )
        (self.stats_dir / "source-b.com.json").write_text(
            json.dumps(stats2), encoding="utf-8"
        )

        rules = make_rules()
        recommendations = source_recommendations.generate_all_recommendations(
            self.stats_dir, rules
        )

        self.assertEqual(len(recommendations), 2)

    def test_returns_empty_for_missing_dir(self):
        """Should return empty list for non-existent directory."""
        recommendations = source_recommendations.generate_all_recommendations(
            Path("/nonexistent/path")
        )

        self.assertEqual(recommendations, [])


# =============================================================================
# TestSaveRecommendations
# =============================================================================


class TestSaveRecommendations(unittest.TestCase):
    """Test recommendation file persistence."""

    def setUp(self):
        """Create temp directory for tests."""
        self.temp_dir = tempfile.mkdtemp()
        self.output_dir = Path(self.temp_dir) / "rec_output"

    def tearDown(self):
        """Clean up temp directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_saves_one_file_per_recommendation(self):
        """Should save one JSON file per recommendation."""
        recommendations = [
            {
                "source_name": "Source A",
                "recommended_action": "keep",
                "reasons": ["Good source"],
                "metrics_snapshot": {},
                "created_at": "2026-04-01T00:00:00+00:00",
            },
            {
                "source_name": "Source B",
                "recommended_action": "disable",
                "reasons": ["Bad source"],
                "metrics_snapshot": {},
                "created_at": "2026-04-01T00:00:00+00:00",
            },
        ]

        source_recommendations.save_recommendations(recommendations, self.output_dir)

        json_files = list(self.output_dir.glob("*.json"))
        self.assertEqual(len(json_files), 2)

    def test_creates_output_directory(self):
        """Should create output directory if it doesn't exist."""
        recommendations = [
            {
                "source_name": "Test",
                "recommended_action": "keep",
                "reasons": [],
                "metrics_snapshot": {},
                "created_at": "2026-04-01T00:00:00+00:00",
            }
        ]

        source_recommendations.save_recommendations(recommendations, self.output_dir)

        self.assertTrue(self.output_dir.exists())
        self.assertTrue(self.output_dir.is_dir())


# =============================================================================
# TestLoadRecommendations
# =============================================================================


class TestLoadRecommendations(unittest.TestCase):
    """Test recommendation file loading."""

    def setUp(self):
        """Create temp directory for tests."""
        self.temp_dir = tempfile.mkdtemp()
        self.rec_dir = Path(self.temp_dir)

    def tearDown(self):
        """Clean up temp directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_loads_saved_recommendations(self):
        """Should load previously saved recommendations."""
        recommendations = [
            {
                "source_name": "Source A",
                "recommended_action": "keep",
                "reasons": ["Good source"],
                "metrics_snapshot": {},
                "created_at": "2026-04-01T00:00:00+00:00",
            },
        ]

        source_recommendations.save_recommendations(recommendations, self.rec_dir)
        loaded = source_recommendations.load_recommendations(self.rec_dir)

        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0]["source_name"], "Source A")

    def test_returns_empty_for_missing_dir(self):
        """Should return empty list for non-existent directory."""
        result = source_recommendations.load_recommendations(Path("/nonexistent/path"))
        self.assertEqual(result, [])


# =============================================================================
# TestRecommendationFilenameGeneration
# =============================================================================


class TestRecommendationFilenameGeneration(unittest.TestCase):
    """Test deterministic filename generation."""

    def test_generates_valid_filename(self):
        """Should generate valid recommendation filename."""
        filename = source_recommendations.generate_recommendation_filename(
            "Test Source"
        )

        self.assertTrue(filename.endswith("_recommendation.json"))
        # Spaces are preserved, but the pattern is "name_recommendation.json"
        self.assertIn("Test Source_recommendation.json", filename)

    def test_is_deterministic(self):
        """Same input should always produce same output."""
        filename1 = source_recommendations.generate_recommendation_filename("Test")
        filename2 = source_recommendations.generate_recommendation_filename("Test")

        self.assertEqual(filename1, filename2)


# =============================================================================
# TestEndToEndPipeline
# =============================================================================


class TestEndToEndPipeline(unittest.TestCase):
    """Integration test: full pipeline from records to recommendations."""

    def test_full_pipeline_with_mock_data(self):
        """Test full pipeline: stats -> recommendations with mock data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            stats_dir = base_dir / "source_stats"
            output_dir = base_dir / "recommendations"
            stats_dir.mkdir(parents=True)
            output_dir.mkdir(parents=True)

            # Create stats files
            good_source = make_stats(
                source_name="Good Source",
                source_domain="good.com",
                records_seen=15,
                accepted_ratio=0.6,
                filtered_ratio=0.1,
                review_ratio=0.2,
            )
            bad_source = make_stats(
                source_name="Bad Source",
                source_domain="bad.com",
                records_seen=25,
                accepted_ratio=0.02,
                filtered_ratio=0.90,
            )

            (stats_dir / "good.com.json").write_text(
                json.dumps(good_source), encoding="utf-8"
            )
            (stats_dir / "bad.com.json").write_text(
                json.dumps(bad_source), encoding="utf-8"
            )

            # Generate recommendations
            recommendations = source_recommendations.generate_all_recommendations(
                stats_dir
            )

            # Save and reload
            source_recommendations.save_recommendations(recommendations, output_dir)
            loaded = source_recommendations.load_recommendations(output_dir)

            # Verify
            self.assertEqual(len(loaded), 2)

            # Find good and bad source recommendations
            good_rec = next(
                (r for r in loaded if r["source_name"] == "Good Source"), None
            )
            bad_rec = next(
                (r for r in loaded if r["source_name"] == "Bad Source"), None
            )

            self.assertIsNotNone(good_rec)
            self.assertIsNotNone(bad_rec)
            self.assertEqual(good_rec["recommended_action"], "keep")
            self.assertEqual(bad_rec["recommended_action"], "disable")

    def test_full_pipeline_with_real_repo_data(self):
        """Test full pipeline with real repository data."""
        base_dir = Path(__file__).resolve().parent.parent
        stats_dir = base_dir / "data" / "source_analytics"

        if not stats_dir.exists():
            self.skipTest("No source_analytics directory found")

        # Load existing stats
        recommendations = source_recommendations.generate_all_recommendations(stats_dir)

        self.assertIsInstance(recommendations, list)
        self.assertGreater(
            len(recommendations), 0, "Expected at least one recommendation"
        )

        # Each recommendation should have required fields
        for rec in recommendations:
            self.assertIn("source_name", rec)
            self.assertIn("recommended_action", rec)
            self.assertIn("reasons", rec)
            self.assertIn("metrics_snapshot", rec)
            self.assertIn("created_at", rec)


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    unittest.main()
