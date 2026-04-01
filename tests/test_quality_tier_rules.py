"""
Tests for the quality_tier_rules.json configuration.
"""

import json
import unittest
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


class QualityTierRulesConfigTests(unittest.TestCase):
    """Test quality_tier_rules.json config loading and structure."""

    def setUp(self):
        config_path = BASE_DIR / "config" / "quality_tier_rules.json"
        with config_path.open("r", encoding="utf-8") as f:
            self.config = json.load(f)

    def test_config_is_valid_json(self):
        """Config should be parseable JSON."""
        self.assertIsInstance(self.config, dict)

    def test_config_has_tiers_section(self):
        """Config must have 'tiers' section."""
        self.assertIn("tiers", self.config)

    def test_tier_1_threshold_is_80(self):
        """Tier 1 minimum score must be 80."""
        self.assertEqual(self.config["tiers"]["tier_1"]["min_score"], 80)

    def test_tier_2_threshold_is_55(self):
        """Tier 2 minimum score must be 55."""
        self.assertEqual(self.config["tiers"]["tier_2"]["min_score"], 55)

    def test_tier_3_threshold_is_0(self):
        """Tier 3 minimum score must be 0."""
        self.assertEqual(self.config["tiers"]["tier_3"]["min_score"], 0)

    def test_tier_1_has_label(self):
        """Tier 1 should have a label."""
        self.assertIn("label", self.config["tiers"]["tier_1"])
        self.assertEqual(self.config["tiers"]["tier_1"]["label"], "highly_reusable")

    def test_tier_2_has_label(self):
        """Tier 2 should have a label."""
        self.assertIn("label", self.config["tiers"]["tier_2"])
        self.assertEqual(self.config["tiers"]["tier_2"]["label"], "useful_context")

    def test_tier_3_has_label(self):
        """Tier 3 should have a label."""
        self.assertIn("label", self.config["tiers"]["tier_3"])
        self.assertEqual(self.config["tiers"]["tier_3"]["label"], "acceptable")

    def test_config_has_features_section(self):
        """Config must have 'features' section."""
        self.assertIn("features", self.config)

    def test_feature_weights_sum_to_one(self):
        """All feature weights should sum to 1.0."""
        features = self.config.get("features", {})
        weights = []
        for feature_name, feature_config in features.items():
            if "weight" in feature_config:
                weights.append(feature_config["weight"])
        total_weight = sum(weights)
        self.assertAlmostEqual(total_weight, 1.0, places=2)

    def test_verification_confidence_weight(self):
        """Verification confidence weight should be 0.25."""
        vc = self.config["features"]["verification_confidence"]
        self.assertEqual(vc["weight"], 0.25)

    def test_verification_confidence_scale(self):
        """Verification confidence scale should be [0, 10]."""
        vc = self.config["features"]["verification_confidence"]
        self.assertEqual(vc["scale"], [0, 10])

    def test_human_approved_weight(self):
        """Human approved weight should be 0.15."""
        ha = self.config["features"]["human_approved"]
        self.assertEqual(ha["weight"], 0.15)

    def test_human_approved_boost(self):
        """Human approved boost should be 10."""
        ha = self.config["features"]["human_approved"]
        self.assertEqual(ha["boost"], 10)

    def test_issues_found_count_weight(self):
        """Issues found count weight should be 0.10."""
        ifc = self.config["features"]["issues_found_count"]
        self.assertEqual(ifc["weight"], 0.10)

    def test_issues_found_count_scale(self):
        """Issues found count scale should be [0, 10]."""
        ifc = self.config["features"]["issues_found_count"]
        self.assertEqual(ifc["scale"], [0, 10])

    def test_why_it_matters_quality_weight(self):
        """Why it matters quality weight should be 0.15."""
        wq = self.config["features"]["why_it_matters_quality"]
        self.assertEqual(wq["weight"], 0.15)

    def test_why_it_matters_quality_scale(self):
        """Why it matters quality scale should be [0, 1]."""
        wq = self.config["features"]["why_it_matters_quality"]
        self.assertEqual(wq["scale"], [0, 1])

    def test_has_structured_numbers_weight(self):
        """Has structured numbers weight should be 0.10."""
        sn = self.config["features"]["has_structured_numbers"]
        self.assertEqual(sn["weight"], 0.10)

    def test_source_trust_score_weight(self):
        """Source trust score weight should be 0.10."""
        sts = self.config["features"]["source_trust_score"]
        self.assertEqual(sts["weight"], 0.10)

    def test_source_trust_score_scale(self):
        """Source trust score scale should be [0, 100]."""
        sts = self.config["features"]["source_trust_score"]
        self.assertEqual(sts["scale"], [0, 100])

    def test_topic_centrality_weight(self):
        """Topic centrality weight should be 0.10."""
        tc = self.config["features"]["topic_centrality"]
        self.assertEqual(tc["weight"], 0.10)

    def test_topic_centrality_scale(self):
        """Topic centrality scale should be [0, 1]."""
        tc = self.config["features"]["topic_centrality"]
        self.assertEqual(tc["scale"], [0, 1])

    def test_event_significance_weight(self):
        """Event significance weight should be 0.05."""
        es = self.config["features"]["event_significance"]
        self.assertEqual(es["weight"], 0.05)

    def test_event_significance_scale(self):
        """Event significance scale should be [0, 1]."""
        es = self.config["features"]["event_significance"]
        self.assertEqual(es["scale"], [0, 1])

    def test_topic_centrality_map_exists(self):
        """Topic centrality map should exist."""
        self.assertIn("topic_centrality_map", self.config)

    def test_topic_centrality_map_monetary_policy(self):
        """Topic centrality map should have 'monetary policy' at 1.0."""
        tcm = self.config["topic_centrality_map"]
        self.assertEqual(tcm["monetary policy"], 1.0)

    def test_topic_centrality_map_fiscal_policy(self):
        """Topic centrality map should have 'fiscal policy' at 1.0."""
        tcm = self.config["topic_centrality_map"]
        self.assertEqual(tcm["fiscal policy"], 1.0)

    def test_topic_centrality_map_market_structure(self):
        """Topic centrality map should have 'market structure' at 0.7."""
        tcm = self.config["topic_centrality_map"]
        self.assertEqual(tcm["market structure"], 0.7)

    def test_topic_centrality_map_has_default(self):
        """Topic centrality map should have a 'default' value."""
        tcm = self.config["topic_centrality_map"]
        self.assertIn("default", tcm)

    def test_event_significance_map_exists(self):
        """Event significance map should exist."""
        self.assertIn("event_significance_map", self.config)

    def test_event_significance_map_fed_policy(self):
        """Event significance map should have 'fed_policy' at 1.0."""
        esm = self.config["event_significance_map"]
        self.assertEqual(esm["fed_policy"], 1.0)

    def test_event_significance_map_central_bank_action(self):
        """Event significance map should have 'central_bank_action' at 1.0."""
        esm = self.config["event_significance_map"]
        self.assertEqual(esm["central_bank_action"], 1.0)

    def test_event_significance_map_fiscal_policy(self):
        """Event significance map should have 'fiscal_policy' at 0.8."""
        esm = self.config["event_significance_map"]
        self.assertEqual(esm["fiscal_policy"], 0.8)

    def test_event_significance_map_data_release(self):
        """Event significance map should have 'data_release' at 0.6."""
        esm = self.config["event_significance_map"]
        self.assertEqual(esm["data_release"], 0.6)

    def test_event_significance_map_liquidity(self):
        """Event significance map should have 'liquidity' at 0.7."""
        esm = self.config["event_significance_map"]
        self.assertEqual(esm["liquidity"], 0.7)

    def test_event_significance_map_yield_curve(self):
        """Event significance map should have 'yield_curve' at 0.6."""
        esm = self.config["event_significance_map"]
        self.assertEqual(esm["yield_curve"], 0.6)

    def test_event_significance_map_has_default(self):
        """Event significance map should have a 'default' value."""
        esm = self.config["event_significance_map"]
        self.assertIn("default", esm)


class LoadConfigTests(unittest.TestCase):
    """Test load_config function with defaults fallback."""

    def test_load_config_returns_dict(self):
        """load_config should return a dictionary."""
        import sys

        sys.path.insert(0, str(BASE_DIR))
        from scripts.assign_quality_tier import load_config

        config = load_config()
        self.assertIsInstance(config, dict)

    def test_load_config_has_tiers(self):
        """load_config should have tiers section."""
        import sys

        sys.path.insert(0, str(BASE_DIR))
        from scripts.assign_quality_tier import load_config

        config = load_config()
        self.assertIn("tiers", config)

    def test_load_config_has_features(self):
        """load_config should have features section."""
        import sys

        sys.path.insert(0, str(BASE_DIR))
        from scripts.assign_quality_tier import load_config

        config = load_config()
        self.assertIn("features", config)

    def test_load_config_with_nonexistent_path_uses_defaults(self):
        """load_config with nonexistent path should return defaults."""
        import sys

        sys.path.insert(0, str(BASE_DIR))
        from scripts.assign_quality_tier import load_config

        config = load_config(Path("/nonexistent/path/config.json"))
        # Should still return defaults structure
        self.assertIn("tiers", config)
        self.assertIn("features", config)

    def test_load_config_defaults_have_tier_thresholds(self):
        """Default config should have correct tier thresholds."""
        import sys

        sys.path.insert(0, str(BASE_DIR))
        from scripts.assign_quality_tier import load_config

        config = load_config(Path("/nonexistent/path/config.json"))
        self.assertEqual(config["tiers"]["tier_1"]["min_score"], 80)
        self.assertEqual(config["tiers"]["tier_2"]["min_score"], 55)
        self.assertEqual(config["tiers"]["tier_3"]["min_score"], 0)

    def test_load_domain_trust_returns_dict(self):
        """load_domain_trust should return a dictionary."""
        import sys

        sys.path.insert(0, str(BASE_DIR))
        from scripts.assign_quality_tier import load_domain_trust

        trust_map = load_domain_trust()
        self.assertIsInstance(trust_map, dict)

    def test_load_domain_trust_has_federalreserve(self):
        """load_domain_trust should have federalreserve.gov mapped."""
        import sys

        sys.path.insert(0, str(BASE_DIR))
        from scripts.assign_quality_tier import load_domain_trust

        trust_map = load_domain_trust()
        self.assertIn("federalreserve.gov", trust_map)
        self.assertEqual(trust_map["federalreserve.gov"], 100)

    def test_load_domain_trust_medium_domains(self):
        """load_domain_trust should map medium trust domains to 50."""
        import sys

        sys.path.insert(0, str(BASE_DIR))
        from scripts.assign_quality_tier import load_domain_trust

        trust_map = load_domain_trust()
        self.assertEqual(trust_map["imf.org"], 50)
        self.assertEqual(trust_map["brookings.edu"], 50)


if __name__ == "__main__":
    unittest.main()
