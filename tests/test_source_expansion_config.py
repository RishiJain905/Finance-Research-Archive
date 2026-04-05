"""
V2.7 Part 6: Source Expansion Config Tests.

Comprehensive tests for article_source_families.json and
article_source_expansion_batch_*.json config files.

Run with:
    python -m unittest tests.test_source_expansion_config -v
"""

import json
import unittest
from pathlib import Path

# Import validation functions from scripts.validate_source_expansion
from scripts.validate_source_expansion import (
    validate_families_config,
    validate_batch_config,
    validate_all_configs,
    check_no_duplicate_urls_across_batches,
    check_family_target_counts,
)

BASE_DIR = Path(__file__).resolve().parent.parent


class TestFamiliesConfig(unittest.TestCase):
    """Test family registry config structure and validity."""

    def setUp(self):
        self.config = json.load(
            open(BASE_DIR / "config" / "article_source_families.json", encoding="utf-8")
        )

    def test_config_is_valid_json(self):
        """Families config file is valid JSON."""
        self.assertIsInstance(self.config, dict)
        self.assertGreater(len(self.config), 0)

    def test_has_families_key(self):
        """Families config has 'families' key."""
        self.assertIn("families", self.config)

    def test_has_target_classes_key(self):
        """Families config has 'target_classes' key."""
        self.assertIn("target_classes", self.config)

    def test_has_priority_tiers_key(self):
        """Families config has 'priority_tiers' key."""
        self.assertIn("priority_tiers", self.config)

    def test_all_seven_families_present(self):
        """All seven families are present."""
        families = self.config.get("families", {})
        expected_families = {
            "central_bank",
            "regional_fed",
            "regulator",
            "exchange_infrastructure",
            "research_institution",
            "treasury_fiscal",
            "global_macro",
        }
        actual_families = set(families.keys())
        self.assertEqual(expected_families, actual_families)

    def test_family_has_required_fields(self):
        """Each family has all required fields."""
        required_fields = [
            "label",
            "description",
            "default_priority_tier",
            "default_topic",
            "batch",
            "target_count",
        ]
        families = self.config.get("families", {})
        for family_id, family in families.items():
            for field in required_fields:
                self.assertIn(
                    field,
                    family,
                    f"Family '{family_id}' missing required field '{field}'",
                )

    def test_target_class_has_required_fields(self):
        """Each target class has all required fields."""
        required_fields = ["description", "typical_max_links"]
        target_classes = self.config.get("target_classes", {})
        for class_id, target_class in target_classes.items():
            for field in required_fields:
                self.assertIn(
                    field,
                    target_class,
                    f"Target class '{class_id}' missing required field '{field}'",
                )

    def test_priority_tier_has_required_fields(self):
        """Each priority tier has all required fields."""
        required_fields = ["description", "enabled_by_default"]
        priority_tiers = self.config.get("priority_tiers", {})
        for tier_name, tier in priority_tiers.items():
            for field in required_fields:
                self.assertIn(
                    field,
                    tier,
                    f"Priority tier '{tier_name}' missing required field '{field}'",
                )

    def test_valid_priority_tier_values(self):
        """Only S, A, B, C are valid priority tier names."""
        valid_tiers = {"S", "A", "B", "C"}
        priority_tiers = self.config.get("priority_tiers", {})
        actual_tiers = set(priority_tiers.keys())
        self.assertEqual(valid_tiers, actual_tiers)

    def test_valid_target_class_values(self):
        """Only valid target class values are used."""
        valid_classes = {
            "press_release",
            "speech",
            "policy_statement",
            "research_article",
            "blog",
            "market_notice",
            "infrastructure_update",
        }
        target_classes = self.config.get("target_classes", {})
        actual_classes = set(target_classes.keys())
        self.assertEqual(valid_classes, actual_classes)

    def test_family_batch_assignments(self):
        """Families are assigned to correct batches."""
        families = self.config.get("families", {})
        batch1_families = {"central_bank", "regional_fed"}
        batch2_families = {"regulator", "exchange_infrastructure"}
        batch3_families = {
            "research_institution",
            "treasury_fiscal",
            "global_macro",
        }

        for family_id, family in families.items():
            batch = family.get("batch")
            if family_id in batch1_families:
                self.assertEqual(batch, 1, f"{family_id} should be in batch 1")
            elif family_id in batch2_families:
                self.assertEqual(batch, 2, f"{family_id} should be in batch 2")
            elif family_id in batch3_families:
                self.assertEqual(batch, 3, f"{family_id} should be in batch 3")


class TestBatchConfigStructure(unittest.TestCase):
    """Test batch config file structure."""

    def setUp(self):
        self.batch1_path = BASE_DIR / "config" / "article_source_expansion_batch_1.json"
        self.batch2_path = BASE_DIR / "config" / "article_source_expansion_batch_2.json"
        self.batch3_path = BASE_DIR / "config" / "article_source_expansion_batch_3.json"

        self.batch1 = json.load(open(self.batch1_path, encoding="utf-8"))
        self.batch2 = json.load(open(self.batch2_path, encoding="utf-8"))
        self.batch3 = json.load(open(self.batch3_path, encoding="utf-8"))

    def test_batch_1_exists(self):
        """Batch 1 config file exists."""
        self.assertTrue(self.batch1_path.exists())

    def test_batch_2_exists(self):
        """Batch 2 config file exists."""
        self.assertTrue(self.batch2_path.exists())

    def test_batch_3_exists(self):
        """Batch 3 config file exists."""
        self.assertTrue(self.batch3_path.exists())

    def test_batch_1_is_valid_json(self):
        """Batch 1 config is valid JSON."""
        self.assertIsInstance(self.batch1, dict)
        self.assertGreater(len(self.batch1), 0)

    def test_batch_2_is_valid_json(self):
        """Batch 2 config is valid JSON."""
        self.assertIsInstance(self.batch2, dict)
        self.assertGreater(len(self.batch2), 0)

    def test_batch_3_is_valid_json(self):
        """Batch 3 config is valid JSON."""
        self.assertIsInstance(self.batch3, dict)
        self.assertGreater(len(self.batch3), 0)

    def test_batch_1_has_required_keys(self):
        """Batch 1 has all required keys."""
        required_keys = ["batch", "description", "families", "targets"]
        for key in required_keys:
            self.assertIn(key, self.batch1, f"Batch 1 missing key '{key}'")

    def test_batch_2_has_required_keys(self):
        """Batch 2 has all required keys."""
        required_keys = ["batch", "description", "families", "targets"]
        for key in required_keys:
            self.assertIn(key, self.batch2, f"Batch 2 missing key '{key}'")

    def test_batch_3_has_required_keys(self):
        """Batch 3 has all required keys."""
        required_keys = ["batch", "description", "families", "targets"]
        for key in required_keys:
            self.assertIn(key, self.batch3, f"Batch 3 missing key '{key}'")

    def test_batch_numbers_are_correct(self):
        """Batch numbers are correctly set."""
        self.assertEqual(self.batch1.get("batch"), 1)
        self.assertEqual(self.batch2.get("batch"), 2)
        self.assertEqual(self.batch3.get("batch"), 3)

    def test_batch_1_families_match_declaration(self):
        """Batch 1 families match family registry declaration."""
        expected_families = ["central_bank", "regional_fed"]
        actual_families = self.batch1.get("families", [])
        self.assertEqual(set(expected_families), set(actual_families))

    def test_batch_2_families_match_declaration(self):
        """Batch 2 families match family registry declaration."""
        expected_families = ["regulator", "exchange_infrastructure"]
        actual_families = self.batch2.get("families", [])
        self.assertEqual(set(expected_families), set(actual_families))

    def test_batch_3_families_match_declaration(self):
        """Batch 3 families match family registry declaration."""
        expected_families = ["research_institution", "treasury_fiscal", "global_macro"]
        actual_families = self.batch3.get("families", [])
        self.assertEqual(set(expected_families), set(actual_families))


class TestTargetRequiredFields(unittest.TestCase):
    """Test that all targets have required fields."""

    def setUp(self):
        self.families_config = json.load(
            open(BASE_DIR / "config" / "article_source_families.json", encoding="utf-8")
        )
        self.batch1 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_1.json",
                encoding="utf-8",
            )
        )
        self.batch2 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_2.json",
                encoding="utf-8",
            )
        )
        self.batch3 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_3.json",
                encoding="utf-8",
            )
        )
        self.all_targets = (
            self.batch1.get("targets", [])
            + self.batch2.get("targets", [])
            + self.batch3.get("targets", [])
        )

    def test_all_targets_have_name(self):
        """All targets have a name field."""
        for i, target in enumerate(self.all_targets):
            self.assertIn(
                "name",
                target,
                f"Target[{i}] missing 'name' field",
            )
            self.assertIsInstance(
                target.get("name"),
                str,
                f"Target[{i}] 'name' must be a string",
            )
            self.assertGreater(
                len(target.get("name", "")),
                0,
                f"Target[{i}] 'name' must be non-empty",
            )

    def test_all_targets_have_topic(self):
        """All targets have a topic field."""
        for i, target in enumerate(self.all_targets):
            self.assertIn(
                "topic",
                target,
                f"Target[{i}] missing 'topic' field",
            )

    def test_all_targets_have_url(self):
        """All targets have a url field."""
        for i, target in enumerate(self.all_targets):
            self.assertIn(
                "url",
                target,
                f"Target[{i}] missing 'url' field",
            )
            self.assertIsInstance(
                target.get("url"),
                str,
                f"Target[{i}] 'url' must be a string",
            )

    def test_all_targets_have_allowed_prefixes(self):
        """All targets have allowed_prefixes field."""
        for i, target in enumerate(self.all_targets):
            self.assertIn(
                "allowed_prefixes",
                target,
                f"Target[{i}] missing 'allowed_prefixes' field",
            )
            self.assertIsInstance(
                target.get("allowed_prefixes"),
                list,
                f"Target[{i}] 'allowed_prefixes' must be a list",
            )

    def test_all_targets_have_max_links(self):
        """All targets have max_links field."""
        for i, target in enumerate(self.all_targets):
            self.assertIn(
                "max_links",
                target,
                f"Target[{i}] missing 'max_links' field",
            )

    def test_all_targets_have_family(self):
        """All targets have family field."""
        for i, target in enumerate(self.all_targets):
            self.assertIn(
                "family",
                target,
                f"Target[{i}] missing 'family' field",
            )

    def test_all_targets_have_target_class(self):
        """All targets have target_class field."""
        for i, target in enumerate(self.all_targets):
            self.assertIn(
                "target_class",
                target,
                f"Target[{i}] missing 'target_class' field",
            )

    def test_all_targets_have_priority_tier(self):
        """All targets have priority_tier field."""
        for i, target in enumerate(self.all_targets):
            self.assertIn(
                "priority_tier",
                target,
                f"Target[{i}] missing 'priority_tier' field",
            )

    def test_all_targets_have_enabled(self):
        """All targets have enabled field."""
        for i, target in enumerate(self.all_targets):
            self.assertIn(
                "enabled",
                target,
                f"Target[{i}] missing 'enabled' field",
            )

    def test_all_targets_have_min_word_count(self):
        """All targets have min_word_count field."""
        for i, target in enumerate(self.all_targets):
            self.assertIn(
                "min_word_count",
                target,
                f"Target[{i}] missing 'min_word_count' field",
            )

    def test_all_targets_have_blocked_keywords(self):
        """All targets have blocked_keywords field."""
        for i, target in enumerate(self.all_targets):
            self.assertIn(
                "blocked_keywords",
                target,
                f"Target[{i}] missing 'blocked_keywords' field",
            )


class TestTargetFieldValues(unittest.TestCase):
    """Test that target field values are valid."""

    def setUp(self):
        self.families_config = json.load(
            open(BASE_DIR / "config" / "article_source_families.json", encoding="utf-8")
        )
        self.families = self.families_config.get("families", {})
        self.target_classes = self.families_config.get("target_classes", {})

        self.batch1 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_1.json",
                encoding="utf-8",
            )
        )
        self.batch2 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_2.json",
                encoding="utf-8",
            )
        )
        self.batch3 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_3.json",
                encoding="utf-8",
            )
        )
        self.all_targets = (
            self.batch1.get("targets", [])
            + self.batch2.get("targets", [])
            + self.batch3.get("targets", [])
        )

    def test_family_references_valid(self):
        """All target family references are valid."""
        valid_families = set(self.families.keys())
        for i, target in enumerate(self.all_targets):
            family = target.get("family", "")
            self.assertIn(
                family,
                valid_families,
                f"Target[{i}] ('{target.get('name', 'unknown')}') references invalid family '{family}'",
            )

    def test_target_class_references_valid(self):
        """All target target_class references are valid."""
        valid_classes = set(self.target_classes.keys())
        for i, target in enumerate(self.all_targets):
            target_class = target.get("target_class", "")
            self.assertIn(
                target_class,
                valid_classes,
                f"Target[{i}] ('{target.get('name', 'unknown')}') references invalid target_class '{target_class}'",
            )

    def test_priority_tier_is_valid(self):
        """All target priority_tier values are valid."""
        valid_tiers = {"S", "A", "B", "C"}
        for i, target in enumerate(self.all_targets):
            tier = target.get("priority_tier", "")
            self.assertIn(
                tier,
                valid_tiers,
                f"Target[{i}] ('{target.get('name', 'unknown')}') has invalid priority_tier '{tier}'",
            )

    def test_enabled_is_boolean(self):
        """All target enabled values are boolean."""
        for i, target in enumerate(self.all_targets):
            enabled = target.get("enabled")
            self.assertIsInstance(
                enabled,
                bool,
                f"Target[{i}] ('{target.get('name', 'unknown')}') enabled must be boolean, got {type(enabled)}",
            )

    def test_topic_is_valid_enum(self):
        """All target topic values are valid."""
        valid_topics = {"macro catalysts", "market structure"}
        for i, target in enumerate(self.all_targets):
            topic = target.get("topic", "")
            self.assertIn(
                topic,
                valid_topics,
                f"Target[{i}] ('{target.get('name', 'unknown')}') has invalid topic '{topic}'",
            )

    def test_allowed_prefixes_is_non_empty_list(self):
        """All target allowed_prefixes are non-empty lists."""
        for i, target in enumerate(self.all_targets):
            prefixes = target.get("allowed_prefixes", [])
            self.assertIsInstance(
                prefixes,
                list,
                f"Target[{i}] ('{target.get('name', 'unknown')}') allowed_prefixes must be a list",
            )
            self.assertGreater(
                len(prefixes),
                0,
                f"Target[{i}] ('{target.get('name', 'unknown')}') allowed_prefixes must be non-empty",
            )

    def test_max_links_is_positive_integer(self):
        """All target max_links are positive integers."""
        for i, target in enumerate(self.all_targets):
            max_links = target.get("max_links")
            self.assertIsInstance(
                max_links,
                int,
                f"Target[{i}] ('{target.get('name', 'unknown')}') max_links must be an integer",
            )
            self.assertGreater(
                max_links,
                0,
                f"Target[{i}] ('{target.get('name', 'unknown')}') max_links must be positive",
            )

    def test_min_word_count_is_positive_integer(self):
        """All target min_word_count are positive integers."""
        for i, target in enumerate(self.all_targets):
            min_word_count = target.get("min_word_count")
            self.assertIsInstance(
                min_word_count,
                int,
                f"Target[{i}] ('{target.get('name', 'unknown')}') min_word_count must be an integer",
            )
            self.assertGreater(
                min_word_count,
                0,
                f"Target[{i}] ('{target.get('name', 'unknown')}') min_word_count must be positive",
            )

    def test_url_is_non_empty_string(self):
        """All target URLs are non-empty strings."""
        for i, target in enumerate(self.all_targets):
            url = target.get("url", "")
            self.assertIsInstance(
                url,
                str,
                f"Target[{i}] ('{target.get('name', 'unknown')}') url must be a string",
            )
            self.assertGreater(
                len(url),
                0,
                f"Target[{i}] ('{target.get('name', 'unknown')}') url must be non-empty",
            )

    def test_url_contains_protocol(self):
        """All target URLs contain a protocol (://)."""
        for i, target in enumerate(self.all_targets):
            url = target.get("url", "")
            self.assertIn(
                "://",
                url,
                f"Target[{i}] ('{target.get('name', 'unknown')}') url must contain '://'",
            )


class TestDisabledTargets(unittest.TestCase):
    """Test disabled target conventions."""

    def setUp(self):
        self.batch1 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_1.json",
                encoding="utf-8",
            )
        )
        self.batch2 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_2.json",
                encoding="utf-8",
            )
        )
        self.batch3 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_3.json",
                encoding="utf-8",
            )
        )
        self.all_targets = (
            self.batch1.get("targets", [])
            + self.batch2.get("targets", [])
            + self.batch3.get("targets", [])
        )

    def test_all_targets_start_disabled(self):
        """Phase 1: all targets should be disabled."""
        enabled_targets = [
            t.get("name", f"unnamed[{i}]")
            for i, t in enumerate(self.all_targets)
            if t.get("enabled", True)
        ]
        self.assertEqual(
            [],
            enabled_targets,
            f"All targets should be disabled in Phase 1. Found enabled: {enabled_targets}",
        )

    def test_disabled_targets_have_reason(self):
        """All disabled targets should have disabled_reason field."""
        missing_reason = []
        for i, target in enumerate(self.all_targets):
            if target.get("enabled") is False:
                if not target.get("disabled_reason"):
                    missing_reason.append(target.get("name", f"unnamed[{i}]"))

        self.assertEqual(
            [],
            missing_reason,
            f"Disabled targets missing disabled_reason: {missing_reason}",
        )


class TestNoDuplicateURLs(unittest.TestCase):
    """Test that no URLs are duplicated across batches."""

    def setUp(self):
        self.batch1 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_1.json",
                encoding="utf-8",
            )
        )
        self.batch2 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_2.json",
                encoding="utf-8",
            )
        )
        self.batch3 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_3.json",
                encoding="utf-8",
            )
        )

    def test_no_duplicate_urls_within_batch_1(self):
        """Batch 1 has no duplicate URLs."""
        targets = self.batch1.get("targets", [])
        urls = [t.get("url", "") for t in targets if t.get("url")]
        seen = set()
        duplicates = []
        for url in urls:
            if url in seen:
                duplicates.append(url)
            seen.add(url)
        self.assertEqual([], duplicates, f"Duplicate URLs in batch 1: {duplicates}")

    def test_no_duplicate_urls_within_batch_2(self):
        """Batch 2 has no duplicate URLs."""
        targets = self.batch2.get("targets", [])
        urls = [t.get("url", "") for t in targets if t.get("url")]
        seen = set()
        duplicates = []
        for url in urls:
            if url in seen:
                duplicates.append(url)
            seen.add(url)
        self.assertEqual([], duplicates, f"Duplicate URLs in batch 2: {duplicates}")

    def test_no_duplicate_urls_within_batch_3(self):
        """Batch 3 has no duplicate URLs."""
        targets = self.batch3.get("targets", [])
        urls = [t.get("url", "") for t in targets if t.get("url")]
        seen = set()
        duplicates = []
        for url in urls:
            if url in seen:
                duplicates.append(url)
            seen.add(url)
        self.assertEqual([], duplicates, f"Duplicate URLs in batch 3: {duplicates}")

    def test_no_duplicate_urls_across_all_batches(self):
        """No URLs are duplicated across all batches."""
        errors = check_no_duplicate_urls_across_batches(
            [self.batch1, self.batch2, self.batch3]
        )
        self.assertEqual([], errors, f"Duplicate URLs across batches: {errors}")


class TestFamilyTargetCounts(unittest.TestCase):
    """Test that family target counts meet minimums from spec."""

    def setUp(self):
        self.families_config = json.load(
            open(BASE_DIR / "config" / "article_source_families.json", encoding="utf-8")
        )
        self.batch1 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_1.json",
                encoding="utf-8",
            )
        )
        self.batch2 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_2.json",
                encoding="utf-8",
            )
        )
        self.batch3 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_3.json",
                encoding="utf-8",
            )
        )
        self.batches = [self.batch1, self.batch2, self.batch3]

        # Count targets per family
        self.family_counts = {f: 0 for f in self.families_config.get("families", {})}
        for batch in self.batches:
            for target in batch.get("targets", []):
                family = target.get("family", "")
                if family in self.family_counts:
                    self.family_counts[family] += 1

    def test_central_bank_has_20_plus(self):
        """Central bank family has at least 20 targets."""
        count = self.family_counts.get("central_bank", 0)
        self.assertGreaterEqual(
            count,
            20,
            f"central_bank family has {count} targets, expected at least 20",
        )

    def test_regional_fed_has_15_plus(self):
        """Regional Fed family has at least 15 targets."""
        count = self.family_counts.get("regional_fed", 0)
        self.assertGreaterEqual(
            count,
            15,
            f"regional_fed family has {count} targets, expected at least 15",
        )

    def test_regulator_has_15_plus(self):
        """Regulator family has at least 15 targets."""
        count = self.family_counts.get("regulator", 0)
        self.assertGreaterEqual(
            count,
            15,
            f"regulator family has {count} targets, expected at least 15",
        )

    def test_exchange_infrastructure_has_15_plus(self):
        """Exchange infrastructure family has at least 15 targets."""
        count = self.family_counts.get("exchange_infrastructure", 0)
        self.assertGreaterEqual(
            count,
            15,
            f"exchange_infrastructure family has {count} targets, expected at least 15",
        )

    def test_research_institution_has_20_plus(self):
        """Research institution family has at least 20 targets."""
        count = self.family_counts.get("research_institution", 0)
        self.assertGreaterEqual(
            count,
            20,
            f"research_institution family has {count} targets, expected at least 20",
        )

    def test_treasury_fiscal_has_10_plus(self):
        """Treasury fiscal family has at least 10 targets."""
        count = self.family_counts.get("treasury_fiscal", 0)
        self.assertGreaterEqual(
            count,
            10,
            f"treasury_fiscal family has {count} targets, expected at least 10",
        )

    def test_global_macro_has_10_plus(self):
        """Global macro family has at least 10 targets."""
        count = self.family_counts.get("global_macro", 0)
        self.assertGreaterEqual(
            count,
            10,
            f"global_macro family has {count} targets, expected at least 10",
        )

    def test_total_sources_100_plus(self):
        """Total sources across all batches is at least 100."""
        total = sum(self.family_counts.values())
        self.assertGreaterEqual(
            total,
            100,
            f"Total sources is {total}, expected at least 100",
        )


class TestIntegrationWithIngestSources(unittest.TestCase):
    """Test that new targets are compatible with ingest_sources.py field consumption."""

    def setUp(self):
        self.batch1 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_1.json",
                encoding="utf-8",
            )
        )
        self.batch2 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_2.json",
                encoding="utf-8",
            )
        )
        self.batch3 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_3.json",
                encoding="utf-8",
            )
        )
        self.all_targets = (
            self.batch1.get("targets", [])
            + self.batch2.get("targets", [])
            + self.batch3.get("targets", [])
        )

    def test_targets_have_fields_ingest_sources_expects(self):
        """All targets have fields that ingest_sources.py expects."""
        required_fields = [
            "name",
            "topic",
            "url",
            "allowed_prefixes",
            "url_blocklist_fragments",
            "max_links",
            "required_keywords",
            "blocked_keywords",
            "min_word_count",
            "expected_language",
            "allowed_page_types",
        ]

        missing_fields = []
        for i, target in enumerate(self.all_targets):
            for field in required_fields:
                if field not in target:
                    missing_fields.append(
                        f"Target[{i}] ('{target.get('name', 'unknown')}') missing '{field}'"
                    )

        self.assertEqual([], missing_fields, f"Missing fields: {missing_fields}")

    def test_targets_have_sensible_blocklist_fragments(self):
        """All targets have at least 3 blocklist fragments."""
        targets_with_few_fragments = []
        for i, target in enumerate(self.all_targets):
            fragments = target.get("url_blocklist_fragments", [])
            if len(fragments) < 3:
                targets_with_few_fragments.append(
                    f"Target[{i}] ('{target.get('name', 'unknown')}') has {len(fragments)} fragments"
                )

        self.assertEqual(
            [],
            targets_with_few_fragments,
            f"Targets with fewer than 3 blocklist fragments: {targets_with_few_fragments}",
        )

    def test_targets_have_allowed_page_types(self):
        """All targets have allowed_page_types field."""
        missing = []
        for i, target in enumerate(self.all_targets):
            if "allowed_page_types" not in target:
                missing.append(f"Target[{i}] ('{target.get('name', 'unknown')}')")

        self.assertEqual([], missing, f"Targets missing allowed_page_types: {missing}")


class TestEndToEndValidation(unittest.TestCase):
    """Test the full validation script runs clean."""

    def test_validation_script_imports(self):
        """Validation script can be imported without errors."""
        from scripts import validate_source_expansion

        self.assertIsNotNone(validate_source_expansion)

    def test_validate_all_configs_returns_no_errors(self):
        """validate_all_configs returns no errors for valid configs."""
        results = validate_all_configs(BASE_DIR)

        # Filter out configs with errors
        configs_with_errors = {
            k: v for k, v in results.items() if v and k != "_cross_validation"
        }

        # Get cross-validation errors
        cross_errors = results.get("_cross_validation", [])

        all_errors = []
        for path, errors in configs_with_errors.items():
            all_errors.extend([f"{path}: {e}" for e in errors])
        all_errors.extend(cross_errors)

        self.assertEqual([], all_errors, f"Validation errors found: {all_errors}")

    def test_validate_families_config_passes(self):
        """validate_families_config returns no errors."""
        families_config = json.load(
            open(BASE_DIR / "config" / "article_source_families.json", encoding="utf-8")
        )
        errors = validate_families_config(families_config)
        self.assertEqual(
            [],
            errors,
            f"Families config validation errors: {errors}",
        )

    def test_validate_batch_configs_pass(self):
        """All batch configs pass validation."""
        families_config = json.load(
            open(BASE_DIR / "config" / "article_source_families.json", encoding="utf-8")
        )
        families = families_config.get("families", {})
        target_classes = families_config.get("target_classes", {})

        batch_files = [
            BASE_DIR / "config" / "article_source_expansion_batch_1.json",
            BASE_DIR / "config" / "article_source_expansion_batch_2.json",
            BASE_DIR / "config" / "article_source_expansion_batch_3.json",
        ]

        all_errors = []
        for batch_path in batch_files:
            batch_config = json.load(open(batch_path, encoding="utf-8"))
            errors = validate_batch_config(batch_config, families, target_classes)
            if errors:
                all_errors.extend([f"{batch_path.name}: {e}" for e in errors])

        self.assertEqual([], all_errors, f"Batch validation errors: {all_errors}")

    def test_no_duplicate_urls_check_passes(self):
        """No duplicate URLs check passes across all batches."""
        batch1 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_1.json",
                encoding="utf-8",
            )
        )
        batch2 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_2.json",
                encoding="utf-8",
            )
        )
        batch3 = json.load(
            open(
                BASE_DIR / "config" / "article_source_expansion_batch_3.json",
                encoding="utf-8",
            )
        )

        errors = check_no_duplicate_urls_across_batches([batch1, batch2, batch3])
        self.assertEqual([], errors, f"Duplicate URL errors: {errors}")


if __name__ == "__main__":
    unittest.main()
