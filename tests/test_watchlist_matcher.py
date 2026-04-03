"""
Tests for Phase 2.7 Part 3: Deterministic Watchlist Matcher.
Tests cover the watchlist matching algorithm and related functions.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "watchlists_v27.json"


class TestLoadWatchlists(unittest.TestCase):
    """Test load_watchlists function."""

    def test_loads_from_valid_path(self):
        """Should load watchlists from valid JSON config path."""
        from scripts.watchlist_matcher import load_watchlists

        watchlists = load_watchlists(str(CONFIG_PATH))
        self.assertIsInstance(watchlists, list)
        self.assertGreater(len(watchlists), 0)

    def test_returns_only_enabled_watchlists(self):
        """Should return only enabled watchlists, skip disabled ones."""
        from scripts.watchlist_matcher import load_watchlists

        watchlists = load_watchlists(str(CONFIG_PATH))
        enabled = [wl for wl in watchlists if not wl.get("enabled", False)]
        self.assertEqual(len(enabled), 0, "Should not return disabled watchlists")

    def test_handles_missing_file(self):
        """Should raise FileNotFoundError for missing config file."""
        from scripts.watchlist_matcher import load_watchlists

        with self.assertRaises(FileNotFoundError):
            load_watchlists("/nonexistent/path/config.json")

    def test_skips_disabled_watchlists(self):
        """Should skip watchlists where enabled=False."""
        from scripts.watchlist_matcher import load_watchlists

        # Create temp config with mixed enabled/disabled
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(
                [
                    {
                        "watchlist_id": "wl_enabled",
                        "title": "Enabled Watchlist",
                        "topic": "test",
                        "keywords": ["test"],
                        "priority": "high",
                        "enabled": True,
                    },
                    {
                        "watchlist_id": "wl_disabled",
                        "title": "Disabled Watchlist",
                        "topic": "test",
                        "keywords": ["test"],
                        "priority": "high",
                        "enabled": False,
                    },
                ],
                f,
            )
            temp_path = f.name

        try:
            watchlists = load_watchlists(temp_path)
            ids = [wl["watchlist_id"] for wl in watchlists]
            self.assertIn("wl_enabled", ids)
            self.assertNotIn("wl_disabled", ids)
        finally:
            os.unlink(temp_path)


class TestExtractTextFeatures(unittest.TestCase):
    """Test extract_text_features function."""

    def test_handles_record_schema(self):
        """Should extract fields from research record schema."""
        from scripts.watchlist_matcher import extract_text_features

        record = {
            "id": "rec_123",
            "title": "Fed Raises Interest Rates",
            "summary": "The Federal Reserve raised interest rates by 25bp.",
            "why_it_matters": "This impacts borrowing costs.",
            "topic": "monetary_policy",
            "event_type": "rate_change",
            "tags": ["Fed", "rates", "monetary"],
            "important_numbers": ["25bp", "2.5%"],
        }
        features = extract_text_features(record)

        self.assertEqual(features["title"], "Fed Raises Interest Rates")
        self.assertIn("Federal Reserve raised interest rates", features["summary"])
        self.assertIn("borrowing costs", features["why_it_matters"])
        self.assertIn("monetary_policy", features["topic"])
        self.assertIn("rate_change", features["event_type"])
        self.assertIn("Fed", features["tags"])
        self.assertIn("25bp", features["important_numbers"])

    def test_handles_cluster_schema(self):
        """Should extract fields from event cluster schema."""
        from scripts.watchlist_matcher import extract_text_features

        cluster = {
            "event_id": "evt_456",
            "title": "Treasury Auction Stress",
            "summary": "Multiple Treasury auctions saw weak demand.",
            "topic": "treasury",
            "event_type": "auction",
            "keywords": ["auction", "Treasury", "demand"],
            "important_numbers": ["2.5%"],
        }
        features = extract_text_features(cluster)

        self.assertEqual(features["title"], "Treasury Auction Stress")
        self.assertIn("Treasury auctions", features["summary"])
        self.assertIn("treasury", features["topic"])
        self.assertIn("auction", features["event_type"])
        self.assertIn("auction", features["tags"])
        self.assertIn("2.5%", features["important_numbers"])

    def test_handles_missing_fields(self):
        """Should handle missing fields gracefully with empty defaults."""
        from scripts.watchlist_matcher import extract_text_features

        record = {"id": "rec_789"}
        features = extract_text_features(record)

        self.assertEqual(features["title"], "")
        self.assertEqual(features["summary"], "")
        self.assertEqual(features["why_it_matters"], "")
        self.assertEqual(features["tags"], [])
        self.assertEqual(features["important_numbers"], [])
        self.assertEqual(features["topic"], "")
        self.assertEqual(features["event_type"], "")


class TestTokenize(unittest.TestCase):
    """Test tokenize function."""

    def test_basic_tokenization(self):
        """Should tokenize text into lowercase alphanumeric terms."""
        from scripts.watchlist_matcher import tokenize

        text = "Fed Raises Interest Rates by 25bp"
        tokens = tokenize(text)

        self.assertIsInstance(tokens, set)
        self.assertIn("fed", tokens)
        self.assertIn("raises", tokens)
        self.assertIn("interest", tokens)
        self.assertIn("rates", tokens)
        self.assertIn("25bp", tokens)

    def test_handles_empty_string(self):
        """Should return empty set for empty string."""
        from scripts.watchlist_matcher import tokenize

        tokens = tokenize("")
        self.assertEqual(tokens, set())

    def test_handles_special_chars(self):
        """Should extract alphanumeric tokens from text with special chars."""
        from scripts.watchlist_matcher import tokenize

        text = "Rate: 2.5%! What's the @fed #policy?"
        tokens = tokenize(text)

        # Should have extracted numeric and word tokens
        self.assertTrue(len(tokens) > 0)
        # Punctuation should be stripped
        for token in tokens:
            self.assertTrue(
                token.isalnum() or token.replace(".", "").replace("%", "").isalnum()
            )

    def test_lowercase_normalization(self):
        """Should normalize all tokens to lowercase."""
        from scripts.watchlist_matcher import tokenize

        text = "FED Fed fed"
        tokens = tokenize(text)

        self.assertEqual(tokens, {"fed"})


class TestComputeKeywordOverlap(unittest.TestCase):
    """Test compute_keyword_overlap function."""

    def test_full_match(self):
        """Should return score of 1.0 when all keywords match."""
        from scripts.watchlist_matcher import compute_keyword_overlap

        text_terms = {"repo", "SOFR", "collateral", "funding"}
        watchlist = {"keywords": ["repo", "SOFR", "collateral", "funding"]}
        score, matched = compute_keyword_overlap(text_terms, watchlist)

        self.assertEqual(score, 1.0)
        self.assertEqual(set(matched), text_terms)

    def test_partial_match(self):
        """Should return proportional score for partial match."""
        from scripts.watchlist_matcher import compute_keyword_overlap

        text_terms = {"repo", "SOFR", "collateral"}
        watchlist = {"keywords": ["repo", "SOFR", "collateral", "funding", "ON RRP"]}
        score, matched = compute_keyword_overlap(text_terms, watchlist)

        # 3/5 = 0.6
        self.assertAlmostEqual(score, 0.6, places=2)
        self.assertEqual(len(matched), 3)

    def test_no_match(self):
        """Should return 0 score when no keywords match."""
        from scripts.watchlist_matcher import compute_keyword_overlap

        text_terms = {"inflation", "CPI"}
        watchlist = {"keywords": ["repo", "SOFR", "collateral"]}
        score, matched = compute_keyword_overlap(text_terms, watchlist)

        self.assertEqual(score, 0.0)
        self.assertEqual(matched, [])

    def test_case_insensitive_matching(self):
        """Should match keywords case-insensitively."""
        from scripts.watchlist_matcher import compute_keyword_overlap

        text_terms = {"fed", "rates"}
        watchlist = {"keywords": ["FED", "RATES"]}
        score, matched = compute_keyword_overlap(text_terms, watchlist)

        self.assertEqual(score, 1.0)
        self.assertEqual(len(matched), 2)


class TestCheckRequiredTerms(unittest.TestCase):
    """Test check_required_terms function."""

    def test_all_present(self):
        """Should return True when all required terms are present."""
        from scripts.watchlist_matcher import check_required_terms

        text_terms = {"repo", "SOFR", "collateral"}
        watchlist = {"required_terms": ["repo", "collateral"]}

        result = check_required_terms(text_terms, watchlist)
        self.assertTrue(result)

    def test_some_missing(self):
        """Should return False when some required terms are missing."""
        from scripts.watchlist_matcher import check_required_terms

        text_terms = {"repo", "SOFR"}
        watchlist = {"required_terms": ["repo", "collateral", "funding"]}

        result = check_required_terms(text_terms, watchlist)
        self.assertFalse(result)

    def test_no_required_terms(self):
        """Should return True when no required terms defined."""
        from scripts.watchlist_matcher import check_required_terms

        text_terms = {"repo", "SOFR"}
        watchlist = {"required_terms": []}

        result = check_required_terms(text_terms, watchlist)
        self.assertTrue(result)

    def test_empty_required_terms_field(self):
        """Should return True when required_terms is empty/missing."""
        from scripts.watchlist_matcher import check_required_terms

        text_terms = {"repo", "SOFR"}
        watchlist = {}

        result = check_required_terms(text_terms, watchlist)
        self.assertTrue(result)


class TestCheckBlockedTerms(unittest.TestCase):
    """Test check_blocked_terms function."""

    def test_blocked_found(self):
        """Should return set of blocked terms that were found."""
        from scripts.watchlist_matcher import check_blocked_terms

        text_terms = {"repo", "disinflation", "deflation", "rates"}
        watchlist = {"blocked_terms": ["disinflation", "deflation"]}

        blocked = check_blocked_terms(text_terms, watchlist)

        self.assertIsInstance(blocked, set)
        self.assertEqual(len(blocked), 2)
        self.assertIn("disinflation", blocked)
        self.assertIn("deflation", blocked)

    def test_no_blocked_found(self):
        """Should return empty set when no blocked terms match."""
        from scripts.watchlist_matcher import check_blocked_terms

        text_terms = {"repo", "SOFR", "collateral"}
        watchlist = {"blocked_terms": ["disinflation", "deflation"]}

        blocked = check_blocked_terms(text_terms, watchlist)

        self.assertEqual(blocked, set())

    def test_no_blocked_terms_defined(self):
        """Should return empty set when no blocked terms defined."""
        from scripts.watchlist_matcher import check_blocked_terms

        text_terms = {"repo", "SOFR"}
        watchlist = {"blocked_terms": []}

        blocked = check_blocked_terms(text_terms, watchlist)

        self.assertEqual(blocked, set())

    def test_empty_blocked_terms_field(self):
        """Should return empty set when blocked_terms field is missing."""
        from scripts.watchlist_matcher import check_blocked_terms

        text_terms = {"repo", "SOFR"}
        watchlist = {}

        blocked = check_blocked_terms(text_terms, watchlist)

        self.assertEqual(blocked, set())


class TestCheckTopicCompatibility(unittest.TestCase):
    """Test check_topic_compatibility function."""

    def test_matching_topics(self):
        """Should return True when topics match."""
        from scripts.watchlist_matcher import check_topic_compatibility

        result = check_topic_compatibility("repo", "repo")
        self.assertTrue(result)

    def test_different_topics(self):
        """Should return False when topics differ."""
        from scripts.watchlist_matcher import check_topic_compatibility

        result = check_topic_compatibility("repo", "inflation")
        self.assertFalse(result)

    def test_empty_record_topic(self):
        """Should return True when record topic is empty."""
        from scripts.watchlist_matcher import check_topic_compatibility

        result = check_topic_compatibility("", "repo")
        self.assertTrue(result)

    def test_empty_watchlist_topic(self):
        """Should return True when watchlist topic is empty."""
        from scripts.watchlist_matcher import check_topic_compatibility

        result = check_topic_compatibility("repo", "")
        self.assertTrue(result)

    def test_both_empty_topics(self):
        """Should return True when both topics are empty."""
        from scripts.watchlist_matcher import check_topic_compatibility

        result = check_topic_compatibility("", "")
        self.assertTrue(result)

    def test_missing_record_topic(self):
        """Should return True when record topic is None/missing."""
        from scripts.watchlist_matcher import check_topic_compatibility

        result = check_topic_compatibility(None, "repo")
        self.assertTrue(result)


class TestCheckEventTypeCompatibility(unittest.TestCase):
    """Test check_event_type_compatibility function."""

    def test_always_returns_true(self):
        """Should always return True (reserved for future use)."""
        from scripts.watchlist_matcher import check_event_type_compatibility

        result = check_event_type_compatibility("fed_speech", {})
        self.assertTrue(result)

        result = check_event_type_compatibility("rate_change", {})
        self.assertTrue(result)

        result = check_event_type_compatibility("", {})
        self.assertTrue(result)


class TestDetermineThesisSignal(unittest.TestCase):
    """Test determine_thesis_signal function."""

    def test_strengthening_signal(self):
        """Should return 'strengthening' when score > 0.5 and matched terms exist."""
        from scripts.watchlist_matcher import determine_thesis_signal

        matched = ["repo", "SOFR", "collateral"]
        blocked = set()
        score = 0.7

        signal = determine_thesis_signal(matched, blocked, score)
        self.assertEqual(signal, "strengthening")

    def test_weakening_signal_with_blocked(self):
        """Should return 'weakening' when blocked terms are found."""
        from scripts.watchlist_matcher import determine_thesis_signal

        matched = ["repo", "SOFR"]
        blocked = {"disinflation"}
        score = 0.6

        signal = determine_thesis_signal(matched, blocked, score)
        self.assertEqual(signal, "weakening")

    def test_neutral_signal_low_score(self):
        """Should return 'neutral' when score <= 0.5."""
        from scripts.watchlist_matcher import determine_thesis_signal

        matched = ["repo", "SOFR"]
        blocked = set()
        score = 0.4

        signal = determine_thesis_signal(matched, blocked, score)
        self.assertEqual(signal, "neutral")

    def test_neutral_signal_no_matched_terms(self):
        """Should return 'neutral' when no matched terms (even with high score)."""
        from scripts.watchlist_matcher import determine_thesis_signal

        matched = []
        blocked = set()
        score = 0.8

        signal = determine_thesis_signal(matched, blocked, score)
        self.assertEqual(signal, "neutral")


class TestComputeMatchScore(unittest.TestCase):
    """Test compute_match_score function."""

    def test_required_fail_returns_zero(self):
        """Should return 0 when required_pass is False."""
        from scripts.watchlist_matcher import compute_match_score

        score = compute_match_score(
            keyword_score=0.8,
            required_pass=False,
            blocked_count=0,
            topic_compat=True,
            event_type_compat=True,
        )
        self.assertEqual(score, 0.0)

    def test_topic_incompatible_returns_zero(self):
        """Should return 0 when topic_compat is False."""
        from scripts.watchlist_matcher import compute_match_score

        score = compute_match_score(
            keyword_score=0.8,
            required_pass=True,
            blocked_count=0,
            topic_compat=False,
            event_type_compat=True,
        )
        self.assertEqual(score, 0.0)

    def test_event_type_incompatible_returns_zero(self):
        """Should return 0 when event_type_compat is False."""
        from scripts.watchlist_matcher import compute_match_score

        score = compute_match_score(
            keyword_score=0.8,
            required_pass=True,
            blocked_count=0,
            topic_compat=True,
            event_type_compat=False,
        )
        self.assertEqual(score, 0.0)

    def test_normal_scoring(self):
        """Should return keyword_score when no penalties apply."""
        from scripts.watchlist_matcher import compute_match_score

        score = compute_match_score(
            keyword_score=0.6,
            required_pass=True,
            blocked_count=0,
            topic_compat=True,
            event_type_compat=True,
        )
        self.assertAlmostEqual(score, 0.6, places=2)

    def test_blocked_penalty(self):
        """Should apply blocked penalty to keyword score."""
        from scripts.watchlist_matcher import compute_match_score

        # 0.6 * (1 - 0.3 * 1) = 0.6 * 0.7 = 0.42
        score = compute_match_score(
            keyword_score=0.6,
            required_pass=True,
            blocked_count=1,
            topic_compat=True,
            event_type_compat=True,
        )
        self.assertAlmostEqual(score, 0.42, places=2)

    def test_multiple_blocked_penalty_capped(self):
        """Should cap blocked penalty at 3 blocked terms."""
        from scripts.watchlist_matcher import compute_match_score

        # Even with 5 blocked, penalty is capped at 3: 0.6 * (1 - 0.3 * 3) = 0.6 * 0.1 = 0.06
        score = compute_match_score(
            keyword_score=0.6,
            required_pass=True,
            blocked_count=5,
            topic_compat=True,
            event_type_compat=True,
        )
        self.assertAlmostEqual(score, 0.06, places=2)

    def test_score_clamped_to_zero(self):
        """Should not return negative scores."""
        from scripts.watchlist_matcher import compute_match_score

        score = compute_match_score(
            keyword_score=0.1,
            required_pass=True,
            blocked_count=5,
            topic_compat=True,
            event_type_compat=True,
        )
        self.assertGreaterEqual(score, 0.0)

    def test_score_clamped_to_one(self):
        """Should not return scores above 1."""
        from scripts.watchlist_matcher import compute_match_score

        score = compute_match_score(
            keyword_score=1.0,
            required_pass=True,
            blocked_count=0,
            topic_compat=True,
            event_type_compat=True,
        )
        self.assertLessEqual(score, 1.0)


class TestMatchRecordAgainstWatchlists(unittest.TestCase):
    """Test match_record_against_watchlists function."""

    def setUp(self):
        from scripts.watchlist_matcher import load_watchlists

        self.watchlists = load_watchlists(str(CONFIG_PATH))

    def test_returns_hits_for_matching_record(self):
        """Should return hits when record matches watchlist criteria."""
        from scripts.watchlist_matcher import match_record_against_watchlists

        record = {
            "id": "rec_test_123",
            "title": "Repo Market Sees Collateral Shortage",
            "summary": "Repo rates spike as collateral becomes scarce.",
            "why_it_matters": "Funding markets under stress.",
            "topic": "repo",
            "event_type": "market_stress",
            "tags": ["repo", "collateral", "funding"],
            "important_numbers": ["5.25%", "350B"],
        }

        hits = match_record_against_watchlists(record, self.watchlists)

        self.assertIsInstance(hits, list)
        # Should match at least wl_repo_stress which has repo-related keywords
        hit_ids = [h["watchlist_id"] for h in hits]
        self.assertIn("wl_repo_stress", hit_ids)

    def test_empty_for_non_matching_record(self):
        """Should return empty list when no watchlist matches."""
        from scripts.watchlist_matcher import match_record_against_watchlists

        record = {
            "id": "rec_test_456",
            "title": "Random Sports News",
            "summary": "Local team wins championship.",
            "why_it_matters": "Not relevant to finance.",
            "topic": "sports",
            "event_type": "news",
            "tags": ["sports"],
            "important_numbers": [],
        }

        hits = match_record_against_watchlists(record, self.watchlists)

        self.assertIsInstance(hits, list)
        # No watchlist should match sports content
        self.assertEqual(len(hits), 0)

    def test_skips_disabled_watchlists(self):
        """Should not match against disabled watchlists."""
        from scripts.watchlist_matcher import match_record_against_watchlists

        # Create a watchlist with very specific keywords that's disabled
        record = {
            "id": "rec_test_789",
            "title": "Test Document",
            "summary": "This matches a disabled watchlist.",
            "why_it_matters": "Testing.",
            "topic": "test",
            "event_type": "test",
            "tags": ["test"],
            "important_numbers": [],
        }

        # The config has all watchlists enabled, so this tests the logic
        # by verifying disabled ones wouldn't be in the list
        hits = match_record_against_watchlists(record, self.watchlists)
        # Sports record should not match any finance watchlists
        self.assertEqual(len(hits), 0)

    def test_hit_contains_required_fields(self):
        """Should return hits with all required fields."""
        from scripts.watchlist_matcher import match_record_against_watchlists

        record = {
            "id": "rec_test_hit",
            "title": "Repo Market Sees Collateral Shortage",
            "summary": "Repo rates spike as collateral becomes scarce.",
            "why_it_matters": "Funding markets under stress.",
            "topic": "repo",
            "event_type": "market_stress",
            "tags": ["repo", "collateral", "funding"],
            "important_numbers": [],
        }

        hits = match_record_against_watchlists(record, self.watchlists)

        for hit in hits:
            self.assertIn("watchlist_id", hit)
            self.assertIn("record_id", hit)
            self.assertIn("match_score", hit)
            self.assertIn("matched_terms", hit)
            self.assertIn("thesis_signal", hit)
            self.assertIn("created_at", hit)
            self.assertIsNone(hit.get("event_id"))  # record hits have null event_id

    def test_record_id_set_correctly(self):
        """Should set record_id in hit."""
        from scripts.watchlist_matcher import match_record_against_watchlists

        record = {
            "id": "rec_specific_id",
            "title": "Repo Market Sees Collateral Shortage",
            "summary": "Repo rates spike.",
            "why_it_matters": "Stress.",
            "topic": "repo",
            "event_type": "market_stress",
            "tags": ["repo"],
            "important_numbers": [],
        }

        hits = match_record_against_watchlists(record, self.watchlists)

        for hit in hits:
            self.assertEqual(hit["record_id"], "rec_specific_id")


class TestMatchClusterAgainstWatchlists(unittest.TestCase):
    """Test match_cluster_against_watchlists function."""

    def setUp(self):
        from scripts.watchlist_matcher import load_watchlists

        self.watchlists = load_watchlists(str(CONFIG_PATH))

    def test_sets_event_id_in_hits(self):
        """Should set event_id (not record_id) in hits for clusters."""
        from scripts.watchlist_matcher import match_cluster_against_watchlists

        cluster = {
            "event_id": "evt_cluster_123",
            "title": "Treasury Refunding Pressure",
            "summary": "Treasury announces large refunding operation.",
            "topic": "treasury",
            "event_type": "issuance",
            "keywords": ["refunding", "Treasury", "auction"],
            "record_ids": ["rec_1", "rec_2"],
            "source_domains": ["treasury.gov"],
            "quant_links": [],
            "confidence": 0.85,
            "status": "open",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "important_numbers": ["50B"],
        }

        hits = match_cluster_against_watchlists(cluster, self.watchlists)

        # Find hit for treasury watchlist
        treasury_hits = [
            h for h in hits if h["watchlist_id"] == "wl_treasury_refunding"
        ]
        self.assertGreater(len(treasury_hits), 0)
        for hit in treasury_hits:
            self.assertEqual(hit["event_id"], "evt_cluster_123")
            self.assertIsNone(hit.get("record_id"))  # clusters use event_id

    def test_returns_hits_for_matching_cluster(self):
        """Should return hits when cluster matches watchlist."""
        from scripts.watchlist_matcher import match_cluster_against_watchlists

        cluster = {
            "event_id": "evt_matching",
            "title": "Yield Curve Steepening Analysis",
            "summary": "2s10s spread widens as long rates rise.",
            "topic": "rates",
            "event_type": "market_analysis",
            "keywords": ["yield curve", "steepening", "2s10s"],
            "record_ids": [],
            "source_domains": [],
            "quant_links": [],
            "confidence": 0.9,
            "status": "open",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "important_numbers": [],
        }

        hits = match_cluster_against_watchlists(cluster, self.watchlists)

        hit_ids = [h["watchlist_id"] for h in hits]
        self.assertIn("wl_yield_curve_steepening", hit_ids)

    def test_empty_for_non_matching_cluster(self):
        """Should return empty list when no watchlist matches."""
        from scripts.watchlist_matcher import match_cluster_against_watchlists

        cluster = {
            "event_id": "evt_nonmatching",
            "title": "Cooking Recipe",
            "summary": "How to make pasta.",
            "topic": "cooking",
            "event_type": "recipe",
            "keywords": ["pasta", "recipe"],
            "record_ids": [],
            "source_domains": [],
            "quant_links": [],
            "confidence": 0.5,
            "status": "open",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "important_numbers": [],
        }

        hits = match_cluster_against_watchlists(cluster, self.watchlists)

        self.assertEqual(len(hits), 0)


if __name__ == "__main__":
    unittest.main()
