"""
Comprehensive tests for Article-Quant Linking feature.

Tests the bidirectional linking functionality including:
- Time proximity scoring
- Topic overlap scoring
- Compatibility bonus calculations
- Link score computations
- find_related_quant_records and find_related_article_records
- load_accepted_records and link_all_records
"""

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from scripts import find_related_quant_records
from scripts import find_related_article_records
from scripts import link_article_and_quant_records


class TimeProximityScoreTests(unittest.TestCase):
    """Tests for compute_time_proximity_score function."""

    def test_same_day_returns_100(self):
        """Same calendar day should return score of 100."""
        score = find_related_quant_records.compute_time_proximity_score(
            "2026-03-18", "2026-03-18"
        )
        self.assertEqual(score, 100.0)

    def test_one_business_day_apart_returns_80(self):
        """Dates 1 business day apart should return score of 80."""
        # Monday to Tuesday
        score = find_related_quant_records.compute_time_proximity_score(
            "2026-03-16", "2026-03-17"
        )
        self.assertEqual(score, 80.0)

    def test_three_business_days_apart_returns_60(self):
        """Dates 3 business days apart should return score of 60."""
        # Monday to Thursday
        score = find_related_quant_records.compute_time_proximity_score(
            "2026-03-16", "2026-03-19"
        )
        self.assertEqual(score, 60.0)

    def test_seven_business_days_apart_returns_40(self):
        """Dates 7 business days apart should return score of 40."""
        # Monday to Tuesday next week (5 business days gap)
        score = find_related_quant_records.compute_time_proximity_score(
            "2026-03-16", "2026-03-23"
        )
        self.assertEqual(score, 40.0)

    def test_thirty_business_days_apart_returns_20(self):
        """Dates ~30 business days apart should return score of 20."""
        # Roughly 1 month apart
        score = find_related_quant_records.compute_time_proximity_score(
            "2026-03-02", "2026-04-01"
        )
        self.assertEqual(score, 20.0)

    def test_beyond_30_business_days_returns_0(self):
        """Dates more than 30 business days apart should return 0."""
        # ~45 days apart
        score = find_related_quant_records.compute_time_proximity_score(
            "2026-01-01", "2026-03-02"
        )
        self.assertEqual(score, 0.0)

    def test_friday_to_monday_is_one_business_day(self):
        """Friday to Monday should count as 1 business day (score 80)."""
        # Friday March 13 to Monday March 16
        score = find_related_quant_records.compute_time_proximity_score(
            "2026-03-13", "2026-03-16"
        )
        self.assertEqual(score, 80.0)

    def test_weekend_days_not_counted_as_business_days(self):
        """Weekend days should not count toward business day calculation."""
        # Saturday March 14 to Tuesday March 17 - only 1 business day (Mon->Tue)
        score = find_related_quant_records.compute_time_proximity_score(
            "2026-03-14", "2026-03-17"
        )
        self.assertEqual(score, 80.0)

    def test_missing_date_returns_0(self):
        """Missing/empty date should return 0."""
        score = find_related_quant_records.compute_time_proximity_score(
            "2026-03-18", ""
        )
        self.assertEqual(score, 0.0)

    def test_invalid_date_returns_0(self):
        """Invalid date string should return 0."""
        score = find_related_quant_records.compute_time_proximity_score(
            "2026-03-18", "not-a-date"
        )
        self.assertEqual(score, 0.0)

    def test_none_date_returns_0(self):
        """None date should return 0."""
        score = find_related_quant_records.compute_time_proximity_score(
            "2026-03-18", None
        )
        self.assertEqual(score, 0.0)

    def test_iso_8601_format_parsed_correctly(self):
        """ISO 8601 format (YYYY-MM-DD) should be parsed correctly."""
        score = find_related_quant_records.compute_time_proximity_score(
            "2026-03-18T14:30:00", "2026-03-18"
        )
        self.assertEqual(score, 100.0)

    def test_mm_dd_yyyy_format_parsed_correctly(self):
        """MM/DD/YYYY format should be parsed correctly."""
        score = find_related_quant_records.compute_time_proximity_score(
            "03/18/2026", "03/18/2026"
        )
        self.assertEqual(score, 100.0)

    def test_yyyy_mm_dd_format_parsed_correctly(self):
        """YYYY_MM_DD format should be parsed correctly."""
        score = find_related_quant_records.compute_time_proximity_score(
            "2026_03_18", "2026_03_18"
        )
        self.assertEqual(score, 100.0)

    def test_date_with_additional_content_parsed(self):
        """Date with additional content like '3/18/2026 (FOMC statement)' should parse."""
        score = find_related_quant_records.compute_time_proximity_score(
            "3/18/2026 (FOMC statement)", "2026-03-18"
        )
        self.assertEqual(score, 100.0)


class IsQuantRecordTests(unittest.TestCase):
    """Tests for is_quant_record function."""

    def test_quant_snapshot_source_type_is_quant(self):
        """Record with source_type='quant_snapshot' should be identified as quant."""
        record = {
            "id": "sofr_2026_03_18",
            "source": {"source_type": "quant_snapshot", "published_at": "2026-03-18"},
        }
        self.assertTrue(find_related_quant_records.is_quant_record(record))

    def test_dataset_snapshot_source_type_is_quant(self):
        """Record with source_type='dataset_snapshot' should be identified as quant."""
        record = {
            "id": "treasury_auctions_2026_03_18",
            "source": {"source_type": "dataset_snapshot", "published_at": "2026-03-18"},
        }
        self.assertTrue(find_related_quant_records.is_quant_record(record))

    def test_sofr_prefix_id_is_quant(self):
        """Record with id starting with 'sofr_' should be identified as quant."""
        record = {
            "id": "sofr_2026_03_18",
            "source": {"source_type": "article", "published_at": "2026-03-18"},
        }
        self.assertTrue(find_related_quant_records.is_quant_record(record))

    def test_fed_funds_prefix_id_is_quant(self):
        """Record with id starting with 'fed_funds_' should be identified as quant."""
        record = {
            "id": "fed_funds_2026_03_18",
            "source": {"source_type": "article", "published_at": "2026-03-18"},
        }
        self.assertTrue(find_related_quant_records.is_quant_record(record))

    def test_regular_article_is_not_quant(self):
        """Record with article source_type and regular id should not be quant."""
        record = {
            "id": "fomc_statement_2026_03_18",
            "source": {"source_type": "press_release", "published_at": "2026-03-18"},
        }
        self.assertFalse(find_related_quant_records.is_quant_record(record))

    def test_empty_record_is_not_quant(self):
        """Empty record should not be identified as quant."""
        self.assertFalse(find_related_quant_records.is_quant_record({}))


class TopicOverlapScoreTests(unittest.TestCase):
    """Tests for compute_topic_overlap_score function."""

    def test_exact_topic_match_returns_50_plus_topic(self):
        """Exact topic match should return score of 50 and shared topic."""
        record1 = {"topic": "macro catalysts", "tags": []}
        record2 = {"topic": "macro catalysts", "tags": []}

        score, shared = find_related_quant_records.compute_topic_overlap_score(
            record1, record2
        )

        self.assertEqual(score, 50.0)
        self.assertIn("macro catalysts", shared)

    def test_no_topic_match_no_tags_returns_0(self):
        """No topic match and no tags should return 0."""
        record1 = {"topic": "macro catalysts", "tags": []}
        record2 = {"topic": "market structure", "tags": []}

        score, shared = find_related_quant_records.compute_topic_overlap_score(
            record1, record2
        )

        self.assertEqual(score, 0.0)
        self.assertEqual(shared, [])

    def test_partial_tag_overlap_returns_proportional_score(self):
        """Partial tag overlap should return proportional score (up to 30)."""
        record1 = {"topic": "", "tags": ["monetary_policy", "fomc", "federal_reserve"]}
        record2 = {"topic": "", "tags": ["monetary_policy", "fomc"]}

        score, shared = find_related_quant_records.compute_topic_overlap_score(
            record1, record2
        )

        # 2 shared tags / 3 max tags * 30 = 20
        self.assertEqual(score, 20.0)
        self.assertIn("monetary_policy", shared)
        self.assertIn("fomc", shared)

    def test_one_record_has_no_tags_returns_0_for_tag_component(self):
        """When one record has no tags, tag component should be 0."""
        record1 = {"topic": "", "tags": ["monetary_policy"]}
        record2 = {"topic": "", "tags": []}

        score, shared = find_related_quant_records.compute_topic_overlap_score(
            record1, record2
        )

        self.assertEqual(score, 0.0)

    def test_both_topic_and_tags_match_returns_combined_score(self):
        """Both topic match and tag overlap should combine scores."""
        record1 = {"topic": "macro catalysts", "tags": ["fomc", "rates"]}
        record2 = {"topic": "macro catalysts", "tags": ["fomc", "treasury"]}

        score, shared = find_related_quant_records.compute_topic_overlap_score(
            record1, record2
        )

        # 50 (topic) + 15 (1 shared tag / 2 max * 30) = 65
        self.assertEqual(score, 65.0)
        self.assertIn("macro catalysts", shared)
        self.assertIn("fomc", shared)

    def test_topic_match_is_case_insensitive(self):
        """Topic matching should be case insensitive."""
        record1 = {"topic": "MACRO CATALYSTS", "tags": []}
        record2 = {"topic": "macro catalysts", "tags": []}

        score, shared = find_related_quant_records.compute_topic_overlap_score(
            record1, record2
        )

        self.assertEqual(score, 50.0)

    def test_empty_topics_return_0(self):
        """Empty topic strings should return 0 for topic component."""
        record1 = {"topic": "", "tags": []}
        record2 = {"topic": "", "tags": []}

        score, shared = find_related_quant_records.compute_topic_overlap_score(
            record1, record2
        )

        self.assertEqual(score, 0.0)


class CompatibilityBonusTests(unittest.TestCase):
    """Tests for compute_compatibility_bonus function."""

    def test_monetary_policy_article_plus_rates_quant_returns_15(self):
        """Monetary policy article + rates quant should return +15."""
        article = {
            "event_type": "policy_statement",
            "topic": "monetary policy",
            "tags": ["fomc"],
        }
        quant = {
            "id": "fed_funds_2026_03_18",
            "topic": "rates",
            "tags": [],
        }

        bonus = find_related_quant_records.compute_compatibility_bonus(article, quant)
        self.assertEqual(bonus, 15.0)

    def test_treasury_article_plus_auction_quant_returns_15(self):
        """Treasury article + auction quant should return +15."""
        article = {
            "event_type": "",
            "topic": "treasury issuance",
            "tags": ["treasury"],
        }
        quant = {
            "id": "treasury_auctions_2026_03_18",
            "topic": "",
            "tags": [],
        }

        bonus = find_related_quant_records.compute_compatibility_bonus(article, quant)
        self.assertEqual(bonus, 15.0)

    def test_market_structure_article_plus_sofr_quant_returns_15(self):
        """Market structure article + SOFR/repo quant should return +15."""
        article = {
            "event_type": "",
            "topic": "market structure",
            "tags": [],
        }
        quant = {
            "id": "sofr_2026_03_18",
            "topic": "",
            "tags": ["sofr"],
        }

        bonus = find_related_quant_records.compute_compatibility_bonus(article, quant)
        self.assertEqual(bonus, 15.0)

    def test_no_compatibility_returns_0(self):
        """Unrelated article and quant should return 0."""
        article = {
            "event_type": "",
            "topic": "employment",
            "tags": ["jobs"],
        }
        quant = {
            "id": "sofr_2026_03_18",
            "topic": "sofr",
            "tags": [],
        }

        bonus = find_related_quant_records.compute_compatibility_bonus(article, quant)
        self.assertEqual(bonus, 0.0)

    def test_multiple_compatibilities_can_stack(self):
        """Multiple compatible pairs can stack bonuses (tested individually)."""
        # This tests that each compatibility type can independently add 15
        article1 = {
            "event_type": "policy_statement",
            "topic": "monetary policy",
            "tags": [],
        }
        quant1 = {
            "id": "fed_funds_2026_03_18",
            "topic": "rates",
            "tags": [],
        }
        bonus1 = find_related_quant_records.compute_compatibility_bonus(
            article1, quant1
        )
        self.assertEqual(bonus1, 15.0)

        article2 = {
            "event_type": "",
            "topic": "treasury",
            "tags": [],
        }
        quant2 = {
            "id": "treasury_auctions_2026_03_18",
            "topic": "",
            "tags": [],
        }
        bonus2 = find_related_quant_records.compute_compatibility_bonus(
            article2, quant2
        )
        self.assertEqual(bonus2, 15.0)


class ComputeLinkScoreTests(unittest.TestCase):
    """Tests for compute_link_score function."""

    def test_combined_score_calculation_is_correct(self):
        """Combined score should sum time + topic + compatibility."""
        article = {
            "source": {"published_at": "2026-03-18"},
            "topic": "macro catalysts",
            "event_type": "policy_statement",
            "tags": ["fomc"],
        }
        quant = {
            "source": {"published_at": "2026-03-18"},
            "id": "fed_funds_2026_03_18",
            "topic": "macro catalysts",
            "tags": [],
        }

        score, shared, reason = find_related_quant_records.compute_link_score(
            article, quant
        )

        # Same day (100) + exact topic match (50) + monetary/rates bonus (15) = 165
        self.assertEqual(score, 165.0)
        self.assertIn("macro catalysts", shared)
        self.assertIn("same-day", reason)
        self.assertIn("exact topic match", reason)

    def test_reason_string_is_descriptive(self):
        """Reason string should describe the match quality."""
        article = {
            "source": {"published_at": "2026-03-18"},
            "topic": "macro catalysts",
            "event_type": "",
            "tags": [],
        }
        quant = {
            "source": {"published_at": "2026-03-25"},
            "id": "fed_funds_2026_03_25",
            "topic": "macro catalysts",
            "tags": [],
        }

        _, _, reason = find_related_quant_records.compute_link_score(article, quant)

        # ~5 business days apart = 40, plus exact topic = 90
        self.assertIn("within a week", reason)
        self.assertIn("exact topic match", reason)

    def test_empty_article_record_handled_gracefully(self):
        """Empty article record should return 0 score."""
        score, shared, reason = find_related_quant_records.compute_link_score(
            {}, {"source": {"published_at": "2026-03-18"}}
        )
        self.assertEqual(score, 0.0)

    def test_empty_quant_record_handled_gracefully(self):
        """Empty quant record should return 0 score."""
        score, shared, reason = find_related_quant_records.compute_link_score(
            {"source": {"published_at": "2026-03-18"}}, {}
        )
        self.assertEqual(score, 0.0)


class FindRelatedQuantRecordsTests(unittest.TestCase):
    """Tests for find_related_quant_records function."""

    def setUp(self):
        """Set up test data."""
        self.article = {
            "id": "fomc_statement_2026_03_18",
            "source": {"published_at": "2026-03-18", "source_type": "press_release"},
            "topic": "macro catalysts",
            "event_type": "policy_statement",
            "tags": ["fomc"],
        }

        self.quant_records = [
            {
                "id": "fed_funds_2026_03_18",
                "source": {
                    "published_at": "2026-03-18",
                    "source_type": "quant_snapshot",
                },
                "topic": "macro catalysts",
                "tags": [],
            },
            {
                "id": "sofr_2026_03_18",
                "source": {
                    "published_at": "2026-03-18",
                    "source_type": "quant_snapshot",
                },
                "topic": "liquidity",
                "tags": ["sofr"],
            },
            {
                "id": "treasury_2026_03_18",
                "source": {
                    "published_at": "2026-04-01",
                    "source_type": "quant_snapshot",
                },
                "topic": "treasury",
                "tags": [],
            },
        ]

    def test_returns_top_n_results_sorted_by_score(self):
        """Should return top N results sorted by score descending."""
        results = find_related_quant_records.find_related_quant_records(
            self.article, self.quant_records, top_n=2
        )

        self.assertEqual(len(results), 2)
        self.assertGreaterEqual(results[0]["link_score"], results[1]["link_score"])

    def test_filters_by_min_score_threshold(self):
        """Should filter out results below min_score threshold."""
        # fed_funds: 100 time + 50 topic + 15 compat = 165
        # sofr: 100 time + 0 topic + 0 compat = 100
        # Using min_score=150 to filter out sofr
        results = find_related_quant_records.find_related_quant_records(
            self.article, self.quant_records, top_n=3, min_score=150.0
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["record_id"], "fed_funds_2026_03_18")

    def test_returns_empty_list_for_no_matches(self):
        """Should return empty list when no quants match criteria."""
        # Treasury is 10+ business days away, should score 0
        treasury_quant = [
            {
                "id": "treasury_2026_05_01",
                "source": {
                    "published_at": "2026-05-01",
                    "source_type": "quant_snapshot",
                },
                "topic": "employment",
                "tags": [],
            }
        ]

        results = find_related_quant_records.find_related_quant_records(
            self.article, treasury_quant, top_n=3, min_score=50.0
        )

        self.assertEqual(results, [])

    def test_returns_empty_list_for_empty_inputs(self):
        """Should return empty list for empty article or quant list."""
        self.assertEqual(
            find_related_quant_records.find_related_quant_records(
                self.article, [], top_n=3
            ),
            [],
        )
        self.assertEqual(
            find_related_quant_records.find_related_quant_records(
                {}, self.quant_records, top_n=3
            ),
            [],
        )

    def test_link_entry_format_is_correct(self):
        """Link entry should have correct keys and types."""
        results = find_related_quant_records.find_related_quant_records(
            self.article, self.quant_records, top_n=1
        )

        self.assertEqual(len(results), 1)
        link = results[0]

        self.assertIn("record_id", link)
        self.assertIn("relationship", link)
        self.assertIn("reason", link)
        self.assertIn("topic_overlap", link)
        self.assertIn("link_score", link)

        self.assertIsInstance(link["record_id"], str)
        self.assertIsInstance(link["relationship"], str)
        self.assertIsInstance(link["reason"], str)
        self.assertIsInstance(link["topic_overlap"], list)
        self.assertIsInstance(link["link_score"], float)

    def test_relationship_is_nearest_when_time_score_80_or_higher(self):
        """Relationship should be 'nearest_relevant_quant_snapshot' when time >= 80."""
        results = find_related_quant_records.find_related_quant_records(
            self.article, self.quant_records, top_n=1
        )

        # fed_funds is same day (100 time score)
        self.assertEqual(results[0]["relationship"], "nearest_relevant_quant_snapshot")

    def test_relationship_is_temporal_when_time_score_below_80(self):
        """Relationship should be 'temporally_proximate_quant' when time < 80."""
        far_quant = [
            {
                "id": "treasury_2026_03_25",
                "source": {
                    "published_at": "2026-03-25",
                    "source_type": "quant_snapshot",
                },
                "topic": "treasury",
                "tags": [],
            }
        ]

        results = find_related_quant_records.find_related_quant_records(
            self.article, far_quant, top_n=1, min_score=0.0
        )

        # March 18 to March 25 is ~5 business days = 40 time score
        self.assertEqual(results[0]["relationship"], "temporally_proximate_quant")


class FindRelatedArticleRecordsTests(unittest.TestCase):
    """Tests for find_related_article_records function."""

    def setUp(self):
        """Set up test data."""
        self.quant = {
            "id": "fed_funds_2026_03_18",
            "source": {"published_at": "2026-03-18", "source_type": "quant_snapshot"},
            "topic": "macro catalysts",
            "tags": [],
        }

        self.article_records = [
            {
                "id": "fomc_statement_2026_03_18",
                "source": {
                    "published_at": "2026-03-18",
                    "source_type": "press_release",
                },
                "topic": "macro catalysts",
                "event_type": "policy_statement",
                "tags": ["fomc"],
            },
            {
                "id": "fed_speech_2026_03_18",
                "source": {"published_at": "2026-03-18", "source_type": "speech"},
                "topic": "liquidity",
                "event_type": "",
                "tags": ["sofr"],
            },
            {
                "id": "employment_report_2026_04_01",
                "source": {"published_at": "2026-04-01", "source_type": "article"},
                "topic": "employment",
                "event_type": "",
                "tags": [],
            },
        ]

    def test_returns_top_n_results_sorted_by_score(self):
        """Should return top N results sorted by score descending."""
        results = find_related_article_records.find_related_article_records(
            self.quant, self.article_records, top_n=2
        )

        self.assertEqual(len(results), 2)
        self.assertGreaterEqual(results[0]["link_score"], results[1]["link_score"])

    def test_filters_by_min_score_threshold(self):
        """Should filter out results below min_score threshold."""
        # fomc_statement: 100 time + 50 topic + 15 compat = 165
        # fed_speech: 100 time + 0 topic + 0 compat = 100
        # Using min_score=150 to filter out fed_speech
        results = find_related_article_records.find_related_article_records(
            self.quant, self.article_records, top_n=3, min_score=150.0
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["record_id"], "fomc_statement_2026_03_18")

    def test_returns_empty_list_for_no_matches(self):
        """Should return empty list when no articles match criteria."""
        distant_quant = [
            {
                "id": "treasury_2026_06_01",
                "source": {
                    "published_at": "2026-06-01",
                    "source_type": "quant_snapshot",
                },
                "topic": "treasury",
                "tags": [],
            }
        ]

        results = find_related_article_records.find_related_article_records(
            distant_quant[0], self.article_records, top_n=3, min_score=50.0
        )

        self.assertEqual(results, [])

    def test_returns_empty_list_for_empty_inputs(self):
        """Should return empty list for empty quant or article list."""
        self.assertEqual(
            find_related_article_records.find_related_article_records(
                self.quant, [], top_n=3
            ),
            [],
        )
        self.assertEqual(
            find_related_article_records.find_related_article_records(
                {}, self.article_records, top_n=3
            ),
            [],
        )

    def test_link_entry_format_is_correct(self):
        """Link entry should have correct keys and types."""
        results = find_related_article_records.find_related_article_records(
            self.quant, self.article_records, top_n=1
        )

        self.assertEqual(len(results), 1)
        link = results[0]

        self.assertIn("record_id", link)
        self.assertIn("relationship", link)
        self.assertIn("reason", link)
        self.assertIn("topic_overlap", link)
        self.assertIn("link_score", link)

    def test_relationship_types_are_correct_for_quant_to_article(self):
        """Quant→article relationships should have narrative context types."""
        results = find_related_article_records.find_related_article_records(
            self.quant, self.article_records, top_n=3
        )

        # Same-day articles should have narrative_context_for_quant_snapshot
        for link in results:
            if link["link_score"] >= 80:
                self.assertEqual(
                    link["relationship"], "narrative_context_for_quant_snapshot"
                )


class LoadAcceptedRecordsTests(unittest.TestCase):
    """Tests for load_accepted_records function."""

    def test_separates_articles_from_quants_correctly(self):
        """Should correctly separate articles from quant records."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            accepted_dir = base_dir / "data" / "accepted"
            accepted_dir.mkdir(parents=True)

            # Create article record
            article = {
                "id": "article_001",
                "source": {
                    "source_type": "press_release",
                    "published_at": "2026-03-18",
                },
                "topic": "test",
            }
            (accepted_dir / "article_001.json").write_text(
                json.dumps(article), encoding="utf-8"
            )

            # Create quant record
            quant = {
                "id": "sofr_2026_03_18",
                "source": {
                    "source_type": "quant_snapshot",
                    "published_at": "2026-03-18",
                },
                "topic": "test",
            }
            (accepted_dir / "sofr_2026_03_18.json").write_text(
                json.dumps(quant), encoding="utf-8"
            )

            articles, quants = link_article_and_quant_records.load_accepted_records(
                base_dir
            )

            self.assertEqual(len(articles), 1)
            self.assertEqual(len(quants), 1)
            self.assertEqual(articles[0]["id"], "article_001")
            self.assertEqual(quants[0]["id"], "sofr_2026_03_18")

    def test_handles_empty_directory(self):
        """Should handle empty accepted directory gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            accepted_dir = base_dir / "data" / "accepted"
            accepted_dir.mkdir(parents=True)

            articles, quants = link_article_and_quant_records.load_accepted_records(
                base_dir
            )

            self.assertEqual(articles, [])
            self.assertEqual(quants, [])

    def test_handles_missing_directory(self):
        """Should handle missing accepted directory gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)

            articles, quants = link_article_and_quant_records.load_accepted_records(
                base_dir
            )

            self.assertEqual(articles, [])
            self.assertEqual(quants, [])


class LinkAllRecordsTests(unittest.TestCase):
    """Tests for link_all_records function."""

    def test_updates_records_with_linked_context(self):
        """Should update records with linked_quant_context and linked_article_context."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            accepted_dir = base_dir / "data" / "accepted"
            accepted_dir.mkdir(parents=True)

            # Create article record
            article = {
                "id": "fomc_2026_03_18",
                "source": {
                    "source_type": "press_release",
                    "published_at": "2026-03-18",
                },
                "topic": "macro catalysts",
                "event_type": "policy_statement",
                "tags": ["fomc"],
            }
            (accepted_dir / "fomc_2026_03_18.json").write_text(
                json.dumps(article), encoding="utf-8"
            )

            # Create quant record
            quant = {
                "id": "fed_funds_2026_03_18",
                "source": {
                    "source_type": "quant_snapshot",
                    "published_at": "2026-03-18",
                },
                "topic": "macro catalysts",
                "tags": [],
            }
            (accepted_dir / "fed_funds_2026_03_18.json").write_text(
                json.dumps(quant), encoding="utf-8"
            )

            # Load and link
            articles, quants = link_article_and_quant_records.load_accepted_records(
                base_dir
            )
            stats = link_article_and_quant_records.link_all_records(
                articles, quants, base_dir
            )

            # Verify stats
            self.assertEqual(stats["total_articles"], 1)
            self.assertEqual(stats["total_quants"], 1)
            self.assertEqual(stats["articles_with_links"], 1)
            self.assertEqual(stats["quants_with_links"], 1)

            # Verify article has linked_quant_context
            updated_article = json.loads(
                (accepted_dir / "fomc_2026_03_18.json").read_text(encoding="utf-8")
            )
            self.assertIn("linked_quant_context", updated_article)
            self.assertEqual(len(updated_article["linked_quant_context"]), 1)
            self.assertEqual(
                updated_article["linked_quant_context"][0]["record_id"],
                "fed_funds_2026_03_18",
            )

            # Verify quant has linked_article_context
            updated_quant = json.loads(
                (accepted_dir / "fed_funds_2026_03_18.json").read_text(encoding="utf-8")
            )
            self.assertIn("linked_article_context", updated_quant)
            self.assertEqual(len(updated_quant["linked_article_context"]), 1)
            self.assertEqual(
                updated_quant["linked_article_context"][0]["record_id"],
                "fomc_2026_03_18",
            )

    def test_does_not_duplicate_existing_links(self):
        """Should not duplicate existing links when re-running linking."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            accepted_dir = base_dir / "data" / "accepted"
            accepted_dir.mkdir(parents=True)

            # Create article with existing link
            article = {
                "id": "fomc_2026_03_18",
                "source": {
                    "source_type": "press_release",
                    "published_at": "2026-03-18",
                },
                "topic": "macro catalysts",
                "event_type": "policy_statement",
                "tags": ["fomc"],
                "linked_quant_context": [
                    {
                        "record_id": "fed_funds_2026_03_18",
                        "relationship": "nearest_relevant_quant_snapshot",
                        "reason": "same-day",
                        "topic_overlap": ["macro catalysts"],
                        "link_score": 165.0,
                    }
                ],
            }
            (accepted_dir / "fomc_2026_03_18.json").write_text(
                json.dumps(article), encoding="utf-8"
            )

            # Create quant record
            quant = {
                "id": "fed_funds_2026_03_18",
                "source": {
                    "source_type": "quant_snapshot",
                    "published_at": "2026-03-18",
                },
                "topic": "macro catalysts",
                "tags": [],
            }
            (accepted_dir / "fed_funds_2026_03_18.json").write_text(
                json.dumps(quant), encoding="utf-8"
            )

            # Load and link (run twice)
            articles, quants = link_article_and_quant_records.load_accepted_records(
                base_dir
            )
            link_article_and_quant_records.link_all_records(articles, quants, base_dir)

            # Reload and link again
            articles, quants = link_article_and_quant_records.load_accepted_records(
                base_dir
            )
            link_article_and_quant_records.link_all_records(articles, quants, base_dir)

            # Verify no duplicate links
            updated_article = json.loads(
                (accepted_dir / "fomc_2026_03_18.json").read_text(encoding="utf-8")
            )
            self.assertEqual(len(updated_article["linked_quant_context"]), 1)

    def test_returns_correct_stats(self):
        """Should return accurate statistics about linking."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            accepted_dir = base_dir / "data" / "accepted"
            accepted_dir.mkdir(parents=True)

            # Create 2 articles
            for i in range(2):
                article = {
                    "id": f"article_{i}",
                    "source": {
                        "source_type": "article",
                        "published_at": "2026-03-18",
                    },
                    "topic": "macro catalysts",
                    "event_type": "policy_statement",
                    "tags": ["fomc"],
                }
                (accepted_dir / f"article_{i}.json").write_text(
                    json.dumps(article), encoding="utf-8"
                )

            # Create 2 quant records
            for i in range(2):
                quant = {
                    "id": f"quant_{i}",
                    "source": {
                        "source_type": "quant_snapshot",
                        "published_at": "2026-03-18",
                    },
                    "topic": "macro catalysts",
                    "tags": [],
                }
                (accepted_dir / f"quant_{i}.json").write_text(
                    json.dumps(quant), encoding="utf-8"
                )

            articles, quants = link_article_and_quant_records.load_accepted_records(
                base_dir
            )
            stats = link_article_and_quant_records.link_all_records(
                articles, quants, base_dir
            )

            self.assertEqual(stats["total_articles"], 2)
            self.assertEqual(stats["total_quants"], 2)
            self.assertEqual(stats["articles_with_links"], 2)
            self.assertEqual(stats["quants_with_links"], 2)
            # Each article links to 2 quants (but we need to check total)
            self.assertEqual(stats["total_links_added"], 4)

    def test_writes_files_back_to_disk(self):
        """Should write updated records back to disk."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            accepted_dir = base_dir / "data" / "accepted"
            accepted_dir.mkdir(parents=True)

            # Create article
            article = {
                "id": "article_001",
                "source": {
                    "source_type": "article",
                    "published_at": "2026-03-18",
                },
                "topic": "macro catalysts",
                "event_type": "",
                "tags": [],
            }
            (accepted_dir / "article_001.json").write_text(
                json.dumps(article), encoding="utf-8"
            )

            # Create quant
            quant = {
                "id": "quant_001",
                "source": {
                    "source_type": "quant_snapshot",
                    "published_at": "2026-03-18",
                },
                "topic": "macro catalysts",
                "tags": [],
            }
            (accepted_dir / "quant_001.json").write_text(
                json.dumps(quant), encoding="utf-8"
            )

            # Link
            articles, quants = link_article_and_quant_records.load_accepted_records(
                base_dir
            )
            link_article_and_quant_records.link_all_records(articles, quants, base_dir)

            # Verify files were updated
            updated_article = json.loads(
                (accepted_dir / "article_001.json").read_text(encoding="utf-8")
            )
            self.assertIn("linked_quant_context", updated_article)

            updated_quant = json.loads(
                (accepted_dir / "quant_001.json").read_text(encoding="utf-8")
            )
            self.assertIn("linked_article_context", updated_quant)


class EndToEndIntegrationTests(unittest.TestCase):
    """End-to-end integration tests for the linking feature."""

    def test_end_to_end_create_temp_records_run_linking_verify_output(self):
        """Full end-to-end test: create records, run linking, verify output."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            accepted_dir = base_dir / "data" / "accepted"
            accepted_dir.mkdir(parents=True)

            # Create a monetary policy article
            article = {
                "id": "fomc_decision_2026_03_18",
                "source": {
                    "source_type": "press_release",
                    "published_at": "2026-03-18",
                    "name": "Federal Reserve",
                },
                "topic": "macro catalysts",
                "event_type": "policy_statement",
                "tags": ["fomc", "monetary_policy"],
                "summary": "FOMC holds rates steady",
            }
            (accepted_dir / "fomc_decision_2026_03_18.json").write_text(
                json.dumps(article), encoding="utf-8"
            )

            # Create a Treasury auction article
            treasury_article = {
                "id": "treasury_auction_2026_03_18",
                "source": {
                    "source_type": "press_release",
                    "published_at": "2026-03-18",
                    "name": "Treasury Department",
                },
                "topic": "treasury issuance",
                "event_type": "",
                "tags": ["treasury", "treasury_auction"],
                "summary": "Treasury announces auction schedule",
            }
            (accepted_dir / "treasury_auction_2026_03_18.json").write_text(
                json.dumps(treasury_article), encoding="utf-8"
            )

            # Create fed_funds quant (related to FOMC)
            fed_funds_quant = {
                "id": "fed_funds_2026_03_18",
                "source": {
                    "source_type": "quant_snapshot",
                    "published_at": "2026-03-18",
                    "name": "FRED",
                },
                "topic": "rates",
                "tags": ["fed_funds"],
                "summary": "Federal funds rate data",
            }
            (accepted_dir / "fed_funds_2026_03_18.json").write_text(
                json.dumps(fed_funds_quant), encoding="utf-8"
            )

            # Create treasury_auctions quant (related to Treasury)
            treasury_quant = {
                "id": "treasury_auctions_2026_03_18",
                "source": {
                    "source_type": "quant_snapshot",
                    "published_at": "2026-03-18",
                    "name": "Treasury Direct",
                },
                "topic": "auctions",
                "tags": ["treasury_auction"],
                "summary": "Treasury auction results",
            }
            (accepted_dir / "treasury_auctions_2026_03_18.json").write_text(
                json.dumps(treasury_quant), encoding="utf-8"
            )

            # Create SOFR quant (should link to market structure articles if we had any)
            sofr_quant = {
                "id": "sofr_2026_03_18",
                "source": {
                    "source_type": "quant_snapshot",
                    "published_at": "2026-03-18",
                    "name": "FRED",
                },
                "topic": "liquidity",
                "tags": ["sofr"],
                "summary": "SOFR rate data",
            }
            (accepted_dir / "sofr_2026_03_18.json").write_text(
                json.dumps(sofr_quant), encoding="utf-8"
            )

            # Load records
            articles, quants = link_article_and_quant_records.load_accepted_records(
                base_dir
            )

            self.assertEqual(len(articles), 2)
            self.assertEqual(len(quants), 3)

            # Verify is_quant_record classification
            for quant in quants:
                self.assertTrue(link_article_and_quant_records.is_quant_record(quant))
            for art in articles:
                self.assertFalse(link_article_and_quant_records.is_quant_record(art))

            # Run bidirectional linking
            stats = link_article_and_quant_records.link_all_records(
                articles, quants, base_dir
            )

            # Verify stats
            self.assertEqual(stats["total_articles"], 2)
            self.assertEqual(stats["total_quants"], 3)
            self.assertEqual(stats["articles_with_links"], 2)
            self.assertEqual(stats["quants_with_links"], 3)

            # Load and verify updated article
            updated_article = json.loads(
                (accepted_dir / "fomc_decision_2026_03_18.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertIn("linked_quant_context", updated_article)
            self.assertGreater(len(updated_article["linked_quant_context"]), 0)

            # Check that fed_funds is in the linked quants (should have high score)
            linked_ids = [
                link["record_id"] for link in updated_article["linked_quant_context"]
            ]
            self.assertIn("fed_funds_2026_03_18", linked_ids)

            # Load and verify updated quant
            updated_quant = json.loads(
                (accepted_dir / "fed_funds_2026_03_18.json").read_text(encoding="utf-8")
            )
            self.assertIn("linked_article_context", updated_quant)
            linked_article_ids = [
                link["record_id"] for link in updated_quant["linked_article_context"]
            ]
            self.assertIn("fomc_decision_2026_03_18", linked_article_ids)

            # Verify the link scores are reasonable
            for link in updated_article["linked_quant_context"]:
                self.assertGreater(link["link_score"], 0)
                self.assertIn("reason", link)
                self.assertIn("topic_overlap", link)

            # Verify treasury article links to treasury quant
            updated_treasury_article = json.loads(
                (accepted_dir / "treasury_auction_2026_03_18.json").read_text(
                    encoding="utf-8"
                )
            )
            treasury_linked_ids = [
                link["record_id"]
                for link in updated_treasury_article["linked_quant_context"]
            ]
            self.assertIn("treasury_auctions_2026_03_18", treasury_linked_ids)


if __name__ == "__main__":
    unittest.main()
