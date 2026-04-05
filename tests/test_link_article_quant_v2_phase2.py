"""
Tests for V2.7 Part 2: Core Linking Script (Part 1) - link_article_quant.py

Tests the standalone article-quant linking functionality:
- Config loading and JSON helpers
- Record classification (articles vs quants)
- Date parsing and time window scoring
- Topic compatibility scoring with series mapping
- Keyword overlap scoring
- Event alignment scoring
- Combined link scoring
- Relationship classification
- Link ID generation and link creation
- Link persistence (save/load)
- Main orchestration functions
"""

import hashlib
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from scripts import link_article_quant


class ConfigLoadingTests(unittest.TestCase):
    """Tests for config loading functions."""

    def test_load_config_returns_dict(self):
        """load_config should return a dict with expected keys."""
        config = link_article_quant.load_config(
            Path(
                "F:/Personal/RAG/Finance-Research-Archive/config/quant_linking_rules.json"
            )
        )
        self.assertIsInstance(config, dict)
        self.assertIn("scoring_bands", config)
        self.assertIn("topic_to_series", config)
        self.assertIn("keyword_to_series", config)
        self.assertIn("dimension_weights", config)

    def test_load_json_returns_dict(self):
        """load_json should return parsed JSON dict."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"key": "value"}, f)
            temp_path = Path(f.name)

        try:
            result = link_article_quant.load_json(temp_path)
            self.assertEqual(result, {"key": "value"})
        finally:
            temp_path.unlink()

    def test_load_json_returns_default_for_missing_file(self):
        """load_json should return default when file doesn't exist."""
        result = link_article_quant.load_json(Path("nonexistent.json"), default={})
        self.assertEqual(result, {})

    def test_save_json_writes_file(self):
        """save_json should write JSON data to file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.json"
            link_article_quant.save_json(path, {"key": "value"})

            self.assertTrue(path.exists())
            with path.open() as f:
                self.assertEqual(json.load(f), {"key": "value"})


class RecordClassificationTests(unittest.TestCase):
    """Tests for is_quant_record and is_article_record functions."""

    def test_quant_snapshot_is_quant_record(self):
        """Record with source_type='quant_snapshot' should be quant."""
        record = {
            "id": "sofr_2026_03_18",
            "source": {"source_type": "quant_snapshot"},
        }
        self.assertTrue(link_article_quant.is_quant_record(record))
        self.assertFalse(link_article_quant.is_article_record(record))

    def test_dataset_snapshot_is_quant_record(self):
        """Record with source_type='dataset_snapshot' should be quant."""
        record = {
            "id": "treasury_auctions_2026_03_18",
            "source": {"source_type": "dataset_snapshot"},
        }
        self.assertTrue(link_article_quant.is_quant_record(record))
        self.assertFalse(link_article_quant.is_article_record(record))

    def test_sofr_id_prefix_is_quant(self):
        """Record with id starting with 'sofr_' should be quant."""
        record = {
            "id": "sofr_2026_03_18",
            "source": {"source_type": "article"},
        }
        self.assertTrue(link_article_quant.is_quant_record(record))

    def test_fed_funds_id_prefix_is_quant(self):
        """Record with id starting with 'fed_funds_' should be quant."""
        record = {
            "id": "fed_funds_2026_03_18",
            "source": {"source_type": "article"},
        }
        self.assertTrue(link_article_quant.is_quant_record(record))

    def test_regular_article_is_not_quant(self):
        """Regular article record should not be classified as quant."""
        record = {
            "id": "fomc_statement_2026_03_18",
            "source": {"source_type": "press_release"},
        }
        self.assertFalse(link_article_quant.is_quant_record(record))
        self.assertTrue(link_article_quant.is_article_record(record))

    def test_empty_record_is_article(self):
        """Empty record should be classified as article (not quant)."""
        self.assertFalse(link_article_quant.is_quant_record({}))
        self.assertTrue(link_article_quant.is_article_record({}))


class LoadAcceptedRecordsTests(unittest.TestCase):
    """Tests for load_accepted_records function."""

    def test_loads_and_separates_records(self):
        """Should load and separate articles from quants."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            accepted_dir = base_dir / "data" / "accepted"
            accepted_dir.mkdir(parents=True)

            # Create article record
            article = {
                "id": "article_001",
                "source": {"source_type": "article", "published_at": "2026-03-18"},
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
            }
            (accepted_dir / "sofr_2026_03_18.json").write_text(
                json.dumps(quant), encoding="utf-8"
            )

            articles, quants = link_article_quant.load_accepted_records(base_dir)

            self.assertEqual(len(articles), 1)
            self.assertEqual(len(quants), 1)
            self.assertEqual(articles[0]["id"], "article_001")
            self.assertEqual(quants[0]["id"], "sofr_2026_03_18")

    def test_handles_empty_directory(self):
        """Should handle empty accepted directory gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            (base_dir / "data" / "accepted").mkdir(parents=True)

            articles, quants = link_article_quant.load_accepted_records(base_dir)
            self.assertEqual(articles, [])
            self.assertEqual(quants, [])

    def test_handles_missing_directory(self):
        """Should handle missing accepted directory gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)

            articles, quants = link_article_quant.load_accepted_records(base_dir)
            self.assertEqual(articles, [])
            self.assertEqual(quants, [])


class DateParsingTests(unittest.TestCase):
    """Tests for date parsing functions."""

    def test_parse_date_iso_format(self):
        """Should parse ISO format dates."""
        result = link_article_quant.parse_date("2026-03-18")
        self.assertEqual(result, datetime(2026, 3, 18))

    def test_parse_date_iso_with_time(self):
        """Should parse ISO format dates with time."""
        result = link_article_quant.parse_date("2026-03-18T14:30:00")
        self.assertEqual(result, datetime(2026, 3, 18, 14, 30, 0))

    def test_parse_date_mm_dd_yyyy(self):
        """Should parse MM/DD/YYYY format."""
        result = link_article_quant.parse_date("03/18/2026")
        self.assertEqual(result, datetime(2026, 3, 18))

    def test_parse_date_invalid_returns_none(self):
        """Should return None for invalid dates."""
        self.assertIsNone(link_article_quant.parse_date("not-a-date"))
        self.assertIsNone(link_article_quant.parse_date(""))
        self.assertIsNone(link_article_quant.parse_date(None))

    def test_is_business_day(self):
        """Should correctly identify business days."""
        # Monday
        self.assertTrue(link_article_quant.is_business_day(datetime(2026, 3, 16)))
        # Friday
        self.assertTrue(link_article_quant.is_business_day(datetime(2026, 3, 20)))
        # Saturday
        self.assertFalse(link_article_quant.is_business_day(datetime(2026, 3, 21)))
        # Sunday
        self.assertFalse(link_article_quant.is_business_day(datetime(2026, 3, 22)))


class TimeWindowScoreTests(unittest.TestCase):
    """Tests for compute_time_window_score function."""

    def setUp(self):
        """Set up config for time window tests."""
        self.config = {
            "time_window_days": 7,
            "weights": {
                "time_window": 0.30,
                "topic_compatibility": 0.35,
                "keyword_overlap": 0.20,
                "event_alignment": 0.15,
            },
        }

    def test_same_day_returns_max_score(self):
        """Same day dates should return high score."""
        score = link_article_quant.compute_time_window_score(
            "2026-03-18", "2026-03-18", self.config
        )
        self.assertGreaterEqual(score, 80.0)

    def test_within_time_window_returns_positive_score(self):
        """Dates within time_window_days should return positive score."""
        score = link_article_quant.compute_time_window_score(
            "2026-03-18", "2026-03-20", self.config
        )
        self.assertGreater(score, 0.0)

    def test_outside_time_window_returns_zero(self):
        """Dates outside time window should return 0."""
        # 10 days apart, time_window is 7
        score = link_article_quant.compute_time_window_score(
            "2026-03-01", "2026-03-15", self.config
        )
        self.assertEqual(score, 0.0)


class TopicCompatibilityTests(unittest.TestCase):
    """Tests for topic compatibility scoring functions."""

    def setUp(self):
        """Set up config for topic tests."""
        self.config = link_article_quant.load_config(
            Path(
                "F:/Personal/RAG/Finance-Research-Archive/config/quant_linking_rules.json"
            )
        )

    def test_get_topic_series_returns_correct_series(self):
        """topic_to_series mapping should return correct series list."""
        series = link_article_quant.get_topic_series("monetary policy", self.config)
        self.assertIn("fed_funds", series)
        self.assertIn("iorb", series)

    def test_get_topic_series_unknown_topic_returns_empty(self):
        """Unknown topic should return empty list."""
        series = link_article_quant.get_topic_series("unknown topic", self.config)
        self.assertEqual(series, [])

    def test_compute_topic_score_returns_score_and_matched_series(self):
        """compute_topic_score should return (score, matched_series) tuple."""
        article = {"topic": "monetary policy", "tags": []}
        quant = {"id": "fed_funds_2026_03_18", "tags": []}

        score, matched = link_article_quant.compute_topic_score(
            article, quant, self.config
        )

        self.assertIsInstance(score, float)
        self.assertIsInstance(matched, list)
        self.assertGreater(score, 0.0)
        self.assertIn("fed_funds", matched)

    def test_compute_topic_score_no_match_returns_zero(self):
        """No topic match should return 0 score."""
        article = {"topic": "employment", "tags": []}
        quant = {"id": "sofr_2026_03_18", "tags": []}

        score, matched = link_article_quant.compute_topic_score(
            article, quant, self.config
        )

        self.assertEqual(score, 0.0)
        self.assertEqual(matched, [])


class KeywordOverlapTests(unittest.TestCase):
    """Tests for keyword overlap scoring functions."""

    def setUp(self):
        """Set up config for keyword tests."""
        self.config = link_article_quant.load_config(
            Path(
                "F:/Personal/RAG/Finance-Research-Archive/config/quant_linking_rules.json"
            )
        )

    def test_get_keyword_series_returns_correct_series(self):
        """keyword_to_series mapping should return correct series."""
        series = link_article_quant.get_keyword_series(
            ["sofr", "liquidity"], self.config
        )
        self.assertIn("sofr", series)

    def test_get_keyword_series_empty_tags_returns_empty(self):
        """Empty tags should return empty list."""
        series = link_article_quant.get_keyword_series([], self.config)
        self.assertEqual(series, [])

    def test_compute_keyword_overlap_score_returns_score_and_keywords(self):
        """compute_keyword_overlap_score should return (score, matched_keywords)."""
        article = {"topic": "", "tags": ["fomc", "monetary_policy"]}
        quant = {"id": "fed_funds_2026_03_18", "tags": ["fed_funds"]}

        score, matched = link_article_quant.compute_keyword_overlap_score(
            article, quant, self.config
        )

        self.assertIsInstance(score, float)
        self.assertIsInstance(matched, list)
        # Should have some keyword overlap
        self.assertGreaterEqual(score, 0.0)


class EventAlignmentTests(unittest.TestCase):
    """Tests for event alignment scoring functions."""

    def test_load_events_returns_list(self):
        """load_events should return a list of event dicts."""
        events = link_article_quant.load_events(
            Path("F:/Personal/RAG/Finance-Research-Archive/data/events")
        )
        self.assertIsInstance(events, list)

    def test_compute_event_alignment_score_returns_tuple(self):
        """compute_event_alignment_score should return (score, event_id_or_none)."""
        article = {
            "id": "article_001",
            "source": {"published_at": "2026-03-18"},
            "tags": ["fomc"],
        }
        quant = {
            "id": "fed_funds_2026_03_18",
            "source": {"published_at": "2026-03-18"},
            "tags": [],
        }
        events = [
            {
                "event_id": "event_001",
                "created_at": "2026-03-18",
                "keywords": ["fomc", "fed"],
                "record_ids": ["article_001", "quant_001"],
            }
        ]

        score, event_id = link_article_quant.compute_event_alignment_score(
            article, quant, events
        )

        self.assertIsInstance(score, float)
        self.assertGreaterEqual(score, 0.0)
        # If article is in event that has quant too, should return event_id
        # If not, event_id may be None
        self.assertTrue(event_id is None or isinstance(event_id, str))


class CombinedScoringTests(unittest.TestCase):
    """Tests for compute_link_score function."""

    def setUp(self):
        """Set up config for scoring tests."""
        self.config = link_article_quant.load_config(
            Path(
                "F:/Personal/RAG/Finance-Research-Archive/config/quant_linking_rules.json"
            )
        )

    def test_compute_link_score_returns_score_and_dimensions(self):
        """compute_link_score should return (score, matched_dimensions)."""
        article = {
            "id": "fomc_2026_03_18",
            "source": {"published_at": "2026-03-18"},
            "topic": "monetary policy",
            "tags": ["fomc"],
        }
        quant = {
            "id": "fed_funds_2026_03_18",
            "source": {"published_at": "2026-03-18"},
            "topic": "rates",
            "tags": ["fed_funds"],
        }
        events = []

        score, dimensions = link_article_quant.compute_link_score(
            article, quant, events, self.config
        )

        self.assertIsInstance(score, float)
        self.assertIsInstance(dimensions, list)
        self.assertGreater(score, 0.0)

    def test_compute_link_score_combines_all_dimensions(self):
        """compute_link_score should combine time, topic, keyword, and event scores."""
        article = {
            "id": "fomc_2026_03_18",
            "source": {"published_at": "2026-03-18"},
            "topic": "monetary policy",
            "tags": ["fomc"],
        }
        quant = {
            "id": "fed_funds_2026_03_18",
            "source": {"published_at": "2026-03-18"},
            "topic": "rates",
            "tags": ["fed_funds"],
        }
        events = []

        score, dimensions = link_article_quant.compute_link_score(
            article, quant, events, self.config
        )

        # Same day + matching topic/keywords should give high score
        self.assertGreaterEqual(score, 50.0)
        self.assertIn("time_window", dimensions)
        self.assertIn("topic_compatibility", dimensions)


class RelationshipClassificationTests(unittest.TestCase):
    """Tests for classify_relationship function."""

    def setUp(self):
        """Set up config for classification tests."""
        self.config = link_article_quant.load_config(
            Path(
                "F:/Personal/RAG/Finance-Research-Archive/config/quant_linking_rules.json"
            )
        )

    def test_score_80_returns_supports(self):
        """Score >= 80 should return 'supports'."""
        result = link_article_quant.classify_relationship(85, self.config)
        self.assertEqual(result, "supports")

    def test_score_70_returns_context(self):
        """Score 60-79 should return 'context'."""
        result = link_article_quant.classify_relationship(70, self.config)
        self.assertEqual(result, "context")

    def test_score_50_returns_weak_context(self):
        """Score 40-59 should return 'weak_context'."""
        result = link_article_quant.classify_relationship(50, self.config)
        self.assertEqual(result, "weak_context")

    def test_score_below_40_returns_none(self):
        """Score < 40 should return None (no link created)."""
        result = link_article_quant.classify_relationship(35, self.config)
        self.assertIsNone(result)


class LinkIdGenerationTests(unittest.TestCase):
    """Tests for generate_link_id function."""

    def test_generate_link_id_is_deterministic(self):
        """Same article_id and quant_id should produce same link_id."""
        id1 = link_article_quant.generate_link_id("article_001", "quant_001")
        id2 = link_article_quant.generate_link_id("article_001", "quant_001")
        self.assertEqual(id1, id2)

    def test_generate_link_id_different_inputs_different_ids(self):
        """Different inputs should produce different link_ids."""
        id1 = link_article_quant.generate_link_id("article_001", "quant_001")
        id2 = link_article_quant.generate_link_id("article_002", "quant_001")
        self.assertNotEqual(id1, id2)

    def test_generate_link_id_is_string(self):
        """generate_link_id should return a string."""
        result = link_article_quant.generate_link_id("article_001", "quant_001")
        self.assertIsInstance(result, str)


class LinkCreationTests(unittest.TestCase):
    """Tests for create_link function."""

    def test_create_link_returns_valid_dict(self):
        """create_link should return a properly structured link dict."""
        link = link_article_quant.create_link(
            article_id="article_001",
            quant_id="quant_001",
            event_id="event_001",
            relationship="supports",
            score=85.5,
            matched_dimensions=["time_window", "topic_compatibility"],
        )

        self.assertIsInstance(link, dict)
        self.assertIn("link_id", link)
        self.assertIn("article_id", link)
        self.assertIn("quant_id", link)
        self.assertEqual(link["article_id"], "article_001")
        self.assertEqual(link["quant_id"], "quant_001")
        self.assertEqual(link["relationship"], "supports")
        self.assertEqual(link["score"], 85.5)
        self.assertEqual(link["event_id"], "event_001")
        self.assertIn("matched_dimensions", link)
        self.assertIn("created_at", link)

    def test_create_link_without_event_id(self):
        """create_link should handle None event_id."""
        link = link_article_quant.create_link(
            article_id="article_001",
            quant_id="quant_001",
            event_id=None,
            relationship="context",
            score=65.0,
            matched_dimensions=["time_window"],
        )

        self.assertIsNone(link.get("event_id"))


class LinkPersistenceTests(unittest.TestCase):
    """Tests for link persistence functions."""

    def test_save_link_writes_json(self):
        """save_link should write link dict to JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)
            link = {
                "link_id": "test_link_001",
                "article_id": "article_001",
                "quant_id": "quant_001",
                "relationship": "supports",
                "score": 85.0,
                "matched_dimensions": ["time_window"],
                "created_at": "2026-03-18T00:00:00",
            }

            link_article_quant.save_link(link, output_dir)

            expected_path = output_dir / "test_link_001.json"
            self.assertTrue(expected_path.exists())
            with expected_path.open() as f:
                self.assertEqual(json.load(f), link)

    def test_load_links_returns_list(self):
        """load_links should return a list of link dicts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Create some link files
            for i in range(3):
                link = {
                    "link_id": f"link_{i}",
                    "article_id": f"article_{i}",
                    "quant_id": f"quant_{i}",
                    "relationship": "supports",
                    "score": 85.0,
                    "matched_dimensions": [],
                    "created_at": "2026-03-18T00:00:00",
                }
                link_article_quant.save_link(link, output_dir)

            links = link_article_quant.load_links(output_dir)

            self.assertIsInstance(links, list)
            self.assertEqual(len(links), 3)

    def test_load_event_links_returns_dedup_dict(self):
        """load_event_links should return dict keyed by (article_id, quant_id)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            link1 = {
                "link_id": "link_1",
                "article_id": "article_001",
                "quant_id": "quant_001",
                "relationship": "supports",
                "score": 85.0,
                "matched_dimensions": [],
                "created_at": "2026-03-18T00:00:00",
            }
            link_article_quant.save_link(link1, output_dir)

            link2 = {
                "link_id": "link_2",
                "article_id": "article_001",
                "quant_id": "quant_002",
                "relationship": "context",
                "score": 70.0,
                "matched_dimensions": [],
                "created_at": "2026-03-18T00:00:00",
            }
            link_article_quant.save_link(link2, output_dir)

            event_links = link_article_quant.load_event_links(output_dir)

            self.assertIsInstance(event_links, dict)
            self.assertIn(("article_001", "quant_001"), event_links)
            self.assertIn(("article_001", "quant_002"), event_links)


class OrchestrationTests(unittest.TestCase):
    """Tests for main orchestration functions."""

    def setUp(self):
        """Set up config and test data."""
        self.config = link_article_quant.load_config(
            Path(
                "F:/Personal/RAG/Finance-Research-Archive/config/quant_linking_rules.json"
            )
        )

    def test_find_related_quants_for_article_returns_list(self):
        """find_related_quants_for_article should return list of related quants."""
        article = {
            "id": "fomc_2026_03_18",
            "source": {"published_at": "2026-03-18"},
            "topic": "monetary policy",
            "tags": ["fomc"],
        }
        quants = [
            {
                "id": "fed_funds_2026_03_18",
                "source": {"published_at": "2026-03-18"},
                "topic": "rates",
                "tags": ["fed_funds"],
            },
            {
                "id": "sofr_2026_03_18",
                "source": {"published_at": "2026-03-18"},
                "topic": "liquidity",
                "tags": ["sofr"],
            },
        ]
        events = []

        results = link_article_quant.find_related_quants_for_article(
            article, quants, events, self.config, top_n=2
        )

        self.assertIsInstance(results, list)
        self.assertLessEqual(len(results), 2)

    def test_find_related_articles_for_quant_returns_list(self):
        """find_related_articles_for_quant should return list of related articles."""
        quant = {
            "id": "fed_funds_2026_03_18",
            "source": {"published_at": "2026-03-18"},
            "topic": "rates",
            "tags": ["fed_funds"],
        }
        articles = [
            {
                "id": "fomc_2026_03_18",
                "source": {"published_at": "2026-03-18"},
                "topic": "monetary policy",
                "tags": ["fomc"],
            },
            {
                "id": "other_2026_03_18",
                "source": {"published_at": "2026-03-18"},
                "topic": "employment",
                "tags": [],
            },
        ]
        events = []

        results = link_article_quant.find_related_articles_for_quant(
            quant, articles, events, self.config, top_n=2
        )

        self.assertIsInstance(results, list)
        self.assertLessEqual(len(results), 2)

    def test_link_all_records_returns_stats(self):
        """link_all_records should return stats dict."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "article_quant_links"
            output_dir.mkdir(parents=True)

            articles = [
                {
                    "id": "article_001",
                    "source": {"published_at": "2026-03-18"},
                    "topic": "monetary policy",
                    "tags": ["fomc"],
                }
            ]
            quants = [
                {
                    "id": "fed_funds_2026_03_18",
                    "source": {"published_at": "2026-03-18"},
                    "topic": "rates",
                    "tags": ["fed_funds"],
                }
            ]
            events = []

            stats = link_article_quant.link_all_records(
                articles, quants, events, self.config, output_dir
            )

            self.assertIsInstance(stats, dict)
            self.assertIn("total_articles", stats)
            self.assertIn("total_quants", stats)
            self.assertIn("links_created", stats)


class EndToEndTests(unittest.TestCase):
    """End-to-end tests for the linking script."""

    def test_full_linking_pipeline(self):
        """Test complete linking pipeline from articles/quants to saved links."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            output_dir = base_dir / "article_quant_links"
            output_dir.mkdir(parents=True)

            config_path = base_dir / "config" / "quant_linking_rules.json"
            config_dir = base_dir / "config"
            config_dir.mkdir(parents=True)

            # Copy config
            original_config = link_article_quant.load_config(
                Path(
                    "F:/Personal/RAG/Finance-Research-Archive/config/quant_linking_rules.json"
                )
            )
            link_article_quant.save_json(config_path, original_config)

            config = link_article_quant.load_config(config_path)

            articles = [
                {
                    "id": "fomc_2026_03_18",
                    "source": {"published_at": "2026-03-18"},
                    "topic": "monetary policy",
                    "tags": ["fomc"],
                }
            ]
            quants = [
                {
                    "id": "fed_funds_2026_03_18",
                    "source": {"published_at": "2026-03-18"},
                    "topic": "rates",
                    "tags": ["fed_funds"],
                }
            ]
            events = []

            # Run linking
            stats = link_article_quant.link_all_records(
                articles, quants, events, config, output_dir
            )

            # Verify
            self.assertGreater(stats["links_created"], 0)

            # Verify link file was created
            links = link_article_quant.load_links(output_dir)
            self.assertGreater(len(links), 0)


if __name__ == "__main__":
    unittest.main()
