"""
Tests for V2.7 Part 4 Phase 3: Enrichment and Event Cluster Integration

Tests for:
- Record enrichment (quant_context/article_context blocks)
- Event cluster enrichment (article_links field)
- Main orchestration with enrichment
"""

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from scripts import link_article_quant


class TestRecordEnrichment(unittest.TestCase):
    """Tests for record enrichment functions."""

    def test_enrich_accepted_record_adds_quant_context(self):
        """enrich_accepted_record should add quant_context for article records."""
        record = {
            "id": "fomc_statement_2026_04_05",
            "source": {"source_type": "press_release"},
            "topic": "monetary policy",
        }
        related_links = [
            {
                "record_id": "fed_funds_2026_04_05",
                "relationship": "supports",
                "link_score": 85.0,
            },
            {
                "record_id": "2y_yield_2026_04_05",
                "relationship": "context",
                "link_score": 72.0,
            },
        ]

        enriched = link_article_quant.enrich_accepted_record(
            record, related_links, link_type="quant"
        )

        self.assertIn("quant_context", enriched)
        self.assertEqual(
            enriched["quant_context"]["linked_quant_records"],
            ["fed_funds_2026_04_05", "2y_yield_2026_04_05"],
        )
        self.assertIn("summary", enriched["quant_context"])
        self.assertIsInstance(enriched["quant_context"]["summary"], str)

    def test_enrich_accepted_record_adds_article_context(self):
        """enrich_accepted_record should add article_context for quant records."""
        record = {
            "id": "fed_funds_2026_04_05",
            "source": {"source_type": "quant_snapshot"},
            "topic": "rates",
        }
        related_links = [
            {
                "record_id": "fomc_statement_2026_04_05",
                "relationship": "supports",
                "link_score": 85.0,
            },
        ]

        enriched = link_article_quant.enrich_accepted_record(
            record, related_links, link_type="article"
        )

        self.assertIn("article_context", enriched)
        self.assertEqual(
            enriched["article_context"]["linked_article_records"],
            ["fomc_statement_2026_04_05"],
        )
        self.assertIn("summary", enriched["article_context"])

    def test_enrich_accepted_record_preserves_existing_fields(self):
        """enrich_accepted_record should preserve existing record fields."""
        record = {
            "id": "fomc_statement_2026_04_05",
            "source": {"source_type": "press_release"},
            "topic": "monetary policy",
            "title": "FOMC Statement",
            "summary": "Original summary",
        }
        related_links = [
            {
                "record_id": "fed_funds_2026_04_05",
                "relationship": "supports",
                "link_score": 85.0,
            },
        ]

        enriched = link_article_quant.enrich_accepted_record(
            record, related_links, link_type="quant"
        )

        self.assertEqual(enriched["id"], "fomc_statement_2026_04_05")
        self.assertEqual(enriched["title"], "FOMC Statement")
        self.assertEqual(enriched["summary"], "Original summary")
        self.assertIn("quant_context", enriched)

    def test_enrich_accepted_record_empty_links(self):
        """enrich_accepted_record should handle empty links gracefully."""
        record = {"id": "test_article"}
        enriched = link_article_quant.enrich_accepted_record(
            record, [], link_type="quant"
        )

        # Should still return record with empty context
        self.assertIn("quant_context", enriched)
        self.assertEqual(enriched["quant_context"]["linked_quant_records"], [])

    def test_build_enrichment_summary_returns_string(self):
        """build_enrichment_summary should return a string summary."""
        links = [
            {
                "record_id": "fed_funds_2026_04_05",
                "relationship": "supports",
                "link_score": 85.0,
            },
            {
                "record_id": "2y_yield_2026_04_05",
                "relationship": "context",
                "link_score": 72.0,
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            # Create mock quant records
            quant_dir = base_dir / "data" / "accepted"
            quant_dir.mkdir(parents=True)

            # Create fed_funds record
            fed_record = {
                "id": "fed_funds_2026_04_05",
                "title": "Fed Funds Rate Data",
            }
            (quant_dir / "fed_funds_2026_04_05.json").write_text(
                json.dumps(fed_record), encoding="utf-8"
            )

            # Create 2y_yield record
            yield_record = {
                "id": "2y_yield_2026_04_05",
                "title": "2-Year Treasury Yield",
            }
            (quant_dir / "2y_yield_2026_04_05.json").write_text(
                json.dumps(yield_record), encoding="utf-8"
            )

            summary = link_article_quant.build_enrichment_summary(links, base_dir)

            self.assertIsInstance(summary, str)
            self.assertGreater(len(summary), 0)

    def test_write_enriched_record_writes_file(self):
        """write_enriched_record should write enriched record to accepted directory."""
        record = {
            "id": "test_article_2026_04_05",
            "source": {"source_type": "article"},
            "topic": "monetary policy",
            "quant_context": {
                "linked_quant_records": ["fed_funds_2026_04_05"],
                "summary": "Test summary",
            },
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            accepted_dir = base_dir / "data" / "accepted"
            accepted_dir.mkdir(parents=True)

            link_article_quant.write_enriched_record(record, base_dir)

            expected_path = accepted_dir / "test_article_2026_04_05.json"
            self.assertTrue(expected_path.exists())

            with expected_path.open() as f:
                saved = json.load(f)

            self.assertEqual(saved["id"], "test_article_2026_04_05")
            self.assertIn("quant_context", saved)


class TestEventClusterEnrichment(unittest.TestCase):
    """Tests for event cluster enrichment functions."""

    def test_schema_has_article_links_field(self):
        """Schema should have article_links field."""
        schema_path = Path(
            "F:/Personal/RAG/Finance-Research-Archive/schemas/event_cluster.json"
        )

        with schema_path.open() as f:
            schema = json.load(f)

        self.assertIn("properties", schema)
        self.assertIn("article_links", schema["properties"])
        self.assertEqual(schema["properties"]["article_links"]["type"], "array")
        self.assertEqual(
            schema["properties"]["article_links"]["items"]["type"], "string"
        )

    def test_load_event_returns_dict(self):
        """load_event should return dict or None for missing event."""
        with tempfile.TemporaryDirectory() as tmpdir:
            event_dir = Path(tmpdir) / "events"
            event_dir.mkdir(parents=True)

            # Create an event file
            event = {
                "event_id": "event_001",
                "title": "Test Event",
                "topic": "monetary policy",
                "event_type": "fed_speech",
                "summary": "Test summary",
                "status": "open",
                "created_at": "2026-04-05T00:00:00Z",
                "updated_at": "2026-04-05T00:00:00Z",
                "record_ids": [],
                "source_domains": [],
                "keywords": [],
                "quant_links": [],
                "article_links": [],
                "confidence": 0.8,
            }
            (event_dir / "event_001.json").write_text(
                json.dumps(event), encoding="utf-8"
            )

            loaded = link_article_quant.load_event("event_001", event_dir)

            self.assertIsInstance(loaded, dict)
            self.assertEqual(loaded["event_id"], "event_001")

    def test_load_event_returns_none_for_missing(self):
        """load_event should return None for non-existent event."""
        with tempfile.TemporaryDirectory() as tmpdir:
            event_dir = Path(tmpdir) / "events"
            event_dir.mkdir(parents=True)

            result = link_article_quant.load_event("nonexistent", event_dir)

            self.assertIsNone(result)

    def test_enrich_event_cluster_adds_both_link_types(self):
        """enrich_event_cluster should add both article_links and quant_links."""
        event = {
            "event_id": "event_001",
            "title": "Test Event",
            "topic": "monetary policy",
            "event_type": "fed_speech",
            "summary": "Test summary",
            "status": "open",
            "created_at": "2026-04-05T00:00:00Z",
            "updated_at": "2026-04-05T00:00:00Z",
            "record_ids": [],
            "source_domains": [],
            "keywords": [],
            "quant_links": [],
            "article_links": [],
            "confidence": 0.8,
        }

        enriched = link_article_quant.enrich_event_cluster(
            event,
            article_links=["article_001", "article_002"],
            quant_links=["fed_funds_2026_04_05"],
        )

        self.assertIn("article_links", enriched)
        self.assertEqual(enriched["article_links"], ["article_001", "article_002"])
        self.assertIn("quant_links", enriched)
        self.assertEqual(enriched["quant_links"], ["fed_funds_2026_04_05"])

    def test_enrich_event_cluster_preserves_existing_links(self):
        """enrich_event_cluster should preserve and extend existing links."""
        event = {
            "event_id": "event_001",
            "title": "Test Event",
            "topic": "monetary policy",
            "event_type": "fed_speech",
            "summary": "Test summary",
            "status": "open",
            "created_at": "2026-04-05T00:00:00Z",
            "updated_at": "2026-04-05T00:00:00Z",
            "record_ids": [],
            "source_domains": [],
            "keywords": [],
            "quant_links": ["existing_quant_001"],
            "article_links": ["existing_article_001"],
            "confidence": 0.8,
        }

        enriched = link_article_quant.enrich_event_cluster(
            event,
            article_links=["article_002"],
            quant_links=["fed_funds_2026_04_05"],
        )

        # Should have both old and new links
        self.assertIn("existing_article_001", enriched["article_links"])
        self.assertIn("article_002", enriched["article_links"])
        self.assertIn("existing_quant_001", enriched["quant_links"])
        self.assertIn("fed_funds_2026_04_05", enriched["quant_links"])

    def test_save_event_cluster_writes_file(self):
        """save_event_cluster should write event to data/events/{event_id}.json."""
        event = {
            "event_id": "event_001",
            "title": "Test Event",
            "topic": "monetary policy",
            "event_type": "fed_speech",
            "summary": "Test summary",
            "status": "open",
            "created_at": "2026-04-05T00:00:00Z",
            "updated_at": "2026-04-05T00:00:00Z",
            "record_ids": [],
            "source_domains": [],
            "keywords": [],
            "quant_links": [],
            "article_links": [],
            "confidence": 0.8,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            event_dir = Path(tmpdir) / "events"
            event_dir.mkdir(parents=True)

            link_article_quant.save_event_cluster(event, event_dir)

            expected_path = event_dir / "event_001.json"
            self.assertTrue(expected_path.exists())

            with expected_path.open() as f:
                saved = json.load(f)

            self.assertEqual(saved["event_id"], "event_001")
            self.assertEqual(saved["title"], "Test Event")

    def test_enrich_event_clusters_from_links_returns_count(self):
        """enrich_event_clusters_from_links should return count of enriched clusters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            event_dir = Path(tmpdir) / "events"
            event_dir.mkdir(parents=True)

            # Create an event
            event = {
                "event_id": "event_001",
                "title": "Test Event",
                "topic": "monetary policy",
                "event_type": "fed_speech",
                "summary": "Test summary",
                "status": "open",
                "created_at": "2026-04-05T00:00:00Z",
                "updated_at": "2026-04-05T00:00:00Z",
                "record_ids": ["article_001", "quant_001"],
                "source_domains": [],
                "keywords": [],
                "quant_links": [],
                "article_links": [],
                "confidence": 0.8,
            }
            (event_dir / "event_001.json").write_text(
                json.dumps(event), encoding="utf-8"
            )

            links = [
                {
                    "link_id": "link_001",
                    "article_id": "article_001",
                    "quant_id": "quant_001",
                    "event_id": "event_001",
                    "relationship": "supports",
                    "score": 85.0,
                },
                {
                    "link_id": "link_002",
                    "article_id": "article_002",
                    "quant_id": "quant_002",
                    "event_id": None,  # No event
                    "relationship": "context",
                    "score": 70.0,
                },
            ]

            count = link_article_quant.enrich_event_clusters_from_links(
                links, event_dir
            )

            # Should have enriched 1 event cluster
            self.assertEqual(count, 1)

            # Verify the event was enriched
            enriched_event = link_article_quant.load_event("event_001", event_dir)
            self.assertIn("article_001", enriched_event["article_links"])
            self.assertIn("quant_001", enriched_event["quant_links"])


class TestMainOrchestrationWithEnrichment(unittest.TestCase):
    """Tests for main orchestration with enrichment."""

    def test_main_enriches_accepted_records(self):
        """main() should enrich accepted records after linking."""
        import sys

        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)

            # Set up directory structure
            accepted_dir = base_dir / "data" / "accepted"
            accepted_dir.mkdir(parents=True)
            events_dir = base_dir / "data" / "events"
            events_dir.mkdir(parents=True)
            links_dir = base_dir / "data" / "article_quant_links"
            links_dir.mkdir(parents=True)
            config_dir = base_dir / "config"
            config_dir.mkdir(parents=True)

            # Copy config
            original_config = link_article_quant.load_config(
                Path(
                    "F:/Personal/RAG/Finance-Research-Archive/config/quant_linking_rules.json"
                )
            )
            link_article_quant.save_json(
                config_dir / "quant_linking_rules.json", original_config
            )

            # Create test article
            article = {
                "id": "fomc_statement_2026_04_05",
                "source": {
                    "source_type": "press_release",
                    "published_at": "2026-04-05T18:00:00Z",
                },
                "topic": "monetary policy",
                "tags": ["fomc"],
            }
            (accepted_dir / "fomc_statement_2026_04_05.json").write_text(
                json.dumps(article), encoding="utf-8"
            )

            # Create test quant
            quant = {
                "id": "fed_funds_2026_04_05",
                "source": {
                    "source_type": "quant_snapshot",
                    "published_at": "2026-04-05T18:00:00Z",
                },
                "topic": "rates",
                "tags": ["fed_funds"],
            }
            (accepted_dir / "fed_funds_2026_04_05.json").write_text(
                json.dumps(quant), encoding="utf-8"
            )

            # Set up for the test by patching BASE_DIR
            import scripts.link_article_quant as laq

            original_base_dir = laq.BASE_DIR
            original_accepted_dir = laq.ACCEPTED_DIR
            original_output_dir = laq.OUTPUT_DIR
            original_config_path = laq.CONFIG_PATH

            laq.BASE_DIR = base_dir
            laq.ACCEPTED_DIR = accepted_dir
            laq.OUTPUT_DIR = links_dir
            laq.CONFIG_PATH = config_dir / "quant_linking_rules.json"

            # Save original argv and replace with test-specific args
            original_argv = sys.argv

            try:
                # Set test-specific argv
                sys.argv = ["link_article_quant.py"]

                # Run main
                laq.main()

                # Check that article was enriched
                article_path = accepted_dir / "fomc_statement_2026_04_05.json"
                with article_path.open() as f:
                    enriched_article = json.load(f)

                # Article should have quant_context
                self.assertIn("quant_context", enriched_article)

            finally:
                # Restore
                sys.argv = original_argv
                laq.BASE_DIR = original_base_dir
                laq.ACCEPTED_DIR = original_accepted_dir
                laq.OUTPUT_DIR = original_output_dir
                laq.CONFIG_PATH = original_config_path

    def test_main_enriches_event_clusters(self):
        """main() should enrich event clusters when links have event_id."""
        import sys

        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)

            # Set up directory structure
            accepted_dir = base_dir / "data" / "accepted"
            accepted_dir.mkdir(parents=True)
            events_dir = base_dir / "data" / "events"
            events_dir.mkdir(parents=True)
            links_dir = base_dir / "data" / "article_quant_links"
            links_dir.mkdir(parents=True)
            config_dir = base_dir / "config"
            config_dir.mkdir(parents=True)

            # Copy config
            original_config = link_article_quant.load_config(
                Path(
                    "F:/Personal/RAG/Finance-Research-Archive/config/quant_linking_rules.json"
                )
            )
            link_article_quant.save_json(
                config_dir / "quant_linking_rules.json", original_config
            )

            # Create test article
            article = {
                "id": "article_001",
                "source": {
                    "source_type": "press_release",
                    "published_at": "2026-04-05T18:00:00Z",
                },
                "topic": "monetary policy",
                "tags": ["fomc"],
            }
            (accepted_dir / "article_001.json").write_text(
                json.dumps(article), encoding="utf-8"
            )

            # Create test quant
            quant = {
                "id": "quant_001",
                "source": {
                    "source_type": "quant_snapshot",
                    "published_at": "2026-04-05T18:00:00Z",
                },
                "topic": "rates",
                "tags": ["fed_funds"],
            }
            (accepted_dir / "quant_001.json").write_text(
                json.dumps(quant), encoding="utf-8"
            )

            # Create event that both article and quant belong to
            event = {
                "event_id": "event_001",
                "title": "FOMC Meeting",
                "topic": "monetary policy",
                "event_type": "fomc_meeting",
                "summary": "FOMC policy meeting",
                "status": "open",
                "created_at": "2026-04-05T00:00:00Z",
                "updated_at": "2026-04-05T00:00:00Z",
                "record_ids": ["article_001", "quant_001"],
                "source_domains": [],
                "keywords": ["fomc"],
                "quant_links": [],
                "article_links": [],
                "confidence": 0.9,
            }
            (events_dir / "event_001.json").write_text(
                json.dumps(event), encoding="utf-8"
            )

            # Set up for the test by patching
            import scripts.link_article_quant as laq

            original_base_dir = laq.BASE_DIR
            original_accepted_dir = laq.ACCEPTED_DIR
            original_output_dir = laq.OUTPUT_DIR
            original_config_path = laq.CONFIG_PATH

            # Save original argv
            original_argv = sys.argv

            laq.BASE_DIR = base_dir
            laq.ACCEPTED_DIR = accepted_dir
            laq.OUTPUT_DIR = links_dir
            laq.CONFIG_PATH = config_dir / "quant_linking_rules.json"

            try:
                # Set test-specific argv
                sys.argv = ["link_article_quant.py"]

                laq.main()

                # Check that event was enriched
                event_path = events_dir / "event_001.json"
                with event_path.open() as f:
                    enriched_event = json.load(f)

                # Event should have article_links and quant_links
                self.assertIn("article_links", enriched_event)
                self.assertIn("quant_links", enriched_event)

            finally:
                # Restore
                sys.argv = original_argv
                laq.BASE_DIR = original_base_dir
                laq.ACCEPTED_DIR = original_accepted_dir
                laq.OUTPUT_DIR = original_output_dir
                laq.CONFIG_PATH = original_config_path

    def test_main_enriches_event_clusters(self):
        """main() should enrich event clusters when links have event_id."""
        import sys

        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)

            # Set up directory structure
            accepted_dir = base_dir / "data" / "accepted"
            accepted_dir.mkdir(parents=True)
            events_dir = base_dir / "data" / "events"
            events_dir.mkdir(parents=True)
            links_dir = base_dir / "data" / "article_quant_links"
            links_dir.mkdir(parents=True)
            config_dir = base_dir / "config"
            config_dir.mkdir(parents=True)

            # Copy config
            original_config = link_article_quant.load_config(
                Path(
                    "F:/Personal/RAG/Finance-Research-Archive/config/quant_linking_rules.json"
                )
            )
            link_article_quant.save_json(
                config_dir / "quant_linking_rules.json", original_config
            )

            # Create test article
            article = {
                "id": "article_001",
                "source": {
                    "source_type": "press_release",
                    "published_at": "2026-04-05T18:00:00Z",
                },
                "topic": "monetary policy",
                "tags": ["fomc"],
            }
            (accepted_dir / "article_001.json").write_text(
                json.dumps(article), encoding="utf-8"
            )

            # Create test quant
            quant = {
                "id": "quant_001",
                "source": {
                    "source_type": "quant_snapshot",
                    "published_at": "2026-04-05T18:00:00Z",
                },
                "topic": "rates",
                "tags": ["fed_funds"],
            }
            (accepted_dir / "quant_001.json").write_text(
                json.dumps(quant), encoding="utf-8"
            )

            # Create event that both article and quant belong to
            event = {
                "event_id": "event_001",
                "title": "FOMC Meeting",
                "topic": "monetary policy",
                "event_type": "fomc_meeting",
                "summary": "FOMC policy meeting",
                "status": "open",
                "created_at": "2026-04-05T00:00:00Z",
                "updated_at": "2026-04-05T00:00:00Z",
                "record_ids": ["article_001", "quant_001"],
                "source_domains": [],
                "keywords": ["fomc"],
                "quant_links": [],
                "article_links": [],
                "confidence": 0.9,
            }
            (events_dir / "event_001.json").write_text(
                json.dumps(event), encoding="utf-8"
            )

            # Set up for the test by patching
            import scripts.link_article_quant as laq

            original_base_dir = laq.BASE_DIR
            original_accepted_dir = laq.ACCEPTED_DIR
            original_output_dir = laq.OUTPUT_DIR
            original_config_path = laq.CONFIG_PATH

            # Save original argv and replace with test-specific args
            original_argv = sys.argv

            laq.BASE_DIR = base_dir
            laq.ACCEPTED_DIR = accepted_dir
            laq.OUTPUT_DIR = links_dir
            laq.CONFIG_PATH = config_dir / "quant_linking_rules.json"

            try:
                # Set test-specific argv
                sys.argv = ["link_article_quant.py"]

                laq.main()

                # Check that event was enriched
                event_path = events_dir / "event_001.json"
                with event_path.open() as f:
                    enriched_event = json.load(f)

                # Event should have article_links and quant_links
                self.assertIn("article_links", enriched_event)
                self.assertIn("quant_links", enriched_event)

            finally:
                # Restore
                laq.BASE_DIR = original_base_dir
                laq.ACCEPTED_DIR = original_accepted_dir
                laq.OUTPUT_DIR = original_output_dir
                laq.CONFIG_PATH = original_config_path


if __name__ == "__main__":
    unittest.main()
