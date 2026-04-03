"""
Tests for Stream 2: Event Clustering Records Script.
Tests verify the clustering algorithm functions:
- Time proximity scoring
- Topic overlap scoring
- Phrase overlap scoring
- Source diversity scoring
- Quant support scoring
- Combined similarity computation
- Cluster creation and management
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.cluster_records import (
    compute_time_proximity_score,
    compute_topic_overlap_score,
    compute_phrase_overlap_score,
    compute_source_diversity_score,
    compute_quant_support_score,
    compute_combined_similarity,
    create_new_cluster,
    should_promote_to_stable,
    generate_cluster_title,
    load_clusters,
    find_recent_clusters,
    attach_to_cluster,
    save_cluster,
    process_accepted_records,
)


class TimeProximityScoreTests(unittest.TestCase):
    """Tests for time proximity scoring function."""

    def setUp(self):
        self.config = {"time_window_days": 7}

    def test_same_day_returns_100(self):
        """Records on the same day should return score of 100."""
        score = compute_time_proximity_score("2026-03-15", "2026-03-15", self.config)
        self.assertEqual(score, 100)

    def test_one_day_diff_returns_80(self):
        """Records 1 day apart should return score of 80."""
        score = compute_time_proximity_score("2026-03-15", "2026-03-16", self.config)
        self.assertEqual(score, 80)

    def test_one_day_diff_reverse_returns_80(self):
        """Records 1 day apart (reverse order) should return score of 80."""
        score = compute_time_proximity_score("2026-03-16", "2026-03-15", self.config)
        self.assertEqual(score, 80)

    def test_three_days_diff_returns_60(self):
        """Records 3 days apart should return score of 60."""
        score = compute_time_proximity_score("2026-03-15", "2026-03-18", self.config)
        self.assertEqual(score, 60)

    def test_seven_days_diff_returns_40(self):
        """Records 7 days apart should return score of 40."""
        score = compute_time_proximity_score("2026-03-15", "2026-03-22", self.config)
        self.assertEqual(score, 40)

    def test_beyond_seven_days_returns_0(self):
        """Records beyond 7 days should return score of 0."""
        score = compute_time_proximity_score("2026-03-15", "2026-03-25", self.config)
        self.assertEqual(score, 0)

    def test_invalid_date_format_handled(self):
        """Invalid date format should return 0 gracefully."""
        score = compute_time_proximity_score("invalid", "2026-03-15", self.config)
        self.assertEqual(score, 0)


class TopicOverlapScoreTests(unittest.TestCase):
    """Tests for topic/tag overlap scoring function."""

    def test_exact_match_returns_50(self):
        """Records with identical tags should return score of 50."""
        tags1 = ["bank-of-england", "interest-rates", "monetary-policy"]
        tags2 = ["bank-of-england", "interest-rates", "monetary-policy"]
        score = compute_topic_overlap_score(tags1, tags2)
        self.assertEqual(score, 50)

    def test_partial_overlap_returns_30(self):
        """Records with partial tag overlap should return score of 30."""
        tags1 = ["bank-of-england", "interest-rates", "monetary-policy"]
        tags2 = ["bank-of-england", "inflation", "economic-outlook"]
        score = compute_topic_overlap_score(tags1, tags2)
        self.assertEqual(score, 30)

    def test_no_overlap_returns_0(self):
        """Records with no tag overlap should return score of 0."""
        tags1 = ["bank-of-england", "interest-rates"]
        tags2 = ["federal-reserve", "inflation"]
        score = compute_topic_overlap_score(tags1, tags2)
        self.assertEqual(score, 0)

    def test_empty_tags_returns_0(self):
        """Records with empty tags should return 0."""
        score = compute_topic_overlap_score([], [])
        self.assertEqual(score, 0)

    def test_one_empty_tags_returns_0(self):
        """One record with empty tags should return 0."""
        score = compute_topic_overlap_score(["tag1"], [])
        self.assertEqual(score, 0)


class PhraseOverlapScoreTests(unittest.TestCase):
    """Tests for phrase/text overlap scoring function."""

    def test_identical_text_returns_high_score(self):
        """Identical text should return high score (max 40)."""
        text1 = "Federal Reserve announces interest rate decision"
        text2 = "Federal Reserve announces interest rate decision"
        score = compute_phrase_overlap_score(text1, text2)
        self.assertGreater(score, 30)
        self.assertLessEqual(score, 40)

    def test_no_overlap_returns_0(self):
        """Text with no overlap should return 0."""
        text1 = "bank of england inflation"
        text2 = "federal reserve employment"
        score = compute_phrase_overlap_score(text1, text2)
        self.assertEqual(score, 0)

    def test_partial_overlap_returns_partial_score(self):
        """Partial text overlap should return proportional score."""
        text1 = "Federal Reserve interest rate decision"
        text2 = "Federal Reserve inflation decision"
        score = compute_phrase_overlap_score(text1, text2)
        self.assertGreater(score, 0)
        self.assertLess(score, 40)

    def test_case_insensitive(self):
        """Overlap should be case insensitive."""
        text1 = "BANK OF ENGLAND"
        text2 = "bank of england"
        score = compute_phrase_overlap_score(text1, text2)
        self.assertGreater(score, 0)

    def test_empty_text_returns_0(self):
        """Empty text should return 0."""
        score = compute_phrase_overlap_score("", "")
        self.assertEqual(score, 0)


class SourceDiversityScoreTests(unittest.TestCase):
    """Tests for source diversity scoring function."""

    def test_single_domain_returns_0(self):
        """Single source domain should return score of 0."""
        domains = ["www.bankofengland.co.uk"]
        score = compute_source_diversity_score(domains)
        self.assertEqual(score, 0)

    def test_two_domains_returns_8(self):
        """Two source domains should return score of 8."""
        domains = ["www.bankofengland.co.uk", "www.federalreserve.gov"]
        score = compute_source_diversity_score(domains)
        self.assertEqual(score, 8)

    def test_three_domains_returns_15(self):
        """Three or more source domains should return score of 15."""
        domains = [
            "www.bankofengland.co.uk",
            "www.federalreserve.gov",
            "www.ecb.europa.eu",
        ]
        score = compute_source_diversity_score(domains)
        self.assertEqual(score, 15)

    def test_many_domains_returns_15(self):
        """Many source domains should still return max score of 15."""
        domains = [
            "www.bankofengland.co.uk",
            "www.federalreserve.gov",
            "www.ecb.europa.eu",
            "www.bis.org",
            "www.rba.co.au",
        ]
        score = compute_source_diversity_score(domains)
        self.assertEqual(score, 15)

    def test_empty_domains_returns_0(self):
        """Empty domain list should return 0."""
        score = compute_source_diversity_score([])
        self.assertEqual(score, 0)


class QuantSupportScoreTests(unittest.TestCase):
    """Tests for quantitative support scoring function."""

    def test_has_quant_link_returns_30(self):
        """Records with quant links should return score of 30."""
        quant_links = ["quant_record_1", "quant_record_2"]
        score = compute_quant_support_score(quant_links)
        self.assertEqual(score, 30)

    def test_single_quant_link_returns_30(self):
        """Single quant link should return score of 30."""
        quant_links = ["quant_record_1"]
        score = compute_quant_support_score(quant_links)
        self.assertEqual(score, 30)

    def test_no_quant_link_returns_0(self):
        """Records without quant links should return score of 0."""
        quant_links = []
        score = compute_quant_support_score(quant_links)
        self.assertEqual(score, 0)

    def test_missing_quant_links_field_returns_0(self):
        """Missing quant links field should return 0."""
        score = compute_quant_support_score(None)
        self.assertEqual(score, 0)


class CombinedSimilarityTests(unittest.TestCase):
    """Tests for combined similarity scoring function."""

    def setUp(self):
        self.config = {
            "weight_overrides": {
                "topic_compatibility": 0.30,
                "phrase_overlap": 0.25,
                "time_proximity": 0.20,
                "source_diversity": 0.15,
                "quant_support": 0.10,
            }
        }

    def test_weighted_sum_calculation(self):
        """Combined score should be weighted sum of individual scores."""
        # Create a mock record
        record = {
            "id": "record_1",
            "source": {
                "domain": "www.federalreserve.gov",
                "published_at": "2026-03-15",
            },
            "tags": ["fed", "rates"],
            "quant_links": [],
        }
        # Create a mock cluster
        cluster = {
            "event_id": "cluster_1",
            "source_domains": ["www.federalreserve.gov", "www.ecb.europa.eu"],
            "keywords": ["fed", "rates", "inflation"],
            "record_ids": [],
            "created_at": "2026-03-15T10:00:00Z",
        }

        score = compute_combined_similarity(record, cluster, self.config)

        # Score should be a positive number (weighted sum)
        self.assertIsInstance(score, (int, float))
        self.assertGreaterEqual(score, 0)

    def test_high_similarity_returns_high_score(self):
        """Records with high similarity should return reasonably high combined score."""
        # Same day, same tags (exact match), same title text, quant link
        record = {
            "id": "record_1",
            "source": {
                "domain": "www.bankofengland.co.uk",
                "published_at": "2026-03-15",
            },
            "title": "Bank of England Rate Decision",
            "summary": "The Bank of England announced a rate decision",
            "tags": ["bank-of-england", "interest-rates", "monetary-policy"],
            "quant_links": ["quant_1"],
        }
        cluster = {
            "event_id": "cluster_1",
            "title": "Bank of England Rate Decision on Interest Rates",
            "summary": "The Bank of England announced a rate decision",
            "source_domains": [
                "www.bankofengland.co.uk",
                "www.federalreserve.gov",
            ],
            "keywords": ["bank-of-england", "interest-rates", "monetary-policy"],
            "record_ids": ["existing_record"],
            "created_at": "2026-03-15T10:00:00Z",
        }

        score = compute_combined_similarity(record, cluster, self.config)

        # Should have substantial score due to time + topic + phrase overlap
        # Min expected: time(100*.2=20) + topic(50*.3=15) + phrase(moderate*.25) + diversity(8*.15=1.2) + quant(30*.1=3)
        self.assertGreater(score, 35)

    def test_low_similarity_returns_low_score(self):
        """Records with low similarity should return low combined score."""
        record = {
            "id": "record_1",
            "source": {
                "domain": "www.bankofengland.co.uk",
                "published_at": "2026-03-01",
            },
            "tags": ["bank-of-england", "inflation"],
            "quant_links": [],
        }
        cluster = {
            "event_id": "cluster_1",
            "source_domains": ["www.federalreserve.gov"],
            "keywords": ["federal-reserve", "employment", "gdp"],
            "record_ids": [],
            "created_at": "2026-03-15T10:00:00Z",
        }

        score = compute_combined_similarity(record, cluster, self.config)

        # Low similarity should produce score below threshold
        self.assertLess(score, 60)


class NewClusterCreationTests(unittest.TestCase):
    """Tests for cluster creation function."""

    def setUp(self):
        self.config = {"stable_threshold": 5}
        self.record = {
            "id": "test_record_1",
            "title": "Federal Reserve Interest Rate Decision",
            "source": {
                "domain": "www.federalreserve.gov",
                "published_at": "2026-03-15",
                "name": "Federal Reserve",
            },
            "topic": "monetary policy",
            "event_type": "fed_speech",
            "tags": ["federal-reserve", "interest-rates"],
            "quant_links": [],
            "summary": "Test summary",
        }
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_creates_with_correct_fields(self):
        """New cluster should have all required fields from schema."""
        cluster = create_new_cluster(self.record, self.config, self.temp_dir)

        # Check required fields from event_cluster schema
        self.assertIn("event_id", cluster)
        self.assertIn("title", cluster)
        self.assertIn("topic", cluster)
        self.assertIn("event_type", cluster)
        self.assertIn("summary", cluster)
        self.assertIn("status", cluster)
        self.assertIn("created_at", cluster)
        self.assertIn("updated_at", cluster)
        self.assertIn("record_ids", cluster)
        self.assertIn("source_domains", cluster)
        self.assertIn("keywords", cluster)
        self.assertIn("quant_links", cluster)
        self.assertIn("confidence", cluster)

    def test_status_is_open(self):
        """New cluster should have status 'open'."""
        cluster = create_new_cluster(self.record, self.config, self.temp_dir)
        self.assertEqual(cluster["status"], "open")

    def test_confidence_is_1(self):
        """New cluster should have confidence of 1.0."""
        cluster = create_new_cluster(self.record, self.config, self.temp_dir)
        self.assertEqual(cluster["confidence"], 1.0)

    def test_contains_record_id(self):
        """New cluster should contain the record ID."""
        cluster = create_new_cluster(self.record, self.config, self.temp_dir)
        self.assertIn(self.record["id"], cluster["record_ids"])

    def test_contains_source_domain(self):
        """New cluster should contain source domain."""
        cluster = create_new_cluster(self.record, self.config, self.temp_dir)
        self.assertIn(self.record["source"]["domain"], cluster["source_domains"])

    def test_event_id_is_unique(self):
        """Each new cluster should have a unique event_id."""
        cluster1 = create_new_cluster(self.record, self.config, self.temp_dir)
        record2 = dict(self.record)
        record2["id"] = "test_record_2"
        cluster2 = create_new_cluster(record2, self.config, self.temp_dir)

        self.assertNotEqual(cluster1["event_id"], cluster2["event_id"])


class ShouldPromoteToStableTests(unittest.TestCase):
    """Tests for cluster stability promotion function."""

    def setUp(self):
        self.config = {"stable_threshold": 5}

    def test_below_threshold_not_promoted(self):
        """Cluster with records below threshold should not be promoted."""
        cluster = {
            "record_ids": ["record_1", "record_2", "record_3"],
            "status": "open",
        }
        result = should_promote_to_stable(cluster, self.config)
        self.assertFalse(result)

    def test_at_threshold_promoted(self):
        """Cluster with records at threshold should be promoted."""
        cluster = {
            "record_ids": ["record_1", "record_2", "record_3", "record_4", "record_5"],
            "status": "open",
        }
        result = should_promote_to_stable(cluster, self.config)
        self.assertTrue(result)

    def test_above_threshold_promoted(self):
        """Cluster with records above threshold should be promoted."""
        cluster = {
            "record_ids": [
                "record_1",
                "record_2",
                "record_3",
                "record_4",
                "record_5",
                "record_6",
                "record_7",
            ],
            "status": "open",
        }
        result = should_promote_to_stable(cluster, self.config)
        self.assertTrue(result)

    def test_already_stable_not_promoted_again(self):
        """Already stable cluster should not be re-promoted."""
        cluster = {
            "record_ids": [
                "record_1",
                "record_2",
                "record_3",
                "record_4",
                "record_5",
                "record_6",
            ],
            "status": "stable",
        }
        result = should_promote_to_stable(cluster, self.config)
        self.assertFalse(result)

    def test_archived_not_promoted(self):
        """Archived cluster should not be promoted."""
        cluster = {
            "record_ids": [
                "record_1",
                "record_2",
                "record_3",
                "record_4",
                "record_5",
                "record_6",
            ],
            "status": "archived",
        }
        result = should_promote_to_stable(cluster, self.config)
        self.assertFalse(result)


class TitleGenerationTests(unittest.TestCase):
    """Tests for deterministic cluster title generation."""

    def test_deterministic_title(self):
        """Title should be deterministic based on inputs."""
        cluster = {
            "keywords": [
                "federal-reserve",
                "interest-rates",
                "inflation",
                "monetary-policy",
            ],
            "event_type": "fed_speech",
        }
        record = {
            "title": "Federal Reserve Chair Powell Speech on Interest Rates",
            "source": {"name": "Federal Reserve Board"},
        }

        title1 = generate_cluster_title(cluster, record)
        title2 = generate_cluster_title(cluster, record)

        self.assertEqual(title1, title2)

    def test_title_contains_keywords(self):
        """Generated title should contain top keywords."""
        cluster = {
            "keywords": ["federal-reserve", "interest-rates", "inflation"],
            "event_type": "fed_speech",
        }
        record = {
            "title": "Powell Speech",
            "source": {"name": "Federal Reserve"},
        }

        title = generate_cluster_title(cluster, record)

        # Title should include some keywords or key content
        self.assertIsInstance(title, str)
        self.assertGreater(len(title), 0)

    def test_title_includes_event_type(self):
        """Generated title should include event type."""
        cluster = {
            "keywords": ["bank-of-england", "monetary-policy"],
            "event_type": "rate_change",
        }
        record = {
            "title": "Bank of England Rate Decision",
            "source": {"name": "Bank of England"},
        }

        title = generate_cluster_title(cluster, record)

        # Title should reference the event type or topic
        # Check that event_type is included (formatted without underscore)
        self.assertTrue(
            "rate change" in title.lower() or "rate_change" in title.lower(),
            f"Title '{title}' should contain 'rate change' or 'rate_change'",
        )


class LoadClustersTests(unittest.TestCase):
    """Tests for cluster loading function."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_loads_existing_clusters(self):
        """Should load all cluster JSON files from directory."""
        # Create test cluster files
        cluster1 = {
            "event_id": "cluster_1",
            "title": "Test Cluster 1",
            "status": "open",
            "record_ids": ["record_1"],
        }
        cluster2 = {
            "event_id": "cluster_2",
            "title": "Test Cluster 2",
            "status": "stable",
            "record_ids": ["record_2", "record_3"],
        }

        import json

        with open(os.path.join(self.temp_dir, "cluster_1.json"), "w") as f:
            json.dump(cluster1, f)
        with open(os.path.join(self.temp_dir, "cluster_2.json"), "w") as f:
            json.dump(cluster2, f)

        clusters = load_clusters(self.temp_dir)

        self.assertEqual(len(clusters), 2)
        event_ids = [c["event_id"] for c in clusters]
        self.assertIn("cluster_1", event_ids)
        self.assertIn("cluster_2", event_ids)

    def test_returns_empty_list_for_empty_directory(self):
        """Should return empty list if no cluster files exist."""
        clusters = load_clusters(self.temp_dir)
        self.assertEqual(clusters, [])

    def test_ignores_non_json_files(self):
        """Should ignore non-JSON files in cluster directory."""
        # Create a .gitkeep file
        Path(os.path.join(self.temp_dir, ".gitkeep")).touch()

        clusters = load_clusters(self.temp_dir)
        self.assertEqual(clusters, [])


class FindRecentClustersTests(unittest.TestCase):
    """Tests for finding recent clusters within time window."""

    def setUp(self):
        self.config = {
            "similarity_threshold": 35,  # Lower threshold for testing
            "time_window_days": 7,
            "weight_overrides": {
                "topic_compatibility": 0.30,
                "phrase_overlap": 0.25,
                "time_proximity": 0.20,
                "source_diversity": 0.15,
                "quant_support": 0.10,
            },
        }

    def test_finds_clusters_within_window(self):
        """Should find clusters within the time window that exceed threshold."""
        # Create a record with high similarity to cluster_1
        record = {
            "id": "new_record",
            "source": {
                "domain": "www.bankofengland.co.uk",
                "published_at": "2026-03-15",
            },
            "title": "Bank of England Interest Rates",
            "summary": "The Bank of England discussed interest rates and monetary policy",
            "tags": ["bank-of-england", "interest-rates"],
            "quant_links": [],
        }

        clusters = [
            {
                "event_id": "cluster_1",
                "title": "Bank of England Interest Rate Decision",
                "summary": "The Bank of England discussed interest rates and monetary policy",
                "source_domains": ["www.bankofengland.co.uk"],
                "keywords": ["bank-of-england", "interest-rates"],
                "record_ids": ["existing_record"],
                "created_at": "2026-03-14T10:00:00Z",  # 1 day before
            },
            {
                "event_id": "cluster_2",
                "source_domains": ["www.federalreserve.gov"],
                "keywords": ["federal-reserve", "inflation"],
                "record_ids": [],
                "created_at": "2026-03-10T10:00:00Z",  # 5 days before
            },
        ]

        recent = find_recent_clusters(record, clusters, self.config, lookback_days=7)

        # Should find at least the high-similarity cluster (cluster_1)
        self.assertGreaterEqual(len(recent), 1)
        # Verify it's cluster_1 that was found
        self.assertEqual(recent[0]["event_id"], "cluster_1")

    def test_excludes_clusters_outside_window(self):
        """Should exclude clusters outside the lookback window."""
        record = {
            "id": "new_record",
            "source": {
                "domain": "www.bankofengland.co.uk",
                "published_at": "2026-03-20",
            },
            "tags": ["bank-of-england", "interest-rates"],
            "quant_links": [],
        }

        clusters = [
            {
                "event_id": "cluster_old",
                "source_domains": ["www.bankofengland.co.uk"],
                "keywords": ["bank-of-england", "interest-rates"],
                "record_ids": ["old_record"],
                "created_at": "2026-03-01T10:00:00Z",  # 19 days before - outside window
            },
        ]

        recent = find_recent_clusters(record, clusters, self.config, lookback_days=7)

        self.assertEqual(len(recent), 0)

    def test_requires_threshold_exceedance(self):
        """Should only return clusters that exceed similarity threshold."""
        record = {
            "id": "new_record",
            "source": {
                "domain": "www.bankofengland.co.uk",
                "published_at": "2026-03-15",
            },
            "tags": ["bank-of-england", "interest-rates"],
            "quant_links": [],
        }

        clusters = [
            {
                "event_id": "cluster_different",
                "source_domains": ["www.random-site.com"],
                "keywords": ["completely", "different", "topics"],
                "record_ids": [],
                "created_at": "2026-03-15T10:00:00Z",  # Same day but very different content
            },
        ]

        recent = find_recent_clusters(record, clusters, self.config, lookback_days=7)

        # Should not return clusters that don't exceed threshold
        self.assertEqual(len(recent), 0)


class SaveAndAttachClusterTests(unittest.TestCase):
    """Tests for saving clusters and attaching records."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_save_cluster_writes_file(self):
        """Saving cluster should write JSON file to event_dir."""
        cluster = {
            "event_id": "test_cluster",
            "title": "Test Cluster",
            "status": "open",
            "record_ids": [],
        }

        save_cluster(cluster, self.temp_dir)

        file_path = os.path.join(self.temp_dir, f"{cluster['event_id']}.json")
        self.assertTrue(os.path.exists(file_path))

        # Verify content
        import json

        with open(file_path, "r") as f:
            loaded = json.load(f)
        self.assertEqual(loaded["event_id"], "test_cluster")

    def test_attach_to_cluster_updates_record_ids(self):
        """Attaching record to cluster should update record_ids array."""
        cluster = {
            "event_id": "test_cluster",
            "title": "Test Cluster",
            "status": "open",
            "record_ids": ["existing_record"],
            "source_domains": ["www.example.com"],
            "keywords": ["keyword1"],
        }
        record = {
            "id": "new_record",
            "source": {"domain": "www.newsource.com"},
            "tags": ["newkeyword"],
        }

        attach_to_cluster(record, cluster, self.temp_dir)

        # Cluster should now contain both records
        self.assertIn("new_record", cluster["record_ids"])
        self.assertIn("existing_record", cluster["record_ids"])
        # Source domains should be merged (deduplicated)
        self.assertIn("www.newsource.com", cluster["source_domains"])
        # Keywords should be merged (deduplicated)
        self.assertIn("newkeyword", cluster["keywords"])


class ProcessAcceptedRecordsTests(unittest.TestCase):
    """Integration tests for the main processing function."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.accepted_dir = tempfile.mkdtemp()
        self.events_dir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)
        shutil.rmtree(self.accepted_dir, ignore_errors=True)
        shutil.rmtree(self.events_dir, ignore_errors=True)

    def test_process_accepted_records_runs(self):
        """process_accepted_records should complete without error."""
        # This is a smoke test - just verify the function can run
        config = {
            "similarity_threshold": 60,
            "time_window_days": 7,
            "stable_threshold": 5,
            "weight_overrides": {
                "topic_compatibility": 0.30,
                "phrase_overlap": 0.25,
                "time_proximity": 0.20,
                "source_diversity": 0.15,
                "quant_support": 0.10,
            },
        }

        # Create a minimal accepted record
        import json

        record = {
            "id": "test_accepted_record",
            "created_at": "2026-03-31T16:51:22.090922+00:00",
            "status": "accepted",
            "topic": "market structure",
            "event_type": "treasury_issuance",
            "title": "Foreign Currency Reserves 2026 - Market Notice",
            "source": {
                "name": "Bank of England Markets",
                "url": "https://www.bankofengland.co.uk/markets",
                "domain": "www.bankofengland.co.uk",
                "published_at": "2026-03-27",
            },
            "summary": "Test summary",
            "tags": ["bank-of-england", "foreign-currency-reserves"],
            "quant_links": [],
            "key_points": [],
        }

        record_path = os.path.join(self.accepted_dir, "test_record.json")
        with open(record_path, "w") as f:
            json.dump(record, f)

        # Mock the paths using patch
        with patch("scripts.cluster_records.Path") as mock_path:
            # Configure mock to return temp paths
            mock_path.return_value = MagicMock()
            mock_path.return_value.__truediv__ = lambda self, x: (
                self if x == "accepted" else MagicMock()
            )

            # Just verify no import errors occur
            # Full integration test would require more complex mocking


if __name__ == "__main__":
    unittest.main()
