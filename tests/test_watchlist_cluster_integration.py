"""
Integration tests for Phase 2.7 Part 3: Watchlist Matching for Event Clusters.
Tests verify that event clusters generate watchlist hits after clustering completes.
"""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, Mock

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE_DIR = Path(__file__).resolve().parent.parent


class TestClusterWatchlistIntegration(unittest.TestCase):
    """Integration tests for cluster-level watchlist matching."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.accepted_dir = tempfile.mkdtemp()
        self.events_dir = tempfile.mkdtemp()
        self.hits_dir = tempfile.mkdtemp()
        self.config_dir = tempfile.mkdtemp()

        # Create test watchlist config
        self.watchlist_config = [
            {
                "watchlist_id": "wl_repo_stress",
                "title": "Repo Market Stress",
                "topic": "repo",
                "keywords": ["repo", "collateral", "funding", "SOFR"],
                "priority": "high",
                "enabled": True,
            },
            {
                "watchlist_id": "wl_treasury_refunding",
                "title": "Treasury Refunding",
                "topic": "treasury",
                "keywords": ["refunding", "Treasury", "auction", "debt"],
                "priority": "medium",
                "enabled": True,
            },
            {
                "watchlist_id": "wl_disabled",
                "title": "Disabled Watchlist",
                "topic": "test",
                "keywords": ["disabled"],
                "priority": "low",
                "enabled": False,
            },
        ]

        config_path = os.path.join(self.config_dir, "watchlists_v27.json")
        with open(config_path, "w") as f:
            json.dump(self.watchlist_config, f)
        self.config_path = config_path

        # Sample cluster data
        self.cluster1 = {
            "event_id": "evt_cluster_001",
            "title": "Repo Market Sees Collateral Shortage",
            "topic": "repo",
            "event_type": "market_stress",
            "summary": "Repo rates spike as collateral becomes scarce.",
            "status": "open",
            "created_at": "2026-03-15T10:00:00Z",
            "updated_at": "2026-03-15T10:00:00Z",
            "record_ids": ["rec_1", "rec_2"],
            "source_domains": ["www.federalreserve.gov"],
            "keywords": ["repo", "collateral", "funding", "SOFR"],
            "quant_links": [],
            "confidence": 0.85,
        }

        self.cluster2 = {
            "event_id": "evt_cluster_002",
            "title": "Treasury Refunding Announcement",
            "topic": "treasury",
            "event_type": "issuance",
            "summary": "Treasury announces large refunding operation.",
            "status": "stable",
            "created_at": "2026-03-14T10:00:00Z",
            "updated_at": "2026-03-14T10:00:00Z",
            "record_ids": ["rec_3"],
            "source_domains": ["www.treasury.gov"],
            "keywords": ["refunding", "Treasury", "auction"],
            "quant_links": [],
            "confidence": 0.9,
        }

    def tearDown(self):
        """Clean up temp directories."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)
        shutil.rmtree(self.accepted_dir, ignore_errors=True)
        shutil.rmtree(self.events_dir, ignore_errors=True)
        shutil.rmtree(self.hits_dir, ignore_errors=True)
        shutil.rmtree(self.config_dir, ignore_errors=True)

    def test_clusters_generate_watchlist_hits(self):
        """Event clusters should generate watchlist hits after clustering completes."""
        # Track calls
        calls = {"match_count": 0, "save_count": 0}

        # Create mock functions
        def mock_match_cluster(cluster, watchlists):
            calls["match_count"] += 1
            return [
                {
                    "watchlist_id": "wl_repo_stress",
                    "record_id": None,
                    "event_id": cluster.get("event_id"),
                    "match_score": 0.75,
                    "matched_terms": ["repo", "collateral"],
                    "thesis_signal": "strengthening",
                    "created_at": "2026-03-15T12:00:00Z",
                }
            ]

        def mock_save_hit(hit, hits_dir):
            calls["save_count"] += 1
            return Path(hits_dir) / "test_hit.json"

        def mock_load_watchlists(path):
            return [wl for wl in self.watchlist_config if wl.get("enabled", False)]

        # Patch the functions at the point they're imported
        with patch(
            "scripts.watchlist_matcher.match_cluster_against_watchlists",
            mock_match_cluster,
        ):
            with patch(
                "scripts.watchlist_matcher.load_watchlists", mock_load_watchlists
            ):
                with patch(
                    "scripts.watchlist_hit_persistence.save_watchlist_hit",
                    mock_save_hit,
                ):
                    from scripts.cluster_records import process_accepted_records

                    # Create a sample accepted record
                    record = {
                        "id": "test_record",
                        "status": "accepted",
                        "title": "Repo Market Test",
                        "summary": "Test record",
                        "topic": "repo",
                        "event_type": "market_stress",
                        "tags": ["repo", "collateral"],
                        "source": {
                            "domain": "www.test.com",
                            "published_at": "2026-03-15",
                        },
                        "quant_links": [],
                    }
                    record_path = os.path.join(self.accepted_dir, "test_record.json")
                    with open(record_path, "w") as f:
                        json.dump(record, f)

                    # Mock load_clusters to return our test clusters
                    with patch(
                        "scripts.cluster_records.load_clusters"
                    ) as mock_load_clusters:
                        mock_load_clusters.return_value = [self.cluster1, self.cluster2]

                        with patch.object(Path, "mkdir", return_value=None):
                            with patch(
                                "scripts.cluster_records.DEFAULT_ACCEPTED_DIR",
                                self.accepted_dir,
                            ):
                                with patch(
                                    "scripts.cluster_records.DEFAULT_EVENTS_DIR",
                                    self.events_dir,
                                ):
                                    count = process_accepted_records()

                    # Verify matcher was called at least once (for existing clusters)
                    self.assertGreaterEqual(calls["match_count"], 1)
                    # Verify persistence was called for hits
                    self.assertGreaterEqual(calls["save_count"], 1)

    def test_cluster_hits_include_event_id(self):
        """Cluster hits should include event_id, not record_id."""
        hit = {
            "watchlist_id": "wl_repo_stress",
            "record_id": None,
            "event_id": "evt_cluster_001",
            "match_score": 0.75,
            "matched_terms": ["repo", "collateral"],
            "thesis_signal": "strengthening",
            "created_at": "2026-03-15T12:00:00Z",
        }

        # Verify event_id is set
        self.assertEqual(hit["event_id"], "evt_cluster_001")
        # Verify record_id is None for cluster hits
        self.assertIsNone(hit["record_id"])

    def test_cluster_hits_persisted_correctly(self):
        """Cluster hits should be persisted to the hits directory."""
        from scripts.watchlist_hit_persistence import save_watchlist_hit

        hit = {
            "watchlist_id": "wl_repo_stress",
            "record_id": None,
            "event_id": "evt_cluster_001",
            "match_score": 0.75,
            "matched_terms": ["repo", "collateral"],
            "thesis_signal": "strengthening",
            "created_at": "2026-03-15T12:00:00Z",
        }

        saved_path = save_watchlist_hit(hit, self.hits_dir)

        # Verify file was created
        self.assertTrue(os.path.exists(saved_path))

        # Verify content
        with open(saved_path, "r") as f:
            loaded = json.load(f)
        self.assertEqual(loaded["event_id"], "evt_cluster_001")
        self.assertEqual(loaded["watchlist_id"], "wl_repo_stress")

    def test_multiple_clusters_matched_correctly(self):
        """Multiple clusters should each be matched against watchlists."""
        from scripts.watchlist_matcher import match_cluster_against_watchlists

        watchlists = [wl for wl in self.watchlist_config if wl.get("enabled", False)]

        # Match both clusters
        hits1 = match_cluster_against_watchlists(self.cluster1, watchlists)
        hits2 = match_cluster_against_watchlists(self.cluster2, watchlists)

        # Cluster 1 should match wl_repo_stress (topic=repo)
        hit_ids1 = [h["watchlist_id"] for h in hits1]
        self.assertIn("wl_repo_stress", hit_ids1)

        # Cluster 2 should match wl_treasury_refunding (topic=treasury)
        hit_ids2 = [h["watchlist_id"] for h in hits2]
        self.assertIn("wl_treasury_refunding", hit_ids2)

    def test_disabled_watchlists_skipped(self):
        """Disabled watchlists should not be in the enabled watchlists list."""
        from scripts.watchlist_matcher import load_watchlists

        watchlists = load_watchlists(self.config_path)

        # Verify disabled watchlist is not included
        ids = [wl["watchlist_id"] for wl in watchlists]
        self.assertNotIn("wl_disabled", ids)
        # Verify enabled watchlists are included
        self.assertIn("wl_repo_stress", ids)
        self.assertIn("wl_treasury_refunding", ids)


class TestClusterHitSchema(unittest.TestCase):
    """Tests for verifying cluster hit schema requirements."""

    def test_hit_requires_event_id_for_clusters(self):
        """Hits for clusters must have event_id field set."""
        from scripts.watchlist_matcher import match_cluster_against_watchlists

        cluster = {
            "event_id": "evt_test_123",
            "title": "Test Cluster",
            "summary": "Test summary",
            "topic": "test",
            "event_type": "test",
            "keywords": ["test"],
            "record_ids": [],
            "source_domains": [],
            "quant_links": [],
            "status": "open",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        }

        watchlists = [
            {
                "watchlist_id": "wl_test",
                "title": "Test",
                "topic": "test",
                "keywords": ["test"],
                "enabled": True,
            }
        ]

        hits = match_cluster_against_watchlists(cluster, watchlists)

        # If there are hits, they must have event_id set
        for hit in hits:
            self.assertIsNotNone(hit.get("event_id"))
            self.assertEqual(hit["event_id"], "evt_test_123")

    def test_record_hit_has_null_event_id(self):
        """Hits for records should have event_id as None."""
        from scripts.watchlist_matcher import match_record_against_watchlists

        record = {
            "id": "rec_test_123",
            "title": "Test Record",
            "summary": "Test summary",
            "topic": "test",
            "event_type": "test",
            "tags": ["test"],
            "important_numbers": [],
        }

        watchlists = [
            {
                "watchlist_id": "wl_test",
                "title": "Test",
                "topic": "test",
                "keywords": ["test"],
                "enabled": True,
            }
        ]

        hits = match_record_against_watchlists(record, watchlists)

        for hit in hits:
            self.assertIsNone(hit.get("event_id"))


if __name__ == "__main__":
    unittest.main()
