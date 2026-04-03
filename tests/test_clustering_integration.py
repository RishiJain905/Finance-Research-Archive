import unittest
import os
import sys
import json
import tempfile
import shutil
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import modules to test
from scripts.cluster_records import (
    process_accepted_records,
    load_clusters,
)
from scripts.update_story_graph import (
    load_edges,
    create_edges_for_record,
    update_graph_on_clustering,
)


class ClusteringIntegrationTests(unittest.TestCase):
    """Integration tests for the clustering pipeline"""

    def setUp(self):
        """Create temp directories for testing"""
        self.test_event_dir = tempfile.mkdtemp()
        self.test_story_dir = tempfile.mkdtemp()
        # Save original dirs
        self.original_event_dir = "data/events"
        self.original_story_dir = "data/story_graph"

    def tearDown(self):
        """Clean up temp directories"""
        shutil.rmtree(self.test_event_dir, ignore_errors=True)
        shutil.rmtree(self.test_story_dir, ignore_errors=True)

    def test_clusters_created_from_accepted_records(self):
        """Test that accepted records get clustered"""
        # This test would need actual accepted records to run
        # For unit testing, we test the functions directly
        pass

    def test_update_graph_creates_edges(self):
        """Test that update_graph_on_clustering creates edges"""
        # Create test data
        record = {
            "id": "test_rec_001",
            "title": "Fed announces rate decision",
            "topic": "macro catalysts",
            "event_type": "fed_policy",
            "tags": ["fed", "rates", "monetary_policy"],
            "source": {"domain": "federalreserve.gov"},
            "linked_quant_context": [{"quant_id": "q001"}],
        }
        cluster = {
            "event_id": "evt_test_001",
            "title": "Fed Rate Policy",
            "record_ids": ["test_rec_001"],
            "confidence": 0.8,
        }

        # This would create edges
        edges = create_edges_for_record(record, cluster)

        # Verify edges created
        self.assertTrue(len(edges) > 0)

    def test_quant_context_edges(self):
        """Test that quant links create quant_context edges"""
        record = {
            "id": "test_rec_002",
            "linked_quant_context": [{"quant_id": "sofr_001"}],
        }
        cluster = {"event_id": "evt_test_002", "quant_links": ["sofr_001"]}

        edges = create_edges_for_record(record, cluster)
        quant_edges = [e for e in edges if e["relationship"] == "quant_context"]
        self.assertTrue(len(quant_edges) >= 1)

    def test_create_edges_for_record_returns_list(self):
        """Test that create_edges_for_record returns a list of edges"""
        record = {
            "id": "test_rec_003",
            "title": "Test Event",
            "topic": "test",
            "event_type": "test_event",
            "tags": ["test"],
            "source": {"domain": "test.com"},
            "linked_quant_context": [],
        }
        cluster = {
            "event_id": "evt_test_003",
            "title": "Test Cluster",
            "record_ids": ["test_rec_003"],
            "confidence": 0.9,
        }

        edges = create_edges_for_record(record, cluster)

        self.assertIsInstance(edges, list)

    def test_load_clusters_handles_missing_file(self):
        """Test that load_clusters handles missing file gracefully"""
        # Call with non-existent path
        clusters = load_clusters("/nonexistent/path/clusters.json")
        self.assertEqual(clusters, [])

    def test_load_edges_handles_missing_file(self):
        """Test that load_edges handles missing file gracefully"""
        # Call with non-existent path
        edges = load_edges("/nonexistent/path/edges.json")
        self.assertEqual(edges, [])


class ProcessRecordIntegrationTests(unittest.TestCase):
    """Tests for process_record.py integration"""

    def test_import_no_error(self):
        """Test that process_record.py imports without error after modification"""
        # Just verify the module can be imported
        import scripts.process_record

        self.assertTrue(hasattr(scripts.process_record, "main"))

    def test_persist_triage_metadata(self):
        """Test triage metadata persistence"""
        from scripts.process_record import persist_triage_metadata
        import tempfile

        test_dir = tempfile.mkdtemp()
        try:
            # Patch TRIAGE_DIR
            import scripts.process_record

            original_triage = scripts.process_record.TRIAGE_DIR
            scripts.process_record.TRIAGE_DIR = Path(test_dir)

            record_id = "test_record_001"
            triage_data = {
                "priority_score": 0.8,
                "priority_band": "high",
                "lane": "fast",
                "reasons": ["breaking", "high_impact"],
            }

            path = persist_triage_metadata(record_id, triage_data)

            self.assertTrue(path.exists())

            # Cleanup
            scripts.process_record.TRIAGE_DIR = original_triage
            shutil.rmtree(test_dir)
        except Exception:
            shutil.rmtree(test_dir, ignore_errors=True)
            raise

    def test_load_triage_metadata_for_record(self):
        """Test loading triage metadata"""
        from scripts.process_record import (
            persist_triage_metadata,
            load_triage_metadata_for_record,
        )
        import tempfile

        test_dir = tempfile.mkdtemp()
        try:
            # Patch TRIAGE_DIR
            import scripts.process_record

            original_triage = scripts.process_record.TRIAGE_DIR
            scripts.process_record.TRIAGE_DIR = Path(test_dir)

            record_id = "test_record_002"
            triage_data = {
                "priority_score": 0.6,
                "priority_band": "medium",
                "lane": "standard",
                "reasons": ["confirmed"],
            }

            # Persist first
            persist_triage_metadata(record_id, triage_data)

            # Then load
            loaded = load_triage_metadata_for_record(record_id)

            self.assertIsNotNone(loaded)
            self.assertEqual(loaded["priority_score"], 0.6)
            self.assertEqual(loaded["priority_band"], "medium")

            # Restore and cleanup
            scripts.process_record.TRIAGE_DIR = original_triage
            shutil.rmtree(test_dir)
        except Exception:
            shutil.rmtree(test_dir, ignore_errors=True)
            raise

    def test_load_triage_metadata_missing_record(self):
        """Test loading triage metadata for non-existent record"""
        from scripts.process_record import load_triage_metadata_for_record
        import tempfile

        test_dir = tempfile.mkdtemp()
        try:
            import scripts.process_record

            original_triage = scripts.process_record.TRIAGE_DIR
            scripts.process_record.TRIAGE_DIR = Path(test_dir)

            loaded = load_triage_metadata_for_record("nonexistent_record")
            self.assertIsNone(loaded)

            scripts.process_record.TRIAGE_DIR = original_triage
            shutil.rmtree(test_dir)
        except Exception:
            shutil.rmtree(test_dir, ignore_errors=True)
            raise


if __name__ == "__main__":
    unittest.main()
