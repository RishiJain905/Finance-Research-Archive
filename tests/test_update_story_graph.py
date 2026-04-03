"""
Tests for Story Graph Update Script (S3).

Tests edge creation, deduplication, graph traversal, and narrative building.
"""

import unittest
import sys
import os
import json
import tempfile
import shutil

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.update_story_graph import (
    create_edge,
    edge_exists,
    deduplicate_edges,
    create_edges_for_record,
    compute_edge_weight,
    get_edges_for_node,
    get_related_nodes,
    get_event_narrative,
    find_duplicate_themes,
    load_edges,
    save_edges,
)


class EdgeCreationTests(unittest.TestCase):
    """Tests for create_edge function."""

    def test_creates_edge_with_correct_fields(self):
        """Edge should have all required fields from story_edge schema."""
        edge = create_edge(
            from_type="record",
            from_id="rec_123",
            to_type="event",
            to_id="evt_456",
            relationship="supports",
            weight=0.85,
        )

        self.assertEqual(edge["from_type"], "record")
        self.assertEqual(edge["from_id"], "rec_123")
        self.assertEqual(edge["to_type"], "event")
        self.assertEqual(edge["to_id"], "evt_456")
        self.assertEqual(edge["relationship"], "supports")
        self.assertEqual(edge["weight"], 0.85)

    def test_relationship_enum_validation(self):
        """Edge should accept all valid relationship types."""
        valid_relationships = [
            "supports",
            "extends",
            "quant_context",
            "contradicts",
            "duplicate_theme",
        ]
        for rel in valid_relationships:
            edge = create_edge(
                from_type="record",
                from_id="rec_1",
                to_type="event",
                to_id="evt_1",
                relationship=rel,
                weight=0.5,
            )
            self.assertEqual(edge["relationship"], rel)

    def test_weight_bounds(self):
        """Weight should be within 0.0-1.0 bounds."""
        # Test lower bound
        edge_low = create_edge(
            from_type="record",
            from_id="rec_1",
            to_type="event",
            to_id="evt_1",
            relationship="supports",
            weight=0.0,
        )
        self.assertEqual(edge_low["weight"], 0.0)

        # Test upper bound
        edge_high = create_edge(
            from_type="record",
            from_id="rec_1",
            to_type="event",
            to_id="evt_1",
            relationship="supports",
            weight=1.0,
        )
        self.assertEqual(edge_high["weight"], 1.0)

    def test_from_to_types_enum(self):
        """From_type and to_type should accept valid node types."""
        valid_types = ["record", "event", "quant", "theme"]
        for from_type in valid_types:
            for to_type in valid_types:
                edge = create_edge(
                    from_type=from_type,
                    from_id="src_1",
                    to_type=to_type,
                    to_id="dst_1",
                    relationship="supports",
                    weight=0.5,
                )
                self.assertEqual(edge["from_type"], from_type)
                self.assertEqual(edge["to_type"], to_type)


class EdgeExistenceTests(unittest.TestCase):
    """Tests for edge_exists function."""

    def setUp(self):
        self.edges = [
            create_edge(
                from_type="record",
                from_id="rec_1",
                to_type="event",
                to_id="evt_1",
                relationship="supports",
                weight=0.8,
            ),
            create_edge(
                from_type="record",
                from_id="rec_2",
                to_type="event",
                to_id="evt_1",
                relationship="supports",
                weight=0.6,
            ),
            create_edge(
                from_type="event",
                from_id="evt_1",
                to_type="quant",
                to_id="quant_1",
                relationship="quant_context",
                weight=0.9,
            ),
        ]

    def test_detects_existing_edge(self):
        """Should return True when edge exists."""
        result = edge_exists(
            self.edges,
            from_type="record",
            from_id="rec_1",
            to_type="event",
            to_id="evt_1",
            relationship="supports",
        )
        self.assertTrue(result)

    def test_detects_nonexistent_edge(self):
        """Should return False when edge does not exist."""
        result = edge_exists(
            self.edges,
            from_type="record",
            from_id="rec_999",
            to_type="event",
            to_id="evt_1",
            relationship="supports",
        )
        self.assertFalse(result)

    def test_detects_nonexistent_relationship(self):
        """Should return False when relationship differs."""
        result = edge_exists(
            self.edges,
            from_type="record",
            from_id="rec_1",
            to_type="event",
            to_id="evt_1",
            relationship="extends",  # Different relationship
        )
        self.assertFalse(result)


class DeduplicationTests(unittest.TestCase):
    """Tests for deduplicate_edges function."""

    def setUp(self):
        self.existing_edges = [
            create_edge(
                from_type="record",
                from_id="rec_1",
                to_type="event",
                to_id="evt_1",
                relationship="supports",
                weight=0.8,
            ),
        ]

    def test_removes_duplicate_edges(self):
        """Should remove edges that already exist."""
        new_edges = [
            create_edge(
                from_type="record",
                from_id="rec_1",
                to_type="event",
                to_id="evt_1",
                relationship="supports",
                weight=0.9,  # Different weight but same identity
            ),
            create_edge(
                from_type="record",
                from_id="rec_2",
                to_type="event",
                to_id="evt_1",
                relationship="supports",
                weight=0.7,
            ),
        ]

        result = deduplicate_edges(new_edges, self.existing_edges)

        # rec_1 -> evt_1 should be removed (duplicate)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["from_id"], "rec_2")

    def test_keeps_unique_edges(self):
        """Should keep edges that don't exist."""
        new_edges = [
            create_edge(
                from_type="record",
                from_id="rec_3",
                to_type="event",
                to_id="evt_2",  # Different target
                relationship="supports",
                weight=0.7,
            ),
        ]

        result = deduplicate_edges(new_edges, self.existing_edges)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["from_id"], "rec_3")


class EdgesForNodeTests(unittest.TestCase):
    """Tests for get_edges_for_node function."""

    def setUp(self):
        self.edges = [
            create_edge(
                from_type="record",
                from_id="rec_1",
                to_type="event",
                to_id="evt_1",
                relationship="supports",
                weight=0.8,
            ),
            create_edge(
                from_type="record",
                from_id="rec_2",
                to_type="event",
                to_id="evt_1",
                relationship="supports",
                weight=0.6,
            ),
            create_edge(
                from_type="event",
                from_id="evt_1",
                to_type="quant",
                to_id="quant_1",
                relationship="quant_context",
                weight=0.9,
            ),
        ]

    def test_gets_edges_as_from(self):
        """Should return edges where node is the from_id."""
        result = get_edges_for_node(self.edges, node_type="record", node_id="rec_1")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["from_id"], "rec_1")

    def test_gets_edges_as_to(self):
        """Should return edges where node is the to_id."""
        result = get_edges_for_node(self.edges, node_type="event", node_id="evt_1")
        # Should get all 3 edges: 2 where evt_1 is to, 1 where evt_1 is from
        self.assertEqual(len(result), 3)


class RelatedNodesTests(unittest.TestCase):
    """Tests for get_related_nodes function."""

    def setUp(self):
        self.edges = [
            create_edge(
                from_type="record",
                from_id="rec_1",
                to_type="event",
                to_id="evt_1",
                relationship="supports",
                weight=0.8,
            ),
            create_edge(
                from_type="record",
                from_id="rec_2",
                to_type="event",
                to_id="evt_1",
                relationship="extends",
                weight=0.6,
            ),
            create_edge(
                from_type="event",
                from_id="evt_1",
                to_type="quant",
                to_id="quant_1",
                relationship="quant_context",
                weight=0.9,
            ),
        ]

    def test_filters_by_relationship(self):
        """Should return only nodes with specified relationship."""
        result = get_related_nodes(
            self.edges,
            node_type="record",
            node_id="rec_1",
            relationship="supports",
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["to_id"], "evt_1")

    def test_returns_all_when_no_filter(self):
        """Should return all connected nodes when no relationship filter."""
        result = get_related_nodes(
            self.edges,
            node_type="record",
            node_id="rec_1",
            relationship=None,
        )
        # rec_1 connects to evt_1 via supports
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["to_id"], "evt_1")


class EdgeWeightTests(unittest.TestCase):
    """Tests for compute_edge_weight function."""

    def test_supports_uses_confidence(self):
        """Supports relationship should use cluster confidence."""
        record = {"id": "rec_1", "title": "Test"}
        cluster = {"event_id": "evt_1", "confidence": 0.75}

        weight = compute_edge_weight(record, cluster, "supports")
        self.assertEqual(weight, 0.75)

    def test_extends_uses_half_confidence(self):
        """Extends relationship should use half cluster confidence."""
        record = {"id": "rec_1", "title": "Test"}
        cluster = {"event_id": "evt_1", "confidence": 0.8}

        weight = compute_edge_weight(record, cluster, "extends")
        self.assertEqual(weight, 0.4)

    def test_quant_context_fixed(self):
        """Quant context relationship should have fixed high weight."""
        record = {"id": "rec_1", "title": "Test"}
        cluster = {"event_id": "evt_1", "confidence": 0.5}

        weight = compute_edge_weight(record, cluster, "quant_context")
        self.assertEqual(weight, 0.95)

    def test_contradicts_low_weight(self):
        """Contradicts relationship should have low weight."""
        record = {"id": "rec_1", "title": "Test"}
        cluster = {"event_id": "evt_1", "confidence": 0.9}

        weight = compute_edge_weight(record, cluster, "contradicts")
        self.assertEqual(weight, 0.3)

    def test_duplicate_theme_weight(self):
        """Duplicate theme should have threshold-based weight."""
        record = {"id": "rec_1", "title": "Test"}
        cluster = {"event_id": "evt_1", "confidence": 0.85}

        weight = compute_edge_weight(record, cluster, "duplicate_theme")
        # Should be around 0.7 for high confidence clusters
        self.assertGreater(weight, 0.6)
        self.assertLessEqual(weight, 1.0)


class CreateEdgesForRecordTests(unittest.TestCase):
    """Tests for create_edges_for_record function."""

    def test_creates_record_to_event_edge(self):
        """Should create record -> event edge with supports relationship."""
        record = {"id": "rec_1", "title": "Test Record"}
        cluster = {
            "event_id": "evt_1",
            "confidence": 0.8,
            "quant_links": [],
        }

        edges = create_edges_for_record(record, cluster)

        # Should have 1 edge: record -> event
        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0]["from_type"], "record")
        self.assertEqual(edges[0]["from_id"], "rec_1")
        self.assertEqual(edges[0]["to_type"], "event")
        self.assertEqual(edges[0]["to_id"], "evt_1")
        self.assertEqual(edges[0]["relationship"], "supports")

    def test_creates_quant_context_edge_when_linked(self):
        """Should create quant edges when record has linked_quant_context."""
        record = {
            "id": "rec_1",
            "title": "Test Record",
            "linked_quant_context": "quant_123",
        }
        cluster = {
            "event_id": "evt_1",
            "confidence": 0.8,
            "quant_links": [],
        }

        edges = create_edges_for_record(record, cluster)

        # Should have 2 edges: record -> event and record -> quant
        self.assertEqual(len(edges), 2)

        # Find the quant edge
        quant_edge = next(e for e in edges if e["to_type"] == "quant")
        self.assertEqual(quant_edge["from_type"], "record")
        self.assertEqual(quant_edge["from_id"], "rec_1")
        self.assertEqual(quant_edge["to_id"], "quant_123")
        self.assertEqual(quant_edge["relationship"], "quant_context")

    def test_creates_event_to_quant_edge_when_cluster_has_quant_links(self):
        """Should create event -> quant edges when cluster has quant_links."""
        record = {"id": "rec_1", "title": "Test Record"}
        cluster = {
            "event_id": "evt_1",
            "confidence": 0.8,
            "quant_links": ["quant_456", "quant_789"],
        }

        edges = create_edges_for_record(record, cluster)

        # Should have 3 edges: record -> event + event -> quant_456 + event -> quant_789
        self.assertEqual(len(edges), 3)

        # Find quant edges
        quant_edges = [e for e in edges if e["to_type"] == "quant"]
        self.assertEqual(len(quant_edges), 2)
        for qe in quant_edges:
            self.assertEqual(qe["from_type"], "event")
            self.assertEqual(qe["from_id"], "evt_1")
            self.assertEqual(qe["relationship"], "quant_context")

    def test_no_quant_edge_when_no_link(self):
        """Should not create quant edges when no quant links exist."""
        record = {"id": "rec_1", "title": "Test Record"}
        cluster = {
            "event_id": "evt_1",
            "confidence": 0.8,
            "quant_links": [],
        }

        edges = create_edges_for_record(record, cluster)

        # Should have only 1 edge: record -> event
        self.assertEqual(len(edges), 1)
        self.assertEqual(edges[0]["to_type"], "event")


class FindDuplicateThemesTests(unittest.TestCase):
    """Tests for find_duplicate_themes function."""

    def test_finds_duplicate_themes(self):
        """Should find clusters connected by duplicate_theme edges."""
        edges = [
            create_edge(
                from_type="event",
                from_id="evt_1",
                to_type="event",
                to_id="evt_2",
                relationship="duplicate_theme",
                weight=0.75,
            ),
            create_edge(
                from_type="event",
                from_id="evt_2",
                to_type="event",
                to_id="evt_3",
                relationship="duplicate_theme",
                weight=0.8,
            ),
        ]

        clusters = [
            {"event_id": "evt_1", "title": "Theme A"},
            {"event_id": "evt_2", "title": "Theme A Variant"},
            {"event_id": "evt_3", "title": "Theme B"},
        ]

        duplicates = find_duplicate_themes(edges, clusters, threshold=0.7)

        # Should find pairs above threshold (evt_1-evt_2 at 0.75, evt_2-evt_3 at 0.8)
        self.assertEqual(len(duplicates), 2)
        duplicate_pairs = [(d["from_id"], d["to_id"]) for d in duplicates]
        self.assertIn(("evt_1", "evt_2"), duplicate_pairs)
        self.assertIn(("evt_2", "evt_3"), duplicate_pairs)

    def test_respects_threshold(self):
        """Should not include duplicates below threshold."""
        edges = [
            create_edge(
                from_type="event",
                from_id="evt_1",
                to_type="event",
                to_id="evt_2",
                relationship="duplicate_theme",
                weight=0.5,  # Below 0.7 threshold
            ),
        ]

        clusters = [
            {"event_id": "evt_1", "title": "Theme A"},
            {"event_id": "evt_2", "title": "Theme A Variant"},
        ]

        duplicates = find_duplicate_themes(edges, clusters, threshold=0.7)

        self.assertEqual(len(duplicates), 0)


class LoadSaveEdgesTests(unittest.TestCase):
    """Tests for load_edges and save_edges functions."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.story_graph_dir = os.path.join(self.test_dir, "story_graph")
        os.makedirs(self.story_graph_dir)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_save_and_load_edges(self):
        """Should persist edges to file and load them back."""
        edges = [
            create_edge(
                from_type="record",
                from_id="rec_1",
                to_type="event",
                to_id="evt_1",
                relationship="supports",
                weight=0.8,
            ),
            create_edge(
                from_type="event",
                from_id="evt_1",
                to_type="quant",
                to_id="quant_1",
                relationship="quant_context",
                weight=0.95,
            ),
        ]

        # Save edges
        save_edges(edges, self.story_graph_dir)

        # Load edges
        loaded = load_edges(self.story_graph_dir)

        self.assertEqual(len(loaded), 2)
        self.assertEqual(loaded[0]["from_id"], "rec_1")
        self.assertEqual(loaded[1]["to_id"], "quant_1")

    def test_load_nonexistent_file(self):
        """Should return empty list when edges file doesn't exist."""
        loaded = load_edges(self.story_graph_dir)
        self.assertEqual(loaded, [])

    def test_atomic_write(self):
        """Save should use atomic write (temp + rename)."""
        edges = [
            create_edge(
                from_type="record",
                from_id="rec_1",
                to_type="event",
                to_id="evt_1",
                relationship="supports",
                weight=0.8,
            ),
        ]

        save_edges(edges, self.story_graph_dir)

        # Check that only the final file exists, not temp
        files = os.listdir(self.story_graph_dir)
        self.assertIn("edges.json", files)
        self.assertNotIn("edges.json.tmp", files)


class GetEventNarrativeTests(unittest.TestCase):
    """Tests for get_event_narrative function."""

    def setUp(self):
        self.edges = [
            create_edge(
                from_type="record",
                from_id="rec_1",
                to_type="event",
                to_id="evt_1",
                relationship="supports",
                weight=0.9,
            ),
            create_edge(
                from_type="record",
                from_id="rec_2",
                to_type="event",
                to_id="evt_1",
                relationship="supports",
                weight=0.7,
            ),
            create_edge(
                from_type="event",
                from_id="evt_1",
                to_type="quant",
                to_id="quant_1",
                relationship="quant_context",
                weight=0.95,
            ),
        ]

    def test_gets_event_narrative(self):
        """Should return narrative dict for event cluster."""
        result = get_event_narrative(self.edges, cluster_id="evt_1")

        self.assertIn("event_id", result)
        self.assertEqual(result["event_id"], "evt_1")
        self.assertIn("supporting_records", result)
        self.assertIn("quant_contexts", result)
        self.assertEqual(len(result["supporting_records"]), 2)
        self.assertEqual(len(result["quant_contexts"]), 1)

    def test_empty_narrative_for_nonexistent_event(self):
        """Should return empty narrative for event with no edges."""
        result = get_event_narrative(self.edges, cluster_id="evt_nonexistent")

        self.assertEqual(result["event_id"], "evt_nonexistent")
        self.assertEqual(result["supporting_records"], [])
        self.assertEqual(result["quant_contexts"], [])


if __name__ == "__main__":
    unittest.main()
