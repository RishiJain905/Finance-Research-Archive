"""
Stream 3: Story Graph Update Script.

Manages story edges connecting records, events, quant data, and themes.
Called when records are clustered into event clusters.

Edge types:
- record → event (supports): Record confirms/supports an event
- record → quant (quant_context): Record is explained by quant data
- event → quant (quant_context): Event has quantitative context
- event → event (extends): Events build on each other
- event → event (contradicts): Events conflict
- event → event (duplicate_theme): Events cover same theme
"""

import json
import os
from pathlib import Path
from typing import Optional


# Valid node types and relationships per schema
VALID_NODE_TYPES = ["record", "event", "quant", "theme"]
VALID_RELATIONSHIPS = [
    "supports",
    "extends",
    "quant_context",
    "contradicts",
    "duplicate_theme",
]

# Default directories
DEFAULT_EVENTS_DIR = "data/events"
DEFAULT_STORY_GRAPH_DIR = "data/story_graph"


def load_edges(story_graph_dir: str = DEFAULT_STORY_GRAPH_DIR) -> list:
    """
    Load all existing story edges from story_graph_dir/edges.json.

    Args:
        story_graph_dir: Directory containing edges.json file

    Returns:
        List of edge dicts, empty list if file doesn't exist
    """
    edges_path = Path(story_graph_dir) / "edges.json"

    if not edges_path.exists():
        return []

    try:
        with open(edges_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return []


def save_edges(edges: list, story_graph_dir: str = DEFAULT_STORY_GRAPH_DIR) -> None:
    """
    Atomic write all edges to story_graph_dir/edges.json.

    Uses temp file + rename pattern for atomic writes.

    Args:
        edges: List of edge dicts to save
        story_graph_dir: Directory to write edges.json
    """
    story_path = Path(story_graph_dir)
    story_path.mkdir(parents=True, exist_ok=True)

    edges_path = story_path / "edges.json"
    temp_path = edges_path.with_suffix(".json.tmp")

    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(edges, f, indent=2, ensure_ascii=False)

    temp_path.replace(edges_path)


def create_edge(
    from_type: str,
    from_id: str,
    to_type: str,
    to_id: str,
    relationship: str,
    weight: float,
) -> dict:
    """
    Create new story edge dict.

    Args:
        from_type: Type of source node ('record' | 'event' | 'quant' | 'theme')
        from_id: ID of source node
        to_type: Type of target node ('record' | 'event' | 'quant' | 'theme')
        to_id: ID of target node
        relationship: Relationship type ('supports' | 'extends' | 'quant_context' | 'contradicts' | 'duplicate_theme')
        weight: Edge weight (0.0-1.0, higher = stronger relationship)

    Returns:
        Edge dict following story_edge schema
    """
    return {
        "from_type": from_type,
        "from_id": from_id,
        "to_type": to_type,
        "to_id": to_id,
        "relationship": relationship,
        "weight": weight,
    }


def edge_exists(
    edges: list,
    from_type: str,
    from_id: str,
    to_type: str,
    to_id: str,
    relationship: str,
) -> bool:
    """
    Check if edge already exists in the graph.

    Args:
        edges: List of existing edges
        from_type: Source node type
        from_id: Source node ID
        to_type: Target node type
        to_id: Target node ID
        relationship: Relationship type

    Returns:
        True if edge exists, False otherwise
    """
    for edge in edges:
        if (
            edge["from_type"] == from_type
            and edge["from_id"] == from_id
            and edge["to_type"] == to_type
            and edge["to_id"] == to_id
            and edge["relationship"] == relationship
        ):
            return True
    return False


def deduplicate_edges(new_edges: list, existing_edges: list) -> list:
    """
    Remove edges that already exist from new_edges.

    Args:
        new_edges: List of edges to filter
        existing_edges: List of edges that already exist

    Returns:
        List of edges from new_edges that don't already exist
    """
    result = []
    for new_edge in new_edges:
        if not edge_exists(
            existing_edges,
            from_type=new_edge["from_type"],
            from_id=new_edge["from_id"],
            to_type=new_edge["to_type"],
            to_id=new_edge["to_id"],
            relationship=new_edge["relationship"],
        ):
            result.append(new_edge)
    return result


def compute_edge_weight(record: dict, cluster: dict, relationship: str) -> float:
    """
    Calculate edge weight based on relationship type and context.

    Weight computation:
    - supports: uses cluster confidence directly
    - extends: half of cluster confidence (weaker link)
    - quant_context: fixed high weight (0.95)
    - contradicts: fixed low weight (0.3)
    - duplicate_theme: based on cluster confidence (threshold ~0.7)

    Args:
        record: Record dict
        cluster: Cluster dict with confidence score
        relationship: Relationship type

    Returns:
        Edge weight (0.0-1.0)
    """
    confidence = cluster.get("confidence", 0.5)

    if relationship == "supports":
        return confidence

    elif relationship == "extends":
        return confidence * 0.5

    elif relationship == "quant_context":
        return 0.95

    elif relationship == "contradicts":
        return 0.3

    elif relationship == "duplicate_theme":
        # Scale duplicate theme weight based on confidence
        # Higher confidence clusters get higher duplicate weights
        return 0.5 + (confidence * 0.4)  # Range: 0.5-0.9

    # Default fallback
    return 0.5


def create_edges_for_record(record: dict, cluster: dict) -> list:
    """
    Create story edges when a record is clustered.

    Creates:
    - record → event (relationship='supports', weight=cluster.confidence)
    - record → quant (relationship='quant_context') if record has linked_quant_context
    - event → quant (relationship='quant_context') if cluster has quant_links

    Args:
        record: Record dict with id, linked_quant_context
        cluster: Cluster dict with event_id, confidence, quant_links

    Returns:
        List of new edge dicts
    """
    edges = []
    record_id = record.get("id")
    event_id = cluster.get("event_id")

    if not record_id or not event_id:
        return edges

    # Create record → event edge (supports)
    record_to_event = create_edge(
        from_type="record",
        from_id=record_id,
        to_type="event",
        to_id=event_id,
        relationship="supports",
        weight=compute_edge_weight(record, cluster, "supports"),
    )
    edges.append(record_to_event)

    # Create record → quant edge if record has linked_quant_context
    linked_quant = record.get("linked_quant_context")
    if linked_quant:
        record_to_quant = create_edge(
            from_type="record",
            from_id=record_id,
            to_type="quant",
            to_id=linked_quant,
            relationship="quant_context",
            weight=compute_edge_weight(record, cluster, "quant_context"),
        )
        edges.append(record_to_quant)

    # Create event → quant edges if cluster has quant_links
    cluster_quant_links = cluster.get("quant_links", [])
    for quant_id in cluster_quant_links:
        event_to_quant = create_edge(
            from_type="event",
            from_id=event_id,
            to_type="quant",
            to_id=quant_id,
            relationship="quant_context",
            weight=compute_edge_weight(record, cluster, "quant_context"),
        )
        edges.append(event_to_quant)

    return edges


def get_edges_for_node(edges: list, node_type: str, node_id: str) -> list:
    """
    Get all edges where node is from_id or to_id.

    Args:
        edges: List of all edges
        node_type: Type of node ('record' | 'event' | 'quant' | 'theme')
        node_id: ID of node

    Returns:
        List of edges connected to the node
    """
    result = []
    for edge in edges:
        # Check if node is the source
        if edge["from_type"] == node_type and edge["from_id"] == node_id:
            result.append(edge)
        # Check if node is the target
        elif edge["to_type"] == node_type and edge["to_id"] == node_id:
            result.append(edge)
    return result


def get_related_nodes(
    edges: list,
    node_type: str,
    node_id: str,
    relationship: str = None,
) -> list:
    """
    Get all nodes connected to given node, optionally filtered by relationship.

    Args:
        edges: List of all edges
        node_type: Type of the source node
        node_id: ID of the source node
        relationship: Optional relationship type to filter by

    Returns:
        List of edge dicts for connected nodes
    """
    connected_edges = get_edges_for_node(edges, node_type, node_id)

    if relationship is None:
        return connected_edges

    return [e for e in connected_edges if e["relationship"] == relationship]


def get_event_narrative(edges: list, cluster_id: str) -> dict:
    """
    Get full narrative for an event cluster.

    Returns a dict with:
    - event_id: The cluster ID
    - supporting_records: List of records that support this event
    - quant_contexts: List of quant IDs providing context
    - extending_events: List of events that extend this one
    - contradicting_events: List of events that contradict this one
    - duplicate_themes: List of events with duplicate themes

    Args:
        edges: List of all edges
        cluster_id: Event cluster ID

    Returns:
        Narrative dict
    """
    narrative = {
        "event_id": cluster_id,
        "supporting_records": [],
        "quant_contexts": [],
        "extending_events": [],
        "contradicting_events": [],
        "duplicate_themes": [],
    }

    for edge in edges:
        # Get edges where this event is the target (supporting records)
        if edge["to_type"] == "event" and edge["to_id"] == cluster_id:
            if edge["from_type"] == "record" and edge["relationship"] == "supports":
                narrative["supporting_records"].append(edge["from_id"])

        # Get edges where this event is the source
        if edge["from_type"] == "event" and edge["from_id"] == cluster_id:
            if edge["relationship"] == "quant_context":
                narrative["quant_contexts"].append(edge["to_id"])
            elif edge["relationship"] == "extends":
                narrative["extending_events"].append(edge["to_id"])
            elif edge["relationship"] == "contradicts":
                narrative["contradicting_events"].append(edge["to_id"])
            elif edge["relationship"] == "duplicate_theme":
                narrative["duplicate_themes"].append(edge["to_id"])

    return narrative


def find_duplicate_themes(edges: list, clusters: list, threshold: float = 0.7) -> list:
    """
    Find clusters with duplicate_theme edges above weight threshold.

    Args:
        edges: List of all edges
        clusters: List of cluster dicts (for validation)
        threshold: Minimum weight to consider (default 0.7)

    Returns:
        List of duplicate_theme edge dicts above threshold
    """
    cluster_ids = {c.get("event_id") for c in clusters}

    duplicates = []
    for edge in edges:
        if edge["relationship"] == "duplicate_theme":
            # Validate both ends are actual clusters
            if (
                edge["from_type"] == "event"
                and edge["to_type"] == "event"
                and edge["from_id"] in cluster_ids
                and edge["to_id"] in cluster_ids
                and edge["weight"] >= threshold
            ):
                duplicates.append(edge)

    return duplicates


def update_graph_on_clustering(
    record_id: str,
    cluster_id: str,
    event_dir: str = DEFAULT_EVENTS_DIR,
    story_graph_dir: str = DEFAULT_STORY_GRAPH_DIR,
) -> None:
    """
    Called when a record is added to a cluster.

    Loads existing edges, creates new edges for the record-cluster relationship,
    and persists the updated graph.

    Args:
        record_id: ID of the record that was clustered
        cluster_id: ID of the event cluster
        event_dir: Directory containing event cluster files
        story_graph_dir: Directory for story graph edges.json
    """
    # Load the record
    record_path = Path(event_dir).parent / "accepted" / f"{record_id}.json"
    if not record_path.exists():
        # Try raw directory
        record_path = Path(event_dir).parent / "raw" / f"{record_id}.json"

    record = None
    if record_path.exists():
        try:
            with open(record_path, "r", encoding="utf-8") as f:
                record = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass

    if not record:
        # Create minimal record dict
        record = {"id": record_id}

    # Load the cluster
    cluster_path = Path(event_dir) / f"{cluster_id}.json"
    cluster = None
    if cluster_path.exists():
        try:
            with open(cluster_path, "r", encoding="utf-8") as f:
                cluster = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass

    if not cluster:
        # Create minimal cluster dict
        cluster = {"event_id": cluster_id, "confidence": 0.5, "quant_links": []}

    # Load existing edges
    existing_edges = load_edges(story_graph_dir)

    # Create new edges for this clustering
    new_edges = create_edges_for_record(record, cluster)

    # Remove duplicates
    edges_to_add = deduplicate_edges(new_edges, existing_edges)

    if edges_to_add:
        # Combine and save
        all_edges = existing_edges + edges_to_add
        save_edges(all_edges, story_graph_dir)


def main():
    """CLI entry point for story graph updates."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Update story graph when records are clustered"
    )
    parser.add_argument(
        "--events-dir",
        default=DEFAULT_EVENTS_DIR,
        help=f"Directory containing event clusters (default: {DEFAULT_EVENTS_DIR})",
    )
    parser.add_argument(
        "--story-graph-dir",
        default=DEFAULT_STORY_GRAPH_DIR,
        help=f"Directory for story graph edges (default: {DEFAULT_STORY_GRAPH_DIR})",
    )
    parser.add_argument(
        "--record-id",
        help="Process a specific record by ID",
    )
    parser.add_argument(
        "--cluster-id",
        help="Cluster ID to update (required with --record-id)",
    )

    args = parser.parse_args()

    if args.record_id and args.cluster_id:
        update_graph_on_clustering(
            record_id=args.record_id,
            cluster_id=args.cluster_id,
            event_dir=args.events_dir,
            story_graph_dir=args.story_graph_dir,
        )
        print(f"Updated graph: record {args.record_id} -> cluster {args.cluster_id}")
    elif args.record_id or args.cluster_id:
        parser.error("Both --record-id and --cluster-id are required together")
    else:
        # Just load and display current graph stats
        edges = load_edges(args.story_graph_dir)
        print(f"Story graph has {len(edges)} edges")
        print(f"Edges file: {args.story_graph_dir}/edges.json")


if __name__ == "__main__":
    main()
