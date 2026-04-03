"""
Stream 2: Event Clustering Records Script.

Clusters accepted records into event clusters based on:
- Time proximity (same day = 100, ±1 day = 80, ±3 days = 60, ±7 days = 40)
- Topic/tag overlap (exact = 50, partial = 30, none = 0)
- Phrase/text overlap (token intersection ratio, 0-40)
- Source diversity (1 domain = 0, 2 = 8, 3+ = 15)
- Quant support (has quant link = 30, else 0)

Combined score uses weighted sum per config weight_overrides.
Clusters are promoted to 'stable' when reaching stable_threshold (5) records.
"""

import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# Default paths
DEFAULT_ACCEPTED_DIR = "data/accepted"
DEFAULT_EVENTS_DIR = "data/events"
DEFAULT_CONFIG_PATH = "config/clustering_rules.json"


def load_config(config_path: str = DEFAULT_CONFIG_PATH) -> dict:
    """Load clustering configuration."""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def compute_time_proximity_score(date1: str, date2: str, config: dict) -> float:
    """
    Compute time proximity score between two dates.

    Scoring:
    - Same day: 100
    - ±1 day: 80
    - ±3 days: 60
    - ±7 days: 40
    - Beyond 7 days: 0

    Args:
        date1: First date string (YYYY-MM-DD format)
        date2: Second date string (YYYY-MM-DD format)
        config: Clustering configuration dict

    Returns:
        Similarity score (0-100)
    """
    try:
        d1 = datetime.strptime(date1, "%Y-%m-%d")
        d2 = datetime.strptime(date2, "%Y-%m-%d")
    except (ValueError, TypeError):
        return 0

    days_diff = abs((d1 - d2).days)

    if days_diff == 0:
        return 100
    elif days_diff == 1:
        return 80
    elif days_diff <= 3:
        return 60
    elif days_diff <= 7:
        return 40
    else:
        return 0


def compute_topic_overlap_score(tags1: list, tags2: list) -> float:
    """
    Compute topic/tag overlap score between two tag lists.

    Scoring:
    - Exact match (all tags same): 50
    - Partial overlap: 30
    - No overlap: 0

    Args:
        tags1: First list of tags
        tags2: Second list of tags

    Returns:
        Similarity score (0-50)
    """
    if not tags1 or not tags2:
        return 0

    set1 = set(tags1)
    set2 = set(tags2)

    intersection = set1 & set2
    union = set1 | set2

    if not intersection:
        return 0

    # Check for exact match
    if set1 == set2:
        return 50

    # Partial overlap - scale based on Jaccard similarity
    jaccard = len(intersection) / len(union) if union else 0

    # Return partial score (30 * jaccard similarity, but at least some overlap)
    return 30


def compute_phrase_overlap_score(text1: str, text2: str) -> float:
    """
    Compute phrase/text overlap score using token intersection.

    Uses tokenized word intersection ratio scaled to max 40 points.

    Args:
        text1: First text string
        text2: Second text string

    Returns:
        Similarity score (0-40)
    """
    if not text1 or not text2:
        return 0

    # Tokenize and normalize (lowercase, extract alphanumeric tokens)
    tokens1 = set(re.findall(r"\b[a-zA-Z0-9]+\b", text1.lower()))
    tokens2 = set(re.findall(r"\b[a-zA-Z0-9]+\b", text2.lower()))

    if not tokens1 or not tokens2:
        return 0

    intersection = tokens1 & tokens2
    union = tokens1 | tokens2

    if not intersection:
        return 0

    # Jaccard similarity * max score (40)
    jaccard = len(intersection) / len(union) if union else 0
    return round(jaccard * 40, 2)


def compute_source_diversity_score(domains: list) -> float:
    """
    Compute source diversity score based on number of unique domains.

    Scoring:
    - 1 domain: 0
    - 2 domains: 8
    - 3+ domains: 15

    Args:
        domains: List of source domain strings

    Returns:
        Diversity score (0-15)
    """
    if not domains:
        return 0

    unique_domains = len(set(domains))

    if unique_domains == 1:
        return 0
    elif unique_domains == 2:
        return 8
    else:  # 3 or more
        return 15


def compute_quant_support_score(quant_links: list) -> float:
    """
    Compute quant support score based on presence of quant links.

    Scoring:
    - Has quant links: 30
    - No quant links: 0

    Args:
        quant_links: List of quant record IDs linked to the record

    Returns:
        Support score (0 or 30)
    """
    if quant_links and len(quant_links) > 0:
        return 30
    return 0


def compute_combined_similarity(record: dict, cluster: dict, config: dict) -> float:
    """
    Compute weighted combined similarity score between record and cluster.

    Uses weights from config:
    - topic_compatibility: 0.30
    - phrase_overlap: 0.25
    - time_proximity: 0.20
    - source_diversity: 0.15
    - quant_support: 0.10

    Args:
        record: Record dict with source, tags, quant_links, etc.
        cluster: Cluster dict with source_domains, keywords, created_at, etc.
        config: Clustering configuration

    Returns:
        Combined similarity score (0-100 scale before weighting, resulting in 0-100)
    """
    weights = config.get("weight_overrides", {})

    # Time proximity score
    record_date = record.get("source", {}).get("published_at", "")[:10]  # YYYY-MM-DD
    cluster_date = cluster.get("created_at", "")[:10]
    time_score = compute_time_proximity_score(record_date, cluster_date, config)

    # Topic overlap score
    record_tags = record.get("tags", [])
    cluster_keywords = cluster.get("keywords", [])
    topic_score = compute_topic_overlap_score(record_tags, cluster_keywords)

    # Phrase overlap score (using title and summary)
    record_text = f"{record.get('title', '')} {record.get('summary', '')}"
    cluster_text = f"{cluster.get('title', '')} {cluster.get('summary', '')}"
    phrase_score = compute_phrase_overlap_score(record_text, cluster_text)

    # Source diversity score
    record_domain = record.get("source", {}).get("domain", "")
    cluster_domains = cluster.get("source_domains", [])
    all_domains = (
        [record_domain] + cluster_domains if record_domain else cluster_domains
    )
    diversity_score = compute_source_diversity_score(all_domains)

    # Quant support score
    record_quant = record.get("quant_links", [])
    quant_score = compute_quant_support_score(record_quant)

    # Weighted sum
    combined = (
        time_score * weights.get("time_proximity", 0.20)
        + topic_score * weights.get("topic_compatibility", 0.30)
        + phrase_score * weights.get("phrase_overlap", 0.25)
        + diversity_score * weights.get("source_diversity", 0.15)
        + quant_score * weights.get("quant_support", 0.10)
    )

    return round(combined, 2)


def load_clusters(event_dir: str = DEFAULT_EVENTS_DIR) -> list:
    """
    Load all clusters from event_dir/*.json files.

    Args:
        event_dir: Directory containing cluster JSON files

    Returns:
        List of cluster dicts
    """
    clusters = []
    event_path = Path(event_dir)

    if not event_path.exists():
        return clusters

    for cluster_file in event_path.glob("*.json"):
        try:
            with open(cluster_file, "r", encoding="utf-8") as f:
                cluster = json.load(f)
                clusters.append(cluster)
        except (json.JSONDecodeError, IOError):
            continue

    return clusters


def save_cluster(cluster: dict, event_dir: str = DEFAULT_EVENTS_DIR) -> None:
    """
    Atomically write cluster to event_dir/{event_id}.json.

    Args:
        cluster: Cluster dict to save
        event_dir: Directory to save cluster files
    """
    event_path = Path(event_dir)
    event_path.mkdir(parents=True, exist_ok=True)

    event_id = cluster.get("event_id", cluster.get("id", str(uuid.uuid4())))
    file_path = event_path / f"{event_id}.json"

    # Atomic write using temp file + rename
    temp_path = file_path.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(cluster, f, indent=2, ensure_ascii=False)

    temp_path.replace(file_path)


def generate_cluster_title(cluster: dict, record: dict) -> str:
    """
    Generate deterministic cluster title.

    Title format: top_keywords + dominant event_type + source title

    Args:
        cluster: Cluster dict with keywords and event_type
        record: Record dict with title and source

    Returns:
        Generated title string
    """
    keywords = cluster.get("keywords", [])
    event_type = cluster.get("event_type", "")
    source_title = record.get("title", "")
    source_name = record.get("source", {}).get("name", "")

    # Take top 2-3 most significant keywords
    top_keywords = keywords[:3] if keywords else []

    # Build title components
    title_parts = []

    # Add keywords (if available)
    if top_keywords:
        keyword_str = " / ".join(top_keywords).replace("-", " ")
        title_parts.append(keyword_str)

    # Add event type
    if event_type:
        title_parts.append(event_type.replace("_", " ").title())

    # Add source title if still short
    if source_title and len(title_parts) < 3:
        # Truncate source title if too long
        truncated_title = (
            source_title[:60] + "..." if len(source_title) > 60 else source_title
        )
        title_parts.append(truncated_title)

    if not title_parts:
        title_parts = ["Uncategorized Event"]

    return " | ".join(title_parts)


def create_new_cluster(record: dict, config: dict, event_dir: str) -> dict:
    """
    Create a new cluster from a record.

    Args:
        record: Record dict to create cluster from
        config: Clustering configuration
        event_dir: Directory for cluster files

    Returns:
        New cluster dict with status='open', confidence=1.0
    """
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    event_id = f"event_{record.get('id', str(uuid.uuid4()))[:32]}"

    cluster = {
        "event_id": event_id,
        "title": record.get("title", "Untitled Event"),
        "topic": record.get("topic", "general"),
        "event_type": record.get("event_type", "unknown"),
        "summary": record.get("summary", ""),
        "status": "open",
        "created_at": now,
        "updated_at": now,
        "record_ids": [record.get("id")],
        "source_domains": [record.get("source", {}).get("domain", "")],
        "keywords": record.get("tags", []).copy(),
        "quant_links": record.get("quant_links", []).copy()
        if record.get("quant_links")
        else [],
        "confidence": 1.0,
    }

    # Generate a better title
    cluster["title"] = generate_cluster_title(cluster, record)

    # Save the cluster
    save_cluster(cluster, event_dir)

    return cluster


def attach_to_cluster(record: dict, cluster: dict, event_dir: str) -> None:
    """
    Attach a record to an existing cluster.

    Updates cluster record_ids, keywords, and source_domains.

    Args:
        record: Record dict to attach
        cluster: Cluster dict to attach to
        event_dir: Directory for cluster files
    """
    record_id = record.get("id")
    if not record_id:
        return

    # Add record ID if not already present
    if record_id not in cluster.get("record_ids", []):
        cluster["record_ids"].append(record_id)

    # Merge source domains (deduplicate)
    record_domain = record.get("source", {}).get("domain", "")
    if record_domain:
        existing_domains = set(cluster.get("source_domains", []))
        existing_domains.add(record_domain)
        cluster["source_domains"] = list(existing_domains)

    # Merge keywords (deduplicate)
    record_tags = set(record.get("tags", []))
    existing_keywords = set(cluster.get("keywords", []))
    combined_keywords = existing_keywords | record_tags
    cluster["keywords"] = list(combined_keywords)

    # Merge quant links if present
    record_quant = record.get("quant_links", [])
    if record_quant:
        existing_quant = set(cluster.get("quant_links", []))
        existing_quant.update(record_quant)
        cluster["quant_links"] = list(existing_quant)

    # Update timestamp
    cluster["updated_at"] = (
        datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )

    # Save updated cluster
    save_cluster(cluster, event_dir)


def should_promote_to_stable(cluster: dict, config: dict) -> bool:
    """
    Determine if cluster should be promoted to stable status.

    Promotion occurs when record count >= stable_threshold (5).

    Args:
        cluster: Cluster dict
        config: Clustering configuration

    Returns:
        True if cluster should be promoted, False otherwise
    """
    stable_threshold = config.get("stable_threshold", 5)
    current_status = cluster.get("status", "open")

    # Don't re-promote already stable or archived clusters
    if current_status != "open":
        return False

    record_count = len(cluster.get("record_ids", []))
    return record_count >= stable_threshold


def find_recent_clusters(
    record: dict, clusters: list, config: dict, lookback_days: int = 7
) -> list:
    """
    Find clusters within time window that exceed similarity threshold.

    Args:
        record: Record to match against
        clusters: List of existing clusters
        config: Clustering configuration
        lookback_days: Number of days to look back

    Returns:
        List of matching clusters with similarity scores
    """
    similarity_threshold = config.get("similarity_threshold", 60)

    record_date_str = record.get("source", {}).get("published_at", "")
    if record_date_str:
        try:
            record_date = datetime.strptime(record_date_str[:10], "%Y-%m-%d")
        except ValueError:
            record_date = datetime.now(timezone.utc)
    else:
        record_date = datetime.now(timezone.utc)

    matching_clusters = []

    for cluster in clusters:
        # Skip archived clusters
        if cluster.get("status") == "archived":
            continue

        # Compute time proximity first as a quick filter
        cluster_date_str = cluster.get("created_at", "")
        if cluster_date_str:
            try:
                cluster_date = datetime.strptime(cluster_date_str[:10], "%Y-%m-%d")
            except ValueError:
                cluster_date = datetime.now(timezone.utc)
        else:
            cluster_date = datetime.now(timezone.utc)

        days_diff = abs((record_date - cluster_date).days)

        # Skip if outside lookback window
        if days_diff > lookback_days:
            continue

        # Compute full similarity score
        similarity = compute_combined_similarity(record, cluster, config)

        if similarity >= similarity_threshold:
            matching_clusters.append((cluster, similarity))

    # Sort by similarity descending
    matching_clusters.sort(key=lambda x: x[1], reverse=True)

    return [c[0] for c in matching_clusters]


def process_accepted_records(lookback_days: int = 7) -> None:
    """
    Main entry point - process accepted records and cluster them.

    Reads accepted records, finds/creates clusters, and persists changes.

    Args:
        lookback_days: Number of days to look back for existing clusters
    """
    config = load_config()

    accepted_dir = DEFAULT_ACCEPTED_DIR
    event_dir = DEFAULT_EVENTS_DIR

    # Ensure event directory exists
    Path(event_dir).mkdir(parents=True, exist_ok=True)

    # Load existing clusters
    existing_clusters = load_clusters(event_dir)

    # Get accepted records
    accepted_path = Path(accepted_dir)
    if not accepted_path.exists():
        return

    processed_count = 0

    for accepted_file in accepted_path.glob("*.json"):
        try:
            with open(accepted_file, "r", encoding="utf-8") as f:
                record = json.load(f)

            # Skip non-accepted records
            if record.get("status") != "accepted":
                continue

            # Find recent matching clusters
            matching_clusters = find_recent_clusters(
                record, existing_clusters, config, lookback_days
            )

            if matching_clusters:
                # Attach to best matching cluster
                best_cluster = matching_clusters[0]
                attach_to_cluster(record, best_cluster, event_dir)

                # Check if should promote to stable
                if should_promote_to_stable(best_cluster, config):
                    best_cluster["status"] = "stable"
                    save_cluster(best_cluster, event_dir)

                # Update in-memory list
                for i, c in enumerate(existing_clusters):
                    if c["event_id"] == best_cluster["event_id"]:
                        existing_clusters[i] = best_cluster
                        break
            else:
                # Create new cluster
                new_cluster = create_new_cluster(record, config, event_dir)
                existing_clusters.append(new_cluster)

            processed_count += 1

        except (json.JSONDecodeError, IOError, KeyError):
            continue

    return processed_count


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Cluster accepted records into event clusters"
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Number of days to look back for existing clusters (default: 7)",
    )
    args = parser.parse_args()

    count = process_accepted_records(args.lookback_days)
    if count is not None:
        print(f"Processed {count} accepted records")


if __name__ == "__main__":
    main()
