"""
Keyword Discovery Lane Orchestration Script.

Main orchestration script for the keyword discovery lane. Loads query config,
executes searches, builds candidates, and feeds them into the shared candidate pipeline.

Flow:
    load keyword_queries.json
    for each enabled query:
        call discovery provider (web or news based on query type)
        normalize results
        filter by blocked domains / required terms
        build candidate records
    shared dedupe → scoring → filtering → convert → process_record

Usage:
    python -m scripts.run_keyword_discovery [--dry-run]

Arguments:
    --dry-run    Run without executing searches or saving candidates
"""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any

# Base directory is the project root (two levels up from this file)
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from scripts.candidate_utils import (
    ensure_candidate_directories,
    update_lane_stats,
    CANDIDATES_DIR,
)
from scripts.dedupe_candidates import process_dedupe
from scripts.score_candidates import score_candidate, filter_by_score
from scripts.convert_candidates_to_raw import convert_candidates
from scripts.build_keyword_candidates import (
    build_keyword_candidates,
    save_keyword_candidates,
)
from scripts.discovery_providers import (
    search_web,
    search_news,
    search_preferred_domains,
)
from scripts.normalize_search_results import normalize_results


# Configuration paths
QUERY_CONFIG_PATH = BASE_DIR / "config" / "keyword_queries.json"

# Lane name for keyword discovery
LANE = "keyword_discovery"


# ============================================================================
# Query Configuration Loading
# ============================================================================


def load_query_config() -> dict[str, Any]:
    """
    Load keyword queries configuration from config/keyword_queries.json.

    Returns:
        Full configuration dictionary with 'queries' key containing list of query configs
    """
    if not QUERY_CONFIG_PATH.exists():
        raise FileNotFoundError(f"Query config not found: {QUERY_CONFIG_PATH}")

    import json

    with open(QUERY_CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================================
# Query Execution
# ============================================================================


def execute_query(query_config: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Execute a single keyword query using appropriate search provider.

    Uses:
    - search_preferred_domains if preferred_domains are specified
    - search_web for general queries
    - search_news for news-focused queries

    Args:
        query_config: Query configuration dict with query, max_results, etc.

    Returns:
        List of normalized search results
    """
    query = query_config.get("query", "")
    max_results = query_config.get("max_results", 10)
    preferred_domains = query_config.get("preferred_domains", [])

    if not query:
        return []

    try:
        # Use preferred_domains search if domains are specified
        if preferred_domains:
            results = search_preferred_domains(
                query=query,
                domains=preferred_domains,
                max_results=max_results,
            )
        else:
            # Fall back to general web search
            results = search_web(query=query, max_results=max_results)

        # Normalize results to standard schema
        normalized = normalize_results(results, provider="tavily")

        return normalized

    except Exception as e:
        print(f"[Discovery] Error executing query '{query}': {e}")
        return []


# ============================================================================
# Main Pipeline
# ============================================================================


def run_keyword_discovery(dry_run: bool = False) -> list[str]:
    """
    Main entry point for keyword discovery.

    Loads query config, executes enabled queries, builds candidates,
    and processes through the shared pipeline.

    Args:
        dry_run: If True, execute searches but don't save candidates

    Returns:
        List of candidate IDs discovered and processed
    """
    print(f"\n{'=' * 60}")
    print("Keyword Discovery Lane")
    print(f"{'=' * 60}")

    # Ensure directories exist
    ensure_candidate_directories()

    # Load query configuration
    config = load_query_config()
    queries = config.get("queries", [])

    if not queries:
        print("[Discovery] No queries found in configuration.")
        return []

    # Filter to enabled queries only
    enabled_queries = [q for q in queries if q.get("enabled", True)]

    if not enabled_queries:
        print("[Discovery] No enabled queries found.")
        return []

    print(f"[Discovery] Loaded {len(enabled_queries)} enabled queries")

    # Execute each query and collect candidates
    all_candidates: list[dict[str, Any]] = []
    discovered_ids: list[str] = []

    for query_config in enabled_queries:
        query_id = query_config.get("id", "unknown")
        query_text = query_config.get("query", "")
        topic = query_config.get("topic", "unknown")

        print(f"\n[Discovery] Executing query '{query_id}': {query_text[:50]}...")

        # Execute search
        results = execute_query(query_config)

        if not results:
            print(f"[Discovery] No results for query '{query_id}'")
            continue

        print(f"[Discovery] Got {len(results)} results for query '{query_id}'")

        # Build candidates from results
        candidates = build_keyword_candidates(
            results=results,
            query_config=query_config,
            lane=LANE,
        )

        if not candidates:
            print(f"[Discovery] No valid candidates from query '{query_id}'")
            continue

        print(f"[Discovery] Built {len(candidates)} candidates from query '{query_id}'")

        # Save candidates if not dry run
        if not dry_run:
            save_keyword_candidates(candidates)
            for candidate in candidates:
                discovered_ids.append(candidate["candidate_id"])
                all_candidates.append(candidate)
        else:
            print(f"[Discovery] [DRY RUN] Would save {len(candidates)} candidates")

    if not all_candidates:
        print("\n[Pipeline] No candidates discovered. Exiting.")
        return []

    print(f"\n{'=' * 60}")
    print(f"Keyword Discovery Summary: {len(all_candidates)} candidates")
    print(f"{'=' * 60}")

    # Update lane stats for discovered candidates
    if not dry_run:
        update_lane_stats(LANE, "discovered", len(all_candidates))

    # ========================================================================
    # Shared Pipeline: Dedupe → Scoring → Filtering → Convert → Process
    # ========================================================================

    if dry_run:
        print("\n[Pipeline] [DRY RUN] Skipping shared pipeline")
        return discovered_ids

    # Step 1: Dedupe
    print(f"\n[Pipeline] Step 1: Deduplicating {len(all_candidates)} candidates...")
    candidates, duplicates = process_dedupe(all_candidates, lane=LANE)
    dup_count = len(duplicates)
    print(f"[Dedup] {dup_count}/{len(all_candidates)} candidates were duplicates")

    if not candidates:
        print("[Pipeline] No candidates survived deduplication. Exiting.")
        return []

    # Step 2: Score
    print(f"\n[Pipeline] Step 2: Scoring {len(candidates)} candidates...")
    for candidate in candidates:
        candidate = score_candidate(candidate)
        score = candidate.get("candidate_scores", {}).get("total_score", 0)
        print(f"  - {candidate['candidate_id']}: score={score}")

    # Step 3: Filter by score
    print(f"\n[Pipeline] Step 3: Filtering by score...")
    threshold = 25
    candidates, filtered = filter_by_score(candidates, threshold=threshold)
    filtered_count = len(filtered)
    print(
        f"[Filter] {filtered_count} candidates did not pass score filter (threshold={threshold})"
    )

    if not candidates:
        print("[Pipeline] No candidates survived scoring filter. Exiting.")
        return []

    # Step 4: Convert to raw records
    print(f"\n[Pipeline] Step 4: Converting to raw records...")
    record_paths = convert_candidates(candidates)

    if not record_paths:
        print("[Pipeline] No records were created. Exiting.")
        return []

    update_lane_stats(LANE, "converted", len(record_paths))

    # Step 5: Process each record through process_record.py
    print(f"\n[Pipeline] Step 5: Processing {len(record_paths)} records...")
    record_ids = [p.stem for p in record_paths]

    for record_id in record_ids:
        print(f"  - Processing {record_id}...")
        try:
            result = subprocess.run(
                [sys.executable, "-m", "scripts.process_record", record_id],
                cwd=BASE_DIR,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                print(
                    f"    [Warning] process_record failed for {record_id}: {result.stderr}"
                )
            else:
                print(f"    [OK] Processed {record_id}")
        except Exception as e:
            print(f"    [Warning] Error processing {record_id}: {e}")

    print(f"\n{'=' * 60}")
    print(f"Keyword Discovery Pipeline Complete")
    print(f"Converted {len(record_ids)} candidates to raw records")
    print(f"{'=' * 60}\n")

    return discovered_ids


# ============================================================================
# CLI Entry Point
# ============================================================================


def main() -> None:
    """Main entry point for the keyword discovery script."""
    parser = argparse.ArgumentParser(
        description="Run keyword discovery lane orchestration."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Execute searches but don't save candidates or run pipeline",
    )

    args = parser.parse_args()

    try:
        discovered_ids = run_keyword_discovery(dry_run=args.dry_run)

        if args.dry_run:
            print(f"\n[DRY RUN] Would have discovered {len(discovered_ids)} candidates")

        print("\nKeyword discovery completed successfully.")
        sys.exit(0)

    except KeyboardInterrupt:
        print("\n[Pipeline] Interrupted by user. Exiting.")
        sys.exit(1)

    except Exception as e:
        print(f"\n[Pipeline] Error: {e}")
        raise


if __name__ == "__main__":
    main()
