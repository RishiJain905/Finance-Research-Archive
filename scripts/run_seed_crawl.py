"""
Seed Site Crawling Lane Orchestration Script.

Main orchestration script for the seed site crawling lane. Loads seed site
configuration, crawls each enabled seed, and feeds discovered candidates
into the shared candidate pipeline.

Flow:
    load seed_sites.json
    for each enabled seed:
        crawl via crawl_seed_site (or mock if not available)
        save candidates to data/candidates/discovered/
        update lane stats
    shared dedupe → scoring → filtering → convert → process_record

Usage:
    python -m scripts.run_seed_crawl [--dry-run]

Arguments:
    --dry-run    Run without executing crawls or saving candidates
"""

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Base directory is the project root (two levels up from this file)
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from scripts.candidate_utils import (
    ensure_candidate_directories,
    update_lane_stats,
    save_candidate_json,
    generate_candidate_id,
    hash_url,
    CANDIDATES_DIR,
    CANDIDATES_DISCOVERED_DIR,
    DATA_DIR,
)

# Try to import shared pipeline modules
try:
    from scripts.dedupe_candidates import process_dedupe
    from scripts.score_candidates import score_candidate, filter_by_score
    from scripts.convert_candidates_to_raw import convert_candidates
    from scripts.build_keyword_candidates import fetch_candidate_contents
except ImportError as e:
    print(f"[Warning] Could not import shared pipeline modules: {e}")
    process_dedupe = None
    score_candidate = None
    filter_by_score = None
    convert_candidates = None
    fetch_candidate_contents = None

# Configuration paths
SEED_SITES_CONFIG_PATH = BASE_DIR / "config" / "seed_sites.json"

# Lane name for seed crawl
LANE = "seed_crawl"

# Score threshold for seed crawl (stricter than keyword discovery)
SEED_CRAWL_SCORE_THRESHOLD = 25


# ============================================================================
# Configuration Loading and Validation
# ============================================================================


def load_seed_config(config_path: Optional[Path] = None) -> dict[str, Any]:
    """
    Load seed sites configuration from config/seed_sites.json.

    Args:
        config_path: Optional path to config file (defaults to SEED_SITES_CONFIG_PATH)

    Returns:
        Full configuration dictionary with 'seeds' key containing list of seed configs

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config file contains invalid JSON
    """
    path = config_path or SEED_SITES_CONFIG_PATH

    if not path.exists():
        raise FileNotFoundError(f"Seed sites config not found: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in seed config: {e}")


def validate_seed_config(config: dict[str, Any]) -> list[str]:
    """
    Validate seed sites configuration structure.

    Args:
        config: Configuration dictionary to validate

    Returns:
        List of error messages (empty if valid)
    """
    errors = []

    # Check for 'seeds' key
    if "seeds" not in config:
        errors.append("Missing required 'seeds' key in configuration")
        return errors

    seeds = config.get("seeds", [])

    if not isinstance(seeds, list):
        errors.append("'seeds' must be a list")
        return errors

    # Validate each seed
    for i, seed in enumerate(seeds):
        seed_errors = _validate_single_seed(seed, i)
        errors.extend(seed_errors)

    return errors


def _validate_single_seed(seed: dict[str, Any], index: int) -> list[str]:
    """
    Validate a single seed entry.

    Args:
        seed: Seed configuration dictionary
        index: Index of seed in list (for error messages)

    Returns:
        List of error messages for this seed
    """
    errors = []
    prefix = f"Seed[{index}]"

    # Required fields
    required_fields = ["id", "domain", "start_urls"]
    for field in required_fields:
        if field not in seed or not seed[field]:
            errors.append(f"{prefix}: Missing required field '{field}'")

    # Validate domain format if present
    domain = seed.get("domain", "")
    if domain:
        if not _is_valid_domain(domain):
            errors.append(f"{prefix}: Invalid domain format '{domain}'")

    # Validate URLs if present
    start_urls = seed.get("start_urls", [])
    if isinstance(start_urls, list):
        for url in start_urls:
            if not _is_valid_url(url):
                errors.append(f"{prefix}: Invalid URL '{url}'")
    elif start_urls:  # Not empty but not a list
        errors.append(f"{prefix}: 'start_urls' must be a list")

    # Validate trust_tier if present
    trust_tier = seed.get("trust_tier", "")
    if trust_tier and trust_tier not in ["high", "medium", "low"]:
        errors.append(
            f"{prefix}: Invalid trust_tier '{trust_tier}' (must be high, medium, or low)"
        )

    return errors


def _is_valid_domain(domain: str) -> bool:
    """Check if domain has valid format."""
    if not domain:
        return False
    # Basic domain validation - contains at least one dot and no spaces
    pattern = r"^[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?)+$"
    return bool(re.match(pattern, domain))


def _is_valid_url(url: str) -> bool:
    """Check if URL has valid format."""
    if not url:
        return False
    # Basic URL validation
    pattern = r"^https?://[a-zA-Z0-9]([a-zA-Z0-9\-\.]*[a-zA-Z0-9])?(:[0-9]+)?(/.*)?$"
    return bool(re.match(pattern, url))


# ============================================================================
# Seed Crawling
# ============================================================================


def crawl_seed_site(seed: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Crawl a single seed site and return discovered candidates.

    Args:
        seed: Seed configuration dictionary with id, domain, start_urls, etc.

    Returns:
        List of discovered candidate dictionaries
    """
    seed_id = seed.get("id", "unknown")
    domain = seed.get("domain", "")
    start_urls = seed.get("start_urls", [])
    topic = seed.get("topic", "unknown")
    trust_tier = seed.get("trust_tier", "low")

    print(f"\n[Crawl] Starting crawl for seed '{seed_id}' ({domain})")

    # Try to import crawl_seed from Stream A
    try:
        from scripts.crawl_seed_site import crawl_seed

        print(f"[Crawl] Using Stream A crawl_seed")
        candidates = crawl_seed(seed)
        return candidates

    except ImportError:
        # Stream A not available - use synthetic data for testing
        print(f"[Crawl] crawl_seed_site not available, using mock data")
        return _create_mock_candidates(seed)


def _create_mock_candidates(seed: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Create mock candidates for testing when Stream A is not available.

    Args:
        seed: Seed configuration dictionary

    Returns:
        List of mock candidate dictionaries
    """
    seed_id = seed.get("id", "unknown")
    domain = seed.get("domain", "")
    topic = seed.get("topic", "unknown")
    trust_tier = seed.get("trust_tier", "low")

    discovered_at = datetime.now(timezone.utc).isoformat()

    # Create 1-3 mock candidates per seed
    mock_candidates = []
    for i in range(3):
        url = f"https://{domain}/mock-page-{i}"
        title = f"Mock Document {i} from {seed_id}"

        candidate_id = generate_candidate_id(
            lane=LANE,
            domain=domain,
            title=title,
            url=url,
        )

        candidate = {
            "candidate_id": candidate_id,
            "lane": LANE,
            "discovered_at": discovered_at,
            "topic": topic,
            "source": {
                "domain": domain,
                "source_name": domain.replace("www.", "").title(),
                "url": url,
                "discovery_url": url,
                "discovery_method": "crawl",
                "trust_tier": trust_tier,
            },
            "title": title,
            "anchor_text": title,
            "raw_html_path": "",
            "raw_text_path": "",
            "metadata": {
                "http_status": 200,
                "content_type": "text/html",
                "published_at": discovered_at,
                "language": "en",
                "word_count": 1000,
            },
            "dedupe": {
                "url_hash": hash_url(url),
                "normalized_title_hash": "",
                "content_hash": "",
            },
            "status": "discovered",
            "notes": f"Mock candidate from seed '{seed_id}' (Stream A not available)",
        }

        mock_candidates.append(candidate)

    return mock_candidates


# ============================================================================
# Main Pipeline
# ============================================================================


def run_seed_crawl(dry_run: bool = False) -> list[str]:
    """
    Main entry point for seed site crawling.

    Loads seed config, crawls enabled seeds, builds candidates,
    and processes through the shared pipeline.

    Args:
        dry_run: If True, execute crawls but don't save candidates or run pipeline

    Returns:
        List of candidate IDs discovered and processed
    """
    print(f"\n{'=' * 60}")
    print("Seed Site Crawling Lane")
    print(f"{'=' * 60}")

    # Ensure directories exist
    ensure_candidate_directories()

    # Load seed configuration
    try:
        config = load_seed_config()
    except FileNotFoundError as e:
        print(f"[Config] {e}")
        print("[Config] Seed crawl requires config/seed_sites.json from Stream A")
        print("[Config] Creating minimal config for testing...")
        config = {"seeds": []}

    # Validate configuration
    errors = validate_seed_config(config)
    if errors:
        print(f"[Config] Configuration errors found:")
        for error in errors:
            print(f"  - {error}")
        print("[Config] Proceeding with valid seeds only...")

    seeds = config.get("seeds", [])

    # Filter to enabled seeds only
    enabled_seeds = [s for s in seeds if s.get("enabled", True)]

    if not enabled_seeds:
        print("[Discovery] No enabled seeds found in configuration.")
        return []

    print(f"[Discovery] Loaded {len(enabled_seeds)} enabled seeds")

    # Initialize stats
    total_stats = {
        "pages_visited": 0,
        "links_scored": 0,
        "links_enqueued": 0,
        "candidates_emitted": 0,
        "deduped_out": 0,
        "filtered_out": 0,
        "converted": 0,
        "accepted": 0,
        "rejected": 0,
    }

    # Execute crawl for each seed
    all_candidates: list[dict[str, Any]] = []
    discovered_ids: list[str] = []

    for seed in enabled_seeds:
        seed_id = seed.get("id", "unknown")

        try:
            # Crawl seed site
            candidates = crawl_seed_site(seed)

            # Save candidates if not dry run
            if not dry_run:
                for candidate in candidates:
                    save_candidate_json(
                        candidate,
                        CANDIDATES_DISCOVERED_DIR / f"{candidate['candidate_id']}.json",
                    )
                    discovered_ids.append(candidate["candidate_id"])
                    all_candidates.append(candidate)

                    # Update stats
                    total_stats["candidates_emitted"] += 1

            else:
                print(f"[Discovery] [DRY RUN] Would save {len(candidates)} candidates")

            print(f"[Discovery] Crawled seed '{seed_id}': {len(candidates)} candidates")

        except Exception as e:
            print(f"[Discovery] Error crawling seed '{seed_id}': {e}")
            continue

    if not all_candidates:
        print("\n[Pipeline] No candidates discovered. Exiting.")
        _save_stats(total_stats)
        return []

    print(f"\n{'=' * 60}")
    print(f"Seed Crawl Summary: {len(all_candidates)} candidates")
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

    if process_dedupe is None:
        print("[Pipeline] Shared dedupe module not available. Skipping dedup.")
        deduped_candidates = all_candidates
        duplicates = []
    else:
        # Step 1: Dedupe
        print(f"\n[Pipeline] Step 1: Deduplicating {len(all_candidates)} candidates...")
        deduped_candidates, duplicates = process_dedupe(all_candidates, lane=LANE)
        dup_count = len(duplicates)
        total_stats["deduped_out"] = dup_count
        print(f"[Dedup] {dup_count}/{len(all_candidates)} candidates were duplicates")

    if not deduped_candidates:
        print("[Pipeline] No candidates survived deduplication. Exiting.")
        _save_stats(total_stats)
        return []

    if score_candidate is None:
        print("[Pipeline] Shared scoring module not available. Skipping scoring.")
        scored_candidates = deduped_candidates
    else:
        # Step 2: Score
        print(f"\n[Pipeline] Step 2: Scoring {len(deduped_candidates)} candidates...")
        scored_candidates = []
        for candidate in deduped_candidates:
            scored = score_candidate(candidate)
            score = scored.get("candidate_scores", {}).get("total_score", 0)
            print(f"  - {scored['candidate_id']}: score={score}")
            scored_candidates.append(scored)

    if filter_by_score is None:
        print("[Pipeline] Shared filtering module not available. Skipping filter.")
        filtered_candidates = scored_candidates
        filtered_out = []
    else:
        # Step 3: Filter by score
        print(
            f"\n[Pipeline] Step 3: Filtering by score (threshold={SEED_CRAWL_SCORE_THRESHOLD})..."
        )
        filtered_candidates, filtered_out = filter_by_score(
            scored_candidates, threshold=SEED_CRAWL_SCORE_THRESHOLD
        )
        total_stats["filtered_out"] = len(filtered_out)
        print(
            f"[Filter] {len(filtered_out)} candidates did not pass score filter "
            f"(threshold={SEED_CRAWL_SCORE_THRESHOLD})"
        )

    if not filtered_candidates:
        print("[Pipeline] No candidates survived scoring filter. Exiting.")
        _save_stats(total_stats)
        return []

    total_stats["accepted"] = len(filtered_candidates)
    total_stats["rejected"] = len(filtered_out)

    # Step 3.5: Fetch content for candidates
    if fetch_candidate_contents is None:
        print(
            "[Pipeline] fetch_candidate_contents not available. Skipping content fetch."
        )
        fetched_candidates = filtered_candidates
        failed_candidates = []
    else:
        print(
            f"\n[Pipeline] Step 3.5: Fetching content for {len(filtered_candidates)} candidates..."
        )
        fetched_candidates, failed_candidates = fetch_candidate_contents(
            filtered_candidates
        )
        if failed_candidates:
            print(
                f"[Pipeline] Failed to fetch content for {len(failed_candidates)} candidates"
            )
        print(f"[Pipeline] Fetched content for {len(fetched_candidates)} candidates")

    if not fetched_candidates:
        print("[Pipeline] No candidates had content fetched. Exiting.")
        _save_stats(total_stats)
        return []

    if convert_candidates is None:
        print("[Pipeline] Shared convert module not available. Skipping convert.")
        _save_stats(total_stats)
        return discovered_ids

    # Step 4: Convert to raw records
    print(f"\n[Pipeline] Step 4: Converting to raw records...")
    record_paths = convert_candidates(fetched_candidates)

    if not record_paths:
        print("[Pipeline] No records were created. Exiting.")
        _save_stats(total_stats)
        return []

    total_stats["converted"] = len(record_paths)
    update_lane_stats(LANE, "converted", len(record_paths))

    # Step 5: Process each record through the minimax cycle
    # (summarize → verify → route to accepted / review_queue / rejected)
    print(f"\n[Pipeline] Step 5: Running minimax cycle for {len(record_paths)} records...")
    record_ids = [p.stem for p in record_paths]

    routing_counts: dict[str, int] = {"accepted": 0, "rejected": 0, "review_queue": 0, "failed": 0}
    PROCESS_RECORD_TIMEOUT = 300  # 5 minutes per record

    for idx, record_id in enumerate(record_ids, 1):
        print(f"  [{idx}/{len(record_ids)}] Processing {record_id}...")
        try:
            result = subprocess.run(
                [sys.executable, "-m", "scripts.process_record", record_id],
                cwd=BASE_DIR,
                capture_output=True,
                text=True,
                timeout=PROCESS_RECORD_TIMEOUT,
            )
            if result.returncode != 0:
                routing_counts["failed"] += 1
                stderr_preview = (result.stderr or "").strip().splitlines()
                err_line = stderr_preview[-1] if stderr_preview else "unknown error"
                print(f"    [FAILED] {err_line}")
            else:
                # Determine where the record was routed from stdout
                stdout = result.stdout or ""
                if "Moved record to accepted" in stdout:
                    outcome = "accepted"
                elif "Moved record to rejected" in stdout:
                    outcome = "rejected"
                else:
                    outcome = "review_queue"
                routing_counts[outcome] += 1
                print(f"    [OK] → {outcome}")
        except subprocess.TimeoutExpired:
            routing_counts["failed"] += 1
            print(f"    [TIMEOUT] process_record exceeded {PROCESS_RECORD_TIMEOUT}s — skipping")
        except Exception as e:
            routing_counts["failed"] += 1
            print(f"    [ERROR] {e}")

    # Save stats
    _save_stats(total_stats)

    print(f"\n{'=' * 60}")
    print(f"Seed Crawl Pipeline Complete")
    print(f"Converted {len(record_paths)} candidates to raw records")
    print(f"")
    print(f"Minimax Cycle Results:")
    print(f"  Accepted   : {routing_counts['accepted']}")
    print(f"  Review     : {routing_counts['review_queue']}")
    print(f"  Rejected   : {routing_counts['rejected']}")
    if routing_counts["failed"]:
        print(f"  Failed     : {routing_counts['failed']}")
    print(f"{'=' * 60}\n")

    return discovered_ids


def _save_stats(stats: dict[str, int]) -> None:
    """
    Save seed crawl stats to manifest file.

    Args:
        stats: Statistics dictionary
    """
    stats_path = DATA_DIR / "candidate_manifests" / "seed_crawl_stats.json"
    stats_path.parent.mkdir(parents=True, exist_ok=True)

    # Load existing stats if available
    existing = {}
    if stats_path.exists():
        try:
            with open(stats_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, IOError):
            existing = {}

    # Update with new stats
    existing["last_run"] = datetime.now(timezone.utc).isoformat()
    existing["last_stats"] = stats

    # Keep history
    if "runs" not in existing:
        existing["runs"] = []
    existing["runs"].append(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "stats": stats,
        }
    )

    # Keep last 10 runs
    existing["runs"] = existing["runs"][-10:]

    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2)


# ============================================================================
# CLI Entry Point
# ============================================================================


def main() -> None:
    """Main entry point for the seed crawl script."""
    parser = argparse.ArgumentParser(
        description="Run seed site crawling lane orchestration."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Execute crawls but don't save candidates or run pipeline",
    )

    args = parser.parse_args()

    try:
        discovered_ids = run_seed_crawl(dry_run=args.dry_run)

        if args.dry_run:
            print(f"\n[DRY RUN] Would have discovered {len(discovered_ids)} candidates")

        print("\nSeed crawl completed successfully.")
        sys.exit(0)

    except KeyboardInterrupt:
        print("\n[Pipeline] Interrupted by user. Exiting.")
        sys.exit(1)

    except Exception as e:
        print(f"\n[Pipeline] Error: {e}")
        raise


if __name__ == "__main__":
    main()
