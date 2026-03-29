"""
V2 Candidate Pipeline Orchestration Script.

This script orchestrates the candidate processing pipeline for a given discovery lane:
1. Discover candidates (synthetic for now)
2. Dedupe candidates
3. Score candidates
4. Filter by score
5. Convert survivors to raw records
6. Optionally call existing process_record.py for each converted record

Usage:
    python -m scripts.run_v2_candidate_pipeline [--lane LANE]

Arguments:
    --lane LANE    The discovery lane to process (default: trusted_sources)
                  Options: trusted_sources, keyword_discovery, seed_crawl
"""

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Add parent directory to path for imports
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from scripts.candidate_utils import (
    ensure_candidate_directories,
    generate_candidate_id,
    hash_url,
    save_candidate_json,
    load_candidate_json,
    update_lane_stats,
    CANDIDATES_DIR,
    DATA_DIR,
)
from scripts.dedupe_candidates import process_dedupe
from scripts.score_candidates import score_candidate, filter_by_score
from scripts.convert_candidates_to_raw import convert_candidates


# Valid lanes
VALID_LANES = ["trusted_sources", "keyword_discovery", "seed_crawl"]


def create_synthetic_candidate(
    lane: str,
    domain: str,
    title: str,
    anchor_text: str,
    url: str,
    topic: str = "macro catalysts",
    discovery_method: str = "monitor",
    trust_tier: str = "high",
) -> dict[str, Any]:
    """
    Create a synthetic test candidate for pipeline testing.

    Args:
        lane: Discovery lane
        domain: Source domain
        title: Page title
        anchor_text: Anchor text that led to discovery
        url: Source URL
        topic: Topic category
        discovery_method: How it was discovered
        trust_tier: Trust tier of the source

    Returns:
        Synthetic candidate record
    """
    candidate_id = generate_candidate_id(lane=lane, domain=domain, title=title, url=url)

    discovered_at = datetime.now(timezone.utc).isoformat()

    return {
        "candidate_id": candidate_id,
        "lane": lane,
        "discovered_at": discovered_at,
        "topic": topic,
        "source": {
            "domain": domain,
            "source_name": domain_to_source_name(domain),
            "url": url,
            "discovery_url": url,
            "discovery_method": discovery_method,
            "trust_tier": trust_tier,
        },
        "title": title,
        "anchor_text": anchor_text,
        "raw_html_path": "",
        "raw_text_path": "",
        "metadata": {
            "http_status": 200,
            "content_type": "text/html",
            "published_at": discovered_at,
            "language": "en",
            "word_count": 0,
        },
        "dedupe": {
            "url_hash": hash_url(url),
            "normalized_title_hash": "",
            "content_hash": "",
        },
        "status": "discovered",
        "notes": "Synthetic test candidate",
    }


def domain_to_source_name(domain: str) -> str:
    """
    Convert domain to a readable source name.

    Args:
        domain: Domain name

    Returns:
        Human-readable source name
    """
    domain_to_name = {
        "federalreserve.gov": "Federal Reserve",
        "newyorkfed.org": "New York Fed",
        "treasury.gov": "U.S. Treasury",
        "ecb.europa.eu": "European Central Bank",
        "bankofengland.co.uk": "Bank of England",
        "sec.gov": "SEC",
        "bis.org": "Bank for International Settlements",
        "imf.org": "IMF",
        "brookings.edu": "Brookings Institution",
        "piie.com": "Peterson Institute",
    }
    return domain_to_name.get(domain, domain.replace("www.", "").title())


def discover_candidates(lane: str) -> list[dict[str, Any]]:
    """
    Discover candidates for a given lane.

    For V2 Part 1, this creates synthetic test candidates.
    In future, this will call actual discovery methods per lane.

    Args:
        lane: The discovery lane

    Returns:
        List of discovered candidates
    """
    print(f"\n[Discovery] Starting candidate discovery for lane: {lane}")

    if lane == "trusted_sources":
        # Synthetic test candidates for trusted sources lane
        candidates = [
            create_synthetic_candidate(
                lane=lane,
                domain="federalreserve.gov",
                title="FOMC Statement March 29 2026",
                anchor_text="Federal Reserve issues FOMC statement",
                url="https://www.federalreserve.gov/newsevents/pressreleases/monetary20260329a.htm",
                topic="macro catalysts",
                discovery_method="monitor",
                trust_tier="high",
            ),
            create_synthetic_candidate(
                lane=lane,
                domain="newyorkfed.org",
                title="New York Fed President Williams Speaks on Economic Outlook",
                anchor_text="Williams discusses monetary policy outlook",
                url="https://www.newyorkfed.org/newsevents/speeches/2026/williams20260329",
                topic="macro catalysts",
                discovery_method="monitor",
                trust_tier="high",
            ),
        ]
    elif lane == "keyword_discovery":
        # Synthetic test candidates for keyword discovery lane
        candidates = [
            create_synthetic_candidate(
                lane=lane,
                domain="brookings.edu",
                title="The Future of Inflation Targeting",
                anchor_text="Brookings paper on inflation targeting framework",
                url="https://www.brookings.edu/research/future-of-inflation-targeting/",
                topic="macro catalysts",
                discovery_method="search",
                trust_tier="medium",
            ),
        ]
    elif lane == "seed_crawl":
        # Synthetic test candidates for seed crawl lane
        candidates = [
            create_synthetic_candidate(
                lane=lane,
                domain="bis.org",
                title="BIS Quarterly Review March 2026",
                anchor_text="BIS publishes quarterly review",
                url="https://www.bis.org/publ/qtrpdf/",
                topic="market structure",
                discovery_method="crawl",
                trust_tier="medium",
            ),
        ]
    else:
        candidates = []

    # Save discovered candidates
    for candidate in candidates:
        save_candidate_json(
            candidate,
            CANDIDATES_DIR / "discovered" / f"{candidate['candidate_id']}.json",
        )

    print(f"[Discovery] Found {len(candidates)} candidates")
    update_lane_stats(lane, "discovered", len(candidates))

    return candidates


def run_pipeline(lane: str = "trusted_sources") -> None:
    """
    Run the full V2 candidate pipeline for a given lane.

    Steps:
    1. Discover candidates (synthetic for now)
    2. Dedupe candidates
    3. Score candidates
    4. Filter by score
    5. Convert survivors to raw records
    6. Optionally call existing process_record.py for each converted record

    Args:
        lane: The discovery lane to process
    """
    print(f"\n{'=' * 60}")
    print(f"V2 Candidate Pipeline - Lane: {lane}")
    print(f"{'=' * 60}")

    # Ensure directories exist
    ensure_candidate_directories()

    # Step 1: Discover candidates
    candidates = discover_candidates(lane)

    if not candidates:
        print(f"[Pipeline] No candidates discovered for lane '{lane}'. Exiting.")
        return

    # Step 2: Dedupe candidates
    print(f"\n[Pipeline] Step 2: Deduplicating candidates...")
    candidates, duplicates = process_dedupe(candidates, lane)
    print(
        f"[Dedup] {len(duplicates)}/{len(candidates) + len(duplicates)} candidates were duplicates"
    )

    if not candidates:
        print(f"[Pipeline] No candidates survived deduplication. Exiting.")
        return

    # Step 3: Score candidates
    print(f"\n[Pipeline] Step 3: Scoring candidates...")
    for candidate in candidates:
        candidate = score_candidate(candidate)
        print(
            f"  - {candidate['candidate_id']}: score={candidate['candidate_scores']['total_score']}"
        )

    # Step 4: Filter by score
    print(f"\n[Pipeline] Step 4: Filtering by score...")
    candidates, filtered = filter_by_score(candidates, threshold=25)
    print(
        f"[Filter] {len(filtered)}/{len(candidates) + len(filtered)} candidates did not pass score filter"
    )

    if not candidates:
        print(f"[Pipeline] No candidates survived scoring filter. Exiting.")
        return

    # Step 5: Convert to raw records
    print(f"\n[Pipeline] Step 5: Converting to raw records...")
    record_ids = convert_candidates(candidates)

    if not record_ids:
        print(f"[Pipeline] No records were created. Exiting.")
        return

    update_lane_stats(lane, "converted", len(record_ids))

    # Step 6: Call existing process_record.py (optional, skip for testing)
    print(f"\n[Pipeline] Step 6: Processed {len(record_ids)} records:")
    for record_id in record_ids:
        print(f"  - {record_id}")

    print(f"\n{'=' * 60}")
    print(f"Pipeline complete for lane '{lane}'")
    print(f"Converted {len(record_ids)} candidates to raw records")
    print(f"{'=' * 60}\n")


def main() -> None:
    """Main entry point for the pipeline script."""
    parser = argparse.ArgumentParser(
        description="Run V2 candidate pipeline for a given discovery lane."
    )
    parser.add_argument(
        "--lane",
        type=str,
        default="trusted_sources",
        choices=VALID_LANES,
        help=f"Lane to process (default: trusted_sources)",
    )

    args = parser.parse_args()

    try:
        run_pipeline(lane=args.lane)
    except KeyboardInterrupt:
        print("\n[Pipeline] Interrupted by user. Exiting.")
        sys.exit(1)
    except Exception as e:
        print(f"\n[Pipeline] Error: {e}")
        raise


if __name__ == "__main__":
    main()
