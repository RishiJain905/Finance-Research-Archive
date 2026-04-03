"""Route Record Script.

Routes finalized records to accepted/rejected directories and updates memory.
Part of V2.5 Part 7 Human Review Intelligence.

Enhanced to pass feedback context to memory updates:
- If record has human_feedback block, uses specific feedback type as outcome
- Maps human_feedback.decision to appropriate memory outcome types

Usage:
    python scripts/route_record.py <record_id>
"""

import hashlib
import json
import shutil
import sys
from pathlib import Path
from urllib.parse import urlparse
from typing import Any, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.verification_store import canonicalize_verification_artifact
from scripts.ingest_sources import ensure_manifest_shape, load_json, save_json
from scripts.assign_quality_tier import assign_quality_tier

REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"
ACCEPTED_DIR = BASE_DIR / "data" / "accepted"
REJECTED_DIR = BASE_DIR / "data" / "rejected"
MANIFEST_PATH = BASE_DIR / "data" / "ingestion_manifest.json"


def load_json_file(path: Path) -> dict[str, Any]:
    """Load JSON file.

    Args:
        path: Path to JSON file

    Returns:
        Parsed JSON dictionary
    """
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def move_file_if_exists(source: Path, destination: Path) -> None:
    """Move file if it exists.

    Args:
        source: Source path
        destination: Destination path
    """
    if source.exists():
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(destination))


def compute_event_fingerprint(
    domain: str, published_at: str, event_type: str
) -> Optional[str]:
    """Return a sha1 fingerprint for (domain, date, event_type), or None if inputs are missing.

    Args:
        domain: Source domain
        published_at: Publication datetime
        event_type: Type of event

    Returns:
        SHA1 fingerprint string or None
    """
    date_part = (published_at or "")[:10].strip()
    if not domain or not date_part or not event_type:
        return None
    key = f"{domain}|{date_part}|{event_type}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def get_memory_outcome_from_feedback(
    record: dict[str, Any],
) -> tuple[str, Optional[str]]:
    """Extract memory outcome from record's human_feedback block.

    Maps human_feedback.decision to appropriate memory outcome types.
    Falls back to generic accepted/rejected if no human_feedback present.

    Args:
        record: The record dictionary

    Returns:
        Tuple of (outcome, feedback_type) where feedback_type describes the specific
        memory action taken
    """
    human_feedback = record.get("human_feedback", {})

    if not human_feedback:
        # Fall back to human_review for legacy records
        decision = record.get("human_review", {}).get("decision", "")
        if decision == "approved_by_human":
            return ("accepted_human", None)
        elif decision == "rejected_by_human":
            return ("rejected_human", None)
        # Generic fallback
        status = record.get("status", "")
        return (status, None)

    feedback_decision = human_feedback.get("decision", "")

    # Map feedback decisions to memory outcomes
    if feedback_decision == "approve":
        return ("accepted_human", "approve")
    elif feedback_decision == "reject":
        return ("rejected_human", "reject")
    elif feedback_decision == "approve_but_weak":
        # Lower weight - use "accepted" (auto) instead of "accepted_human"
        return ("accepted", "approve_but_weak")
    elif feedback_decision == "approve_and_promote":
        # Higher weight - use "accepted_human" with promotion flag
        return ("accepted_human", "approve_and_promote")
    elif feedback_decision == "bad_source":
        return ("rejected_human", "bad_source")
    elif feedback_decision == "good_source":
        return ("accepted_human", "good_source")
    elif feedback_decision == "expand_this_topic":
        return ("accepted_human", "expand_this_topic")
    elif feedback_decision == "suppress_similar_items":
        return ("rejected_human", "suppress_similar_items")
    else:
        # Unknown decision, use generic outcome
        status = record.get("status", "")
        return (status, None)


def main() -> None:
    """Main entry point for CLI usage."""
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python scripts/route_record.py <record_id>")

    record_id = sys.argv[1]

    record_path = REVIEW_QUEUE_DIR / f"{record_id}.json"
    canonicalize_verification_artifact(record_id)

    record = load_json_file(record_path)
    status = record.get("status", "review_queue")

    # Import here to avoid circular imports
    from scripts.memory_manager import update_all_memory_on_outcome

    source_url = record.get("source", {}).get("url", "")
    source_domain = record.get("source", {}).get("domain", "")
    # Fallback for legacy records produced before domain was added to the schema
    if not source_domain and source_url:
        source_domain = urlparse(source_url).netloc

    manifest = ensure_manifest_shape(load_json(MANIFEST_PATH, {}))

    if status == "accepted":
        # Event-level deduplication: demote to review_queue if the same
        # (domain, date, event_type) has already been accepted.
        event_type = record.get("event_type", "")
        published_at = record.get("source", {}).get("published_at", "")
        event_fp = compute_event_fingerprint(source_domain, published_at, event_type)

        if event_fp:
            existing = manifest["event_fingerprints"].get(event_fp)
            if existing and existing != record_id:
                print(
                    f"  Event-level duplicate detected (fingerprint matches {existing}). "
                    "Demoting to review_queue."
                )
                record["status"] = "review_queue"
                record["human_review"]["required"] = False
                existing_note = record["human_review"].get("notes", "")
                dup_note = f"duplicate_event:{existing}"
                if dup_note not in existing_note:
                    record["human_review"]["notes"] = (
                        f"{existing_note}, {dup_note}".lstrip(", ")
                    )
                with record_path.open("w", encoding="utf-8") as f:
                    json.dump(record, f, indent=2, ensure_ascii=False)
                status = "review_queue"

        if status == "accepted":
            target_record_path = ACCEPTED_DIR / record_path.name

            # Assign quality tier before moving to accepted
            tier_block = assign_quality_tier(record)
            record["quality_tier"] = tier_block["quality_tier"]

            # Write updated record (with tier) to target location
            target_record_path.parent.mkdir(parents=True, exist_ok=True)
            with target_record_path.open("w", encoding="utf-8") as f:
                json.dump(record, f, indent=2, ensure_ascii=False)

            # Remove the original from review_queue
            if record_path.exists():
                record_path.unlink()

            # V2.7 Part 3: Watchlist matching for accepted records
            try:
                from scripts.watchlist_matcher import (
                    match_record_against_watchlists,
                    load_watchlists,
                )
                from scripts.watchlist_hit_persistence import save_watchlist_hit

                watchlists = load_watchlists(
                    str(BASE_DIR / "config" / "watchlists_v27.json")
                )
                hits = match_record_against_watchlists(record, watchlists)
                for hit in hits:
                    save_watchlist_hit(hit, str(BASE_DIR / "data" / "watchlist_hits"))
                if hits:
                    print(f"  Watchlist hits: {len(hits)}")
            except Exception as e:
                print(f"  Warning: Watchlist matching skipped: {e}")

            print("Moved record to accepted:")
            print(target_record_path.relative_to(BASE_DIR))
            print(
                f"  Quality tier: {tier_block['quality_tier']['tier']} (score: {tier_block['quality_tier']['score']})"
            )

            # Register event fingerprint so future duplicates are caught.
            if event_fp:
                manifest["event_fingerprints"][event_fp] = record_id
                save_json(MANIFEST_PATH, manifest)

            if source_domain:
                # Get memory outcome from human_feedback if present
                outcome, feedback_type = get_memory_outcome_from_feedback(record)

                result = update_all_memory_on_outcome(
                    domain=source_domain,
                    outcome=outcome,
                    source_id=record.get("source", {}).get("source_id"),
                    source_type=record.get("lane", "manual"),
                    url=record.get("url", ""),
                    candidate_id=record.get("id", record_path.stem),
                )
                print(
                    f"Memory updated - domain trust: {result['domain_memory']['trust_score']}"
                )
                if feedback_type:
                    print(f"  Feedback type: {feedback_type}")

    elif status == "rejected":
        target_record_path = REJECTED_DIR / record_path.name

        move_file_if_exists(record_path, target_record_path)

        print("Moved record to rejected:")
        print(target_record_path.relative_to(BASE_DIR))

        if source_domain:
            # Get memory outcome from human_feedback if present
            outcome, feedback_type = get_memory_outcome_from_feedback(record)

            result = update_all_memory_on_outcome(
                domain=source_domain,
                outcome=outcome,
                source_id=record.get("source", {}).get("source_id"),
                source_type=record.get("lane", "manual"),
                url=record.get("url", ""),
                candidate_id=record.get("id", record_path.stem),
            )
            print(
                f"Memory updated - domain trust: {result['domain_memory']['trust_score']}"
            )
            if feedback_type:
                print(f"  Feedback type: {feedback_type}")

    else:
        print("Record remains in review_queue:")
        print(record_path.relative_to(BASE_DIR))

        if source_domain:
            # For review_queue status, use "review" outcome
            result = update_all_memory_on_outcome(
                domain=source_domain,
                outcome="review",
                source_id=record.get("source", {}).get("source_id"),
                source_type=record.get("lane", "manual"),
                url=record.get("url", ""),
                candidate_id=record.get("id", record_path.stem),
            )
            print(
                f"Memory updated - domain trust: {result['domain_memory']['trust_score']}"
            )

        # V2.7 Part 3: Watchlist matching for review_queue records
        # Note: review records stay in review_queue, so we load from there
        try:
            from scripts.watchlist_matcher import (
                match_record_against_watchlists,
                load_watchlists,
            )
            from scripts.watchlist_hit_persistence import save_watchlist_hit

            # Load record from review_queue (it should be there since status is review_queue)
            review_record_path = REVIEW_QUEUE_DIR / f"{record_id}.json"
            if review_record_path.exists():
                with open(review_record_path) as f:
                    review_record = json.load(f)
                watchlists = load_watchlists(
                    str(BASE_DIR / "config" / "watchlists_v27.json")
                )
                hits = match_record_against_watchlists(review_record, watchlists)
                for hit in hits:
                    save_watchlist_hit(hit, str(BASE_DIR / "data" / "watchlist_hits"))
                if hits:
                    print(f"  Watchlist hits: {len(hits)}")
        except Exception as e:
            print(f"  Warning: Watchlist matching skipped: {e}")


if __name__ == "__main__":
    main()
