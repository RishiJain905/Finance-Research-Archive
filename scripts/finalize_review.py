"""Finalize Review Script.

Finalizes human review decisions for records in the review queue.
Part of V2.5 Part 7 Human Review Intelligence.

Supports rich feedback decisions with memory routing:
- approve, reject (binary, backward compatible)
- approve_but_weak, approve_and_promote (nuanced accepts)
- bad_source, good_source (source quality feedback)
- expand_this_topic, suppress_similar_items (topic actions)

Usage:
    python scripts/finalize_review.py <record_id> <decision> [--notes "text"]
        [--source-feedback bad_source|good_source] [--topic-feedback expand_this_topic]

Examples:
    python scripts/finalize_review.py rec_123 approve
    python scripts/finalize_review.py rec_456 approve_and_promote --source-feedback good_source
    python scripts/finalize_review.py rec_789 reject --notes "Low quality source"
"""

import json
import hashlib
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.filter_raw_records import parse_raw_record
from scripts.run_verifier import collect_hard_blockers
from scripts.verification_store import canonicalize_verification_artifact
from scripts.assign_quality_tier import assign_quality_tier

REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"
ACCEPTED_DIR = BASE_DIR / "data" / "accepted"
REJECTED_DIR = BASE_DIR / "data" / "rejected"
INGESTION_MANIFEST_PATH = BASE_DIR / "data" / "ingestion_manifest.json"

# Valid decision types
VALID_DECISIONS = [
    "approve",
    "reject",
    "approve_but_weak",
    "approve_and_promote",
    "bad_source",
    "good_source",
    "expand_this_topic",
    "suppress_similar_items",
]

# Decisions that map to accepted status
ACCEPT_DECISIONS = [
    "approve",
    "approve_but_weak",
    "approve_and_promote",
    "good_source",
    "expand_this_topic",
]


def make_callback_key(record_id: str) -> str:
    """Create the Telegram callback key used in review buttons."""
    return hashlib.sha1(record_id.encode()).hexdigest()[:28]


def _find_record_by_id(record_id: str) -> tuple[str, Path, str] | None:
    """Find a record by exact ID across review states."""
    candidates = [
        ("review_queue", REVIEW_QUEUE_DIR / f"{record_id}.json"),
        ("accepted", ACCEPTED_DIR / f"{record_id}.json"),
        ("rejected", REJECTED_DIR / f"{record_id}.json"),
    ]
    for location, path in candidates:
        if path.exists():
            return record_id, path, location
    return None


def _find_record_by_callback_key(callback_key: str) -> tuple[str, Path, str] | None:
    """Resolve a Telegram callback key to a record in review states."""
    matches: list[tuple[str, Path, str]] = []
    for location, directory in [
        ("review_queue", REVIEW_QUEUE_DIR),
        ("accepted", ACCEPTED_DIR),
        ("rejected", REJECTED_DIR),
    ]:
        if not directory.exists():
            continue
        for record_path in directory.glob("*.json"):
            if record_path.name.endswith("_verification.json"):
                continue
            record_id = record_path.stem
            if make_callback_key(record_id) == callback_key:
                matches.append((record_id, record_path, location))

    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]

    matching_ids = ", ".join(match[0] for match in matches)
    raise ValueError(
        f"Callback key '{callback_key}' matches multiple records: {matching_ids}"
    )


def resolve_record_identifier(record_identifier: str) -> tuple[str, Path, str]:
    """Resolve either a full record ID or Telegram callback key.

    Returns:
        Tuple of (resolved_record_id, path, location)
    """
    exact_match = _find_record_by_id(record_identifier)
    if exact_match:
        return exact_match

    callback_match = _find_record_by_callback_key(record_identifier)
    if callback_match:
        return callback_match

    raise FileNotFoundError(
        f"Record '{record_identifier}' not found in review_queue, accepted, or rejected.\n"
        "This can happen when a Telegram callback key was sent but no matching record "
        "exists in this branch, or when the record ID is incorrect."
    )


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


def save_json_file(path: Path, data: dict[str, Any]) -> None:
    """Save JSON file.

    Args:
        path: Path to save to
        data: Data to save
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def move_file_if_exists(source: Path, destination: Path) -> None:
    """Move file if it exists.

    Args:
        source: Source path
        destination: Destination path
    """
    if source.exists():
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(destination))


def load_review_context(
    record_id: str, record: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Load review context for a record.

    Args:
        record_id: The record ID
        record: The record dictionary

    Returns:
        Tuple of (metadata, rules)
    """
    metadata = {}
    rules = {}

    raw_text_path = record.get("raw_text_path", "")
    if raw_text_path:
        candidate_path = BASE_DIR / Path(raw_text_path)
        if candidate_path.exists():
            metadata = parse_raw_record(
                candidate_path.read_text(encoding="utf-8", errors="ignore")
            ).get("metadata", {})

    if INGESTION_MANIFEST_PATH.exists():
        ingestion_manifest = load_json_file(INGESTION_MANIFEST_PATH)
        rules = ingestion_manifest.get("record_rules", {}).get(record_id, {})

    return metadata, rules


def build_human_feedback_block(
    decision: str,
    notes: Optional[str] = None,
    source_feedback: Optional[str] = None,
    topic_feedback: Optional[str] = None,
    reviewer_id: Optional[str] = None,
) -> dict[str, Any]:
    """Build a human_feedback block.

    Args:
        decision: The feedback decision
        notes: Optional reviewer notes
        source_feedback: Optional source feedback
        topic_feedback: Optional topic feedback
        reviewer_id: Optional reviewer identifier

    Returns:
        Human feedback dictionary
    """
    feedback = {
        "decision": decision,
        "reviewed_at": datetime.now(timezone.utc).isoformat(),
    }

    if notes:
        feedback["notes"] = notes

    if source_feedback:
        feedback["source_feedback"] = source_feedback

    if topic_feedback:
        feedback["topic_feedback"] = topic_feedback

    if reviewer_id:
        feedback["reviewer_id"] = reviewer_id

    return feedback


def apply_review_decision(
    record: dict[str, Any],
    decision: str,
    hard_blockers: list[str],
    notes: Optional[str] = None,
    source_feedback: Optional[str] = None,
    topic_feedback: Optional[str] = None,
) -> dict[str, Any]:
    """Apply a review decision to a record.

    Handles both binary (approve/reject) and rich feedback decisions.
    Sets both human_feedback (new) and human_review (backward compat) blocks.

    Args:
        record: The record to update
        decision: The feedback decision
        hard_blockers: List of quality gate failures
        notes: Optional reviewer notes
        source_feedback: Optional source feedback
        topic_feedback: Optional topic feedback

    Returns:
        Updated record
    """
    # Check for hard blockers with approve decision
    if decision == "approve" and hard_blockers:
        record["status"] = "rejected"
        record["human_review"]["required"] = False
        record["human_review"]["decision"] = "rejected_by_quality_gate"
        record["human_review"]["notes"] = ", ".join(hard_blockers)

        # Also set human_feedback block for consistency
        record["human_feedback"] = build_human_feedback_block(
            decision="reject",
            notes=", ".join(hard_blockers),
            source_feedback=source_feedback,
            topic_feedback=topic_feedback,
        )
        return record

    # Determine if this is an acceptance or rejection
    is_acceptance = decision in ACCEPT_DECISIONS or decision == "approve"

    # Build notes for backward compat
    if notes:
        review_notes = notes
    elif is_acceptance:
        review_notes = "Approved from Telegram review flow."
    else:
        review_notes = "Rejected from Telegram review flow."

    # Set status
    if is_acceptance:
        record["status"] = "accepted"
        human_review_decision = "approved_by_human"
    else:
        record["status"] = "rejected"
        human_review_decision = "rejected_by_human"

    # Set human_review block (backward compatibility)
    record["human_review"]["required"] = False
    record["human_review"]["decision"] = human_review_decision
    record["human_review"]["notes"] = review_notes

    # Set human_feedback block (new rich format)
    record["human_feedback"] = build_human_feedback_block(
        decision=decision,
        notes=notes,
        source_feedback=source_feedback,
        topic_feedback=topic_feedback,
    )

    return record


def apply_feedback_to_memory(record: dict[str, Any]) -> dict[str, Any]:
    """Apply feedback to memory systems.

    Args:
        record: The record with feedback

    Returns:
        Memory update result
    """
    # Import here to avoid circular imports
    from scripts import update_feedback_memory

    feedback = record.get("human_feedback", {})
    if not feedback:
        return {"action": "no_feedback_to_apply"}

    return update_feedback_memory.apply_feedback_to_memory(record, feedback)


def main() -> None:
    """Main entry point for CLI usage."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Finalize a human review decision for a record",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python scripts/finalize_review.py rec_123 approve
    python scripts/finalize_review.py rec_456 approve_and_promote --source-feedback good_source
    python scripts/finalize_review.py rec_789 reject --notes "Low quality source"
    python scripts/finalize_review.py rec_101 bad_source --notes "Unreliable source"
        """,
    )

    parser.add_argument(
        "record_id",
        help="Record ID to finalize",
    )
    parser.add_argument(
        "decision",
        choices=VALID_DECISIONS,
        help="Review decision",
    )
    parser.add_argument(
        "--notes",
        type=str,
        default="",
        help="Reviewer notes",
    )
    parser.add_argument(
        "--source-feedback",
        type=str,
        choices=["good_source", "bad_source"],
        help="Source quality feedback",
    )
    parser.add_argument(
        "--topic-feedback",
        type=str,
        choices=["expand_this_topic"],
        help="Topic feedback",
    )
    parser.add_argument(
        "--reviewer-id",
        type=str,
        default="",
        help="Reviewer identifier",
    )

    args = parser.parse_args()

    requested_record_id = args.record_id
    decision = args.decision

    record_id, record_path, location = resolve_record_identifier(requested_record_id)

    if location != "review_queue":
        print(
            f"Record '{record_id}' was already finalized and is in {location}/. Nothing to do."
        )
        return

    if requested_record_id != record_id:
        print(
            f"Resolved callback key '{requested_record_id}' to record ID '{record_id}'."
        )

    canonicalize_verification_artifact(record_id)

    record = load_json_file(record_path)
    metadata, rules = load_review_context(record_id, record)
    hard_blockers = collect_hard_blockers(record, metadata, rules)

    record = apply_review_decision(
        record,
        decision,
        hard_blockers,
        notes=args.notes if args.notes else None,
        source_feedback=args.source_feedback,
        topic_feedback=args.topic_feedback,
    )

    # Apply feedback to memory systems
    memory_result = apply_feedback_to_memory(record)

    save_json_file(record_path, record)

    if record["status"] == "accepted":
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

        print("Approved and moved to accepted:")
        print(target_record_path.relative_to(BASE_DIR))
        print(
            f"  Quality tier: {tier_block['quality_tier']['tier']} (score: {tier_block['quality_tier']['score']})"
        )
        print(f"  Decision: {decision}")
    else:
        target_record_path = REJECTED_DIR / record_path.name

        move_file_if_exists(record_path, target_record_path)

        print("Moved record to rejected:")
        print(target_record_path.relative_to(BASE_DIR))
        print(f"  Decision: {decision}")

    # Print memory update info
    if memory_result and memory_result.get("action") != "no_feedback_to_apply":
        print(f"  Memory: {memory_result}")


if __name__ == "__main__":
    main()
