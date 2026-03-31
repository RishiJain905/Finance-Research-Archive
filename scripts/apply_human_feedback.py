"""Apply Human Feedback Script.

Applies human feedback decisions to research records with rich action types.
Part of V2.5 Part 7 Human Review Intelligence.

Handles:
- Validating feedback against the human_feedback schema
- Mapping rich actions to record status changes
- Setting both human_feedback (new) and human_review (backward compat) blocks
- Routing feedback to memory systems

Usage:
    python scripts/apply_human_feedback.py <record_id> <decision> [--notes "text"]
        [--source-feedback bad_source|good_source] [--topic-feedback expand_this_topic]

    python scripts/apply_human_feedback.py --from-file <feedback_json_path> <record_id>

    python scripts/apply_human_feedback.py --from-stdin <record_id>

Examples:
    python scripts/apply_human_feedback.py rec_123 approve --notes "Good analysis"
    python scripts/apply_human_feedback.py rec_456 approve_and_promote --source-feedback good_source
    python scripts/apply_human_feedback.py rec_789 reject --notes "Low quality" --source-feedback bad_source
    python scripts/apply_human_feedback.py --from-file feedback.json rec_123
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts import update_feedback_memory

# Constants for directory paths
REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"
ACCEPTED_DIR = BASE_DIR / "data" / "accepted"
REJECTED_DIR = BASE_DIR / "data" / "rejected"

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

# Decisions that map to rejected status
REJECT_DECISIONS = [
    "reject",
    "bad_source",
    "suppress_similar_items",
]


def load_json_file(path: Path) -> dict[str, Any]:
    """Load JSON file.

    Args:
        path: Path to JSON file

    Returns:
        Parsed JSON dictionary

    Raises:
        FileNotFoundError: If file doesn't exist
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


def validate_feedback(feedback: dict[str, Any]) -> tuple[bool, Optional[str]]:
    """Validate feedback against the human_feedback schema.

    Args:
        feedback: Feedback dictionary to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check required fields
    if "decision" not in feedback:
        return False, "Missing required field: decision"

    decision = feedback.get("decision", "")
    if decision not in VALID_DECISIONS:
        return False, f"Invalid decision: {decision}. Must be one of {VALID_DECISIONS}"

    # Check notes length if present
    notes = feedback.get("notes", "")
    if notes and len(notes) > 500:
        return (
            False,
            f"Notes exceeds maximum length of 500 characters (got {len(notes)})",
        )

    # Check source_feedback enum if present
    source_feedback = feedback.get("source_feedback")
    if source_feedback is not None and source_feedback not in [
        "good_source",
        "bad_source",
    ]:
        return False, f"Invalid source_feedback: {source_feedback}"

    # Check topic_feedback enum if present
    topic_feedback = feedback.get("topic_feedback")
    if topic_feedback is not None and topic_feedback != "expand_this_topic":
        return False, f"Invalid topic_feedback: {topic_feedback}"

    return True, None


def map_decision_to_status(decision: str) -> str:
    """Map a feedback decision to a record status.

    Args:
        decision: The feedback decision

    Returns:
        Status string: "accepted" or "rejected"
    """
    if decision in ACCEPT_DECISIONS:
        return "accepted"
    elif decision in REJECT_DECISIONS:
        return "rejected"
    else:
        # Default to rejected for unknown decisions
        return "rejected"


def map_decision_to_human_review_decision(decision: str) -> str:
    """Map a feedback decision to a human_review decision for backward compat.

    Args:
        decision: The feedback decision

    Returns:
        human_review decision string
    """
    if decision in ACCEPT_DECISIONS:
        return "approved_by_human"
    else:
        return "rejected_by_human"


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


def apply_feedback_to_record(
    record: dict[str, Any],
    feedback: dict[str, Any],
) -> dict[str, Any]:
    """Apply feedback to a record, updating both status and feedback blocks.

    Args:
        record: The record to update
        feedback: The feedback dictionary

    Returns:
        Updated record
    """
    decision = feedback.get("decision", "")
    notes = feedback.get("notes", "")

    # Map decision to status
    status = map_decision_to_status(decision)
    record["status"] = status

    # Set human_feedback block (new richer format)
    record["human_feedback"] = feedback

    # Set human_review block (backward compatibility)
    record["human_review"]["required"] = False
    record["human_review"]["decision"] = map_decision_to_human_review_decision(decision)
    record["human_review"]["notes"] = notes or ""

    return record


def find_record_path(record_id: str) -> tuple[Optional[Path], Optional[str]]:
    """Find the path to a record by ID.

    Args:
        record_id: The record ID

    Returns:
        Tuple of (path, location_label) or (None, None) if not found
    """
    # Check review_queue
    path = REVIEW_QUEUE_DIR / f"{record_id}.json"
    if path.exists():
        return path, "review_queue"

    # Check accepted
    path = ACCEPTED_DIR / f"{record_id}.json"
    if path.exists():
        return path, "accepted"

    # Check rejected
    path = REJECTED_DIR / f"{record_id}.json"
    if path.exists():
        return path, "rejected"

    return None, None


def apply_human_feedback(
    record_id: str,
    feedback: dict[str, Any],
) -> dict[str, Any]:
    """Apply human feedback to a record and route to memory.

    Main function for applying feedback. Validates feedback, updates the record,
    and routes feedback to memory systems.

    Args:
        record_id: The record ID
        feedback: The feedback dictionary

    Returns:
        Dictionary with result info including updated record
    """
    # Validate feedback
    is_valid, error = validate_feedback(feedback)
    if not is_valid:
        raise ValueError(f"Invalid feedback: {error}")

    # Find record
    record_path, location = find_record_path(record_id)
    if record_path is None:
        raise FileNotFoundError(
            f"Record '{record_id}' not found in review_queue, accepted, or rejected"
        )

    # Load record
    record = load_json_file(record_path)
    original_status = record.get("status", "unknown")

    # Apply feedback to record
    record = apply_feedback_to_record(record, feedback)

    # Save updated record
    save_json_file(record_path, record)

    # Route feedback to memory
    memory_result = update_feedback_memory.apply_feedback_to_memory(record, feedback)

    return {
        "record_id": record_id,
        "record_path": str(record_path),
        "location": location,
        "original_status": original_status,
        "new_status": record["status"],
        "decision": feedback.get("decision"),
        "memory_result": memory_result,
    }


def main() -> None:
    """Main entry point for CLI usage."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Apply human feedback to a research record",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python scripts/apply_human_feedback.py rec_123 approve --notes "Good analysis"
    python scripts/apply_human_feedback.py rec_456 approve_and_promote --source-feedback good_source
    python scripts/apply_human_feedback.py rec_789 reject --notes "Low quality" --source-feedback bad_source
    python scripts/apply_human_feedback.py --from-file feedback.json rec_123
        """,
    )

    parser.add_argument(
        "record_id",
        nargs="?",
        help="Record ID to apply feedback to",
    )
    parser.add_argument(
        "decision",
        nargs="?",
        choices=VALID_DECISIONS,
        help="Feedback decision",
    )
    parser.add_argument(
        "--notes",
        type=str,
        default="",
        help="Reviewer notes (max 500 characters)",
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
    parser.add_argument(
        "--from-file",
        type=Path,
        dest="feedback_file",
        help="Path to JSON file containing feedback",
    )
    parser.add_argument(
        "--from-stdin",
        action="store_true",
        dest="from_stdin",
        help="Read feedback JSON from stdin",
    )

    args = parser.parse_args()

    # Handle stdin input
    if args.from_stdin:
        feedback = json.load(sys.stdin)
        if not args.record_id:
            print("Error: record_id required when using --from-stdin")
            sys.exit(1)
    elif args.feedback_file:
        # Load feedback from file
        feedback = load_json_file(args.feedback_file)
        if not args.record_id:
            # Try to get record_id from feedback file
            args.record_id = feedback.get("record_id")
            if not args.record_id:
                print("Error: record_id not provided and not found in feedback file")
                sys.exit(1)
    else:
        # Build feedback from arguments
        if not args.record_id:
            print("Error: record_id is required")
            parser.print_help()
            sys.exit(1)

        if not args.decision:
            print("Error: decision is required (or use --from-file)")
            parser.print_help()
            sys.exit(1)

        feedback = build_human_feedback_block(
            decision=args.decision,
            notes=args.notes if args.notes else None,
            source_feedback=args.source_feedback,
            topic_feedback=args.topic_feedback,
            reviewer_id=args.reviewer_id if args.reviewer_id else None,
        )

    # Validate feedback
    is_valid, error = validate_feedback(feedback)
    if not is_valid:
        print(f"Error: {error}")
        sys.exit(1)

    # Apply feedback
    try:
        result = apply_human_feedback(args.record_id, feedback)
        print(f"Feedback applied successfully")
        print(f"  Record: {result['record_id']}")
        print(f"  Location: {result['location']}")
        print(f"  Status: {result['original_status']} -> {result['new_status']}")
        print(f"  Decision: {result['decision']}")
        print(f"  Memory: {result['memory_result']}")
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
