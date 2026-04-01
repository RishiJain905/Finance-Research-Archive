"""Update Feedback Memory Module.

Routes different human feedback types to appropriate memory updates.
Part of V2.5 Part 7 Human Review Intelligence.

Feedback types and their memory routing:
- bad_source: Stronger negative weight on source/domain memory
- good_source: Stronger positive weight on source/domain memory
- expand_this_topic: Increase priority for the record's topic in theme memory
- approve_and_promote: Higher weight than accepted_human
- approve_but_weak: Lower weight than accepted_human
- suppress_similar_items: Log suppression signal for future filtering

Usage:
    python scripts/update_feedback_memory.py <record_json_path>
    python scripts/update_feedback_memory.py --record-id <record_id> [--feedback-type <type>]
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.memory_manager import update_all_memory_on_outcome
from scripts import theme_memory_persistence

# Suppressions log directory
SUPPRESSIONS_LOG_DIR = BASE_DIR / "logs" / "suppressions"
SUPPRESSIONS_LOG_FILE = SUPPRESSIONS_LOG_DIR / "suppressions.jsonl"


def _ensure_suppressions_log_dir() -> None:
    """Ensure the suppressions log directory exists."""
    SUPPRESSIONS_LOG_DIR.mkdir(parents=True, exist_ok=True)


def _get_feedback_logger() -> logging.Logger:
    """Get or create logger for feedback routing.

    Returns:
        Logger instance for feedback operations
    """
    _ensure_suppressions_log_dir()
    logger = logging.getLogger("feedback_memory")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler(SUPPRESSIONS_LOG_DIR / "feedback_routing.jsonl")
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    return logger


def _log_feedback_routing(
    record_id: str,
    feedback_type: str,
    action: str,
    details: dict[str, Any],
) -> None:
    """Log a feedback routing action.

    Args:
        record_id: Record identifier
        feedback_type: Type of feedback received
        action: Action taken (e.g., 'memory_update', 'suppression_logged')
        details: Additional details about the action
    """
    logger = _get_feedback_logger()
    log_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "record_id": record_id,
        "feedback_type": feedback_type,
        "action": action,
        **details,
    }
    logger.info(json.dumps(log_entry))


def handle_bad_source(
    record: dict[str, Any], feedback: dict[str, Any]
) -> dict[str, Any]:
    """Handle bad_source feedback - applies stronger negative weight.

    Args:
        record: The record being reviewed
        feedback: The feedback dictionary

    Returns:
        Result dictionary from memory update
    """
    from urllib.parse import urlparse

    record_id = record.get("id", "")
    source_url = record.get("source", {}).get("url", "")
    source_domain = record.get("source", {}).get("domain", "")

    # Fallback for legacy records
    if not source_domain and source_url:
        source_domain = urlparse(source_url).netloc

    # Use "rejected_human" with a note that it was bad_source
    # The memory_manager will apply human_multiplier for human decisions
    result = update_all_memory_on_outcome(
        domain=source_domain,
        outcome="rejected_human",
        source_id=record.get("source", {}).get("source_id"),
        source_type=record.get("lane", "manual"),
        url=source_url,
        candidate_id=record_id,
    )

    _log_feedback_routing(
        record_id=record_id,
        feedback_type="bad_source",
        action="memory_update",
        details={
            "domain": source_domain,
            "outcome": "rejected_human",
            "note": "bad_source feedback - stronger negative applied",
        },
    )

    return result


def handle_good_source(
    record: dict[str, Any], feedback: dict[str, Any]
) -> dict[str, Any]:
    """Handle good_source feedback - applies stronger positive weight.

    Args:
        record: The record being reviewed
        feedback: The feedback dictionary

    Returns:
        Result dictionary from memory update
    """
    from urllib.parse import urlparse

    record_id = record.get("id", "")
    source_url = record.get("source", {}).get("url", "")
    source_domain = record.get("source", {}).get("domain", "")

    # Fallback for legacy records
    if not source_domain and source_url:
        source_domain = urlparse(source_url).netloc

    # Use "accepted_human" with a note that it was good_source
    result = update_all_memory_on_outcome(
        domain=source_domain,
        outcome="accepted_human",
        source_id=record.get("source", {}).get("source_id"),
        source_type=record.get("lane", "manual"),
        url=source_url,
        candidate_id=record_id,
    )

    _log_feedback_routing(
        record_id=record_id,
        feedback_type="good_source",
        action="memory_update",
        details={
            "domain": source_domain,
            "outcome": "accepted_human",
            "note": "good_source feedback - stronger positive applied",
        },
    )

    return result


def handle_expand_this_topic(
    record: dict[str, Any], feedback: dict[str, Any]
) -> dict[str, Any]:
    """Handle expand_this_topic feedback - increases theme priority.

    Args:
        record: The record being reviewed
        feedback: The feedback dictionary

    Returns:
        Result dictionary with theme update info
    """
    record_id = record.get("id", "")
    topic = record.get("topic", "")
    title = record.get("title", "")

    # Extract terms from the record to find/update theme
    from scripts.extract_theme_terms import (
        extract_text_from_record,
        tokenize_and_clean,
        extract_ngrams,
    )

    text = extract_text_from_record(record)
    tokens = tokenize_and_clean(text)
    terms = extract_ngrams(tokens, n_range=(1, 2))

    # Find existing theme or create new one
    themes = theme_memory_persistence.read_theme_memory()
    matching_theme_id = None

    for theme_id, theme in themes.items():
        theme_terms = set(theme.get("positive_terms", []))
        if theme_terms & terms:
            matching_theme_id = theme_id
            break

    # Prepare new theme data
    positive_terms = list(terms)
    negative_terms = []

    new_theme_data = {
        "theme_label": title[:50] if title else topic or "Unknown Theme",
        "positive_terms": positive_terms,
        "negative_terms": negative_terms,
    }

    if matching_theme_id:
        # Update existing theme - increase its priority
        theme = themes[matching_theme_id]
        theme["review_count"] = theme.get("review_count", 0) + 1

        # Merge positive terms
        existing_positive = set(theme.get("positive_terms", []))
        new_positive = set(positive_terms)
        theme["positive_terms"] = list(existing_positive | new_positive)

        # Recompute priority score
        from scripts.update_theme_memory import compute_priority_score

        theme["priority_score"] = compute_priority_score(theme)
        theme["last_seen"] = datetime.now(timezone.utc).isoformat()

        themes[matching_theme_id] = theme
        theme_memory_persistence.write_theme_memory(themes)
        updated_theme = theme
    else:
        # Create new theme with high priority
        from scripts.update_theme_memory import (
            create_or_update_theme,
            _generate_theme_id,
        )

        # Generate theme ID from topic
        theme_id = _generate_theme_id(topic or title or "unknown")
        new_theme = create_or_update_theme(
            themes,
            new_theme_data,
            theme_id=theme_id,
        )
        # Set higher priority for expand_this_topic feedback
        new_theme["priority_score"] = min(100, new_theme.get("priority_score", 0) + 20)
        themes[new_theme["theme_id"]] = new_theme
        theme_memory_persistence.write_theme_memory(themes)
        updated_theme = new_theme

    _log_feedback_routing(
        record_id=record_id,
        feedback_type="expand_this_topic",
        action="theme_priority_updated",
        details={
            "theme_id": updated_theme.get("theme_id"),
            "new_priority": updated_theme.get("priority_score"),
            "topic": topic,
        },
    )

    return {
        "theme_memory": updated_theme,
        "action": "expand_this_topic_handled",
    }


def handle_approve_and_promote(
    record: dict[str, Any], feedback: dict[str, Any]
) -> dict[str, Any]:
    """Handle approve_and_promote feedback - higher weight than accepted_human.

    Args:
        record: The record being reviewed
        feedback: The feedback dictionary

    Returns:
        Result dictionary from memory update
    """
    from urllib.parse import urlparse

    record_id = record.get("id", "")
    source_url = record.get("source", {}).get("url", "")
    source_domain = record.get("source", {}).get("domain", "")

    # Fallback for legacy records
    if not source_domain and source_url:
        source_domain = urlparse(source_url).netloc

    # Use "accepted_human" but with a note - the promote action is logged
    result = update_all_memory_on_outcome(
        domain=source_domain,
        outcome="accepted_human",
        source_id=record.get("source", {}).get("source_id"),
        source_type=record.get("lane", "manual"),
        url=source_url,
        candidate_id=record_id,
    )

    _log_feedback_routing(
        record_id=record_id,
        feedback_type="approve_and_promote",
        action="memory_update",
        details={
            "domain": source_domain,
            "outcome": "accepted_human",
            "note": "approve_and_promote - promoted content",
        },
    )

    return result


def handle_approve_but_weak(
    record: dict[str, Any], feedback: dict[str, Any]
) -> dict[str, Any]:
    """Handle approve_but_weak feedback - lower weight than accepted_human.

    Args:
        record: The record being reviewed
        feedback: The feedback dictionary

    Returns:
        Result dictionary from memory update
    """
    from urllib.parse import urlparse

    record_id = record.get("id", "")
    source_url = record.get("source", {}).get("url", "")
    source_domain = record.get("source", {}).get("domain", "")

    # Fallback for legacy records
    if not source_domain and source_url:
        source_domain = urlparse(source_url).netloc

    # Use "accepted" (auto) instead of "accepted_human" to indicate weak acceptance
    result = update_all_memory_on_outcome(
        domain=source_domain,
        outcome="accepted",
        source_id=record.get("source", {}).get("source_id"),
        source_type=record.get("lane", "manual"),
        url=source_url,
        candidate_id=record_id,
    )

    _log_feedback_routing(
        record_id=record_id,
        feedback_type="approve_but_weak",
        action="memory_update",
        details={
            "domain": source_domain,
            "outcome": "accepted",
            "note": "approve_but_weak - weak acceptance, no human multiplier",
        },
    )

    return result


def handle_suppress_similar_items(
    record: dict[str, Any], feedback: dict[str, Any]
) -> dict[str, Any]:
    """Handle suppress_similar_items feedback - logs suppression for future filtering.

    Args:
        record: The record being reviewed
        feedback: The feedback dictionary

    Returns:
        Result dictionary with suppression details
    """
    _ensure_suppressions_log_dir()

    record_id = record.get("id", "")
    topic = record.get("topic", "")
    title = record.get("title", "")
    source_domain = record.get("source", {}).get("domain", "")

    # Extract terms for similarity matching
    from scripts.extract_theme_terms import (
        extract_text_from_record,
        tokenize_and_clean,
        extract_ngrams,
    )

    text = extract_text_from_record(record)
    tokens = tokenize_and_clean(text)
    terms = extract_ngrams(tokens, n_range=(1, 2))

    suppression_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "record_id": record_id,
        "topic": topic,
        "title": title,
        "source_domain": source_domain,
        "suppression_terms": list(terms),
        "reason": feedback.get("notes", ""),
        "reviewer_id": feedback.get("reviewer_id", ""),
    }

    # Append to suppression log
    with open(SUPPRESSIONS_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(suppression_entry) + "\n")

    _log_feedback_routing(
        record_id=record_id,
        feedback_type="suppress_similar_items",
        action="suppression_logged",
        details={
            "suppression_terms": list(terms),
            "log_file": str(SUPPRESSIONS_LOG_FILE),
        },
    )

    return {
        "action": "suppression_logged",
        "suppression_entry": suppression_entry,
    }


def apply_feedback_to_memory(
    record: dict[str, Any], feedback: dict[str, Any]
) -> dict[str, Any]:
    """Route feedback to appropriate memory updates based on feedback type.

    This is the main entry point for applying feedback to memory systems.

    Args:
        record: The record being reviewed
        feedback: The human feedback dictionary

    Returns:
        Dictionary summarizing all memory updates
    """
    decision = feedback.get("decision", "")
    record_id = record.get("id", "")

    # Handle source_feedback if present
    source_feedback = feedback.get("source_feedback")
    if source_feedback == "bad_source":
        return handle_bad_source(record, feedback)
    elif source_feedback == "good_source":
        return handle_good_source(record, feedback)

    # Handle topic_feedback if present
    topic_feedback = feedback.get("topic_feedback")
    if topic_feedback == "expand_this_topic":
        return handle_expand_this_topic(record, feedback)

    # Handle main decision
    if decision == "bad_source":
        return handle_bad_source(record, feedback)
    elif decision == "good_source":
        return handle_good_source(record, feedback)
    elif decision == "expand_this_topic":
        return handle_expand_this_topic(record, feedback)
    elif decision == "approve_and_promote":
        return handle_approve_and_promote(record, feedback)
    elif decision == "approve_but_weak":
        return handle_approve_but_weak(record, feedback)
    elif decision == "suppress_similar_items":
        return handle_suppress_similar_items(record, feedback)
    elif decision in ("approve", "reject"):
        # Standard decisions - handled by route_record.py with generic outcomes
        return {
            "action": "standard_decision",
            "decision": decision,
        }
    else:
        return {
            "action": "unknown_feedback_type",
            "decision": decision,
        }


def main() -> None:
    """Main entry point for CLI usage."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Route human feedback to memory systems"
    )
    parser.add_argument(
        "record_path",
        nargs="?",
        type=Path,
        help="Path to record JSON file",
    )
    parser.add_argument(
        "--record-id",
        type=str,
        help="Record ID (used with --feedback-type)",
    )
    parser.add_argument(
        "--feedback-type",
        type=str,
        choices=[
            "bad_source",
            "good_source",
            "expand_this_topic",
            "approve_and_promote",
            "approve_but_weak",
            "suppress_similar_items",
        ],
        help="Type of feedback to apply",
    )
    parser.add_argument(
        "--notes",
        type=str,
        default="",
        help="Feedback notes",
    )
    parser.add_argument(
        "--reviewer-id",
        type=str,
        default="",
        help="Reviewer identifier",
    )

    args = parser.parse_args()

    if args.record_path and args.record_path.exists():
        # Load record from file
        with open(args.record_path, "r", encoding="utf-8") as f:
            record = json.load(f)

        # Load feedback from record if present
        feedback = record.get("human_feedback", {})
        if not feedback:
            print("No human_feedback block found in record")
            sys.exit(1)
    elif args.record_id:
        # Look for record in review_queue
        REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"
        record_path = REVIEW_QUEUE_DIR / f"{args.record_id}.json"

        if not record_path.exists():
            print(f"Record not found: {args.record_id}")
            sys.exit(1)

        with open(record_path, "r", encoding="utf-8") as f:
            record = json.load(f)

        feedback = {
            "decision": args.feedback_type or "approve",
            "notes": args.notes,
            "reviewer_id": args.reviewer_id,
            "reviewed_at": datetime.now(timezone.utc).isoformat(),
        }
    else:
        print("Either record_path or --record-id must be provided")
        parser.print_help()
        sys.exit(1)

    result = apply_feedback_to_memory(record, feedback)

    print(f"Feedback applied: {feedback.get('decision')}")
    print(f"Result: {result}")


if __name__ == "__main__":
    main()
