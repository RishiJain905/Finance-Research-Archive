"""Update Theme Memory Module.

Updates theme memory based on extracted terms from accepted/rejected records.
"""

import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_BASE_DIR = Path(__file__).resolve().parent.parent
if str(_BASE_DIR) not in sys.path:
    sys.path.insert(0, str(_BASE_DIR))

import scripts.theme_memory_persistence as theme_memory_persistence
from scripts.extract_theme_terms import (
    extract_text_from_record,
    tokenize_and_clean,
    extract_ngrams,
    extract_positive_candidates,
    extract_negative_candidates,
)


# =============================================================================
# Priority Score Computation
# =============================================================================


def compute_priority_score(theme: dict[str, Any]) -> int:
    """Compute priority score based on accepted/rejected/review counts.

    Formula:
        base = accepted_count * 10
        penalty = rejected_count * 5
        bonus = review_count * 3
        final = min(100, max(0, base - penalty + bonus))

    Args:
        theme: Theme dictionary with accepted_count, rejected_count, review_count

    Returns:
        Priority score between 0 and 100
    """
    accepted = theme.get("accepted_count", 0)
    rejected = theme.get("rejected_count", 0)
    review = theme.get("review_count", 0)

    base = accepted * 10
    penalty = rejected * 5
    bonus = review * 3

    score = base - penalty + bonus

    # Clamp to valid range
    return min(100, max(0, score))


# =============================================================================
# Theme Memory Load/Save
# =============================================================================


def load_theme_memory() -> dict[str, dict[str, Any]]:
    """Load existing themes from storage.

    Returns:
        Dictionary mapping theme_id to theme record
    """
    return theme_memory_persistence.get_all_themes()


def save_theme_memory(themes: dict[str, dict[str, Any]]) -> None:
    """Save updated themes to storage.

    Args:
        themes: Dictionary mapping theme_id to theme record
    """
    for theme_id, theme in themes.items():
        theme_memory_persistence.save_theme(theme_id, theme)


# Private versions that work directly with persistence
def _load_theme_memory() -> dict[str, dict[str, Any]]:
    """Load existing themes from storage."""
    return theme_memory_persistence.get_all_themes()


def _save_theme_memory(themes: dict[str, dict[str, Any]]) -> None:
    """Save updated themes to storage."""
    for theme_id, theme in themes.items():
        theme_memory_persistence.save_theme(theme_id, theme)


# =============================================================================
# Theme Creation and Update
# =============================================================================


def create_or_update_theme(
    existing_themes: dict[str, dict[str, Any]],
    new_theme_data: dict[str, Any],
    theme_id: str | None = None,
) -> dict[str, Any]:
    """Create new theme or update existing.

    Args:
        existing_themes: Current theme memory dictionary
        new_theme_data: Dictionary containing:
            - theme_label: Human-readable label
            - positive_terms: List of positive terms
            - negative_terms: List of negative terms
        theme_id: Optional theme ID to update existing theme

    Returns:
        Updated theme dictionary
    """
    # Determine theme ID
    if theme_id is None:
        theme_label = new_theme_data.get("theme_label", "")
        theme_id = _generate_theme_id(theme_label)

    # Check if theme exists
    if theme_id in existing_themes:
        theme = existing_themes[theme_id]
        # Update existing theme
        return _update_existing_theme(theme, new_theme_data)
    else:
        # Create new theme
        return _create_new_theme(theme_id, new_theme_data)


def _generate_theme_id(label: str) -> str:
    """Generate URL-safe slug from theme label.

    Args:
        label: Human-readable theme label

    Returns:
        Slug string
    """
    # Convert to lowercase
    slug = label.lower()

    # Replace spaces and special chars with hyphens
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[-\s]+", "-", slug)

    # Remove leading/trailing hyphens
    slug = slug.strip("-")

    return slug


def _create_new_theme(theme_id: str, theme_data: dict[str, Any]) -> dict[str, Any]:
    """Create a new theme record.

    Args:
        theme_id: Theme identifier (slug)
        theme_data: Theme data dictionary

    Returns:
        New theme dictionary
    """
    now = datetime.now(timezone.utc).isoformat()

    theme = {
        "theme_id": theme_id,
        "theme_label": theme_data.get(
            "theme_label", theme_id.replace("-", " ").title()
        ),
        "positive_terms": list(theme_data.get("positive_terms", [])),
        "negative_terms": list(theme_data.get("negative_terms", [])),
        "accepted_count": 1,
        "review_count": 0,
        "rejected_count": 0,
        "last_seen": now,
        "priority_score": 10,  # base = 1 * 10
    }

    # Recompute priority score
    theme["priority_score"] = compute_priority_score(theme)

    return theme


def _update_existing_theme(
    theme: dict[str, Any], new_data: dict[str, Any]
) -> dict[str, Any]:
    """Update an existing theme record.

    Args:
        theme: Existing theme dictionary
        new_data: New data to merge

    Returns:
        Updated theme dictionary
    """
    # Merge positive terms
    existing_positive = set(theme.get("positive_terms", []))
    new_positive = set(new_data.get("positive_terms", []))
    combined_positive = list(existing_positive | new_positive)

    # Merge negative terms
    existing_negative = set(theme.get("negative_terms", []))
    new_negative = set(new_data.get("negative_terms", []))
    combined_negative = list(existing_negative | new_negative)

    # Update counts based on how we were called
    # This is a simplified version - in production you'd track more state
    accepted_count = theme.get("accepted_count", 0) + 1

    now = datetime.now(timezone.utc).isoformat()

    updated_theme = {
        **theme,
        "positive_terms": combined_positive,
        "negative_terms": combined_negative,
        "accepted_count": accepted_count,
        "last_seen": now,
    }

    # Recompute priority score
    updated_theme["priority_score"] = compute_priority_score(updated_theme)

    return updated_theme


# =============================================================================
# Similar Theme Merging
# =============================================================================


def merge_similar_themes(
    themes: dict[str, dict[str, Any]], threshold: float = 0.7
) -> dict[str, dict[str, Any]]:
    """Merge themes with >threshold term overlap.

    Uses Jaccard similarity to find themes with significant term overlap.

    Args:
        themes: Dictionary of theme_id -> theme record
        threshold: Similarity threshold (0.0 to 1.0)

    Returns:
        Dictionary of merged themes
    """
    if not themes:
        return {}

    # Build list of theme items
    theme_items = list(themes.items())
    merged_ids = set()
    result = {}

    for i, (id_a, theme_a) in enumerate(theme_items):
        if id_a in merged_ids:
            continue

        # Start with this theme as base
        base_theme = dict(theme_a)

        for id_b, theme_b in theme_items[i + 1 :]:
            if id_b in merged_ids:
                continue

            similarity = _calculate_term_similarity(base_theme, theme_b)

            if similarity >= threshold:
                # Merge theme_b into base_theme
                base_theme = _merge_two_themes(base_theme, theme_b)
                merged_ids.add(id_b)

        merged_ids.add(id_a)
        result[id_a] = base_theme

    return result


def _calculate_term_similarity(
    theme_a: dict[str, Any], theme_b: dict[str, Any]
) -> float:
    """Calculate Jaccard similarity between two themes based on positive terms.

    Args:
        theme_a: First theme
        theme_b: Second theme

    Returns:
        Similarity score between 0.0 and 1.0
    """
    terms_a = set(theme_a.get("positive_terms", []))
    terms_b = set(theme_b.get("positive_terms", []))

    if not terms_a and not terms_b:
        return 0.0

    # Jaccard similarity: intersection / union
    intersection = len(terms_a & terms_b)
    union = len(terms_a | terms_b)

    if union == 0:
        return 0.0

    return intersection / union


def _merge_two_themes(
    theme_a: dict[str, Any], theme_b: dict[str, Any]
) -> dict[str, Any]:
    """Merge two themes together.

    Args:
        theme_a: Base theme (will be updated)
        theme_b: Theme to merge in

    Returns:
        Merged theme
    """
    # Combine positive terms
    positive_a = set(theme_a.get("positive_terms", []))
    positive_b = set(theme_b.get("positive_terms", []))
    combined_positive = list(positive_a | positive_b)

    # Combine negative terms
    negative_a = set(theme_a.get("negative_terms", []))
    negative_b = set(theme_b.get("negative_terms", []))
    combined_negative = list(negative_a | negative_b)

    # Combine counts (use max for overlapping themes - they're the same)
    accepted_count = max(
        theme_a.get("accepted_count", 0), theme_b.get("accepted_count", 0)
    )
    review_count = max(theme_a.get("review_count", 0), theme_b.get("review_count", 0))
    rejected_count = max(
        theme_a.get("rejected_count", 0), theme_b.get("rejected_count", 0)
    )

    # Use most recent last_seen
    last_seen_a = theme_a.get("last_seen", "")
    last_seen_b = theme_b.get("last_seen", "")
    last_seen = (
        max(last_seen_a, last_seen_b)
        if last_seen_a and last_seen_b
        else last_seen_a or last_seen_b
    )

    # Use higher priority score
    priority_score = max(
        theme_a.get("priority_score", 0),
        theme_b.get("priority_score", 0),
    )

    merged = {
        "theme_id": theme_a.get("theme_id", theme_b.get("theme_id")),
        "theme_label": theme_a.get("theme_label", theme_b.get("theme_label")),
        "positive_terms": combined_positive,
        "negative_terms": combined_negative,
        "accepted_count": accepted_count,
        "review_count": review_count,
        "rejected_count": rejected_count,
        "last_seen": last_seen,
        "priority_score": priority_score,
    }

    return merged


# =============================================================================
# Process Records
# =============================================================================


def process_accepted_record(
    record: dict[str, Any], themes: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """Update themes based on new accepted record.

    Args:
        record: Accepted record dictionary
        themes: Current theme memory

    Returns:
        Updated themes dictionary
    """
    # Extract terms from record
    text = extract_text_from_record(record)
    tokens = tokenize_and_clean(text)
    terms = extract_ngrams(tokens, n_range=(1, 2))

    # Find matching themes
    matching_theme_id = None
    for theme_id, theme in themes.items():
        theme_terms = set(theme.get("positive_terms", []))
        # Check if any theme term appears in record
        if theme_terms & terms:  # Intersection
            matching_theme_id = theme_id
            break

    # Prepare new theme data
    positive_terms = list(terms)
    negative_terms = []  # Accepted records don't add negative terms

    new_theme_data = {
        "theme_label": record.get("title", "Unknown Theme")[:50],
        "positive_terms": positive_terms,
        "negative_terms": negative_terms,
    }

    if matching_theme_id:
        # Update existing theme
        themes[matching_theme_id] = _update_existing_theme(
            themes[matching_theme_id], new_theme_data
        )
    else:
        # Create new theme
        new_theme = create_or_update_theme(themes, new_theme_data)
        themes[new_theme["theme_id"]] = new_theme

    return themes


def apply_topic_expansion(
    record: dict[str, Any], themes: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """Apply topic expansion feedback to theme memory.

    When a reviewer marks expand_this_topic, increase the priority score
    for matching themes and add the record's topic/terms as positive evidence.

    Args:
        record: The accepted record with topic/theme information
        themes: Current theme memory dictionary

    Returns:
        Updated themes dictionary
    """
    # Get the record's topic
    topic = record.get("topic", "")
    if not topic:
        return themes

    # Normalize topic for matching
    topic_normalized = topic.lower().strip()

    # Get key_points as positive terms
    key_points = record.get("key_points", [])
    positive_terms = []
    if isinstance(key_points, list):
        for point in key_points:
            if isinstance(point, str):
                positive_terms.append(point.lower().strip())
            elif isinstance(point, dict):
                # Handle structured key_points
                term = point.get("term", "") or point.get("point", "")
                if term:
                    positive_terms.append(term.lower().strip())

    # Also try to get terms from title and abstract
    title = record.get("title", "")
    if title:
        positive_terms.append(title.lower().strip())

    # Find themes matching the record's topic
    matching_theme_id = None
    for theme_id, theme in themes.items():
        theme_label = theme.get("theme_label", "").lower()
        theme_terms = set(theme.get("positive_terms", []))
        # Check if topic matches theme label or any theme term
        if topic_normalized == theme_label or topic_normalized in theme_terms:
            matching_theme_id = theme_id
            break

    now = datetime.now(timezone.utc).isoformat()

    if matching_theme_id:
        # Update existing theme
        theme = themes[matching_theme_id]
        theme["priority_score"] = min(100, theme.get("priority_score", 0) + 20)
        theme["accepted_count"] = theme.get("accepted_count", 0) + 1
        theme["last_seen"] = now
        # Add new positive terms
        existing_positive = set(theme.get("positive_terms", []))
        new_positive = set(positive_terms)
        theme["positive_terms"] = list(existing_positive | new_positive)
        themes[matching_theme_id] = theme
    else:
        # Create a new theme with the record's topic as label
        new_theme_data = {
            "theme_label": topic,
            "positive_terms": positive_terms,
            "negative_terms": [],
        }
        new_theme = _create_new_theme(
            theme_id=_generate_theme_id(topic),
            theme_data=new_theme_data,
        )
        new_theme["priority_score"] = 30  # Base priority for new expansion theme
        themes[new_theme["theme_id"]] = new_theme

    return themes


def process_rejected_record(
    record: dict[str, Any], themes: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """Update negative terms based on rejected record.

    Args:
        record: Rejected record dictionary
        themes: Current theme memory

    Returns:
        Updated themes dictionary
    """
    # Extract negative candidates from rejected record
    negative_candidates = extract_negative_candidates([record], min_occurrences=1)

    # For rejected records, add negative terms to themes
    # We don't know which theme it was supposed to match,
    # so we add to all themes as potential negative evidence
    for theme_id, theme in themes.items():
        existing_negative = set(theme.get("negative_terms", []))
        new_negative = set(negative_candidates)
        combined_negative = list(existing_negative | new_negative)

        theme["negative_terms"] = combined_negative
        theme["rejected_count"] = theme.get("rejected_count", 0) + 1
        theme["priority_score"] = compute_priority_score(theme)

    # If no themes exist, create a "noise" theme
    if not themes:
        noise_theme_data = {
            "theme_label": "Rejected Noise",
            "positive_terms": [],
            "negative_terms": negative_candidates,
        }
        new_theme = create_or_update_theme({}, noise_theme_data)
        themes[new_theme["theme_id"]] = new_theme

    return themes


# =============================================================================
# CLI Interface
# =============================================================================


def main():
    """Main entry point for CLI usage."""
    import argparse
    import json
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Update theme memory based on records")
    parser.add_argument(
        "--accepted-dir",
        type=Path,
        help="Directory containing accepted records JSON files",
    )
    parser.add_argument(
        "--rejected-dir",
        type=Path,
        help="Directory containing rejected records JSON files",
    )
    parser.add_argument(
        "--process-accepted",
        action="store_true",
        help="Process accepted records",
    )
    parser.add_argument(
        "--process-rejected",
        action="store_true",
        help="Process rejected records",
    )
    parser.add_argument(
        "--merge-threshold",
        type=float,
        default=0.7,
        help="Threshold for merging similar themes",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print theme memory statistics",
    )
    parser.add_argument(
        "--expand-topic",
        type=str,
        metavar="RECORD_FILE",
        help="Apply topic expansion from a specific accepted record file",
    )

    args = parser.parse_args()

    # Load existing themes
    themes = load_theme_memory()

    # Process accepted records
    if args.process_accepted and args.accepted_dir:
        accepted_records = []
        if args.accepted_dir.exists():
            for json_file in args.accepted_dir.glob("*.json"):
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        accepted_records.extend(data)
                    elif isinstance(data, dict) and "records" in data:
                        accepted_records.extend(data["records"])

        for record in accepted_records:
            themes = process_accepted_record(record, themes)

    # Process rejected records
    if args.process_rejected and args.rejected_dir:
        rejected_records = []
        if args.rejected_dir.exists():
            for json_file in args.rejected_dir.glob("*.json"):
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        rejected_records.extend(data)
                    elif isinstance(data, dict) and "records" in data:
                        rejected_records.extend(data["records"])

        for record in rejected_records:
            themes = process_rejected_record(record, themes)

    # Apply topic expansion from a specific record
    if args.expand_topic:
        record_path = Path(args.expand_topic)
        if record_path.exists():
            with open(record_path, "r", encoding="utf-8") as f:
                record = json.load(f)
            themes = apply_topic_expansion(record, themes)
            print(f"Applied topic expansion from: {record_path.name}")
        else:
            print(f"Warning: Record file not found: {record_path}")

    # Merge similar themes
    if args.merge_threshold > 0:
        themes = merge_similar_themes(themes, threshold=args.merge_threshold)

    # Save updated themes
    save_theme_memory(themes)

    # Print stats if requested
    if args.stats:
        print(f"Total themes: {len(themes)}")
        for theme_id, theme in themes.items():
            print(f"  {theme_id}:")
            print(f"    Label: {theme.get('theme_label')}")
            print(f"    Priority: {theme.get('priority_score')}")
            print(f"    Accepted: {theme.get('accepted_count')}")
            print(f"    Rejected: {theme.get('rejected_count')}")
            print(f"    Positive terms: {len(theme.get('positive_terms', []))}")
            print(f"    Negative terms: {len(theme.get('negative_terms', []))}")


if __name__ == "__main__":
    main()
