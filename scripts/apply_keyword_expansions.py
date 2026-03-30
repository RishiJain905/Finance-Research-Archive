"""Keyword Expansion Application Module.

Applies approved (or auto-activated) keyword expansions to keyword bundles.
Handles adding terms to existing bundles, creating new bundles,
and adjusting bundle priorities.
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Memory storage paths
MEMORY_DIR = BASE_DIR / "data" / "theme_memory"
EXPANSIONS_PATH = MEMORY_DIR / "expansions.json"

# Config paths
CONFIG_DIR = BASE_DIR / "config"
BUNDLES_PATH = CONFIG_DIR / "keyword_bundles.json"
NEG_BUNDLES_PATH = CONFIG_DIR / "negative_keyword_bundles.json"

# Auto-activation thresholds
HIGH_CONFIDENCE_THRESHOLD = 80
MEDIUM_CONFIDENCE_THRESHOLD = 50


def load_expansions() -> list[dict[str, Any]]:
    """Load pending expansions from theme_memory/expansions.json.

    Returns:
        List of proposal dictionaries
    """
    if not EXPANSIONS_PATH.exists():
        return []

    try:
        with open(EXPANSIONS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("proposals", [])
    except (json.JSONDecodeError, IOError):
        return []


def load_keyword_bundles() -> dict[str, Any]:
    """Load keyword bundles from config/keyword_bundles.json.

    Returns:
        Dictionary mapping bundle_id to bundle data
    """
    if not BUNDLES_PATH.exists():
        return {"bundles": {}}

    try:
        with open(BUNDLES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("bundles", {})
    except (json.JSONDecodeError, IOError):
        return {}


def load_negative_bundles() -> dict[str, Any]:
    """Load negative keyword bundles from config/negative_keyword_bundles.json.

    Returns:
        Dictionary mapping bundle_id to negative bundle data
    """
    if not NEG_BUNDLES_PATH.exists():
        return {}

    try:
        with open(NEG_BUNDLES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("negative_bundles", {})
    except (json.JSONDecodeError, IOError):
        return {}


def save_keyword_bundles(bundles: dict[str, Any]) -> None:
    """Save keyword bundles to config/keyword_bundles.json.

    Args:
        bundles: Dictionary of bundle data
    """
    BUNDLES_PATH.parent.mkdir(parents=True, exist_ok=True)

    data = {"bundles": bundles}

    # Atomic write
    temp_path = BUNDLES_PATH.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    temp_path.replace(BUNDLES_PATH)


def save_negative_bundles(bundles: dict[str, Any]) -> None:
    """Save negative keyword bundles to config/negative_keyword_bundles.json.

    Args:
        bundles: Dictionary of negative bundle data
    """
    NEG_BUNDLES_PATH.parent.mkdir(parents=True, exist_ok=True)

    data = {"negative_bundles": bundles}

    # Atomic write
    temp_path = NEG_BUNDLES_PATH.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    temp_path.replace(NEG_BUNDLES_PATH)


def save_expansions(expansions: list[dict[str, Any]]) -> None:
    """Save expansions to theme_memory/expansions.json.

    Args:
        expansions: List of proposal dictionaries
    """
    EXPANSIONS_PATH.parent.mkdir(parents=True, exist_ok=True)

    data = {
        "proposals": expansions,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }

    # Atomic write
    temp_path = EXPANSIONS_PATH.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    temp_path.replace(EXPANSIONS_PATH)


def apply_bundle_addition(
    expansion: dict[str, Any], bundles: dict[str, Any]
) -> dict[str, Any]:
    """Add terms to an existing bundle.

    Args:
        expansion: Expansion proposal with terms_to_add and target_bundle_id
        bundles: Dictionary of bundle data

    Returns:
        Updated bundles dictionary

    Raises:
        ValueError: If target bundle not found
    """
    target_id = expansion["target_bundle_id"]
    terms_to_add = expansion.get("terms_to_add", [])

    if target_id not in bundles:
        raise ValueError(f"Bundle '{target_id}' not found")

    # Add terms without duplicates (case-insensitive)
    existing_keywords = set(k.lower() for k in bundles[target_id].get("keywords", []))
    for term in terms_to_add:
        if term.lower() not in existing_keywords:
            bundles[target_id]["keywords"].append(term)
            existing_keywords.add(term.lower())

    bundles[target_id]["last_updated"] = datetime.now(timezone.utc).isoformat()

    return bundles


def apply_new_bundle(
    expansion: dict[str, Any], bundles: dict[str, Any]
) -> dict[str, Any]:
    """Create a new bundle from an expansion.

    Args:
        expansion: Expansion proposal with new_bundle_label and terms_to_add
        bundles: Dictionary of bundle data

    Returns:
        Updated bundles dictionary
    """
    # Generate bundle_id if not provided
    bundle_id = expansion.get("target_bundle_id")
    if not bundle_id or bundle_id in bundles:
        # Generate from label
        label = expansion.get("new_bundle_label", "New Bundle")
        base_id = label.lower().replace(" ", "_")
        bundle_id = base_id
        counter = 1
        while bundle_id in bundles:
            bundle_id = f"{base_id}_{counter}"
            counter += 1

    new_bundle = {
        "bundle_id": bundle_id,
        "label": expansion.get("new_bundle_label", "New Bundle"),
        "keywords": list(expansion.get("terms_to_add", [])),
        "priority": 2,  # Default priority
        "created_at": datetime.now(timezone.utc).isoformat(),
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }

    bundles[bundle_id] = new_bundle

    return bundles


def apply_priority_increase(
    expansion: dict[str, Any], bundles: dict[str, Any]
) -> dict[str, Any]:
    """Increase a bundle's priority.

    Args:
        expansion: Expansion proposal with target_bundle_id and new_priority
        bundles: Dictionary of bundle data

    Returns:
        Updated bundles dictionary

    Raises:
        ValueError: If target bundle not found
    """
    target_id = expansion["target_bundle_id"]
    new_priority = expansion.get("new_priority", 3)

    if target_id not in bundles:
        raise ValueError(f"Bundle '{target_id}' not found")

    bundles[target_id]["priority"] = new_priority
    bundles[target_id]["last_updated"] = datetime.now(timezone.utc).isoformat()

    return bundles


def apply_negative_bundle(
    expansion: dict[str, Any], negative_bundles: dict[str, Any]
) -> dict[str, Any]:
    """Add terms to a negative bundle.

    Args:
        expansion: Expansion proposal with terms_to_add and target_bundle_id
        negative_bundles: Dictionary of negative bundle data

    Returns:
        Updated negative bundles dictionary
    """
    target_id = expansion.get("target_bundle_id")
    terms_to_add = expansion.get("terms_to_add", [])

    # Create bundle if it doesn't exist
    if target_id not in negative_bundles:
        negative_bundles[target_id] = {
            "bundle_id": target_id,
            "label": f"Negative - {target_id}",
            "negative_keywords": [],
            "priority": 1,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    # Add terms without duplicates
    existing = set(
        k.lower() for k in negative_bundles[target_id].get("negative_keywords", [])
    )
    for term in terms_to_add:
        if term.lower() not in existing:
            negative_bundles[target_id]["negative_keywords"].append(term)
            existing.add(term.lower())

    negative_bundles[target_id]["last_updated"] = datetime.now(timezone.utc).isoformat()

    return negative_bundles


def mark_expansion_applied(expansion: dict[str, Any]) -> dict[str, Any]:
    """Mark an expansion as applied.

    Args:
        expansion: Expansion proposal dictionary

    Returns:
        Updated expansion dictionary
    """
    expansion["status"] = "applied"
    expansion["applied_at"] = datetime.now(timezone.utc).isoformat()
    return expansion


def auto_activate_high_confidence(
    expansions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Auto-activate or reject expansions based on confidence score.

    Rules:
    - confidence >= 80: auto-activate (status = "approved", auto_activated = true)
    - confidence 50-79: require human approval (status = "pending")
    - confidence < 50: reject (status = "rejected")

    Args:
        expansions: List of expansion proposals

    Returns:
        Updated expansions list
    """
    for expansion in expansions:
        # Only process pending expansions
        if expansion.get("status") != "pending":
            continue

        confidence = expansion.get("confidence_score", 0)

        if confidence >= HIGH_CONFIDENCE_THRESHOLD:
            expansion["status"] = "approved"
            expansion["auto_activated"] = True
            expansion["reviewed_at"] = datetime.now(timezone.utc).isoformat()
            expansion["reviewed_by"] = "system"
        elif confidence < MEDIUM_CONFIDENCE_THRESHOLD:
            expansion["status"] = "rejected"
            expansion["auto_activated"] = False
            expansion["reviewed_at"] = datetime.now(timezone.utc).isoformat()
            expansion["reviewed_by"] = "system"
        # else: keep as pending for human review

    return expansions


def main() -> dict[str, Any]:
    """Main entry point for applying keyword expansions.

    Returns:
        Dictionary with application results
    """
    results = {
        "applied": 0,
        "skipped": 0,
        "errors": 0,
    }

    # Load data
    expansions = load_expansions()
    bundles = load_keyword_bundles()
    negative_bundles = load_negative_bundles()

    # Auto-activate high confidence expansions first
    expansions = auto_activate_high_confidence(expansions)

    # Process expansions
    for expansion in expansions:
        # Skip non-approved expansions
        if expansion.get("status") != "approved":
            results["skipped"] += 1
            continue

        expansion_type = expansion.get("type")

        try:
            if expansion_type == "add_to_bundle":
                bundles = apply_bundle_addition(expansion, bundles)
            elif expansion_type == "create_bundle":
                bundles = apply_new_bundle(expansion, bundles)
            elif expansion_type == "increase_priority":
                bundles = apply_priority_increase(expansion, bundles)
            elif expansion_type == "add_to_negative_bundle":
                negative_bundles = apply_negative_bundle(expansion, negative_bundles)
            else:
                # Unknown type, skip
                results["skipped"] += 1
                continue

            # Mark as applied
            mark_expansion_applied(expansion)
            results["applied"] += 1

        except Exception as e:
            expansion["status"] = "error"
            expansion["error_message"] = str(e)
            results["errors"] += 1

    # Save updated data
    save_keyword_bundles(bundles)
    save_negative_bundles(negative_bundles)
    save_expansions(expansions)

    return results


if __name__ == "__main__":
    result = main()
    print(
        f"Applied {result['applied']} expansions, "
        f"skipped {result['skipped']}, "
        f"errors {result['errors']}"
    )
