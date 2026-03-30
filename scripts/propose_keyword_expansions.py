"""Keyword Expansion Proposal Module.

Generates keyword expansion proposals based on theme memory analysis.
Proposals include adding terms to bundles, creating new bundles, and
increasing bundle priorities based on theme confidence scores.
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Memory storage paths
MEMORY_DIR = BASE_DIR / "data" / "theme_memory"
THEMES_PATH = MEMORY_DIR / "themes.json"
EXPANSIONS_PATH = MEMORY_DIR / "expansions.json"

# Config paths
CONFIG_DIR = BASE_DIR / "config"
BUNDLES_PATH = CONFIG_DIR / "keyword_bundles.json"

# Auto-activation threshold
HIGH_CONFIDENCE_THRESHOLD = 80
MEDIUM_CONFIDENCE_THRESHOLD = 50


def load_themes() -> dict[str, Any]:
    """Load themes from theme_memory/themes.json.

    Returns:
        Dictionary mapping theme_id to theme data
    """
    if not THEMES_PATH.exists():
        return {}

    try:
        with open(THEMES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("themes", {})
    except (json.JSONDecodeError, IOError):
        return {}


def load_keyword_bundles() -> dict[str, Any]:
    """Load existing keyword bundles from config/keyword_bundles.json.

    Returns:
        Dictionary mapping bundle_id to bundle data
    """
    if not BUNDLES_PATH.exists():
        return {}

    try:
        with open(BUNDLES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("bundles", {})
    except (json.JSONDecodeError, IOError):
        return {}


def calculate_theme_confidence(theme: dict[str, Any]) -> float:
    """Calculate confidence score for a theme.

    confidence = (accepted_count / (accepted_count + rejected_count + 1)) * 100

    Args:
        theme: Theme dictionary with accepted_count and rejected_count

    Returns:
        Confidence score as percentage (0-100)
    """
    accepted_count = theme.get("accepted_count", 0)
    rejected_count = theme.get("rejected_count", 0)

    denominator = accepted_count + rejected_count + 1
    confidence = (accepted_count / denominator) * 100

    return round(confidence, 2)


def identify_expansion_opportunities(
    themes: dict[str, Any], bundles: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    """Find themes that represent expansion opportunities.

    An opportunity exists when:
    - Theme is not in any bundle (create_bundle)
    - Theme has terms not in any bundle (add_to_bundle)
    - Theme has high confidence but low priority bundle (increase_priority)

    Args:
        themes: Dictionary of theme data
        bundles: Dictionary of bundle data

    Returns:
        Dictionary mapping theme_id to opportunity details
    """
    opportunities = {}

    for theme_id, theme in themes.items():
        terms = set(theme.get("terms", []))
        if not terms:
            continue

        # Find bundles that contain any of this theme's terms
        matching_bundles = []
        missing_terms = set(terms)

        for bundle_id, bundle in bundles.items():
            bundle_keywords = set(k.lower() for k in bundle.get("keywords", []))
            overlap = terms & bundle_keywords
            if overlap:
                matching_bundles.append(bundle_id)
                missing_terms -= overlap

        confidence = calculate_theme_confidence(theme)

        # Determine the best action
        if not matching_bundles:
            # No matching bundle - propose creating new one
            opportunities[theme_id] = {
                "theme": theme,
                "action": "create_bundle",
                "missing_terms": list(terms),
                "confidence": confidence,
            }
        elif missing_terms:
            # Some terms not covered - propose adding to best matching bundle
            # Choose bundle with highest priority among matching ones
            best_bundle = None
            best_priority = -1
            for bundle_id in matching_bundles:
                priority = bundles[bundle_id].get("priority", 1)
                if priority > best_priority:
                    best_priority = priority
                    best_bundle = bundle_id

            opportunities[theme_id] = {
                "theme": theme,
                "action": "add_to_bundle",
                "target_bundle_id": best_bundle,
                "missing_terms": list(missing_terms),
                "confidence": confidence,
            }
        else:
            # All terms are covered by bundles - check if we should recommend priority increase
            # Only if high confidence and low priority
            best_bundle = None
            best_priority = -1
            for bundle_id in matching_bundles:
                priority = bundles[bundle_id].get("priority", 1)
                if priority > best_priority:
                    best_priority = priority
                    best_bundle = bundle_id

            if confidence >= HIGH_CONFIDENCE_THRESHOLD and best_priority < 3:
                opportunities[theme_id] = {
                    "theme": theme,
                    "action": "increase_priority",
                    "target_bundle_id": best_bundle,
                    "missing_terms": [],
                    "confidence": confidence,
                    "current_priority": best_priority,
                }

    return opportunities


def propose_bundle_addition(
    theme: dict[str, Any], bundle: dict[str, Any]
) -> dict[str, Any]:
    """Propose adding theme terms to an existing bundle.

    Args:
        theme: Theme dictionary
        bundle: Bundle dictionary to add terms to

    Returns:
        Proposal dictionary
    """
    confidence = calculate_theme_confidence(theme)

    return {
        "proposal_id": str(uuid.uuid4()),
        "type": "add_to_bundle",
        "target_bundle_id": bundle["bundle_id"],
        "new_bundle_label": None,
        "terms_to_add": theme.get("terms", []),
        "confidence_score": confidence,
        "auto_activate": confidence >= HIGH_CONFIDENCE_THRESHOLD,
        "status": "pending",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "reviewed_at": None,
        "reviewed_by": None,
    }


def propose_new_bundle(theme: dict[str, Any]) -> dict[str, Any]:
    """Propose creating a new bundle from a theme.

    Args:
        theme: Theme dictionary

    Returns:
        Proposal dictionary
    """
    confidence = calculate_theme_confidence(theme)

    return {
        "proposal_id": str(uuid.uuid4()),
        "type": "create_bundle",
        "target_bundle_id": None,
        "new_bundle_label": theme.get("label", theme["theme_id"]),
        "terms_to_add": theme.get("terms", []),
        "confidence_score": confidence,
        "auto_activate": confidence >= HIGH_CONFIDENCE_THRESHOLD,
        "status": "pending",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "reviewed_at": None,
        "reviewed_by": None,
    }


def propose_priority_increase(
    theme: dict[str, Any], bundle: dict[str, Any]
) -> dict[str, Any]:
    """Propose raising a bundle's priority.

    Args:
        theme: Theme dictionary
        bundle: Bundle dictionary

    Returns:
        Proposal dictionary
    """
    confidence = calculate_theme_confidence(theme)
    current_priority = bundle.get("priority", 1)

    return {
        "proposal_id": str(uuid.uuid4()),
        "type": "increase_priority",
        "target_bundle_id": bundle["bundle_id"],
        "new_bundle_label": None,
        "current_priority": current_priority,
        "new_priority": current_priority + 2,  # Increase by 2
        "terms_to_add": theme.get("terms", []),
        "confidence_score": confidence,
        "auto_activate": confidence >= HIGH_CONFIDENCE_THRESHOLD,
        "status": "pending",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "reviewed_at": None,
        "reviewed_by": None,
    }


def generate_proposals(
    themes: dict[str, Any], bundles: dict[str, Any], min_confidence: float = 50.0
) -> list[dict[str, Any]]:
    """Generate all keyword expansion proposals.

    Args:
        themes: Dictionary of theme data
        bundles: Dictionary of bundle data
        min_confidence: Minimum confidence score to generate proposal

    Returns:
        List of proposal dictionaries
    """
    proposals = []
    opportunities = identify_expansion_opportunities(themes, bundles)

    for theme_id, opportunity in opportunities.items():
        theme = opportunity["theme"]
        confidence = opportunity["confidence"]

        # Skip if below minimum confidence threshold
        if confidence < min_confidence:
            continue

        action = opportunity["action"]

        if action == "create_bundle":
            proposal = propose_new_bundle(theme)
            proposals.append(proposal)

        elif action == "add_to_bundle":
            target_bundle = bundles[opportunity["target_bundle_id"]]
            # Create proposal with only missing terms
            theme_copy = theme.copy()
            theme_copy["terms"] = opportunity["missing_terms"]
            proposal = propose_bundle_addition(theme_copy, target_bundle)
            proposals.append(proposal)

        elif action == "increase_priority":
            target_bundle = bundles[opportunity["target_bundle_id"]]
            proposal = propose_priority_increase(theme, target_bundle)
            proposals.append(proposal)

    return proposals


def save_proposals(proposals: list[dict[str, Any]]) -> None:
    """Save proposals to theme_memory/expansions.json.

    Args:
        proposals: List of proposal dictionaries
    """
    EXPANSIONS_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Load existing expansions file if it exists
    existing_data = {}
    if EXPANSIONS_PATH.exists():
        try:
            with open(EXPANSIONS_PATH, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
        except (json.JSONDecodeError, IOError):
            existing_data = {}

    # Update proposals list
    existing_data["proposals"] = proposals
    existing_data["last_updated"] = datetime.now(timezone.utc).isoformat()

    # Atomic write
    temp_path = EXPANSIONS_PATH.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, indent=2)
    temp_path.replace(EXPANSIONS_PATH)


def main() -> dict[str, Any]:
    """Main entry point for proposal generation.

    Returns:
        Dictionary with generation results
    """
    # Load data
    themes = load_themes()
    bundles = load_keyword_bundles()

    # Generate proposals
    proposals = generate_proposals(
        themes, bundles, min_confidence=MEDIUM_CONFIDENCE_THRESHOLD
    )

    # Save proposals
    if proposals:
        save_proposals(proposals)

    return {
        "themes_found": len(themes),
        "bundles_found": len(bundles),
        "proposals_generated": len(proposals),
        "auto_activated": sum(1 for p in proposals if p.get("auto_activate")),
    }


if __name__ == "__main__":
    result = main()
    print(
        f"Generated {result['proposals_generated']} proposals "
        f"({result['auto_activated']} auto-activated) from "
        f"{result['themes_found']} themes and {result['bundles_found']} bundles"
    )
