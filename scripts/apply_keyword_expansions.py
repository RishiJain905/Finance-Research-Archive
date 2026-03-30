"""Apply Keyword Expansions Module.

Applies approved keyword expansions to bundles and theme memory.
Part of Stream C - Theme Memory Integration.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from scripts.theme_memory_persistence import (
    load_keyword_bundles,
    save_theme,
    initialize_theme,
    save_negative_bundle,
    initialize_negative_bundle,
    get_themes,
    get_negative_bundles,
)

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent
KEYWORD_BUNDLES_PATH = BASE_DIR / "config" / "keyword_bundles.json"


def apply_keyword_expansion(
    term: str,
    bundle_id: str,
    term_type: str = "optional",
) -> dict[str, Any]:
    """Apply a single keyword expansion to a bundle.

    Args:
        term: The keyword term to add
        bundle_id: Target bundle ID
        term_type: Type of term ('required' or 'optional')

    Returns:
        Result dictionary with success status and details
    """
    bundles_config = load_keyword_bundles()
    bundles = bundles_config.get("bundles", {})

    if bundle_id not in bundles:
        return {
            "success": False,
            "error": f"Bundle '{bundle_id}' not found",
        }

    bundle = bundles[bundle_id]

    if bundle.get("is_negative", False):
        return {
            "success": False,
            "error": "Cannot add positive terms to negative bundle",
        }

    # Add term to appropriate list
    if term_type == "required":
        if term not in bundle.get("required_terms", []):
            bundle["required_terms"].append(term)
    else:
        if term not in bundle.get("optional_terms", []):
            bundle["optional_terms"].append(term)

    # Save updated bundles
    _save_bundles(bundles_config)

    return {
        "success": True,
        "bundle_id": bundle_id,
        "term": term,
        "term_type": term_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def apply_negative_term_expansion(
    term: str,
    bundle_id: str = "negative_signals",
    penalty_strength: float = 30.0,
) -> dict[str, Any]:
    """Apply a negative term expansion.

    Args:
        term: The keyword term to add to negative signals
        bundle_id: Target bundle ID (default: negative_signals)
        penalty_strength: Strength of the penalty (0-100)

    Returns:
        Result dictionary with success status and details
    """
    bundles_config = load_keyword_bundles()
    bundles = bundles_config.get("bundles", {})

    if bundle_id not in bundles:
        return {
            "success": False,
            "error": f"Bundle '{bundle_id}' not found",
        }

    bundle = bundles[bundle_id]

    if not bundle.get("is_negative", False):
        return {
            "success": False,
            "error": f"Bundle '{bundle_id}' is not a negative bundle",
        }

    # Add term to optional_terms
    if term not in bundle.get("optional_terms", []):
        bundle["optional_terms"].append(term)

    # Save updated bundles
    _save_bundles(bundles_config)

    return {
        "success": True,
        "bundle_id": bundle_id,
        "term": term,
        "penalty_strength": penalty_strength,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def bulk_apply_expansions(
    expansions: list[dict[str, Any]],
) -> dict[str, Any]:
    """Apply multiple keyword expansions at once.

    Args:
        expansions: List of expansion dictionaries with:
            - term: The keyword term
            - bundle_id: Target bundle ID
            - term_type: 'required' or 'optional' (for positive)
            - is_negative: True if adding to negative bundle
            - penalty_strength: Penalty strength (for negative)

    Returns:
        Result dictionary with success count and failures
    """
    results = []
    success_count = 0
    failure_count = 0

    for expansion in expansions:
        term = expansion.get("term")
        bundle_id = expansion.get("bundle_id")
        is_negative = expansion.get("is_negative", False)

        if not term or not bundle_id:
            results.append(
                {
                    "success": False,
                    "error": "Missing term or bundle_id",
                    "expansion": expansion,
                }
            )
            failure_count += 1
            continue

        if is_negative:
            result = apply_negative_term_expansion(
                term=term,
                bundle_id=bundle_id,
                penalty_strength=expansion.get("penalty_strength", 30.0),
            )
        else:
            result = apply_keyword_expansion(
                term=term,
                bundle_id=bundle_id,
                term_type=expansion.get("term_type", "optional"),
            )

        if result.get("success", False):
            success_count += 1
        else:
            failure_count += 1

        results.append(result)

    return {
        "total": len(expansions),
        "success_count": success_count,
        "failure_count": failure_count,
        "results": results,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def learn_theme_from_proposal(
    proposal: dict[str, Any],
    priority: float = None,
) -> dict[str, Any]:
    """Learn a new theme from a proposal.

    Args:
        proposal: Theme proposal dictionary from propose_keyword_expansions
        priority: Override priority (uses proposal value if None)

    Returns:
        Result dictionary with success status
    """
    bundle_id = proposal.get("bundle_id")
    keywords = proposal.get("keywords", [])
    source_candidate_id = proposal.get("source_candidate_id")

    if not bundle_id or not keywords:
        return {
            "success": False,
            "error": "Missing bundle_id or keywords in proposal",
        }

    if priority is None:
        priority = proposal.get("priority", 50.0)

    # Check if theme already exists
    existing_themes = get_themes()
    for existing in existing_themes.values():
        if existing.get("bundle_id") == bundle_id:
            return {
                "success": False,
                "error": f"Theme for bundle '{bundle_id}' already exists",
            }

    # Initialize and save theme
    theme = initialize_theme(
        bundle_id=bundle_id,
        keywords=keywords,
        priority=priority,
        source_candidate_id=source_candidate_id,
    )

    save_theme(
        theme_id=theme["theme_id"],
        theme_data=theme,
        priority=priority,
        matched_terms=keywords,
        source_candidate_id=source_candidate_id,
    )

    return {
        "success": True,
        "theme_id": theme["theme_id"],
        "bundle_id": bundle_id,
        "keywords": keywords,
        "priority": priority,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def learn_negative_bundle_from_proposal(
    proposal: dict[str, Any],
    penalty_strength: float = None,
) -> dict[str, Any]:
    """Learn a negative bundle from a proposal.

    Args:
        proposal: Negative bundle proposal dictionary
        penalty_strength: Override penalty strength (uses proposal value if None)

    Returns:
        Result dictionary with success status
    """
    bundle_id = proposal.get("bundle_id", "negative_signals")
    terms = proposal.get("terms", proposal.get("keywords", []))
    source_candidate_id = proposal.get("source_candidate_id")

    if not terms:
        return {
            "success": False,
            "error": "Missing terms in proposal",
        }

    if penalty_strength is None:
        penalty_strength = proposal.get("penalty_strength", 30.0)

    # Initialize and save negative bundle
    negative_bundle = initialize_negative_bundle(
        bundle_id=bundle_id,
        terms=terms,
        penalty_strength=penalty_strength,
        source_candidate_id=source_candidate_id,
    )

    save_negative_bundle(
        bundle_id=bundle_id,
        bundle_data=negative_bundle,
        penalty_strength=penalty_strength,
        source_candidate_id=source_candidate_id,
    )

    return {
        "success": True,
        "bundle_id": bundle_id,
        "terms": terms,
        "penalty_strength": penalty_strength,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def update_theme_priority(
    theme_id: str,
    new_priority: float,
) -> dict[str, Any]:
    """Update the priority of an existing theme.

    Args:
        theme_id: Theme identifier
        new_priority: New priority value (0-100)

    Returns:
        Result dictionary with success status
    """
    existing_themes = get_themes()

    if theme_id not in existing_themes:
        return {
            "success": False,
            "error": f"Theme '{theme_id}' not found",
        }

    theme = existing_themes[theme_id]
    theme["priority"] = new_priority

    save_theme(
        theme_id=theme_id,
        theme_data=theme,
        priority=new_priority,
        matched_terms=theme.get("matched_terms", []),
    )

    return {
        "success": True,
        "theme_id": theme_id,
        "new_priority": new_priority,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def remove_term_from_bundle(
    term: str,
    bundle_id: str,
    term_type: str = "optional",
) -> dict[str, Any]:
    """Remove a term from a bundle.

    Args:
        term: The keyword term to remove
        bundle_id: Bundle ID
        term_type: Type of term ('required' or 'optional')

    Returns:
        Result dictionary with success status
    """
    bundles_config = load_keyword_bundles()
    bundles = bundles_config.get("bundles", {})

    if bundle_id not in bundles:
        return {
            "success": False,
            "error": f"Bundle '{bundle_id}' not found",
        }

    bundle = bundles[bundle_id]

    if term_type == "required":
        if term in bundle.get("required_terms", []):
            bundle["required_terms"].remove(term)
    else:
        if term in bundle.get("optional_terms", []):
            bundle["optional_terms"].remove(term)

    # Save updated bundles
    _save_bundles(bundles_config)

    return {
        "success": True,
        "bundle_id": bundle_id,
        "term": term,
        "term_type": term_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _save_bundles(bundles_config: dict[str, Any]) -> None:
    """Save keyword bundles configuration.

    Args:
        bundles_config: Updated bundles configuration
    """
    with open(KEYWORD_BUNDLES_PATH, "w", encoding="utf-8") as f:
        json.dump(bundles_config, f, indent=2)


def get_expansion_history() -> list[dict[str, Any]]:
    """Get history of applied keyword expansions.

    Returns:
        List of expansion history records (placeholder - would need separate storage)
    """
    # This is a placeholder - in production, you'd want separate history tracking
    return []


def rollback_expansion(
    term: str,
    bundle_id: str,
) -> dict[str, Any]:
    """Rollback a previously applied expansion.

    Args:
        term: The keyword term to remove
        bundle_id: Bundle ID

    Returns:
        Result dictionary with success status
    """
    return remove_term_from_bundle(term, bundle_id, "optional")
