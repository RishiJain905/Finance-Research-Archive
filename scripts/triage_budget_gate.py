"""
Triage Budget Gate Module.

Applies budget limits to control how many candidates are processed per run.
"""

from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
TRIAGE_DIR = BASE_DIR / "data" / "triage"

# Default limits per lane
DEFAULT_LIMITS = {
    "trusted_sources": 25,
    "keyword_discovery": 25,
    "seed_crawl": 25,
    "quant": 10,
}


def apply_budget_gate(
    process_list: list[dict],
    defer_list: list[dict],
    discard_list: list[dict],
    budget_config: dict,
    lane: str,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Apply budget gate to control processing volume.

    Args:
        process_list: List of candidates to process (sorted by priority desc)
        defer_list: List of deferred candidates
        discard_list: List of discarded candidates
        budget_config: Budget configuration dict with article_process_limit,
                       quant_process_limit, defer_medium
        lane: Lane name (trusted_sources, keyword_discovery, seed_crawl, quant)

    Returns:
        Tuple of (process_list, defer_list, discard_list) after budget applied
    """
    # Import here to avoid circular imports
    from scripts.triage_metrics import record_triage_metric

    # Determine the limit for this lane
    if lane == "quant":
        limit = budget_config.get("quant_process_limit", 10)
    else:
        # Article-like lanes use article_process_limit
        limit = budget_config.get("article_process_limit", 25)

    defer_medium = budget_config.get("defer_medium", True)

    # Filter process_list to only include candidates under budget
    # But if defer_medium is True, also defer medium priority candidates
    process_to_keep = []
    deferred_by_budget = []

    for candidate in process_list:
        triage = candidate.get("triage_result", {})
        band = triage.get("priority_band", "medium")

        if band == "medium" and defer_medium:
            # Defer medium priority if defer_medium is True
            deferred_by_budget.append(candidate)
            record_triage_metric(candidate, "defer")
        elif len(process_to_keep) < limit:
            # Under budget, keep in process list
            process_to_keep.append(candidate)
            record_triage_metric(candidate, "process_now")
        else:
            # Over budget, move to defer
            deferred_by_budget.append(candidate)
            record_triage_metric(candidate, "defer")

    # Record metrics for existing defer and discard lists
    for candidate in defer_list:
        record_triage_metric(candidate, "defer")
    for candidate in discard_list:
        record_triage_metric(candidate, "discard")

    # Combine any existing defer_list with newly deferred
    combined_defer = defer_list + deferred_by_budget

    # Sort all lists by priority score descending
    process_to_keep.sort(
        key=lambda c: c.get("triage_result", {}).get("priority_score", 0), reverse=True
    )
    combined_defer.sort(
        key=lambda c: c.get("triage_result", {}).get("priority_score", 0), reverse=True
    )

    return process_to_keep, combined_defer, discard_list
