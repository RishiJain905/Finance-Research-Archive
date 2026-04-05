"""
V2.7 Part 5: Source Recommendations Engine.

Reads source stats files and generates actionable recommendations:
- keep: strong accepted ratio, reasonable filtered/review ratios
- tighten: too many review results, high filtered ratio but still has potential
- lower_max_links: useful but too high-volume and noisy
- disable: very low accepted ratio, very high filtered ratio, repeated junk
- investigate: volatile performance, inconsistent behavior

Phase 1: recommendations only (no auto-editing configs).
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
STATS_DIR = BASE_DIR / "data" / "source_analytics"
OUTPUT_DIR = BASE_DIR / "data" / "source_recommendations"

VALID_ACTIONS = frozenset(
    {"keep", "tighten", "lower_max_links", "disable", "investigate"}
)

DEFAULT_RULES = {
    "disable": {
        "min_records_seen": 20,
        "max_accepted_ratio": 0.05,
        "min_filtered_ratio": 0.70,
    },
    "lower_max_links": {
        "min_accepted_ratio": 0.10,
        "min_filtered_ratio": 0.50,
        "min_records_seen": 10,
    },
    "tighten": {
        "min_accepted_ratio": 0.05,
        "min_review_ratio": 0.30,
        "min_records_seen": 10,
    },
    "investigate": {
        "min_records_seen": 5,
        "max_accepted_ratio": 0.50,
        "min_review_ratio": 0.20,
    },
}


# =============================================================================
# C1: Rule Engine
# =============================================================================


def load_recommendation_rules(rules_path: Path | None = None) -> dict:
    """Load recommendation rules from file or use defaults.

    Args:
        rules_path: Optional path to rules JSON file

    Returns:
        Rules dict with disable, tighten, lower_max_links, investigate thresholds
    """
    if rules_path and rules_path.exists():
        with rules_path.open("r", encoding="utf-8") as f:
            loaded = json.load(f)
        # Merge with defaults to ensure all keys present
        rules = dict(DEFAULT_RULES)
        rules.update(loaded)
        return rules
    return dict(DEFAULT_RULES)


def evaluate_disable_rule(stats: dict, rules: dict) -> bool:
    """Check if source should be disabled.

    Disable if:
    - 20+ records seen
    - accepted ratio < 5%
    - filtered ratio > 70%

    Args:
        stats: Source stats dict
        rules: Rules dict with thresholds

    Returns:
        True if disable conditions met
    """
    r = rules.get("disable", DEFAULT_RULES["disable"])
    return (
        stats.get("records_seen", 0) >= r["min_records_seen"]
        and stats.get("accepted_ratio", 0) < r["max_accepted_ratio"]
        and stats.get("filtered_ratio", 0) >= r["min_filtered_ratio"]
    )


def evaluate_tighten_rule(stats: dict, rules: dict) -> bool:
    """Check if source should be tightened.

    Tighten if:
    - accepted ratio exists (>= 5%)
    - many records go to review (>= 30%)
    - source still has potential (10+ records)

    Args:
        stats: Source stats dict
        rules: Rules dict with thresholds

    Returns:
        True if tighten conditions met
    """
    r = rules.get("tighten", DEFAULT_RULES["tighten"])
    return (
        stats.get("accepted_ratio", 0) >= r["min_accepted_ratio"]
        and stats.get("review_ratio", 0) >= r["min_review_ratio"]
        and stats.get("records_seen", 0) >= r["min_records_seen"]
    )


def evaluate_lower_max_links_rule(stats: dict, rules: dict) -> bool:
    """Check if source max links should be lowered.

    Lower max links if:
    - accepted ratio moderate (>= 10%)
    - filtered ratio high (>= 50%)
    - source volume very high (10+ records)

    Args:
        stats: Source stats dict
        rules: Rules dict with thresholds

    Returns:
        True if lower_max_links conditions met
    """
    r = rules.get("lower_max_links", DEFAULT_RULES["lower_max_links"])
    return (
        stats.get("accepted_ratio", 0) >= r["min_accepted_ratio"]
        and stats.get("filtered_ratio", 0) >= r["min_filtered_ratio"]
        and stats.get("records_seen", 0) >= r["min_records_seen"]
    )


def evaluate_investigate_rule(stats: dict, rules: dict) -> bool:
    """Check if source should be investigated.

    Investigate if:
    - volatile performance (5+ records)
    - mixed but unclear results (accepted ratio < 50%, review ratio >= 20%)

    Args:
        stats: Source stats dict
        rules: Rules dict with thresholds

    Returns:
        True if investigate conditions met
    """
    r = rules.get("investigate", DEFAULT_RULES["investigate"])
    return (
        stats.get("records_seen", 0) >= r["min_records_seen"]
        and stats.get("accepted_ratio", 0) < r["max_accepted_ratio"]
        and stats.get("review_ratio", 0) >= r["min_review_ratio"]
    )


# =============================================================================
# C2: Recommendation Generation
# =============================================================================


def build_reasons(action: str, stats: dict, rules: dict) -> list[str]:
    """Build human-readable reasons for a recommendation.

    Args:
        action: Recommended action string
        stats: Source stats dict
        rules: Rules dict with thresholds

    Returns:
        List of reason strings
    """
    reasons = []
    accepted_ratio = stats.get("accepted_ratio", 0)
    filtered_ratio = stats.get("filtered_ratio", 0)
    review_ratio = stats.get("review_ratio", 0)
    records_seen = stats.get("records_seen", 0)
    accepted_count = stats.get("accepted_count", 0)

    if action == "disable":
        r = rules.get("disable", DEFAULT_RULES["disable"])
        reasons.append(
            f"Accepted ratio {accepted_ratio:.1%} is below {r['max_accepted_ratio']:.1%} threshold"
        )
        reasons.append(
            f"Filtered ratio {filtered_ratio:.1%} exceeds {r['min_filtered_ratio']:.1%} threshold"
        )
        reasons.append(
            f"{records_seen} records seen with only {accepted_count} accepted"
        )

    elif action == "tighten":
        r = rules.get("tighten", DEFAULT_RULES["tighten"])
        reasons.append(
            f"Review ratio {review_ratio:.1%} is high (threshold: {r['min_review_ratio']:.1%})"
        )
        reasons.append(f"Source still produces accepted records ({accepted_count})")
        reasons.append("Consider tightening ingestion filters or source targeting")

    elif action == "lower_max_links":
        r = rules.get("lower_max_links", DEFAULT_RULES["lower_max_links"])
        reasons.append(
            f"Source is useful ({accepted_ratio:.1%} accepted) but high-volume and noisy"
        )
        reasons.append(
            f"Filtered ratio {filtered_ratio:.1%} exceeds {r['min_filtered_ratio']:.1%} threshold"
        )
        reasons.append("Consider reducing max_links_per_run for this source")

    elif action == "investigate":
        reasons.append(f"Zero accepted records across {records_seen} records seen")
        if filtered_ratio > 0.5:
            reasons.append(
                f"High filtered ratio ({filtered_ratio:.1%}) suggests source targeting issues"
            )
        if review_ratio > 0:
            reasons.append(
                f"Review ratio {review_ratio:.1%} — some records borderline relevant"
            )
        reasons.append("Manual review recommended to assess source relevance")

    elif action == "keep":
        if accepted_ratio > 0.15:
            reasons.append(f"Strong accepted ratio ({accepted_ratio:.1%})")
        elif accepted_ratio > 0:
            reasons.append(
                f"Moderate accepted ratio ({accepted_ratio:.1%}) — source has some value"
            )
        else:
            reasons.append(
                f"Low volume ({records_seen} records) — insufficient data for stronger recommendation"
            )
        reasons.append(
            f"Filtered ratio ({filtered_ratio:.1%}) and review ratio ({review_ratio:.1%}) within bounds"
        )
        reasons.append("No immediate action needed")

    return reasons


def generate_recommendation(stats: dict, rules: dict) -> dict:
    """Generate a single recommendation for a source.

    Evaluates rules in priority order: disable > lower_max_links > tighten > investigate > keep.

    Args:
        stats: Source stats dict conforming to source_stats schema
        rules: Rules dict with thresholds

    Returns:
        Recommendation dict conforming to source_recommendation schema
    """
    records_seen = stats.get("records_seen", 0)
    accepted_ratio = stats.get("accepted_ratio", 0)
    filtered_ratio = stats.get("filtered_ratio", 0)

    # Evaluate rules in priority order
    if evaluate_disable_rule(stats, rules):
        action = "disable"
    elif evaluate_lower_max_links_rule(stats, rules):
        action = "lower_max_links"
    elif evaluate_tighten_rule(stats, rules):
        action = "tighten"
    elif evaluate_investigate_rule(stats, rules):
        action = "investigate"
    elif records_seen > 0 and accepted_ratio == 0.0 and filtered_ratio > 0.5:
        # Catch sources that are all filtered but below disable thresholds
        action = "investigate"
    elif records_seen > 0 and accepted_ratio == 0.0:
        # Sources with zero accepted records but some volume
        action = "investigate"
    else:
        action = "keep"

    reasons = build_reasons(action, stats, rules)

    return {
        "source_name": stats.get("source_name", ""),
        "recommended_action": action,
        "reasons": reasons,
        "metrics_snapshot": {
            "accepted_ratio": stats.get("accepted_ratio", 0.0),
            "filtered_ratio": stats.get("filtered_ratio", 0.0),
            "review_ratio": stats.get("review_ratio", 0.0),
        },
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def generate_all_recommendations(
    stats_dir: Path, rules: dict | None = None
) -> list[dict]:
    """Generate recommendations for all sources.

    Args:
        stats_dir: Directory containing source stats files
        rules: Optional rules dict (uses defaults if None)

    Returns:
        List of recommendation dicts
    """
    if rules is None:
        rules = dict(DEFAULT_RULES)

    if not stats_dir.exists():
        return []

    recommendations = []

    for json_file in sorted(stats_dir.glob("*.json")):
        if json_file.name == ".gitkeep":
            continue
        try:
            with json_file.open("r", encoding="utf-8") as f:
                stats = json.load(f)
            rec = generate_recommendation(stats, rules)
            recommendations.append(rec)
        except Exception as e:
            print(f"Warning: Failed to process {json_file}: {e}")
            continue

    return recommendations


# =============================================================================
# C3: Persistence and CLI
# =============================================================================


def sanitize_filename(name: str) -> str:
    """Sanitize a source name for use as a filename.

    Args:
        name: Source name or domain

    Returns:
        Filename-safe string
    """
    import re

    return re.sub(r'[<>:"/\\|?*]', "_", name)


def generate_recommendation_filename(source_name: str) -> str:
    """Generate deterministic filename for a recommendation file.

    Args:
        source_name: Source name

    Returns:
        Filename string (e.g., "federalreserve_gov_recommendation.json")
    """
    safe = sanitize_filename(source_name)
    return f"{safe}_recommendation.json"


def save_recommendations(recommendations: list[dict], output_dir: Path) -> None:
    """Save recommendation files.

    One JSON file per recommendation.

    Args:
        recommendations: List of recommendation dicts
        output_dir: Directory to write recommendation files to
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    for rec in recommendations:
        filename = generate_recommendation_filename(rec["source_name"])
        filepath = output_dir / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with filepath.open("w", encoding="utf-8") as f:
            json.dump(rec, f, indent=2, ensure_ascii=False)


def load_recommendations(output_dir: Path) -> list[dict]:
    """Load existing recommendations.

    Args:
        output_dir: Directory containing recommendation files

    Returns:
        List of recommendation dicts
    """
    if not output_dir.exists():
        return []

    recommendations = []
    for json_file in sorted(output_dir.glob("*.json")):
        if json_file.name == ".gitkeep":
            continue
        try:
            with json_file.open("r", encoding="utf-8") as f:
                rec = json.load(f)
            recommendations.append(rec)
        except Exception as e:
            print(f"Warning: Failed to load {json_file}: {e}")
            continue

    return recommendations


def run_recommendations(
    stats_dir: Path | None = None,
    output_dir: Path | None = None,
    rules_path: Path | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Main recommendations entry point.

    Loads stats, generates recommendations, saves to output.

    Args:
        stats_dir: Override for stats directory
        output_dir: Override for output directory
        rules_path: Optional path to custom rules file
        dry_run: If True, preview without writing

    Returns:
        List of recommendation dicts
    """
    if stats_dir is None:
        stats_dir = STATS_DIR
    if output_dir is None:
        output_dir = OUTPUT_DIR

    rules = load_recommendation_rules(rules_path)
    print("Loading source statistics...")
    recommendations = generate_all_recommendations(stats_dir, rules)
    print(f"  Generated {len(recommendations)} recommendations")

    # Print summary
    action_counts: dict[str, int] = {}
    for rec in recommendations:
        action = rec["recommended_action"]
        action_counts[action] = action_counts.get(action, 0) + 1

    print("\n=== Source Recommendations Summary ===")
    for action in sorted(action_counts.keys()):
        count = action_counts[action]
        print(f"  {action}: {count} source(s)")

    print()

    # Print per-source recommendations
    for rec in sorted(recommendations, key=lambda r: r["source_name"]):
        action = rec["recommended_action"]
        source = rec["source_name"] or rec.get("metrics_snapshot", {}).get(
            "source_name", "unknown"
        )
        reasons = rec["reasons"]
        print(f"[{action.upper()}] {source}")
        for reason in reasons:
            print(f"  - {reason}")
        print()

    # Save recommendations
    if dry_run:
        print(
            f"[DRY RUN] Would save {len(recommendations)} recommendation files to: {output_dir}"
        )
    else:
        print(f"Saving recommendations to: {output_dir}")
        save_recommendations(recommendations, output_dir)
        print(f"  Saved {len(recommendations)} recommendation files")

    return recommendations


def main():
    """CLI entry point for source recommendations."""
    parser = argparse.ArgumentParser(description="V2.7 Source Recommendations Engine")
    parser.add_argument(
        "--stats-dir",
        type=Path,
        default=None,
        help="Directory containing source stats (default: data/source_analytics)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for recommendation output (default: data/source_recommendations)",
    )
    parser.add_argument(
        "--rules-file",
        type=Path,
        default=None,
        help="Path to custom rules JSON file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview recommendations without writing files",
    )
    args = parser.parse_args()

    run_recommendations(
        stats_dir=args.stats_dir,
        output_dir=args.output_dir,
        rules_path=args.rules_file,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
