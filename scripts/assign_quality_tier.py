"""
assign_quality_tier.py — Assign quality tiers to accepted research records.

Quality tiers distinguish between:
  - tier_1: highly reusable, strong signal, core RAG assets
  - tier_2: useful context, but not core
  - tier_3: acceptable but lower-value or narrower utility

Usage:
  # Single record:
  python scripts/assign_quality_tier.py --record data/accepted/my_record.json

  # Batch (all accepted records):
  python scripts/assign_quality_tier.py --batch data/accepted

  # Dry-run (report only, no writes):
  python scripts/assign_quality_tier.py --batch data/accepted --dry-run
"""

import argparse
import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

CONFIG_PATH = BASE_DIR / "config" / "quality_tier_rules.json"
DOMAIN_TRUST_PATH = BASE_DIR / "config" / "domain_trust_tiers.json"


def load_json_file(path: Path) -> dict:
    """Load a JSON file, returning empty dict if missing."""
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_config(config_path: Path | None = None) -> dict:
    """Load quality tier rules config."""
    path = config_path or CONFIG_PATH
    config = load_json_file(path)
    if not config:
        # Provide sensible defaults if config is missing
        config = {
            "tiers": {
                "tier_1": {"min_score": 80, "label": "highly_reusable"},
                "tier_2": {"min_score": 55, "label": "useful_context"},
                "tier_3": {"min_score": 0, "label": "acceptable"},
            },
            "features": {
                "verification_confidence": {"weight": 0.25, "scale": [0, 10]},
                "human_approved": {"weight": 0.15, "boost": 10},
                "issues_found_count": {"weight": 0.10, "scale": [0, 10]},
                "why_it_matters_quality": {"weight": 0.15, "scale": [0, 1]},
                "has_structured_numbers": {"weight": 0.10},
                "source_trust_score": {"weight": 0.10, "scale": [0, 100]},
                "topic_centrality": {"weight": 0.10, "scale": [0, 1]},
                "event_significance": {"weight": 0.05, "scale": [0, 1]},
            },
            "topic_centrality_map": {
                "monetary policy": 1.0,
                "fiscal policy": 1.0,
                "market structure": 0.7,
                "macro data": 0.8,
                "regulation": 0.6,
                "default": 0.5,
            },
            "event_significance_map": {
                "fed_policy": 1.0,
                "central_bank_action": 1.0,
                "fiscal_policy": 0.8,
                "data_release": 0.6,
                "liquidity": 0.7,
                "yield_curve": 0.6,
                "default": 0.4,
            },
        }
    return config


def load_domain_trust() -> dict:
    """Load domain trust tier mapping."""
    data = load_json_file(DOMAIN_TRUST_PATH)
    trust_map = {}
    for tier_name, domains in data.items():
        if isinstance(domains, list):
            score = {"high": 100, "medium": 50, "low": 10}.get(tier_name, 10)
            for domain in domains:
                trust_map[domain.lower()] = score
    return trust_map


def normalize(value: float, scale: list[float]) -> float:
    """Normalize a value to 0-1 range given [min, max] scale."""
    min_val, max_val = scale
    if max_val == min_val:
        return 0.0
    return max(0.0, min(1.0, (value - min_val) / (max_val - min_val)))


def extract_feature_values(record: dict, domain_trust: dict | None = None) -> dict:
    """Extract raw feature values from a research record.

    Returns a dict of feature_name -> raw_value for scoring.
    """
    llm_review = record.get("llm_review", {})
    human_review = record.get("human_review", {})
    source = record.get("source", {})
    domain = (source.get("domain", "") or "").lower()

    # Verification confidence (0-10)
    verification_confidence = float(llm_review.get("verification_confidence", 0))

    # Human approval
    human_decision = human_review.get("decision", "")
    human_approved = human_decision in ("approved_by_human", "approved")

    # Human feedback signal (additional feedback beyond simple approval)
    human_feedback = human_review.get("human_feedback", {})
    feedback_signal = human_feedback.get("feedback_decision", "")

    # Issues found count (0-10, capped)
    issues = llm_review.get("issues_found", [])
    issues_count = min(len(issues), 10)

    # Why it matters quality (0-1)
    why_it_matters = record.get("why_it_matters", "")
    if not why_it_matters or not why_it_matters.strip():
        why_quality = 0.0
    elif len(why_it_matters.strip()) > 30:
        why_quality = 1.0
    else:
        why_quality = 0.5

    # Structured numbers
    important_numbers = record.get("important_numbers", [])
    has_numbers = len(important_numbers) > 0

    # Source trust score
    trust_score = 10  # default low
    if domain_trust and domain:
        trust_score = domain_trust.get(domain, 10)

    # Topic centrality (loaded from config at scoring time, stored as raw topic)
    topic = (record.get("topic", "") or "").lower()

    # Event significance (loaded from config at scoring time, stored as raw event_type)
    event_type = (record.get("event_type", "") or "").lower()

    return {
        "verification_confidence": verification_confidence,
        "human_approved": human_approved,
        "feedback_signal": feedback_signal,
        "issues_found_count": issues_count,
        "why_it_matters_quality": why_quality,
        "has_structured_numbers": has_numbers,
        "source_trust_score": trust_score,
        "topic": topic,
        "event_type": event_type,
    }


def compute_tier_score(
    features: dict,
    config: dict,
    domain_trust: dict | None = None,
) -> float:
    """Compute a 0-100 quality score from extracted features.

    Args:
        features: dict of raw feature values from extract_feature_values
        config: quality tier rules config
        domain_trust: optional domain trust mapping (already used in extraction)

    Returns:
        Score between 0 and 100.
    """
    features_config = config.get("features", {})
    topic_map = config.get("topic_centrality_map", {})
    event_map = config.get("event_significance_map", {})

    score = 0.0

    # 1. Verification confidence (0.25 weight, scale 0-10)
    fc = features_config.get("verification_confidence", {})
    if fc.get("weight", 0) > 0:
        norm = normalize(features["verification_confidence"], fc.get("scale", [0, 10]))
        score += norm * fc["weight"] * 100

    # 2. Human approval boost (0.15 weight as boost)
    ha = features_config.get("human_approved", {})
    if ha.get("weight", 0) > 0 and features["human_approved"]:
        score += ha.get("boost", 10) * ha["weight"]

    # 3. Issues found (0.10 weight, inverted: fewer = better)
    ifc = features_config.get("issues_found_count", {})
    if ifc.get("weight", 0) > 0:
        norm = normalize(features["issues_found_count"], ifc.get("scale", [0, 10]))
        inverted = 1.0 - norm  # fewer issues = higher score
        score += inverted * ifc["weight"] * 100

    # 4. Why it matters quality (0.15 weight, scale 0-1)
    wq = features_config.get("why_it_matters_quality", {})
    if wq.get("weight", 0) > 0:
        score += features["why_it_matters_quality"] * wq["weight"] * 100

    # 5. Structured numbers (0.10 weight, binary)
    sn = features_config.get("has_structured_numbers", {})
    if sn.get("weight", 0) > 0 and features["has_structured_numbers"]:
        score += sn["weight"] * 100

    # 6. Source trust score (0.10 weight, scale 0-100)
    sts = features_config.get("source_trust_score", {})
    if sts.get("weight", 0) > 0:
        norm = normalize(features["source_trust_score"], sts.get("scale", [0, 100]))
        score += norm * sts["weight"] * 100

    # 7. Topic centrality (0.10 weight)
    tc = features_config.get("topic_centrality", {})
    if tc.get("weight", 0) > 0:
        topic_val = topic_map.get(features["topic"], topic_map.get("default", 0.5))
        score += topic_val * tc["weight"] * 100

    # 8. Event significance (0.05 weight)
    es = features_config.get("event_significance", {})
    if es.get("weight", 0) > 0:
        event_val = event_map.get(features["event_type"], event_map.get("default", 0.4))
        score += event_val * es["weight"] * 100

    # 9. Feedback signal adjustments
    feedback_signal = features.get("feedback_signal", "")
    if feedback_signal == "approve_and_promote":
        # Stronger positive signal
        score += 10
    elif feedback_signal == "approve_but_weak":
        # Weaker positive signal
        score -= 5
    elif feedback_signal == "bad_source":
        # Negative source quality signal
        score -= 10
    elif feedback_signal == "good_source":
        # Positive source quality signal
        score += 5

    return round(min(100.0, max(0.0, score)), 1)


def get_tier_label(score: float, config: dict) -> str:
    """Map a numeric score to a tier label.

    Tiers are evaluated from highest to lowest threshold.
    """
    tiers = config.get("tiers", {})
    # Sort by min_score descending so we check tier_1 first
    sorted_tiers = sorted(
        tiers.items(),
        key=lambda x: x[1].get("min_score", 0),
        reverse=True,
    )
    for tier_name, tier_config in sorted_tiers:
        if score >= tier_config.get("min_score", 0):
            return tier_name
    # Fallback to lowest tier
    return sorted_tiers[-1][0] if sorted_tiers else "tier_3"


def generate_reasoning(
    record: dict,
    features: dict,
    score: float,
    tier: str,
    config: dict,
) -> list[str]:
    """Generate human-readable reasoning for the tier assignment.

    Returns a list of reason strings.
    """
    reasons = []
    llm_review = record.get("llm_review", {})
    human_review = record.get("human_review", {})

    # Verification confidence
    vc = features["verification_confidence"]
    if vc >= 8:
        reasons.append("high verification confidence")
    elif vc >= 6:
        reasons.append("moderate verification confidence")
    elif vc >= 4:
        reasons.append("low verification confidence")
    else:
        reasons.append("very low verification confidence")

    # Human approval
    if features["human_approved"]:
        reasons.append("human-approved")

    # Feedback signal
    feedback_signal = features.get("feedback_signal", "")
    if feedback_signal == "approve_and_promote":
        reasons.append("feedback: approve and promote (+10)")
    elif feedback_signal == "approve_but_weak":
        reasons.append("feedback: approve but weak (-5)")
    elif feedback_signal == "bad_source":
        reasons.append("feedback: bad source quality (-10)")
    elif feedback_signal == "good_source":
        reasons.append("feedback: good source quality (+5)")

    # Issues
    ic = features["issues_found_count"]
    if ic == 0:
        reasons.append("no issues found during verification")
    elif ic <= 2:
        reasons.append(f"minor issues found ({ic})")
    else:
        reasons.append(f"multiple issues found ({ic})")

    # Why it matters
    if features["why_it_matters_quality"] >= 1.0:
        reasons.append("clear why_it_matters explanation")
    elif features["why_it_matters_quality"] >= 0.5:
        reasons.append("brief why_it_matters explanation")
    else:
        reasons.append("missing or weak why_it_matters")

    # Structured numbers
    if features["has_structured_numbers"]:
        reasons.append("contains useful structured numbers")
    else:
        reasons.append("no structured quantitative data")

    # Source trust
    st = features["source_trust_score"]
    if st >= 80:
        reasons.append("high-trust source")
    elif st >= 40:
        reasons.append("medium-trust source")
    else:
        reasons.append("lower-trust source")

    # Topic centrality
    topic = features["topic"]
    topic_map = config.get("topic_centrality_map", {})
    tc_val = topic_map.get(topic, topic_map.get("default", 0.5))
    if tc_val >= 0.8:
        reasons.append("core topic relevance")
    elif tc_val >= 0.6:
        reasons.append("moderate topic relevance")
    else:
        reasons.append("peripheral topic")

    # Event significance
    event_type = features["event_type"]
    event_map = config.get("event_significance_map", {})
    es_val = event_map.get(event_type, event_map.get("default", 0.4))
    if es_val >= 0.8:
        reasons.append("high event significance")
    elif es_val >= 0.6:
        reasons.append("moderate event significance")
    else:
        reasons.append("lower event significance")

    # Summary line
    tier_labels = config.get("tiers", {}).get(tier, {})
    label = tier_labels.get("label", tier)
    reasons.insert(0, f"quality score: {score}/100 ({label})")

    return reasons


def assign_quality_tier(
    record: dict,
    config: dict | None = None,
    config_path: Path | None = None,
    domain_trust: dict | None = None,
) -> dict:
    """Assign a quality tier to a research record.

    Args:
        record: the research record dict
        config: pre-loaded config (optional)
        config_path: path to config file (optional, defaults to CONFIG_PATH)
        domain_trust: pre-loaded domain trust map (optional)

    Returns:
        The quality_tier block dict with tier, score, and reasoning.
    """
    if config is None:
        config = load_config(config_path)
    if domain_trust is None:
        domain_trust = load_domain_trust()

    features = extract_feature_values(record, domain_trust)
    score = compute_tier_score(features, config)
    tier = get_tier_label(score, config)
    reasoning = generate_reasoning(record, features, score, tier, config)

    return {
        "quality_tier": {
            "tier": tier,
            "score": score,
            "reasoning": reasoning,
        }
    }


def process_record_file(
    file_path: Path,
    config: dict | None = None,
    dry_run: bool = False,
) -> dict:
    """Process a single record file and assign a quality tier.

    Args:
        file_path: path to the record JSON file
        config: pre-loaded config (optional)
        dry_run: if True, don't write changes

    Returns:
        Dict with file_path, old_tier, new_tier, score, and success status.
    """
    result = {
        "file": str(file_path),
        "old_tier": None,
        "new_tier": None,
        "score": None,
        "success": False,
        "skipped": False,
    }

    try:
        with file_path.open("r", encoding="utf-8") as f:
            record = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        result["error"] = str(e)
        return result

    # Check if tier already exists
    existing_tier = record.get("quality_tier")
    if existing_tier:
        result["old_tier"] = existing_tier.get("tier")
        result["skipped"] = True
        result["success"] = True
        return result

    tier_block = assign_quality_tier(record, config)
    result["new_tier"] = tier_block["quality_tier"]["tier"]
    result["score"] = tier_block["quality_tier"]["score"]

    if not dry_run:
        record["quality_tier"] = tier_block["quality_tier"]
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)

    result["success"] = True
    return result


def process_batch(
    directory: Path,
    config: dict | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Process all JSON files in a directory.

    Args:
        directory: path to directory containing record JSON files
        config: pre-loaded config (optional)
        dry_run: if True, don't write changes

    Returns:
        List of result dicts from process_record_file.
    """
    results = []
    json_files = sorted(directory.glob("*.json"))

    for file_path in json_files:
        result = process_record_file(file_path, config, dry_run)
        results.append(result)

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Assign quality tiers to accepted research records."
    )
    parser.add_argument(
        "--record",
        type=str,
        help="Path to a single record JSON file to process.",
    )
    parser.add_argument(
        "--batch",
        type=str,
        help="Path to a directory of record JSON files to process.",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to quality_tier_rules.json config file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report tier assignments without writing changes.",
    )

    args = parser.parse_args()

    if not args.record and not args.batch:
        parser.error("Provide either --record or --batch.")

    config_path = Path(args.config) if args.config else None
    config = load_config(config_path)
    dry_run = args.dry_run

    if dry_run:
        print("[DRY RUN] No files will be modified.\n")

    if args.record:
        file_path = Path(args.record)
        if not file_path.exists():
            print(f"Error: File not found: {file_path}")
            sys.exit(1)
        result = process_record_file(file_path, config, dry_run)
        if result["skipped"]:
            print(
                f"Skipped (already has tier): {result['file']} -> {result['old_tier']}"
            )
        elif result["success"]:
            print(
                f"Assigned: {result['file']} -> {result['new_tier']} (score: {result['score']})"
            )
        else:
            print(f"Failed: {result['file']} -> {result.get('error', 'unknown error')}")
            sys.exit(1)

    elif args.batch:
        directory = Path(args.batch)
        if not directory.is_dir():
            print(f"Error: Directory not found: {directory}")
            sys.exit(1)

        results = process_batch(directory, config, dry_run)

        # Summary
        total = len(results)
        assigned = sum(1 for r in results if r["success"] and not r["skipped"])
        skipped = sum(1 for r in results if r["skipped"])
        failed = sum(1 for r in results if not r["success"])

        tier_counts = {}
        for r in results:
            if r["new_tier"]:
                tier_counts[r["new_tier"]] = tier_counts.get(r["new_tier"], 0) + 1
            elif r["old_tier"]:
                tier_counts[r["old_tier"]] = tier_counts.get(r["old_tier"], 0) + 1

        print(f"Processed {total} records:")
        print(f"  Assigned: {assigned}")
        print(f"  Skipped (already tiered): {skipped}")
        print(f"  Failed: {failed}")
        print(f"\nTier distribution:")
        for tier_name in ["tier_1", "tier_2", "tier_3"]:
            count = tier_counts.get(tier_name, 0)
            label = config.get("tiers", {}).get(tier_name, {}).get("label", "")
            print(f"  {tier_name} ({label}): {count}")

        # Print individual results
        print(f"\nDetails:")
        for r in results:
            if r["skipped"]:
                print(f"  [SKIP] {Path(r['file']).name} -> {r['old_tier']}")
            elif r["success"]:
                print(
                    f"  [OK]   {Path(r['file']).name} -> {r['new_tier']} (score: {r['score']})"
                )
            else:
                print(f"  [FAIL] {Path(r['file']).name} -> {r.get('error', 'unknown')}")


if __name__ == "__main__":
    main()
