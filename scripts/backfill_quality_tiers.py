"""
backfill_quality_tiers.py — Backfill quality tiers for existing accepted records.

Scans all JSON files in data/accepted/ and assigns quality tiers to records
that don't already have one.

Usage:
  # Dry run (report only):
  python scripts/backfill_quality_tiers.py --dry-run

  # Apply changes:
  python scripts/backfill_quality_tiers.py --apply

  # Custom directory:
  python scripts/backfill_quality_tiers.py --apply --dir data/accepted

  # Custom config:
  python scripts/backfill_quality_tiers.py --apply --config config/quality_tier_rules.json
"""

import argparse
import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.assign_quality_tier import (
    assign_quality_tier,
    load_config,
    load_domain_trust,
    process_record_file,
    process_batch,
)

DEFAULT_ACCEPTED_DIR = BASE_DIR / "data" / "accepted"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill quality tiers for existing accepted records."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report tier assignments without writing changes.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write tier assignments to record files.",
    )
    parser.add_argument(
        "--dir",
        type=str,
        default=None,
        help=f"Directory containing accepted record JSON files (default: {DEFAULT_ACCEPTED_DIR}).",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to quality_tier_rules.json config file.",
    )

    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.error("Provide either --dry-run or --apply.")

    if args.dry_run and args.apply:
        parser.error("Cannot use both --dry-run and --apply.")

    directory = Path(args.dir) if args.dir else DEFAULT_ACCEPTED_DIR
    if not directory.is_dir():
        print(f"Error: Directory not found: {directory}")
        sys.exit(1)

    config_path = Path(args.config) if args.config else None
    config = load_config(config_path)
    domain_trust = load_domain_trust()

    mode = "DRY RUN" if args.dry_run else "APPLY"
    print(f"[{mode}] Backfilling quality tiers in: {directory}")
    print(f"Config: {config_path or 'default'}")
    print()

    results = process_batch(directory, config, dry_run=args.dry_run)

    # Summary
    total = len(results)
    assigned = sum(1 for r in results if r["success"] and not r["skipped"])
    skipped = sum(1 for r in results if r["skipped"])
    failed = sum(1 for r in results if not r["success"])

    tier_counts = {}
    for r in results:
        tier = r["new_tier"] or r["old_tier"]
        if tier:
            tier_counts[tier] = tier_counts.get(tier, 0) + 1

    print(f"Processed {total} records:")
    print(f"  Newly assigned: {assigned}")
    print(f"  Already tiered (skipped): {skipped}")
    print(f"  Failed: {failed}")
    print(f"\nTier distribution:")
    for tier_name in ["tier_1", "tier_2", "tier_3"]:
        count = tier_counts.get(tier_name, 0)
        label = config.get("tiers", {}).get(tier_name, {}).get("label", "")
        print(f"  {tier_name} ({label}): {count}")

    # Print individual results
    if total <= 50:
        print(f"\nDetails:")
        for r in results:
            name = Path(r["file"]).name
            if r["skipped"]:
                print(f"  [SKIP] {name} -> {r['old_tier']}")
            elif r["success"]:
                print(f"  [OK]   {name} -> {r['new_tier']} (score: {r['score']})")
            else:
                print(f"  [FAIL] {name} -> {r.get('error', 'unknown')}")

    if failed > 0:
        print(f"\n{failed} record(s) failed processing.")
        sys.exit(1)


if __name__ == "__main__":
    main()
