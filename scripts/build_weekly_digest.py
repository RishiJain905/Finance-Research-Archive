"""
Build weekly synthesis digest from accepted records.

Generates a comprehensive weekly summary covering major themes, important accepted
records, key review outcomes, and relevant quant shifts across a full week.

CLI usage:
    python scripts/build_weekly_digest.py [--week-start YYYY-MM-DD] [--week-end YYYY-MM-DD]
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.digest_utils import (
    MACRO_THEMES,
    MARKET_STRUCTURE_THEMES,
    build_digest_record,
    build_records_context,
    build_weekly_digest_id,
    call_minimax_for_synthesis,
    classify_record_theme,
    extract_linked_quant_ids,
    extract_linked_record_ids,
    get_week_range,
    load_accepted_records_for_range,
    save_digest_record,
)

DIGEST_TYPE = "weekly"
OUTPUT_DIR = BASE_DIR / "data" / "digests" / "weekly"

# Combined theme map for weekly synthesis
ALL_THEMES = {**MACRO_THEMES, **MARKET_STRUCTURE_THEMES}

SYNTHESIS_PROMPT = """You are a senior financial research analyst producing a weekly synthesis report.

Given a set of accepted research records spanning a full week, produce a comprehensive weekly digest.

Your synthesis should cover:
1. Major themes - What were the dominant topics and narratives this week?
2. Important accepted records - Which records were most significant? (Consider quality tier scores)
3. Key review outcomes - What patterns emerge from what was accepted vs rejected?
4. Quant shifts - What quantitative data points or market moves are most relevant?

Instructions:
- Write a coherent weekly narrative, not just a list of records
- Identify the 3-5 most important developments of the week
- Connect related developments across days
- Highlight any shifts in market narratives or policy direction
- Note the quality and reliability of the underlying records
- Be specific about numbers, dates, and sources when mentioned

Return ONLY a JSON object with this exact structure:
{
  "summary": "A comprehensive 5-8 paragraph weekly synthesis covering major themes, important records, review outcomes, and quant shifts",
  "key_themes": ["theme1", "theme2", "theme3", "theme4"],
  "confidence": 80
}

The "summary" field should be a well-written narrative synthesis of the week.
The "key_themes" field should list 3-6 major themes identified across the week.
The "confidence" field should be 0-100 reflecting how comprehensive the synthesis is based on available records.
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build weekly synthesis digest from accepted records"
    )
    parser.add_argument(
        "--week-start",
        type=str,
        default=None,
        help="Week start date (Monday) in YYYY-MM-DD format (default: current week Monday)",
    )
    parser.add_argument(
        "--week-end",
        type=str,
        default=None,
        help="Week end date (Sunday) in YYYY-MM-DD format (default: current week Sunday)",
    )
    return parser.parse_args()


def parse_date(date_str: str | None) -> date | None:
    """Parse a date string or return None."""
    if date_str:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            raise SystemExit(f"Invalid date format: {date_str}. Use YYYY-MM-DD.")
    return None


def build_weekly_digest(week_start: date, week_end: date) -> dict | None:
    """Build a weekly synthesis digest for the given week range."""
    print(
        f"=== Building Weekly Digest: {week_start.isoformat()} to {week_end.isoformat()} ===\n"
    )

    # Step 1: Collect accepted records for the week
    records = load_accepted_records_for_range(week_start, week_end)
    print(f"Found {len(records)} accepted records for the week")

    if not records:
        print("No accepted records found for this week.")
        print("Creating empty digest record.")
        digest = build_digest_record(
            digest_type=DIGEST_TYPE,
            start_date=week_start,
            end_date=week_end,
            summary="No accepted records available for this week.",
            key_themes=[],
            linked_record_ids=[],
            linked_quant_ids=[],
            confidence=0,
            notes="No accepted records found for this week.",
        )
        return digest

    # Step 2: Analyze record distribution
    # Group by theme
    theme_groups: dict[str, list[dict]] = {}
    for record in records:
        themes = classify_record_theme(record, ALL_THEMES)
        for theme in themes:
            if theme not in theme_groups:
                theme_groups[theme] = []
            theme_groups[theme].append(record)

    print(f"\nTheme distribution:")
    for theme, group_records in sorted(
        theme_groups.items(), key=lambda x: len(x[1]), reverse=True
    ):
        print(f"  - {theme}: {len(group_records)} records")

    # Analyze by quality tier
    tier_distribution: dict[str, int] = {}
    for record in records:
        tier = record.get("quality_tier", {}).get("tier", "unknown")
        tier_distribution[tier] = tier_distribution.get(tier, 0) + 1

    print(f"\nQuality tier distribution:")
    for tier, count in sorted(tier_distribution.items()):
        print(f"  - {tier}: {count} records")

    # Identify high-significance records (tier_1 or high scores)
    high_sig_records = [
        r
        for r in records
        if r.get("quality_tier", {}).get("tier") in ("tier_1",)
        or r.get("quality_tier", {}).get("score", 0) >= 80
    ]
    print(f"\nHigh-significance records: {len(high_sig_records)}")

    # Extract linked IDs
    linked_record_ids = extract_linked_record_ids(records)
    linked_quant_ids = extract_linked_quant_ids(records)
    print(f"Linked record IDs: {len(linked_record_ids)}")
    print(f"Linked quant IDs: {len(linked_quant_ids)}")

    # Step 3: Build context for AI synthesis
    # Include all records for weekly synthesis (broader context)
    records_context = build_records_context(records, max_records=30)

    # Add metadata about the week
    metadata_context = (
        f"Week: {week_start.isoformat()} to {week_end.isoformat()}\n"
        f"Total accepted records: {len(records)}\n"
        f"High-significance records: {len(high_sig_records)}\n"
        f"Themes identified: {', '.join(sorted(theme_groups.keys()))}\n"
        f"Quality tiers: {', '.join(f'{k}={v}' for k, v in sorted(tier_distribution.items()))}\n"
    )
    full_context = metadata_context + "\n" + records_context

    # Step 4: Generate synthesis via MiniMax
    print(f"\nCalling MiniMax for weekly synthesis...")
    try:
        synthesis = call_minimax_for_synthesis(SYNTHESIS_PROMPT, full_context)

        summary = synthesis.get("summary", "Synthesis generation returned no summary.")
        key_themes = synthesis.get("key_themes", [])
        confidence = synthesis.get("confidence", 0)

        print(f"Synthesis generated successfully.")
        print(f"Key themes: {key_themes}")
        print(f"Confidence: {confidence}")

    except (EnvironmentError, ValueError, Exception) as e:
        print(f"Warning: AI synthesis failed: {e}")
        print("Falling back to rule-based summary.")

        # Build fallback summary from theme groups and tier analysis
        summary_parts = [
            f"Weekly Summary for {week_start.isoformat()} to {week_end.isoformat()}",
            f"",
            f"Total accepted records: {len(records)}",
            f"High-significance records: {len(high_sig_records)}",
            f"",
            f"Theme distribution:",
        ]
        for theme, group_records in sorted(
            theme_groups.items(), key=lambda x: len(x[1]), reverse=True
        ):
            summary_parts.append(f"  - {theme}: {len(group_records)} records")

        summary_parts.append(f"\nQuality tier distribution:")
        for tier, count in sorted(tier_distribution.items()):
            summary_parts.append(f"  - {tier}: {count} records")

        summary = "\n".join(summary_parts)
        key_themes = sorted(
            theme_groups.keys(), key=lambda t: len(theme_groups[t]), reverse=True
        )[:5]
        confidence = 50

    # Step 5: Build digest record
    digest = build_digest_record(
        digest_type=DIGEST_TYPE,
        start_date=week_start,
        end_date=week_end,
        summary=summary,
        key_themes=key_themes,
        linked_record_ids=linked_record_ids,
        linked_quant_ids=linked_quant_ids,
        confidence=confidence,
        notes=(
            f"Generated from {len(records)} accepted records across {len(theme_groups)} themes. "
            f"{len(high_sig_records)} high-significance records included. "
            f"Quality tiers: {', '.join(f'{k}={v}' for k, v in sorted(tier_distribution.items()))}."
        ),
    )

    return digest


def main() -> None:
    args = parse_args()

    # Determine week range
    week_start = parse_date(args.week_start)
    week_end = parse_date(args.week_end)

    if week_start and not week_end:
        # If only start provided, calculate end as Sunday of that week
        week_end = week_start + __import__("datetime").timedelta(days=6)
    elif week_end and not week_start:
        # If only end provided, calculate start as Monday of that week
        week_start = week_end - __import__("datetime").timedelta(days=6)
    elif not week_start and not week_end:
        # Default to current week
        week_start, week_end = get_week_range()

    digest = build_weekly_digest(week_start, week_end)

    if digest:
        # Step 6: Save digest
        output_path = save_digest_record(digest, OUTPUT_DIR)
        print(f"\nDigest saved to: {output_path.relative_to(BASE_DIR)}")
        print(f"Digest ID: {digest['digest_id']}")
        print(f"Type: {digest['digest_type']}")
        print(
            f"Date range: {digest['date_range']['start']} to {digest['date_range']['end']}"
        )
        print(f"Linked records: {len(digest['linked_record_ids'])}")
        print(f"Linked quants: {len(digest['linked_quant_ids'])}")
    else:
        print("No digest was generated.")


if __name__ == "__main__":
    main()
