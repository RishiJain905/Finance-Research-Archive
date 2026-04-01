"""
Build daily macro digest from accepted records.

Generates a synthesized summary of macro-related developments for a given day,
covering inflation, labor/growth, central bank communications, and rate/curve shifts.

CLI usage:
    python scripts/build_daily_macro_digest.py [--date YYYY-MM-DD]
"""

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.digest_utils import (
    MACRO_THEMES,
    build_digest_record,
    build_records_context,
    call_minimax_for_synthesis,
    classify_record_theme,
    extract_linked_quant_ids,
    extract_linked_record_ids,
    format_theme_summary,
    get_week_range,
    group_records_by_theme,
    load_accepted_records_for_date,
    save_digest_record,
)

DIGEST_TYPE = "daily_macro"
OUTPUT_DIR = BASE_DIR / "data" / "digests" / "daily_macro"

SYNTHESIS_PROMPT = """You are a financial research analyst synthesizing daily macroeconomic developments.

Given a set of accepted research records for a single day, produce a concise macro digest.

Focus on these areas:
1. Inflation-related developments (CPI, PPI, price pressures, inflation expectations)
2. Labor and growth developments (employment, GDP, wages, economic growth)
3. Central bank communications (Fed, ECB, BOE, monetary policy signals)
4. Rate and curve shifts (yield curve changes, rate expectations, bond market moves)

Instructions:
- Synthesize the records into a coherent narrative, not just a list
- Highlight the most significant developments
- Note any connections between records
- Be specific about numbers and dates when mentioned
- If no records exist for an area, note that explicitly

Return ONLY a JSON object with this exact structure:
{
  "summary": "A comprehensive 3-5 paragraph synthesis of the day's macro developments",
  "key_themes": ["theme1", "theme2", "theme3"],
  "confidence": 75
}

The "summary" field should be a well-written narrative synthesis.
The "key_themes" field should list 2-5 major themes identified.
The "confidence" field should be 0-100 reflecting how comprehensive the synthesis is based on available records.
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build daily macro digest from accepted records"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date in YYYY-MM-DD format (default: today)",
    )
    return parser.parse_args()


def parse_date(date_str: str | None) -> date:
    """Parse a date string or return today."""
    if date_str:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            raise SystemExit(f"Invalid date format: {date_str}. Use YYYY-MM-DD.")
    return date.today()


def filter_macro_records(records: list[dict]) -> list[dict]:
    """Filter records to only those relevant to macro themes."""
    macro_records = []
    for record in records:
        themes = classify_record_theme(record, MACRO_THEMES)
        if themes:
            macro_records.append(record)
    return macro_records


def build_macro_digest(target_date: date) -> dict | None:
    """Build a daily macro digest for the given date."""
    print(f"=== Building Daily Macro Digest for {target_date.isoformat()} ===\n")

    # Step 1: Collect accepted records for the day
    records = load_accepted_records_for_date(target_date)
    print(f"Found {len(records)} accepted records for {target_date.isoformat()}")

    if not records:
        print("No accepted records found for this date.")
        print("Creating empty digest record.")
        digest = build_digest_record(
            digest_type=DIGEST_TYPE,
            start_date=target_date,
            end_date=target_date,
            summary="No accepted records available for this date.",
            key_themes=[],
            linked_record_ids=[],
            linked_quant_ids=[],
            confidence=0,
            notes="No accepted records found for this date.",
        )
        return digest

    # Step 2: Filter for macro-relevant records
    macro_records = filter_macro_records(records)
    print(f"Found {len(macro_records)} macro-relevant records")

    if not macro_records:
        print("No macro-relevant records found.")
        print("Creating digest noting absence of macro content.")
        all_ids = extract_linked_record_ids(records)
        quant_ids = extract_linked_quant_ids(records)
        digest = build_digest_record(
            digest_type=DIGEST_TYPE,
            start_date=target_date,
            end_date=target_date,
            summary=f"{len(records)} accepted records found for this date, but none were classified as macro-relevant (inflation, labor/growth, central bank, or rates/curve topics).",
            key_themes=[],
            linked_record_ids=all_ids,
            linked_quant_ids=quant_ids,
            confidence=0,
            notes="Records present but none matched macro theme classification.",
        )
        return digest

    # Step 3: Group by theme
    theme_groups = group_records_by_theme(macro_records, MACRO_THEMES)
    print(f"\nTheme groups identified:")
    for theme, group_records in theme_groups.items():
        print(f"  - {theme}: {len(group_records)} records")

    # Step 4: Extract linked IDs
    linked_record_ids = extract_linked_record_ids(macro_records)
    linked_quant_ids = extract_linked_quant_ids(macro_records)
    print(f"\nLinked record IDs: {len(linked_record_ids)}")
    print(f"Linked quant IDs: {len(linked_quant_ids)}")

    # Step 5: Build context for AI synthesis
    records_context = build_records_context(macro_records)

    # Step 6: Generate synthesis via MiniMax
    print(f"\nCalling MiniMax for macro synthesis...")
    try:
        synthesis = call_minimax_for_synthesis(SYNTHESIS_PROMPT, records_context)

        summary = synthesis.get("summary", "Synthesis generation returned no summary.")
        key_themes = synthesis.get("key_themes", [])
        confidence = synthesis.get("confidence", 0)

        print(f"Synthesis generated successfully.")
        print(f"Key themes: {key_themes}")
        print(f"Confidence: {confidence}")

    except (EnvironmentError, ValueError, Exception) as e:
        print(f"Warning: AI synthesis failed: {e}")
        print("Falling back to rule-based summary.")

        summary = format_theme_summary(theme_groups)
        key_themes = list(theme_groups.keys())
        confidence = 50

    # Step 7: Build digest record
    digest = build_digest_record(
        digest_type=DIGEST_TYPE,
        start_date=target_date,
        end_date=target_date,
        summary=summary,
        key_themes=key_themes,
        linked_record_ids=linked_record_ids,
        linked_quant_ids=linked_quant_ids,
        confidence=confidence,
        notes=f"Generated from {len(macro_records)} macro-relevant records out of {len(records)} total accepted records.",
    )

    return digest


def main() -> None:
    args = parse_args()
    target_date = parse_date(args.date)

    digest = build_macro_digest(target_date)

    if digest:
        # Step 8: Save digest
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
