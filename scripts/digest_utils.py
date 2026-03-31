"""
Shared utilities for digest generation.

Provides common functions for loading records, grouping by theme,
building digest IDs, saving digest records, and calling MiniMax for synthesis.
"""

import json
import os
import sys
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
from openai import OpenAI

ACCEPTED_DIR = BASE_DIR / "data" / "accepted"
DIGESTS_DIR = BASE_DIR / "data" / "digests"
SCHEMAS_DIR = BASE_DIR / "schemas"

# Theme classification keywords
MACRO_THEMES = {
    "inflation": ["inflation", "cpi", "ppi", "price", "deflation", "pce"],
    "labor_growth": [
        "labor",
        "employment",
        "jobs",
        "growth",
        "gdp",
        "unemployment",
        "wage",
    ],
    "central_bank": [
        "fed",
        "ecb",
        "boe",
        "central bank",
        "monetary policy",
        "fomc",
        "powell",
        "rate decision",
    ],
    "rates_curve": ["rate", "yield", "curve", "treasury", "bond", "spread"],
}

MARKET_STRUCTURE_THEMES = {
    "liquidity": ["liquidity", "funding", "repo", "reverse repo", "on rrpon", "on rrp"],
    "treasury_issuance": [
        "treasury",
        "issuance",
        "auction",
        "debt",
        "t-bill",
        "t-note",
        "t-bond",
    ],
    "clearing_exchange": ["clearing", "exchange", "settlement", "dtcc", "lch", "cme"],
    "market_plumbing": [
        "plumbing",
        "infrastructure",
        "operations",
        "market structure",
        "plumbing",
    ],
}


def load_accepted_records() -> list[dict]:
    """Load all accepted records from the accepted directory."""
    records = []
    if not ACCEPTED_DIR.exists():
        return records

    for record_path in ACCEPTED_DIR.glob("*.json"):
        try:
            with record_path.open("r", encoding="utf-8") as f:
                record = json.load(f)
                records.append(record)
        except (json.JSONDecodeError, OSError) as e:
            print(f"Warning: Could not load {record_path.name}: {e}")

    return records


def load_accepted_records_for_date(target_date: date) -> list[dict]:
    """Load accepted records created on a specific date."""
    all_records = load_accepted_records()
    target_str = target_date.isoformat()

    matching = []
    for record in all_records:
        created_at = record.get("created_at", "")
        if created_at and created_at[:10] == target_str:
            matching.append(record)

    return matching


def load_accepted_records_for_range(start_date: date, end_date: date) -> list[dict]:
    """Load accepted records created within a date range (inclusive)."""
    all_records = load_accepted_records()
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()

    matching = []
    for record in all_records:
        created_at = record.get("created_at", "")
        if created_at:
            record_date = created_at[:10]
            if start_str <= record_date <= end_str:
                matching.append(record)

    return matching


def classify_record_theme(record: dict, theme_map: dict[str, list[str]]) -> list[str]:
    """Classify a record into themes based on topic and tags."""
    matched_themes = []
    topic = (record.get("topic", "") or "").lower()
    tags = [t.lower() for t in (record.get("tags", []) or [])]
    summary = (record.get("summary", "") or "").lower()
    title = (record.get("title", "") or "").lower()

    searchable_text = f"{topic} {' '.join(tags)} {summary} {title}"

    for theme, keywords in theme_map.items():
        for keyword in keywords:
            if keyword in searchable_text:
                if theme not in matched_themes:
                    matched_themes.append(theme)
                break

    return matched_themes


def group_records_by_theme(
    records: list[dict], theme_map: dict[str, list[str]]
) -> dict[str, list[dict]]:
    """Group records by their classified themes."""
    groups: dict[str, list[dict]] = {}

    for record in records:
        themes = classify_record_theme(record, theme_map)
        for theme in themes:
            if theme not in groups:
                groups[theme] = []
            groups[theme].append(record)

    return groups


def extract_linked_quant_ids(records: list[dict]) -> list[str]:
    """Extract unique quant record IDs from linked_quant_context across records."""
    quant_ids = set()

    for record in records:
        linked_quant = record.get("linked_quant_context", [])
        if isinstance(linked_quant, list):
            for link in linked_quant:
                if isinstance(link, dict) and "record_id" in link:
                    quant_ids.add(link["record_id"])

    return sorted(quant_ids)


def extract_linked_record_ids(records: list[dict]) -> list[str]:
    """Extract unique record IDs from the given records list."""
    return sorted(r.get("id", "") for r in records if r.get("id"))


def build_digest_id(digest_type: str, ref_date: date) -> str:
    """Generate a consistent digest ID."""
    date_str = ref_date.strftime("%Y_%m_%d")
    return f"{digest_type}_{date_str}"


def build_weekly_digest_id(week_start: date, week_end: date) -> str:
    """Generate a weekly digest ID."""
    start_str = week_start.strftime("%Y_%m_%d")
    end_str = week_end.strftime("%Y_%m_%d")
    return f"weekly_{start_str}_to_{end_str}"


def load_digest_schema() -> dict:
    """Load the digest record schema template."""
    schema_path = SCHEMAS_DIR / "digest_record.json"
    if not schema_path.exists():
        raise FileNotFoundError(f"Digest schema not found: {schema_path}")

    with schema_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_digest_record(
    digest_type: str,
    start_date: date,
    end_date: date,
    summary: str,
    key_themes: list[str],
    linked_record_ids: list[str],
    linked_quant_ids: list[str],
    confidence: int = 0,
    notes: str = "",
) -> dict:
    """Build a digest record from the schema template."""
    schema = load_digest_schema()
    ref_date = start_date

    if digest_type == "weekly":
        digest_id = build_weekly_digest_id(start_date, end_date)
    else:
        digest_id = build_digest_id(digest_type, ref_date)

    record = deepcopy(schema)
    record["digest_id"] = digest_id
    record["digest_type"] = digest_type
    record["created_at"] = datetime.now(timezone.utc).isoformat()
    record["date_range"]["start"] = start_date.isoformat()
    record["date_range"]["end"] = end_date.isoformat()
    record["summary"] = summary
    record["key_themes"] = key_themes
    record["linked_record_ids"] = linked_record_ids
    record["linked_quant_ids"] = linked_quant_ids
    record["confidence"] = confidence
    record["notes"] = notes

    return record


def save_digest_record(digest: dict, output_dir: Path) -> Path:
    """Save a digest record to the specified output directory."""
    output_dir.mkdir(parents=True, exist_ok=True)

    digest_id = digest.get("digest_id", "unknown")
    output_path = output_dir / f"{digest_id}.json"

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(digest, f, indent=2, ensure_ascii=False)

    return output_path


def build_records_context(records: list[dict], max_records: int = 20) -> str:
    """Build a text context block from records for AI synthesis."""
    if not records:
        return "No records available for this period."

    # Sort by quality tier score descending if available
    sorted_records = sorted(
        records,
        key=lambda r: r.get("quality_tier", {}).get("score", 0),
        reverse=True,
    )

    selected = sorted_records[:max_records]
    context_parts = []

    for i, record in enumerate(selected, 1):
        part = (
            f"--- Record {i} ---\n"
            f"ID: {record.get('id', 'unknown')}\n"
            f"Title: {record.get('title', 'N/A')}\n"
            f"Topic: {record.get('topic', 'N/A')}\n"
            f"Source: {record.get('source', {}).get('name', 'N/A')}\n"
            f"Summary: {record.get('summary', 'N/A')}\n"
            f"Key Points: {', '.join(record.get('key_points', [])[:5])}\n"
            f"Why it matters: {record.get('why_it_matters', 'N/A')}\n"
            f"Macro Context: {record.get('macro_context', 'N/A')}\n"
            f"Market Structure Context: {record.get('market_structure_context', 'N/A')}\n"
            f"Tags: {', '.join(record.get('tags', []))}\n"
        )

        if record.get("linked_quant_context"):
            quant_links = [
                q.get("record_id", "") for q in record["linked_quant_context"]
            ]
            part += f"Linked Quants: {', '.join(quant_links)}\n"

        part += "---\n"
        context_parts.append(part)

    return "\n".join(context_parts)


def call_minimax_for_synthesis(
    prompt: str, records_context: str, max_retries: int = 2
) -> dict:
    """Call MiniMax API for digest synthesis.

    Follows the same pattern as run_summarizer.py for consistency.
    """
    load_dotenv()

    api_key = os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("OPENAI_BASE_URL", "https://api.minimax.io/v1")
    model = os.getenv("MINIMAX_MODEL", "MiniMax-M2.7")

    if not api_key:
        raise EnvironmentError("OPENAI_API_KEY is missing. Add it to your .env file.")

    client = OpenAI(api_key=api_key, base_url=base_url, timeout=120.0)

    full_prompt = (
        f"{prompt}\n\n=== RECORDS CONTEXT ===\n{records_context}\n=== END CONTEXT ==="
    )

    messages = [
        {
            "role": "system",
            "content": "Return only valid JSON. Do not include markdown fences.",
        },
        {"role": "user", "content": full_prompt},
    ]

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 2):
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0.2,
            max_completion_tokens=3000,
        )

        content = response.choices[0].message.content
        if not content:
            last_error = ValueError("MiniMax returned an empty response.")
            messages.append({"role": "assistant", "content": ""})
            messages.append(
                {
                    "role": "user",
                    "content": "Your response was empty. Return only valid JSON.",
                }
            )
            continue

        try:
            return extract_json_from_response(content)
        except ValueError as e:
            last_error = e
            if attempt <= max_retries:
                print(f"  JSON parse failed on attempt {attempt}, retrying...")
                messages.append({"role": "assistant", "content": content})
                messages.append(
                    {
                        "role": "user",
                        "content": "Your response was not valid JSON. Return only the JSON object, no markdown fences or extra text.",
                    }
                )

    raise ValueError(
        f"MiniMax failed to return valid JSON after {max_retries + 1} attempts: {last_error}"
    )


def extract_json_from_response(text: str) -> dict:
    """Extract JSON from an LLM response string."""
    text = text.strip()

    try:
        result = json.loads(text)
        if isinstance(result, dict):
            return result
        # If it's valid JSON but not a dict (array, string, etc.), treat as invalid
        raise json.JSONDecodeError("Expected dict", text, 0)
    except json.JSONDecodeError:
        pass

    decoder = json.JSONDecoder()
    for start, character in enumerate(text):
        if character != "{":
            continue
        try:
            candidate, _ = decoder.raw_decode(text, idx=start)
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict):
            return candidate

    raise ValueError("Model response did not contain valid JSON.")


def get_week_range(ref_date: date | None = None) -> tuple[date, date]:
    """Get the Monday-Sunday week range for a given date.

    Returns (monday, sunday) tuple.
    """
    if ref_date is None:
        ref_date = date.today()

    monday = ref_date - timedelta(days=ref_date.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


def format_theme_summary(groups: dict[str, list[dict]]) -> str:
    """Format grouped records into a readable theme summary."""
    if not groups:
        return "No themes identified."

    parts = []
    for theme, records in sorted(groups.items()):
        parts.append(f"\n## Theme: {theme.replace('_', ' ').title()}")
        parts.append(f"Records: {len(records)}")
        for record in records:
            parts.append(f"  - {record.get('title', 'N/A')} ({record.get('id', '')})")

    return "\n".join(parts)
