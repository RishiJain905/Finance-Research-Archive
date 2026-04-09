"""
Ephemeral Query Generator for Keyword Discovery Lane.

Generates fresh, LLM-derived Tavily search queries each run based on the most
recently accepted records in the archive. These queries complement the static
keyword_queries.json config by surfacing follow-up coverage on fast-moving topics.

Public API
----------
generate_ephemeral_queries(n_records, n_queries) -> list[str]
    Main entry point called by run_keyword_discovery.py.

load_recent_accepted_records(n) -> list[dict]
    Returns the n most recently created accepted records.

detect_topic_surge(records, window_hours, threshold) -> list[str]
    Returns topics that appear in >= threshold records within window_hours.
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

ACCEPTED_DIR = BASE_DIR / "data" / "accepted"

# Max characters of context to feed the LLM (keeps token cost low)
_MAX_CONTEXT_CHARS = 2_000


# ============================================================================
# Record Loading
# ============================================================================


def load_recent_accepted_records(n: int = 10) -> list[dict]:
    """
    Load the n most recently created accepted records from data/accepted/.

    Skips non-JSON files (e.g. .gitkeep). Records are sorted descending by
    their ``created_at`` ISO timestamp; ties preserve filesystem order.

    Args:
        n: Maximum number of records to return.

    Returns:
        List of record dicts, newest first.
    """
    records: list[dict] = []

    for path in ACCEPTED_DIR.glob("*.json"):
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                records.append(data)
        except (json.JSONDecodeError, IOError):
            continue

    # Sort newest-first by created_at; missing field sorts to the beginning
    records.sort(
        key=lambda r: r.get("created_at", ""),
        reverse=True,
    )
    return records[:n]


# ============================================================================
# Trend / Surge Detection (Step 4 — stretch)
# ============================================================================


def detect_topic_surge(
    records: list[dict],
    window_hours: int = 48,
    threshold: int = 3,
) -> list[str]:
    """
    Identify topics that appear in >= threshold accepted records within the
    last window_hours hours.

    Args:
        records: Accepted record dicts (any age — the function applies the
                 window filter internally).
        window_hours: How far back to look (default 48 h).
        threshold: Minimum record count to count as a surge.

    Returns:
        Sorted list of topic strings that are currently surging.
    """
    cutoff = datetime.now(timezone.utc).timestamp() - window_hours * 3600
    topic_counts: dict[str, int] = {}

    for record in records:
        created_raw = record.get("created_at", "")
        if not created_raw:
            continue
        try:
            # Handle both offset-aware and offset-naive ISO strings
            dt = datetime.fromisoformat(created_raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            ts = dt.timestamp()
        except (ValueError, TypeError):
            continue

        if ts < cutoff:
            continue

        topic = record.get("topic", "").strip()
        if topic:
            topic_counts[topic] = topic_counts.get(topic, 0) + 1

    return sorted(t for t, count in topic_counts.items() if count >= threshold)


# ============================================================================
# LLM-Based Query Generation
# ============================================================================


def _build_context_snippet(records: list[dict], max_chars: int = _MAX_CONTEXT_CHARS) -> str:
    """
    Build a compact multi-line context string from accepted records.

    Each line: "{title} [{topic}] ({published_at})"
    Truncated to max_chars to keep the LLM prompt token-efficient.
    """
    lines: list[str] = []
    for rec in records:
        title = rec.get("title", "").strip()
        topic = rec.get("topic", "").strip()
        published = rec.get("source", {}).get("published_at", "").strip()
        if title:
            line = f"- {title}"
            if topic:
                line += f" [{topic}]"
            if published:
                line += f" ({published})"
            lines.append(line)

    context = "\n".join(lines)
    if len(context) > max_chars:
        context = context[:max_chars].rsplit("\n", 1)[0]  # trim at last full line
    return context


def generate_ephemeral_queries(
    n_records: int = 10,
    n_queries: int = 4,
) -> list[str]:
    """
    Generate ephemeral Tavily search queries from recent accepted records.

    Uses the existing OPENAI_API_KEY / OPENAI_BASE_URL environment variables
    (same credentials as run_summarizer.py). Falls back to [] on any error so
    the keyword discovery run is never blocked.

    Args:
        n_records: Number of recent accepted records to use as context.
        n_queries: Target number of queries to request from the LLM.

    Returns:
        List of query strings (may be empty if generation fails or no records
        exist).
    """
    # --- Load recent records ---
    records = load_recent_accepted_records(n_records)
    if not records:
        print("[Ephemeral] No accepted records found — skipping LLM query generation")
        return []

    context = _build_context_snippet(records)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    month_year = datetime.now(timezone.utc).strftime("%B %Y")

    prompt = (
        f"You are a financial research assistant. Today is {today}.\n\n"
        f"The following research records were recently accepted into a finance archive:\n\n"
        f"{context}\n\n"
        f"Based on these records, generate exactly {n_queries} Tavily web-search queries "
        f"that would surface important follow-up developments as of {month_year}. "
        f"Each query should be concise (5-12 words), specific, and likely to return "
        f"recent news or research. "
        f"Return a JSON array of query strings only, with no explanation or extra keys. "
        f"Example format: [\"query one\", \"query two\", \"query three\", \"query four\"]"
    )

    # --- Call LLM ---
    try:
        from openai import OpenAI  # local import to keep module importable without openai

        api_key = os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("OPENAI_BASE_URL") or None
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

        if not api_key:
            print("[Ephemeral] OPENAI_API_KEY not set — skipping LLM query generation")
            return []

        client = OpenAI(api_key=api_key, base_url=base_url)
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
        )
        raw_content = response.choices[0].message.content or ""
    except Exception as e:
        print(f"[Ephemeral] LLM call failed: {e}")
        return []

    # --- Parse JSON array from response ---
    queries: list[str] = []
    try:
        # Strip markdown code fences if the model wrapped the output
        cleaned = raw_content.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("```")[1]
            if cleaned.startswith("json"):
                cleaned = cleaned[4:]
        parsed = json.loads(cleaned.strip())
        if isinstance(parsed, list):
            queries = [str(q).strip() for q in parsed if str(q).strip()]
    except (json.JSONDecodeError, IndexError, ValueError) as e:
        print(f"[Ephemeral] Failed to parse LLM response as JSON: {e}")
        print(f"[Ephemeral] Raw response: {raw_content[:200]}")
        return []

    if not queries:
        print("[Ephemeral] LLM returned an empty query list")
        return []

    print(f"[Ephemeral] Generated {len(queries)} LLM queries from {len(records)} records")

    # --- Trend surge hook (Step 4) ---
    # Load all accepted records (not just the n_records slice) for a full 48-h window
    all_records = load_recent_accepted_records(n=200)
    surging_topics = detect_topic_surge(all_records)
    if surging_topics:
        print(f"[Ephemeral] Topic surge detected: {surging_topics}")
        for topic in surging_topics:
            surge_query = f"{topic} latest developments {month_year}"
            if surge_query not in queries:
                queries.append(surge_query)
        print(f"[Ephemeral] Added {len(surging_topics)} surge query/queries")

    return queries
