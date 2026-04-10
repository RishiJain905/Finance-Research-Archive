# Phase 2 — Dynamic Keyword Injection: Implementation Summary

**Completed:** 2026-04-09
**Branch:** `dev-Rishi`
**Spec:** [`docs/V3-upgrades/phase-2-dynamic-keyword-injection.md`](../V3-upgrades/phase-2-dynamic-keyword-injection.md)

---

## Changes Shipped

### Step 1 — New Script: `scripts/generate_ephemeral_queries.py`

A standalone module with three public functions wired into the keyword discovery lane:

**`load_recent_accepted_records(n=10)`**
- Globs `data/accepted/*.json`, parses each file, sorts descending by `created_at`, and returns the top `n` records. Non-JSON files (e.g. `.gitkeep`) are silently skipped.

**`detect_topic_surge(records, window_hours=48, threshold=3)`**
- Trend detection hook (Step 4 — stretch). Scans records created within the last `window_hours` hours, counts occurrences per `topic` field, and returns any topic that appears in ≥ `threshold` records. Used to append one "surge query" per hot topic to the ephemeral batch.

**`generate_ephemeral_queries(n_records=10, n_queries=4)`**
- Loads recent accepted records; returns `[]` immediately if none exist (no LLM call, no error).
- Builds a compact context string (titles + topics + published dates, capped at 2 000 chars).
- Calls the existing OpenAI-compatible endpoint (`OPENAI_API_KEY` / `OPENAI_BASE_URL` / `OPENAI_MODEL`) with a prompt asking for a JSON array of `n_queries` Tavily search strings.
- Strips optional markdown code fences from the response before JSON-parsing.
- Falls back to `[]` on any network, auth, or parse error — the keyword discovery run is never blocked.
- Appends surge queries from `detect_topic_surge` before returning.

---

### Step 2 — Modified: `scripts/run_keyword_discovery.py`

**Import**
```python
from scripts.generate_ephemeral_queries import generate_ephemeral_queries
```

**Ephemeral query injection** (inserted after the rotation block, before the main candidate loop)

`generate_ephemeral_queries()` is called once per run. Each returned string is wrapped into a temporary query config dict:

```python
{
    "id": "ephemeral_0",   # index-based
    "query": "<LLM-generated string>",
    "source": "ephemeral",
    "search_type": "news",
    "days_back": 2,
    "max_results": 8,
    ...
}
```

These are prepended to `enabled_queries` so they run first. If generation fails, the run continues with static queries only.

**Ephemeral queries excluded from `query_performance.json`**

All three `_record_query_run(...)` call sites inside the query loop are now guarded:
```python
if query_config.get("source") != "ephemeral":
    _record_query_run(...)
```

**Date-stamping static queries** (Step 3)

In `execute_query()`, after reading the query string:
```python
if query_config.get("source") != "ephemeral":
    _month_year = datetime.now(timezone.utc).strftime("%B %Y")
    query = f"{query} {_month_year}"
```

The local variable `query` is modified; `keyword_queries.json` is never written to. Ephemeral queries are excluded because they are already date-aware from their LLM-generation prompt.

---

## Files Changed

| File | Change |
|------|--------|
| `scripts/generate_ephemeral_queries.py` | New — LLM-based ephemeral query generator with trend-surge detection |
| `scripts/run_keyword_discovery.py` | Import added; ephemeral injection block; `_record_query_run` guards; date-stamp in `execute_query()` |

---

## Acceptance Criteria — Verified

- Each keyword discovery run calls `generate_ephemeral_queries()` and injects the results as temporary query configs prepended to the static query list
- Static queries submitted to Tavily include the current month/year suffix (e.g. `"repo market stress liquidity April 2026"`)
- Ephemeral query configs carry `"source": "ephemeral"`; all three `_record_query_run` call sites are gated on `source != "ephemeral"` — ephemeral queries will never appear in `data/candidate_manifests/query_performance.json`
- If `generate_ephemeral_queries()` returns an empty list (no records, no API key, LLM error), the run proceeds with static queries only — no exception is raised
- Trend detection hook: if 3+ accepted records share the same `topic` within the last 48 hours, an additional surge query is appended to the ephemeral batch
