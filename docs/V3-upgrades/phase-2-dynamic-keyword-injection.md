# Phase 2 — Dynamic Keyword Injection

**Depends on:** Phase 1 complete  
**Priority:** High — makes the keyword discovery lane genuinely useful across time rather than stagnating on fixed topics

---

## What This Phase Does

Replaces the fully static keyword query system with a two-layer approach:

1. **Static base queries** (existing `keyword_queries.json`) — kept for evergreen topics
2. **Dynamic ephemeral queries** — generated fresh each run from recent accepted records and current date context, not persisted to config

---

## Why Static Queries Fail Over Time

`run_keyword_discovery.py` reads `config/keyword_queries.json` and submits the same literal strings to Tavily every run. With `days_back: 3` (fixed in Phase 1), Tavily's news index will at least return recent articles — but the *topics* being searched never evolve. If a new macro event emerges (e.g. a sudden tariff shock, a new central bank framework review), the static queries will not surface it until someone manually edits the JSON.

---

## Implementation Plan

### Step 1 — Create `scripts/generate_ephemeral_queries.py`

A new script that:

1. Loads the 10 most recently accepted records from `data/accepted/` (sorted by `processed_at`)
2. Calls the existing MiniMax/OpenAI-compatible LLM endpoint (already wired in `run_summarizer.py`) with a prompt like:
  > "You are a financial research assistant. Based on these recent research records, generate 4 Tavily search queries that would surface important follow-up developments from today. Return a JSON array of query strings only."
3. Returns a list of query strings to be used as one-time searches in the current keyword discovery run
4. Appends the current month/year to each static base query when submitting to Tavily (e.g. `"repo market stress liquidity April 2026"`)

### Step 2 — Modify `scripts/run_keyword_discovery.py`

At the top of each run, before the existing query rotation logic:

1. Call `generate_ephemeral_queries()` — get 3–4 LLM-generated queries
2. Construct temporary query config objects (same shape as `keyword_queries.json` entries) with `days_back: 2`, `max_results: 8`, `search_type: "news"`, `source: "ephemeral"`
3. Run these through the exact same Tavily search + candidate pipeline as static queries
4. Do **not** write ephemeral queries to `query_performance.json` — they are not tracked for rotation

### Step 3 — Date-Stamp Static Queries

In the query submission function, automatically append `" {month} {year}"` to the query string sent to Tavily (without modifying `keyword_queries.json`). This ensures Tavily's ranking favors recent results for the same topic.

### Step 4 — Trend Detection Hook (Optional / Stretch)

If the same topic cluster appears in 3+ accepted records within a 48-hour window (using existing clustering logic from `cluster_records.py`), automatically generate a "surge query" for that topic and add it to the ephemeral batch. This creates a feedback loop where the archive self-reinforces coverage of fast-moving stories.

---

## New GitHub Secrets Required

None. The LLM calls use the existing `OPENAI_API_KEY` and `OPENAI_BASE_URL` secrets already in the workflows.

---

## Files Changed in This Phase


| File                                              | Change                                                               |
| ------------------------------------------------- | -------------------------------------------------------------------- |
| `scripts/generate_ephemeral_queries.py`           | New script — LLM-based query generation                              |
| `scripts/run_keyword_discovery.py`                | Import and call ephemeral query generator; date-stamp static queries |
| `.github/workflows/process-keyword-discovery.yml` | No change needed — script handles new logic internally               |


---

## Acceptance Criteria

- Each keyword discovery run produces at least 2 ephemeral queries derived from recent accepted records
- Static queries submitted to Tavily include the current month/year suffix
- Ephemeral queries do not appear in `data/candidate_manifests/query_performance.json`
- At least one ephemeral query per run returns a candidate not surfaced by static queries