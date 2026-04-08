# Phase 1 — Fix Core Pipeline Bugs

**Priority:** Critical — do this first. These are bugs causing the existing pipelines to find zero new content on most CI runs.

---

## What This Phase Does

Fixes four compounding issues in the current V2 pipelines that together cause GitHub Actions to run successfully but ingest nothing new.

---

## Problem 1 — Listing Pages Are Permanently Blocked

**File:** `scripts/ingest_sources.py`, `scripts/run_ingest_and_process.py`

When any URL is fully processed, `mark_record_processed()` writes it to `processed_urls` in `data/ingestion_manifest.json`. This is correct for article URLs (don't re-summarize the same article). But **hub and listing pages** — like `federalreserve.gov/newsevents/pressreleases/2026-press.htm` — are also landing in `processed_urls`. Once that happens, `ingest_sources.py` hits the early-exit check at line ~654 and never re-fetches that index page, meaning **no new links from that source are ever discovered**.

**Fix:** In `ingest_sources.py`, before the `processed_urls` check, compare the article URL against the configured `allowed_prefixes` for that target. If the URL matches a prefix that looks like a listing/index page (i.e., it is also an entry in `ingestion_targets.json` as a `url`), do not skip it — always re-fetch it to extract fresh links. Alternatively, maintain a separate `listing_urls` set in the manifest that is never used as a skip gate.

---

## Problem 2 — Disabled RSS Feeds

**File:** `config/rss_feeds.json`

Several high-signal feeds have `"enabled": false` and are never polled. At minimum the following should be re-enabled:

- IMF press releases
- US Treasury news
- Richmond Fed

**Fix:** Set `"enabled": true` on these feeds. Review all disabled entries and add a comment field (e.g. `"disabled_reason"`) for any that are intentionally off so the reason is not lost.

---

## Problem 3 — Caps Are Too Low

**Files:** `config/ingestion_targets.json`, `config/rss_feeds.json`

Current values:

- `max_links_per_target: 3` — a source with 20 new articles today contributes 3
- `max_entries_per_feed: 5` — same problem for RSS

**Fix:**

- Raise `max_links_per_target` to `15`
- Raise `max_entries_per_feed` to `20`

These are safe to raise because the downstream deduplication and scoring pipeline filters aggressively anyway.

---

## Problem 4 — Keyword Lane `days_back` Too Wide for Run Cadence

**File:** `config/keyword_queries.json`

The keyword discovery workflow runs every 4 hours. With `days_back: 14`, Tavily searches across the last two weeks every single run. The result set barely changes between runs, and the 7-day URL dedup window in `dedupe_candidates.py` ensures the same results block new ones.

**Fix:**

- Set `days_back: 3` for all keyword queries (covers recent news without reloading the same 14-day pool)
- Set `max_queries_per_run: 4` to spread budget across more queries per day

---

## New GitHub Secrets Required

None. All fixes are configuration and logic changes to existing scripts.

---

## Files Changed in This Phase


| File                                | Change                                                                |
| ----------------------------------- | --------------------------------------------------------------------- |
| `scripts/ingest_sources.py`         | Add listing URL detection; skip `processed_urls` gate for index pages |
| `scripts/run_ingest_and_process.py` | Guard `mark_record_processed` to not flag index/listing URLs          |
| `config/rss_feeds.json`             | Re-enable disabled feeds; add `disabled_reason` fields                |
| `config/ingestion_targets.json`     | Raise `max_links_per_target` to 15                                    |
| `config/keyword_queries.json`       | Lower `days_back` to 3; adjust `max_queries_per_run`                  |


---

## Acceptance Criteria

- After this phase, the `process-articles` workflow should show non-zero new records on every run during active news periods
- `data/ingestion_manifest.json` should not contain any of the configured target index URLs inside `processed_urls`
- Keyword discovery runs should consistently return candidates not seen in the previous run