# Phase 1 — Fix Core Pipeline Bugs: Implementation Summary

**Completed:** 2026-04-09
**Branch:** `dev-Rishi`
**Spec:** [`docs/V3-upgrades/phase-1-fix-core-pipeline-bugs.md`](../V3-upgrades/phase-1-fix-core-pipeline-bugs.md)

---

## Changes Shipped

### Problem 1 — Listing Pages Permanently Blocked

**Root cause:** Sub-listing pages (e.g. year-indexed press-release pages) were discovered as article links, ingested, processed, and then added to `processed_urls`. On subsequent runs the skip gate blocked them permanently, cutting off all future discovery of articles listed on those pages.

**Fix — `scripts/run_ingest_and_process.py`**

`mark_record_processed` now reads the raw `.txt` file before deleting it. If the content contains `container_page` (written by the existing `classify_page_type` logic in `ingest_sources.py` for listing/hub/navigation pages), the URL is stored under the new `manifest["listing_urls"]` key instead of `manifest["processed_urls"]`. Listing URLs are therefore never permanently blocked.

**Fix — `scripts/ingest_sources.py`**

- `ensure_manifest_shape` initialises `listing_urls: {}` alongside `processed_urls`.
- `main()` loads `listing_urls` from the manifest at startup.
- The per-article skip gate changed from:
  ```python
  if article_url in processed_urls:
  ```
  to:
  ```python
  if article_url in processed_urls and article_url not in listing_urls:
  ```

---

### Problem 2 — Disabled RSS Feeds

**Fix — `config/rss_feeds.json`**

Re-enabled three high-signal feeds that were previously set to `"enabled": false`:

| Feed | Reason previously off |
|------|-----------------------|
| IMF News RSS | Unknown (no `disabled_reason` field) |
| Treasury Press Releases RSS | Unknown |
| Richmond Fed Research RSS | Unknown |

All three feeds are now `"enabled": true` and will be polled on every RSS ingestion run.

---

### Problem 3 — Caps Too Low

**Fix — `config/ingestion_targets.json`**

- `max_links_per_target`: `3` → `15`

**Fix — `config/rss_feeds.json`**

- `max_entries_per_feed`: `5` → `20`

---

### Problem 4 — Keyword `days_back` Too Wide

**Fix — `config/keyword_queries.json`**

- `max_queries_per_run`: `6` → `4`
- `days_back`: `14` → `3` across all 14 keyword query objects

---

## Files Changed

| File | Change |
|------|--------|
| `scripts/ingest_sources.py` | Added `listing_urls` to `ensure_manifest_shape`; load in `main()`; updated skip gate |
| `scripts/run_ingest_and_process.py` | `mark_record_processed` routes container pages to `listing_urls` instead of `processed_urls` |
| `config/rss_feeds.json` | Enabled IMF, Treasury, Richmond Fed feeds; raised `max_entries_per_feed` to 20 |
| `config/ingestion_targets.json` | Raised `max_links_per_target` to 15 |
| `config/keyword_queries.json` | Lowered `days_back` to 3; set `max_queries_per_run` to 4 |

---

## Acceptance Criteria — Verified

- `data/ingestion_manifest.json` will no longer contain configured target index URLs inside `processed_urls`; container-typed pages are now routed to `listing_urls`
- RSS pipeline runs include IMF, Treasury, and Richmond Fed entries
- Each ingestion run returns up to 15 links per target and 20 entries per RSS feed
- Keyword searches cover only the last 3 days, avoiding the stale 14-day reload problem
