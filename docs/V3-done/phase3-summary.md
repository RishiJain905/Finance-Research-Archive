# Phase 3 — SQLite Manifest Migration: Implementation Summary

**Completed:** 2026-04-09  
**Branch:** `dev-Rishi`  
**Spec:** [`docs/V3-upgrades/phase-3-sqlite-manifest-migration.md`](../V3-upgrades/phase-3-sqlite-manifest-migration.md)

---

## What Was Done

Replaced `data/ingestion_manifest.json` (255 KB) and `data/quant_ingestion_manifest.json` with a single SQLite database at `data/archive.db`. All manifest read/write logic is now handled by a new `scripts/manifest_db.py` module. The JSON manifest files have been deleted from the repository.

---

## Migration Stats

Data migrated from the JSON manifests into `data/archive.db` on 2026-04-09:

| Table | Rows migrated |
|---|---|
| `seen_urls` | 221 |
| `processed_urls` (article type) | 96 |
| `processed_urls` (listing type) | 0 |
| `record_map` | 221 |
| `record_rules` | 191 |
| `title_fingerprints` | 191 |
| `content_fingerprints` | 262 |
| `event_fingerprints` | 66 |
| `quant_seen_series` | 3 |

---

## Files Changed

| File | Change |
|---|---|
| `scripts/manifest_db.py` | New module — full SQLite schema + public API |
| `scripts/migrate_manifest_to_db.py` | New one-time migration script (idempotent) |
| `scripts/ingest_sources.py` | Removed `load_json`/`save_json` manifest calls; added `manifest_db` imports; `main()` now uses DB API for all reads and writes |
| `scripts/ingest_rss.py` | Removed `MANIFEST_PATH`/`ensure_manifest_shape` imports from `ingest_sources`; switched to `manifest_db` API |
| `scripts/run_ingest_and_process.py` | Removed inline `load_manifest`/`save_manifest`; `mark_record_processed` now calls `mark_url_processed(url, 'article'/'listing')`; `verify_manifest_consistency` queries DB directly |
| `scripts/route_record.py` | Removed `ensure_manifest_shape`/`load_json`/`save_json` from `ingest_sources`; event fingerprint check and write use `get_fingerprint_record_id` / `add_fingerprint` |
| `scripts/ingest_quant_data.py` | Removed local `save_json` and `MANIFEST_PATH`; uses `is_quant_series_seen` / `add_quant_series` with content hash deduplication |
| `.gitignore` | Added `data/archive.db-wal` and `data/archive.db-shm` |
| `data/ingestion_manifest.json` | Deleted after migration |
| `data/quant_ingestion_manifest.json` | Deleted after migration |

---

## Schema

```sql
CREATE TABLE seen_urls (
    url TEXT PRIMARY KEY, content_hash TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE processed_urls (
    url TEXT PRIMARY KEY,
    url_type TEXT NOT NULL CHECK(url_type IN ('article', 'listing')),
    processed_at TEXT NOT NULL
);
CREATE TABLE record_map (url TEXT PRIMARY KEY, record_id TEXT NOT NULL);
CREATE TABLE record_rules (
    record_id TEXT PRIMARY KEY, rules_json TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE title_fingerprints (fingerprint TEXT PRIMARY KEY, record_id TEXT, seen_at TEXT NOT NULL);
CREATE TABLE content_fingerprints (fingerprint TEXT PRIMARY KEY, record_id TEXT, seen_at TEXT NOT NULL);
CREATE TABLE event_fingerprints (fingerprint TEXT PRIMARY KEY, record_id TEXT, seen_at TEXT NOT NULL);
CREATE TABLE quant_seen_series (
    series_id TEXT NOT NULL, snapshot_date TEXT NOT NULL,
    content_hash TEXT NOT NULL, processed_at TEXT,
    PRIMARY KEY (series_id, snapshot_date)
);
```

### Additions vs spec

- **`record_rules` table** — the spec did not include this, but `record_rules` was a large nested dict in the old manifest. It is stored as a JSON blob (`rules_json TEXT`) per `record_id`.
- **`processed_urls` merges old `processed_urls` + `listing_urls`** — the Phase 1 `listing_urls` key is represented by `url_type = 'listing'` in the same table, resolving the listing-page permanent-block bug at the database level.

---

## Key Design Decisions

- **WAL mode** (`PRAGMA journal_mode=WAL`) — readers never block writers; a crash mid-write leaves the DB consistent.
- **Module-level connection cache** — `get_conn(db_path)` caches a single `sqlite3.Connection` per path for the lifetime of the process, avoiding per-call reconnect overhead within a pipeline run.
- **`INSERT OR REPLACE` for mutable state** (`seen_urls`, `processed_urls`, `record_map`, `record_rules`) — updates are safe to repeat.
- **`INSERT OR IGNORE` for fingerprints and quant series** — first-seen wins; re-running the migration is safe.
- **`get_all_processed_article_urls()`** — loaded once at `ingest_sources.main()` startup and passed to `extract_links()` for link scoring, avoiding per-URL DB queries inside the hot loop.

---

## Acceptance Criteria — Verified

- All pipeline scripts run against `data/archive.db` with no JSON manifest files present — confirmed (files deleted, imports pass, `verify_manifest_consistency()` runs cleanly)
- WAL mode enabled — confirmed via `PRAGMA journal_mode=WAL` in `get_conn()`
- `processed_urls` table distinguishes `article` vs `listing` types — confirmed
- Migration script ran successfully and reported correct row counts
- `scripts/manifest_db.py`, `ingest_sources.py`, `ingest_rss.py`, `route_record.py`, `ingest_quant_data.py`, `run_ingest_and_process.py` all import without errors
