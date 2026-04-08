# Phase 3 — SQLite Manifest Migration

**Depends on:** Phase 1 complete (Phase 2 can run in parallel with this)  
**Priority:** High infrastructure — `data/ingestion_manifest.json` is already thousands of lines and growing every run; JSON file I/O on the full file is slow and a single corrupted write loses all state

---

## What This Phase Does

Replaces `data/ingestion_manifest.json` (and `data/quant_ingestion_manifest.json`) with a local SQLite database at `data/archive.db`. All manifest read/write helpers are replaced with thin SQLite wrappers behind the same function signatures, so higher-level pipeline scripts need minimal changes.

---

## Why JSON Manifests Are a Problem Now

- The manifest is loaded in full on every pipeline run and written back in full on every exit. As `seen_urls`, `processed_urls`, `title_fingerprints`, `content_fingerprints`, and `event_fingerprints` grow, this becomes megabytes of JSON being parsed and serialized multiple times per GitHub Actions minute.
- There is no atomic write — a crash mid-write corrupts the file and loses all manifest state.
- There is no queryable index. Checking "is this URL in `processed_urls`?" requires loading the entire manifest into memory.
- Git diffs on `data/ingestion_manifest.json` are enormous and noise-heavy in the commit history.

---

## Implementation Plan

### Step 1 — Create `scripts/manifest_db.py`

A new module that owns all database interaction. Schema:

```sql
-- URL tracking
CREATE TABLE seen_urls (
    url TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE processed_urls (
    url TEXT PRIMARY KEY,
    url_type TEXT NOT NULL CHECK(url_type IN ('article', 'listing')),
    processed_at TEXT NOT NULL
);

-- Record tracking
CREATE TABLE record_map (
    url TEXT PRIMARY KEY,
    record_id TEXT NOT NULL
);

-- Fingerprints
CREATE TABLE title_fingerprints (
    fingerprint TEXT PRIMARY KEY,
    record_id TEXT,
    seen_at TEXT NOT NULL
);

CREATE TABLE content_fingerprints (
    fingerprint TEXT PRIMARY KEY,
    record_id TEXT,
    seen_at TEXT NOT NULL
);

CREATE TABLE event_fingerprints (
    fingerprint TEXT PRIMARY KEY,
    record_id TEXT,
    seen_at TEXT NOT NULL
);

-- Quant tracking
CREATE TABLE quant_seen_series (
    series_id TEXT NOT NULL,
    snapshot_date TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    processed_at TEXT,
    PRIMARY KEY (series_id, snapshot_date)
);
```

Key design decisions:

- `processed_urls` has a `url_type` column distinguishing `'article'` (permanent skip) from `'listing'` (re-check for new links, skip only content hash match). This directly resolves the Phase 1 listing-page bug at the database level.
- All writes are wrapped in transactions so a crash mid-write is safe.
- The database file is committed to git just like the JSON was.

### Step 2 — Migration Script `scripts/migrate_manifest_to_db.py`

Reads `data/ingestion_manifest.json` and `data/quant_ingestion_manifest.json` and inserts all records into `data/archive.db`. Marks all existing `processed_urls` entries as `url_type = 'article'` (conservative default). Run once before decommissioning JSON manifests.

### Step 3 — Update All Scripts That Touch the Manifest

Replace `load_manifest()` / `save_manifest()` calls with `manifest_db` function calls. Affected scripts:

- `scripts/ingest_sources.py`
- `scripts/ingest_rss.py`
- `scripts/run_ingest_and_process.py`
- `scripts/route_record.py`
- `scripts/dedupe_candidates.py` (candidate index remains a separate JSON — out of scope here)
- `scripts/ingest_quant_data.py`
- `scripts/run_quant_pipeline.py`

### Step 4 — Update `.gitignore`

Keep `data/archive.db` tracked in git (same as the JSON was). Add `data/archive.db-wal` and `data/archive.db-shm` to `.gitignore` (SQLite WAL mode temp files).

### Step 5 — Remove Old JSON Manifests

After a successful migration run, delete `data/ingestion_manifest.json` and `data/quant_ingestion_manifest.json` from the repository.

---

## New GitHub Secrets Required

None. SQLite is part of Python's standard library — no new dependencies.

---

## Files Changed in This Phase


| File                                 | Change                                         |
| ------------------------------------ | ---------------------------------------------- |
| `scripts/manifest_db.py`             | New module — all SQLite interaction            |
| `scripts/migrate_manifest_to_db.py`  | New one-time migration script                  |
| `scripts/ingest_sources.py`          | Replace manifest dict calls with `manifest_db` |
| `scripts/ingest_rss.py`              | Same                                           |
| `scripts/run_ingest_and_process.py`  | Same                                           |
| `scripts/route_record.py`            | Same                                           |
| `scripts/ingest_quant_data.py`       | Same                                           |
| `scripts/run_quant_pipeline.py`      | Same                                           |
| `.gitignore`                         | Add WAL/SHM files                              |
| `data/ingestion_manifest.json`       | Deleted after migration                        |
| `data/quant_ingestion_manifest.json` | Deleted after migration                        |


---

## Acceptance Criteria

- All pipeline scripts run against `data/archive.db` with no JSON manifest files present
- A crashed mid-run leaves the database in a consistent state (verify by killing a run mid-execution and re-running)
- Git diff size for a normal pipeline run drops significantly (no more full JSON manifest diffs)
- `processed_urls` entries correctly distinguish `article` vs `listing` types

