"""One-time migration script: JSON manifests → data/archive.db.

Reads data/ingestion_manifest.json and data/quant_ingestion_manifest.json
and inserts all records into the SQLite database at data/archive.db.

Run this script once before decommissioning the JSON manifest files:
    python scripts/migrate_manifest_to_db.py

The script is idempotent: re-running it on an already-migrated database
will skip duplicate rows (INSERT OR IGNORE / INSERT OR REPLACE semantics
match those used during live operation).
"""

import json
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

MANIFEST_PATH = BASE_DIR / "data" / "ingestion_manifest.json"
QUANT_MANIFEST_PATH = BASE_DIR / "data" / "quant_ingestion_manifest.json"

import sys

sys.path.insert(0, str(BASE_DIR))

from scripts.manifest_db import (
    DB_PATH,
    add_fingerprint,
    add_quant_series,
    ensure_schema,
    mark_url_processed,
    set_record_map,
    set_record_rules,
    upsert_seen_url,
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict:
    if not path.exists():
        print(f"  File not found, skipping: {path.relative_to(BASE_DIR)}")
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def migrate_ingestion_manifest() -> None:
    print(f"\nMigrating {MANIFEST_PATH.relative_to(BASE_DIR)} ...")
    manifest = _load_json(MANIFEST_PATH)
    if not manifest:
        return

    counts: dict[str, int] = {}

    # seen_urls: url → content_hash
    seen_urls = manifest.get("seen_urls", {})
    for url, content_hash in seen_urls.items():
        upsert_seen_url(url, content_hash or "")
    counts["seen_urls"] = len(seen_urls)

    # processed_urls: url → True  (conservative default: article type)
    processed_urls = manifest.get("processed_urls", {})
    for url in processed_urls:
        mark_url_processed(url, "article")
    counts["processed_urls (article)"] = len(processed_urls)

    # listing_urls: url → True  (listing type — re-crawl for new links)
    listing_urls = manifest.get("listing_urls", {})
    for url in listing_urls:
        mark_url_processed(url, "listing")
    counts["processed_urls (listing)"] = len(listing_urls)

    # record_map: url → record_id
    record_map = manifest.get("record_map", {})
    for url, record_id in record_map.items():
        set_record_map(url, record_id)
    counts["record_map"] = len(record_map)

    # record_rules: record_id → rules dict (stored as JSON blob)
    record_rules = manifest.get("record_rules", {})
    for record_id, rules in record_rules.items():
        set_record_rules(record_id, rules)
    counts["record_rules"] = len(record_rules)

    # title_fingerprints: fingerprint → record_id
    title_fps = manifest.get("title_fingerprints", {})
    for fp, record_id in title_fps.items():
        add_fingerprint("title_fingerprints", fp, record_id)
    counts["title_fingerprints"] = len(title_fps)

    # content_fingerprints: fingerprint → record_id
    content_fps = manifest.get("content_fingerprints", {})
    for fp, record_id in content_fps.items():
        add_fingerprint("content_fingerprints", fp, record_id)
    counts["content_fingerprints"] = len(content_fps)

    # event_fingerprints: fingerprint → record_id
    event_fps = manifest.get("event_fingerprints", {})
    for fp, record_id in event_fps.items():
        add_fingerprint("event_fingerprints", fp, record_id)
    counts["event_fingerprints"] = len(event_fps)

    print("  Migration counts:")
    for key, n in counts.items():
        print(f"    {key}: {n:,}")


def migrate_quant_manifest() -> None:
    print(f"\nMigrating {QUANT_MANIFEST_PATH.relative_to(BASE_DIR)} ...")
    manifest = _load_json(QUANT_MANIFEST_PATH)
    if not manifest:
        return

    created = manifest.get("created", [])
    today = datetime.now(timezone.utc).strftime("%Y_%m_%d")
    now = _now()

    migrated = 0
    for record_id in created:
        # record_id format: "{series_id}_{YYYY_MM_DD}"
        # Attempt to split off the trailing date stamp.
        parts = record_id.rsplit("_", 3)
        if len(parts) == 4:
            series_id = "_".join(parts[:1])
            snapshot_date = "_".join(parts[1:])
        else:
            # Fallback: treat the whole ID as the series_id, use today
            series_id = record_id
            snapshot_date = today

        add_quant_series(series_id, snapshot_date, "", processed_at=now)
        migrated += 1

    print(f"  quant_seen_series: {migrated:,}")


def main() -> None:
    print("=== Manifest to SQLite Migration ===")
    print(f"Database: {DB_PATH.relative_to(BASE_DIR)}")

    ensure_schema()
    print("Schema ready.")

    migrate_ingestion_manifest()
    migrate_quant_manifest()

    print("\nMigration complete.")
    print(
        "After verifying pipelines work with the new DB, you may delete:\n"
        f"  {MANIFEST_PATH.relative_to(BASE_DIR)}\n"
        f"  {QUANT_MANIFEST_PATH.relative_to(BASE_DIR)}"
    )


if __name__ == "__main__":
    main()
