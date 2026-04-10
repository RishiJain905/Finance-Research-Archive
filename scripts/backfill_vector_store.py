"""Backfill Vector Store — one-time script to populate ChromaDB from accepted records.

Iterates all existing records in data/accepted/, embeds title + summary for each,
and upserts them into the vector store at data/vector_store/.

Safe to run multiple times — upserts are idempotent (existing IDs are overwritten).

Usage:
    python scripts/backfill_vector_store.py
"""

import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

ACCEPTED_DIR = BASE_DIR / "data" / "accepted"


def main() -> None:
    from scripts.vector_store import upsert_record, get_collection

    record_files = sorted(
        p for p in ACCEPTED_DIR.glob("*.json") if p.name != ".gitkeep"
    )

    if not record_files:
        print("No accepted records found — nothing to backfill.")
        return

    print(f"Backfilling {len(record_files)} accepted records into vector store...")

    success = 0
    skipped = 0
    errors = 0

    for i, record_path in enumerate(record_files, start=1):
        try:
            with record_path.open("r", encoding="utf-8") as f:
                record = json.load(f)

            record_id = record.get("id") or record_path.stem
            title = record.get("title", "")
            summary = record.get("summary", "")
            content = (title + "\n\n" + summary).strip()

            if not content:
                print(f"  [{i}/{len(record_files)}] SKIP (no content): {record_id}")
                skipped += 1
                continue

            source = record.get("source", {})
            metadata = {
                "title": title,
                "domain": source.get("domain", ""),
                "published_at": source.get("published_at", ""),
                "event_type": record.get("event_type", ""),
                "url": source.get("url", ""),
            }

            upsert_record(record_id, content, metadata)
            print(f"  [{i}/{len(record_files)}] OK: {record_id[:80]}")
            success += 1

        except Exception as e:
            print(f"  [{i}/{len(record_files)}] ERROR: {record_path.name}: {e}")
            errors += 1

    collection = get_collection()
    total_in_store = collection.count()

    print(f"\nBackfill complete.")
    print(f"  Upserted: {success}")
    print(f"  Skipped:  {skipped}")
    print(f"  Errors:   {errors}")
    print(f"  Total records in vector store: {total_in_store}")


if __name__ == "__main__":
    main()
