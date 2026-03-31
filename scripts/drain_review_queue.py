"""Drain the review queue by promoting borderline records to accepted.

Targets records where:
  - status == "review_queue"
  - human_review.required == False  (set by the two-tier acceptance gate for
    records with verification_confidence 6-7 and no other issues)

Pre-flight checks before promoting each record:
  - Event-level dedup: skip if the same (domain, date, event_type) is already
    accepted in the manifest event_fingerprints table.
  - Explicit duplicate notes: skip if human_review.notes contains
    "duplicate_event:" (set by route_record.py during a previous run).

After passing pre-flight, sets status → "accepted" and delegates to
route_record.py, which handles the file move, event fingerprint registration,
and memory score update.
"""
import json
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.ingest_sources import ensure_manifest_shape, load_json
from scripts.route_record import compute_event_fingerprint

REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"
MANIFEST_PATH = BASE_DIR / "data" / "ingestion_manifest.json"


def load_json_file(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json_file(path: Path, data: dict) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def find_drainable_records() -> list[Path]:
    """Return paths to review_queue records eligible for auto-promotion."""
    candidates = []
    for path in sorted(REVIEW_QUEUE_DIR.glob("*.json")):
        if path.name.endswith("_verification.json"):
            continue
        try:
            record = load_json_file(path)
        except (json.JSONDecodeError, OSError):
            continue
        if record.get("status") != "review_queue":
            continue
        if record.get("human_review", {}).get("required", True):
            continue
        # Skip records already flagged as event duplicates
        notes = record.get("human_review", {}).get("notes", "")
        if "duplicate_event:" in notes:
            continue
        candidates.append(path)
    return candidates


def main() -> None:
    candidates = find_drainable_records()

    if not candidates:
        print("No drainable records found in review_queue.")
        return

    print(f"Found {len(candidates)} drainable record(s):\n")
    for p in candidates:
        print(f"  {p.stem}")

    manifest = ensure_manifest_shape(load_json(MANIFEST_PATH, {}))
    event_fingerprints = manifest.get("event_fingerprints", {})

    python_cmd = sys.executable
    promoted: list[str] = []
    skipped: list[str] = []

    for record_path in candidates:
        record_id = record_path.stem
        record = load_json_file(record_path)

        print(f"\nProcessing: {record_id}")

        # Pre-flight: check event-level dedup before flipping status.
        source = record.get("source", {})
        source_domain = source.get("domain", "")
        if not source_domain:
            source_url = source.get("url", "")
            if source_url:
                source_domain = urlparse(source_url).netloc
        event_type = record.get("event_type", "")
        published_at = source.get("published_at", "")
        event_fp = compute_event_fingerprint(source_domain, published_at, event_type)

        if event_fp:
            existing = event_fingerprints.get(event_fp)
            if existing and existing != record_id:
                print(
                    f"  Skipping: event-level duplicate of already-accepted record: {existing}"
                )
                skipped.append(record_id)
                continue

        # Flip to "accepted" so route_record.py will move the file.
        record["status"] = "accepted"
        save_json_file(record_path, record)

        result = subprocess.run(
            [python_cmd, "-m", "scripts.route_record", record_id],
            cwd=BASE_DIR,
            text=True,
            capture_output=True,
        )

        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)

        if result.returncode != 0:
            print(f"  route_record failed (exit {result.returncode}). Reverting status.")
            record["status"] = "review_queue"
            save_json_file(record_path, record)
            skipped.append(record_id)
        else:
            promoted.append(record_id)
            # Reload manifest so event_fingerprints stays in sync between iterations.
            manifest = ensure_manifest_shape(load_json(MANIFEST_PATH, {}))
            event_fingerprints = manifest.get("event_fingerprints", {})

    print("\n--- Drain complete ---")
    print(f"Promoted : {len(promoted)}")
    for r in promoted:
        print(f"  + {r}")
    if skipped:
        print(f"Skipped  : {len(skipped)}")
        for r in skipped:
            print(f"  - {r}")


if __name__ == "__main__":
    main()
