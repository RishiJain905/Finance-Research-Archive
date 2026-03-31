import hashlib
import json
import shutil
import sys
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.verification_store import canonicalize_verification_artifact
from scripts.ingest_sources import ensure_manifest_shape, load_json, save_json
from scripts.assign_quality_tier import assign_quality_tier

REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"
ACCEPTED_DIR = BASE_DIR / "data" / "accepted"
REJECTED_DIR = BASE_DIR / "data" / "rejected"
MANIFEST_PATH = BASE_DIR / "data" / "ingestion_manifest.json"


def load_json_file(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def move_file_if_exists(source: Path, destination: Path) -> None:
    if source.exists():
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(destination))


def compute_event_fingerprint(
    domain: str, published_at: str, event_type: str
) -> str | None:
    """Return a sha1 fingerprint for (domain, date, event_type), or None if inputs are missing."""
    date_part = (published_at or "")[:10].strip()
    if not domain or not date_part or not event_type:
        return None
    key = f"{domain}|{date_part}|{event_type}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python scripts/route_record.py <record_id>")

    record_id = sys.argv[1]

    record_path = REVIEW_QUEUE_DIR / f"{record_id}.json"
    canonicalize_verification_artifact(record_id)

    record = load_json_file(record_path)
    status = record.get("status", "review_queue")

    # Import here to avoid circular imports
    from scripts.memory_manager import update_all_memory_on_outcome

    source_url = record.get("source", {}).get("url", "")
    source_domain = record.get("source", {}).get("domain", "")
    # Fallback for legacy records produced before domain was added to the schema
    if not source_domain and source_url:
        source_domain = urlparse(source_url).netloc

    manifest = ensure_manifest_shape(load_json(MANIFEST_PATH, {}))

    if status == "accepted":
        # Event-level deduplication: demote to review_queue if the same
        # (domain, date, event_type) has already been accepted.
        event_type = record.get("event_type", "")
        published_at = record.get("source", {}).get("published_at", "")
        event_fp = compute_event_fingerprint(source_domain, published_at, event_type)

        if event_fp:
            existing = manifest["event_fingerprints"].get(event_fp)
            if existing and existing != record_id:
                print(
                    f"  Event-level duplicate detected (fingerprint matches {existing}). "
                    "Demoting to review_queue."
                )
                record["status"] = "review_queue"
                record["human_review"]["required"] = False
                existing_note = record["human_review"].get("notes", "")
                dup_note = f"duplicate_event:{existing}"
                if dup_note not in existing_note:
                    record["human_review"]["notes"] = (
                        f"{existing_note}, {dup_note}".lstrip(", ")
                    )
                with record_path.open("w", encoding="utf-8") as f:
                    json.dump(record, f, indent=2, ensure_ascii=False)
                status = "review_queue"

        if status == "accepted":
            target_record_path = ACCEPTED_DIR / record_path.name

            # Assign quality tier before moving to accepted
            tier_block = assign_quality_tier(record)
            record["quality_tier"] = tier_block["quality_tier"]

            # Write updated record (with tier) to target location
            target_record_path.parent.mkdir(parents=True, exist_ok=True)
            with target_record_path.open("w", encoding="utf-8") as f:
                json.dump(record, f, indent=2, ensure_ascii=False)

            # Remove the original from review_queue
            if record_path.exists():
                record_path.unlink()

            print("Moved record to accepted:")
            print(target_record_path.relative_to(BASE_DIR))
            print(
                f"  Quality tier: {tier_block['quality_tier']['tier']} (score: {tier_block['quality_tier']['score']})"
            )

            # Register event fingerprint so future duplicates are caught.
            if event_fp:
                manifest["event_fingerprints"][event_fp] = record_id
                save_json(MANIFEST_PATH, manifest)

            if source_domain:
                result = update_all_memory_on_outcome(
                    domain=source_domain,
                    outcome="accepted",
                    source_id=record.get("source", {}).get("source_id"),
                    source_type=record.get("lane", "manual"),
                    url=record.get("url", ""),
                    candidate_id=record.get("id", record_path.stem),
                )
                print(
                    f"Memory updated - domain trust: {result['domain_memory']['trust_score']}"
                )

    elif status == "rejected":
        target_record_path = REJECTED_DIR / record_path.name

        move_file_if_exists(record_path, target_record_path)

        print("Moved record to rejected:")
        print(target_record_path.relative_to(BASE_DIR))

        if source_domain:
            result = update_all_memory_on_outcome(
                domain=source_domain,
                outcome="rejected",
                source_id=record.get("source", {}).get("source_id"),
                source_type=record.get("lane", "manual"),
                url=record.get("url", ""),
                candidate_id=record.get("id", record_path.stem),
            )
            print(
                f"Memory updated - domain trust: {result['domain_memory']['trust_score']}"
            )

    else:
        print("Record remains in review_queue:")
        print(record_path.relative_to(BASE_DIR))

        if source_domain:
            result = update_all_memory_on_outcome(
                domain=source_domain,
                outcome="review",
                source_id=record.get("source", {}).get("source_id"),
                source_type=record.get("lane", "manual"),
                url=record.get("url", ""),
                candidate_id=record.get("id", record_path.stem),
            )
            print(
                f"Memory updated - domain trust: {result['domain_memory']['trust_score']}"
            )


if __name__ == "__main__":
    main()
