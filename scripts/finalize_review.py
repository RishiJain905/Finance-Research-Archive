import json
import shutil
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.filter_raw_records import parse_raw_record
from scripts.run_verifier import collect_hard_blockers
from scripts.verification_store import canonicalize_verification_artifact

REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"
ACCEPTED_DIR = BASE_DIR / "data" / "accepted"
REJECTED_DIR = BASE_DIR / "data" / "rejected"
INGESTION_MANIFEST_PATH = BASE_DIR / "data" / "ingestion_manifest.json"


def load_json_file(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json_file(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def move_file_if_exists(source: Path, destination: Path) -> None:
    if source.exists():
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(destination))


def load_review_context(record_id: str, record: dict) -> tuple[dict, dict]:
    metadata = {}
    rules = {}

    raw_text_path = record.get("raw_text_path", "")
    if raw_text_path:
        candidate_path = BASE_DIR / Path(raw_text_path)
        if candidate_path.exists():
            metadata = parse_raw_record(candidate_path.read_text(encoding="utf-8", errors="ignore")).get("metadata", {})

    if INGESTION_MANIFEST_PATH.exists():
        ingestion_manifest = load_json_file(INGESTION_MANIFEST_PATH)
        rules = ingestion_manifest.get("record_rules", {}).get(record_id, {})

    return metadata, rules


def apply_review_decision(record: dict, decision: str, hard_blockers: list[str]) -> dict:
    if decision == "approve" and hard_blockers:
        record["status"] = "rejected"
        record["human_review"]["required"] = False
        record["human_review"]["decision"] = "rejected_by_quality_gate"
        record["human_review"]["notes"] = ", ".join(hard_blockers)
        return record

    if decision == "approve":
        record["status"] = "accepted"
        record["human_review"]["required"] = False
        record["human_review"]["decision"] = "approved_by_human"
        record["human_review"]["notes"] = "Approved from Telegram review flow."
        return record

    record["status"] = "rejected"
    record["human_review"]["required"] = False
    record["human_review"]["decision"] = "rejected_by_human"
    record["human_review"]["notes"] = "Rejected from Telegram review flow."
    return record


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit("Usage: python scripts/finalize_review.py <record_id> <approve|reject>")

    record_id = sys.argv[1]
    decision = sys.argv[2].strip().lower()

    if decision not in {"approve", "reject"}:
        raise ValueError("Decision must be 'approve' or 'reject'.")

    record_path = REVIEW_QUEUE_DIR / f"{record_id}.json"

    if not record_path.exists():
        for alt_dir, label in [(ACCEPTED_DIR, "accepted"), (REJECTED_DIR, "rejected")]:
            alt_path = alt_dir / f"{record_id}.json"
            if alt_path.exists():
                print(f"Record '{record_id}' was already finalized and is in {label}/. Nothing to do.")
                return
        raise FileNotFoundError(
            f"Record '{record_id}' not found in review_queue, accepted, or rejected.\n"
            "This may mean the record was committed to a different branch, "
            "or the record ID is incorrect."
        )

    canonicalize_verification_artifact(record_id)

    record = load_json_file(record_path)
    metadata, rules = load_review_context(record_id, record)
    hard_blockers = collect_hard_blockers(record, metadata, rules)

    record = apply_review_decision(record, decision, hard_blockers)
    save_json_file(record_path, record)

    if record["status"] == "accepted":
        target_record_path = ACCEPTED_DIR / record_path.name

        move_file_if_exists(record_path, target_record_path)

        print("Approved and moved to accepted:")
        print(target_record_path.relative_to(BASE_DIR))
    else:
        target_record_path = REJECTED_DIR / record_path.name

        move_file_if_exists(record_path, target_record_path)

        print("Moved record to rejected:")
        print(target_record_path.relative_to(BASE_DIR))


if __name__ == "__main__":
    main()
