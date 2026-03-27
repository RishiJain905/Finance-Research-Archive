import json
import shutil
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"
ACCEPTED_DIR = BASE_DIR / "data" / "accepted"
REJECTED_DIR = BASE_DIR / "data" / "rejected"


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


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit("Usage: python scripts/finalize_review.py <record_id> <approve|reject>")

    record_id = sys.argv[1]
    decision = sys.argv[2].strip().lower()

    if decision not in {"approve", "reject"}:
        raise ValueError("Decision must be 'approve' or 'reject'.")

    record_path = REVIEW_QUEUE_DIR / f"{record_id}.json"
    verification_path = REVIEW_QUEUE_DIR / f"{record_id}_verification.json"

    record = load_json_file(record_path)

    if decision == "approve":
        record["status"] = "accepted"
        record["human_review"]["required"] = False
        record["human_review"]["decision"] = "approved_by_human"
        record["human_review"]["notes"] = "Approved from Telegram review flow."

        save_json_file(record_path, record)

        target_record_path = ACCEPTED_DIR / record_path.name
        target_verification_path = ACCEPTED_DIR / verification_path.name

        move_file_if_exists(record_path, target_record_path)
        move_file_if_exists(verification_path, target_verification_path)

        print("Approved and moved to accepted:")
        print(target_record_path.relative_to(BASE_DIR))

    else:
        record["status"] = "rejected"
        record["human_review"]["required"] = False
        record["human_review"]["decision"] = "rejected_by_human"
        record["human_review"]["notes"] = "Rejected from Telegram review flow."

        save_json_file(record_path, record)

        target_record_path = REJECTED_DIR / record_path.name
        target_verification_path = REJECTED_DIR / verification_path.name

        move_file_if_exists(record_path, target_record_path)
        move_file_if_exists(verification_path, target_verification_path)

        print("Rejected and moved to rejected:")
        print(target_record_path.relative_to(BASE_DIR))


if __name__ == "__main__":
    main()