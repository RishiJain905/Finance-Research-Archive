import json
import shutil
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.verification_store import canonicalize_verification_artifact

REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"
ACCEPTED_DIR = BASE_DIR / "data" / "accepted"
REJECTED_DIR = BASE_DIR / "data" / "rejected"


def load_json_file(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def move_file_if_exists(source: Path, destination: Path) -> None:
    if source.exists():
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(destination))


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python scripts/route_record.py <record_id>")

    record_id = sys.argv[1]

    record_path = REVIEW_QUEUE_DIR / f"{record_id}.json"
    canonicalize_verification_artifact(record_id)

    record = load_json_file(record_path)
    status = record.get("status", "review_queue")

    if status == "accepted":
        target_record_path = ACCEPTED_DIR / record_path.name

        move_file_if_exists(record_path, target_record_path)

        print("Moved record to accepted:")
        print(target_record_path.relative_to(BASE_DIR))

    elif status == "rejected":
        target_record_path = REJECTED_DIR / record_path.name

        move_file_if_exists(record_path, target_record_path)

        print("Moved record to rejected:")
        print(target_record_path.relative_to(BASE_DIR))

    else:
        print("Record remains in review_queue:")
        print(record_path.relative_to(BASE_DIR))


if __name__ == "__main__":
    main()
