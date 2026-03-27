import json
import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"


def load_json_file(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def should_send_review(record: dict) -> bool:
    human_review = record.get("human_review", {})
    status = record.get("status", "")
    return human_review.get("required", False) and status == "review_queue"


def main() -> None:
    python_cmd = sys.executable
    sent_any = False

    if not REVIEW_QUEUE_DIR.exists():
        print("Review queue directory does not exist.")
        return

    for record_path in sorted(REVIEW_QUEUE_DIR.glob("*.json")):
        if record_path.name.endswith("_verification.json"):
            continue

        try:
            record = load_json_file(record_path)
        except Exception as e:
            print(f"Skipping unreadable file {record_path.name}: {e}")
            continue

        if not should_send_review(record):
            continue

        record_id = record.get("id") or record_path.stem
        telegram_sent = record.get("telegram_review_sent", False)

        if telegram_sent:
            print(f"Already sent to Telegram: {record_id}")
            continue

        print(f"Sending review to Telegram: {record_id}")

        result = subprocess.run(
            [python_cmd, "scripts/send_review_to_telegram.py", record_id],
            cwd=BASE_DIR,
            text=True,
            capture_output=True
        )

        if result.stdout:
            print(result.stdout)

        if result.stderr:
            print(result.stderr)

        if result.returncode != 0:
            print(f"Failed to send Telegram review for {record_id}")
            continue

        record["telegram_review_sent"] = True
        with record_path.open("w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)

        sent_any = True

    if not sent_any:
        print("No pending review messages were sent.")


if __name__ == "__main__":
    main()