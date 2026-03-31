import json
import subprocess
import sys
import time
from pathlib import Path
import hashlib


BASE_DIR = Path(__file__).resolve().parent.parent
REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"


def build_review_fingerprint(record: dict) -> str:
    relevant = {
        "title": record.get("title", ""),
        "summary": record.get("summary", ""),
        "issues_found": record.get("llm_review", {}).get("issues_found", []),
        "verdict": record.get("llm_review", {}).get("verdict", ""),
        "notes": record.get("human_review", {}).get("notes", ""),
    }
    raw = json.dumps(relevant, sort_keys=True)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def load_json_file(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json_file(path: Path, data: dict) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def should_send_review(record: dict) -> bool:
    human_review = record.get("human_review", {})
    status = record.get("status", "")
    return human_review.get("required", False) and status == "review_queue"


def send_review_with_retry(
    python_cmd: str, record_id: str, base_dir: Path, max_retries: int = 3
) -> bool:
    for attempt in range(max_retries):
        result = subprocess.run(
            [python_cmd, "scripts/send_review_to_telegram.py", record_id],
            cwd=base_dir,
            text=True,
            capture_output=True,
        )

        if result.returncode == 0:
            return True

        if result.returncode != 0 and attempt < max_retries - 1:
            wait_time = (attempt + 1) * 2
            print(
                f"Retry {attempt + 2}/{max_retries} for {record_id} after {wait_time}s. Error: {result.stderr}"
            )
            time.sleep(wait_time)

    return False


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

        record_id = record_path.stem
        current_review_fp = build_review_fingerprint(record)
        last_review_fp = record.get("telegram_review_fingerprint", "")

        if (
            record.get("telegram_review_sent", False)
            and last_review_fp == current_review_fp
        ):
            print(f"Already sent to Telegram with same review content: {record_id}")
            continue

        print(f"Sending review to Telegram: {record_id}")

        success = send_review_with_retry(python_cmd, record_id, BASE_DIR)

        if not success:
            print(f"Failed to send Telegram review for {record_id} after retries")
            continue

        record["telegram_review_sent"] = True
        record["telegram_review_fingerprint"] = current_review_fp
        save_json_file(record_path, record)

        sent_any = True
        time.sleep(1.5)

    if not sent_any:
        print("No pending review messages were sent.")


if __name__ == "__main__":
    main()
