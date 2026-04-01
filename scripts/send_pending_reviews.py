import json
import subprocess
import sys
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path
import hashlib


BASE_DIR = Path(__file__).resolve().parent.parent
REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"
REVIEW_BUDGET_STATE_PATH = BASE_DIR / "data" / "candidate_manifests" / "review_budget_state.json"


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


def load_budget_state() -> dict:
    if not REVIEW_BUDGET_STATE_PATH.exists():
        return {}
    try:
        return json.loads(REVIEW_BUDGET_STATE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_budget_state(state: dict) -> None:
    REVIEW_BUDGET_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    REVIEW_BUDGET_STATE_PATH.write_text(
        json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def record_priority(record: dict, record_path: Path) -> tuple[float, float]:
    """Higher tuple values are prioritized first."""
    llm_review = record.get("llm_review", {})
    confidence = float(llm_review.get("verification_confidence", 0))
    score = float(record.get("candidate_scores", {}).get("total_score", 0))
    # Confidence first, score second, newest file time as tie-breaker.
    return confidence + (score / 1000.0), float(record_path.stat().st_mtime)


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
    parser = argparse.ArgumentParser(
        description="Send pending human-review records to Telegram with queue guards."
    )
    parser.add_argument(
        "--record-id",
        action="append",
        default=[],
        help="Only consider specific record IDs (repeatable)",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=8,
        help="Maximum pending review messages to send in one run",
    )
    parser.add_argument(
        "--daily-budget",
        type=int,
        default=20,
        help="Maximum new Telegram review sends allowed per UTC day (0 means unlimited)",
    )
    args = parser.parse_args()

    python_cmd = sys.executable
    sent_any = False
    sent_count = 0
    requested_ids = set(args.record_id or [])

    if not REVIEW_QUEUE_DIR.exists():
        print("Review queue directory does not exist.")
        return

    candidate_records: list[tuple[Path, dict]] = []
    for p in REVIEW_QUEUE_DIR.glob("*.json"):
        if p.name.endswith("_verification.json"):
            continue
        rid = p.stem
        if requested_ids and rid not in requested_ids:
            continue
        try:
            rec = load_json_file(p)
        except Exception as e:
            print(f"Skipping unreadable file {p.name}: {e}")
            continue
        candidate_records.append((p, rec))

    candidate_records.sort(key=lambda item: record_priority(item[1], item[0]), reverse=True)

    state = load_budget_state()
    today_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    sent_today = int(state.get(today_key, 0))
    remaining_daily_budget = (
        max(args.daily_budget - sent_today, 0) if args.daily_budget > 0 else None
    )

    skipped_for_cap = 0
    skipped_for_budget = 0

    for record_path, record in candidate_records:
        record_id = record_path.stem

        if sent_count >= args.max_items:
            skipped_for_cap += 1
            continue

        if remaining_daily_budget is not None and remaining_daily_budget <= 0:
            skipped_for_budget += 1
            continue

        if not should_send_review(record):
            continue

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
        sent_count += 1
        if remaining_daily_budget is not None:
            remaining_daily_budget -= 1
            sent_today += 1
        time.sleep(1.5)

    if skipped_for_cap:
        print(
            f"Skipped {skipped_for_cap} additional pending review(s) due to --max-items={args.max_items}."
        )
    if skipped_for_budget:
        print(
            f"Held {skipped_for_budget} pending review(s) due to daily budget --daily-budget={args.daily_budget}."
        )

    if args.daily_budget > 0:
        state[today_key] = sent_today
        save_budget_state(state)
        print(f"Daily review budget usage (UTC): {sent_today}/{args.daily_budget}")

    if not sent_any:
        print("No pending review messages were sent.")


if __name__ == "__main__":
    main()
