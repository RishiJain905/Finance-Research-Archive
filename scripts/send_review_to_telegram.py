import hashlib
import json
import os
import sys
import time
from pathlib import Path

import requests


BASE_DIR = Path(__file__).resolve().parent.parent
REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"


def load_json_file(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def make_callback_key(record_id: str) -> str:
    """Return a short deterministic key for use in Telegram callback_data.

    Telegram enforces a 64-byte hard limit on callback_data.  The longest
    prefix we use is 'approve:' (8 bytes), leaving 56 bytes for the key.
    A 28-character SHA-1 hex digest is unique enough for this purpose and
    fits comfortably within that budget.
    """
    return hashlib.sha1(record_id.encode()).hexdigest()[:28]


TELEGRAM_MAX_MESSAGE_LENGTH = 4096


def truncate(text: str, max_len: int = TELEGRAM_MAX_MESSAGE_LENGTH) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "\u2026"


def build_message(record_id: str, record: dict) -> str:
    title = record.get("title", "Untitled")
    topic = record.get("topic", "unknown")
    event_type = record.get("event_type", "unknown")
    summary = record.get("summary", "")
    verdict = record.get("llm_review", {}).get("verdict", "unknown")
    issues = record.get("llm_review", {}).get("issues_found", [])
    review_notes = record.get("human_review", {}).get("notes", "")

    issue_text = "\n".join(f"- {issue}" for issue in issues[:5]) if issues else "- none"

    text = (
        f"Review needed for: {record_id}\n\n"
        f"Title: {title}\n"
        f"Topic: {topic}\n"
        f"Event type: {event_type}\n"
        f"LLM verdict: {verdict}\n\n"
        f"Summary:\n{summary}\n\n"
        f"Issues found:\n{issue_text}\n\n"
        f"Review notes:\n{review_notes}"
    )
    return truncate(text)


def send_telegram_message(
    token: str, chat_id: str, text: str, record_id: str, max_retries: int = 5
) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    callback_key = make_callback_key(record_id)
    payload = {
        "chat_id": chat_id,
        "text": text,
        "reply_markup": {
            "inline_keyboard": [
                [
                    {"text": "Approve", "callback_data": f"approve:{callback_key}"},
                    {"text": "Reject", "callback_data": f"reject:{callback_key}"},
                ]
            ]
        },
    }

    for attempt in range(max_retries):
        response = requests.post(url, json=payload, timeout=30)

        if response.status_code == 429:
            retry_after = int(response.json().get("parameters", {}).get("retry_after", 5))
            print(f"Rate limited by Telegram; waiting {retry_after}s before retry {attempt + 1}/{max_retries}")
            time.sleep(retry_after + 1)
            continue

        response.raise_for_status()
        return

    response.raise_for_status()


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python scripts/send_review_to_telegram.py <record_id>")

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token:
        raise EnvironmentError("TELEGRAM_BOT_TOKEN is missing.")
    if not chat_id:
        raise EnvironmentError("TELEGRAM_CHAT_ID is missing.")

    record_id = sys.argv[1]
    record_path = REVIEW_QUEUE_DIR / f"{record_id}.json"
    record = load_json_file(record_path)

    text = build_message(record_id, record)
    send_telegram_message(token, chat_id, text, record_id)

    print(f"Sent Telegram review message for: {record_id}")


if __name__ == "__main__":
    main()
