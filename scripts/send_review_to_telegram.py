import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"


def load_json_file(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_message(record_id: str, record: dict) -> str:
    title = record.get("title", "Untitled")
    topic = record.get("topic", "unknown")
    event_type = record.get("event_type", "unknown")
    summary = record.get("summary", "")
    verdict = record.get("llm_review", {}).get("verdict", "unknown")
    issues = record.get("llm_review", {}).get("issues_found", [])
    review_notes = record.get("human_review", {}).get("notes", "")

    issue_text = "\n".join(f"- {issue}" for issue in issues[:5]) if issues else "- none"

    return (
        f"Review needed for: {record_id}\n\n"
        f"Title: {title}\n"
        f"Topic: {topic}\n"
        f"Event type: {event_type}\n"
        f"LLM verdict: {verdict}\n\n"
        f"Summary:\n{summary}\n\n"
        f"Issues found:\n{issue_text}\n\n"
        f"Review notes:\n{review_notes}"
    )


def send_telegram_message(token: str, chat_id: str, text: str, record_id: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": text,
        "reply_markup": {
            "inline_keyboard": [
                [
                    {"text": "Approve", "callback_data": f"approve:{record_id}"},
                    {"text": "Reject", "callback_data": f"reject:{record_id}"}
                ]
            ]
        }
    }

    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python scripts/send_review_to_telegram.py <record_id>")

    load_dotenv(BASE_DIR / ".env")

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