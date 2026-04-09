"""Telegram polling bot for inbound content ingestion.

Polls a private Telegram bot for new messages and queues them for processing:
- URL messages are added to data/inbox_queue.json for the article pipeline
- Plain text messages are saved to data/inbox/ as telegram_*.txt files
  (processed by the file drop pipeline)
"""

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

BOT_TOKEN = os.getenv("TELEGRAM_INGEST_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_INGEST_CHAT_ID", "")

BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
QUEUE_PATH = BASE_DIR / "data" / "inbox_queue.json"
INBOX_DIR = BASE_DIR / "data" / "inbox"


def is_url(text: str) -> bool:
    if not text:
        return False
    text = text.strip()
    if not text.startswith(("http://", "https://")):
        return False
    try:
        result = urlparse(text)
        return bool(result.scheme and result.netloc)
    except Exception:
        return False


def load_queue() -> list[dict]:
    if not QUEUE_PATH.exists():
        return []
    try:
        return json.loads(QUEUE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


def save_queue(queue: list[dict]) -> None:
    QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
    QUEUE_PATH.write_text(
        json.dumps(queue, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def add_url_to_queue(url: str) -> None:
    queue = load_queue()
    for item in queue:
        if item.get("url") == url:
            return
    queue.append(
        {
            "url": url,
            "added_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    save_queue(queue)


def save_text_to_inbox(text: str, sender: str = "unknown") -> str:
    INBOX_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    hash_suffix = hashlib.sha1(f"{text}{timestamp}".encode("utf-8")).hexdigest()[:8]
    filename = f"telegram_{timestamp}_{hash_suffix}.txt"
    filepath = INBOX_DIR / filename
    content = (
        f"SENDER: {sender}\nDATE: {datetime.now(timezone.utc).isoformat()}\n\n{text}"
    )
    filepath.write_text(content, encoding="utf-8")
    return filename


def fetch_updates(offset: int = 0, timeout: int = 30) -> list[dict]:
    url = f"{BASE_URL}/getUpdates"
    params = {"offset": offset, "timeout": timeout}
    response = requests.get(url, params=params, timeout=timeout + 10)
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")
    return data.get("result", [])


def acknowledge_updates(offset: int) -> None:
    url = f"{BASE_URL}/getUpdates"
    params = {"offset": offset + 1}
    requests.get(url, params=params, timeout=10)


def process_update(update: dict) -> bool:
    if "message" not in update:
        return False

    message = update["message"]
    chat = message.get("chat", {})
    text = message.get("text", "").strip()

    if str(chat.get("id")) != str(CHAT_ID):
        return False

    if not text:
        return False

    if is_url(text):
        add_url_to_queue(text)
        print(f"  Queued URL: {text}")
        return True
    else:
        sender = message.get("from", {}).get("username", "unknown")
        filename = save_text_to_inbox(text, sender)
        print(f"  Saved text to inbox: {filename}")
        return True


def main() -> int:
    if not BOT_TOKEN:
        raise EnvironmentError(
            "TELEGRAM_INGEST_BOT_TOKEN environment variable is not set"
        )
    if not CHAT_ID:
        raise EnvironmentError(
            "TELEGRAM_INGEST_CHAT_ID environment variable is not set"
        )

    updates = fetch_updates(offset=0)
    if not updates:
        print("No new updates.")
        return 0

    print(f"Processing {len(updates)} update(s)...")
    processed_count = 0
    last_update_id = 0

    for update in updates:
        update_id = update.get("update_id", 0)
        last_update_id = max(last_update_id, update_id)
        if process_update(update):
            processed_count += 1

    if last_update_id > 0:
        acknowledge_updates(last_update_id)

    print(f"Processed {processed_count} message(s).")
    return processed_count


if __name__ == "__main__":
    main()
