import hashlib
import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, request


BASE_DIR = Path(__file__).resolve().parent.parent
REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"
load_dotenv(BASE_DIR / ".env")

app = Flask(__name__)

GITHUB_OWNER = os.getenv("GITHUB_OWNER")
GITHUB_REPO = os.getenv("GITHUB_REPO")
GITHUB_WORKFLOW_FILENAME = os.getenv("GITHUB_WORKFLOW_FILENAME", "finalize-review.yml")
GITHUB_PAT = os.getenv("GITHUB_PAT")
GITHUB_REF = os.getenv("GITHUB_REF", "main")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")


def _make_callback_key(record_id: str) -> str:
    """Must stay in sync with send_review_to_telegram.make_callback_key."""
    return hashlib.sha1(record_id.encode()).hexdigest()[:28]


def resolve_record_id(callback_key: str) -> str | None:
    """Return the full record_id whose SHA-1 key matches *callback_key*.

    Scans the review_queue directory and recomputes the key for each filename
    stem until a match is found.  Returns None if no match exists.
    """
    if not REVIEW_QUEUE_DIR.exists():
        return None
    for record_path in REVIEW_QUEUE_DIR.glob("*.json"):
        if record_path.name.endswith("_verification.json"):
            continue
        if _make_callback_key(record_path.stem) == callback_key:
            return record_path.stem
    return None


def normalize_decision_for_workflow(decision: str) -> str:
    """Map Telegram button actions to finalize_review decisions."""
    decision_map = {
        "promote": "approve_and_promote",
        "weak": "approve_but_weak",
    }
    return decision_map.get(decision, decision)


def trigger_github_workflow(
    record_id: str,
    decision: str,
    source_feedback: str = "",
    topic_feedback: str = "",
    notes: str = "",
) -> None:
    if not GITHUB_OWNER:
        raise EnvironmentError("GITHUB_OWNER is missing.")
    if not GITHUB_REPO:
        raise EnvironmentError("GITHUB_REPO is missing.")
    if not GITHUB_PAT:
        raise EnvironmentError("GITHUB_PAT is missing.")

    url = (
        f"https://api.github.com/repos/"
        f"{GITHUB_OWNER}/{GITHUB_REPO}/actions/workflows/"
        f"{GITHUB_WORKFLOW_FILENAME}/dispatches"
    )

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_PAT}",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    inputs: dict[str, str] = {
        "record_id": record_id,
        "decision": decision,
    }
    if source_feedback:
        inputs["source_feedback"] = source_feedback
    if topic_feedback:
        inputs["topic_feedback"] = topic_feedback
    if notes:
        inputs["notes"] = notes

    payload = {
        "ref": GITHUB_REF,
        "inputs": inputs,
    }

    response = requests.post(url, headers=headers, json=payload, timeout=30)
    if not response.ok:
        raise RuntimeError(
            f"GitHub workflow dispatch failed: {response.status_code} {response.text}"
        )


def answer_callback_query(callback_id: str, text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not callback_id:
        return

    answer_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    requests.post(
        answer_url,
        json={
            "callback_query_id": callback_id,
            "text": text,
            "show_alert": False,
        },
        timeout=15,
    )


_TELEGRAM_MAX_LEN = 4096


def _truncate(text: str, max_len: int = _TELEGRAM_MAX_LEN) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "\u2026"


def load_record(record_id: str) -> dict | None:
    """Load a record from the review queue, or None if not found."""
    if not REVIEW_QUEUE_DIR.exists():
        return None
    record_path = REVIEW_QUEUE_DIR / f"{record_id}.json"
    if not record_path.exists():
        return None
    with record_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_record(record_id: str, record: dict) -> None:
    """Save a record back to the review queue."""
    REVIEW_QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    record_path = REVIEW_QUEUE_DIR / f"{record_id}.json"
    with record_path.open("w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)


def send_followup_prompt(
    chat_id: int, message_id: int, original_text: str, source_feedback: str
) -> None:
    """Send a follow-up message after source feedback, asking for final decision."""
    if not TELEGRAM_BOT_TOKEN:
        return

    # Edit the original message to note the source feedback
    feedback_label = "Good source" if source_feedback == "good_source" else "Bad source"
    updated_text = _truncate(
        f"{original_text}\n\nSource feedback: {feedback_label}\n\nNow approve or reject?"
    )

    edit_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"

    # Rebuild callback_key from the original_text to reconstruct the keyboard
    # We reuse the same record_id pattern; for the follow-up we only show Approve/Reject
    # We need the callback_key to reconstruct - but we don't have it directly.
    # Instead, we send a NEW message with the follow-up prompt rather than editing.
    # This is cleaner since we don't need to reconstruct the original callback_key.
    send_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    # We reference the original message by quoting it, but simpler: just edit with no buttons
    # then send a fresh message.
    requests.post(
        edit_url,
        json={
            "chat_id": chat_id,
            "message_id": message_id,
            "text": updated_text,
            "reply_markup": {"inline_keyboard": []},
        },
        timeout=15,
    )

    # Send a fresh follow-up message with Approve/Reject
    # Extract record_id from original_text if possible, otherwise use a placeholder
    # The original_text starts with "Review needed for: {record_id}"
    record_id = None
    if "Review needed for:" in original_text:
        record_id = original_text.split("Review needed for:")[1].split("\n")[0].strip()

    if record_id:
        callback_key = _make_callback_key(record_id)
        keyboard = {
            "inline_keyboard": [
                [
                    {
                        "text": "\u2705 Approve",
                        "callback_data": f"approve:{callback_key}",
                    },
                    {
                        "text": "\u274c Reject",
                        "callback_data": f"reject:{callback_key}",
                    },
                ]
            ]
        }
    else:
        keyboard = {"inline_keyboard": []}

    requests.post(
        send_url,
        json={
            "chat_id": chat_id,
            "text": f"Source marked as {feedback_label}. Now approve or reject the record?",
            "reply_markup": keyboard,
        },
        timeout=15,
    )


def edit_message_after_decision(
    chat_id: int, message_id: int, original_text: str, decision: str
) -> None:
    if not TELEGRAM_BOT_TOKEN:
        return

    decision_label = "Approved" if decision == "approve" else "Rejected"
    updated_text = _truncate(f"{original_text}\n\nDecision: {decision_label}")

    edit_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    response = requests.post(
        edit_url,
        json={
            "chat_id": chat_id,
            "message_id": message_id,
            "text": updated_text,
            "reply_markup": {"inline_keyboard": []},
        },
        timeout=15,
    )

    if not response.ok:
        # Fallback: at least remove buttons if text edit fails
        markup_url = (
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageReplyMarkup"
        )
        requests.post(
            markup_url,
            json={
                "chat_id": chat_id,
                "message_id": message_id,
                "reply_markup": {"inline_keyboard": []},
            },
            timeout=15,
        )


@app.route("/telegram/webhook", methods=["POST"])
def telegram_webhook():
    update = request.get_json(silent=True) or {}
    print("Incoming Telegram update:", update)

    callback_query = update.get("callback_query")
    if not callback_query:
        return jsonify({"ok": True, "message": "No callback_query received"}), 200

    callback_data = callback_query.get("data", "")
    callback_id = callback_query.get("id")
    message = callback_query.get("message", {})

    if ":" not in callback_data:
        answer_callback_query(callback_id, "Invalid callback data")
        return jsonify({"ok": False, "message": "Invalid callback data"}), 400

    decision, callback_key = callback_data.split(":", 1)

    valid_decisions = {
        "approve",
        "reject",
        "promote",
        "weak",
        "good_source",
        "bad_source",
    }
    if decision not in valid_decisions:
        answer_callback_query(callback_id, "Invalid decision")
        return jsonify({"ok": False, "message": "Invalid decision"}), 400

    record_id = resolve_record_id(callback_key)
    if record_id is None:
        answer_callback_query(
            callback_id,
            "Record not found in queue (already finalized or from old branch).",
        )
        return (
            jsonify(
                {
                    "ok": True,
                    "message": f"Stale callback key {callback_key!r}; no matching review_queue record.",
                }
            ),
            200,
        )

    result_text = f"Processing {decision} for {record_id}"
    try:
        chat_id = message.get("chat", {}).get("id")
        message_id = message.get("message_id")
        original_text = message.get("text", "")

        if decision in {"good_source", "bad_source"}:
            # Source feedback — store temporarily and ask for final decision
            record = load_record(record_id)
            if record is None:
                answer_callback_query(callback_id, "Record not found")
                return jsonify({"ok": False, "message": "Record not found"}), 404

            # Store pending source feedback
            record["pending_source_feedback"] = decision
            save_record(record_id, record)

            result_text = f"Source feedback recorded. Waiting for final decision."
            answer_callback_query(callback_id, result_text)

            if chat_id and message_id and original_text:
                send_followup_prompt(chat_id, message_id, original_text, decision)

        else:
            # Final decision path (approve/reject/promote/weak).
            # Attach any pending source feedback collected from a prior button click.
            record = load_record(record_id)
            source_feedback = ""
            if record:
                source_feedback = record.pop(
                    "pending_source_feedback",
                    "",
                )
                if source_feedback:
                    save_record(record_id, record)

            trigger_github_workflow(
                record_id,
                normalize_decision_for_workflow(decision),
                source_feedback=source_feedback,
            )
            result_text = f"{decision.title()} sent for {record_id}"
            print(result_text)

            answer_callback_query(callback_id, result_text)

            if chat_id and message_id and original_text:
                edit_message_after_decision(
                    chat_id, message_id, original_text, decision
                )

    except Exception as e:
        result_text = f"Failed for {record_id}: {e}"
        print(result_text)
        answer_callback_query(callback_id, result_text)

    return jsonify({"ok": True, "message": result_text}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True}), 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
