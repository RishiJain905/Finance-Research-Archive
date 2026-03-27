import os
from pathlib import Path

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, request


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

app = Flask(__name__)

GITHUB_OWNER = os.getenv("GITHUB_OWNER")
GITHUB_REPO = os.getenv("GITHUB_REPO")
GITHUB_WORKFLOW_FILENAME = os.getenv("GITHUB_WORKFLOW_FILENAME", "finalize-review.yml")
GITHUB_PAT = os.getenv("GITHUB_PAT")
GITHUB_REF = os.getenv("GITHUB_REF", "main")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")


def trigger_github_workflow(record_id: str, decision: str) -> None:
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

    payload = {
        "ref": GITHUB_REF,
        "inputs": {
            "record_id": record_id,
            "decision": decision,
        },
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


def edit_message_after_decision(chat_id: int, message_id: int, original_text: str, decision: str) -> None:
    if not TELEGRAM_BOT_TOKEN:
        return

    decision_label = "Approved" if decision == "approve" else "Rejected"
    updated_text = f"{original_text}\n\nDecision: {decision_label}"

    edit_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    response = requests.post(
        edit_url,
        json={
            "chat_id": chat_id,
            "message_id": message_id,
            "text": updated_text,
            "reply_markup": {
                "inline_keyboard": []
            }
        },
        timeout=15,
    )

    if not response.ok:
        # Fallback: at least remove buttons if text edit fails
        markup_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageReplyMarkup"
        requests.post(
            markup_url,
            json={
                "chat_id": chat_id,
                "message_id": message_id,
                "reply_markup": {
                    "inline_keyboard": []
                }
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

    decision, record_id = callback_data.split(":", 1)

    if decision not in {"approve", "reject"}:
        answer_callback_query(callback_id, "Invalid decision")
        return jsonify({"ok": False, "message": "Invalid decision"}), 400

    try:
        trigger_github_workflow(record_id, decision)
        result_text = f"{decision.title()} sent for {record_id}"
        print(result_text)

        answer_callback_query(callback_id, result_text)

        chat_id = message.get("chat", {}).get("id")
        message_id = message.get("message_id")
        original_text = message.get("text", "")

        if chat_id and message_id and original_text:
            edit_message_after_decision(chat_id, message_id, original_text, decision)

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