"""Tests for the Telegram ingestion bot (scripts/telegram_ingest_bot.py)."""

import importlib.util
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

BASE_DIR = Path(__file__).resolve().parent.parent
MODULE_PATH = BASE_DIR / "scripts" / "telegram_ingest_bot.py"
MODULE_SPEC = importlib.util.spec_from_file_location("telegram_ingest_bot", MODULE_PATH)
assert MODULE_SPEC and MODULE_SPEC.loader
telegram_bot = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(telegram_bot)


QUEUE_PATH = BASE_DIR / "data" / "inbox_queue.json"
INBOX_DIR = BASE_DIR / "data" / "inbox"


class TestUrlDetection:
    def test_is_url_with_http(self):
        assert telegram_bot.is_url("http://example.com/article") is True

    def test_is_url_with_https(self):
        assert telegram_bot.is_url("https://example.com/article") is True

    def test_is_url_with_www(self):
        assert telegram_bot.is_url("https://www.example.com/page") is True

    def test_is_url_rejects_plain_text(self):
        assert telegram_bot.is_url("Just some plain text") is False
        assert telegram_bot.is_url("Hello world") is False

    def test_is_url_rejects_partial_urls(self):
        assert telegram_bot.is_url("example.com") is False
        assert telegram_bot.is_url("www.example.com") is False


class TestQueueManagement:
    def test_load_queue_empty(self, tmp_path):
        queue_file = tmp_path / "queue.json"
        with patch.object(telegram_bot, "QUEUE_PATH", queue_file):
            queue = telegram_bot.load_queue()
            assert queue == []

    def test_load_queue_with_items(self, tmp_path):
        queue_file = tmp_path / "queue.json"
        queue_file.write_text(
            json.dumps(
                [{"url": "https://example.com", "added_at": "2026-04-09T12:00:00Z"}]
            )
        )
        with patch.object(telegram_bot, "QUEUE_PATH", queue_file):
            queue = telegram_bot.load_queue()
            assert len(queue) == 1
            assert queue[0]["url"] == "https://example.com"

    def test_save_queue(self, tmp_path):
        queue_file = tmp_path / "queue.json"
        items = [{"url": "https://example.com", "added_at": "2026-04-09T12:00:00Z"}]
        with patch.object(telegram_bot, "QUEUE_PATH", queue_file):
            telegram_bot.save_queue(items)
        loaded = json.loads(queue_file.read_text())
        assert len(loaded) == 1
        assert loaded[0]["url"] == "https://example.com"

    def test_add_url_to_queue(self, tmp_path):
        queue_file = tmp_path / "queue.json"
        with patch.object(telegram_bot, "QUEUE_PATH", queue_file):
            telegram_bot.add_url_to_queue("https://example.com/article")
            queue = telegram_bot.load_queue()
            assert len(queue) == 1
            assert queue[0]["url"] == "https://example.com/article"
            assert "added_at" in queue[0]

    def test_add_url_avoids_duplicates(self, tmp_path):
        queue_file = tmp_path / "queue.json"
        queue_file.write_text(
            json.dumps(
                [
                    {
                        "url": "https://example.com/article",
                        "added_at": "2026-04-09T12:00:00Z",
                    }
                ]
            )
        )
        with patch.object(telegram_bot, "QUEUE_PATH", queue_file):
            telegram_bot.add_url_to_queue("https://example.com/article")
            queue = telegram_bot.load_queue()
            assert len(queue) == 1


class TestSaveTelegramText:
    def test_save_text_to_inbox(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir(parents=True)
        with patch.object(telegram_bot, "INBOX_DIR", inbox):
            result = telegram_bot.save_text_to_inbox(
                "Test message content", "test_user"
            )
            assert result is not None
            assert result.endswith(".txt")
            content = (inbox / result).read_text()
            assert "Test message content" in content
            assert "test_user" in content


class TestProcessUpdate:
    def test_process_url_message(self, tmp_path):
        queue_file = tmp_path / "queue.json"
        with patch.object(telegram_bot, "QUEUE_PATH", queue_file):
            with patch.object(telegram_bot, "CHAT_ID", "12345"):
                update = {
                    "message_id": 1,
                    "message": {
                        "chat": {"id": 12345},
                        "text": "https://example.com/finance/article",
                    },
                }
                result = telegram_bot.process_update(update)
                assert result is True
                queue = telegram_bot.load_queue()
                assert len(queue) == 1
                assert queue[0]["url"] == "https://example.com/finance/article"

    def test_process_text_message_saves_to_inbox(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir(parents=True)
        queue_file = tmp_path / "queue.json"
        with patch.object(telegram_bot, "QUEUE_PATH", queue_file):
            with patch.object(telegram_bot, "INBOX_DIR", inbox):
                with patch.object(telegram_bot, "CHAT_ID", "12345"):
                    update = {
                        "message_id": 2,
                        "message": {
                            "chat": {"id": 12345},
                            "text": "This is a note I want to save",
                        },
                    }
                    result = telegram_bot.process_update(update)
                    assert result is True
                    queue = telegram_bot.load_queue()
                    assert len(queue) == 0
                    txt_files = list(inbox.glob("telegram_*.txt"))
                    assert len(txt_files) == 1

    def test_process_ignores_other_chats(self, tmp_path):
        queue_file = tmp_path / "queue.json"
        with patch.object(telegram_bot, "QUEUE_PATH", queue_file):
            with patch.object(telegram_bot, "CHAT_ID", "12345"):
                update = {
                    "message_id": 1,
                    "message": {
                        "chat": {"id": 99999},
                        "text": "https://example.com/article",
                    },
                }
                result = telegram_bot.process_update(update)
                assert result is False
                queue = telegram_bot.load_queue()
                assert len(queue) == 0

    def test_process_ignores_empty_text(self, tmp_path):
        queue_file = tmp_path / "queue.json"
        with patch.object(telegram_bot, "QUEUE_PATH", queue_file):
            with patch.object(telegram_bot, "CHAT_ID", "12345"):
                update = {
                    "message_id": 1,
                    "message": {
                        "chat": {"id": 12345},
                        "text": "",
                    },
                }
                result = telegram_bot.process_update(update)
                assert result is False

    def test_process_ignores_update_without_message(self, tmp_path):
        queue_file = tmp_path / "queue.json"
        with patch.object(telegram_bot, "QUEUE_PATH", queue_file):
            with patch.object(telegram_bot, "CHAT_ID", "12345"):
                update = {"update_id": 1}
                result = telegram_bot.process_update(update)
                assert result is False


class TestFetchUpdates:
    def test_fetch_updates_returns_messages(self):
        mock_response = {
            "ok": True,
            "result": [
                {
                    "update_id": 1,
                    "message": {"message_id": 1, "chat": {"id": 12345}, "text": "test"},
                }
            ],
        }
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response
            with patch.object(
                telegram_bot, "BASE_URL", "https://api.telegram.org/botTOKEN"
            ):
                updates = telegram_bot.fetch_updates(offset=0)
                assert len(updates) == 1
                assert updates[0]["update_id"] == 1

    def test_fetch_updates_empty_result(self):
        mock_response = {"ok": True, "result": []}
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response
            with patch.object(
                telegram_bot, "BASE_URL", "https://api.telegram.org/botTOKEN"
            ):
                updates = telegram_bot.fetch_updates(offset=0)
                assert len(updates) == 0


class TestAcknowledgeUpdates:
    def test_acknowledge_updates_calls_with_offset(self):
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = {"ok": True, "result": []}
            with patch.object(
                telegram_bot, "BASE_URL", "https://api.telegram.org/botTOKEN"
            ):
                telegram_bot.acknowledge_updates(offset=100)
                mock_get.assert_called_once()
                call_args = mock_get.call_args
                assert call_args.kwargs.get("params", {}).get("offset") == 101


class TestMainBot:
    def test_main_processes_pending_updates(self, tmp_path):
        queue_file = tmp_path / "queue.json"
        inbox = tmp_path / "inbox"
        inbox.mkdir(parents=True)

        mock_updates = {
            "ok": True,
            "result": [
                {
                    "update_id": 5,
                    "message": {
                        "message_id": 10,
                        "chat": {"id": 12345},
                        "text": "https://example.com/important/article",
                    },
                }
            ],
        }

        with patch.object(telegram_bot, "BOT_TOKEN", "test_token"):
            with patch.object(telegram_bot, "CHAT_ID", "12345"):
                with patch.object(telegram_bot, "QUEUE_PATH", queue_file):
                    with patch.object(telegram_bot, "INBOX_DIR", inbox):
                        with patch.object(
                            telegram_bot,
                            "fetch_updates",
                            return_value=mock_updates["result"],
                        ):
                            with patch.object(telegram_bot, "acknowledge_updates"):
                                telegram_bot.main()
                            queue = telegram_bot.load_queue()
                            assert len(queue) == 1
                            assert (
                                queue[0]["url"]
                                == "https://example.com/important/article"
                            )

    def test_main_saves_text_to_inbox(self, tmp_path):
        queue_file = tmp_path / "queue.json"
        inbox = tmp_path / "inbox"
        inbox.mkdir(parents=True)

        mock_updates = {
            "ok": True,
            "result": [
                {
                    "update_id": 7,
                    "message": {
                        "message_id": 12,
                        "chat": {"id": 12345},
                        "text": "Remember to review the Fed minutes",
                    },
                }
            ],
        }

        with patch.object(telegram_bot, "BOT_TOKEN", "test_token"):
            with patch.object(telegram_bot, "CHAT_ID", "12345"):
                with patch.object(telegram_bot, "QUEUE_PATH", queue_file):
                    with patch.object(telegram_bot, "INBOX_DIR", inbox):
                        with patch.object(
                            telegram_bot,
                            "fetch_updates",
                            return_value=mock_updates["result"],
                        ):
                            with patch.object(telegram_bot, "acknowledge_updates"):
                                telegram_bot.main()

        queue = telegram_bot.load_queue()
        assert len(queue) == 0
        txt_files = list(inbox.glob("telegram_*.txt"))
        assert len(txt_files) == 1
        content = txt_files[0].read_text()
        assert "Fed minutes" in content
