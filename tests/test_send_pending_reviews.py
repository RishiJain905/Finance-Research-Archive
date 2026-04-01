import importlib.util
import json
import os
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
MODULE_PATH = BASE_DIR / "scripts" / "send_pending_reviews.py"
MODULE_SPEC = importlib.util.spec_from_file_location("send_pending_reviews", MODULE_PATH)
send_pending_reviews = importlib.util.module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
MODULE_SPEC.loader.exec_module(send_pending_reviews)


def _write_record(path: Path, required: bool = True) -> None:
    payload = {
        "status": "review_queue",
        "title": path.stem,
        "summary": "summary",
        "llm_review": {"issues_found": [], "verdict": "review"},
        "human_review": {"required": required, "notes": ""},
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_send_pending_reviews_honors_max_items(tmp_path, monkeypatch):
    queue_dir = tmp_path / "review_queue"
    queue_dir.mkdir()

    old_record = queue_dir / "old_record.json"
    new_record = queue_dir / "new_record.json"
    newest_record = queue_dir / "newest_record.json"

    _write_record(old_record)
    _write_record(new_record)
    _write_record(newest_record)

    os.utime(old_record, (1000, 1000))
    os.utime(new_record, (2000, 2000))
    os.utime(newest_record, (3000, 3000))

    monkeypatch.setattr(send_pending_reviews, "REVIEW_QUEUE_DIR", queue_dir)
    monkeypatch.setattr(
        send_pending_reviews,
        "REVIEW_BUDGET_STATE_PATH",
        tmp_path / "review_budget_state.json",
    )
    monkeypatch.setattr(
        send_pending_reviews, "send_review_with_retry", lambda *_args, **_kwargs: True
    )
    monkeypatch.setattr(send_pending_reviews.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["send_pending_reviews.py", "--max-items", "2"],
    )

    send_pending_reviews.main()

    old_data = json.loads(old_record.read_text(encoding="utf-8"))
    new_data = json.loads(new_record.read_text(encoding="utf-8"))
    newest_data = json.loads(newest_record.read_text(encoding="utf-8"))

    assert old_data.get("telegram_review_sent") is not True
    assert new_data.get("telegram_review_sent") is True
    assert newest_data.get("telegram_review_sent") is True


def test_send_pending_reviews_can_target_specific_ids(tmp_path, monkeypatch):
    queue_dir = tmp_path / "review_queue"
    queue_dir.mkdir()

    keep_record = queue_dir / "keep_me.json"
    skip_record = queue_dir / "skip_me.json"
    _write_record(keep_record)
    _write_record(skip_record)

    monkeypatch.setattr(send_pending_reviews, "REVIEW_QUEUE_DIR", queue_dir)
    monkeypatch.setattr(
        send_pending_reviews,
        "REVIEW_BUDGET_STATE_PATH",
        tmp_path / "review_budget_state.json",
    )
    monkeypatch.setattr(
        send_pending_reviews, "send_review_with_retry", lambda *_args, **_kwargs: True
    )
    monkeypatch.setattr(send_pending_reviews.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["send_pending_reviews.py", "--record-id", "keep_me", "--max-items", "5"],
    )

    send_pending_reviews.main()

    keep_data = json.loads(keep_record.read_text(encoding="utf-8"))
    skip_data = json.loads(skip_record.read_text(encoding="utf-8"))

    assert keep_data.get("telegram_review_sent") is True
    assert skip_data.get("telegram_review_sent") is not True


def test_send_pending_reviews_honors_daily_budget(tmp_path, monkeypatch):
    queue_dir = tmp_path / "review_queue"
    queue_dir.mkdir()
    r1 = queue_dir / "r1.json"
    r2 = queue_dir / "r2.json"
    _write_record(r1)
    _write_record(r2)

    budget_path = tmp_path / "review_budget_state.json"
    monkeypatch.setattr(send_pending_reviews, "REVIEW_QUEUE_DIR", queue_dir)
    monkeypatch.setattr(send_pending_reviews, "REVIEW_BUDGET_STATE_PATH", budget_path)
    monkeypatch.setattr(
        send_pending_reviews, "send_review_with_retry", lambda *_args, **_kwargs: True
    )
    monkeypatch.setattr(send_pending_reviews.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        sys, "argv", ["send_pending_reviews.py", "--max-items", "5", "--daily-budget", "1"]
    )

    send_pending_reviews.main()

    d1 = json.loads(r1.read_text(encoding="utf-8"))
    d2 = json.loads(r2.read_text(encoding="utf-8"))
    sent_count = int(d1.get("telegram_review_sent") is True) + int(
        d2.get("telegram_review_sent") is True
    )
    assert sent_count == 1
