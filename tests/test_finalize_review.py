import hashlib
import importlib.util
import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
MODULE_PATH = BASE_DIR / "scripts" / "finalize_review.py"
MODULE_SPEC = importlib.util.spec_from_file_location("finalize_review", MODULE_PATH)
finalize_review = importlib.util.module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
MODULE_SPEC.loader.exec_module(finalize_review)


def test_resolve_record_identifier_from_callback_key(tmp_path, monkeypatch):
    review_queue = tmp_path / "review_queue"
    accepted = tmp_path / "accepted"
    rejected = tmp_path / "rejected"
    review_queue.mkdir()
    accepted.mkdir()
    rejected.mkdir()

    record_id = "seed_crawl_example_abcdef1234567890"
    record_path = review_queue / f"{record_id}.json"
    record_path.write_text(json.dumps({"id": record_id}), encoding="utf-8")

    callback_key = hashlib.sha1(record_id.encode()).hexdigest()[:28]

    monkeypatch.setattr(finalize_review, "REVIEW_QUEUE_DIR", review_queue)
    monkeypatch.setattr(finalize_review, "ACCEPTED_DIR", accepted)
    monkeypatch.setattr(finalize_review, "REJECTED_DIR", rejected)

    resolved_id, resolved_path, location = finalize_review.resolve_record_identifier(
        callback_key
    )

    assert resolved_id == record_id
    assert resolved_path == record_path
    assert location == "review_queue"


def test_resolve_record_identifier_for_already_accepted_record(tmp_path, monkeypatch):
    review_queue = tmp_path / "review_queue"
    accepted = tmp_path / "accepted"
    rejected = tmp_path / "rejected"
    review_queue.mkdir()
    accepted.mkdir()
    rejected.mkdir()

    record_id = "seed_crawl_example_already_accepted"
    accepted_path = accepted / f"{record_id}.json"
    accepted_path.write_text(json.dumps({"id": record_id}), encoding="utf-8")

    monkeypatch.setattr(finalize_review, "REVIEW_QUEUE_DIR", review_queue)
    monkeypatch.setattr(finalize_review, "ACCEPTED_DIR", accepted)
    monkeypatch.setattr(finalize_review, "REJECTED_DIR", rejected)

    resolved_id, resolved_path, location = finalize_review.resolve_record_identifier(
        record_id
    )

    assert resolved_id == record_id
    assert resolved_path == accepted_path
    assert location == "accepted"
