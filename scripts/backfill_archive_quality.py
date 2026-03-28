import argparse
import json
import shutil
import sys
from copy import deepcopy
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.filter_raw_records import (
    detect_quant_record,
    evaluate_article_record,
    evaluate_quant_record,
    parse_raw_record,
)
from scripts.ingest_sources import classify_page_type, detect_language
from scripts.run_verifier import apply_archive_quality_gate

ACCEPTED_DIR = BASE_DIR / "data" / "accepted"
REJECTED_DIR = BASE_DIR / "data" / "rejected"
REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"
INGESTION_MANIFEST_PATH = BASE_DIR / "data" / "ingestion_manifest.json"


def load_json_file(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json_file(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def move_file_if_exists(source: Path, destination: Path) -> None:
    if source.exists():
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(destination))


def parse_reason_text(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def classify_missing_raw_record(record: dict) -> tuple[str, list[str]]:
    record_id = record.get("id", "").strip().lower()
    source = record.get("source", {})
    source_type = source.get("source_type", "").strip().lower()
    source_url = source.get("url", "").strip()
    lower_text = " ".join([record.get("summary", ""), record.get("notes", "")]).lower()

    if source_type == "website_navigation" or "navigation page" in lower_text:
        return "rejected", ["missing_raw_navigation_record"]

    if record_id.startswith("sample_") or "_test" in record_id:
        return "rejected", ["missing_raw_sample_record"]

    if source_type in {"placeholder", "dataset_snapshot"} or "placeholder" in lower_text:
        return "rejected", ["missing_raw_placeholder_record"]

    if source_type not in {"placeholder", "dataset_snapshot"} and not source_url:
        return "rejected", ["missing_raw_source_attribution"]

    return "review_queue", ["missing_raw_source"]


def evaluate_record_for_backfill(
    record: dict,
    verification: dict,
    raw_text: str,
    rules: dict,
) -> tuple[str, list[str]]:
    if detect_quant_record(raw_text):
        keep, reasons = evaluate_quant_record(raw_text)
        if not keep:
            return "rejected", reasons
        return "accepted", []

    parsed = parse_raw_record(raw_text)
    metadata = enrich_article_metadata(record, parsed.get("metadata", {}), parsed.get("body") or raw_text, rules)
    body_text = parsed.get("body") or raw_text

    keep, reasons = evaluate_article_record(body_text, rules, metadata)
    if not keep:
        return "rejected", reasons

    gated_record = apply_archive_quality_gate(deepcopy(record), verification, metadata, rules)
    if gated_record["status"] == "accepted":
        return "accepted", []

    return gated_record["status"], parse_reason_text(gated_record.get("human_review", {}).get("notes", ""))


def enrich_article_metadata(record: dict, metadata: dict, body_text: str, rules: dict) -> dict:
    enriched = dict(metadata)

    source = record.get("source", {})
    article_url = enriched.get("ARTICLE_URL") or enriched.get("URL") or source.get("url", "")
    title = enriched.get("TITLE") or record.get("title", "") or source.get("name", "")
    published_at = enriched.get("PUBLISHED_AT") or source.get("published_at", "")

    if article_url:
        enriched.setdefault("URL", article_url)
    if published_at:
        enriched.setdefault("PUBLISHED_AT", published_at)
    if rules.get("expected_language"):
        enriched.setdefault("EXPECTED_LANGUAGE", rules.get("expected_language"))
    if body_text:
        enriched.setdefault("DETECTED_LANGUAGE", detect_language(body_text))
    if article_url and title and "PAGE_TYPE" not in enriched:
        enriched["PAGE_TYPE"] = classify_page_type(article_url, title, body_text, published_at)

    return enriched


def verification_payload_for(record: dict, verification_path: Path) -> dict:
    if verification_path.exists():
        return load_json_file(verification_path)

    llm_review = record.get("llm_review", {})
    return {
        "verification_confidence": llm_review.get("verification_confidence", 0),
        "verdict": llm_review.get("verdict", "review"),
        "issues_found": llm_review.get("issues_found", []),
        "human_review_required": record.get("human_review", {}).get("required", False),
        "human_review_reason": record.get("human_review", {}).get("notes", ""),
        "suggested_status": record.get("status", "review_queue"),
        "corrected_fields": {},
    }


def destination_dir_for_status(status: str) -> Path:
    if status == "accepted":
        return ACCEPTED_DIR
    if status == "rejected":
        return REJECTED_DIR
    return REVIEW_QUEUE_DIR


def evaluate_existing_record(record_path: Path, verification_path: Path, rules: dict) -> tuple[dict, str, list[str]]:
    record = load_json_file(record_path)
    verification = verification_payload_for(record, verification_path)

    raw_text_path = record.get("raw_text_path", "")
    if raw_text_path:
        candidate_path = BASE_DIR / Path(raw_text_path)
        if candidate_path.exists():
            status, reasons = evaluate_record_for_backfill(record, verification, candidate_path.read_text(encoding="utf-8", errors="ignore"), rules)
            return record, status, reasons

    status, reasons = classify_missing_raw_record(record)
    return record, status, reasons


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill archive quality gates across accepted and review_queue records.")
    parser.add_argument("--apply", action="store_true", help="Apply moves and record updates.")
    parser.add_argument("--dry-run", action="store_true", help="Show planned moves without mutating files.")
    args = parser.parse_args()

    apply_changes = args.apply

    ingestion_manifest = load_json_file(INGESTION_MANIFEST_PATH) if INGESTION_MANIFEST_PATH.exists() else {"record_rules": {}}
    record_rules = ingestion_manifest.get("record_rules", {})

    record_paths = []
    for directory in [ACCEPTED_DIR, REVIEW_QUEUE_DIR]:
        record_paths.extend(
            path
            for path in sorted(directory.glob("*.json"))
            if not path.name.endswith("_verification.json")
        )

    results = []
    for record_path in record_paths:
        verification_path = record_path.with_name(f"{record_path.stem}_verification.json")
        rules = record_rules.get(record_path.stem, {})
        record, status, reasons = evaluate_existing_record(record_path, verification_path, rules)
        results.append((record_path, verification_path, record, status, reasons))

    for record_path, verification_path, record, status, reasons in results:
        reason_text = ", ".join(reasons)
        print(f"{record_path.relative_to(BASE_DIR)} -> {status}" + (f" [{reason_text}]" if reason_text else ""))

        if not apply_changes:
            continue

        record["status"] = status
        if reasons:
            record.setdefault("human_review", {})
            record["human_review"]["notes"] = reason_text
            if status == "rejected":
                record["human_review"]["required"] = False

        target_dir = destination_dir_for_status(status)
        target_record_path = target_dir / record_path.name
        target_verification_path = target_dir / verification_path.name

        save_json_file(record_path, record)
        if record_path.resolve() != target_record_path.resolve():
            move_file_if_exists(record_path, target_record_path)
            move_file_if_exists(verification_path, target_verification_path)

    if apply_changes:
        print("Applied backfill archive quality routing.")
    else:
        print("Dry run complete. Re-run with --apply to move files.")


if __name__ == "__main__":
    main()
