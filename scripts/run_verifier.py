import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

from scripts.filter_raw_records import parse_raw_record
from scripts.verification_store import canonicalize_verification_artifact


CONTAINER_PAGE_TYPES = {"homepage", "navigation_page", "listing_page", "search_page"}


BASE_DIR = Path(__file__).resolve().parent.parent
PROMPTS_DIR = BASE_DIR / "prompts"
RAW_DIR = BASE_DIR / "data" / "raw"
REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"
INGESTION_MANIFEST_PATH = BASE_DIR / "data" / "ingestion_manifest.json"


def load_text_file(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return path.read_text(encoding="utf-8").strip()


def load_json_file(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json_file(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def normalize_page_types(values: list[str] | None) -> list[str]:
    if not values:
        return []
    return [
        value.strip().lower()
        for value in values
        if isinstance(value, str) and value.strip()
    ]


def collect_hard_blockers(record: dict, metadata: dict, rules: dict) -> list[str]:
    page_type = metadata.get("PAGE_TYPE", "").strip().lower()
    warnings = {
        warning.strip()
        for warning in metadata.get("EXTRACTION_WARNINGS", "").split(",")
        if warning.strip()
    }
    blockers = set()
    source = record.get("source", {})
    source_type = source.get("source_type", "").strip().lower()
    effective_page_type = page_type or source_type

    if effective_page_type in CONTAINER_PAGE_TYPES or source_type == "website_navigation" or "container_page" in warnings:
        blockers.add("container_page")

    allowed_page_types = normalize_page_types(rules.get("allowed_page_types", []))
    if allowed_page_types and effective_page_type and effective_page_type not in allowed_page_types:
        blockers.add("page_type_not_allowed")

    expected_language = metadata.get("EXPECTED_LANGUAGE", "").strip().lower()
    detected_language = metadata.get("DETECTED_LANGUAGE", "").strip().lower()
    if expected_language and detected_language and detected_language not in {expected_language, "unknown"}:
        blockers.add("language_mismatch")

    source_name = source.get("name", "").strip()
    source_url = source.get("url", "").strip()
    if effective_page_type and effective_page_type not in CONTAINER_PAGE_TYPES and source_type not in {"placeholder", "dataset_snapshot"} and (not source_name or not source_url):
        blockers.add("missing_source_attribution")

    lower_summary = record.get("summary", "").lower()
    lower_notes = record.get("notes", "").lower()
    if source_type in {"placeholder", "dataset_snapshot"} or "placeholder" in lower_summary or "placeholder" in lower_notes or "no actual data" in lower_summary or "no actual data" in lower_notes:
        blockers.add("placeholder_source")

    return sorted(blockers)


def collect_soft_blockers(record: dict, verification: dict) -> list[str]:
    blockers = []
    issues = record.get("llm_review", {}).get("issues_found", [])
    verification_confidence = record.get("llm_review", {}).get("verification_confidence", 0)

    if verification_confidence < 8:
        blockers.append("low_verification_confidence")
    if issues:
        blockers.append("issues_found")
    if verification.get("suggested_status") != "accepted":
        blockers.append("suggested_status_not_accepted")

    return blockers


def build_verify_input(prompt_text: str, source_record: dict, research_record: dict) -> str:
    metadata = source_record.get("metadata", {})
    body_text = source_record.get("body", "")
    return (
        f"{prompt_text}\n\n"
        f"=== SOURCE METADATA START ===\n"
        f"{json.dumps(metadata, indent=2, ensure_ascii=False)}\n"
        f"=== SOURCE METADATA END ===\n\n"
        f"=== SOURCE BODY START ===\n"
        f"{body_text}\n"
        f"=== SOURCE BODY END ===\n\n"
        f"=== GENERATED RESEARCH RECORD START ===\n"
        f"{json.dumps(research_record, indent=2)}\n"
        f"=== GENERATED RESEARCH RECORD END ===\n"
    )


def extract_json_from_response(text: str) -> dict:
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = text[start:end + 1]
        return json.loads(candidate)

    raise ValueError("Model response did not contain valid JSON.")


def call_minimax(prompt_input: str) -> dict:
    load_dotenv()

    api_key = os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("OPENAI_BASE_URL", "https://api.minimax.io/v1")
    model = os.getenv("MINIMAX_MODEL", "MiniMax-M2.7")

    if not api_key:
        raise EnvironmentError("OPENAI_API_KEY is missing. Add it to your .env file.")

    client = OpenAI(api_key=api_key, base_url=base_url)

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "Return only valid JSON. Do not include markdown fences."},
            {"role": "user", "content": prompt_input},
        ],
        temperature=0.1,
        max_completion_tokens=2000,
    )

    content = response.choices[0].message.content
    if not content:
        raise ValueError("MiniMax returned an empty response.")

    return extract_json_from_response(content)


def apply_verification_result(record: dict, verification: dict) -> dict:
    record["llm_review"]["verification_confidence"] = verification.get("verification_confidence", 0)
    record["llm_review"]["verdict"] = verification.get("verdict", "review")
    record["llm_review"]["issues_found"] = verification.get("issues_found", [])

    record["human_review"]["required"] = verification.get("human_review_required", False)

    human_reason = verification.get("human_review_reason", "")
    if human_reason:
        record["human_review"]["notes"] = human_reason

    suggested_status = verification.get("suggested_status", "review_queue")
    if suggested_status in ["accepted", "review_queue", "rejected"]:
        record["status"] = suggested_status
    else:
        record["status"] = "review_queue"

    corrected_fields = verification.get("corrected_fields", {})
    for field_name in ["summary", "why_it_matters", "macro_context", "market_structure_context"]:
        corrected_value = corrected_fields.get(field_name, "")
        if corrected_value:
            record[field_name] = corrected_value

    return record


def apply_archive_quality_gate(record: dict, verification: dict, metadata: dict, rules: dict) -> dict:
    hard_blockers = collect_hard_blockers(record, metadata, rules)
    if hard_blockers:
        record["status"] = "rejected"
        record["llm_review"]["verdict"] = "reject"
        record["human_review"]["required"] = False
        record["human_review"]["notes"] = ", ".join(hard_blockers)
        return record

    soft_blockers = collect_soft_blockers(record, verification)
    if soft_blockers:
        record["status"] = "review_queue"
        record["human_review"]["required"] = True
        if not record["human_review"].get("notes"):
            record["human_review"]["notes"] = ", ".join(soft_blockers)
        return record

    record["status"] = "accepted"
    return record


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python scripts/run_verifier.py <record_id>")

    record_id = sys.argv[1]

    raw_file_path = RAW_DIR / f"{record_id}.txt"
    record_path = REVIEW_QUEUE_DIR / f"{record_id}.json"
    verify_prompt_path = PROMPTS_DIR / "verify.txt"

    source_text = load_text_file(raw_file_path)
    source_record = parse_raw_record(source_text)
    research_record = load_json_file(record_path)
    verify_prompt = load_text_file(verify_prompt_path)
    ingestion_manifest = load_json_file(INGESTION_MANIFEST_PATH)
    rules = ingestion_manifest.get("record_rules", {}).get(record_id, {})

    prompt_input = build_verify_input(verify_prompt, source_record, research_record)

    print(f"\nCalling MiniMax verifier for: {record_id}\n")
    verification_result = call_minimax(prompt_input)

    updated_record = apply_verification_result(research_record, verification_result)
    updated_record = apply_archive_quality_gate(
        updated_record,
        verification_result,
        source_record.get("metadata", {}),
        rules,
    )

    verification_output_path = canonicalize_verification_artifact(record_id)
    save_json_file(verification_output_path, verification_result)
    save_json_file(record_path, updated_record)

    print("Saved verification result to:")
    print(verification_output_path.relative_to(BASE_DIR))

    print("\nUpdated research record:")
    print(record_path.relative_to(BASE_DIR))


if __name__ == "__main__":
    main()
