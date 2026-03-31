import json
import os
import sys
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
from openai import OpenAI

from scripts.filter_raw_records import parse_raw_record
from scripts.verification_store import canonicalize_verification_artifact


CONTAINER_PAGE_TYPES = {"homepage", "navigation_page", "listing_page", "search_page"}


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

    if (
        effective_page_type in CONTAINER_PAGE_TYPES
        or source_type == "website_navigation"
        or "container_page" in warnings
    ):
        blockers.add("container_page")

    allowed_page_types = normalize_page_types(rules.get("allowed_page_types", []))
    if (
        allowed_page_types
        and effective_page_type
        and effective_page_type not in allowed_page_types
    ):
        blockers.add("page_type_not_allowed")

    expected_language = metadata.get("EXPECTED_LANGUAGE", "").strip().lower()
    detected_language = metadata.get("DETECTED_LANGUAGE", "").strip().lower()
    if (
        expected_language
        and detected_language
        and detected_language not in {expected_language, "unknown"}
    ):
        blockers.add("language_mismatch")

    source_name = source.get("name", "").strip()
    source_url = source.get("url", "").strip()
    if (
        effective_page_type
        and effective_page_type not in CONTAINER_PAGE_TYPES
        and source_type not in {"placeholder", "dataset_snapshot"}
        and (not source_name or not source_url)
    ):
        blockers.add("missing_source_attribution")

    lower_summary = record.get("summary", "").lower()
    lower_notes = record.get("notes", "").lower()
    if (
        source_type in {"placeholder", "dataset_snapshot"}
        or "placeholder" in lower_summary
        or "placeholder" in lower_notes
        or "no actual data" in lower_summary
        or "no actual data" in lower_notes
    ):
        blockers.add("placeholder_source")

    return sorted(blockers)


def collect_soft_blockers(record: dict, verification: dict) -> list[str]:
    """
    Soft blockers send a record to review_queue rather than auto-accepting.
    A record with confidence 6-7, no hard blockers, and suggested_status==accepted
    is considered borderline: it goes to review_queue but does NOT require human
    review unconditionally - a human sweep can promote it later.
    Records with confidence < 6 or suggested_status != accepted are also soft-blocked.
    """
    blockers = []
    issues = record.get("llm_review", {}).get("issues_found", [])
    verification_confidence = record.get("llm_review", {}).get(
        "verification_confidence", 0
    )

    if verification_confidence < 6:
        blockers.append("low_verification_confidence")
    elif verification_confidence < 8:
        blockers.append("borderline_confidence")
    if issues:
        blockers.append("issues_found")
    if verification.get("suggested_status") != "accepted":
        blockers.append("suggested_status_not_accepted")

    return blockers


MAX_BODY_CHARS = 8_000


def build_verify_input(
    prompt_text: str, source_record: dict, research_record: dict
) -> str:
    metadata = source_record.get("metadata", {})
    body_text = source_record.get("body", "")
    today = date.today().isoformat()

    truncated = len(body_text) > MAX_BODY_CHARS
    if truncated:
        body_text = body_text[:MAX_BODY_CHARS]

    body_block = body_text
    if truncated:
        body_block += "\n[TRUNCATED: source body exceeded character limit]"

    return (
        f"{prompt_text}\n\n"
        f"TODAY: {today}\n\n"
        f"=== SOURCE METADATA START ===\n"
        f"{json.dumps(metadata, indent=2, ensure_ascii=False)}\n"
        f"=== SOURCE METADATA END ===\n\n"
        f"=== SOURCE BODY START ===\n"
        f"{body_block}\n"
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

    decoder = json.JSONDecoder()
    for start, character in enumerate(text):
        if character != "{":
            continue
        try:
            candidate, _ = decoder.raw_decode(text, idx=start)
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict):
            return candidate

    raise ValueError("Model response did not contain valid JSON.")


def call_minimax(prompt_input: str, max_retries: int = 2) -> dict:
    load_dotenv()

    api_key = os.getenv("OPENAI_API_KEY")
    base_url = os.getenv("OPENAI_BASE_URL", "https://api.minimax.io/v1")
    model = os.getenv("MINIMAX_MODEL", "MiniMax-M2.7")

    if not api_key:
        raise EnvironmentError("OPENAI_API_KEY is missing. Add it to your .env file.")

    client = OpenAI(api_key=api_key, base_url=base_url, timeout=120.0)
    messages = [
        {
            "role": "system",
            "content": "Return only valid JSON. Do not include markdown fences.",
        },
        {"role": "user", "content": prompt_input},
    ]

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 2):
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0.1,
            max_completion_tokens=2000,
        )

        content = response.choices[0].message.content
        if not content:
            last_error = ValueError("MiniMax returned an empty response.")
            messages.append({"role": "assistant", "content": ""})
            messages.append({"role": "user", "content": "Your response was empty. Return only valid JSON."})
            continue

        try:
            return extract_json_from_response(content)
        except ValueError as e:
            last_error = e
            if attempt <= max_retries:
                print(f"  JSON parse failed on attempt {attempt}, retrying...")
                messages.append({"role": "assistant", "content": content})
                messages.append({
                    "role": "user",
                    "content": "Your response was not valid JSON. Return only the JSON object, no markdown fences or extra text.",
                })

    raise ValueError(f"MiniMax failed to return valid JSON after {max_retries + 1} attempts: {last_error}")


def apply_verification_result(record: dict, verification: dict) -> dict:
    record["llm_review"]["verification_confidence"] = verification.get(
        "verification_confidence", 0
    )
    record["llm_review"]["verdict"] = verification.get("verdict", "review")
    record["llm_review"]["issues_found"] = verification.get("issues_found", [])

    record["human_review"]["required"] = verification.get(
        "human_review_required", False
    )

    human_reason = verification.get("human_review_reason", "")
    if human_reason:
        record["human_review"]["notes"] = human_reason

    suggested_status = verification.get("suggested_status", "review_queue")
    if suggested_status in ["accepted", "review_queue", "rejected"]:
        record["status"] = suggested_status
    else:
        record["status"] = "review_queue"

    corrected_fields = verification.get("corrected_fields", {})
    for field_name in [
        "summary",
        "why_it_matters",
        "macro_context",
        "market_structure_context",
    ]:
        corrected_value = corrected_fields.get(field_name, "")
        if corrected_value:
            record[field_name] = corrected_value

    return record


def apply_archive_quality_gate(
    record: dict, verification: dict, metadata: dict, rules: dict
) -> dict:
    """
    Acceptance gate routing logic:

      Hard blockers (container page, language mismatch, etc.)
        → rejected immediately, no human review.

      No soft blockers (confidence ≥ 8, no issues, suggested_status == accepted)
        → auto-accepted.

      suggested_status == "rejected" (with any soft blockers)
        → auto-rejected, no human review. The LLM already made a clear rejection
           decision; escalating to Telegram would only spam the human review queue.

      suggested_status == "accepted", confidence ≥ 8, human_review_required == False
        (only soft blocker is issues_found — LLM noted something minor but still
         recommends accepting with high confidence)
        → auto-accepted. Trusting a high-confidence LLM "accept" with noted
           trivialities avoids unnecessary Telegram notifications.

      Borderline-only: confidence 6-7, suggested_status != rejected, no other issues
        → review_queue, human_review NOT required (drain script promotes in bulk).

      Any other soft blocker (low_verification_confidence < 6, issues_found when
        suggested_status == "review_queue", etc.)
        → review_queue, human_review required → sent to Telegram.
    """
    hard_blockers = collect_hard_blockers(record, metadata, rules)
    if hard_blockers:
        record["status"] = "rejected"
        record["llm_review"]["verdict"] = "reject"
        record["human_review"]["required"] = False
        record["human_review"]["notes"] = ", ".join(hard_blockers)
        return record

    soft_blockers = collect_soft_blockers(record, verification)

    if not soft_blockers:
        record["status"] = "accepted"
        return record

    suggested_status = verification.get("suggested_status", "review_queue")

    # If the LLM explicitly suggests rejection, auto-reject rather than routing to
    # the human review queue. This prevents clearly-rejected records from spamming
    # Telegram — only genuinely ambiguous records need human eyes.
    if suggested_status == "rejected":
        record["status"] = "rejected"
        record["human_review"]["required"] = False
        if not record["human_review"].get("notes"):
            record["human_review"]["notes"] = ", ".join(soft_blockers)
        return record

    # High-confidence "accept" where the LLM noted a minor issue but explicitly
    # said no human review is needed. The only soft blocker in this scenario is
    # issues_found; there are no confidence or status blockers. Trust the LLM.
    llm_no_human = not verification.get("human_review_required", True)
    confidence = record.get("llm_review", {}).get("verification_confidence", 0)
    if (
        suggested_status == "accepted"
        and llm_no_human
        and confidence >= 8
        and soft_blockers == ["issues_found"]
    ):
        record["status"] = "accepted"
        return record

    # Borderline-only: confidence 6-7, otherwise clean → review_queue without
    # forcing human_review so the drain script can sweep them automatically.
    borderline_only = soft_blockers == ["borderline_confidence"]
    record["status"] = "review_queue"
    record["human_review"]["required"] = not borderline_only
    if not record["human_review"].get("notes"):
        record["human_review"]["notes"] = ", ".join(soft_blockers)
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
