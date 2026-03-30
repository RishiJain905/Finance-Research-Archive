import json
import os
import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
from openai import OpenAI

from scripts.filter_raw_records import parse_raw_record

PROMPTS_DIR = BASE_DIR / "prompts"
SCHEMAS_DIR = BASE_DIR / "schemas"
RAW_DIR = BASE_DIR / "data" / "raw"
REVIEW_QUEUE_DIR = BASE_DIR / "data" / "review_queue"


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


def preferred_source_url(metadata: dict) -> str:
    return (
        metadata.get("CANONICAL_URL")
        or metadata.get("ARTICLE_URL")
        or metadata.get("URL", "")
    )


def build_record_template(
    schema: dict,
    metadata: dict,
    raw_file_path: Path | None = None,
    raw_file_stem: str | None = None,
) -> dict:
    record = deepcopy(schema)
    if raw_file_path is None and raw_file_stem is None:
        raise ValueError("raw_file_path or raw_file_stem is required")

    record_id = raw_file_path.stem if raw_file_path else raw_file_stem
    record["id"] = record_id
    record["created_at"] = datetime.now(timezone.utc).isoformat()
    if raw_file_path is not None:
        record["raw_text_path"] = str(raw_file_path.relative_to(BASE_DIR))
    record["status"] = "review_queue"
    record["topic"] = metadata.get("TOPIC", record.get("topic", ""))
    record["source"]["name"] = metadata.get("TARGET", record["source"].get("name", ""))
    record["source"]["url"] = preferred_source_url(metadata)
    record["source"]["published_at"] = metadata.get("PUBLISHED_AT", "")
    record["source"]["source_type"] = metadata.get("PAGE_TYPE", "")
    record["llm_review"]["verdict"] = "pending"
    record["llm_review"]["verification_confidence"] = 0
    return record


def hydrate_generated_record(record: dict, metadata: dict, template: dict) -> dict:
    record.setdefault("source", {})
    record["topic"] = record.get("topic") or metadata.get("TOPIC", "")
    record["source"]["name"] = record["source"].get("name") or metadata.get(
        "TARGET", ""
    )
    record["source"]["url"] = record["source"].get("url") or preferred_source_url(
        metadata
    )
    record["source"]["published_at"] = record["source"].get(
        "published_at"
    ) or metadata.get("PUBLISHED_AT", "")
    record["source"]["source_type"] = record["source"].get(
        "source_type"
    ) or metadata.get("PAGE_TYPE", "")

    if "llm_review" not in record and "llm_review" in template:
        record["llm_review"] = deepcopy(template["llm_review"])
    if "human_review" not in record and "human_review" in template:
        record["human_review"] = deepcopy(template["human_review"])

    for field in [
        "summary",
        "key_points",
        "why_it_matters",
        "macro_context",
        "market_structure_context",
        "notes",
    ]:
        if field not in record and field in template:
            record[field] = deepcopy(template[field])
    for field in ["market_impact", "important_numbers", "tags"]:
        if field not in record and field in template:
            record[field] = deepcopy(template[field])

    return record


def build_prompt_input(
    prompt_text: str, source_record: dict, record_template: dict
) -> str:
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
        f"=== RECORD TEMPLATE START ===\n"
        f"{json.dumps(record_template, indent=2)}\n"
        f"=== RECORD TEMPLATE END ===\n"
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
            {
                "role": "system",
                "content": "Return only valid JSON. Do not include markdown fences.",
            },
            {"role": "user", "content": prompt_input},
        ],
        temperature=0.2,
        max_completion_tokens=2500,
    )

    content = response.choices[0].message.content
    if not content:
        raise ValueError("MiniMax returned an empty response.")

    return extract_json_from_response(content)


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python scripts/run_summarizer.py <record_id>")

    record_id = sys.argv[1]

    raw_file_path = RAW_DIR / f"{record_id}.txt"
    summarize_prompt_path = PROMPTS_DIR / "summarize.txt"
    schema_path = SCHEMAS_DIR / "research_record.json"

    source_text = load_text_file(raw_file_path)
    summarize_prompt = load_text_file(summarize_prompt_path)
    schema = load_json_file(schema_path)
    source_record = parse_raw_record(source_text)

    record_template = build_record_template(
        schema, source_record.get("metadata", {}), raw_file_path=raw_file_path
    )
    prompt_input = build_prompt_input(summarize_prompt, source_record, record_template)

    print(f"\nCalling MiniMax summarizer for: {record_id}\n")
    generated_record = call_minimax(prompt_input)
    generated_record = hydrate_generated_record(
        generated_record, source_record.get("metadata", {}), record_template
    )

    output_path = REVIEW_QUEUE_DIR / f"{record_id}.json"
    save_json_file(output_path, generated_record)

    print("Saved generated record to:")
    print(output_path.relative_to(BASE_DIR))


if __name__ == "__main__":
    main()
