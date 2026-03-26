import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI


BASE_DIR = Path(__file__).resolve().parent.parent
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


def build_record_template(schema: dict, raw_file_path: Path) -> dict:
    record = deepcopy(schema)
    record["id"] = raw_file_path.stem
    record["created_at"] = datetime.now(timezone.utc).isoformat()
    record["raw_text_path"] = str(raw_file_path.relative_to(BASE_DIR))
    record["status"] = "review_queue"
    record["llm_review"]["verdict"] = "pending"
    record["llm_review"]["verification_confidence"] = 0
    return record


def build_prompt_input(prompt_text: str, source_text: str, record_template: dict) -> str:
    return (
        f"{prompt_text}\n\n"
        f"=== SOURCE TEXT START ===\n"
        f"{source_text}\n"
        f"=== SOURCE TEXT END ===\n\n"
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
            {
                "role": "system",
                "content": "Return only valid JSON. Do not include markdown fences."
            },
            {
                "role": "user",
                "content": prompt_input
            }
        ],
        temperature=0.2,
        max_completion_tokens=2500
    )

    content = response.choices[0].message.content
    if not content:
        raise ValueError("MiniMax returned an empty response.")

    return extract_json_from_response(content)


def main() -> None:
    raw_file_path = RAW_DIR / "sample_source.txt"
    summarize_prompt_path = PROMPTS_DIR / "summarize.txt"
    schema_path = SCHEMAS_DIR / "research_record.json"

    source_text = load_text_file(raw_file_path)
    summarize_prompt = load_text_file(summarize_prompt_path)
    schema = load_json_file(schema_path)

    record_template = build_record_template(schema, raw_file_path)
    prompt_input = build_prompt_input(summarize_prompt, source_text, record_template)

    print("\nCalling MiniMax...\n")
    generated_record = call_minimax(prompt_input)

    output_path = REVIEW_QUEUE_DIR / f"{raw_file_path.stem}.json"
    save_json_file(output_path, generated_record)

    print("Saved generated record to:")
    print(output_path.relative_to(BASE_DIR))


if __name__ == "__main__":
    main()