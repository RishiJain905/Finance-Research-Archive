import json
from pathlib import Path
from datetime import datetime, timezone


BASE_DIR = Path(__file__).resolve().parent.parent
PROMPTS_DIR = BASE_DIR / "prompts"
SCHEMAS_DIR = BASE_DIR / "schemas"
RAW_DIR = BASE_DIR / "data" / "raw"


def load_text_file(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return path.read_text(encoding="utf-8").strip()


def load_json_file(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_record_template(schema: dict, raw_file_path: Path) -> dict:
    record = schema.copy()
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


def main() -> None:
    raw_file_path = RAW_DIR / "sample_source.txt"
    summarize_prompt_path = PROMPTS_DIR / "summarize.txt"
    schema_path = SCHEMAS_DIR / "research_record.json"

    source_text = load_text_file(raw_file_path)
    summarize_prompt = load_text_file(summarize_prompt_path)
    schema = load_json_file(schema_path)

    record_template = build_record_template(schema, raw_file_path)
    prompt_input = build_prompt_input(summarize_prompt, source_text, record_template)

    print("\n===== RECORD TEMPLATE =====\n")
    print(json.dumps(record_template, indent=2))

    print("\n===== PROMPT INPUT PREVIEW =====\n")
    print(prompt_input[:4000])


if __name__ == "__main__":
    main()