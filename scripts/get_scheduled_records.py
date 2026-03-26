import json
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "scheduled_records.json"


def main() -> None:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH}")

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    record_ids = data.get("record_ids", [])
    if not isinstance(record_ids, list):
        raise ValueError("record_ids must be a list.")

    cleaned = []
    for record_id in record_ids:
        if isinstance(record_id, str) and record_id.strip():
            cleaned.append(record_id.strip())

    print(json.dumps(cleaned))


if __name__ == "__main__":
    main()