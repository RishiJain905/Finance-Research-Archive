import json
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "quant_sources.json"
RAW_DIR = BASE_DIR / "data" / "raw"
MANIFEST_PATH = BASE_DIR / "data" / "quant_ingestion_manifest.json"


def load_json(path: Path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def today_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y_%m_%d")


def write_raw_snapshot(record_id: str, content: str) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RAW_DIR / f"{record_id}.txt"
    output_path.write_text(content, encoding="utf-8")


def build_fred_snapshot(series_item: dict) -> tuple[str, str]:
    stamp = today_stamp()
    record_id = f"{series_item['id']}_{stamp}"

    content = (
        f"TARGET: {series_item['name']}\n"
        f"TOPIC: {series_item['topic']}\n"
        f"SOURCE: fred\n"
        f"SERIES_CODE: {series_item['series_code']}\n"
        f"SNAPSHOT_DATE: {stamp}\n\n"
        f"This is a quantitative snapshot placeholder for the FRED series "
        f"{series_item['series_code']} ({series_item['name']}).\n"
        f"Later versions will fetch the latest numeric values, changes, and recent trend context.\n"
        f"For now, this record exists to establish the quant ingestion pipeline structure."
    )

    return record_id, content


def build_dataset_snapshot(dataset_item: dict) -> tuple[str, str]:
    stamp = today_stamp()
    record_id = f"{dataset_item['id']}_{stamp}"

    content = (
        f"TARGET: {dataset_item['name']}\n"
        f"TOPIC: {dataset_item['topic']}\n"
        f"SOURCE: {dataset_item['source']}\n"
        f"SNAPSHOT_DATE: {stamp}\n\n"
        f"This is a quantitative dataset snapshot placeholder for "
        f"{dataset_item['name']} from {dataset_item['source']}.\n"
        f"Later versions will fetch current structured numeric data and summarize "
        f"the latest relevant values."
    )

    return record_id, content


def main() -> None:
    config = load_json(CONFIG_PATH, {"series": [], "datasets": []})
    manifest = load_json(MANIFEST_PATH, {"created": []})

    created = []

    for series_item in config.get("series", []):
        if not series_item.get("enabled", False):
            continue

        record_id, content = build_fred_snapshot(series_item)
        write_raw_snapshot(record_id, content)
        created.append(record_id)
        print(f"Created series snapshot: {record_id}")

    for dataset_item in config.get("datasets", []):
        if not dataset_item.get("enabled", False):
            continue

        record_id, content = build_dataset_snapshot(dataset_item)
        write_raw_snapshot(record_id, content)
        created.append(record_id)
        print(f"Created dataset snapshot: {record_id}")

    manifest["created"] = created
    save_json(MANIFEST_PATH, manifest)

    print("\nCreated quant record ids:")
    if created:
        for record_id in created:
            print(f"- {record_id}")
    else:
        print("- none")

    return created


if __name__ == "__main__":
    created = main()
    print("\nJSON_OUTPUT_START")
    print(json.dumps(created))
    print("JSON_OUTPUT_END")