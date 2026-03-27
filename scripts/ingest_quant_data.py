import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "quant_sources.json"
RAW_DIR = BASE_DIR / "data" / "raw"
MANIFEST_PATH = BASE_DIR / "data" / "quant_ingestion_manifest.json"

FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"


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


def fetch_fred_observations(series_code: str, api_key: str) -> dict:
    params = {
        "series_id": series_code,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 5
    }

    response = requests.get(FRED_OBSERVATIONS_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def extract_recent_valid_observations(payload: dict) -> list[dict]:
    observations = payload.get("observations", [])
    cleaned = []

    for obs in observations:
        value = obs.get("value", ".")
        if value == ".":
            continue

        cleaned.append({
            "date": obs.get("date", ""),
            "value": value
        })

    return cleaned[:5]


def build_fred_snapshot(series_item: dict, api_key: str) -> tuple[str, str]:
    stamp = today_stamp()
    record_id = f"{series_item['id']}_{stamp}"

    payload = fetch_fred_observations(series_item["series_code"], api_key)
    recent = extract_recent_valid_observations(payload)

    if not recent:
        content = (
            f"TARGET: {series_item['name']}\n"
            f"TOPIC: {series_item['topic']}\n"
            f"SOURCE: fred\n"
            f"SERIES_CODE: {series_item['series_code']}\n"
            f"SNAPSHOT_DATE: {stamp}\n\n"
            f"No recent valid observations were returned for this series."
        )
        return record_id, content

    latest = recent[0]
    previous = recent[1] if len(recent) > 1 else None

    content_lines = [
        f"TARGET: {series_item['name']}",
        f"TOPIC: {series_item['topic']}",
        f"SOURCE: fred",
        f"SERIES_CODE: {series_item['series_code']}",
        f"SNAPSHOT_DATE: {stamp}",
        "",
        f"LATEST_OBSERVATION_DATE: {latest['date']}",
        f"LATEST_OBSERVATION_VALUE: {latest['value']}",
    ]

    if previous:
        content_lines.append(f"PREVIOUS_OBSERVATION_DATE: {previous['date']}")
        content_lines.append(f"PREVIOUS_OBSERVATION_VALUE: {previous['value']}")

    content_lines.append("")
    content_lines.append("RECENT_OBSERVATIONS:")

    for obs in recent:
        content_lines.append(f"- {obs['date']}: {obs['value']}")

    content_lines.append("")
    content_lines.append(
        f"This is a quantitative snapshot for the FRED series {series_item['series_code']} ({series_item['name']})."
    )

    return record_id, "\n".join(content_lines)


def build_dataset_snapshot(dataset_item: dict) -> tuple[str, str]:
    stamp = today_stamp()
    record_id = f"{dataset_item['id']}_{stamp}"

    content = (
        f"TARGET: {dataset_item['name']}\n"
        f"TOPIC: {dataset_item['topic']}\n"
        f"SOURCE: {dataset_item['source']}\n"
        f"SNAPSHOT_DATE: {stamp}\n\n"
        f"This is still a placeholder dataset snapshot for "
        f"{dataset_item['name']} from {dataset_item['source']}.\n"
        f"Next we can replace this with real dataset/API fetches."
    )

    return record_id, content


def main() -> None:
    load_dotenv(BASE_DIR / ".env")

    fred_api_key = os.getenv("FRED_API_KEY")
    config = load_json(CONFIG_PATH, {"series": [], "datasets": []})
    manifest = load_json(MANIFEST_PATH, {"created": []})

    created = []

    for series_item in config.get("series", []):
        if not series_item.get("enabled", False):
            continue

        if series_item.get("source") != "fred":
            continue

        if not fred_api_key:
            print(f"Skipping FRED series {series_item['id']}: missing FRED_API_KEY")
            continue

        try:
            record_id, content = build_fred_snapshot(series_item, fred_api_key)
            write_raw_snapshot(record_id, content)
            created.append(record_id)
            print(f"Created FRED series snapshot: {record_id}")
        except Exception as e:
            print(f"Failed FRED series {series_item['id']}: {e}")

    for dataset_item in config.get("datasets", []):
        if not dataset_item.get("enabled", False):
            continue

        try:
            record_id, content = build_dataset_snapshot(dataset_item)
            write_raw_snapshot(record_id, content)
            created.append(record_id)
            print(f"Created dataset snapshot: {record_id}")
        except Exception as e:
            print(f"Failed dataset {dataset_item['id']}: {e}")

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