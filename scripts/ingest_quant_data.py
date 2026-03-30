import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "quant_sources.json"
RAW_DIR = BASE_DIR / "data" / "raw"
MANIFEST_PATH = BASE_DIR / "data" / "quant_ingestion_manifest.json"

FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"


def normalize_api_key(value: str | None) -> str:
    return (value or "").strip()


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
    normalized_api_key = normalize_api_key(api_key)
    if not normalized_api_key:
        raise ValueError("FRED API key is missing or blank.")

    params = {
        "series_id": series_code,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 10,
    }

    response = requests.get(
        FRED_OBSERVATIONS_URL,
        params=params,
        headers={"Authorization": f"Bearer {normalized_api_key}"},
        timeout=30,
    )

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        response_text = response.text.strip()
        detail = f"{response.status_code}"
        if response_text:
            detail = f"{detail} {response_text}"
        raise RuntimeError(f"FRED request failed for {series_code}: {detail}") from exc

    return response.json()


def extract_recent_valid_observations(payload: dict) -> list[dict]:
    observations = payload.get("observations", [])
    cleaned = []

    for obs in observations:
        value = obs.get("value", ".")
        if value == ".":
            continue

        try:
            numeric_value = float(value)
        except ValueError:
            continue

        cleaned.append({"date": obs.get("date", ""), "value": numeric_value})

    return cleaned[:5]


def compute_direction(latest: float, previous: float) -> str:
    if latest > previous:
        return "up"
    if latest < previous:
        return "down"
    return "flat"


def format_number(value: float) -> str:
    return f"{value:.4f}".rstrip("0").rstrip(".")


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
        f"LATEST_OBSERVATION_VALUE: {format_number(latest['value'])}",
    ]

    if previous:
        absolute_change = latest["value"] - previous["value"]
        direction = compute_direction(latest["value"], previous["value"])

        content_lines.extend(
            [
                f"PREVIOUS_OBSERVATION_DATE: {previous['date']}",
                f"PREVIOUS_OBSERVATION_VALUE: {format_number(previous['value'])}",
                f"ABSOLUTE_CHANGE: {format_number(absolute_change)}",
                f"DIRECTION: {direction}",
            ]
        )
    else:
        content_lines.extend(
            [
                "PREVIOUS_OBSERVATION_DATE: unknown",
                "PREVIOUS_OBSERVATION_VALUE: unknown",
                "ABSOLUTE_CHANGE: unknown",
                "DIRECTION: unknown",
            ]
        )

    content_lines.append("")
    content_lines.append("RECENT_OBSERVATIONS:")

    for obs in recent:
        content_lines.append(f"- {obs['date']}: {format_number(obs['value'])}")

    content_lines.append("")
    content_lines.append("QUANT_SUMMARY:")
    if previous:
        content_lines.append(
            f"{series_item['name']} latest value is {format_number(latest['value'])} on {latest['date']}, "
            f"versus {format_number(previous['value'])} on {previous['date']}, "
            f"for a change of {format_number(latest['value'] - previous['value'])} ({direction})."
        )
    else:
        content_lines.append(
            f"{series_item['name']} latest value is {format_number(latest['value'])} on {latest['date']}."
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
    fred_api_key = normalize_api_key(os.getenv("FRED_API_KEY"))
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
