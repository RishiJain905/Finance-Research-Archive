import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.manifest_db import (
    add_quant_series,
    ensure_schema,
    get_quant_series_latest_data_date,
)

CONFIG_PATH = BASE_DIR / "config" / "quant_sources.json"
RAW_DIR = BASE_DIR / "data" / "raw"

FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"
WORLD_BANK_API_URL = "https://api.worldbank.org/v2/indicator"
TREASURY_FISCAL_DATA_URL = "https://api.fiscaldata.treasury.gov/services/api/v1"
NYFED_REPO_URL = "https://markets.newyorkfed.org/api/repo/all/results/latest.json"


def normalize_api_key(value: str | None) -> str:
    return (value or "").strip()


def load_json(path: Path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


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
        "api_key": normalized_api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 10,
    }

    response = requests.get(
        FRED_OBSERVATIONS_URL,
        params=params,
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


def build_fred_snapshot(series_item: dict, api_key: str) -> tuple[str, str, str | None]:
    """Build a FRED series snapshot.

    Returns (record_id, content, latest_data_date).
    latest_data_date is None when no valid observations are available.
    """
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
        return record_id, content, None

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
            f"New release: {series_item['name']}. "
            f"Latest value: {format_number(latest['value'])} ({latest['date']}). "
            f"Previous: {format_number(previous['value'])} ({previous['date']}). "
            f"Change: {format_number(latest['value'] - previous['value'])} ({direction})."
        )
    else:
        content_lines.append(
            f"{series_item['name']} latest value is {format_number(latest['value'])} on {latest['date']}."
        )

    return record_id, "\n".join(content_lines), latest["date"]


def build_wb_snapshot(wb_item: dict) -> tuple[str, str, str | None]:
    """Build a World Bank indicator snapshot.

    Returns (record_id, content, latest_data_date).
    """
    stamp = today_stamp()
    record_id = f"{wb_item['id']}_{stamp}"
    indicator = wb_item["indicator"]
    url = f"{WORLD_BANK_API_URL}/{indicator}?format=json&country=USA&mrv=5&per_page=10"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        content = (
            f"TARGET: {wb_item['name']}\n"
            f"TOPIC: {wb_item['topic']}\n"
            f"SOURCE: worldbank\n"
            f"INDICATOR: {indicator}\n"
            f"SNAPSHOT_DATE: {stamp}\n\n"
            f"Failed to fetch World Bank data: {exc}"
        )
        return record_id, content, None

    observations = []
    if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list):
        for obs in data[1]:
            value = obs.get("value")
            if value is None:
                continue
            try:
                observations.append({
                    "date": obs.get("date", ""),
                    "value": float(value),
                    "country": obs.get("country", {}).get("value", "Unknown"),
                })
            except (ValueError, TypeError):
                continue

    if not observations:
        content = (
            f"TARGET: {wb_item['name']}\n"
            f"TOPIC: {wb_item['topic']}\n"
            f"SOURCE: worldbank\n"
            f"INDICATOR: {indicator}\n"
            f"SNAPSHOT_DATE: {stamp}\n\n"
            f"No recent valid observations were returned for indicator {indicator}."
        )
        return record_id, content, None

    latest = observations[0]
    previous = observations[1] if len(observations) > 1 else None

    content_lines = [
        f"TARGET: {wb_item['name']}",
        f"TOPIC: {wb_item['topic']}",
        f"SOURCE: worldbank",
        f"INDICATOR: {indicator}",
        f"SNAPSHOT_DATE: {stamp}",
        "",
        f"LATEST_OBSERVATION_DATE: {latest['date']}",
        f"LATEST_OBSERVATION_VALUE: {format_number(latest['value'])}",
        f"LATEST_COUNTRY: {latest.get('country', 'World')}",
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
    for obs in observations:
        country_label = f" [{obs.get('country', '')}]" if obs.get('country') else ""
        content_lines.append(f"- {obs['date']}: {format_number(obs['value'])}{country_label}")

    content_lines.append("")
    content_lines.append("QUANT_SUMMARY:")
    if previous:
        content_lines.append(
            f"New release: {wb_item['name']}. "
            f"Latest value: {format_number(latest['value'])} ({latest['date']}). "
            f"Previous: {format_number(previous['value'])} ({previous['date']}). "
            f"Change: {format_number(latest['value'] - previous['value'])} ({direction})."
        )
    else:
        content_lines.append(
            f"{wb_item['name']} latest value is {format_number(latest['value'])} on {latest['date']}."
        )

    return record_id, "\n".join(content_lines), latest["date"]


# ---------------------------------------------------------------------------
# Treasury FiscalData API fetchers
# ---------------------------------------------------------------------------

def build_treasury_auctions_snapshot(dataset_item: dict) -> tuple[str, str, str | None]:
    """Fetch historical Treasury auction data from the FiscalData API."""
    stamp = today_stamp()
    record_id = f"{dataset_item['id']}_{stamp}"
    endpoint = f"{TREASURY_FISCAL_DATA_URL}/accounting/od/auction_data_securities/"
    params = {
        "fields": "issue_date,security_type,security_term,offering_amt,total_accepted",
        "sort": "-issue_date",
        "page[size]": 5,
        "format": "json",
    }

    try:
        response = requests.get(endpoint, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        content = (
            f"TARGET: {dataset_item['name']}\n"
            f"TOPIC: {dataset_item['topic']}\n"
            f"SOURCE: treasury_fiscal_data\n"
            f"SNAPSHOT_DATE: {stamp}\n\n"
            f"Failed to fetch Treasury auction data: {exc}"
        )
        return record_id, content, None

    records = data.get("data", [])
    if not records:
        content = (
            f"TARGET: {dataset_item['name']}\n"
            f"TOPIC: {dataset_item['topic']}\n"
            f"SOURCE: treasury_fiscal_data\n"
            f"SNAPSHOT_DATE: {stamp}\n\n"
            f"No auction records returned from TreasuryFiscalData API."
        )
        return record_id, content, None

    latest = records[0]
    latest_date = latest.get("issue_date", "")

    content_lines = [
        f"TARGET: {dataset_item['name']}",
        f"TOPIC: {dataset_item['topic']}",
        f"SOURCE: treasury_fiscal_data",
        f"SNAPSHOT_DATE: {stamp}",
        "",
        f"LATEST_ISSUE_DATE: {latest_date}",
        f"SECURITY_TYPE: {latest.get('security_type', 'N/A')}",
        f"SECURITY_TERM: {latest.get('security_term', 'N/A')}",
        f"OFFERING_AMT: {latest.get('offering_amt', 'N/A')}",
        f"TOTAL_ACCEPTED: {latest.get('total_accepted', 'N/A')}",
        "",
        "RECENT_AUCTIONS:",
    ]

    for rec in records:
        content_lines.append(
            f"- {rec.get('issue_date', 'N/A')}: {rec.get('security_type', '')} "
            f"{rec.get('security_term', '')} offering={rec.get('offering_amt', 'N/A')}"
        )

    content_lines.append("")
    content_lines.append("QUANT_SUMMARY:")
    content_lines.append(
        f"New release: {dataset_item['name']}. "
        f"Latest auction: {latest.get('security_type', '')} {latest.get('security_term', '')} "
        f"issued {latest_date}, offering {latest.get('offering_amt', 'N/A')}, "
        f"accepted {latest.get('total_accepted', 'N/A')}."
    )

    return record_id, "\n".join(content_lines), latest_date


def build_treasury_upcoming_snapshot(dataset_item: dict) -> tuple[str, str, str | None]:
    """Fetch upcoming Treasury auction schedule from the FiscalData API."""
    stamp = today_stamp()
    record_id = f"{dataset_item['id']}_{stamp}"
    endpoint = f"{TREASURY_FISCAL_DATA_URL}/accounting/od/upcoming_auctions/"
    params = {
        "fields": "auction_date,security_type,security_term,offering_amt",
        "sort": "auction_date",
        "page[size]": 10,
        "format": "json",
    }

    try:
        response = requests.get(endpoint, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        content = (
            f"TARGET: {dataset_item['name']}\n"
            f"TOPIC: {dataset_item['topic']}\n"
            f"SOURCE: treasury_fiscal_data\n"
            f"SNAPSHOT_DATE: {stamp}\n\n"
            f"Failed to fetch upcoming Treasury auction data: {exc}"
        )
        return record_id, content, None

    records = data.get("data", [])
    if not records:
        content = (
            f"TARGET: {dataset_item['name']}\n"
            f"TOPIC: {dataset_item['topic']}\n"
            f"SOURCE: treasury_fiscal_data\n"
            f"SNAPSHOT_DATE: {stamp}\n\n"
            f"No upcoming auction records returned from TreasuryFiscalData API."
        )
        return record_id, content, None

    # Use the last (furthest-out) auction date as the latest_data_date so we
    # detect when a new auction is scheduled beyond what we previously stored.
    latest_date = records[-1].get("auction_date", "")

    content_lines = [
        f"TARGET: {dataset_item['name']}",
        f"TOPIC: {dataset_item['topic']}",
        f"SOURCE: treasury_fiscal_data",
        f"SNAPSHOT_DATE: {stamp}",
        "",
        "UPCOMING_AUCTIONS:",
    ]

    for rec in records:
        content_lines.append(
            f"- {rec.get('auction_date', 'N/A')}: {rec.get('security_type', '')} "
            f"{rec.get('security_term', '')} offering={rec.get('offering_amt', 'N/A')}"
        )

    content_lines.append("")
    content_lines.append("QUANT_SUMMARY:")
    next_rec = records[0]
    content_lines.append(
        f"New release: {dataset_item['name']}. "
        f"Next auction: {next_rec.get('security_type', '')} {next_rec.get('security_term', '')} "
        f"on {next_rec.get('auction_date', 'N/A')}, "
        f"offering {next_rec.get('offering_amt', 'N/A')}. "
        f"{len(records)} auctions scheduled through {latest_date}."
    )

    return record_id, "\n".join(content_lines), latest_date


def build_nyfed_snapshot(dataset_item: dict) -> tuple[str, str, str | None]:
    """Fetch NY Fed open market repo operation results."""
    stamp = today_stamp()
    record_id = f"{dataset_item['id']}_{stamp}"

    try:
        response = requests.get(NYFED_REPO_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        content = (
            f"TARGET: {dataset_item['name']}\n"
            f"TOPIC: {dataset_item['topic']}\n"
            f"SOURCE: nyfed\n"
            f"SNAPSHOT_DATE: {stamp}\n\n"
            f"Failed to fetch NY Fed repo operation data: {exc}"
        )
        return record_id, content, None

    # NY Fed API returns {"repo": {"operations": [...]}} or similar structure.
    # Normalise defensively.
    operations = []
    if isinstance(data, dict):
        repo = data.get("repo", data)
        ops_raw = repo.get("operations", []) if isinstance(repo, dict) else []
        if isinstance(ops_raw, list):
            operations = ops_raw

    if not operations:
        content = (
            f"TARGET: {dataset_item['name']}\n"
            f"TOPIC: {dataset_item['topic']}\n"
            f"SOURCE: nyfed\n"
            f"SNAPSHOT_DATE: {stamp}\n\n"
            f"No repo operation records returned from NY Fed API."
        )
        return record_id, content, None

    latest_op = operations[0]
    op_date = latest_op.get("operationDate", latest_op.get("date", ""))
    total_submitted = latest_op.get("totalSubmitted", latest_op.get("totalAmtSubmitted", "N/A"))
    total_accepted = latest_op.get("totalAccepted", latest_op.get("totalAmtAccepted", "N/A"))
    operation_type = latest_op.get("type", latest_op.get("operationType", "N/A"))

    content_lines = [
        f"TARGET: {dataset_item['name']}",
        f"TOPIC: {dataset_item['topic']}",
        f"SOURCE: nyfed",
        f"SNAPSHOT_DATE: {stamp}",
        "",
        f"LATEST_OPERATION_DATE: {op_date}",
        f"OPERATION_TYPE: {operation_type}",
        f"TOTAL_SUBMITTED: {total_submitted}",
        f"TOTAL_ACCEPTED: {total_accepted}",
        "",
        "RECENT_OPERATIONS:",
    ]

    for op in operations[:5]:
        d = op.get("operationDate", op.get("date", "N/A"))
        t = op.get("type", op.get("operationType", "N/A"))
        amt = op.get("totalAccepted", op.get("totalAmtAccepted", "N/A"))
        content_lines.append(f"- {d}: {t} accepted={amt}")

    content_lines.append("")
    content_lines.append("QUANT_SUMMARY:")
    content_lines.append(
        f"New release: {dataset_item['name']}. "
        f"Latest operation: {operation_type} on {op_date}. "
        f"Total submitted: {total_submitted}, total accepted: {total_accepted}."
    )

    return record_id, "\n".join(content_lines), op_date


def build_dataset_snapshot(dataset_item: dict) -> tuple[str, str, str | None]:
    """Dispatch to the appropriate real fetcher based on source type."""
    source = dataset_item.get("source", "")
    dataset_id = dataset_item.get("id", "")

    if source == "treasury_fiscal_data":
        if "upcoming" in dataset_id:
            return build_treasury_upcoming_snapshot(dataset_item)
        return build_treasury_auctions_snapshot(dataset_item)

    if source == "nyfed":
        return build_nyfed_snapshot(dataset_item)

    raise ValueError(
        f"Unknown dataset source type {source!r} for dataset {dataset_id!r}. "
        f"Supported: treasury_fiscal_data, nyfed."
    )


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _is_newer_date(fetched_date: str | None, stored_date: str | None) -> bool:
    """Return True if fetched_date is strictly newer than stored_date.

    Dates are compared lexicographically (works for ISO YYYY-MM-DD and
    World Bank year strings like "2023"). When fetched_date is None or empty,
    we conservatively treat it as new data to avoid silent skips.
    """
    if not fetched_date:
        return True
    if not stored_date:
        return True
    return fetched_date > stored_date


def main() -> list[str]:
    ensure_schema()

    fred_api_key = normalize_api_key(os.getenv("FRED_API_KEY"))
    config = load_json(CONFIG_PATH, {"series": [], "datasets": []})

    created = []
    today = today_stamp()

    for series_item in config.get("series", []):
        if not series_item.get("enabled", False):
            continue

        if series_item.get("source") != "fred":
            continue

        if not fred_api_key:
            print(f"Skipping FRED series {series_item['id']}: missing FRED_API_KEY")
            continue

        series_id = series_item["id"]

        try:
            record_id, content, latest_data_date = build_fred_snapshot(series_item, fred_api_key)
        except Exception as e:
            print(f"Failed FRED series {series_id}: {e}")
            continue

        stored_date = get_quant_series_latest_data_date(series_id)
        if not _is_newer_date(latest_data_date, stored_date):
            print(
                f"No new data for FRED series {series_id} "
                f"(latest: {latest_data_date}, stored: {stored_date}), skipping."
            )
            continue

        write_raw_snapshot(record_id, content)
        add_quant_series(series_id, today, _content_hash(content), latest_data_date=latest_data_date)
        created.append(record_id)
        print(f"Created FRED series snapshot: {record_id} (data date: {latest_data_date})")

    for dataset_item in config.get("datasets", []):
        if not dataset_item.get("enabled", False):
            continue

        series_id = dataset_item["id"]

        try:
            record_id, content, latest_data_date = build_dataset_snapshot(dataset_item)
        except Exception as e:
            print(f"Failed dataset {series_id}: {e}")
            continue

        stored_date = get_quant_series_latest_data_date(series_id)
        if not _is_newer_date(latest_data_date, stored_date):
            print(
                f"No new data for dataset {series_id} "
                f"(latest: {latest_data_date}, stored: {stored_date}), skipping."
            )
            continue

        write_raw_snapshot(record_id, content)
        add_quant_series(series_id, today, _content_hash(content), latest_data_date=latest_data_date)
        created.append(record_id)
        print(f"Created dataset snapshot: {record_id} (data date: {latest_data_date})")

    for wb_item in config.get("worldbank_series", []):
        if not wb_item.get("enabled", False):
            continue

        series_id = wb_item["id"]

        try:
            record_id, content, latest_data_date = build_wb_snapshot(wb_item)
        except Exception as e:
            print(f"Failed World Bank series {series_id}: {e}")
            continue

        stored_date = get_quant_series_latest_data_date(series_id)
        if not _is_newer_date(latest_data_date, stored_date):
            print(
                f"No new data for World Bank series {series_id} "
                f"(latest: {latest_data_date}, stored: {stored_date}), skipping."
            )
            continue

        write_raw_snapshot(record_id, content)
        add_quant_series(series_id, today, _content_hash(content), latest_data_date=latest_data_date)
        created.append(record_id)
        print(f"Created World Bank snapshot: {record_id} (data date: {latest_data_date})")

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
