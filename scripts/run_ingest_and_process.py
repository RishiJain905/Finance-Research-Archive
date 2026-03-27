import json
import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent


def run_command(command: list[str], step_name: str) -> str:
    print(f"\n=== Running {step_name} ===\n")

    result = subprocess.run(
        command,
        cwd=BASE_DIR,
        text=True,
        capture_output=True
    )

    if result.stdout:
        print(result.stdout)

    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"{step_name} failed with exit code {result.returncode}")

    return result.stdout


def extract_created_ids(output: str) -> list[str]:
    start_marker = "JSON_OUTPUT_START"
    end_marker = "JSON_OUTPUT_END"

    start_index = output.find(start_marker)
    end_index = output.find(end_marker)

    if start_index == -1 or end_index == -1:
        return []

    json_text = output[start_index + len(start_marker):end_index].strip()
    if not json_text:
        return []

    data = json.loads(json_text)
    if not isinstance(data, list):
        return []

    return [item for item in data if isinstance(item, str) and item.strip()]


def file_still_in_raw(record_id: str) -> bool:
    raw_path = BASE_DIR / "data" / "raw" / f"{record_id}.txt"
    return raw_path.exists()


def main() -> None:
    python_cmd = sys.executable

    ingest_output = run_command(
        [python_cmd, "scripts/ingest_sources.py"],
        "ingestion"
    )

    record_ids = extract_created_ids(ingest_output)

    run_command(
        [python_cmd, "scripts/filter_raw_records.py"],
        "raw record filtering"
    )

    if not record_ids:
        print("\nNo new or updated records to process.")
        return

    filtered_record_ids = [record_id for record_id in record_ids if file_still_in_raw(record_id)]

    if not filtered_record_ids:
        print("\nNo records survived filtering.")
        return

    print("\nRecords to process:")
    for record_id in filtered_record_ids:
        print(f"- {record_id}")

    for record_id in filtered_record_ids:
        run_command(
            [python_cmd, "scripts/process_record.py", record_id],
            f"process_record ({record_id})"
        )

    print("\nIngestion + filtering + processing pipeline finished.")


if __name__ == "__main__":
    main()