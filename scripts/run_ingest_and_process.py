import json
import shutil
import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
MANIFEST_PATH = BASE_DIR / "data" / "ingestion_manifest.json"
RAW_DIR = BASE_DIR / "data" / "raw"


def run_command(command: list[str], step_name: str) -> str:
    print(f"\n=== Running {step_name} ===\n")

    result = subprocess.run(command, cwd=BASE_DIR, text=True, capture_output=True)

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

    json_text = output[start_index + len(start_marker) : end_index].strip()
    if not json_text:
        return []

    data = json.loads(json_text)
    if not isinstance(data, list):
        return []

    return [item for item in data if isinstance(item, str) and item.strip()]


def file_still_in_raw(record_id: str) -> bool:
    raw_path = BASE_DIR / "data" / "raw" / f"{record_id}.txt"
    return raw_path.exists()


def load_manifest() -> dict:
    if not MANIFEST_PATH.exists():
        return {
            "seen_urls": {},
            "record_map": {},
            "record_rules": {},
            "title_fingerprints": {},
            "content_fingerprints": {},
            "processed_urls": {},
        }
    with MANIFEST_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_manifest(manifest: dict) -> None:
    with MANIFEST_PATH.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)


def mark_record_processed(record_id: str) -> None:
    manifest = load_manifest()

    url = manifest.get("record_map", {}).get(record_id)
    if url:
        manifest.setdefault("processed_urls", {})[url] = True
        print(f"\n  Marked URL as processed: {url}")

    raw_path = BASE_DIR / "data" / "raw" / f"{record_id}.txt"
    if raw_path.exists():
        raw_path.unlink()
        print(f"  Removed raw file: {raw_path.name}")

    save_manifest(manifest)


def verify_manifest_consistency() -> None:
    manifest = load_manifest()
    issues = []

    record_map = manifest.get("record_map", {})
    seen_urls = manifest.get("seen_urls", {})
    processed_urls = manifest.get("processed_urls", {})

    for record_id, url in record_map.items():
        if url not in seen_urls:
            issues.append(
                f"record_map has URL for '{record_id}' but URL not in seen_urls: {url}"
            )

    for url in processed_urls:
        if url not in record_map.values():
            issues.append(f"processed_urls has URL not in record_map: {url}")

    orphaned_raw_files = []
    if RAW_DIR.exists():
        for raw_file in RAW_DIR.glob("*.txt"):
            record_id = raw_file.stem
            if record_id not in record_map:
                orphaned_raw_files.append(record_id)

    if issues:
        print("\n=== Manifest Consistency Issues ===")
        for issue in issues:
            print(f"  - {issue}")
        print("==================================\n")

    if orphaned_raw_files:
        print(f"\n=== Found {len(orphaned_raw_files)} orphaned raw files ===")
        for record_id in orphaned_raw_files[:10]:
            print(f"  - {record_id}")
        if len(orphaned_raw_files) > 10:
            print(f"  ... and {len(orphaned_raw_files) - 10} more")
        print("==========================================\n")


def main() -> None:
    python_cmd = sys.executable

    print("\n=== Verifying manifest consistency ===")
    verify_manifest_consistency()

    ingest_output = run_command([python_cmd, "scripts/ingest_sources.py"], "ingestion")

    record_ids = extract_created_ids(ingest_output)

    run_command([python_cmd, "scripts/filter_raw_records.py"], "raw record filtering")

    if not record_ids:
        print("\nNo new or updated records to process.")
        return

    filtered_record_ids = [
        record_id for record_id in record_ids if file_still_in_raw(record_id)
    ]

    if not filtered_record_ids:
        print("\nNo records survived filtering.")
        return

    print("\nRecords to process:")
    for record_id in filtered_record_ids:
        print(f"- {record_id}")

    for record_id in filtered_record_ids:
        run_command(
            [python_cmd, "scripts/process_record.py", record_id],
            f"process_record ({record_id})",
        )
        mark_record_processed(record_id)

    print("\nIngestion + filtering + processing pipeline finished.")


if __name__ == "__main__":
    main()
