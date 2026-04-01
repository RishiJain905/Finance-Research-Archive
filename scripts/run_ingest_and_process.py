import json
import shutil
import subprocess
import sys
import argparse
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

    record_map = manifest.get("record_map", {})
    url = next((u for u, rid in record_map.items() if rid == record_id), None)
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

    for url, record_id in record_map.items():
        if url not in seen_urls:
            issues.append(
                f"record_map has record '{record_id}' but URL not in seen_urls: {url}"
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


def select_records_to_process(
    raw_ids_before_run: set[str],
    raw_ids_after_filter: set[str],
    include_backlog: bool,
    max_records: int,
) -> list[str]:
    """Select which raw records this run should process.

    By default we only process records created in this run to avoid
    cross-lane backlog explosions. Backlog processing can be enabled manually.
    """
    if include_backlog:
        candidates = sorted(raw_ids_after_filter)
    else:
        candidates = sorted(raw_ids_after_filter - raw_ids_before_run)

    if max_records > 0:
        return candidates[:max_records]
    return candidates


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run article ingest/filter/process pipeline with bounded scope."
    )
    parser.add_argument(
        "--include-backlog",
        action="store_true",
        help="Also process pre-existing raw backlog (default: current-run records only)",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=40,
        help="Maximum number of records to process in one run (0 means no cap)",
    )
    args = parser.parse_args()

    python_cmd = sys.executable

    print("\n=== Verifying manifest consistency ===")
    verify_manifest_consistency()

    raw_ids_before_run = {f.stem for f in RAW_DIR.glob("*.txt")}

    run_command([python_cmd, "scripts/ingest_sources.py"], "ingestion")
    run_command([python_cmd, "scripts/ingest_rss.py"], "RSS ingestion")

    run_command([python_cmd, "scripts/filter_raw_records.py"], "raw record filtering")

    raw_ids_after_filter = {f.stem for f in RAW_DIR.glob("*.txt")}
    records_to_process = select_records_to_process(
        raw_ids_before_run,
        raw_ids_after_filter,
        include_backlog=args.include_backlog,
        max_records=args.max_records,
    )

    if not records_to_process:
        if args.include_backlog:
            print("\nNo records to process in raw backlog.")
        else:
            print("\nNo newly-created records to process.")
        return

    if not args.include_backlog:
        newly_created_count = len(raw_ids_after_filter - raw_ids_before_run)
        print(
            f"\nProcessing only records created in this run: {len(records_to_process)}/{newly_created_count}"
        )

    if args.max_records > 0 and len(records_to_process) >= args.max_records:
        print(f"\nApplied max-records cap: {args.max_records}")

    print("\nRecords to process:")
    for record_id in records_to_process:
        print(f"- {record_id}")

    failed_ids = []
    processed_ids: list[str] = []
    for record_id in records_to_process:
        try:
            run_command(
                [python_cmd, "scripts/process_record.py", record_id],
                f"process_record ({record_id})",
            )
            mark_record_processed(record_id)
            processed_ids.append(record_id)
        except RuntimeError as e:
            print(f"\n  Warning: processing failed for {record_id}: {e}")
            failed_ids.append(record_id)

    if failed_ids:
        print(f"\n{len(failed_ids)} record(s) failed to process:")
        for rid in failed_ids:
            print(f"  - {rid}")

    if processed_ids:
        send_command = [
            python_cmd,
            "scripts/send_pending_reviews.py",
            "--max-items",
            "8",
        ]
        for record_id in processed_ids:
            send_command.extend(["--record-id", record_id])
        run_command(send_command, "send pending reviews")

    print("\nIngestion + filtering + processing pipeline finished.")


if __name__ == "__main__":
    main()
