"""EDGAR Filings Pipeline — Orchestrator

Fetches fresh SEC EDGAR filings for configured companies, runs them
through the standard process_record pipeline, and commits results.
"""

import json
import subprocess
import sys
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.manifest_db import (
    get_all_record_map,
    get_url_for_record_id,
    is_url_seen,
    set_record_map,
    mark_url_processed,
)

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
    """Extract record IDs from JSON_OUTPUT_START...JSON_OUTPUT_END block."""
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


def process_record_id(record_id: str, python_cmd: str) -> tuple[str, bool, str]:
    """Run process_record for one ID and return (record_id, success, error_msg)."""
    result = subprocess.run(
        [python_cmd, "scripts/process_record.py", record_id],
        cwd=BASE_DIR,
        text=True,
        capture_output=True,
    )
    if result.returncode == 0:
        return record_id, True, ""
    message = result.stderr.strip() or result.stdout.strip() or "unknown error"
    return record_id, False, message


def mark_record_processed(record_id: str) -> None:
    """Remove the raw file after successful processing."""
    url = get_url_for_record_id(record_id)
    raw_path = BASE_DIR / "data" / "raw" / f"{record_id}.txt"
    if url:
        # No lane classification for EDGAR — just mark seen
        mark_url_processed(url, "article")
        print(f"\n  Marked URL as processed: {url}")
    if raw_path.exists():
        raw_path.unlink()
        print(f"  Removed raw file: {raw_path.name}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run SEC EDGAR filings ingest + process pipeline."
    )
    parser.add_argument(
        "--max-records",
        type=lambda x: int(float(x)),
        default=20,
        help="Maximum records to process (0 means no cap)",
    )
    parser.add_argument(
        "--process-workers",
        type=lambda x: int(float(x)),
        default=2,
        help="Parallel process_record workers",
    )
    args = parser.parse_args()

    python_cmd = sys.executable
    stage_start = time.perf_counter()
    timings: dict[str, float] = {}

    raw_ids_before_run = {f.stem for f in RAW_DIR.glob("*.txt")}

    # ── Step 1: Ingest EDGAR filings ──────────────────────────────────────────
    start = time.perf_counter()
    run_command([python_cmd, "scripts/ingest_edgar.py"], "EDGAR ingestion")
    timings["ingest"] = time.perf_counter() - start

    raw_ids_after_ingest = {f.stem for f in RAW_DIR.glob("*.txt")}
    new_record_ids = sorted(raw_ids_after_ingest - raw_ids_before_run)

    if not new_record_ids:
        print("\nNo new EDGAR filings found.")
        return

    print(f"\nFound {len(new_record_ids)} new filings:")
    for rid in new_record_ids:
        print(f"  - {rid}")

    # Cap to max_filings_per_run from config
    if args.max_records > 0 and len(new_record_ids) > args.max_records:
        new_record_ids = new_record_ids[: args.max_records]
        print(f"\nCapped to max-records={args.max_records}")

    # ── Step 2: Process each filing ─────────────────────────────────────────
    failed_ids = []
    processed_ids: list[str] = []
    process_start = time.perf_counter()

    workers = max(1, args.process_workers)
    if workers == 1:
        for record_id in new_record_ids:
            rid, ok, error = process_record_id(record_id, python_cmd)
            if ok:
                mark_record_processed(rid)
                processed_ids.append(rid)
            else:
                print(f"\n  Warning: processing failed for {rid}: {error}")
                failed_ids.append(rid)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(process_record_id, record_id, python_cmd): record_id
                for record_id in new_record_ids
            }
            for future in as_completed(futures):
                rid, ok, error = future.result()
                if ok:
                    mark_record_processed(rid)
                    processed_ids.append(rid)
                else:
                    print(f"\n  Warning: processing failed for {rid}: {error}")
                    failed_ids.append(rid)

    timings["process"] = time.perf_counter() - process_start

    if failed_ids:
        print(f"\n{len(failed_ids)} record(s) failed to process:")
        for rid in failed_ids:
            print(f"  - {rid}")

    timings["total"] = time.perf_counter() - stage_start
    print("\nTiming summary:")
    for name, seconds in timings.items():
        print(f"  - {name}: {seconds:.1f}s")
    print("\nEDGAR ingest + process pipeline finished.")


if __name__ == "__main__":
    main()
