import json
import shutil
import subprocess
import sys
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        candidates = set(raw_ids_after_filter)
    else:
        candidates = set(raw_ids_after_filter - raw_ids_before_run)

    # Priority-first: newest raw files first.
    ranked = sorted(candidates)
    ranked = sorted(
        ranked,
        key=lambda rid: (RAW_DIR / f"{rid}.txt").stat().st_mtime
        if (RAW_DIR / f"{rid}.txt").exists()
        else 0,
        reverse=True,
    )

    if max_records > 0:
        return ranked[:max_records]
    return ranked


def process_record_id(record_id: str, python_cmd: str) -> tuple[str, bool, str]:
    """Run process_record for one ID and return success + message."""
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


def create_candidates_from_raw_records(
    record_ids: list[str], lane: str = "trusted_sources"
) -> list[dict]:
    """Convert raw records to candidates for triage.

    Args:
        record_ids: List of raw record IDs to convert
        lane: Discovery lane (trusted_sources, keyword_discovery, seed_crawl)

    Returns:
        List of candidate dicts
    """
    from scripts.convert_raw_to_candidate import convert_batch_raw_to_candidates

    return convert_batch_raw_to_candidates(record_ids, lane)


def run_triage_on_candidates(
    candidates: list[dict], lane: str = "trusted_sources"
) -> tuple[list[dict], list[dict], list[dict]]:
    """Run triage engine on candidates.

    Args:
        candidates: List of candidate dicts
        lane: Lane name for budget config selection

    Returns:
        Tuple of (process_now, defer, discard) lists
    """
    from scripts.triage_engine import run_triage, load_weights, load_budget_config
    from scripts.triage_budget_gate import apply_budget_gate

    if not candidates:
        return [], [], []

    weights = load_weights()
    bands = weights.get("bands", {})
    budget_config = load_budget_config()

    weights_dict = weights.get("weights", {})

    # Run triage
    process_now, defer, discard = run_triage(
        candidates, weights_dict, bands, budget_config
    )

    return process_now, defer, discard


def apply_budget_gate_to_triage_results(
    process_list: list[dict],
    defer_list: list[dict],
    discard_list: list[dict],
    lane: str = "trusted_sources",
) -> tuple[list[dict], list[dict], list[dict]]:
    """Apply budget gate to triage results.

    Args:
        process_list: Candidates to process
        defer_list: Deferred candidates
        discard_list: Discarded candidates
        lane: Lane for budget limit selection

    Returns:
        Tuple of (process, defer, discard) after budget applied
    """
    from scripts.triage_budget_gate import apply_budget_gate
    from scripts.triage_engine import load_budget_config

    budget_config = load_budget_config()
    return apply_budget_gate(
        process_list, defer_list, discard_list, budget_config, lane
    )


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
        type=lambda x: int(float(x)),
        default=30,
        help="Maximum number of records to process in one run (0 means no cap)",
    )
    parser.add_argument(
        "--process-workers",
        type=lambda x: int(float(x)),
        default=2,
        help="Parallel process_record workers (1 for sequential)",
    )
    parser.add_argument(
        "--skip-send-reviews",
        action="store_true",
        help="Process records but skip Telegram send step",
    )
    parser.add_argument(
        "--send-review-max-items",
        type=lambda x: int(float(x)),
        default=8,
        help="Maximum Telegram reviews to send after processing",
    )
    parser.add_argument(
        "--send-review-daily-budget",
        type=lambda x: int(float(x)),
        default=20,
        help="Daily Telegram review budget for send step (0 means unlimited)",
    )
    args = parser.parse_args()

    python_cmd = sys.executable
    stage_start = time.perf_counter()
    timings: dict[str, float] = {}

    print("\n=== Verifying manifest consistency ===")
    verify_manifest_consistency()

    raw_ids_before_run = {f.stem for f in RAW_DIR.glob("*.txt")}

    start = time.perf_counter()
    run_command([python_cmd, "scripts/ingest_sources.py"], "ingestion")
    run_command([python_cmd, "scripts/ingest_rss.py"], "RSS ingestion")
    timings["ingest"] = time.perf_counter() - start

    start = time.perf_counter()
    run_command([python_cmd, "scripts/filter_raw_records.py"], "raw record filtering")
    timings["filter"] = time.perf_counter() - start

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
    process_start = time.perf_counter()
    workers = max(1, args.process_workers)
    if workers == 1:
        for record_id in records_to_process:
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
                for record_id in records_to_process
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

    if processed_ids and not args.skip_send_reviews:
        send_command = [
            python_cmd,
            "scripts/send_pending_reviews.py",
            "--max-items",
            str(args.send_review_max_items),
            "--daily-budget",
            str(args.send_review_daily_budget),
        ]
        for record_id in processed_ids:
            send_command.extend(["--record-id", record_id])
        send_start = time.perf_counter()
        run_command(send_command, "send pending reviews")
        timings["send_reviews"] = time.perf_counter() - send_start

    timings["total"] = time.perf_counter() - stage_start
    print("\nTiming summary:")
    for name, seconds in timings.items():
        print(f"  - {name}: {seconds:.1f}s")
    print("\nIngestion + filtering + processing pipeline finished.")


if __name__ == "__main__":
    main()
