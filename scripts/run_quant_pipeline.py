import json
import subprocess
import sys
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
PROCESS_RECORD_TIMEOUT = 300  # 5 minutes per record


def _run_process_record(python_cmd: str, record_id: str) -> tuple[str, str]:
    """Run process_record for a single record.

    Returns (record_id, outcome) where outcome is one of:
    "accepted", "rejected", "review_queue", or "failed:<reason>".
    Never raises — all failure modes are captured as "failed:..." outcomes so
    that a single bad record cannot abort the remaining records or skip the
    send_pending_reviews step.
    """
    try:
        result = subprocess.run(
            [python_cmd, "scripts/process_record.py", record_id],
            cwd=BASE_DIR,
            text=True,
            capture_output=True,
            timeout=PROCESS_RECORD_TIMEOUT,
        )
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr)
        if result.returncode != 0:
            return record_id, f"failed:exit_code={result.returncode}"
        stdout = result.stdout or ""
        if "Moved record to accepted" in stdout:
            return record_id, "accepted"
        if "Moved record to rejected" in stdout:
            return record_id, "rejected"
        return record_id, "review_queue"
    except subprocess.TimeoutExpired:
        return record_id, "failed:timeout"
    except Exception as e:
        return record_id, f"failed:{e}"


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Run quant pipeline.")
    parser.add_argument(
        "--process-workers",
        type=int,
        default=2,
        help="Parallel workers for process_record stage (1 for sequential)",
    )
    args = parser.parse_args()

    python_cmd = sys.executable
    timings: dict[str, float] = {}
    pipeline_start = time.perf_counter()

    start = time.perf_counter()
    quant_output = run_command(
        [python_cmd, "scripts/ingest_quant_data.py"],
        "quant ingestion"
    )
    timings["ingest"] = time.perf_counter() - start

    record_ids = extract_created_ids(quant_output)

    if not record_ids:
        print("\nNo quant records were created.")
        return

    print("\nQuant records to process:")
    for record_id in record_ids:
        print(f"- {record_id}")

    start = time.perf_counter()
    workers = max(1, args.process_workers)
    routing_counts: dict[str, int] = {"accepted": 0, "rejected": 0, "review_queue": 0, "failed": 0}

    if workers == 1:
        for idx, record_id in enumerate(record_ids, 1):
            print(f"\n[{idx}/{len(record_ids)}] Processing {record_id}...")
            _, outcome = _run_process_record(python_cmd, record_id)
            if outcome.startswith("failed:"):
                routing_counts["failed"] += 1
                print(f"  [FAILED] {outcome.removeprefix('failed:')}")
            else:
                routing_counts[outcome] += 1
                print(f"  [OK] → {outcome}")
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_run_process_record, python_cmd, rid): rid
                for rid in record_ids
            }
            for future in as_completed(futures):
                rid, outcome = future.result()
                if outcome.startswith("failed:"):
                    routing_counts["failed"] += 1
                    print(f"  [FAILED] {rid}: {outcome.removeprefix('failed:')}")
                else:
                    routing_counts[outcome] += 1
                    print(f"  [OK] {rid} → {outcome}")
    timings["process"] = time.perf_counter() - start

    print(f"\nProcessing Results:")
    print(f"  Accepted   : {routing_counts['accepted']}")
    print(f"  Review     : {routing_counts['review_queue']}")
    print(f"  Rejected   : {routing_counts['rejected']}")
    if routing_counts["failed"]:
        print(f"  Failed     : {routing_counts['failed']}")

    send_command = [python_cmd, "scripts/send_pending_reviews.py", "--max-items", "8"]
    for record_id in record_ids:
        send_command.extend(["--record-id", record_id])

    start = time.perf_counter()
    run_command(send_command, "send pending reviews")
    timings["send_reviews"] = time.perf_counter() - start
    timings["total"] = time.perf_counter() - pipeline_start

    print("\nTiming summary:")
    for name, seconds in timings.items():
        print(f"  - {name}: {seconds:.1f}s")
    print("\nQuant pipeline finished.")


if __name__ == "__main__":
    main()