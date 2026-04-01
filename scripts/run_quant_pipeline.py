import json
import subprocess
import sys
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    if workers == 1:
        for record_id in record_ids:
            run_command(
                [python_cmd, "scripts/process_record.py", record_id],
                f"process_record ({record_id})"
            )
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    subprocess.run,
                    [python_cmd, "scripts/process_record.py", record_id],
                    cwd=BASE_DIR,
                    text=True,
                    capture_output=True,
                ): record_id
                for record_id in record_ids
            }
            for future in as_completed(futures):
                rid = futures[future]
                result = future.result()
                if result.returncode != 0:
                    raise RuntimeError(
                        f"process_record ({rid}) failed with exit code {result.returncode}"
                    )
                if result.stdout:
                    print(result.stdout)
                if result.stderr:
                    print(result.stderr)
    timings["process"] = time.perf_counter() - start

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