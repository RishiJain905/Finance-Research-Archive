import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent


def run_step(command: list[str], step_name: str) -> None:
    print(f"\n=== Running {step_name} ===\n")

    result = subprocess.run(command, cwd=BASE_DIR, text=True, capture_output=True)

    if result.stdout:
        print(result.stdout)

    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"{step_name} failed with exit code {result.returncode}")


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python scripts/process_record.py <record_id>")

    record_id = sys.argv[1]
    python_cmd = sys.executable

    run_step([python_cmd, "-m", "scripts.run_summarizer", record_id], "summarizer")

    run_step([python_cmd, "-m", "scripts.run_verifier", record_id], "verifier")

    run_step([python_cmd, "-m", "scripts.route_record", record_id], "router")

    # Step 4: Linker (only if record was accepted)
    accepted_path = BASE_DIR / "data" / "accepted" / f"{record_id}.json"
    if accepted_path.exists():
        run_step([python_cmd, "-m", "scripts.link_new_record", record_id], "linker")
    else:
        print("\n=== Skipping linker (record not accepted) ===")

    print(f"\nPipeline finished for record: {record_id}")


if __name__ == "__main__":
    main()
