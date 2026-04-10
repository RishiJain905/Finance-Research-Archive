import json
import subprocess
import sys
from pathlib import Path
from typing import Optional


BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

TRIAGE_DIR = BASE_DIR / "data" / "triage"


def run_step(command: list[str], step_name: str) -> None:
    print(f"\n=== Running {step_name} ===\n")

    result = subprocess.run(command, cwd=BASE_DIR, text=True, capture_output=True)

    if result.stdout:
        print(result.stdout)

    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"{step_name} failed with exit code {result.returncode}")


def persist_triage_metadata(record_id: str, triage_data: dict) -> Path:
    """Persist triage metadata for a record.

    Args:
        record_id: The record ID
        triage_data: Triage metadata dict with priority_score, priority_band, lane, reasons

    Returns:
        Path where the metadata was saved
    """
    TRIAGE_DIR.mkdir(parents=True, exist_ok=True)
    save_path = TRIAGE_DIR / f"{record_id}_triage.json"

    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(triage_data, f, indent=2, ensure_ascii=False)

    return save_path


def load_triage_metadata_for_record(record_id: str) -> Optional[dict]:
    """Load triage metadata for a record.

    Args:
        record_id: The record ID

    Returns:
        Triage metadata dict or None if not found
    """
    triage_path = TRIAGE_DIR / f"{record_id}_triage.json"
    if not triage_path.exists():
        return None

    try:
        with open(triage_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python scripts/process_record.py <record_id>")

    record_id = sys.argv[1]
    python_cmd = sys.executable

    run_step([python_cmd, "-m", "scripts.run_summarizer", record_id], "summarizer")

    run_step([python_cmd, "-m", "scripts.run_verifier", record_id], "verifier")

    run_step([python_cmd, "-m", "scripts.route_record", record_id], "router")

    # Step 3.5: Watchlist matching (V2.7 Part 3)
    # Match accepted/review records against watchlists after routing
    try:
        from scripts.watchlist_matcher import (
            match_record_against_watchlists,
            load_watchlists,
        )
        from scripts.watchlist_hit_persistence import save_watchlist_hit

        # Load record from either accepted or review_queue
        record_path_accepted = BASE_DIR / "data" / "accepted" / f"{record_id}.json"
        record_path_review = BASE_DIR / "data" / "review_queue" / f"{record_id}.json"

        match_record = None
        if record_path_accepted.exists():
            with open(record_path_accepted) as f:
                match_record = json.load(f)
        elif record_path_review.exists():
            with open(record_path_review) as f:
                match_record = json.load(f)

        if match_record:
            watchlists = load_watchlists(
                str(BASE_DIR / "config" / "watchlists_v27.json")
            )
            hits = match_record_against_watchlists(match_record, watchlists)
            for hit in hits:
                save_watchlist_hit(hit, str(BASE_DIR / "data" / "watchlist_hits"))
            print(f"Watchlist matching: {len(hits)} hits found")
    except Exception as e:
        print(f"Warning: Watchlist matching step skipped: {e}")

    # Step 4: Linker (only if record was accepted)
    accepted_path = BASE_DIR / "data" / "accepted" / f"{record_id}.json"
    if accepted_path.exists():
        run_step([python_cmd, "-m", "scripts.link_new_record", record_id], "linker")

        # V2.7 Part 2: Event Clustering - cluster newly accepted records
        try:
            from scripts.cluster_records import cluster_all_records

            cluster_all_records()
        except Exception as e:
            print(f"Warning: Clustering step skipped: {e}")
    else:
        print("\n=== Skipping linker (record not accepted) ===")

    print(f"\nPipeline finished for record: {record_id}")


if __name__ == "__main__":
    main()
