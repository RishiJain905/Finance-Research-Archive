import hashlib
import json
import subprocess
import sys
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.manifest_db import (
    get_all_record_map,
    get_all_processed_article_urls,
    get_url_for_record_id,
    is_url_processed_as_listing,
    mark_url_processed,
    is_url_seen,
    ensure_schema,
    upsert_seen_url,
    set_record_map,
    set_record_rules,
)
from scripts.ingest_sources import (
    fetch_html,
    extract_main_text,
    content_hash,
    sanitize_title,
    extract_title,
    classify_page_type,
    detect_language,
    extract_published_at,
    extract_canonical_url,
)

RAW_DIR = BASE_DIR / "data" / "raw"
QUEUE_PATH = BASE_DIR / "data" / "inbox_queue.json"


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


def mark_record_processed(record_id: str) -> None:
    url = get_url_for_record_id(record_id)
    raw_path = BASE_DIR / "data" / "raw" / f"{record_id}.txt"

    if url:
        is_listing = False
        if raw_path.exists():
            try:
                raw_content = raw_path.read_text(encoding="utf-8", errors="replace")
                if "container_page" in raw_content:
                    is_listing = True
            except OSError:
                pass

        if is_listing:
            mark_url_processed(url, "listing")
            print(f"\n  Marked URL as listing page (will re-fetch): {url}")
        else:
            mark_url_processed(url, "article")
            print(f"\n  Marked URL as processed: {url}")

    if raw_path.exists():
        raw_path.unlink()
        print(f"  Removed raw file: {raw_path.name}")


def verify_manifest_consistency() -> None:
    record_map = get_all_record_map()
    processed_article_urls = get_all_processed_article_urls()
    issues = []

    for url, record_id in record_map.items():
        if not is_url_seen(url):
            issues.append(
                f"record_map has record '{record_id}' but URL not in seen_urls: {url}"
            )

    for url in processed_article_urls:
        if url not in record_map:
            issues.append(f"processed_urls has URL not in record_map: {url}")

    orphaned_raw_files = []
    if RAW_DIR.exists():
        record_ids_in_manifest = set(record_map.values())
        for raw_file in RAW_DIR.glob("*.txt"):
            record_id = raw_file.stem
            if record_id not in record_ids_in_manifest:
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
        key=lambda rid: (
            (RAW_DIR / f"{rid}.txt").stat().st_mtime
            if (RAW_DIR / f"{rid}.txt").exists()
            else 0
        ),
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


def drain_inbox_queue() -> list[str]:
    """Process URLs from the inbox queue and create raw records for each.

    Returns:
        List of created record IDs
    """
    if not QUEUE_PATH.exists():
        return []

    try:
        queue = json.loads(QUEUE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []

    if not queue:
        return []

    print(f"\n=== Draining inbox queue ({len(queue)} URL(s)) ===\n")
    created: list[str] = []
    failed_urls: list[str] = []

    for item in queue:
        url = item.get("url")
        if not url:
            continue

        print(f"  Fetching: {url}")
        try:
            article_html = fetch_html(url)
        except Exception as e:
            print(f"    Failed fetch: {e}")
            failed_urls.append(url)
            continue

        article_text, extraction_warnings = extract_main_text(article_html)
        digest = content_hash(article_text)

        soup = BeautifulSoup(article_html, "html.parser")
        article_title = sanitize_title(extract_title(soup))
        published_at = extract_published_at(soup)
        canonical_url = extract_canonical_url(soup, url)
        detected_language = detect_language(article_text)
        page_type = classify_page_type(url, article_title, article_text, published_at)

        target_name = "telegram_inbox"
        topic = "manual_inbox"
        record_id = (
            f"telegram_inbox_{hashlib.sha1(url.encode('utf-8')).hexdigest()[:12]}"
        )

        from scripts.ingest_sources import build_raw_record_text

        output_text = build_raw_record_text(
            {
                "TARGET": target_name,
                "TOPIC": topic,
                "TITLE": article_title,
                "URL": canonical_url or url,
                "PAGE_TITLE": article_title,
                "PUBLISHED_AT": published_at or "",
                "PAGE_TYPE": page_type,
                "EXPECTED_LANGUAGE": "en",
                "DETECTED_LANGUAGE": detected_language,
                "CONTENT_WORD_COUNT": str(len(article_text.split())),
                "EXTRACTION_WARNINGS": ",".join(sorted(set(extraction_warnings))),
                "INGEST_SOURCE": "telegram_inbox",
                "SOURCE_TYPE": "web",
            },
            article_text,
        )

        output_path = RAW_DIR / f"{record_id}.txt"
        try:
            output_path.write_text(output_text, encoding="utf-8")
        except Exception as e:
            print(f"    Failed write: {e}")
            failed_urls.append(url)
            continue

        ensure_schema()
        upsert_seen_url(url, digest)
        set_record_map(url, record_id)
        set_record_rules(
            record_id,
            {
                "required_keywords": [],
                "blocked_keywords": [],
                "min_word_count": 120,
                "expected_language": "en",
                "allowed_page_types": [
                    "article",
                    "press_release",
                    "speech",
                    "data_release",
                    "market_notice",
                ],
                "target_name": target_name,
                "topic": topic,
            },
        )

        created.append(record_id)
        print(f"    Created: {record_id}")

    # Keep failed URLs in the queue for retry; remove successfully processed ones
    failed_set = set(failed_urls)
    failed = [item for item in queue if item.get("url") in failed_set]
    if created:
        # created contains record_ids, not URLs — collect the actual URLs we processed
        processed_urls = {item.get("url") for item in queue if item.get("url") and item.get("url") not in failed_set}
        remaining_queue = [item for item in queue if item.get("url") not in processed_urls]
        QUEUE_PATH.write_text(json.dumps(remaining_queue), encoding="utf-8")
        print(f"\n  Processed {len(created)} queue item(s), {len(failed)} failed URLs retained")

    return created


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
        "--fetch-workers",
        type=lambda x: int(float(x)),
        default=5,
        help="Parallel HTTP fetch workers for ingest_sources (1 for sequential)",
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
    run_command(
        [python_cmd, "scripts/ingest_sources.py", "--fetch-workers", str(args.fetch_workers)],
        "ingestion",
    )
    run_command([python_cmd, "scripts/ingest_rss.py"], "RSS ingestion")
    run_command([python_cmd, "scripts/ingest_arxiv.py"], "arXiv ingestion")
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
        # Drain inbox queue even when no crawl records are selected
        # so Telegram URLs are not left unprocessed
        queue_created = drain_inbox_queue()
        if queue_created:
            print(f"Drained {len(queue_created)} URL(s) from inbox queue")
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

    start = time.perf_counter()
    queue_created = drain_inbox_queue()
    if queue_created:
        timings["queue_drain"] = time.perf_counter() - start
        print(f"\nDrained {len(queue_created)} URL(s) from inbox queue")

    timings["total"] = time.perf_counter() - stage_start
    print("\nTiming summary:")
    for name, seconds in timings.items():
        print(f"  - {name}: {seconds:.1f}s")
    print("\nIngestion + filtering + processing pipeline finished.")


if __name__ == "__main__":
    main()
