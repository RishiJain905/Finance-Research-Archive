"""
V2.7 Part 5: Source Performance Analytics and Adaptive Control.

Computes per-source statistics from triage pipeline records:
- Loads JSON records from accepted, review_queue, rejected directories
- Parses TXT header metadata from filtered_out directory
- Groups records by source domain
- Computes ratios, averages, and timestamps per source
- Saves stats files to data/source_analytics/
- Generates summary report to stdout
"""

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from collections import defaultdict

BASE_DIR = Path(__file__).resolve().parent.parent
ACCEPTED_DIR = BASE_DIR / "data" / "accepted"
REVIEW_DIR = BASE_DIR / "data" / "review_queue"
REJECTED_DIR = BASE_DIR / "data" / "rejected"
FILTERED_DIR = BASE_DIR / "data" / "filtered_out"
OUTPUT_DIR = BASE_DIR / "data" / "source_analytics"


def load_json(path: Path, default=None):
    """Load JSON file with fallback to default.

    Args:
        path: Path to JSON file
        default: Default value if file doesn't exist

    Returns:
        Parsed JSON data or default
    """
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data) -> None:
    """Save data to JSON file.

    Args:
        path: Path to output file
        data: Data to serialize
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# =============================================================================
# B1: Record Loading Functions
# =============================================================================


def load_json_records(directory: Path) -> list[dict]:
    """Load all JSON files from a directory.

    Returns list of parsed record dicts.
    Skips .gitkeep and non-JSON files.

    Args:
        directory: Path to directory containing JSON records

    Returns:
        List of parsed record dictionaries
    """
    if not directory.exists():
        return []

    records = []
    for json_file in directory.glob("*.json"):
        if json_file.name == ".gitkeep":
            continue
        try:
            record = load_json(json_file)
            if record is not None:
                records.append(record)
        except Exception as e:
            print(f"Warning: Failed to load {json_file}: {e}")
            continue

    return records


def load_filtered_records(directory: Path) -> list[dict]:
    """Parse TXT files with header metadata from filtered_out directory.

    Returns list of dicts with keys: source_name, source_domain, source_url, raw_path
    Parse headers: TARGET -> source_name, URL -> source_url, extract domain from URL.
    Skip files that can't be parsed.

    Args:
        directory: Path to filtered_out directory containing TXT files

    Returns:
        List of parsed filtered record dictionaries
    """
    if not directory.exists():
        return []

    filtered = []
    for txt_file in directory.glob("*.txt"):
        if txt_file.name == ".gitkeep":
            continue
        try:
            record = _parse_filtered_txt(txt_file)
            if record is not None:
                filtered.append(record)
        except Exception as e:
            print(f"Warning: Failed to parse {txt_file}: {e}")
            continue

    return filtered


def _parse_filtered_txt(txt_file: Path) -> dict | None:
    """Parse a single filtered TXT file.

    Reads lines until blank line, splits on first ': ' to get key-value pairs.
    TARGET -> source_name, URL -> source_url. Domain extracted from URL.

    Args:
        txt_file: Path to TXT file

    Returns:
        Dict with source_name, source_domain, source_url, raw_path or None
    """
    source_name = ""
    source_url = ""

    with txt_file.open("r", encoding="utf-8") as f:
        lines = []
        for line in f:
            line = line.rstrip("\n")
            if not line:
                break
            lines.append(line)

    for line in lines:
        if ": " not in line:
            continue
        idx = line.index(": ")
        key = line[:idx].strip()
        value = line[idx + 2 :].strip()

        if key == "TARGET":
            source_name = value
        elif key == "URL":
            source_url = value

    if not source_name and not source_url:
        return None

    # Extract domain from URL
    source_domain = _extract_domain(source_url)

    return {
        "source_name": source_name,
        "source_domain": source_domain,
        "source_url": source_url,
        "raw_path": str(txt_file),
    }


def _extract_domain(url: str) -> str:
    """Extract domain from URL.

    Uses urlparse to extract netloc, then strips www. prefix.

    Args:
        url: URL string

    Returns:
        Domain string (lowercase, without www. prefix)
    """
    if not url:
        return ""

    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        # Fallback: try to extract domain manually
        if "://" in url:
            domain = url.split("://")[1].split("/")[0]
            if domain.startswith("www."):
                domain = domain[4:]
            return domain.lower()
        return ""


def _normalize_domain(domain: str) -> str:
    """Normalize domain for consistent grouping.

    Lowercases and strips www. prefix.

    Args:
        domain: Domain string

    Returns:
        Normalized domain string
    """
    if not domain:
        return ""
    domain = domain.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def extract_source_from_record(record: dict) -> dict:
    """Extract source info from a JSON record.

    Returns dict with: name, domain, url, source_type
    Use record.get("source", {}) with safe defaults.
    Domain is primary key - extract from source.domain or parse from source.url.
    Domain is normalized (lowercase, www. stripped).

    Args:
        record: JSON record dictionary

    Returns:
        Dict with name, domain, url, source_type
    """
    source = record.get("source", {})

    name = source.get("name", "")
    url = source.get("url", "")
    source_type = source.get("source_type", "")

    # Get domain - prefer explicit field, else parse from URL
    domain = source.get("domain", "")
    if not domain:
        domain = _extract_domain(url)

    # Normalize domain (lowercase, strip www.)
    domain = _normalize_domain(domain)

    return {
        "name": name,
        "domain": domain,
        "url": url,
        "source_type": source_type,
    }


def extract_source_from_filtered(filtered: dict) -> dict:
    """Extract source info from a filtered-out record dict.

    Returns dict with: name, domain, url, source_type="filtered"
    Domain extracted from source_url and normalized.

    Args:
        filtered: Filtered record dictionary

    Returns:
        Dict with name, domain, url, source_type="filtered"
    """
    raw_domain = filtered.get("source_domain", "")
    return {
        "name": filtered.get("source_name", ""),
        "domain": _normalize_domain(raw_domain),
        "url": filtered.get("source_url", ""),
        "source_type": "filtered",
    }


# =============================================================================
# B2: Stats Computation Functions
# =============================================================================


def compute_ratios(counts: dict) -> dict:
    """Calculate accepted/review/rejected/filtered ratios.

    Input: {"accepted": N, "review": N, "rejected": N, "filtered": N}
    Output: {"accepted_ratio": 0.0-1.0, "review_ratio": ..., "rejected_ratio": ..., "filtered_ratio": ...}
    Ratios = count / total. If total is 0, all ratios are 0.0.

    Args:
        counts: Dictionary with counts for each bucket

    Returns:
        Dictionary with ratio values
    """
    total = sum(counts.values())

    if total == 0:
        return {
            "accepted_ratio": 0.0,
            "review_ratio": 0.0,
            "rejected_ratio": 0.0,
            "filtered_ratio": 0.0,
        }

    return {
        "accepted_ratio": counts.get("accepted", 0) / total,
        "review_ratio": counts.get("review", 0) / total,
        "rejected_ratio": counts.get("rejected", 0) / total,
        "filtered_ratio": counts.get("filtered", 0) / total,
    }


def compute_avg_priority_score(records: list[dict]) -> float:
    """Average triage priority score from records.

    Look for priority_score in record root or in triage metadata.
    Return 0.0 if no scores found.

    Args:
        records: List of record dictionaries

    Returns:
        Average priority score or 0.0
    """
    scores = []

    for record in records:
        # Check at record root
        score = record.get("priority_score")
        if score is not None:
            try:
                scores.append(float(score))
                continue
            except (ValueError, TypeError):
                pass

        # Check in triage metadata
        triage = record.get("triage", {})
        if triage:
            score = triage.get("priority_score")
            if score is not None:
                try:
                    scores.append(float(score))
                    continue
                except (ValueError, TypeError):
                    pass

    if not scores:
        return 0.0

    return sum(scores) / len(scores)


def compute_avg_verification_confidence(records: list[dict]) -> float:
    """Average llm_review.verification_confidence.

    Return 0.0 if no confidence values found.

    Args:
        records: List of record dictionaries

    Returns:
        Average verification confidence or 0.0
    """
    confidences = []

    for record in records:
        llm_review = record.get("llm_review", {})
        if llm_review:
            confidence = llm_review.get("verification_confidence")
            if confidence is not None:
                try:
                    confidences.append(float(confidence))
                except (ValueError, TypeError):
                    pass

    if not confidences:
        return 0.0

    return sum(confidences) / len(confidences)


def get_last_seen_at(records: list[dict]) -> str:
    """Most recent created_at timestamp as ISO-8601 string.

    Return empty string if no timestamps found.
    ISO-8601 timestamps sort lexicographically.

    Args:
        records: List of record dictionaries

    Returns:
        Most recent created_at timestamp or empty string
    """
    timestamps = []

    for record in records:
        created_at = record.get("created_at")
        if created_at:
            timestamps.append(created_at)

    if not timestamps:
        return ""

    # ISO-8601 sorts lexicographically, so max() works directly
    return max(timestamps)


def compute_source_stats(
    accepted: list[dict],
    review: list[dict],
    rejected: list[dict],
    filtered: list[dict],
) -> dict[str, dict]:
    """Main stats computation.

    Groups all records by source domain.
    For each source, computes:
    - records_seen (total across all buckets)
    - accepted_count, review_count, rejected_count, filtered_out_count
    - accepted_ratio, review_ratio, rejected_ratio, filtered_ratio
    - avg_priority_score (from accepted + review records only)
    - avg_verification_confidence (from accepted + review records only)
    - last_seen_at (most recent created_at across all records)
    - source_name (most common name for this domain)
    Returns dict keyed by source_domain.

    Args:
        accepted: List of accepted record dicts
        review: List of review queue record dicts
        rejected: List of rejected record dicts
        filtered: List of filtered record dicts

    Returns:
        Dictionary keyed by source_domain with stats dictionaries
    """
    # Group records by domain
    # Each entry: {"records": [], "names": [], "accepted": [], "review": [], "rejected": [], "filtered": []}
    source_data: dict[str, dict] = defaultdict(
        lambda: {
            "records": [],
            "names": [],
            "accepted": [],
            "review": [],
            "rejected": [],
            "filtered": [],
        }
    )

    # Process accepted records
    for record in accepted:
        source = extract_source_from_record(record)
        domain = source["domain"]
        if not domain:
            domain = "unknown"

        source_data[domain]["records"].append(record)
        source_data[domain]["names"].append(source["name"])
        source_data[domain]["accepted"].append(record)

    # Process review records
    for record in review:
        source = extract_source_from_record(record)
        domain = source["domain"]
        if not domain:
            domain = "unknown"

        source_data[domain]["records"].append(record)
        source_data[domain]["names"].append(source["name"])
        source_data[domain]["review"].append(record)

    # Process rejected records
    for record in rejected:
        source = extract_source_from_record(record)
        domain = source["domain"]
        if not domain:
            domain = "unknown"

        source_data[domain]["records"].append(record)
        source_data[domain]["names"].append(source["name"])
        source_data[domain]["rejected"].append(record)

    # Process filtered records
    for record in filtered:
        source = extract_source_from_filtered(record)
        domain = source["domain"]
        if not domain:
            domain = "unknown"

        source_data[domain]["records"].append(record)
        source_data[domain]["names"].append(source["name"])
        source_data[domain]["filtered"].append(record)

    # Compute stats per source
    stats: dict[str, dict] = {}

    for domain, data in source_data.items():
        accepted_count = len(data["accepted"])
        review_count = len(data["review"])
        rejected_count = len(data["rejected"])
        filtered_count = len(data["filtered"])
        records_seen = len(data["records"])

        # Compute ratios
        counts = {
            "accepted": accepted_count,
            "review": review_count,
            "rejected": rejected_count,
            "filtered": filtered_count,
        }
        ratios = compute_ratios(counts)

        # Compute avg_priority_score from accepted + review only
        priority_records = data["accepted"] + data["review"]
        avg_priority_score = compute_avg_priority_score(priority_records)

        # Compute avg_verification_confidence from accepted + review only
        avg_verification_confidence = compute_avg_verification_confidence(
            priority_records
        )

        # Get last_seen_at (most recent created_at across all records)
        last_seen_at = get_last_seen_at(data["records"])

        # Get most common source name for this domain
        source_name = _get_most_common_name(data["names"])

        stats[domain] = {
            "source_name": source_name,
            "source_domain": domain,
            "records_seen": records_seen,
            "accepted_count": accepted_count,
            "review_count": review_count,
            "rejected_count": rejected_count,
            "filtered_out_count": filtered_count,
            "accepted_ratio": ratios["accepted_ratio"],
            "review_ratio": ratios["review_ratio"],
            "rejected_ratio": ratios["rejected_ratio"],
            "filtered_ratio": ratios["filtered_ratio"],
            "avg_priority_score": avg_priority_score,
            "avg_verification_confidence": avg_verification_confidence,
            "last_seen_at": last_seen_at,
        }

    return stats


def _get_most_common_name(names: list[str]) -> str:
    """Get the most frequently occurring name from a list.

    Args:
        names: List of source names

    Returns:
        Most common name or empty string
    """
    if not names:
        return ""

    name_counts: dict[str, int] = defaultdict(int)
    for name in names:
        if name:  # Only count non-empty names
            name_counts[name] += 1

    if not name_counts:
        return ""

    return max(name_counts.keys(), key=lambda n: name_counts[n])


# =============================================================================
# B3: Stats Persistence Functions
# =============================================================================


def sanitize_filename(name: str) -> str:
    """Sanitize a source name for use as a filename.

    Replaces /, :, ., and other unsafe characters with underscores.

    Args:
        name: Source name or domain

    Returns:
        Sanitized filename-safe string
    """
    return re.sub(r'[<>:"/\\|?*]', "_", name)


def generate_stats_filename(source_domain: str) -> str:
    """Generate deterministic filename for a source stats file.

    Args:
        source_domain: Normalized source domain

    Returns:
        Filename string (e.g., "federalreserve_gov.json")
    """
    safe = sanitize_filename(source_domain)
    return f"{safe}.json"


def save_source_stats(stats: dict[str, dict], output_dir: Path) -> None:
    """Save stats as one file per source.

    Args:
        stats: Dict keyed by source_domain with stats dictionaries
        output_dir: Directory to write stats files to
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    for domain, source_stats in stats.items():
        filename = generate_stats_filename(domain)
        filepath = output_dir / filename
        save_json(filepath, source_stats)


def load_source_stats(output_dir: Path) -> dict[str, dict]:
    """Load existing stats for comparison.

    Args:
        output_dir: Directory containing stats files

    Returns:
        Dict keyed by source_domain with stats dictionaries
    """
    if not output_dir.exists():
        return {}

    stats = {}
    for json_file in output_dir.glob("*.json"):
        if json_file.name == ".gitkeep":
            continue
        try:
            source_stats = load_json(json_file)
            if source_stats is not None:
                domain = source_stats.get("source_domain", json_file.stem)
                stats[domain] = source_stats
        except Exception as e:
            print(f"Warning: Failed to load {json_file}: {e}")
            continue

    return stats


# =============================================================================
# B4: Main Orchestration
# =============================================================================


def run_analytics(
    base_dir: Path | None = None,
    accepted_dir: Path | None = None,
    review_dir: Path | None = None,
    rejected_dir: Path | None = None,
    filtered_dir: Path | None = None,
    output_dir: Path | None = None,
    dry_run: bool = False,
) -> dict[str, dict]:
    """Main analytics entry point.

    Loads records from all directories, computes stats, saves to output.

    Args:
        base_dir: Base directory (defaults to BASE_DIR)
        accepted_dir: Override for accepted directory
        review_dir: Override for review queue directory
        rejected_dir: Override for rejected directory
        filtered_dir: Override for filtered out directory
        output_dir: Override for output directory
        dry_run: If True, compute but don't write files

    Returns:
        Dictionary of source stats keyed by domain
    """
    if base_dir is None:
        base_dir = BASE_DIR

    if accepted_dir is None:
        accepted_dir = base_dir / "data" / "accepted"
    if review_dir is None:
        review_dir = base_dir / "data" / "review_queue"
    if rejected_dir is None:
        rejected_dir = base_dir / "data" / "rejected"
    if filtered_dir is None:
        filtered_dir = base_dir / "data" / "filtered_out"
    if output_dir is None:
        output_dir = base_dir / "data" / "source_analytics"

    # Load records
    print("Loading accepted records...")
    accepted = load_json_records(accepted_dir)
    print(f"  Found {len(accepted)} accepted records")

    print("Loading review queue records...")
    review = load_json_records(review_dir)
    print(f"  Found {len(review)} review queue records")

    print("Loading rejected records...")
    rejected = load_json_records(rejected_dir)
    print(f"  Found {len(rejected)} rejected records")

    print("Loading filtered-out records...")
    filtered = load_filtered_records(filtered_dir)
    print(f"  Found {len(filtered)} filtered-out records")

    total = len(accepted) + len(review) + len(rejected) + len(filtered)
    print(f"\nTotal records: {total}")

    # Compute stats
    print("\nComputing source statistics...")
    stats = compute_source_stats(accepted, review, rejected, filtered)
    print(f"  Found {len(stats)} unique source domains")

    # Save stats
    if dry_run:
        print(f"\n[DRY RUN] Would save {len(stats)} stats files to: {output_dir}")
    else:
        print(f"\nSaving stats to: {output_dir}")
        save_source_stats(stats, output_dir)
        print(f"  Saved {len(stats)} stats files")

    # Print summary
    print("\n=== Source Performance Summary ===")
    print(
        f"{'Domain':<35} {'Records':>8} {'Accepted':>9} {'Review':>7} {'Rejected':>9} {'Filtered':>9} {'Acc Ratio':>10}"
    )
    print("-" * 100)

    # Sort by records_seen descending
    sorted_stats = sorted(stats.values(), key=lambda s: s["records_seen"], reverse=True)

    for s in sorted_stats:
        print(
            f"{s['source_domain']:<35} {s['records_seen']:>8} {s['accepted_count']:>9} "
            f"{s['review_count']:>7} {s['rejected_count']:>9} {s['filtered_out_count']:>9} "
            f"{s['accepted_ratio']:>9.1%}"
        )

    print(f"\n{'=' * 100}")
    print(f"Total sources: {len(stats)}")

    # Highlight top and bottom performers by accepted ratio (min 5 records)
    qualified = [s for s in sorted_stats if s["records_seen"] >= 5]
    if qualified:
        best = max(qualified, key=lambda s: s["accepted_ratio"])
        worst = min(qualified, key=lambda s: s["accepted_ratio"])
        print(
            f"\nBest performer (5+ records): {best['source_domain']} ({best['accepted_ratio']:.1%} accepted, {best['records_seen']} records)"
        )
        print(
            f"Worst performer (5+ records): {worst['source_domain']} ({worst['accepted_ratio']:.1%} accepted, {worst['records_seen']} records)"
        )

    return stats


def main():
    """CLI entry point for source analytics."""
    parser = argparse.ArgumentParser(description="V2.7 Source Performance Analytics")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=None,
        help="Base directory (default: script parent's parent)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview stats without writing files",
    )
    args = parser.parse_args()

    run_analytics(base_dir=args.base_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
