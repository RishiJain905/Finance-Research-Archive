"""Weekly pipeline health report generator.

Scans data/accepted, data/rejected, and data/review_queue for records
created within the configured lookback window, aggregates statistics
per source, joins with the source_health SQLite table for consecutive
empty-run counters, and writes docs/pipeline_health.md.

Optionally sends a summary Telegram message when notify_telegram_on_stale
is enabled in config/health_config.json.

Run directly or via the pipeline-health.yml GitHub Actions workflow.
"""

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.manifest_db import ensure_schema, get_all_source_health

HEALTH_CONFIG_PATH = BASE_DIR / "config" / "health_config.json"
ACCEPTED_DIR = BASE_DIR / "data" / "accepted"
REJECTED_DIR = BASE_DIR / "data" / "rejected"
REVIEW_DIR = BASE_DIR / "data" / "review_queue"
REPORT_PATH = BASE_DIR / "docs" / "pipeline_health.md"

QUERY_PERF_PATH = BASE_DIR / "data" / "candidate_manifests" / "query_performance.json"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def load_health_config() -> dict:
    if HEALTH_CONFIG_PATH.exists():
        with HEALTH_CONFIG_PATH.open(encoding="utf-8") as f:
            return json.load(f)
    return {
        "auto_disable_after_empty_runs": 10,
        "stale_source_warning_days": 14,
        "report_lookback_days": 30,
        "notify_telegram_on_stale": True,
    }


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------


def _parse_timestamp(ts: str) -> datetime | None:
    """Parse an ISO-8601 timestamp string, returning a timezone-aware datetime."""
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _load_records_from_dir(
    directory: Path,
    cutoff: datetime,
    status_label: str,
) -> list[dict]:
    """Load all JSON records in *directory* created after *cutoff*.

    Returns a list of dicts with keys: source_name, created_at, status.
    """
    records = []
    if not directory.exists():
        return records

    for path in directory.glob("*.json"):
        try:
            with path.open(encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue

        created_at = _parse_timestamp(
            data.get("created_at") or data.get("processed_at") or ""
        )
        if created_at is None or created_at < cutoff:
            continue

        source_block = data.get("source", {})
        source_name = ""
        if isinstance(source_block, dict):
            source_name = source_block.get("name", "")
        if not source_name:
            source_name = data.get("source_name", data.get("target_name", "unknown"))

        records.append(
            {
                "source_name": source_name,
                "created_at": created_at,
                "status": status_label,
            }
        )

    return records


def collect_record_stats(lookback_days: int) -> dict[str, dict]:
    """Aggregate per-source counts over the last *lookback_days* days.

    Returns a dict keyed by source_name with sub-keys:
        accepted, rejected, in_review, last_accepted_at
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    all_records = (
        _load_records_from_dir(ACCEPTED_DIR, cutoff, "accepted")
        + _load_records_from_dir(REJECTED_DIR, cutoff, "rejected")
        + _load_records_from_dir(REVIEW_DIR, cutoff, "in_review")
    )

    stats: dict[str, dict] = defaultdict(
        lambda: {
            "accepted": 0,
            "rejected": 0,
            "in_review": 0,
            "last_accepted_at": None,
        }
    )

    for rec in all_records:
        name = rec["source_name"]
        status = rec["status"]
        stats[name][status] += 1
        if status == "accepted":
            prev = stats[name]["last_accepted_at"]
            if prev is None or rec["created_at"] > prev:
                stats[name]["last_accepted_at"] = rec["created_at"]

    return dict(stats)


def load_source_health_index() -> dict[str, dict]:
    """Return source_health rows keyed by source_name."""
    ensure_schema()
    rows = get_all_source_health()
    return {row["source_name"]: row for row in rows}


def load_query_performance() -> dict:
    """Load keyword lane performance data if available."""
    if QUERY_PERF_PATH.exists():
        try:
            with QUERY_PERF_PATH.open(encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            pass
    return {}


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def _format_last_seen(dt: datetime | None, stale_days: int) -> str:
    if dt is None:
        return "never"
    date_str = dt.strftime("%Y-%m-%d")
    age = (datetime.now(timezone.utc) - dt).days
    if age >= stale_days:
        return f"{date_str} ← STALE ({age}d ago)"
    return date_str


def build_report(
    record_stats: dict[str, dict],
    health_index: dict[str, dict],
    lookback_days: int,
    stale_days: int,
    query_perf: dict,
) -> str:
    now = datetime.now(timezone.utc)
    generated_at = now.strftime("%Y-%m-%d %H:%M UTC")
    window_start = (now - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    lines: list[str] = []

    # ---- Header -------------------------------------------------------
    lines.append("# Pipeline Health Report")
    lines.append("")
    lines.append(
        f"Generated: {generated_at}  |  Lookback window: {window_start} → today ({lookback_days} days)"
    )
    lines.append("")

    # ---- Per-source stats table ---------------------------------------
    lines.append("## Per-Source Statistics")
    lines.append("")

    # Combine sources from record_stats and health_index
    all_source_names = sorted(
        set(record_stats.keys()) | set(health_index.keys())
    )

    col_src = max((len(n) for n in all_source_names), default=20)
    col_src = max(col_src, 20)

    header = (
        f"| {'Source':<{col_src}} | Raw fetched | Passed filter | "
        f"Accepted | Rejected | In review | Consecutive empty | Last new record |"
    )
    sep = (
        f"| {'-' * col_src} | ----------- | ------------- | "
        f"-------- | -------- | --------- | ----------------- | --------------- |"
    )
    lines.append(header)
    lines.append(sep)

    for name in all_source_names:
        s = record_stats.get(name, {"accepted": 0, "rejected": 0, "in_review": 0, "last_accepted_at": None})
        h = health_index.get(name, {})

        accepted = s["accepted"]
        rejected = s["rejected"]
        in_review = s["in_review"]
        passed_filter = accepted + rejected + in_review
        raw_fetched = int(h.get("total_raw_fetched", 0))
        empty_runs = int(h.get("consecutive_empty_runs", 0))
        last_new = _format_last_seen(s["last_accepted_at"], stale_days)

        # Mark auto-disabled sources
        if h.get("auto_disabled"):
            name_cell = f"{name} [AUTO-DISABLED]"
        else:
            name_cell = name

        row = (
            f"| {name_cell:<{col_src}} | {raw_fetched:>11} | {passed_filter:>13} | "
            f"{accepted:>8} | {rejected:>8} | {in_review:>9} | {empty_runs:>17} | {last_new:<15} |"
        )
        lines.append(row)

    lines.append("")

    # ---- Keyword lane stats (if available) ----------------------------
    if query_perf:
        lines.append("## Keyword Lane Statistics")
        lines.append("")
        lines.append("_(from data/candidate_manifests/query_performance.json)_")
        lines.append("")
        lines.append("| Query | Candidates | Accepted |")
        lines.append("| ----- | ---------- | -------- |")
        for query, perf in sorted(query_perf.items()):
            if isinstance(perf, dict):
                cands = perf.get("candidates", 0)
                acc = perf.get("accepted", 0)
                lines.append(f"| {query} | {cands} | {acc} |")
        lines.append("")

    # ---- Summary stats -----------------------------------------------
    total_accepted = sum(
        len(list(ACCEPTED_DIR.glob("*.json"))) if ACCEPTED_DIR.exists() else 0
        for _ in [None]
    )
    total_accepted = len(list(ACCEPTED_DIR.glob("*.json"))) if ACCEPTED_DIR.exists() else 0
    total_review = len(list(REVIEW_DIR.glob("*.json"))) if REVIEW_DIR.exists() else 0

    lines.append("## Archive Summary")
    lines.append("")
    lines.append(f"- **Total records in `data/accepted/`:** {total_accepted}")
    lines.append(f"- **Total records in `data/review_queue/`:** {total_review}")
    lines.append(f"- **Sources tracked this window:** {len(all_source_names)}")
    lines.append("")

    # ---- Stale sources -----------------------------------------------
    stale_sources = [
        (name, s["last_accepted_at"])
        for name, s in record_stats.items()
        if s["last_accepted_at"] is not None
        and (now - s["last_accepted_at"]).days >= stale_days
    ]
    never_seen = [
        name
        for name in all_source_names
        if record_stats.get(name, {}).get("last_accepted_at") is None
        and name in record_stats
    ]

    lines.append("## Stale Sources")
    lines.append("")
    if stale_sources or never_seen:
        lines.append(
            f"Sources with no new accepted record in the last {stale_days}+ days:"
        )
        lines.append("")
        for name, last_dt in sorted(stale_sources, key=lambda x: x[1] or now):
            age = (now - last_dt).days if last_dt else "?"
            lines.append(f"- **{name}** — last accepted: {last_dt.strftime('%Y-%m-%d')} ({age} days ago)")
        for name in sorted(never_seen):
            lines.append(f"- **{name}** — no accepted records in lookback window")
    else:
        lines.append("_No stale sources detected._")
    lines.append("")

    # ---- Top 5 most productive sources --------------------------------
    top5 = sorted(
        record_stats.items(),
        key=lambda kv: kv[1]["accepted"],
        reverse=True,
    )[:5]

    lines.append("## Top 5 Most Productive Sources (this window)")
    lines.append("")
    if top5:
        for rank, (name, s) in enumerate(top5, 1):
            lines.append(f"{rank}. **{name}** — {s['accepted']} accepted records")
    else:
        lines.append("_No accepted records in this window._")
    lines.append("")

    # ---- Auto-disabled sources ----------------------------------------
    auto_disabled = [
        row for row in health_index.values() if row.get("auto_disabled")
    ]

    lines.append("## Auto-Disabled Sources")
    lines.append("")
    if auto_disabled:
        lines.append(
            "These sources produced zero raw records for too many consecutive runs "
            "and have been automatically disabled."
        )
        lines.append("")
        for row in sorted(auto_disabled, key=lambda r: r["source_name"]):
            disabled_at = row.get("disabled_at", "unknown date")
            if disabled_at and disabled_at != "unknown date":
                try:
                    disabled_at = datetime.fromisoformat(disabled_at).strftime("%Y-%m-%d")
                except ValueError:
                    pass
            lines.append(f"- **{row['source_name']}** — disabled {disabled_at}")
        lines.append("")
        lines.append(
            "**To re-enable:** Set `\"auto_disabled\": false` in the relevant config "
            "file (e.g. `config/ingestion_targets.json`, `config/rss_feeds.json`, "
            "or `config/edgar_sources.json`) **and** reset the counter by running:"
        )
        lines.append("")
        lines.append("```python")
        lines.append("from scripts.manifest_db import reset_source_auto_disabled")
        lines.append('reset_source_auto_disabled("Source Name Here")')
        lines.append("```")
    else:
        lines.append("_No auto-disabled sources._")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Telegram notification
# ---------------------------------------------------------------------------


def send_telegram_summary(
    report_stats: dict[str, dict],
    health_index: dict[str, dict],
    stale_days: int,
) -> None:
    """Send a concise Telegram summary using existing bot credentials."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("[health] Telegram credentials missing — skipping notification.")
        return

    import requests as req_lib

    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    new_this_week = sum(
        s["accepted"]
        for s in report_stats.values()
        if s.get("last_accepted_at") and s["last_accepted_at"] >= week_ago
    )
    stale_count = sum(
        1
        for s in report_stats.values()
        if s.get("last_accepted_at")
        and (now - s["last_accepted_at"]).days >= stale_days
    )
    newly_disabled = [
        row["source_name"]
        for row in health_index.values()
        if row.get("auto_disabled")
        and row.get("disabled_at")
        and _parse_timestamp(row["disabled_at"])
        and (now - _parse_timestamp(row["disabled_at"])).days < 7
    ]

    parts = [
        "📊 *Weekly Pipeline Health Report*",
        f"• New records (7d): {new_this_week}",
        f"• Stale sources (>{stale_days}d): {stale_count}",
    ]
    if newly_disabled:
        parts.append(f"• Newly auto-disabled: {', '.join(newly_disabled)}")
    else:
        parts.append("• No newly auto-disabled sources")

    message = "\n".join(parts)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = req_lib.post(
            url,
            json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"},
            timeout=15,
        )
        resp.raise_for_status()
        print("[health] Telegram summary sent.")
    except Exception as exc:
        print(f"[health] Telegram notification failed: {exc}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    cfg = load_health_config()
    lookback_days: int = cfg.get("report_lookback_days", 30)
    stale_days: int = cfg.get("stale_source_warning_days", 14)
    notify: bool = cfg.get("notify_telegram_on_stale", True)

    print(f"[health] Collecting record stats (lookback: {lookback_days} days)...")
    record_stats = collect_record_stats(lookback_days)
    print(f"[health] Found {len(record_stats)} sources in archive data.")

    print("[health] Loading source_health from manifest DB...")
    health_index = load_source_health_index()
    print(f"[health] {len(health_index)} source(s) tracked in SQLite.")

    query_perf = load_query_performance()

    report_text = build_report(
        record_stats, health_index, lookback_days, stale_days, query_perf
    )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(report_text, encoding="utf-8")
    print(f"[health] Report written to {REPORT_PATH.relative_to(BASE_DIR)}")

    if notify:
        send_telegram_summary(record_stats, health_index, stale_days)


if __name__ == "__main__":
    main()
