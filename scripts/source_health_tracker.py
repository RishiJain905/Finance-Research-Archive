"""Per-run source health tracking for the Finance Research Archive.

Called by each ingest script after processing a source.  Records the raw
fetch count in the manifest database, maintains a consecutive_empty_runs
counter, and automatically sets ``"auto_disabled": true`` in the source's
config file when the threshold is exceeded.

Usage in an ingest script::

    from scripts.source_health_tracker import update as track_health, is_auto_disabled

    # Skip auto-disabled sources
    if source_config.get("auto_disabled", False) or is_auto_disabled(name):
        print(f"  {name}: auto-disabled, skipping.")
        continue

    # ... fetch and process ...

    track_health(name, len(created), config_path=CONFIG_PATH, config_list_key="targets")
"""

import json
import sys
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.manifest_db import (
    ensure_schema,
    get_source_health,
    mark_source_auto_disabled,
    upsert_source_run,
)

HEALTH_CONFIG_PATH = BASE_DIR / "config" / "health_config.json"


def _load_health_config() -> dict:
    if HEALTH_CONFIG_PATH.exists():
        with HEALTH_CONFIG_PATH.open(encoding="utf-8") as f:
            return json.load(f)
    return {
        "auto_disable_after_empty_runs": 10,
        "stale_source_warning_days": 14,
        "report_lookback_days": 30,
        "notify_telegram_on_stale": True,
    }


def _write_auto_disabled_to_config(
    source_name: str,
    config_path: Path,
    config_list_key: str,
    config_section_key: str,
) -> bool:
    """Find the matching entry in *config_path* and add ``"auto_disabled": true``.

    Returns True if the entry was found and written, False otherwise.
    """
    try:
        with config_path.open(encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"  [health] Warning: could not read {config_path}: {exc}")
        return False

    entries = data.get(config_list_key)
    if not isinstance(entries, list):
        return False

    found = False
    for entry in entries:
        if isinstance(entry, dict) and entry.get(config_section_key) == source_name:
            entry["auto_disabled"] = True
            found = True
            break

    if not found:
        return False

    try:
        with config_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.write("\n")
    except OSError as exc:
        print(f"  [health] Warning: could not write {config_path}: {exc}")
        return False

    return True


def update(
    source_name: str,
    raw_fetched: int,
    config_path: Optional[Path] = None,
    config_list_key: str = "targets",
    config_section_key: str = "name",
) -> None:
    """Record a pipeline run for *source_name* and apply auto-disable if needed.

    Args:
        source_name:        Human-readable source identifier (must be stable).
        raw_fetched:        Number of raw records produced by this run.
        config_path:        Path to the JSON config containing the source entry.
                            When provided, ``"auto_disabled": true`` is written
                            back to the config if the threshold is exceeded.
        config_list_key:    Top-level key in the config JSON that holds the list
                            of source entries (e.g. "targets", "feeds", "companies").
        config_section_key: Key inside each entry used to match *source_name*
                            (default: "name").
    """
    ensure_schema()
    cfg = _load_health_config()
    threshold: int = cfg.get("auto_disable_after_empty_runs", 10)

    upsert_source_run(source_name, raw_fetched)

    row = get_source_health(source_name)
    if row is None:
        return

    already_disabled = bool(row.get("auto_disabled"))
    empty_runs = int(row.get("consecutive_empty_runs", 0))

    if not already_disabled and empty_runs >= threshold:
        mark_source_auto_disabled(source_name)
        print(
            f"  [health] AUTO-DISABLED: '{source_name}' has produced 0 raw records "
            f"for {empty_runs} consecutive runs (threshold: {threshold})."
        )
        if config_path is not None:
            written = _write_auto_disabled_to_config(
                source_name, config_path, config_list_key, config_section_key
            )
            if written:
                print(
                    f"  [health] Wrote auto_disabled=true to {config_path.name} "
                    f"for '{source_name}'."
                )
            else:
                print(
                    f"  [health] Warning: could not write auto_disabled flag to "
                    f"{config_path.name} for '{source_name}' — update manually."
                )


def is_auto_disabled(source_name: str) -> bool:
    """Return True if *source_name* is flagged as auto-disabled in the manifest DB."""
    ensure_schema()
    row = get_source_health(source_name)
    if row is None:
        return False
    return bool(row.get("auto_disabled"))
