"""SQLite-backed manifest database for the Finance Research Archive.

Replaces data/ingestion_manifest.json and data/quant_ingestion_manifest.json
with a single database at data/archive.db. All writes are wrapped in
transactions so a crash mid-write leaves the database consistent.

WAL mode is enabled so readers never block writers and vice-versa.
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "archive.db"

_CONNECTIONS: dict[str, sqlite3.Connection] = {}

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS seen_urls (
    url          TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_urls (
    url          TEXT PRIMARY KEY,
    url_type     TEXT NOT NULL CHECK(url_type IN ('article', 'listing')),
    processed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS record_map (
    url       TEXT PRIMARY KEY,
    record_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS record_rules (
    record_id  TEXT PRIMARY KEY,
    rules_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS title_fingerprints (
    fingerprint TEXT PRIMARY KEY,
    record_id   TEXT,
    seen_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS content_fingerprints (
    fingerprint TEXT PRIMARY KEY,
    record_id   TEXT,
    seen_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_fingerprints (
    fingerprint TEXT PRIMARY KEY,
    record_id   TEXT,
    seen_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quant_seen_series (
    series_id        TEXT NOT NULL,
    snapshot_date    TEXT NOT NULL,
    content_hash     TEXT NOT NULL,
    processed_at     TEXT,
    latest_data_date TEXT,
    PRIMARY KEY (series_id, snapshot_date)
);

CREATE TABLE IF NOT EXISTS source_health (
    source_name            TEXT PRIMARY KEY,
    consecutive_empty_runs INTEGER NOT NULL DEFAULT 0,
    total_raw_fetched      INTEGER NOT NULL DEFAULT 0,
    last_run_at            TEXT,
    auto_disabled          INTEGER NOT NULL DEFAULT 0,
    disabled_at            TEXT
);
"""

_VALID_FINGERPRINT_TABLES = frozenset(
    {"title_fingerprints", "content_fingerprints", "event_fingerprints"}
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Return a cached connection for *db_path*, creating it if needed.

    WAL mode and foreign keys are enabled on first connection.
    """
    key = str(db_path)
    if key not in _CONNECTIONS:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        _CONNECTIONS[key] = conn
    return _CONNECTIONS[key]


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def ensure_schema(db_path: Path = DB_PATH) -> None:
    """Create all tables if they do not already exist."""
    conn = get_conn(db_path)
    with conn:
        conn.executescript(_SCHEMA_SQL)
        # Migrations: add columns that may be missing in older databases.
        existing_cols = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(quant_seen_series)"
            ).fetchall()
        }
        if "latest_data_date" not in existing_cols:
            conn.execute(
                "ALTER TABLE quant_seen_series ADD COLUMN latest_data_date TEXT"
            )


# ---------------------------------------------------------------------------
# seen_urls
# ---------------------------------------------------------------------------


def is_url_seen(url: str, db_path: Path = DB_PATH) -> bool:
    conn = get_conn(db_path)
    row = conn.execute(
        "SELECT 1 FROM seen_urls WHERE url = ?", (url,)
    ).fetchone()
    return row is not None


def get_url_content_hash(url: str, db_path: Path = DB_PATH) -> Optional[str]:
    conn = get_conn(db_path)
    row = conn.execute(
        "SELECT content_hash FROM seen_urls WHERE url = ?", (url,)
    ).fetchone()
    return row["content_hash"] if row else None


def upsert_seen_url(url: str, content_hash: str, db_path: Path = DB_PATH) -> None:
    conn = get_conn(db_path)
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO seen_urls (url, content_hash, updated_at) VALUES (?, ?, ?)",
            (url, content_hash, _now()),
        )


# ---------------------------------------------------------------------------
# processed_urls  (merges old processed_urls + listing_urls dicts)
# ---------------------------------------------------------------------------


def is_url_processed_as_article(url: str, db_path: Path = DB_PATH) -> bool:
    """Return True only if this URL was permanently processed as an article."""
    conn = get_conn(db_path)
    row = conn.execute(
        "SELECT url_type FROM processed_urls WHERE url = ?", (url,)
    ).fetchone()
    return row is not None and row["url_type"] == "article"


def is_url_processed_as_listing(url: str, db_path: Path = DB_PATH) -> bool:
    """Return True if this URL was classified as a listing/container page."""
    conn = get_conn(db_path)
    row = conn.execute(
        "SELECT url_type FROM processed_urls WHERE url = ?", (url,)
    ).fetchone()
    return row is not None and row["url_type"] == "listing"


def mark_url_processed(
    url: str, url_type: str, db_path: Path = DB_PATH
) -> None:
    """Record that *url* has been processed.

    url_type must be 'article' (permanent skip) or 'listing' (re-crawl for
    new links, skip only when content hash matches).
    """
    if url_type not in ("article", "listing"):
        raise ValueError(f"url_type must be 'article' or 'listing', got {url_type!r}")
    conn = get_conn(db_path)
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO processed_urls (url, url_type, processed_at) VALUES (?, ?, ?)",
            (url, url_type, _now()),
        )


def get_all_processed_article_urls(db_path: Path = DB_PATH) -> set:
    """Return the set of all article-type processed URLs (for link scoring)."""
    conn = get_conn(db_path)
    rows = conn.execute(
        "SELECT url FROM processed_urls WHERE url_type = 'article'"
    ).fetchall()
    return {row["url"] for row in rows}


# ---------------------------------------------------------------------------
# record_map
# ---------------------------------------------------------------------------


def get_record_id_for_url(url: str, db_path: Path = DB_PATH) -> Optional[str]:
    conn = get_conn(db_path)
    row = conn.execute(
        "SELECT record_id FROM record_map WHERE url = ?", (url,)
    ).fetchone()
    return row["record_id"] if row else None


def get_url_for_record_id(record_id: str, db_path: Path = DB_PATH) -> Optional[str]:
    conn = get_conn(db_path)
    row = conn.execute(
        "SELECT url FROM record_map WHERE record_id = ?", (record_id,)
    ).fetchone()
    return row["url"] if row else None


def set_record_map(url: str, record_id: str, db_path: Path = DB_PATH) -> None:
    conn = get_conn(db_path)
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO record_map (url, record_id) VALUES (?, ?)",
            (url, record_id),
        )


def get_all_record_map(db_path: Path = DB_PATH) -> dict:
    """Return the full url→record_id mapping (used by verify_manifest_consistency)."""
    conn = get_conn(db_path)
    rows = conn.execute("SELECT url, record_id FROM record_map").fetchall()
    return {row["url"]: row["record_id"] for row in rows}


# ---------------------------------------------------------------------------
# record_rules
# ---------------------------------------------------------------------------


def get_record_rules(record_id: str, db_path: Path = DB_PATH) -> Optional[dict]:
    conn = get_conn(db_path)
    row = conn.execute(
        "SELECT rules_json FROM record_rules WHERE record_id = ?", (record_id,)
    ).fetchone()
    if row is None:
        return None
    return json.loads(row["rules_json"])


def set_record_rules(
    record_id: str, rules: dict, db_path: Path = DB_PATH
) -> None:
    conn = get_conn(db_path)
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO record_rules (record_id, rules_json, updated_at) VALUES (?, ?, ?)",
            (record_id, json.dumps(rules, ensure_ascii=False), _now()),
        )


# ---------------------------------------------------------------------------
# Fingerprints (title / content / event share the same pattern)
# ---------------------------------------------------------------------------


def _check_fp_table(table: str) -> None:
    if table not in _VALID_FINGERPRINT_TABLES:
        raise ValueError(
            f"Invalid fingerprint table {table!r}. "
            f"Must be one of {sorted(_VALID_FINGERPRINT_TABLES)}"
        )


def is_fingerprint_seen(
    table: str, fingerprint: str, db_path: Path = DB_PATH
) -> bool:
    _check_fp_table(table)
    conn = get_conn(db_path)
    row = conn.execute(
        f"SELECT 1 FROM {table} WHERE fingerprint = ?",  # noqa: S608 – table validated above
        (fingerprint,),
    ).fetchone()
    return row is not None


def get_fingerprint_record_id(
    table: str, fingerprint: str, db_path: Path = DB_PATH
) -> Optional[str]:
    """Return the record_id associated with *fingerprint*, or None."""
    _check_fp_table(table)
    conn = get_conn(db_path)
    row = conn.execute(
        f"SELECT record_id FROM {table} WHERE fingerprint = ?",  # noqa: S608
        (fingerprint,),
    ).fetchone()
    return row["record_id"] if row else None


def add_fingerprint(
    table: str,
    fingerprint: str,
    record_id: Optional[str],
    db_path: Path = DB_PATH,
) -> None:
    """Insert fingerprint → record_id; silently ignores duplicates (first-seen wins)."""
    _check_fp_table(table)
    conn = get_conn(db_path)
    with conn:
        conn.execute(
            f"INSERT OR IGNORE INTO {table} (fingerprint, record_id, seen_at) VALUES (?, ?, ?)",  # noqa: S608
            (fingerprint, record_id, _now()),
        )


# ---------------------------------------------------------------------------
# quant_seen_series
# ---------------------------------------------------------------------------


def is_quant_series_seen(
    series_id: str, snapshot_date: str, db_path: Path = DB_PATH
) -> bool:
    conn = get_conn(db_path)
    row = conn.execute(
        "SELECT 1 FROM quant_seen_series WHERE series_id = ? AND snapshot_date = ?",
        (series_id, snapshot_date),
    ).fetchone()
    return row is not None


def add_quant_series(
    series_id: str,
    snapshot_date: str,
    content_hash: str,
    processed_at: Optional[str] = None,
    latest_data_date: Optional[str] = None,
    db_path: Path = DB_PATH,
) -> None:
    conn = get_conn(db_path)
    with conn:
        conn.execute(
            "INSERT OR IGNORE INTO quant_seen_series "
            "(series_id, snapshot_date, content_hash, processed_at, latest_data_date) "
            "VALUES (?, ?, ?, ?, ?)",
            (series_id, snapshot_date, content_hash, processed_at or _now(), latest_data_date),
        )


def get_quant_series_ids_for_run(
    snapshot_date: str, db_path: Path = DB_PATH
) -> list:
    """Return all series_ids recorded for *snapshot_date* (current run's output)."""
    conn = get_conn(db_path)
    rows = conn.execute(
        "SELECT series_id FROM quant_seen_series WHERE snapshot_date = ?",
        (snapshot_date,),
    ).fetchall()
    return [row["series_id"] for row in rows]


def get_quant_series_latest_data_date(
    series_id: str, db_path: Path = DB_PATH
) -> Optional[str]:
    """Return the latest_data_date stored for the most recent run of *series_id*.

    Returns None if the series has never been ingested or if no data date was
    recorded (e.g. rows written before this column was added).
    """
    conn = get_conn(db_path)
    row = conn.execute(
        "SELECT latest_data_date FROM quant_seen_series "
        "WHERE series_id = ? ORDER BY snapshot_date DESC LIMIT 1",
        (series_id,),
    ).fetchone()
    return row["latest_data_date"] if row else None


# ---------------------------------------------------------------------------
# source_health
# ---------------------------------------------------------------------------


def upsert_source_run(
    source_name: str, raw_fetched: int, db_path: Path = DB_PATH
) -> None:
    """Record a pipeline run for *source_name*.

    Increments consecutive_empty_runs when raw_fetched == 0; resets it to 0
    when at least one raw record was fetched.  total_raw_fetched accumulates
    across all runs.
    """
    conn = get_conn(db_path)
    now = _now()
    with conn:
        conn.execute(
            """
            INSERT INTO source_health
                (source_name, consecutive_empty_runs, total_raw_fetched, last_run_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source_name) DO UPDATE SET
                consecutive_empty_runs = CASE
                    WHEN excluded.total_raw_fetched = 0
                    THEN source_health.consecutive_empty_runs + 1
                    ELSE 0
                END,
                total_raw_fetched = source_health.total_raw_fetched + excluded.total_raw_fetched,
                last_run_at = excluded.last_run_at
            """,
            (source_name, 0 if raw_fetched > 0 else 1, raw_fetched, now),
        )


def mark_source_auto_disabled(
    source_name: str, db_path: Path = DB_PATH
) -> None:
    """Set auto_disabled=1 for *source_name* and record the timestamp."""
    conn = get_conn(db_path)
    now = _now()
    with conn:
        conn.execute(
            """
            INSERT INTO source_health (source_name, auto_disabled, disabled_at, last_run_at)
            VALUES (?, 1, ?, ?)
            ON CONFLICT(source_name) DO UPDATE SET
                auto_disabled = 1,
                disabled_at = excluded.disabled_at
            """,
            (source_name, now, now),
        )


def reset_source_auto_disabled(
    source_name: str, db_path: Path = DB_PATH
) -> None:
    """Clear auto_disabled and reset consecutive_empty_runs for *source_name*."""
    conn = get_conn(db_path)
    with conn:
        conn.execute(
            """
            INSERT INTO source_health (source_name, auto_disabled, consecutive_empty_runs)
            VALUES (?, 0, 0)
            ON CONFLICT(source_name) DO UPDATE SET
                auto_disabled = 0,
                consecutive_empty_runs = 0,
                disabled_at = NULL
            """,
            (source_name,),
        )


def get_source_health(
    source_name: str, db_path: Path = DB_PATH
) -> Optional[dict]:
    """Return health row for *source_name*, or None if not found."""
    conn = get_conn(db_path)
    row = conn.execute(
        "SELECT * FROM source_health WHERE source_name = ?", (source_name,)
    ).fetchone()
    return dict(row) if row else None


def get_all_source_health(db_path: Path = DB_PATH) -> list:
    """Return all rows from source_health ordered by source_name."""
    conn = get_conn(db_path)
    rows = conn.execute(
        "SELECT * FROM source_health ORDER BY source_name"
    ).fetchall()
    return [dict(row) for row in rows]
