"""Theme Memory Persistence Module.

Provides atomic read/write operations for theme memory JSON files.
Thread-safe file operations for themes and expansions.
"""

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Theme memory storage paths
THEME_MEMORY_DIR = BASE_DIR / "data" / "theme_memory"
THEMES_PATH = THEME_MEMORY_DIR / "themes.json"
EXPANSIONS_PATH = THEME_MEMORY_DIR / "expansions.json"

# Config paths
KEYWORD_BUNDLES_PATH = BASE_DIR / "config" / "keyword_bundles.json"
NEGATIVE_KEYWORD_BUNDLES_PATH = BASE_DIR / "config" / "negative_keyword_bundles.json"


def _ensure_theme_memory_dir() -> None:
    """Ensure the theme memory directory exists."""
    THEME_MEMORY_DIR.mkdir(parents=True, exist_ok=True)


def _get_lock() -> threading.Lock:
    """Get a thread lock for atomic file operations."""
    return threading.Lock()


def _load_json_atomic(path: Path) -> dict[str, Any]:
    """Load JSON file with thread safety.

    Args:
        path: Path to JSON file

    Returns:
        Parsed JSON dictionary, or empty dict if file doesn't exist
    """
    with _get_lock():
        if not path.exists():
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}


def _save_json_atomic(path: Path, data: dict[str, Any]) -> None:
    """Save JSON file with thread safety (write to temp then rename).

    Args:
        path: Path to save to
        data: Dictionary to save as JSON
    """
    _ensure_theme_memory_dir()
    with _get_lock():
        temp_path = path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        temp_path.replace(path)


# ============================================================================
# Theme Memory Operations
# ============================================================================


def get_theme(theme_id: str) -> Optional[dict[str, Any]]:
    """Get a theme by ID.

    Args:
        theme_id: Theme identifier (slug)

    Returns:
        Theme dict, or None if not found
    """
    all_themes = _load_json_atomic(THEMES_PATH)
    return all_themes.get(theme_id)


def get_all_themes() -> dict[str, dict[str, Any]]:
    """Get all theme records.

    Returns:
        Dictionary mapping theme_id to theme record
    """
    return _load_json_atomic(THEMES_PATH)


def save_theme(theme_id: str, theme: dict[str, Any]) -> None:
    """Save or update a theme.

    Args:
        theme_id: Theme identifier
        theme: Theme dictionary
    """
    all_themes = _load_json_atomic(THEMES_PATH)
    theme["theme_id"] = theme_id
    theme["last_seen"] = datetime.now(timezone.utc).isoformat()
    all_themes[theme_id] = theme
    _save_json_atomic(THEMES_PATH, all_themes)


def delete_theme(theme_id: str) -> bool:
    """Delete a theme by ID.

    Args:
        theme_id: Theme identifier

    Returns:
        True if theme was deleted, False if not found
    """
    all_themes = _load_json_atomic(THEMES_PATH)
    if theme_id in all_themes:
        del all_themes[theme_id]
        _save_json_atomic(THEMES_PATH, all_themes)
        return True
    return False


def initialize_theme(
    theme_id: str,
    theme_label: str,
    positive_terms: list[str] | None = None,
    negative_terms: list[str] | None = None,
) -> dict[str, Any]:
    """Initialize a new theme record.

    Args:
        theme_id: Theme identifier (slug)
        theme_label: Human-readable label
        positive_terms: Initial positive terms
        negative_terms: Initial negative terms

    Returns:
        New theme dictionary
    """
    now = datetime.now(timezone.utc).isoformat()
    return {
        "theme_id": theme_id,
        "theme_label": theme_label,
        "positive_terms": positive_terms or [],
        "negative_terms": negative_terms or [],
        "accepted_count": 0,
        "review_count": 0,
        "rejected_count": 0,
        "last_seen": now,
        "priority_score": 0,
    }


# ============================================================================
# Expansion Memory Operations
# ============================================================================


def get_expansion(theme_id: str) -> Optional[dict[str, Any]]:
    """Get expansion data for a theme.

    Args:
        theme_id: Theme identifier

    Returns:
        Expansion dict, or None if not found
    """
    all_expansions = _load_json_atomic(EXPANSIONS_PATH)
    return all_expansions.get(theme_id)


def get_all_expansions() -> dict[str, dict[str, Any]]:
    """Get all expansion records.

    Returns:
        Dictionary mapping theme_id to expansion record
    """
    return _load_json_atomic(EXPANSIONS_PATH)


def save_expansion(theme_id: str, expansion: dict[str, Any]) -> None:
    """Save or update expansion data for a theme.

    Args:
        theme_id: Theme identifier
        expansion: Expansion dictionary
    """
    all_expansions = _load_json_atomic(EXPANSIONS_PATH)
    expansion["theme_id"] = theme_id
    expansion["last_updated"] = datetime.now(timezone.utc).isoformat()
    all_expansions[theme_id] = expansion
    _save_json_atomic(EXPANSIONS_PATH, all_expansions)


def initialize_expansion(
    theme_id: str,
    expanded_terms: list[str] | None = None,
    source_terms: list[str] | None = None,
) -> dict[str, Any]:
    """Initialize a new expansion record.

    Args:
        theme_id: Theme identifier
        expanded_terms: Terms expanded from seed keywords
        source_terms: Original seed terms that led to expansion

    Returns:
        New expansion dictionary
    """
    now = datetime.now(timezone.utc).isoformat()
    return {
        "theme_id": theme_id,
        "expanded_terms": expanded_terms or [],
        "source_terms": source_terms or [],
        "last_updated": now,
    }


# ============================================================================
# Keyword Bundles Configuration
# ============================================================================


def load_keyword_bundles() -> dict[str, Any]:
    """Load keyword bundles configuration.

    Returns:
        Keyword bundles configuration dictionary
    """
    if not KEYWORD_BUNDLES_PATH.exists():
        return {}

    try:
        with open(KEYWORD_BUNDLES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def load_negative_keyword_bundles() -> dict[str, Any]:
    """Load negative keyword bundles configuration.

    Returns:
        Negative keyword bundles configuration dictionary
    """
    if not NEGATIVE_KEYWORD_BUNDLES_PATH.exists():
        return {}

    try:
        with open(NEGATIVE_KEYWORD_BUNDLES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


# ============================================================================
# Memory Initialization
# ============================================================================


def initialize_theme_memory_files() -> None:
    """Initialize empty theme memory files if they don't exist.

    Creates empty JSON objects in the theme memory directory.
    """
    _ensure_theme_memory_dir()

    if not THEMES_PATH.exists():
        _save_json_atomic(THEMES_PATH, {})

    if not EXPANSIONS_PATH.exists():
        _save_json_atomic(EXPANSIONS_PATH, {})


# Ensure directories exist on module import
_ensure_theme_memory_dir()
