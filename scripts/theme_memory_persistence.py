"""Theme Memory Persistence Module.

Provides atomic read/write operations for theme memory JSON files.
Thread-safe file operations for learned themes and negative bundles.
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

# Config path
KEYWORD_BUNDLES_PATH = BASE_DIR / "config" / "keyword_bundles.json"


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


def get_theme_memory() -> dict[str, Any]:
    """Get all theme memory.

    Returns:
        Theme memory dictionary with themes, negative_bundles, etc.
    """
    return _load_json_atomic(THEMES_PATH)


def get_themes() -> dict[str, dict[str, Any]]:
    """Get all learned themes.

    Returns:
        Dictionary mapping theme_id to theme record
    """
    memory = _load_json_atomic(THEMES_PATH)
    return memory.get("themes", {})


def get_theme(theme_id: str) -> Optional[dict[str, Any]]:
    """Get a specific theme by ID.

    Args:
        theme_id: Theme identifier

    Returns:
        Theme dictionary, or None if not found
    """
    themes = get_themes()
    return themes.get(theme_id)


def get_high_priority_themes(threshold: float = 70.0) -> list[dict[str, Any]]:
    """Get themes with priority >= threshold.

    Args:
        threshold: Minimum priority value (default 70.0)

    Returns:
        List of theme dictionaries meeting the threshold
    """
    themes = get_themes()
    return [theme for theme in themes.values() if theme.get("priority", 0) >= threshold]


def save_theme(
    theme_id: str,
    theme_data: dict[str, Any],
    priority: float = 50.0,
    matched_terms: list[str] = None,
    source_candidate_id: str = None,
) -> None:
    """Save or update a learned theme.

    Args:
        theme_id: Theme identifier
        theme_data: Theme data dictionary (should contain bundle_id, keywords, etc.)
        priority: Theme priority score (0-100)
        matched_terms: List of terms that matched this theme
        source_candidate_id: Candidate ID that triggered this theme learning
    """
    memory = _load_json_atomic(THEMES_PATH)

    if "themes" not in memory:
        memory["themes"] = {}

    now = datetime.now(timezone.utc).isoformat()

    existing = memory["themes"].get(theme_id, {})
    existing.update(theme_data)
    existing["priority"] = priority
    existing["last_seen"] = now
    existing["last_updated"] = now

    if matched_terms:
        existing["matched_terms"] = matched_terms
    if source_candidate_id:
        existing["source_candidate_id"] = source_candidate_id

    # Track update count
    if "update_count" in existing:
        existing["update_count"] += 1
    else:
        existing["update_count"] = 1

    memory["themes"][theme_id] = existing
    memory["last_updated"] = now

    _save_json_atomic(THEMES_PATH, memory)


def initialize_theme(
    bundle_id: str,
    keywords: list[str],
    priority: float = 50.0,
    source_candidate_id: str = None,
) -> dict[str, Any]:
    """Initialize a new theme from a keyword bundle match.

    Args:
        bundle_id: ID of the keyword bundle that matched
        keywords: List of keywords that define this theme
        priority: Initial priority score (0-100)
        source_candidate_id: Candidate ID that triggered theme learning

    Returns:
        New theme dictionary
    """
    now = datetime.now(timezone.utc).isoformat()
    theme_id = f"theme_{bundle_id}_{int(datetime.now(timezone.utc).timestamp())}"

    return {
        "theme_id": theme_id,
        "bundle_id": bundle_id,
        "keywords": keywords,
        "priority": priority,
        "matched_terms": keywords,
        "source_candidate_id": source_candidate_id,
        "first_seen": now,
        "last_seen": now,
        "last_updated": now,
        "update_count": 1,
        "match_count": 1,
    }


def increment_theme_match(theme_id: str) -> None:
    """Increment the match count for a theme.

    Args:
        theme_id: Theme identifier
    """
    memory = _load_json_atomic(THEMES_PATH)

    if "themes" not in memory:
        return

    if theme_id in memory["themes"]:
        memory["themes"][theme_id]["match_count"] = (
            memory["themes"][theme_id].get("match_count", 0) + 1
        )
        memory["themes"][theme_id]["last_seen"] = datetime.now(timezone.utc).isoformat()
        memory["last_updated"] = datetime.now(timezone.utc).isoformat()
        _save_json_atomic(THEMES_PATH, memory)


def delete_theme(theme_id: str) -> bool:
    """Delete a theme by ID.

    Args:
        theme_id: Theme identifier

    Returns:
        True if theme was deleted, False if not found
    """
    memory = _load_json_atomic(THEMES_PATH)

    if "themes" not in memory:
        return False

    if theme_id in memory["themes"]:
        del memory["themes"][theme_id]
        memory["last_updated"] = datetime.now(timezone.utc).isoformat()
        _save_json_atomic(THEMES_PATH, memory)
        return True

    return False


# ============================================================================
# Negative Bundle Operations
# ============================================================================


def get_negative_bundles() -> dict[str, dict[str, Any]]:
    """Get all negative bundles.

    Returns:
        Dictionary mapping bundle_id to negative bundle record
    """
    memory = _load_json_atomic(THEMES_PATH)
    return memory.get("negative_bundles", {})


def get_negative_bundle(bundle_id: str) -> Optional[dict[str, Any]]:
    """Get a specific negative bundle by ID.

    Args:
        bundle_id: Negative bundle identifier

    Returns:
        Negative bundle dictionary, or None if not found
    """
    negative_bundles = get_negative_bundles()
    return negative_bundles.get(bundle_id)


def save_negative_bundle(
    bundle_id: str,
    bundle_data: dict[str, Any],
    penalty_strength: float = 30.0,
    source_candidate_id: str = None,
) -> None:
    """Save or update a negative bundle.

    Args:
        bundle_id: Bundle identifier
        bundle_data: Bundle data dictionary
        penalty_strength: Strength of the penalty (0-100)
        source_candidate_id: Candidate ID that triggered this negative bundle
    """
    memory = _load_json_atomic(THEMES_PATH)

    if "negative_bundles" not in memory:
        memory["negative_bundles"] = {}

    now = datetime.now(timezone.utc).isoformat()

    existing = memory["negative_bundles"].get(bundle_id, {})
    existing.update(bundle_data)
    existing["penalty_strength"] = penalty_strength
    existing["last_seen"] = now
    existing["last_updated"] = now

    if source_candidate_id:
        existing["source_candidate_id"] = source_candidate_id

    memory["negative_bundles"][bundle_id] = existing
    memory["last_updated"] = now

    _save_json_atomic(THEMES_PATH, memory)


def initialize_negative_bundle(
    bundle_id: str,
    terms: list[str],
    penalty_strength: float = 30.0,
    source_candidate_id: str = None,
) -> dict[str, Any]:
    """Initialize a new negative bundle from rejected candidate signals.

    Args:
        bundle_id: ID for this negative bundle
        terms: List of terms that indicate low quality
        penalty_strength: Initial penalty strength (0-100)
        source_candidate_id: Candidate ID that triggered this negative bundle

    Returns:
        New negative bundle dictionary
    """
    now = datetime.now(timezone.utc).isoformat()

    return {
        "bundle_id": bundle_id,
        "terms": terms,
        "penalty_strength": penalty_strength,
        "source_candidate_id": source_candidate_id,
        "first_seen": now,
        "last_seen": now,
        "last_updated": now,
        "match_count": 1,
    }


# ============================================================================
# Keyword Bundles Operations
# ============================================================================


def load_keyword_bundles() -> dict[str, Any]:
    """Load keyword bundles from config/keyword_bundles.json.

    Returns:
        Keyword bundles configuration dictionary
    """
    if not KEYWORD_BUNDLES_PATH.exists():
        return {"bundles": {}}

    try:
        with open(KEYWORD_BUNDLES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {"bundles": {}}


def get_bundle(bundle_id: str) -> Optional[dict[str, Any]]:
    """Get a specific keyword bundle by ID.

    Args:
        bundle_id: Bundle identifier

    Returns:
        Bundle dictionary, or None if not found
    """
    bundles_config = load_keyword_bundles()
    bundles = bundles_config.get("bundles", {})
    return bundles.get(bundle_id)


def get_all_bundles() -> dict[str, dict[str, Any]]:
    """Get all keyword bundles.

    Returns:
        Dictionary mapping bundle_id to bundle definition
    """
    bundles_config = load_keyword_bundles()
    return bundles_config.get("bundles", {})


def get_negative_bundles_from_config() -> list[dict[str, Any]]:
    """Get bundles marked as negative from config.

    Returns:
        List of negative bundle dictionaries
    """
    bundles_config = load_keyword_bundles()
    bundles = bundles_config.get("bundles", {})
    return [bundle for bundle in bundles.values() if bundle.get("is_negative", False)]


# ============================================================================
# Memory Initialization
# ============================================================================


def initialize_theme_memory_files() -> None:
    """Initialize empty theme memory files if they don't exist.

    Creates empty JSON objects in the theme_memory directory.
    """
    _ensure_theme_memory_dir()

    if not THEMES_PATH.exists():
        _save_json_atomic(
            THEMES_PATH,
            {
                "themes": {},
                "negative_bundles": {},
                "last_updated": datetime.now(timezone.utc).isoformat(),
            },
        )


# Ensure directories exist on module import
_ensure_theme_memory_dir()
