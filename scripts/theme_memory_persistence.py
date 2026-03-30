"""Theme Memory Persistence Module.

Provides atomic read/write operations for theme memory JSON files.
Thread-safe file operations for themes, expansions, and keyword bundles.
"""

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

BASE_DIR = Path(__file__).resolve().parent.parent

THEME_MEMORY_DIR = BASE_DIR / "data" / "theme_memory"
THEMES_PATH = THEME_MEMORY_DIR / "themes.json"
EXPANSIONS_PATH = THEME_MEMORY_DIR / "expansions.json"

KEYWORD_BUNDLES_PATH = BASE_DIR / "config" / "keyword_bundles.json"
NEGATIVE_BUNDLES_PATH = BASE_DIR / "config" / "negative_keyword_bundles.json"


def _ensure_theme_memory_dir() -> None:
    """Ensure the theme memory directory exists."""
    THEME_MEMORY_DIR.mkdir(parents=True, exist_ok=True)


def _get_lock() -> threading.Lock:
    """Get a thread lock for atomic file operations."""
    return threading.Lock()


def _load_json_atomic(path: Path) -> dict[str, Any]:
    """Load JSON file with thread safety."""
    with _get_lock():
        if not path.exists():
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}


def _save_json_atomic(path: Path, data: dict[str, Any]) -> None:
    """Save JSON file with thread safety (write to temp then rename)."""
    _ensure_theme_memory_dir()
    with _get_lock():
        temp_path = path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        temp_path.replace(path)


# ============================================================================
# Theme Memory Operations
# ============================================================================


def get_theme_memory(theme_id: str) -> Optional[dict[str, Any]]:
    """Get memory record for a theme."""
    all_themes = read_theme_memory()
    return all_themes.get(theme_id)


def get_all_theme_memory() -> dict[str, dict[str, Any]]:
    """Get all theme memory records."""
    return read_theme_memory()


def save_theme_memory(theme_id: str, memory: dict[str, Any]) -> None:
    """Save or update memory record for a theme."""
    all_themes = read_theme_memory()
    memory["theme_id"] = theme_id
    memory["last_seen"] = datetime.now(timezone.utc).isoformat()
    all_themes[theme_id] = memory
    write_theme_memory(all_themes)


def initialize_theme_memory(
    theme_id: str,
    theme_label: str,
    positive_terms: Optional[list[str]] = None,
    negative_terms: Optional[list[str]] = None,
    priority_score: float = 50.0,
) -> dict[str, Any]:
    """Initialize a new theme memory record."""
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
        "priority_score": priority_score,
    }


def delete_theme_memory(theme_id: str) -> bool:
    """Delete a theme memory record."""
    all_themes = read_theme_memory()
    if theme_id in all_themes:
        del all_themes[theme_id]
        write_theme_memory(all_themes)
        return True
    return False


def read_theme_memory() -> dict[str, dict[str, Any]]:
    """Read themes.json file."""
    if not THEMES_PATH.exists():
        return {}
    try:
        with open(THEMES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            themes = data.get("themes", {})
            if isinstance(themes, list):
                return {}
            return themes
    except (json.JSONDecodeError, IOError):
        return {}


def write_theme_memory(themes: dict[str, dict[str, Any]]) -> None:
    """Write themes.json file atomically."""
    _ensure_theme_memory_dir()
    data = {
        "themes": themes,
        "version": "1.0",
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }
    _save_json_atomic(THEMES_PATH, data)


# Aliases for compatibility with Stream B naming
get_theme = get_theme_memory
get_all_themes = get_all_theme_memory
save_theme = save_theme_memory


def delete_theme(theme_id: str) -> bool:
    """Delete a theme by ID."""
    return delete_theme_memory(theme_id)


def initialize_theme(
    theme_id: str,
    theme_label: str,
    positive_terms: Optional[list[str]] = None,
    negative_terms: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Initialize a new theme record."""
    return initialize_theme_memory(
        theme_id, theme_label, positive_terms, negative_terms
    )


# ============================================================================
# Expansions Queue Operations
# ============================================================================


def read_expansions() -> dict[str, Any]:
    """Read expansions.json file."""
    if not EXPANSIONS_PATH.exists():
        return {
            "proposals": [],
            "approved": [],
            "rejected": [],
            "applied": [],
            "version": "1.0",
        }
    try:
        with open(EXPANSIONS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {
            "proposals": [],
            "approved": [],
            "rejected": [],
            "applied": [],
            "version": "1.0",
        }


def write_expansions(expansions: dict[str, Any]) -> None:
    """Write expansions.json file atomically."""
    _ensure_theme_memory_dir()
    expansions["version"] = "1.0"
    _save_json_atomic(EXPANSIONS_PATH, expansions)


def add_proposal(proposal: dict[str, Any]) -> None:
    """Add a new expansion proposal."""
    expansions = read_expansions()
    expansions["proposals"].append(proposal)
    write_expansions(expansions)


def approve_proposal(proposal_id: str) -> Optional[dict[str, Any]]:
    """Approve a proposal and move it to approved list."""
    expansions = read_expansions()
    proposal = None
    new_proposals = []
    for p in expansions["proposals"]:
        if p.get("id") == proposal_id:
            proposal = p
        else:
            new_proposals.append(p)
    if proposal:
        proposal["approved_at"] = datetime.now(timezone.utc).isoformat()
        expansions["proposals"] = new_proposals
        expansions["approved"].append(proposal)
        write_expansions(expansions)
    return proposal


def reject_proposal(proposal_id: str, reason: Optional[str] = None) -> bool:
    """Reject a proposal and move it to rejected list."""
    expansions = read_expansions()
    proposal = None
    new_proposals = []
    for p in expansions["proposals"]:
        if p.get("id") == proposal_id:
            proposal = p
        else:
            new_proposals.append(p)
    if proposal:
        proposal["rejected_at"] = datetime.now(timezone.utc).isoformat()
        proposal["rejection_reason"] = reason
        expansions["proposals"] = new_proposals
        expansions["rejected"].append(proposal)
        write_expansions(expansions)
        return True
    return False


def apply_approved_expansion(expansion_id: str) -> bool:
    """Mark an approved expansion as applied."""
    expansions = read_expansions()
    for exp in expansions["approved"]:
        if exp.get("id") == expansion_id:
            exp["applied_at"] = datetime.now(timezone.utc).isoformat()
            expansions["applied"].append(exp)
            expansions["approved"] = [
                e for e in expansions["approved"] if e.get("id") != expansion_id
            ]
            write_expansions(expansions)
            return True
    return False


# ============================================================================
# Keyword Bundles Operations
# ============================================================================


def read_keyword_bundles() -> dict[str, Any]:
    """Read keyword_bundles.json file."""
    if not KEYWORD_BUNDLES_PATH.exists():
        return {"bundles": [], "version": "1.0"}
    try:
        with open(KEYWORD_BUNDLES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {"bundles": [], "version": "1.0"}


def write_keyword_bundles(bundles_config: dict[str, Any]) -> None:
    """Write keyword_bundles.json file atomically."""
    bundles_config["version"] = "1.0"
    _save_json_atomic(KEYWORD_BUNDLES_PATH, bundles_config)


def get_keyword_bundle(bundle_id: str) -> Optional[dict[str, Any]]:
    """Get a keyword bundle by ID."""
    config = read_keyword_bundles()
    for bundle in config.get("bundles", []):
        if bundle.get("bundle_id") == bundle_id:
            return bundle
    return None


def add_keyword_bundle(bundle: dict[str, Any]) -> None:
    """Add a new keyword bundle."""
    config = read_keyword_bundles()
    bundle["created_at"] = datetime.now(timezone.utc).isoformat()
    config["bundles"].append(bundle)
    write_keyword_bundles(config)


def update_keyword_bundle(bundle_id: str, updates: dict[str, Any]) -> bool:
    """Update an existing keyword bundle."""
    config = read_keyword_bundles()
    for bundle in config.get("bundles", []):
        if bundle.get("bundle_id") == bundle_id:
            bundle.update(updates)
            write_keyword_bundles(config)
            return True
    return False


# Alias for compatibility
load_keyword_bundles = read_keyword_bundles


# ============================================================================
# Negative Keyword Bundles Operations
# ============================================================================


def read_negative_bundles() -> dict[str, Any]:
    """Read negative_keyword_bundles.json file."""
    if not NEGATIVE_BUNDLES_PATH.exists():
        return {"bundles": [], "version": "1.0"}
    try:
        with open(NEGATIVE_BUNDLES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {"bundles": [], "version": "1.0"}


def write_negative_bundles(bundles_config: dict[str, Any]) -> None:
    """Write negative_keyword_bundles.json file atomically."""
    bundles_config["version"] = "1.0"
    _save_json_atomic(NEGATIVE_BUNDLES_PATH, bundles_config)


def get_negative_bundle(bundle_id: str) -> Optional[dict[str, Any]]:
    """Get a negative bundle by ID."""
    config = read_negative_bundles()
    for bundle in config.get("bundles", []):
        if bundle.get("bundle_id") == bundle_id:
            return bundle
    return None


def add_negative_bundle(bundle: dict[str, Any]) -> None:
    """Add a new negative keyword bundle."""
    config = read_negative_bundles()
    bundle["created_at"] = datetime.now(timezone.utc).isoformat()
    config["bundles"].append(bundle)
    write_negative_bundles(config)


# Alias for compatibility
load_negative_keyword_bundles = read_negative_bundles
save_negative_bundle = add_negative_bundle


def initialize_negative_bundle(
    bundle_id: str,
    terms: list[str],
    penalty_strength: float = 30.0,
    source_candidate_id: str = None,
) -> dict[str, Any]:
    """Initialize a new negative bundle record."""
    return {
        "bundle_id": bundle_id,
        "terms": terms,
        "penalty_strength": penalty_strength,
        "source_candidate_id": source_candidate_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


# ============================================================================
# High-level Theme Operations
# ============================================================================


# Alias for compatibility with different API names
get_themes = get_all_theme_memory


def get_high_priority_themes(threshold: float = 70.0) -> list[dict[str, Any]]:
    """Get themes with priority score above threshold."""
    all_themes = read_theme_memory()
    return [
        theme
        for theme in all_themes.values()
        if theme.get("priority_score", 0) >= threshold
    ]


# ============================================================================
# Initialization
# ============================================================================


def initialize_theme_memory_files() -> None:
    """Initialize empty theme memory files if they don't exist."""
    _ensure_theme_memory_dir()

    if not THEMES_PATH.exists():
        write_theme_memory({})

    if not EXPANSIONS_PATH.exists():
        write_expansions(
            {
                "proposals": [],
                "approved": [],
                "rejected": [],
                "applied": [],
            }
        )


# Ensure directories exist on module import
_ensure_theme_memory_dir()
