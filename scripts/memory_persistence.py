"""Memory Persistence Module.

Provides atomic read/write operations for memory JSON files.
Thread-safe file operations for domain, path, and source memory.
"""

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Memory storage paths
MEMORY_DIR = BASE_DIR / "data" / "source_memory"
DOMAIN_MEMORY_PATH = MEMORY_DIR / "domain_memory.json"
PATH_MEMORY_PATH = MEMORY_DIR / "path_memory.json"
SOURCE_MEMORY_PATH = MEMORY_DIR / "source_memory.json"

# Config path
MEMORY_CONFIG_PATH = BASE_DIR / "config" / "memory_config.json"


def _ensure_memory_dir() -> None:
    """Ensure the memory directory exists."""
    MEMORY_DIR.mkdir(parents=True, exist_ok=True)


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
    _ensure_memory_dir()
    with _get_lock():
        temp_path = path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        temp_path.replace(path)


def load_memory_config() -> dict[str, Any]:
    """Load memory configuration from config/memory_config.json.

    Returns:
        Memory configuration dictionary
    """
    if not MEMORY_CONFIG_PATH.exists():
        return _default_memory_config()

    try:
        with open(MEMORY_CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return _default_memory_config()


def _default_memory_config() -> dict[str, Any]:
    """Return default memory configuration.

    Returns:
        Default configuration dictionary
    """
    return {
        "cold_start": {
            "min_samples_for_learning": 10,
            "full_learning_threshold": 25,
            "blend_formula": "linear",
        },
        "weights": {
            "accepted_weight": 10,
            "rejected_weight": 5,
            "filtered_weight": 3,
            "review_weight": 0,
            "human_multiplier": 2.0,
        },
        "trust_score_bounds": {
            "min": 1,
            "max": 100,
        },
        "decay": {
            "enabled": False,
            "half_life_days": 180,
            "min_age_days": 30,
        },
        "logging": {
            "log_dir": "logs/source_memory",
            "log_updates": True,
        },
    }


# ============================================================================
# Domain Memory Operations
# ============================================================================


def get_domain_memory(domain: str) -> Optional[dict[str, Any]]:
    """Get memory record for a domain.

    Args:
        domain: Domain name (e.g., 'federalreserve.gov')

    Returns:
        Domain memory dict, or None if not found
    """
    all_memory = _load_json_atomic(DOMAIN_MEMORY_PATH)
    domain_lower = domain.lower()
    return all_memory.get(domain_lower)


def get_all_domain_memory() -> dict[str, dict[str, Any]]:
    """Get all domain memory records.

    Returns:
        Dictionary mapping domain to memory record
    """
    return _load_json_atomic(DOMAIN_MEMORY_PATH)


def save_domain_memory(domain: str, memory: dict[str, Any]) -> None:
    """Save or update memory record for a domain.

    Args:
        domain: Domain name
        memory: Domain memory dictionary
    """
    all_memory = _load_json_atomic(DOMAIN_MEMORY_PATH)
    domain_lower = domain.lower()
    memory["domain"] = domain_lower
    memory["last_seen"] = datetime.now(timezone.utc).isoformat()
    all_memory[domain_lower] = memory
    _save_json_atomic(DOMAIN_MEMORY_PATH, all_memory)


def initialize_domain_memory(
    domain: str,
    baseline_trust: float = 10.0,
    trust_score: float = 10.0,
) -> dict[str, Any]:
    """Initialize a new domain memory record.

    Args:
        domain: Domain name
        baseline_trust: Initial trust from static config (high=100, medium=50, low=10)
        trust_score: Initial trust score

    Returns:
        New domain memory dictionary
    """
    now = datetime.now(timezone.utc).isoformat()
    return {
        "domain": domain.lower(),
        "trust_score": trust_score,
        "baseline_trust": baseline_trust,
        "total_candidates": 0,
        "accepted_count": 0,
        "accepted_human_count": 0,
        "accepted_auto_count": 0,
        "review_count": 0,
        "review_human_count": 0,
        "rejected_count": 0,
        "rejected_human_count": 0,
        "rejected_auto_count": 0,
        "filtered_out_count": 0,
        "yield_score": 0.0,
        "noise_score": 0.0,
        "first_seen": now,
        "last_seen": now,
    }


# ============================================================================
# Path Memory Operations
# ============================================================================


def get_path_memory(domain: str, path_pattern: str) -> Optional[dict[str, Any]]:
    """Get memory record for a path pattern within a domain.

    Args:
        domain: Domain name
        path_pattern: Path pattern (e.g., '/events/')

    Returns:
        Path memory dict, or None if not found
    """
    all_memory = _load_json_atomic(PATH_MEMORY_PATH)
    key = _path_memory_key(domain, path_pattern)
    return all_memory.get(key)


def get_all_path_memory() -> dict[str, dict[str, Any]]:
    """Get all path memory records.

    Returns:
        Dictionary mapping domain/path to memory record
    """
    return _load_json_atomic(PATH_MEMORY_PATH)


def _path_memory_key(domain: str, path_pattern: str) -> str:
    """Generate storage key for path memory.

    Args:
        domain: Domain name
        path_pattern: Path pattern

    Returns:
        Composite key string
    """
    return f"{domain.lower()}::{path_pattern}"


def save_path_memory(domain: str, path_pattern: str, memory: dict[str, Any]) -> None:
    """Save or update memory record for a path pattern.

    Args:
        domain: Domain name
        path_pattern: Path pattern
        memory: Path memory dictionary
    """
    all_memory = _load_json_atomic(PATH_MEMORY_PATH)
    key = _path_memory_key(domain, path_pattern)
    memory["domain"] = domain.lower()
    memory["path_pattern"] = path_pattern
    memory["last_seen"] = datetime.now(timezone.utc).isoformat()
    all_memory[key] = memory
    _save_json_atomic(PATH_MEMORY_PATH, all_memory)


def initialize_path_memory(
    domain: str,
    path_pattern: str,
    baseline_trust: float = 10.0,
) -> dict[str, Any]:
    """Initialize a new path memory record.

    Args:
        domain: Domain name
        path_pattern: Path pattern (e.g., '/events/')
        baseline_trust: Initial trust score

    Returns:
        New path memory dictionary
    """
    now = datetime.now(timezone.utc).isoformat()
    return {
        "domain": domain.lower(),
        "path_pattern": path_pattern,
        "trust_score": baseline_trust,
        "baseline_trust": baseline_trust,
        "total_candidates": 0,
        "accepted_count": 0,
        "accepted_human_count": 0,
        "review_count": 0,
        "review_human_count": 0,
        "rejected_count": 0,
        "rejected_human_count": 0,
        "filtered_out_count": 0,
        "yield_score": 0.0,
        "noise_score": 0.0,
        "first_seen": now,
        "last_seen": now,
    }


def get_path_memory_for_domain(domain: str) -> list[dict[str, Any]]:
    """Get all path memory records for a specific domain.

    Args:
        domain: Domain name

    Returns:
        List of path memory records for the domain
    """
    all_memory = _load_json_atomic(PATH_MEMORY_PATH)
    domain_lower = domain.lower()
    return [m for key, m in all_memory.items() if key.startswith(f"{domain_lower}::")]


# ============================================================================
# Source Memory Operations
# ============================================================================


def get_source_memory(source_id: str) -> Optional[dict[str, Any]]:
    """Get memory record for a source.

    Args:
        source_id: Source identifier

    Returns:
        Source memory dict, or None if not found
    """
    all_memory = _load_json_atomic(SOURCE_MEMORY_PATH)
    return all_memory.get(source_id)


def get_all_source_memory() -> dict[str, dict[str, Any]]:
    """Get all source memory records.

    Returns:
        Dictionary mapping source_id to memory record
    """
    return _load_json_atomic(SOURCE_MEMORY_PATH)


def save_source_memory(source_id: str, memory: dict[str, Any]) -> None:
    """Save or update memory record for a source.

    Args:
        source_id: Source identifier
        memory: Source memory dictionary
    """
    all_memory = _load_json_atomic(SOURCE_MEMORY_PATH)
    memory["source_id"] = source_id
    memory["last_updated"] = datetime.now(timezone.utc).isoformat()
    all_memory[source_id] = memory
    _save_json_atomic(SOURCE_MEMORY_PATH, all_memory)


def initialize_source_memory(
    source_id: str,
    source_type: str = "manual",
    source_domain: Optional[str] = None,
    baseline_trust: float = 50.0,
) -> dict[str, Any]:
    """Initialize a new source memory record.

    Args:
        source_id: Source identifier
        source_type: Type of source (trusted_source_monitor, keyword_discovery, seed_crawl, manual)
        source_domain: Primary domain associated with this source
        baseline_trust: Initial trust score

    Returns:
        New source memory dictionary
    """
    now = datetime.now(timezone.utc).isoformat()
    return {
        "source_id": source_id,
        "source_type": source_type,
        "source_domain": source_domain.lower() if source_domain else None,
        "trust_score": baseline_trust,
        "baseline_trust": baseline_trust,
        "yield_score": 0.0,
        "noise_score": 0.0,
        "total_candidates": 0,
        "accepted_count": 0,
        "accepted_human_count": 0,
        "review_count": 0,
        "review_human_count": 0,
        "rejected_count": 0,
        "rejected_human_count": 0,
        "filtered_out_count": 0,
        "first_seen": now,
        "last_seen": now,
        "last_updated": now,
    }


# ============================================================================
# Memory Initialization (one-time setup)
# ============================================================================


def initialize_memory_files() -> None:
    """Initialize empty memory files if they don't exist.

    Creates empty JSON objects in the memory directory.
    """
    _ensure_memory_dir()

    if not DOMAIN_MEMORY_PATH.exists():
        _save_json_atomic(DOMAIN_MEMORY_PATH, {})

    if not PATH_MEMORY_PATH.exists():
        _save_json_atomic(PATH_MEMORY_PATH, {})

    if not SOURCE_MEMORY_PATH.exists():
        _save_json_atomic(SOURCE_MEMORY_PATH, {})


# Ensure directories exist on module import
_ensure_memory_dir()
