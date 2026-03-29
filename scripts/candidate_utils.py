"""
Candidate utility functions for V2 shared candidate layer.

Provides:
- Candidate ID building
- Hash helpers for deduplication
- Title normalization
- Candidate save/load
- Lane stats management
- Candidate index management
"""

import json
import hashlib
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# Base directory is the project root (two levels up from this file)
BASE_DIR = Path(__file__).resolve().parent.parent

# Manifest paths
CANDIDATE_MANIFESTS_DIR = BASE_DIR / "data" / "candidate_manifests"
CANDIDATE_INDEX_PATH = CANDIDATE_MANIFESTS_DIR / "candidate_index.json"
LANE_STATS_PATH = CANDIDATE_MANIFESTS_DIR / "lane_stats.json"

# Candidate folder paths
CANDIDATES_DIR = BASE_DIR / "data" / "candidates"
CANDIDATES_DISCOVERED_DIR = CANDIDATES_DIR / "discovered"
CANDIDATES_DEDUPED_DIR = CANDIDATES_DIR / "deduped_out"
CANDIDATES_FILTERED_DIR = CANDIDATES_DIR / "filtered_out"
CANDIDATES_CONVERTED_DIR = CANDIDATES_DIR / "converted"


def _ensure_directories():
    """Ensure all required directories exist."""
    CANDIDATE_MANIFESTS_DIR.mkdir(parents=True, exist_ok=True)
    CANDIDATES_DIR.mkdir(parents=True, exist_ok=True)
    CANDIDATES_DISCOVERED_DIR.mkdir(parents=True, exist_ok=True)
    CANDIDATES_DEDUPED_DIR.mkdir(parents=True, exist_ok=True)
    CANDIDATES_FILTERED_DIR.mkdir(parents=True, exist_ok=True)
    CANDIDATES_CONVERTED_DIR.mkdir(parents=True, exist_ok=True)


def _get_lock():
    """Get a thread lock for atomic file operations."""
    return threading.Lock()


# ============================================================================
# Hash Functions
# ============================================================================


def hash_url(url: str) -> str:
    """
    Generate a hash for a URL.

    Args:
        url: The URL to hash

    Returns:
        SHA256 hash of the normalized URL (first 16 chars)
    """
    # Normalize: strip trailing slash, lowercase
    normalized = url.rstrip("/").lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def hash_title(title: str) -> str:
    """
    Generate a hash for a normalized title.

    Args:
        title: The title to hash

    Returns:
        SHA256 hash of the normalized title (first 16 chars)
    """
    normalized = normalize_title(title)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def hash_content(text: str) -> str:
    """
    Generate a content fingerprint for deduplication.

    Args:
        text: The text content to fingerprint

    Returns:
        SHA256 fingerprint of normalized whitespace-collapsed text (first 16 chars)
    """
    # Normalize whitespace and collapse
    normalized = re.sub(r"\s+", " ", text).strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


# ============================================================================
# Title Normalization
# ============================================================================


def normalize_title(title: str) -> str:
    """
    Normalize a title for consistent hashing and comparison.

    Args:
        title: The title to normalize

    Returns:
        Lowercase, whitespace-collapsed, stripped title
    """
    # Lowercase, collapse whitespace, strip
    return re.sub(r"\s+", " ", title.strip().lower())


# ============================================================================
# Candidate ID Building
# ============================================================================


def build_candidate_id(lane: str, domain: str, title: str, url: str) -> str:
    """
    Build a deterministic candidate ID.

    Format: <lane>_<root_domain>_<short_title_slug>_<short_hash>

    Examples:
        - trusted_sources_federalreserve_press_release_12ab34cd
        - keyword_discovery_brookings_fed_repricing_98ef76aa
        - seed_crawl_newyorkfed_repo_liquidity_44dc190b

    Args:
        lane: Discovery lane (trusted_sources, keyword_discovery, seed_crawl)
        domain: Source domain (e.g., federalreserve.gov)
        title: Page title
        url: Full URL

    Returns:
        Candidate ID string
    """
    # Normalize domain: remove www., lowercase, strip TLD
    normalized_domain = re.sub(r"^www\.", "", domain.lower())
    # Strip TLD (e.g., .gov, .edu, .org)
    root_domain = re.sub(r"\.(gov|edu|org|com|ca|co\.uk|eu)$", "", normalized_domain)

    # Create title slug (first 3-5 significant words, max 25 chars total)
    normalized_title = normalize_title(title)
    title_words = normalized_title.split()
    title_slug_words = []
    char_count = 0
    for word in title_words:
        # Break if adding this word would exceed ~25 chars total
        if char_count + len(word) + len(title_slug_words) > 25:
            break
        title_slug_words.append(word)
    title_slug = "_".join(title_slug_words[:5])  # max 5 words

    # Generate short hash from URL
    url_hash = hash_url(url)

    return f"{lane}_{root_domain}_{title_slug}_{url_hash}"


# ============================================================================
# Candidate Save/Load
# ============================================================================


def save_candidate(candidate: Dict[str, Any], path: Path) -> None:
    """
    Save a candidate record to JSON file.

    Args:
        candidate: Candidate dictionary
        path: Path to save to (should be .json)
    """
    _ensure_directories()

    # Ensure parent directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(candidate, f, indent=2, ensure_ascii=False)


def load_candidate(path: Path) -> Dict[str, Any]:
    """
    Load a candidate record from JSON file.

    Args:
        path: Path to the candidate JSON file

    Returns:
        Candidate dictionary
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================================
# Lane Stats Management
# ============================================================================


def _load_json_atomic(path: Path) -> Dict[str, Any]:
    """Load JSON file with thread safety."""
    with _get_lock():
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)


def _save_json_atomic(path: Path, data: Dict[str, Any]) -> None:
    """Save JSON file with thread safety (write to temp then rename)."""
    _ensure_directories()
    with _get_lock():
        # Write to temp file first, then rename (atomic on most systems)
        temp_path = path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        temp_path.replace(path)


def get_lane_stats() -> Dict[str, Dict[str, int]]:
    """
    Get current lane statistics.

    Returns:
        Dictionary with stats for each lane
    """
    default_stats = {
        "trusted_sources": {
            "discovered": 0,
            "deduped_out": 0,
            "filtered_out": 0,
            "converted": 0,
        },
        "keyword_discovery": {
            "discovered": 0,
            "deduped_out": 0,
            "filtered_out": 0,
            "converted": 0,
        },
        "seed_crawl": {
            "discovered": 0,
            "deduped_out": 0,
            "filtered_out": 0,
            "converted": 0,
        },
    }

    if not LANE_STATS_PATH.exists():
        return default_stats

    try:
        stats = _load_json_atomic(LANE_STATS_PATH)
        # Ensure all lanes and stats exist
        for lane in default_stats:
            if lane not in stats:
                stats[lane] = default_stats[lane]
            else:
                for stat_name in default_stats[lane]:
                    if stat_name not in stats[lane]:
                        stats[lane][stat_name] = 0
        return stats
    except (json.JSONDecodeError, IOError):
        return default_stats


def update_lane_stats(lane: str, stat_name: str) -> None:
    """
    Atomically increment a lane stat.

    Args:
        lane: Lane name (trusted_sources, keyword_discovery, seed_crawl)
        stat_name: Stat to increment (discovered, deduped_out, filtered_out, converted)
    """
    valid_lanes = ["trusted_sources", "keyword_discovery", "seed_crawl"]
    valid_stats = ["discovered", "deduped_out", "filtered_out", "converted"]

    if lane not in valid_lanes:
        raise ValueError(f"Invalid lane: {lane}. Must be one of {valid_lanes}")
    if stat_name not in valid_stats:
        raise ValueError(f"Invalid stat: {stat_name}. Must be one of {valid_stats}")

    stats = get_lane_stats()
    stats[lane][stat_name] += 1
    _save_json_atomic(LANE_STATS_PATH, stats)


# ============================================================================
# Candidate Index Management
# ============================================================================


def get_candidate_index() -> Dict[str, Dict[str, Any]]:
    """
    Get the current candidate index.

    Returns:
        Dictionary with seen_url_hashes, seen_title_hashes, seen_content_hashes, candidate_map
    """
    default_index = {
        "seen_url_hashes": {},
        "seen_title_hashes": {},
        "seen_content_hashes": {},
        "candidate_map": {},
    }

    if not CANDIDATE_INDEX_PATH.exists():
        return default_index

    try:
        index = _load_json_atomic(CANDIDATE_INDEX_PATH)
        # Ensure all keys exist
        for key in default_index:
            if key not in index:
                index[key] = default_index[key]
        return index
    except (json.JSONDecodeError, IOError):
        return default_index


def update_candidate_index(
    url_hash: Optional[str] = None,
    title_hash: Optional[str] = None,
    content_hash: Optional[str] = None,
    candidate_id: Optional[str] = None,
    candidate_data: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Atomically update the candidate index with new hashes and/or candidate mapping.

    Args:
        url_hash: URL hash to mark as seen
        title_hash: Title hash to mark as seen
        content_hash: Content hash to mark as seen
        candidate_id: Candidate ID to add to map
        candidate_data: Full candidate data to store
    """
    index = get_candidate_index()

    if url_hash:
        index["seen_url_hashes"][url_hash] = datetime.utcnow().isoformat()
    if title_hash:
        index["seen_title_hashes"][title_hash] = datetime.utcnow().isoformat()
    if content_hash:
        index["seen_content_hashes"][content_hash] = datetime.utcnow().isoformat()
    if candidate_id and candidate_data:
        index["candidate_map"][candidate_id] = candidate_data

    _save_json_atomic(CANDIDATE_INDEX_PATH, index)


# ============================================================================
# Convenience Functions
# ============================================================================


def get_candidate_save_path(candidate_id: str, status: str) -> Path:
    """
    Get the appropriate save path for a candidate based on status.

    Args:
        candidate_id: The candidate's ID
        status: Current status (discovered, deduped_out, filtered_out, converted_to_raw, processed)

    Returns:
        Path where candidate should be saved
    """
    _ensure_directories()

    status_to_dir = {
        "discovered": CANDIDATES_DISCOVERED_DIR,
        "deduped_out": CANDIDATES_DEDUPED_DIR,
        "filtered_out": CANDIDATES_FILTERED_DIR,
        "converted_to_raw": CANDIDATES_CONVERTED_DIR,
        "processed": CANDIDATES_CONVERTED_DIR,  # Processed candidates go to converted
    }

    save_dir = status_to_dir.get(status, CANDIDATES_DISCOVERED_DIR)
    return save_dir / f"{candidate_id}.json"


def is_duplicate(url_hash: str, title_hash: str, content_hash: str) -> Dict[str, bool]:
    """
    Check if a candidate is a duplicate based on its hashes.

    Args:
        url_hash: Hash of the URL
        title_hash: Hash of the normalized title
        content_hash: Hash of the content

    Returns:
        Dict with 'url_duplicate', 'title_duplicate', 'content_duplicate' flags
    """
    index = get_candidate_index()

    return {
        "url_duplicate": url_hash in index["seen_url_hashes"],
        "title_duplicate": title_hash in index["seen_title_hashes"],
        "content_duplicate": content_hash in index["seen_content_hashes"],
    }


# Ensure directories exist on module import
_ensure_directories()
