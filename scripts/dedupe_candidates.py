"""Candidate deduplication module for V2 shared candidate layer.

This module handles deduplication of candidates across all lanes using
three levels of dedup:
- Level 1: URL hash exact match
- Level 2: Normalized title hash from same domain
- Level 3: Content fingerprint match
"""

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from scripts.candidate_utils import (
    BASE_DIR,
    compute_content_hash,
    compute_title_hash,
    compute_url_hash,
    load_candidate_index,
    normalize_title,
    save_candidate_index,
)


# Domain families for title dedup - domains that share content
DOMAIN_FAMILIES = {
    "federalreserve.gov": [
        "federalreserve.gov",
        "newyorkfed.org",
        "frbsf.org",
        "chicagofed.org",
        "dallasfed.org",
        "richmondfed.org",
    ],
    "ecb.europa.eu": ["ecb.europa.eu", "european-central-bank.eu"],
    "treasury.gov": ["treasury.gov", "fiscaldata.treasury.gov"],
    "bankofengland.co.uk": ["bankofengland.co.uk"],
    "bankofcanada.ca": ["bankofcanada.ca"],
}


def check_url_dedupe(candidate: dict[str, Any], index: dict[str, Any]) -> bool:
    """Level 1: If exact URL hash exists, skip.

    Args:
        candidate: Candidate dict with url field
        index: Candidate index dictionary

    Returns:
        True if duplicate (should skip), False otherwise
    """
    url = candidate.get("url", "")
    if not url:
        return False

    url_hash = compute_url_hash(url)
    seen_urls = index.get("seen_url_hashes", {})

    return url_hash in seen_urls


def check_title_dedupe(candidate: dict[str, Any], index: dict[str, Any]) -> bool:
    """Level 2: If normalized title hash exists recently from same domain or family, skip.

    We use a recency window of 30 days for title dedup.

    Args:
        candidate: Candidate dict with title and source.domain fields
        index: Candidate index dictionary

    Returns:
        True if duplicate (should skip), False otherwise
    """
    title = candidate.get("title", "")
    domain = candidate.get("source", {}).get("domain", "")

    if not title or not domain:
        return False

    normalized_title = normalize_title(title)
    title_hash = compute_title_hash(normalized_title)

    seen_titles = index.get("seen_title_hashes", {})

    # Get domain family for cross-domain dedup
    family = None
    for family_key, members in DOMAIN_FAMILIES.items():
        if domain in members:
            family = family_key
            break

    # Check if title hash exists for this domain or family
    for check_domain in [domain, family]:
        if check_domain and check_domain in seen_titles:
            domain_titles = seen_titles[check_domain]
            if title_hash in domain_titles:
                # Check if within recency window (30 days)
                entry = domain_titles[title_hash]
                if isinstance(entry, dict):
                    # New format with timestamp
                    import time

                    timestamp = entry.get("timestamp", 0)
                    if time.time() - timestamp < 30 * 24 * 60 * 60:
                        return True
                else:
                    # Legacy format - assume recent enough
                    return True

    return False


def check_content_dedupe(candidate: dict[str, Any], index: dict[str, Any]) -> bool:
    """Level 3: If content fingerprint matches, skip.

    Args:
        candidate: Candidate dict with raw_text_path field
        index: Candidate index dictionary

    Returns:
        True if duplicate (should skip), False otherwise
    """
    raw_text_path = candidate.get("raw_text_path", "")
    if not raw_text_path:
        return False

    # Read content from file
    text_path = Path(raw_text_path)
    if not text_path.exists():
        return False

    try:
        with open(text_path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception:
        return False

    content_hash = compute_content_hash(content)
    seen_content = index.get("seen_content_hashes", {})

    return content_hash in seen_content


def register_candidate(
    candidate: dict[str, Any], index: dict[str, Any]
) -> dict[str, Any]:
    """Register candidate in index, return updated index.

    Args:
        candidate: Candidate dict with url, title, raw_text_path fields
        index: Candidate index dictionary

    Returns:
        Updated index dictionary
    """
    import time

    url = candidate.get("url", "")
    title = candidate.get("title", "")
    raw_text_path = candidate.get("raw_text_path", "")
    domain = candidate.get("source", {}).get("domain", "")
    candidate_id = candidate.get("candidate_id", "")

    # Register URL hash
    if url:
        url_hash = compute_url_hash(url)
        if "seen_url_hashes" not in index:
            index["seen_url_hashes"] = {}
        index["seen_url_hashes"][url_hash] = {
            "candidate_id": candidate_id,
            "domain": domain,
            "timestamp": time.time(),
        }

    # Register title hash
    if title and domain:
        normalized_title = normalize_title(title)
        title_hash = compute_title_hash(normalized_title)

        if "seen_title_hashes" not in index:
            index["seen_title_hashes"] = {}

        # Get domain family
        family = None
        for family_key, members in DOMAIN_FAMILIES.items():
            if domain in members:
                family = family_key
                break

        # Register under domain and family
        for reg_domain in [domain, family]:
            if reg_domain:
                if reg_domain not in index["seen_title_hashes"]:
                    index["seen_title_hashes"][reg_domain] = {}
                index["seen_title_hashes"][reg_domain][title_hash] = {
                    "candidate_id": candidate_id,
                    "timestamp": time.time(),
                }

    # Register content hash
    if raw_text_path:
        text_path = Path(raw_text_path)
        if text_path.exists():
            try:
                with open(text_path, "r", encoding="utf-8") as f:
                    content = f.read()
                content_hash = compute_content_hash(content)

                if "seen_content_hashes" not in index:
                    index["seen_content_hashes"] = {}
                index["seen_content_hashes"][content_hash] = {
                    "candidate_id": candidate_id,
                    "timestamp": time.time(),
                }
            except Exception:
                pass

    # Register in candidate map
    if candidate_id:
        if "candidate_map" not in index:
            index["candidate_map"] = {}
        index["candidate_map"][candidate_id] = {
            "url": url,
            "title": title,
            "domain": domain,
            "registered_at": time.time(),
        }

    return index


def process_dedupe(
    candidates: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Main dedupe entry point.

    Processes candidates through all dedupe levels and returns
    survivors and duplicates.

    Args:
        candidates: List of candidate dicts, each must have:
            - candidate_id
            - url
            - title
            - raw_text_path

    Returns:
        Tuple of (survivors, duplicates) lists
    """
    index = load_candidate_index()
    survivors = []
    duplicates = []

    for candidate in candidates:
        candidate_id = candidate.get("candidate_id", "unknown")

        # Level 1: URL dedup
        if check_url_dedupe(candidate, index):
            candidate["dedupe_status"] = "url_duplicate"
            duplicates.append(candidate)
            continue

        # Level 2: Title dedup
        if check_title_dedupe(candidate, index):
            candidate["dedupe_status"] = "title_duplicate"
            duplicates.append(candidate)
            continue

        # Level 3: Content dedup
        if check_content_dedupe(candidate, index):
            candidate["dedupe_status"] = "content_duplicate"
            duplicates.append(candidate)
            continue

        # No duplicate found - register and add to survivors
        candidate["dedupe_status"] = "unique"
        index = register_candidate(candidate, index)
        survivors.append(candidate)

    # Save updated index
    save_candidate_index(index)

    return survivors, duplicates
