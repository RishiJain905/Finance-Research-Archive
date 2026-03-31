"""Candidate deduplication module for V2 shared candidate layer.

This module handles deduplication of candidates across all lanes using
three levels of dedup:
- Level 1: URL hash exact match (7-day window)
- Level 2: Normalized title hash from same domain (30-day window)
- Level 3: Content fingerprint match (14-day window)

All windows are time-bounded so the same article becomes eligible again
after the window expires, preventing permanent stale blocking.
"""

import hashlib
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from scripts.candidate_utils import (
    BASE_DIR,
    compute_content_hash,
    compute_title_hash,
    compute_url_hash,
    load_candidate_index,
    normalize_title,
    save_candidate_index,
)

# Dedup window lengths in seconds
URL_DEDUP_WINDOW_DAYS = 7
CONTENT_DEDUP_WINDOW_DAYS = 14
# Reduced from 30 → 14 days: 30-day title windows blocked the same content
# sources every single run since the same pages are crawled each time.
TITLE_DEDUP_WINDOW_DAYS = 14


# Domain families for title dedup - only truly shared-content sources.
# Removed regional Fed banks (newyorkfed, frbsf, etc.) since they publish
# distinct content; grouping them caused false-positive title duplicates.
DOMAIN_FAMILIES = {
    "ecb.europa.eu": ["ecb.europa.eu", "european-central-bank.eu"],
    "treasury.gov": ["treasury.gov", "fiscaldata.treasury.gov"],
}


def _get_candidate_url(candidate: dict[str, Any]) -> str:
    """Return the canonical URL for a candidate.

    Candidates store the URL at ``source.url`` (not a top-level ``url``
    field).  Fall back to the top-level field so the function works with
    both the current schema and any legacy records that do have a top-level
    ``url``.
    """
    return (
        candidate.get("source", {}).get("url", "")
        or candidate.get("url", "")
    )


def _parse_timestamp_entry(entry: Any) -> float:
    """Extract a Unix timestamp from a dedup index entry.

    New entries are dicts: ``{"timestamp": <float>, ...}``.
    Legacy entries are ISO-8601 strings written by the old
    ``update_candidate_index`` helper.  Previously, legacy strings were
    blindly treated as "recent", which caused *everything* to look like a
    duplicate after a single run.  We now parse the string properly.
    """
    if isinstance(entry, dict):
        return float(entry.get("timestamp", 0))
    if isinstance(entry, str):
        try:
            dt = datetime.fromisoformat(entry.replace("Z", "+00:00"))
            return dt.timestamp()
        except (ValueError, AttributeError):
            return 0.0
    return 0.0


def check_url_dedupe(candidate: dict[str, Any], index: dict[str, Any]) -> bool:
    """Level 1: If exact URL hash was seen within URL_DEDUP_WINDOW_DAYS, skip.

    Args:
        candidate: Candidate dict.  URL is read from ``source.url`` (with
            fallback to the legacy top-level ``url`` field).
        index: Candidate index dictionary

    Returns:
        True if duplicate (should skip), False otherwise
    """
    url = _get_candidate_url(candidate)
    if not url:
        return False

    url_hash = compute_url_hash(url)
    seen_urls = index.get("seen_url_hashes", {})

    if url_hash not in seen_urls:
        return False

    timestamp = _parse_timestamp_entry(seen_urls[url_hash])
    return time.time() - timestamp < URL_DEDUP_WINDOW_DAYS * 24 * 60 * 60


def check_title_dedupe(candidate: dict[str, Any], index: dict[str, Any]) -> bool:
    """Level 2: If normalized title hash exists within TITLE_DEDUP_WINDOW_DAYS from same domain/family, skip.

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

    for check_domain in [domain, family]:
        if check_domain and check_domain in seen_titles:
            domain_titles = seen_titles[check_domain]
            if title_hash in domain_titles:
                timestamp = _parse_timestamp_entry(domain_titles[title_hash])
                if time.time() - timestamp < TITLE_DEDUP_WINDOW_DAYS * 24 * 60 * 60:
                    return True

    return False


def check_content_dedupe(candidate: dict[str, Any], index: dict[str, Any]) -> bool:
    """Level 3: If content fingerprint was seen within CONTENT_DEDUP_WINDOW_DAYS, skip.

    Args:
        candidate: Candidate dict with raw_text_path field
        index: Candidate index dictionary

    Returns:
        True if duplicate (should skip), False otherwise
    """
    raw_text_path = candidate.get("raw_text_path", "")
    if not raw_text_path:
        return False

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

    if content_hash not in seen_content:
        return False

    timestamp = _parse_timestamp_entry(seen_content[content_hash])
    return time.time() - timestamp < CONTENT_DEDUP_WINDOW_DAYS * 24 * 60 * 60


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
    url = _get_candidate_url(candidate)
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


def prune_index(index: dict[str, Any]) -> dict[str, Any]:
    """Remove expired entries from the candidate index.

    Expired entries are those older than their respective window:
    - URL hashes: older than URL_DEDUP_WINDOW_DAYS
    - Title hashes: older than TITLE_DEDUP_WINDOW_DAYS
    - Content hashes: older than CONTENT_DEDUP_WINDOW_DAYS

    Args:
        index: Candidate index dictionary

    Returns:
        Pruned index dictionary
    """
    now = time.time()

    # Prune URL hashes
    url_cutoff = now - URL_DEDUP_WINDOW_DAYS * 24 * 60 * 60
    seen_urls = index.get("seen_url_hashes", {})
    pruned_urls = {
        k: v for k, v in seen_urls.items()
        if not isinstance(v, dict) or v.get("timestamp", 0) >= url_cutoff
    }
    url_pruned = len(seen_urls) - len(pruned_urls)
    index["seen_url_hashes"] = pruned_urls

    # Prune title hashes (nested by domain)
    title_cutoff = now - TITLE_DEDUP_WINDOW_DAYS * 24 * 60 * 60
    seen_titles = index.get("seen_title_hashes", {})
    title_pruned = 0
    for domain in list(seen_titles.keys()):
        before = len(seen_titles[domain])
        seen_titles[domain] = {
            k: v for k, v in seen_titles[domain].items()
            if not isinstance(v, dict) or v.get("timestamp", 0) >= title_cutoff
        }
        title_pruned += before - len(seen_titles[domain])
        if not seen_titles[domain]:
            del seen_titles[domain]
    index["seen_title_hashes"] = seen_titles

    # Prune content hashes
    content_cutoff = now - CONTENT_DEDUP_WINDOW_DAYS * 24 * 60 * 60
    seen_content = index.get("seen_content_hashes", {})
    pruned_content = {
        k: v for k, v in seen_content.items()
        if not isinstance(v, dict) or v.get("timestamp", 0) >= content_cutoff
    }
    content_pruned = len(seen_content) - len(pruned_content)
    index["seen_content_hashes"] = pruned_content

    total_pruned = url_pruned + title_pruned + content_pruned
    if total_pruned:
        print(
            f"[Dedup] Pruned {total_pruned} expired index entries "
            f"(url={url_pruned}, title={title_pruned}, content={content_pruned})"
        )

    return index


def process_dedupe(
    candidates: list[dict[str, Any]],
    lane: Optional[str] = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Main dedupe entry point.

    Prunes expired index entries, then processes candidates through all
    dedupe levels and returns survivors and duplicates.

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

    # Remove expired entries before checking so windows are respected
    index = prune_index(index)

    survivors = []
    duplicates = []
    url_dups = 0
    title_dups = 0
    content_dups = 0

    for candidate in candidates:
        # Level 1: URL dedup
        if check_url_dedupe(candidate, index):
            candidate["dedupe_status"] = "url_duplicate"
            duplicates.append(candidate)
            url_dups += 1
            continue

        # Level 2: Title dedup
        if check_title_dedupe(candidate, index):
            candidate["dedupe_status"] = "title_duplicate"
            duplicates.append(candidate)
            title_dups += 1
            continue

        # Level 3: Content dedup
        if check_content_dedupe(candidate, index):
            candidate["dedupe_status"] = "content_duplicate"
            duplicates.append(candidate)
            content_dups += 1
            continue

        # No duplicate found — register and add to survivors
        candidate["dedupe_status"] = "unique"
        index = register_candidate(candidate, index)
        survivors.append(candidate)

    # Save updated index
    save_candidate_index(index)

    total = len(candidates)
    if duplicates:
        print(
            f"[Dedup] Duplicate breakdown: url={url_dups}, "
            f"title={title_dups}, content={content_dups} "
            f"(out of {total})"
        )

    return survivors, duplicates
