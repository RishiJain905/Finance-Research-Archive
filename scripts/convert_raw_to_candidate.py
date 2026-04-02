"""
Convert Raw Records to Candidates Module.

Converts raw ingestion records into candidate format for triage.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
CANDIDATES_DIR = BASE_DIR / "data" / "candidates" / "discovered"


def parse_raw_record(record_id: str, raw_dir: Path = None) -> Optional[dict]:
    """Parse a raw record file and extract content and metadata.

    Args:
        record_id: The record ID (filename without .txt)
        raw_dir: Directory containing raw records (defaults to RAW_DIR)

    Returns:
        Dict with content and metadata, or None if file doesn't exist
    """
    if raw_dir is None:
        raw_dir = RAW_DIR
    raw_path = raw_dir / f"{record_id}.txt"
    if not raw_path.exists():
        return None

    try:
        with open(raw_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Parse basic metadata from record_id
        # Format: {source}_{date} or similar
        metadata = {
            "record_id": record_id,
            "content": content,
            "word_count": len(content.split()),
        }

        return metadata
    except (IOError, UnicodeDecodeError):
        return None


def generate_candidate_id(record_id: str, title: str = "") -> str:
    """Generate a deterministic candidate ID.

    Args:
        record_id: The raw record ID
        title: Optional title for slug generation

    Returns:
        Candidate ID string
    """
    import hashlib

    # Create a short hash from record_id
    hash_part = hashlib.sha256(record_id.encode()).hexdigest()[:8]

    # Use title slug if provided
    if title:
        # Normalize title to slug
        slug = "".join(c if c.isalnum() else "_" for c in title.lower())[:20]
        return f"raw_{slug}_{hash_part}"

    return f"raw_{record_id}_{hash_part}"


def convert_raw_record_to_candidate(
    record_id: str, lane: str = "trusted_sources"
) -> Optional[dict]:
    """Convert a raw record to candidate format.

    Args:
        record_id: The raw record ID
        lane: Discovery lane (trusted_sources, keyword_discovery, seed_crawl, quant)

    Returns:
        Candidate dict or None if raw record doesn't exist
    """
    raw_data = parse_raw_record(record_id)
    if raw_data is None:
        return None

    content = raw_data.get("content", "")

    # Extract title from content (first line or pattern)
    title = ""
    if content:
        lines = content.strip().split("\n")
        if lines:
            title = lines[0].strip()

    # Determine source_type from record_id or content
    source_type = _infer_source_type(record_id, content)

    # Determine domain from record_id or content
    source_domain = _infer_source_domain(record_id, content)

    candidate_id = generate_candidate_id(record_id, title)

    discovered_at = datetime.now(timezone.utc).isoformat()

    candidate = {
        "candidate_id": candidate_id,
        "lane": lane,
        "source_name": source_domain or "unknown",
        "source_domain": source_domain or "unknown",
        "source_url": "",
        "discovered_at": discovered_at,
        "topic": _infer_topic(content),
        "title": title,
        "anchor_text": "",
        "raw_path": str(RAW_DIR / f"{record_id}.txt"),
        "source_type": source_type,
        "discovery_context": {
            "query": None,
            "seed_domain": None,
            "parent_url": None,
        },
    }

    return candidate


def _infer_source_type(record_id: str, content: str) -> str:
    """Infer source type from record_id or content."""
    record_lower = record_id.lower()
    content_lower = content.lower() if content else ""

    if "fred" in record_lower or "quant" in record_lower:
        return "quant_snapshot"
    if "speech" in record_lower:
        return "speech"
    if "press" in record_lower or "statement" in record_lower:
        return "press_release"
    if "research" in record_lower or "analysis" in record_lower:
        return "research_note"

    # Check content for indicators
    if "monetary policy" in content_lower or "fomc" in content_lower:
        return "policy_statement"

    return "article"


def _infer_source_domain(record_id: str, content: str) -> str:
    """Infer source domain from record_id or content."""
    record_lower = record_id.lower()

    # Check record_id patterns
    if "fed" in record_lower or "federal" in record_lower:
        return "federalreserve.gov"
    if "treasury" in record_lower:
        return "treasury.gov"
    if "ecb" in record_lower:
        return "ecb.europa.eu"
    if "fred" in record_lower:
        return "fred.stlouisfed.org"
    if "imf" in record_lower:
        return "imf.org"

    # Default
    return "unknown"


def _infer_topic(content: str) -> str:
    """Infer topic from content."""
    if not content:
        return "other"

    content_lower = content.lower()

    # Macro catalysts keywords
    macro_keywords = [
        "inflation",
        "interest rate",
        "fomc",
        "monetary policy",
        "gdp",
        "employment",
        "pce",
    ]
    for kw in macro_keywords:
        if kw in content_lower:
            return "macro catalysts"

    # Market structure keywords
    market_keywords = [
        "repo",
        "liquidity",
        "treasury",
        "yield curve",
        "volatility",
        "market structure",
    ]
    for kw in market_keywords:
        if kw in content_lower:
            return "market structure"

    return "other"


def convert_batch_raw_to_candidates(
    record_ids: list[str], lane: str = "trusted_sources"
) -> list[dict]:
    """Convert a batch of raw records to candidates.

    Args:
        record_ids: List of raw record IDs
        lane: Discovery lane

    Returns:
        List of candidate dicts (excludes None results for missing records)
    """
    candidates = []
    for record_id in record_ids:
        candidate = convert_raw_record_to_candidate(record_id, lane)
        if candidate is not None:
            candidates.append(candidate)
    return candidates


def save_candidate(candidate: dict) -> Path:
    """Save a candidate to disk.

    Args:
        candidate: Candidate dict

    Returns:
        Path where the candidate was saved
    """
    CANDIDATES_DIR.mkdir(parents=True, exist_ok=True)

    candidate_id = candidate.get("candidate_id", "unknown")
    save_path = CANDIDATES_DIR / f"{candidate_id}.json"

    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(candidate, f, indent=2, ensure_ascii=False)

    return save_path
