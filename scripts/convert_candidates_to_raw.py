"""Convert candidates to V1 raw record format.

This module converts surviving candidates into the V1 raw record format
for processing through the existing archive pipeline.
"""

import re
from pathlib import Path
from typing import Any

from scripts.candidate_utils import BASE_DIR


def candidate_to_raw_header(candidate: dict[str, Any]) -> str:
    """Generate V1-compatible header from candidate.

    The V1 raw record format is:
        TARGET: <target>
        TOPIC: <topic>
        TITLE: <title>
        URL: <url>
        LANE: <lane>
        DISCOVERY_METHOD: <method>
        DOMAIN_TRUST_TIER: <trust_tier>

    Args:
        candidate: Candidate dict with all relevant fields

    Returns:
        Header string with trailing newline
    """
    # Extract fields with defaults (use or to handle empty strings)
    topic = candidate.get("topic") or "other"
    title = candidate.get("title") or "Untitled"
    url = candidate.get("url") or ""
    lane = candidate.get("lane") or "unknown"
    discovery_method = candidate.get("source", {}).get("discovery_method") or "unknown"
    trust_tier = candidate.get("source", {}).get("trust_tier") or "low"

    # Build header
    header_lines = [
        f"TARGET: {topic}",
        f"TOPIC: {topic}",
        f"TITLE: {title}",
        f"URL: {url}",
        f"LANE: {lane}",
        f"DISCOVERY_METHOD: {discovery_method}",
        f"DOMAIN_TRUST_TIER: {trust_tier}",
    ]

    return "\n".join(header_lines) + "\n\n"


def convert_candidate(candidate: dict[str, Any]) -> Path:
    """Convert candidate to V1 raw record.

    Saves to data/raw/<candidate_id>.txt

    Args:
        candidate: Candidate dict to convert

    Returns:
        Path to created file

    Raises:
        ValueError: If candidate_id or raw_text_path is missing
    """
    candidate_id = candidate.get("candidate_id")
    if not candidate_id:
        raise ValueError("Candidate missing candidate_id")

    raw_text_path = candidate.get("raw_text_path")
    if not raw_text_path:
        raise ValueError("Candidate missing raw_text_path")

    # Read body content from raw_text_path
    text_path = Path(raw_text_path)
    if not text_path.exists():
        raise FileNotFoundError(f"Raw text file not found: {raw_text_path}")

    with open(text_path, "r", encoding="utf-8") as f:
        body_content = f.read()

    # Generate header
    header = candidate_to_raw_header(candidate)

    # Combine header and body
    raw_record = header + body_content

    # Write to data/raw/<candidate_id>.txt
    output_path = BASE_DIR / "data" / "raw" / f"{candidate_id}.txt"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(raw_record)

    return output_path


def convert_candidates(candidates: list[dict[str, Any]]) -> list[Path]:
    """Convert multiple candidates, return list of created paths.

    Args:
        candidates: List of candidate dicts to convert

    Returns:
        List of Path objects for created files
    """
    created_paths = []

    for candidate in candidates:
        try:
            path = convert_candidate(candidate)
            candidate["conversion_status"] = "converted"
            candidate["raw_record_path"] = str(path)
            created_paths.append(path)
        except Exception as e:
            candidate["conversion_status"] = f"failed: {str(e)}"

    return created_paths
