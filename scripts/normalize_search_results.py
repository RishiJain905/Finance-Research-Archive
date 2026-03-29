"""
Normalize search results from various providers (Tavily, etc.) into a consistent schema.

Provides:
- Single result normalization
- Batch result normalization
- Result validation
- Domain extraction from URLs
"""

import re
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse


def extract_domain(url: str) -> str:
    """
    Extract root domain from URL.

    Strips www. prefix and returns the domain without path.

    Args:
        url: Full URL (e.g., "https://www.newyorkfed.org/markets/...")

    Returns:
        Root domain string (e.g., "newyorkfed.org")
    """
    if not url:
        return ""

    try:
        parsed = urlparse(url)
        domain = parsed.netloc

        # Remove port number if present
        if ":" in domain:
            domain = domain.rsplit(":", 1)[0]

        # Strip www. prefix
        domain = re.sub(r"^www\.", "", domain)

        return domain
    except Exception:
        return ""


def validate_result(result: dict) -> bool:
    """
    Check that a normalized result has required fields.

    Validates:
    - URL is present and starts with http:// or https://
    - Title is present and not empty

    Args:
        result: Normalized result dictionary

    Returns:
        True if valid, False otherwise
    """
    if not result:
        return False

    # Check URL is valid
    url = result.get("url", "")
    if not url or not isinstance(url, str):
        return False
    if not url.startswith("http://") and not url.startswith("https://"):
        return False

    # Check title is not empty
    title = result.get("title", "")
    if not title or not isinstance(title, str):
        return False
    if not title.strip():
        return False

    return True


def _parse_published_date(published_date: Optional[str]) -> Optional[str]:
    """
    Parse various date formats and convert to ISO timestamp.

    Args:
        published_date: Date string in various formats (YYYY-MM-DD, RFC 3339, etc.)

    Returns:
        ISO format timestamp string or None if parsing fails
    """
    if not published_date:
        return None

    if isinstance(published_date, str):
        # Try common formats
        date_formats = [
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%d %H:%M:%S",
            "%B %d, %Y",
            "%b %d, %Y",
            "%d %B %Y",
            "%d %b %Y",
        ]

        for fmt in date_formats:
            try:
                parsed = datetime.strptime(published_date, fmt)
                return parsed.isoformat()
            except ValueError:
                continue

        # If already ISO format, return as-is if valid
        try:
            datetime.fromisoformat(published_date.replace("Z", "+00:00"))
            return published_date
        except ValueError:
            pass

    return None


def normalize_single_result(result: dict, provider: str) -> dict:
    """
    Normalize a single search result to the standard schema.

    Extracts and maps fields from raw provider results:
    - title, url, snippet -> as-is
    - source_domain -> extracted from URL
    - provider -> passed parameter
    - published_at -> parsed to ISO format if present

    Handles missing fields gracefully by returning empty strings.

    Args:
        result: Raw result dictionary from provider
        provider: Name of the provider (e.g., "tavily", "serpapi")

    Returns:
        Normalized result dictionary with consistent schema
    """
    # Extract raw fields with fallbacks
    raw_url = result.get("url") or result.get("link") or result.get("href") or ""
    raw_title = result.get("title") or result.get("name") or ""
    raw_snippet = (
        result.get("snippet")
        or result.get("description")
        or result.get("content")
        or result.get("excerpt")
        or ""
    )

    # Get published_date from various possible field names
    raw_published_date = (
        result.get("published_date")
        or result.get("published_at")
        or result.get("date")
        or result.get("created_at")
        or None
    )

    # Normalize to string
    url = str(raw_url).strip() if raw_url else ""
    title = str(raw_title).strip() if raw_title else ""
    snippet = str(raw_snippet).strip() if raw_snippet else ""

    # Extract source domain
    source_domain = extract_domain(url)

    # Parse published date to ISO format
    published_at = _parse_published_date(raw_published_date)

    return {
        "title": title,
        "url": url,
        "snippet": snippet,
        "source_domain": source_domain,
        "provider": provider,
        "published_at": published_at,
    }


def normalize_results(results: list[dict], provider: str) -> list[dict]:
    """
    Normalize a batch of search results.

    Filters out results with invalid or missing URLs.

    Args:
        results: List of raw result dictionaries from provider
        provider: Name of the provider

    Returns:
        List of normalized result dictionaries with valid URLs
    """
    normalized = []

    for result in results:
        if not isinstance(result, dict):
            continue

        normalized_result = normalize_single_result(result, provider)

        # Filter out results with invalid/missing URLs
        if normalized_result.get("url"):
            normalized.append(normalized_result)

    return normalized
