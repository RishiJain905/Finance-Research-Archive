"""
Extract internal links from seed sites with scoring.

Provides:
- fetch_page(): Fetch page content with BeautifulSoup, respect robots.txt
- extract_links(): Extract internal links with anchor text
- score_link(): Apply positive/negative scoring based on URL and anchor text
"""

import re
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import requests

# Fetch timeout in seconds
FETCH_TIMEOUT = 10

# HTTP headers for fetching
FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Positive URL hints (these boost score when found in URL)
POSITIVE_HINTS = [
    "press",
    "release",
    "statement",
    "speech",
    "testimony",
    "report",
    "bulletin",
    "commentary",
    "research",
    "staff-report",
    "market notice",
    "policy",
    "article",
]

# Negative URL hints (these reduce score when found in URL)
NEGATIVE_HINTS = [
    "about",
    "careers",
    "events",
    "experts",
    "people",
    "education",
    "programs",
    "museum",
    "archive",
    "category",
    "tag",
    "subscribe",
]


def robots_txt(url: str) -> bool:
    """
    Check if URL is allowed by robots.txt.

    Args:
        url: URL to check

    Returns:
        True if allowed, False if blocked
    """
    try:
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

        response = requests.get(
            robots_url, headers=FETCH_HEADERS, timeout=FETCH_TIMEOUT
        )

        if response.status_code != 200:
            # No robots.txt or can't access it - assume allowed
            return True

        robots_content = response.text

        # Simple robots.txt parsing
        # Check if there's a rule blocking this path
        user_agent = FETCH_HEADERS["User-Agent"]

        in_user_agent_block = False
        for line in robots_content.split("\n"):
            line = line.strip().lower()

            if line.startswith("user-agent:"):
                ua = line.split(":", 1)[1].strip()
                in_user_agent_block = ua == "*" or ua in user_agent.lower()
            elif line.startswith("disallow:"):
                if in_user_agent_block:
                    path = line.split(":", 1)[1].strip()
                    if path and path in parsed.path:
                        return False

        return True

    except Exception:
        # On any error, assume allowed
        return True


def fetch_page(url: str) -> Optional[BeautifulSoup]:
    """
    Fetch a page and return BeautifulSoup object.

    Args:
        url: URL to fetch

    Returns:
        BeautifulSoup object or None if fetch fails
    """
    # Check robots.txt first
    if not robots_txt(url):
        return None

    try:
        response = requests.get(url, headers=FETCH_HEADERS, timeout=FETCH_TIMEOUT)
        response.raise_for_status()

        # Verify content type
        content_type = response.headers.get("Content-Type", "")
        if "text/html" not in content_type and "application/xhtml" not in content_type:
            return None

        soup = BeautifulSoup(response.content, "html.parser")
        return soup

    except Exception:
        return None


def extract_links(soup: BeautifulSoup, base_url: str) -> List[Dict[str, Any]]:
    """
    Extract internal links from a BeautifulSoup object.

    Args:
        soup: BeautifulSoup object of the page
        base_url: Base URL for resolving relative links

    Returns:
        List of dicts with url and anchor_text for each link
    """
    links = []
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc

    # Find all anchor tags
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "")
        anchor_text = anchor.get_text(strip=True)

        # Skip empty hrefs
        if not href:
            continue

        # Resolve relative URLs
        full_url = urljoin(base_url, href)
        parsed_url = urlparse(full_url)

        # Only include internal links (same domain)
        if parsed_url.netloc != base_domain:
            continue

        # Skip javascript and other non-http(s) schemes
        if parsed_url.scheme not in ("http", "https"):
            continue

        # Normalize URL: use https, remove trailing slash
        normalized_url = full_url
        if normalized_url.endswith("/"):
            normalized_url = normalized_url[:-1]

        # Skip if already in links
        if any(link["url"] == normalized_url for link in links):
            continue

        links.append({"url": normalized_url, "anchor_text": anchor_text})

    return links


def score_link(url: str, anchor_text: str) -> float:
    """
    Score a link based on URL and anchor text.

    Positive hints in URL: +5 each
    Negative hints in URL: -5 each
    Positive hints in anchor: +3 each
    Negative hints in anchor: -3 each

    Args:
        url: Link URL
        anchor_text: Anchor text

    Returns:
        Score (positive = more likely to be relevant content)
    """
    url_lower = url.lower()
    anchor_lower = anchor_text.lower()
    combined = f"{url_lower} {anchor_lower}"

    score = 0.0

    # Check positive hints in URL
    for hint in POSITIVE_HINTS:
        if hint in url_lower:
            score += 5.0

    # Check negative hints in URL
    for hint in NEGATIVE_HINTS:
        if hint in url_lower:
            score -= 5.0

    # Check positive hints in anchor text
    for hint in POSITIVE_HINTS:
        if hint in anchor_lower:
            score += 3.0

    # Check negative hints in anchor text
    for hint in NEGATIVE_HINTS:
        if hint in anchor_lower:
            score -= 3.0

    return score


def score_and_filter_links(
    links: List[Dict[str, Any]], min_score: float = 0.0
) -> List[Dict[str, Any]]:
    """
    Score and filter links based on score threshold.

    Args:
        links: List of link dicts with url and anchor_text
        min_score: Minimum score to include

    Returns:
        Filtered list of links with scores added
    """
    scored_links = []

    for link in links:
        score = score_link(link["url"], link["anchor_text"])
        if score >= min_score:
            scored_links.append(
                {"url": link["url"], "anchor_text": link["anchor_text"], "score": score}
            )

    # Sort by score descending
    scored_links.sort(key=lambda x: x["score"], reverse=True)

    return scored_links
