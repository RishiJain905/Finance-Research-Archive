"""Build keyword discovery lane candidate records.

This module transforms normalized search results into candidate records,
applying domain blocking, required-term filtering, and preferred-domain scoring boosts.

Dependencies:
- Reads config/keyword_blocked_domains.json for blocked domains
- Uses scripts/candidate_utils.py for candidate ID generation and saving
- Uses scripts/discovery_providers.py for page content fetching (if needed)
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from scripts.candidate_utils import (
    BASE_DIR,
    generate_candidate_id,
    save_candidate_json,
)

# Configuration paths
BLOCKED_DOMAINS_PATH = BASE_DIR / "config" / "keyword_blocked_domains.json"
DOMAIN_TRUST_TIERS_PATH = BASE_DIR / "config" / "domain_trust_tiers.json"

# Candidate output directory
CANDIDATES_DISCOVERED_DIR = BASE_DIR / "data" / "candidates" / "discovered"

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


# ============================================================================
# Domain Blocking
# ============================================================================


def load_blocked_domains() -> set[str]:
    """
    Load and return blocked domains from config/keyword_blocked_domains.json.

    Returns:
        Set of blocked domain strings (without www. prefix)
    """
    if not BLOCKED_DOMAINS_PATH.exists():
        return set()

    with open(BLOCKED_DOMAINS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    domains = data.get("domains", [])
    # Normalize: remove www. prefix for matching
    return {re.sub(r"^www\.", "", d.lower()) for d in domains}


def extract_domain(url: str) -> str:
    """
    Extract domain from URL.

    Args:
        url: The URL to parse

    Returns:
        Domain string (lowercase, without www. prefix)
    """
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Remove www. prefix
        domain = re.sub(r"^www\.", "", domain)
        return domain
    except Exception:
        return ""


def is_blocked_domain(url: str, blocked_domains: set[str]) -> bool:
    """
    Check if URL's domain is in the blocked list.

    Args:
        url: The URL to check
        blocked_domains: Set of blocked domains

    Returns:
        True if domain (or www. variant) is blocked
    """
    domain = extract_domain(url)
    if not domain:
        return True  # Treat invalid URLs as blocked

    # Check direct match and www. variant
    return domain in blocked_domains or f"www.{domain}" in blocked_domains


# ============================================================================
# Required Term Filtering
# ============================================================================


def check_required_terms(text: str, required_terms: list[str]) -> bool:
    """
    Check if all required terms are present in text (case-insensitive).

    Args:
        text: Text to check
        required_terms: List of required term strings

    Returns:
        True if ALL required terms are found, or if required_terms is empty
    """
    if not required_terms:
        return True

    text_lower = text.lower()
    return all(term.lower() in text_lower for term in required_terms)


def should_fetch_page(title: str, snippet: str, required_terms: list[str]) -> bool:
    """
    Determine if page should be fetched for required-term validation.

    Per spec: only fetch if title or snippet already contains required terms
    (optimization to avoid unnecessary fetches).

    Args:
        title: Page title
        snippet: Search result snippet
        required_terms: List of required terms

    Returns:
        True if page should be fetched for double-check
    """
    if not required_terms:
        return False

    # Check if title or snippet already has the required terms
    combined_text = f"{title} {snippet}".lower()
    return all(term.lower() in combined_text for term in required_terms)


# ============================================================================
# Page Fetching
# ============================================================================


def fetch_and_extract_text(url: str) -> Optional[str]:
    """
    Fetch page content and extract article text.

    Args:
        url: URL to fetch

    Returns:
        Extracted article text, or None if fetch fails
    """
    try:
        response = requests.get(url, headers=FETCH_HEADERS, timeout=FETCH_TIMEOUT)
        response.raise_for_status()

        # Parse HTML
        soup = BeautifulSoup(response.content, "html.parser")

        # Remove script and style elements
        for element in soup(["script", "style", "nav", "header", "footer", "aside"]):
            element.decompose()

        # Try to find main content
        article = None
        for selector in [
            "article",
            "main",
            '[role="main"]',
            ".article",
            ".article-body",
            ".article-content",
            ".story-body",
            ".post-content",
            ".entry-content",
            ".content",
        ]:
            article = soup.select_one(selector)
            if article:
                break

        # Fall back to body
        if not article:
            article = soup.find("body")

        if not article:
            return None

        # Extract text and clean
        text = article.get_text(separator=" ")
        # Normalize whitespace
        text = re.sub(r"\s+", " ", text).strip()

        return text if text else None

    except Exception:
        return None


# ============================================================================
# Trust Tier Determination
# ============================================================================


def load_domain_trust_tiers() -> dict[str, list[str]]:
    """
    Load domain trust tiers from config.

    Returns:
        Dict with 'high', 'medium', 'low' keys mapping to domain lists
    """
    if not DOMAIN_TRUST_TIERS_PATH.exists():
        return {"high": [], "medium": [], "low": []}

    with open(DOMAIN_TRUST_TIERS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    return {
        "high": [d.lower() for d in data.get("high", [])],
        "medium": [d.lower() for d in data.get("medium", [])],
        "low": [d.lower() for d in data.get("low", [])],
    }


def determine_trust_tier(domain: str, preferred_domains: list[str]) -> str:
    """
    Determine trust tier for a domain based on preferred domains and trust tiers.

    Args:
        domain: Domain to check (without www. prefix)
        preferred_domains: List of preferred domains from query config

    Returns:
        Trust tier string: "high", "medium", or "low"
    """
    domain_lower = domain.lower()
    preferred_normalized = {re.sub(r"^www\.", "", d.lower()) for d in preferred_domains}

    # Preferred domains get high trust
    if (
        domain_lower in preferred_normalized
        or f"www.{domain_lower}" in preferred_normalized
    ):
        return "high"

    # Load trust tiers from config
    trust_tiers = load_domain_trust_tiers()

    # Check high trust
    high_domains = {re.sub(r"^www\.", "", d) for d in trust_tiers["high"]}
    if domain_lower in high_domains or f"www.{domain_lower}" in high_domains:
        return "high"

    # Check medium trust
    medium_domains = {re.sub(r"^www\.", "", d) for d in trust_tiers["medium"]}
    if domain_lower in medium_domains or f"www.{domain_lower}" in medium_domains:
        return "medium"

    # Everything else is low trust
    return "low"


# ============================================================================
# Candidate Building
# ============================================================================


def compute_keyword_scores(
    title: str,
    snippet: str,
    content: Optional[str],
    domain: str,
    preferred_domains: list[str],
    query_terms: list[str],
) -> dict[str, Any]:
    """
    Compute keyword-specific scoring signals.

    Args:
        title: Page title
        snippet: Search result snippet
        content: Extracted page content (if available)
        domain: Source domain
        preferred_domains: Preferred domains from query config
        query_terms: Terms from the search query

    Returns:
        Dict with candidate_scores including keyword-specific boosts
    """
    title_lower = title.lower()
    snippet_lower = snippet.lower()
    content_lower = (content or "").lower()
    domain_lower = domain.lower()

    scores: dict[str, Any] = {
        "preferred_domain": 0,
        "title_query_match": 0,
        "snippet_query_match": 0,
        "content_query_match": 0,
        "domain_trust_score": 0,
    }

    # Preferred domain boost
    preferred_normalized = {re.sub(r"^www\.", "", d.lower()) for d in preferred_domains}
    if (
        domain_lower in preferred_normalized
        or f"www.{domain_lower}" in preferred_normalized
    ):
        scores["preferred_domain"] = 1.0

    # Query term matches in title
    query_terms_lower = [t.lower() for t in query_terms]
    title_terms_found = sum(1 for t in query_terms_lower if t in title_lower)
    if query_terms:
        scores["title_query_match"] = title_terms_found / len(query_terms)

    # Query term matches in snippet
    snippet_terms_found = sum(1 for t in query_terms_lower if t in snippet_lower)
    if query_terms:
        scores["snippet_query_match"] = snippet_terms_found / len(query_terms)

    # Query term matches in content (if fetched)
    if content:
        content_terms_found = sum(1 for t in query_terms_lower if t in content_lower)
        scores["content_query_match"] = content_terms_found / len(query_terms)

    # Domain trust score
    trust_tier = determine_trust_tier(domain, preferred_domains)
    trust_scores = {"high": 1.0, "medium": 0.5, "low": 0.1}
    scores["domain_trust_score"] = trust_scores.get(trust_tier, 0.1)

    return scores


def build_candidate_from_result(
    result: dict[str, Any],
    query_config: dict[str, Any],
    lane: str = "keyword_discovery",
) -> Optional[dict[str, Any]]:
    """
    Build a candidate record from a normalized search result.

    Args:
        result: Normalized search result with title, url, snippet, source_domain
        query_config: Query configuration dict with required_terms, preferred_domains, etc.
        lane: Discovery lane identifier

    Returns:
        Candidate dict if valid, None if blocked or fails filtering
    """
    # Extract result fields
    title = result.get("title", "")
    url = result.get("url", "")
    snippet = result.get("snippet", "")
    source_domain = result.get("source_domain", extract_domain(url))

    if not url or not title:
        return None

    # Load blocked domains
    blocked_domains = load_blocked_domains()

    # Filter: blocked domain check (early)
    if is_blocked_domain(url, blocked_domains):
        return None

    # Extract query config fields
    required_terms = query_config.get("required_terms", [])
    preferred_domains = query_config.get("preferred_domains", [])
    query = query_config.get("query", "")
    # Parse query terms for scoring
    query_terms = query.split() if query else []

    # Filter: required terms in title/snippet (fast path)
    combined_title_snippet = f"{title} {snippet}"
    if required_terms and not check_required_terms(
        combined_title_snippet, required_terms
    ):
        return None

    # Optional: fetch page for required-term double-check
    page_content: Optional[str] = None
    if should_fetch_page(title, snippet, required_terms):
        page_content = fetch_and_extract_text(url)
        # If we fetched, verify required terms in content
        if page_content and required_terms:
            if not check_required_terms(page_content, required_terms):
                return None

    # Determine trust tier
    trust_tier = determine_trust_tier(source_domain, preferred_domains)

    # Generate candidate ID
    candidate_id = generate_candidate_id(
        lane=lane,
        domain=source_domain,
        title=title,
        url=url,
    )

    # Compute keyword-specific scores
    candidate_scores = compute_keyword_scores(
        title=title,
        snippet=snippet,
        content=page_content,
        domain=source_domain,
        preferred_domains=preferred_domains,
        query_terms=query_terms,
    )

    # Build candidate record
    discovered_at = datetime.now(timezone.utc).isoformat()

    candidate = {
        "candidate_id": candidate_id,
        "lane": lane,
        "discovered_at": discovered_at,
        "topic": query_config.get("topic", "unknown"),
        "source": {
            "domain": source_domain,
            "source_name": domain_to_source_name(source_domain),
            "url": url,
            "discovery_url": url,
            "discovery_method": "search",
            "trust_tier": trust_tier,
        },
        "title": title,
        "anchor_text": snippet,
        "raw_html_path": "",
        "raw_text_path": "",
        "metadata": {
            "http_status": 200,
            "content_type": "text/html",
            "published_at": result.get("published_at", discovered_at),
            "query_id": query_config.get("id", ""),
            "query_topic": query_config.get("topic", ""),
            "page_fetched": page_content is not None,
        },
        "candidate_scores": candidate_scores,
    }

    return candidate


def domain_to_source_name(domain: str) -> str:
    """
    Convert domain to a human-readable source name.

    Args:
        domain: Domain string

    Returns:
        Human-readable source name
    """
    # Remove common TLDs and clean up
    name = re.sub(r"\.(gov|edu|org|com|ca|co\.uk|eu)$", "", domain.lower())
    name = re.sub(r"^www\.", "", name)
    name = re.sub(r"[-_]", " ", name)
    # Title case
    name = name.title()
    return name


def build_keyword_candidates(
    results: list[dict[str, Any]],
    query_config: dict[str, Any],
    lane: str = "keyword_discovery",
) -> list[dict[str, Any]]:
    """
    Process multiple search results and build candidate records.

    Args:
        results: List of normalized search results
        query_config: Query configuration dict
        lane: Discovery lane identifier

    Returns:
        List of valid candidate dicts
    """
    candidates = []

    for result in results:
        candidate = build_candidate_from_result(result, query_config, lane)
        if candidate:
            candidates.append(candidate)

    return candidates


def save_keyword_candidates(
    candidates: list[dict[str, Any]],
    output_dir: Optional[Path] = None,
) -> list[Path]:
    """
    Save candidate records to JSON files.

    Args:
        candidates: List of candidate dicts to save
        output_dir: Directory to save to (defaults to discovered candidates dir)

    Returns:
        List of paths where candidates were saved
    """
    if output_dir is None:
        output_dir = CANDIDATES_DISCOVERED_DIR

    output_dir.mkdir(parents=True, exist_ok=True)

    saved_paths = []
    for candidate in candidates:
        candidate_id = candidate["candidate_id"]
        path = output_dir / f"{candidate_id}.json"
        save_candidate_json(candidate, path)
        saved_paths.append(path)

    return saved_paths


# ============================================================================
# Main Entry Point (for testing)
# ============================================================================


if __name__ == "__main__":
    # Example usage for testing
    import sys

    # Sample normalized result
    sample_result = {
        "title": "Repo Market Stress and Liquidity Conditions",
        "url": "https://www.newyorkfed.org/markets/reference-rates/rrp",
        "snippet": "Current repo market conditions show increased stress...",
        "source_domain": "newyorkfed.org",
        "provider": "example",
    }

    # Sample query config
    sample_query_config = {
        "id": "repo_stress",
        "topic": "market structure",
        "query": "repo market stress liquidity treasury funding",
        "required_terms": ["repo", "liquidity"],
        "preferred_domains": [
            "newyorkfed.org",
            "federalreserve.gov",
            "treasury.gov",
        ],
        "blocked_domains": [],
        "max_results": 10,
    }

    # Build candidate
    candidate = build_candidate_from_result(sample_result, sample_query_config)

    if candidate:
        print(f"Built candidate: {candidate['candidate_id']}")
        print(f"Trust tier: {candidate['source']['trust_tier']}")
        print(f"Scores: {candidate['candidate_scores']}")
    else:
        print("Failed to build candidate (blocked or filtered)")
        sys.exit(1)
