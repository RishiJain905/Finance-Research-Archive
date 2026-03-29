# Set TAVILY_API_KEY environment variable before running.
# Add to GitHub Actions secrets: https://github.com/<user>/<repo>/settings/secrets

import os
from urllib.parse import urlparse
from typing import Any

from tavily import TavilyClient


def _get_tavily_client() -> TavilyClient:
    api_key = os.environ.get("TAVILY_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "TAVILY_API_KEY environment variable is not set. "
            "Please set it before running this script."
        )
    return TavilyClient(api_key=api_key)


def _normalize_date(date_value: str | None) -> str | None:
    """Convert Tavily date to ISO format. Returns None if not available."""
    if not date_value:
        return None
    return date_value


def _extract_domain(url: str) -> str:
    """Extract domain from URL."""
    parsed = urlparse(url)
    return parsed.netloc or ""


def _normalize_result(result: dict[str, Any]) -> dict[str, Any]:
    """Normalize a single Tavily result to the standard format."""
    return {
        "title": result.get("title", ""),
        "url": result.get("url", ""),
        "snippet": result.get("content", ""),
        "source_domain": _extract_domain(result.get("url", "")),
        "provider": "tavily",
        "published_at": _normalize_date(result.get("published_date")),
    }


def search_web(query: str, max_results: int = 10) -> list[dict[str, Any]]:
    """
    General web search via Tavily.

    Args:
        query: The search query.
        max_results: Maximum number of results to return (default: 10).

    Returns:
        List of normalized search results.

    Raises:
        Exception: If the Tavily API call fails.
    """
    client = _get_tavily_client()
    try:
        response = client.search(query=query, max_results=max_results)
        results = response.get("results", [])
        return [_normalize_result(r) for r in results]
    except Exception as e:
        raise RuntimeError(f"Tavily web search failed for query '{query}': {e}") from e


def search_news(query: str, max_results: int = 10) -> list[dict[str, Any]]:
    """
    News search via Tavily.

    Args:
        query: The search query.
        max_results: Maximum number of results to return (default: 10).

    Returns:
        List of normalized search results.

    Raises:
        Exception: If the Tavily API call fails.
    """
    client = _get_tavily_client()
    try:
        response = client.search(
            query=query, max_results=max_results, search_type="news"
        )
        results = response.get("results", [])
        return [_normalize_result(r) for r in results]
    except Exception as e:
        raise RuntimeError(f"Tavily news search failed for query '{query}': {e}") from e


def search_preferred_domains(
    query: str, domains: list[str], max_results: int = 10
) -> list[dict[str, Any]]:
    """
    Site-restricted search via Tavily.

    Attempts to use Tavily's domain filtering capabilities. If the API does not
    directly support domain parameters, results are filtered post-search to
    only include URLs from the specified domains.

    Args:
        query: The search query.
        domains: List of domain names to restrict search to (e.g., ["example.com"]).
        max_results: Maximum number of results to return (default: 10).

    Returns:
        List of normalized search results from the specified domains.

    Raises:
        Exception: If the Tavily API call fails.
    """
    client = _get_tavily_client()
    domain_set = {d.lower().strip() for d in domains}

    try:
        # Try Tavily's search_depth="basic" with domains if supported
        response = client.search(
            query=query,
            max_results=max_results,
            search_depth="basic",
            domains=domains,
        )
        results = response.get("results", [])
    except TypeError:
        # Tavily doesn't support domains parameter directly - fall back to basic search
        response = client.search(
            query=query, max_results=max_results, search_depth="basic"
        )
        results = response.get("results", [])

    # Filter results to only include URLs from specified domains
    filtered = []
    for r in results:
        url_domain = _extract_domain(r.get("url", "")).lower()
        if any(url_domain == d or url_domain.endswith(f".{d}") for d in domain_set):
            filtered.append(r)

    return [_normalize_result(r) for r in filtered]
