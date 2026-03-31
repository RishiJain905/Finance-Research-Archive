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
    """Normalize a single Tavily result to the standard format.

    raw_content is Tavily's pre-fetched full page text (only present when
    include_raw_content=True is passed to the search call). It is preserved
    here so build_keyword_candidates can use it as a fallback when a direct
    HTTP fetch fails.
    """
    return {
        "title": result.get("title", ""),
        "url": result.get("url", ""),
        "snippet": result.get("content", ""),
        "source_domain": _extract_domain(result.get("url", "")),
        "provider": "tavily",
        "published_at": _normalize_date(result.get("published_date")),
        "tavily_content": result.get("raw_content", "") or "",
    }


def search_web(
    query: str,
    max_results: int = 10,
    days_back: int | None = None,
    search_type: str = "general",
) -> list[dict[str, Any]]:
    """
    General web search via Tavily.

    Args:
        query: The search query.
        max_results: Maximum number of results to return (default: 10).
        days_back: Restrict results to last N days (only applies when search_type="news").
        search_type: "news" uses Tavily news topic with recency filtering; "general" is default.

    Returns:
        List of normalized search results.

    Raises:
        Exception: If the Tavily API call fails.
    """
    client = _get_tavily_client()
    try:
        kwargs: dict[str, Any] = dict(
            query=query, max_results=max_results, include_raw_content=True
        )
        if search_type == "news":
            kwargs["topic"] = "news"
            if days_back:
                kwargs["days"] = days_back
        response = client.search(**kwargs)
        results = response.get("results", [])
        return [_normalize_result(r) for r in results]
    except Exception as e:
        raise RuntimeError(f"Tavily web search failed for query '{query}': {e}") from e


def search_news(
    query: str,
    max_results: int = 10,
    days_back: int | None = None,
) -> list[dict[str, Any]]:
    """
    News search via Tavily.

    Args:
        query: The search query.
        max_results: Maximum number of results to return (default: 10).
        days_back: Restrict results to last N days (default: no limit).

    Returns:
        List of normalized search results.

    Raises:
        Exception: If the Tavily API call fails.
    """
    client = _get_tavily_client()
    try:
        kwargs: dict[str, Any] = dict(
            query=query, max_results=max_results, topic="news", include_raw_content=True
        )
        if days_back:
            kwargs["days"] = days_back
        response = client.search(**kwargs)
        results = response.get("results", [])
        return [_normalize_result(r) for r in results]
    except Exception as e:
        raise RuntimeError(f"Tavily news search failed for query '{query}': {e}") from e


def search_preferred_domains(
    query: str,
    domains: list[str],
    max_results: int = 10,
    days_back: int | None = None,
    search_type: str = "general",
) -> list[dict[str, Any]]:
    """
    Site-restricted search via Tavily.

    Attempts to use Tavily's domain filtering with include_domains (falling back to
    legacy domains parameter). Results are also post-filtered to only include URLs
    from the specified domains.

    Args:
        query: The search query.
        domains: List of domain names to restrict search to (e.g., ["example.com"]).
        max_results: Maximum number of results to return (default: 10).
        days_back: Restrict results to last N days (only applies when search_type="news").
        search_type: "news" uses Tavily news topic with recency filtering; "general" is default.

    Returns:
        List of normalized search results from the specified domains.

    Raises:
        Exception: If the Tavily API call fails.
    """
    client = _get_tavily_client()
    domain_set = {d.lower().strip() for d in domains}

    base_kwargs: dict[str, Any] = dict(
        query=query, max_results=max_results, search_depth="basic", include_raw_content=True
    )
    if search_type == "news":
        base_kwargs["topic"] = "news"
        if days_back:
            base_kwargs["days"] = days_back

    results: list[dict[str, Any]] = []

    # Try include_domains (newer Tavily SDK), fall back to domains, then bare search
    for domain_kwarg in ({"include_domains": domains}, {"domains": domains}, {}):
        try:
            response = client.search(**base_kwargs, **domain_kwarg)
            results = response.get("results", [])
            break
        except TypeError:
            continue

    # Post-filter to only keep results from the requested domains
    filtered = [
        r for r in results
        if any(
            _extract_domain(r.get("url", "")).lower() == d
            or _extract_domain(r.get("url", "")).lower().endswith(f".{d}")
            for d in domain_set
        )
    ]

    return [_normalize_result(r) for r in filtered]
