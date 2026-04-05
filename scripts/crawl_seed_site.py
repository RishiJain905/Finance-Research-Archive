"""
Seed site crawling module for V2 seed-crawl discovery lane.

Crawls seed sites using BFS queue, extracts internal links with scoring,
and generates candidate records following the candidate_record schema.
"""

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Set

from bs4 import BeautifulSoup

from scripts.crawl_queue import CrawlQueue, create_queue_item
from scripts.extract_internal_links import fetch_page, extract_links, score_link
from scripts.candidate_utils import (
    BASE_DIR,
    generate_candidate_id,
    build_candidate_id,
    hash_url,
    hash_title,
    load_candidate_index,
)
from scripts.build_keyword_candidates import domain_to_source_name

# URL patterns that indicate content pages (press releases, speeches, reports).
# Links whose URLs match at least one of these patterns get a score bonus.
_CONTENT_URL_PATTERNS = re.compile(
    r"(\d{4}[-/]\d{2}[-/]\d{2}"        # date in URL  e.g. 2026/03/28
    r"|/press[-_]?release"
    r"|/speech"
    r"|/statement"
    r"|/publication"
    r"|/report"
    r"|/working[-_]?paper"
    r"|/bulletin"
    r"|/minutes"
    r"|/testimony"
    r"|/transcript"
    r"|/article"
    r"|/blog"
    r"|/research"
    r")",
    re.IGNORECASE,
)

# Configuration paths
SEED_SITES_PATH = BASE_DIR / "config" / "seed_sites.json"
DOMAIN_TRUST_TIERS_PATH = BASE_DIR / "config" / "domain_trust_tiers.json"


def _load_recently_crawled_urls(window_days: int = 7) -> Set[str]:
    """Return the set of URL hashes seen within *window_days*.

    This lets the BFS skip pages that were already visited and processed
    recently, preventing the crawl from re-queuing and re-fetching the same
    navigation/content pages every single run.
    """
    try:
        index = load_candidate_index()
    except Exception:
        return set()

    cutoff = time.time() - window_days * 24 * 60 * 60
    seen: Set[str] = set()

    for url_hash, entry in index.get("seen_url_hashes", {}).items():
        if isinstance(entry, dict):
            ts = float(entry.get("timestamp", 0))
        elif isinstance(entry, str):
            try:
                from datetime import datetime as _dt
                ts = _dt.fromisoformat(entry.replace("Z", "+00:00")).timestamp()
            except (ValueError, AttributeError):
                ts = 0.0
        else:
            ts = 0.0

        if ts >= cutoff:
            seen.add(url_hash)

    return seen


def load_seed_sites() -> List[Dict[str, Any]]:
    """
    Load seed sites configuration.

    Returns:
        List of seed site configurations
    """
    if not SEED_SITES_PATH.exists():
        return []

    with open(SEED_SITES_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data.get("seeds", [])


def load_seed_config(seed_id: str) -> Optional[Dict[str, Any]]:
    """
    Load a specific seed site configuration.

    Args:
        seed_id: The seed site ID

    Returns:
        Seed configuration dict or None if not found
    """
    seeds = load_seed_sites()
    for seed in seeds:
        if seed.get("id") == seed_id:
            return seed
    return None


def load_domain_trust_tiers() -> Dict[str, List[str]]:
    """
    Load domain trust tiers.

    Returns:
        Dict with high, medium, low domain lists
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


def get_trust_tier(domain: str) -> str:
    """
    Determine trust tier for a domain.

    Args:
        domain: Domain to check

    Returns:
        Trust tier: "high", "medium", or "low"
    """
    domain_lower = domain.lower()
    trust_tiers = load_domain_trust_tiers()

    if domain_lower in trust_tiers["high"]:
        return "high"
    if domain_lower in trust_tiers["medium"]:
        return "medium"
    return "low"


def is_url_in_scope(
    url: str, allowed_prefixes: List[str], blocked_fragments: List[str]
) -> bool:
    """
    Check if URL is within scope for crawling.

    Args:
        url: URL to check
        allowed_prefixes: List of allowed URL prefixes
        blocked_fragments: List of blocked URL fragments

    Returns:
        True if URL is in scope, False otherwise
    """
    url_lower = url.lower()

    # Check blocked fragments first (higher priority)
    for fragment in blocked_fragments:
        if fragment.lower() in url_lower:
            return False

    # Check allowed prefixes
    if not allowed_prefixes:
        return True

    for prefix in allowed_prefixes:
        if url_lower.startswith(prefix.lower()):
            return True

    return False


def extract_page_title(soup: BeautifulSoup) -> str:
    """
    Extract page title from BeautifulSoup.

    Args:
        soup: BeautifulSoup object

    Returns:
        Page title string
    """
    # Try og:title first
    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        return og_title["content"].strip()

    # Try regular title tag
    title_tag = soup.find("title")
    if title_tag:
        return title_tag.get_text(strip=True)

    # Fall back to h1
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(strip=True)

    return ""


def create_candidate_record(
    crawl_item: Dict[str, Any], seed_config: Dict[str, Any], title: str = ""
) -> Dict[str, Any]:
    """
    Create a candidate record from a crawl item.

    Args:
        crawl_item: Queue item with url, depth, score, parent_url, anchor_text
        seed_config: Seed site configuration
        title: Page title (optional, extracted if not provided)

    Returns:
        Candidate record dict following candidate_record schema
    """
    url = crawl_item["url"]
    domain = seed_config.get("domain", "")

    # Generate candidate ID
    candidate_id = build_candidate_id(
        lane="seed_crawl", domain=domain, title=title or url, url=url
    )

    # Determine trust tier
    trust_tier = get_trust_tier(domain)

    # Get topic from seed config
    topic = seed_config.get("topic", "other")

    discovered_at = datetime.now(timezone.utc).isoformat()

    # Build dedupe hashes (content_hash is empty since we don't have content yet)
    dedupe = {
        "url_hash": hash_url(url),
        "normalized_title_hash": hash_title(title) if title else "",
        "content_hash": "",  # Will be computed later when content is fetched
    }

    candidate = {
        "candidate_id": candidate_id,
        "lane": "seed_crawl",
        "discovered_at": discovered_at,
        "topic": topic,
        "source": {
            "domain": domain,
            "source_name": domain_to_source_name(domain),
            "url": url,
            "discovery_url": crawl_item.get("parent_url", url),
            "discovery_method": "crawl",
            "trust_tier": trust_tier,
        },
        "title": title or url,
        "anchor_text": crawl_item.get("anchor_text", ""),
        "raw_html_path": "",
        "raw_text_path": "",
        "metadata": {
            "http_status": 200,
            "content_type": "text/html",
            "published_at": discovered_at,
            "language": "en",
            "word_count": 0,
            "crawl_depth": crawl_item.get("depth", 0),
            "crawl_score": crawl_item.get("score", 0),
        },
        "candidate_scores": {
            "url_score": 0,
            "anchor_score": 0,
            "domain_trust_score": 0,
            "keyword_score": 0,
            "freshness_score": 0,
            "total_score": 0,
        },
        "dedupe": dedupe,
        "status": "discovered",
        "notes": f"Discovered via seed crawl from {seed_config.get('id', 'unknown')}",
    }

    return candidate


def crawl_seed(seed_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Crawl a seed site and return discovered candidates.

    Args:
        seed_config: Seed site configuration dict with:
            - id: Unique identifier
            - enabled: Whether to crawl
            - domain: Primary domain
            - start_urls: List of starting URLs
            - allowed_prefixes: List of allowed URL prefixes
            - blocked_fragments: List of blocked URL fragments
            - max_depth: Maximum crawl depth
            - max_pages: Maximum pages to crawl
            - topic: Topic category

    Returns:
        List of candidate records
    """
    # Skip disabled seeds
    if not seed_config.get("enabled", True):
        return []

    seed_id = seed_config.get("id", "unknown")
    domain = seed_config.get("domain", "")
    start_urls = seed_config.get("start_urls", [])
    allowed_prefixes = seed_config.get("allowed_prefixes", [])
    blocked_fragments = seed_config.get("blocked_fragments", [])
    max_depth = seed_config.get("max_depth", 2)
    max_pages = seed_config.get("max_pages", 20)

    # Track URLs visited within this single BFS run to avoid fetching the
    # same page twice if multiple parent pages link to it.  Cross-run
    # deduplication is handled downstream by process_dedupe, which checks
    # the persistent candidate index; using the persistent index here was
    # causing the BFS to skip all depth>=1 links on every repeat run,
    # preventing new content from ever being discovered.
    recently_crawled: Set[str] = set()

    # Initialize queue
    queue = CrawlQueue(max_pages=max_pages, max_depth=max_depth)

    # Seed the queue with start URLs at depth 0.
    for start_url in start_urls:
        item = create_queue_item(
            url=start_url,
            depth=0,
            score=10.0,
            parent_url="",
            anchor_text="Start URL",
        )
        queue.enqueue(item)

    candidates = []

    # BFS crawl
    while not queue.is_empty():
        # Check page limit
        if queue.has_reached_page_limit():
            break

        # Dequeue next item
        crawl_item = queue.dequeue()
        if crawl_item is None:
            break

        url = crawl_item["url"]

        # Check scope rules
        if not is_url_in_scope(url, allowed_prefixes, blocked_fragments):
            continue

        # Skip depth>=1 pages already fetched in this run (e.g., linked
        # from multiple parents).  Start URLs (depth=0) are always fetched.
        url_hash = hash_url(url)
        if url_hash in recently_crawled and crawl_item.get("depth", 0) > 0:
            continue

        # Fetch page
        soup = fetch_page(url)
        if soup is None:
            continue

        # Extract title
        title = extract_page_title(soup)

        # Create candidate record
        candidate = create_candidate_record(crawl_item, seed_config, title)
        candidates.append(candidate)

        # Mark this URL as crawled in the local set so we don't re-emit it
        # if it's linked from multiple parent pages in the same run.
        recently_crawled.add(url_hash)

        # Extract and score internal links
        links = extract_links(soup, url)

        for link in links:
            link_url = link["url"]
            anchor_text = link["anchor_text"]

            # Skip if not in scope
            if not is_url_in_scope(link_url, allowed_prefixes, blocked_fragments):
                continue

            # Skip links to recently crawled pages early to keep the queue lean.
            if hash_url(link_url) in recently_crawled:
                continue

            # Score the link; boost content-type URLs so they are prioritised
            # over generic navigation pages.
            score = score_link(link_url, anchor_text)
            if _CONTENT_URL_PATTERNS.search(link_url):
                score += 5.0

            # Create queue item for next depth
            next_item = create_queue_item(
                url=link_url,
                depth=crawl_item["depth"] + 1,
                score=score,
                parent_url=url,
                anchor_text=anchor_text,
            )

            queue.enqueue(next_item)

    return candidates


def crawl_all_seeds() -> Dict[str, List[Dict[str, Any]]]:
    """
    Crawl all enabled seed sites.

    Returns:
        Dict mapping seed_id to list of candidates
    """
    seeds = load_seed_sites()
    results = {}

    for seed in seeds:
        seed_id = seed.get("id", "unknown")
        if not seed.get("enabled", True):
            continue

        candidates = crawl_seed(seed)
        results[seed_id] = candidates

    return results


# ============================================================================
# Main Entry Point
# ============================================================================


if __name__ == "__main__":
    import sys

    # Load and display seed sites
    seeds = load_seed_sites()

    print(f"Loaded {len(seeds)} seed sites:")
    for seed in seeds:
        status = "ENABLED" if seed.get("enabled") else "DISABLED"
        print(f"  - {seed['id']}: {status}")
        print(f"    Domain: {seed['domain']}")
        print(f"    URLs: {len(seed.get('start_urls', []))} start URLs")
        print(
            f"    Max depth: {seed.get('max_depth')}, Max pages: {seed.get('max_pages')}"
        )
        print()
