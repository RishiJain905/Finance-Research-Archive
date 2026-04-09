"""arXiv academic papers ingestion for the Finance Research Archive.

Queries the arXiv REST API for quantitative finance categories, extracts
abstracts and metadata, and writes raw records in the standard pipeline format.

arXiv papers are keyed by arXiv paper ID (e.g. 2403.12345) to avoid
duplicate records on re-runs. Full PDFs are NOT fetched — the abstract
is sufficient for the summarizer and keeps records lean.
"""

import json
import re
import sys
import time
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlencode
import xml.etree.ElementTree as ET

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from ingest_sources import (
    RAW_DIR,
    build_raw_record_text,
    build_record_id,
    classify_page_type,
    collapse_text_lines,
    content_hash,
    detect_language,
    extract_canonical_url,
    extract_published_at,
    extract_title,
    fetch_html,
    get_headers,
    load_json,
    sanitize_title,
    title_fingerprint,
)
from scripts.manifest_db import (
    add_fingerprint,
    ensure_schema,
    get_fingerprint_record_id,
    get_url_content_hash,
    is_url_processed_as_article,
    set_record_map,
    set_record_rules,
    upsert_seen_url,
)
import requests

CONFIG_PATH = BASE_DIR / "config" / "academic_sources.json"
ARXIV_API_BASE = "http://export.arxiv.org/api/query"
DEFAULT_LOOKBACK_DAYS = 5
DEFAULT_MAX_PER_CATEGORY = 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_arxiv_id_from_url(url: str) -> str:
    """Extract arXiv paper ID from an arXiv abstract page URL."""
    # URL patterns:
    #   https://arxiv.org/abs/2403.12345
    #   https://arxiv.org/abs/2301.01234v2
    match = re.search(r"arxiv\.org/abs/([0-9]{4}\.[0-9]+)", url)
    if match:
        return match.group(1)
    return ""


def fetch_arxiv_feed(
    category: str,
    lookback_days: int,
    max_results: int,
) -> list[dict]:
    """Query the arXiv API for a single category and return parsed entries.

    Returns a list of dicts with keys: id, title, summary, authors,
    published_date, primary_category, url (abstract page).
    """
    params = {
        "search_query": f"cat:{category}",
        "sortBy": "submittedDate",
        "sortOrder": "descending",
        "max_results": max_results,
    }
    url = f"{ARXIV_API_BASE}?{urlencode(params)}"
    headers = get_headers()
    headers["Accept"] = "application/atom+xml"

    response = requests.get(url, timeout=30, headers=headers)
    response.raise_for_status()

    return parse_atom_feed(response.text, lookback_days)


def parse_atom_feed(xml_text: str, lookback_days: int) -> list[dict]:
    """Parse arXiv Atom feed XML into a list of paper dicts using stdlib xml."""
    root = ET.fromstring(xml_text)
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    ns = {"atom": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}
    entries = []

    for entry in root.findall("atom:entry", ns):
        published_el = entry.find("atom:published", ns)
        if published_el is None or not published_el.text:
            continue
        try:
            published = datetime.fromisoformat(published_el.text.strip())
        except ValueError:
            continue

        if published.tzinfo is None:
            published = published.replace(tzinfo=timezone.utc)

        if published < cutoff:
            continue

        id_el = entry.find("atom:id", ns)
        paper_url = id_el.text.strip() if id_el is not None and id_el.text else ""
        paper_id = parse_arxiv_id_from_url(paper_url)
        if not paper_id:
            continue

        title_el = entry.find("atom:title", ns)
        title = sanitize_title(title_el.text.strip()) if title_el is not None and title_el.text else "Unknown"

        summary_el = entry.find("atom:summary", ns)
        abstract = collapse_text_lines(summary_el.text.strip()) if summary_el is not None and summary_el.text else ""

        author_names = [
            a.find("atom:name", ns).text.strip()
            for a in entry.findall("atom:author", ns)
            if a.find("atom:name", ns) is not None and a.find("atom:name", ns).text
        ]

        primary_cat_el = entry.find("arxiv:primary_category", ns)
        primary_category = primary_cat_el.get("term", "") if primary_cat_el is not None else ""

        entries.append({
            "id": paper_id,
            "title": title,
            "summary": abstract,
            "authors": author_names,
            "published_date": published.isoformat(),
            "primary_category": primary_category,
            "url": paper_url,
        })

    return entries


def build_arxiv_record_text(
    paper: dict,
    topic: str,
    name: str,
) -> str:
    """Build a raw record text file content for an arXiv paper."""
    article_url = paper["url"]
    article_title = paper["title"]
    published_at = paper["published_date"]
    article_text = paper["summary"]
    detected_language = detect_language(article_text)

    record_id = build_record_id(name, article_title, article_url)

    title_fp = title_fingerprint(article_title)
    content_fp = title_fingerprint(article_text[:5000])

    # Build metadata block (matches ingest_sources raw record format)
    metadata = {
        "TARGET": name,
        "TOPIC": topic,
        "TITLE": article_title,
        "URL": article_url,
        "INDEX_URL": article_url,
        "ARTICLE_URL": article_url,
        "CANONICAL_URL": article_url,
        "PAGE_TITLE": article_title,
        "H1": article_title,
        "PUBLISHED_AT": published_at,
        "PAGE_TYPE": "academic_paper",
        "EXPECTED_LANGUAGE": "en",
        "DETECTED_LANGUAGE": detected_language,
        "CONTENT_WORD_COUNT": str(len(article_text.split())),
        "INGEST_SOURCE": "arxiv",
        "SOURCE_TYPE": "academic",
        "PAPER_ID": paper["id"],
        "PRIMARY_CATEGORY": paper["primary_category"],
        "AUTHORS": "; ".join(paper["authors"]),
    }

    return build_raw_record_text(metadata, article_text)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> list[str]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    ensure_schema()

    config = load_json(CONFIG_PATH, {"arxiv": {}, "ssrn": {}})

    created: list[str] = []

    if not config.get("arxiv", {}).get("enabled", True):
        print("arXiv ingestion disabled in config.")
    else:
        created = _run_arxiv(config["arxiv"])

    # SSRN is handled by the existing RSS ingest — nothing to do here.
    if config.get("ssrn", {}).get("enabled", True):
        print("SSRN ingestion handled by ingest_rss.py in the normal pipeline run.")

    print("\narXiv ingest complete.")
    return created


def _run_arxiv(arxiv_config: dict) -> list[str]:
    """Run arXiv ingestion and return list of created record IDs."""
    lookback_days = arxiv_config.get("lookback_days", DEFAULT_LOOKBACK_DAYS)
    max_per_category = arxiv_config.get(
        "max_results_per_category", DEFAULT_MAX_PER_CATEGORY
    )
    categories = arxiv_config.get(
        "categories",
        ["q-fin.RM", "q-fin.PM", "q-fin.MF", "q-fin.EC", "q-fin.TR", "econ.GN"],
    )

    created: list[str] = []
    current_year = date.today().year

    for category in categories:
        print(f"\narXiv: fetching category {category}")

        try:
            papers = fetch_arxiv_feed(category, lookback_days, max_per_category)
        except Exception as e:
            print(f"  Failed to fetch {category}: {e}")
            continue

        print(f"  Found {len(papers)} papers from last {lookback_days} days")

        for paper in papers:
            paper_id = paper["id"]
            paper_url = paper["url"]

            # Check for existing URL dedup
            if is_url_processed_as_article(paper_url):
                print(f"  Already processed {paper_id}, skipping.")
                continue

            name = f"arXiv {category}"
            topic = "quantitative_finance"
            record_id = build_record_id(name, paper["title"], paper_url)

            # Fingerprint dedup
            title_fp = title_fingerprint(paper["title"])
            content_fp = title_fingerprint(paper["summary"][:5000])

            existing_title = get_fingerprint_record_id("title_fingerprints", title_fp)
            existing_content = get_fingerprint_record_id(
                "content_fingerprints", content_fp
            )

            if existing_title and existing_title != record_id:
                print(f"  Duplicate title fingerprint for {paper_id}, skipping.")
                continue
            if existing_content and existing_content != record_id:
                print(f"  Duplicate content fingerprint for {paper_id}, skipping.")
                continue

            output_path = RAW_DIR / f"{record_id}.txt"
            output_text = build_arxiv_record_text(paper, topic, name)

            try:
                output_path.write_text(output_text, encoding="utf-8")
            except Exception as e:
                print(f"  Failed write for {paper_id}: {e}")
                continue

            digest = content_hash(paper["summary"])
            upsert_seen_url(paper_url, digest)
            set_record_map(paper_url, record_id)
            set_record_rules(record_id, {
                "required_keywords": [],
                "blocked_keywords": [],
                "min_word_count": 50,
                "expected_language": "en",
                "allowed_page_types": ["academic_paper"],
                "target_name": name,
                "topic": topic,
            })
            add_fingerprint("title_fingerprints", title_fp, record_id)
            add_fingerprint("content_fingerprints", content_fp, record_id)

            created.append(record_id)
            print(f"  Saved: {paper_id} → {output_path.name}")

            time.sleep(0.3)  # Polite rate limiting

    return created


if __name__ == "__main__":
    created = main()
    print("\nJSON_OUTPUT_START")
    print(json.dumps(created))
    print("JSON_OUTPUT_END")
