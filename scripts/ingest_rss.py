"""RSS feed ingestion for the Finance Research Archive.

Reads feeds defined in config/rss_feeds.json, fetches each entry's full
article text, and writes raw records in the same format as ingest_sources.py
so the rest of the pipeline (filter → summarise → verify → route) is unchanged.
"""

import json
import sys
import time
from datetime import date
from pathlib import Path

import feedparser
import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from ingest_sources import (
    RAW_DIR,
    build_raw_record_text,
    build_record_id,
    classify_page_type,
    collapse_text_lines,
    content_fingerprint,
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

RSS_CONFIG_PATH = BASE_DIR / "config" / "rss_feeds.json"

DEFAULT_MAX_ENTRIES = 5
MAX_TITLE_SLUG_LENGTH = 60


def fetch_feed(url: str) -> feedparser.FeedParserDict:
    """Download and parse an RSS/Atom feed."""
    # feedparser handles its own HTTP, but we supply our User-Agent header.
    headers = get_headers()
    response = requests.get(url, timeout=30, headers=headers)
    response.raise_for_status()
    return feedparser.parse(response.text)


def entry_url(entry: feedparser.FeedParserDict) -> str:
    return getattr(entry, "link", "") or ""


def entry_published(entry: feedparser.FeedParserDict) -> str:
    return (
        getattr(entry, "published", "")
        or getattr(entry, "updated", "")
        or ""
    )


def is_url_allowed(url: str, allowed_prefixes: list[str]) -> bool:
    if not allowed_prefixes:
        return True
    return any(url.startswith(prefix) for prefix in allowed_prefixes)


def extract_main_text_from_html(html: str) -> tuple[str, list[str]]:
    """Extract readable text from article HTML, preferring main content."""
    from ingest_sources import MAIN_CONTENT_SELECTORS

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]):
        tag.decompose()

    warnings: list[str] = []
    for selector in MAIN_CONTENT_SELECTORS:
        node = soup.select_one(selector)
        if not node:
            continue
        text = collapse_text_lines(node.get_text(separator="\n"))
        if len(text) >= 500:
            return text, warnings

    warnings.append("main_content_fallback")
    return collapse_text_lines(soup.get_text(separator="\n")), warnings


def main() -> list[str]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    ensure_schema()

    config = load_json(RSS_CONFIG_PATH, {"feeds": []})

    default_max_entries = config.get("max_entries_per_feed", DEFAULT_MAX_ENTRIES)
    current_year = date.today().year
    created: list[str] = []

    for feed_config in config.get("feeds", []):
        if not feed_config.get("enabled", True):
            print(f"\nFeed: {feed_config.get('name', 'unknown')}")
            print("  Disabled, skipping.")
            continue

        name = feed_config["name"]
        topic = feed_config["topic"]
        feed_url = feed_config["url"]
        allowed_prefixes = feed_config.get("allowed_url_prefixes", [])
        required_keywords = feed_config.get("required_keywords", [])
        blocked_keywords = feed_config.get("blocked_keywords", [])
        min_word_count = feed_config.get("min_word_count", 120)
        max_entries = feed_config.get("max_entries", default_max_entries)
        expected_language = feed_config.get("expected_language", "en")
        allowed_page_types = feed_config.get(
            "allowed_page_types",
            ["article", "press_release", "speech", "data_release", "market_notice"],
        )

        print(f"\nFeed: {name}")

        try:
            feed = fetch_feed(feed_url)
        except Exception as e:
            print(f"  Failed to fetch feed: {e}")
            continue

        entries = feed.entries or []
        if not entries:
            print("  No entries found in feed.")
            continue

        # Prefer entries with a year signal matching current or previous year,
        # then take up to max_entries from the front of the feed (feeds are
        # typically newest-first).
        def entry_recency_score(entry: feedparser.FeedParserDict) -> int:
            url = entry_url(entry).lower()
            pub = entry_published(entry)
            if str(current_year) in url or str(current_year) in pub:
                return 2
            if str(current_year - 1) in url or str(current_year - 1) in pub:
                return 1
            return 0

        entries = sorted(entries, key=entry_recency_score, reverse=True)
        entries = entries[:max_entries]

        print(f"  Processing up to {max_entries} entries")

        for entry in entries:
            article_url = entry_url(entry)
            if not article_url:
                continue

            if not is_url_allowed(article_url, allowed_prefixes):
                continue

            print(f"  Fetching entry: {article_url}")

            if is_url_processed_as_article(article_url):
                print("    Already processed, skipping.")
                continue

            try:
                article_html = fetch_html(article_url)
            except Exception as e:
                print(f"    Failed fetch: {e}")
                continue

            article_text, extraction_warnings = extract_main_text_from_html(article_html)
            digest = content_hash(article_text)

            previous_hash = get_url_content_hash(article_url)
            if previous_hash == digest:
                print("    No change, skipping.")
                continue

            soup = BeautifulSoup(article_html, "html.parser")
            article_title = sanitize_title(extract_title(soup))
            h1 = soup.find("h1")
            h1_text = h1.get_text(" ", strip=True) if h1 else ""
            published_at = extract_published_at(soup) or entry_published(entry)
            canonical_url = extract_canonical_url(soup, article_url)
            detected_language = detect_language(article_text)
            page_type = classify_page_type(
                article_url, article_title, article_text, published_at
            )

            record_id = build_record_id(name, article_title, article_url)

            title_fp = title_fingerprint(article_title)
            content_fp = content_fingerprint(article_text[:5000])

            existing_title_record = get_fingerprint_record_id("title_fingerprints", title_fp)
            existing_content_record = get_fingerprint_record_id("content_fingerprints", content_fp)

            if existing_title_record and existing_title_record != record_id:
                print(f"    Duplicate by title fingerprint, skipping. Existing: {existing_title_record}")
                continue

            if existing_content_record and existing_content_record != record_id:
                print(f"    Duplicate by content fingerprint, skipping. Existing: {existing_content_record}")
                continue

            output_path = RAW_DIR / f"{record_id}.txt"
            output_text = build_raw_record_text(
                {
                    "TARGET": name,
                    "TOPIC": topic,
                    "TITLE": article_title,
                    "URL": article_url,
                    "INDEX_URL": feed_url,
                    "ARTICLE_URL": article_url,
                    "CANONICAL_URL": canonical_url,
                    "PAGE_TITLE": article_title,
                    "H1": h1_text,
                    "PUBLISHED_AT": published_at,
                    "PAGE_TYPE": page_type,
                    "EXPECTED_LANGUAGE": expected_language,
                    "DETECTED_LANGUAGE": detected_language,
                    "CONTENT_WORD_COUNT": str(len(article_text.split())),
                    "EXTRACTION_WARNINGS": ",".join(sorted(set(extraction_warnings))),
                    "INGEST_SOURCE": "rss",
                },
                article_text,
            )

            try:
                output_path.write_text(output_text, encoding="utf-8")
            except Exception as e:
                print(f"    Failed write: {e}")
                continue

            upsert_seen_url(article_url, digest)
            set_record_map(article_url, record_id)
            set_record_rules(record_id, {
                "required_keywords": required_keywords,
                "blocked_keywords": blocked_keywords,
                "min_word_count": min_word_count,
                "expected_language": expected_language,
                "allowed_page_types": allowed_page_types,
                "target_name": name,
                "topic": topic,
            })
            add_fingerprint("title_fingerprints", title_fp, record_id)
            add_fingerprint("content_fingerprints", content_fp, record_id)

            created.append(record_id)
            print(f"    Saved: {output_path.relative_to(BASE_DIR)}")

            # Small pause between article fetches to be polite.
            time.sleep(0.5)

    print("\nRSS ingest complete. Created/updated record ids:")
    if created:
        for record_id in created:
            print(f"- {record_id}")
    else:
        print("- none")

    return created


if __name__ == "__main__":
    created = main()
    print("\nJSON_OUTPUT_START")
    print(json.dumps(created))
    print("JSON_OUTPUT_END")
