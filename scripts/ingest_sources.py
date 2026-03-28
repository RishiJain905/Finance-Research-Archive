import hashlib
import json
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "ingestion_targets.json"
RAW_DIR = BASE_DIR / "data" / "raw"
MANIFEST_PATH = BASE_DIR / "data" / "ingestion_manifest.json"

DEFAULT_MAX_LINKS_PER_TARGET = 5
MAX_TITLE_SLUG_LENGTH = 60

POSITIVE_LINK_HINTS = [
    "press", "release", "statement", "speech", "testimony", "minutes",
    "decision", "report", "bulletin", "commentary", "economic-letter",
    "staff-report", "staff-reports", "blog", "insight", "analysis",
    "policy", "market-notice", "news", "article", "publication"
]

NEGATIVE_LINK_HINTS = [
    "about", "careers", "events", "experts", "archive", "archives",
    "people", "our-people", "education", "programs", "our-offices",
    "our-vision", "museum", "tag", "category", "topics", "author",
    "contact", "signup", "subscribe", "webinar", "podcast"
]

MAIN_CONTENT_SELECTORS = [
    "article",
    "main",
    "[role='main']",
    ".article",
    ".article-body",
    ".article-content",
    ".story-body",
    ".post-content",
    ".entry-content",
    ".content",
]

CONTAINER_PAGE_TYPES = {"homepage", "navigation_page", "listing_page", "search_page"}


def ensure_manifest_shape(manifest: dict) -> dict:
    if not isinstance(manifest, dict):
        manifest = {}

    manifest.setdefault("seen_urls", {})
    manifest.setdefault("record_map", {})
    manifest.setdefault("record_rules", {})
    manifest.setdefault("title_fingerprints", {})
    manifest.setdefault("content_fingerprints", {})

    return manifest


def normalize_title(title: str) -> str:
    title = title.lower().strip()
    title = re.sub(r"[^a-z0-9\s]", " ", title)
    title = re.sub(r"\s+", " ", title)
    return title.strip()


def title_fingerprint(title: str) -> str:
    normalized = normalize_title(title)
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def content_fingerprint(text: str) -> str:
    normalized = text.lower().strip()
    normalized = re.sub(r"\s+", " ", normalized)
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def shorten_slug(value: str, max_length: int) -> str:
    value = slugify(value)
    return value[:max_length].rstrip("_")


def load_json(path: Path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def get_headers() -> dict:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.google.com/",
    }


def fetch_html(url: str) -> str:
    response = requests.get(url, timeout=30, headers=get_headers())
    response.raise_for_status()
    return response.text


def collapse_text_lines(text: str) -> str:
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines)


def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text(separator="\n")
    return collapse_text_lines(text)


def extract_main_text(html: str) -> tuple[str, list[str]]:
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]):
        tag.decompose()

    warnings = []

    for selector in MAIN_CONTENT_SELECTORS:
        node = soup.select_one(selector)
        if not node:
            continue

        text = collapse_text_lines(node.get_text(separator="\n"))
        if len(text) >= 500:
            return text, warnings

    warnings.append("main_content_fallback")
    return html_to_text(str(soup)), warnings


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def is_allowed_link(url: str, allowed_prefixes: list[str]) -> bool:
    if not allowed_prefixes:
        return True
    return any(url.startswith(prefix) for prefix in allowed_prefixes)


def is_blocked_link(url: str, blocklist_fragments: list[str]) -> bool:
    if not blocklist_fragments:
        return False

    lower_url = url.lower()
    return any(fragment.lower() in lower_url for fragment in blocklist_fragments)


def looks_like_article_link(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()

    bad_suffixes = (
        ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg",
        ".zip", ".xlsx", ".xls", ".csv", ".doc", ".docx"
    )
    if path.endswith(bad_suffixes):
        return False

    if parsed.fragment:
        return False

    return True


def score_candidate_link(url: str, anchor_text: str) -> int:
    score = 0
    url_lower = url.lower()
    anchor_lower = anchor_text.lower().strip()

    for hint in POSITIVE_LINK_HINTS:
        if hint in url_lower:
            score += 3
        if hint in anchor_lower:
            score += 2

    for hint in NEGATIVE_LINK_HINTS:
        if hint in url_lower:
            score -= 4
        if hint in anchor_lower:
            score -= 3

    parsed = urlparse(url)
    path = parsed.path.lower().strip("/")

    path_segments = [segment for segment in path.split("/") if segment]
    if len(path_segments) >= 2:
        score += 1

    if re.search(r"/20\d{2}/", url_lower) or re.search(r"20\d{2}", anchor_lower):
        score += 2

    if url_lower.endswith(".htm") or url_lower.endswith(".html"):
        score += 1

    if path.endswith("/"):
        score -= 1

    if path in {"", "index", "news", "press", "markets", "research", "economy"}:
        score -= 3

    if not path:
        score -= 8

    if path.endswith("/index") or "/index." in path or path.startswith("index."):
        score -= 6

    if anchor_lower in {"home", "about", "contact", "menu"}:
        score -= 6

    return score


def extract_links(
    index_url: str,
    html: str,
    allowed_prefixes: list[str],
    blocklist_fragments: list[str],
    max_links: int
) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    candidates = []
    seen = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        if not href:
            continue

        full_url = urljoin(index_url, href)
        anchor_text = a_tag.get_text(" ", strip=True)

        if full_url in seen:
            continue
        seen.add(full_url)

        if not looks_like_article_link(full_url):
            continue

        if not is_allowed_link(full_url, allowed_prefixes):
            continue

        if is_blocked_link(full_url, blocklist_fragments):
            continue

        score = score_candidate_link(full_url, anchor_text)
        candidates.append((score, full_url, anchor_text))

    candidates.sort(key=lambda item: item[0], reverse=True)

    selected = []
    for score, full_url, anchor_text in candidates:
        if len(selected) >= max_links:
            break

        if score < 0:
            continue

        selected.append(full_url)

    return selected


def extract_title(soup: BeautifulSoup) -> str:
    if soup.title and soup.title.string:
        return soup.title.string.strip()

    h1 = soup.find("h1")
    if h1:
        return h1.get_text(" ", strip=True)

    return "untitled"


def sanitize_title(title: str) -> str:
    title = re.sub(r"\s+", " ", title).strip()
    return title[:120]


def extract_published_at(soup: BeautifulSoup) -> str:
    selectors = [
        ("meta", {"property": "article:published_time"}),
        ("meta", {"name": "pubdate"}),
        ("meta", {"name": "publishdate"}),
        ("meta", {"name": "date"}),
        ("meta", {"itemprop": "datePublished"}),
        ("time", {}),
    ]

    for tag_name, attrs in selectors:
        for tag in soup.find_all(tag_name, attrs=attrs):
            if tag_name == "meta":
                value = tag.get("content", "").strip()
            else:
                value = tag.get("datetime", "").strip() or tag.get_text(" ", strip=True)

            if value:
                return value

    return ""


def extract_canonical_url(soup: BeautifulSoup, fallback_url: str) -> str:
    canonical_tag = soup.find("link", rel=lambda value: value and "canonical" in value)
    if canonical_tag:
        href = canonical_tag.get("href", "").strip()
        if href:
            return href
    return fallback_url


def detect_language(text: str) -> str:
    ascii_chars = sum(1 for char in text if ord(char) < 128)
    total_chars = len(text)
    if total_chars == 0:
        return "unknown"

    ascii_ratio = ascii_chars / total_chars
    return "en" if ascii_ratio >= 0.9 else "non_en"


def classify_page_type(url: str, title: str, text: str, published_at: str) -> str:
    lower_text = text.lower()
    lower_title = title.lower()
    parsed = urlparse(url)
    path = parsed.path.lower()
    clean_path = path.strip("/")

    language_selector_hits = sum(
        1 for token in ["english", "deutsch", "français", "español", "italiano", "bg", "menu"]
        if token in lower_text
    )
    nav_hits = sum(
        1 for token in ["skip to", "navigation", "menu", "frequently asked questions", "contacts"]
        if token in lower_text
    )
    listing_hits = sum(
        1 for token in ["latest", "archive", "archives", "recent", "browse", "all rights reserved"]
        if token in lower_text
    )
    homepage_hits = sum(
        1 for token in ["listings", "trading", "market data", "about", "innovation", "connect"]
        if token in lower_text
    )

    if "search" in path:
        return "search_page"

    if nav_hits >= 2 and language_selector_hits >= 2:
        return "navigation_page"

    if not clean_path:
        return "homepage"

    if clean_path.endswith("index") or "/index." in path or clean_path.startswith("index."):
        if nav_hits >= 1:
            return "navigation_page"
        if not published_at:
            return "listing_page"

    if homepage_hits >= 3 and "|" in lower_title:
        return "homepage"

    if not published_at and listing_hits >= 1:
        return "listing_page"

    return "article"


def build_raw_record_text(metadata: dict[str, str], article_text: str) -> str:
    header_lines = [f"{key}: {value}" for key, value in metadata.items()]
    return "\n".join(header_lines) + f"\n\n{article_text}"


def build_record_id(target_name: str, article_title: str, article_url: str) -> str:
    target_slug = shorten_slug(target_name, 30)
    title_slug = shorten_slug(article_title, MAX_TITLE_SLUG_LENGTH)
    url_hash = hashlib.sha1(article_url.encode("utf-8")).hexdigest()[:8]
    return f"{target_slug}_{title_slug}_{url_hash}"


def main() -> list[str]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    config = load_json(CONFIG_PATH, {"targets": []})
    manifest = load_json(
        MANIFEST_PATH,
        {
            "seen_urls": {},
            "record_map": {},
            "record_rules": {},
            "title_fingerprints": {},
            "content_fingerprints": {}
        }
    )
    manifest = ensure_manifest_shape(manifest)

    default_max_links = config.get("max_links_per_target", DEFAULT_MAX_LINKS_PER_TARGET)
    if not isinstance(default_max_links, int) or default_max_links < 1:
        default_max_links = DEFAULT_MAX_LINKS_PER_TARGET

    created = []

    for target in config.get("targets", []):
        if not target.get("enabled", True):
            print(f"\nTarget: {target.get('name', 'unknown')}")
            print("  Disabled, skipping.")
            continue

        name = target["name"]
        topic = target["topic"]
        url = target["url"]
        allowed_prefixes = target.get("allowed_prefixes", [])
        blocklist_fragments = target.get("url_blocklist_fragments", [])
        max_links = target.get("max_links", default_max_links)
        required_keywords = target.get("required_keywords", [])
        blocked_keywords = target.get("blocked_keywords", [])
        min_word_count = target.get("min_word_count", 120)
        expected_language = target.get("expected_language", "en")
        allowed_page_types = target.get(
            "allowed_page_types",
            ["article", "press_release", "speech", "data_release", "market_notice"]
        )

        if not isinstance(max_links, int) or max_links < 1:
            max_links = default_max_links

        print(f"\nTarget: {name}")
        print(f"  Max links: {max_links}")

        try:
            index_html = fetch_html(url)
        except Exception as e:
            print(f"  Failed to fetch target page: {e}")
            continue

        article_links = extract_links(
            url,
            index_html,
            allowed_prefixes,
            blocklist_fragments,
            max_links
        )

        if not article_links:
            print("  No candidate links found.")
            continue

        print(f"  Found {len(article_links)} candidate links")

        for article_url in article_links:
            print(f"  Fetching article: {article_url}")

            try:
                article_html = fetch_html(article_url)
            except Exception as e:
                print(f"    Failed fetch: {e}")
                continue

            article_text, extraction_warnings = extract_main_text(article_html)
            digest = content_hash(article_text)

            previous_hash = manifest["seen_urls"].get(article_url)
            if previous_hash == digest:
                print("    No change, skipping.")
                continue

            soup = BeautifulSoup(article_html, "html.parser")
            article_title = sanitize_title(extract_title(soup))
            h1 = soup.find("h1")
            h1_text = h1.get_text(" ", strip=True) if h1 else ""
            published_at = extract_published_at(soup)
            canonical_url = extract_canonical_url(soup, article_url)
            detected_language = detect_language(article_text)
            page_type = classify_page_type(article_url, article_title, article_text, published_at)
            if page_type in CONTAINER_PAGE_TYPES:
                extraction_warnings.append("container_page")
            record_id = build_record_id(name, article_title, article_url)

            title_fp = title_fingerprint(article_title)
            content_fp = content_fingerprint(article_text[:5000])

            existing_title_record = manifest["title_fingerprints"].get(title_fp)
            existing_content_record = manifest["content_fingerprints"].get(content_fp)

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
                    "INDEX_URL": url,
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
                },
                article_text,
            )

            try:
                output_path.write_text(output_text, encoding="utf-8")
            except Exception as e:
                print(f"    Failed write: {e}")
                continue

            manifest["seen_urls"][article_url] = digest
            manifest["record_map"][article_url] = record_id
            manifest["record_rules"][record_id] = {
                "required_keywords": required_keywords,
                "blocked_keywords": blocked_keywords,
                "min_word_count": min_word_count,
                "expected_language": expected_language,
                "allowed_page_types": allowed_page_types,
                "target_name": name,
                "topic": topic
            }
            manifest["title_fingerprints"][title_fp] = record_id
            manifest["content_fingerprints"][content_fp] = record_id

            created.append(record_id)
            print(f"    Saved: {output_path.relative_to(BASE_DIR)}")

    save_json(MANIFEST_PATH, manifest)

    print("\nCreated/updated record ids:")
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
