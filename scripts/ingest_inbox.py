"""Local file drop ingestion for the Finance Research Archive.

Drop PDF, HTML, TXT, or MD files into data/inbox/ and run this script to
convert them to raw records and route them through the standard pipeline.
"""

import hashlib
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None

from scripts.manifest_db import (
    add_fingerprint,
    ensure_schema,
    set_record_map,
    set_record_rules,
    upsert_seen_url,
)

INBOX_DIR = BASE_DIR / "data" / "inbox"
PROCESSED_DIR = INBOX_DIR / "processed"
RAW_DIR = BASE_DIR / "data" / "raw"

SUPPORTED_EXTENSIONS = {".pdf", ".txt", ".html", ".md"}


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


def fetch_html(url: str, max_retries: int = 3) -> str:
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=30, headers=get_headers())
            response.raise_for_status()
            return response.text
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and 400 <= exc.response.status_code < 500:
                raise
            last_exc = exc
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as exc:
            last_exc = exc

        if attempt < max_retries:
            wait = 2**attempt
            time.sleep(wait)

    raise requests.exceptions.RequestException(
        f"Failed after {max_retries} attempts: {last_exc}"
    )


def collapse_text_lines(text: str) -> str:
    lines = [line.strip() for line in text.splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines)


def extract_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(
        ["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]
    ):
        tag.decompose()
    return collapse_text_lines(soup.get_text(separator="\n"))


def extract_text_from_pdf(pdf_path: str) -> str:
    if PdfReader is None:
        raise ImportError("pypdf is required to extract text from PDFs")
    reader = PdfReader(pdf_path)
    texts = []
    for page in reader.pages:
        page_text = page.extract_text()
        if page_text:
            texts.append(page_text)
    return collapse_text_lines("\n".join(texts))


def extract_text(file_path: str) -> str:
    path = Path(file_path)
    ext = path.suffix.lower()

    if ext == ".pdf":
        return extract_text_from_pdf(file_path)
    elif ext == ".html":
        html = Path(file_path).read_text(encoding="utf-8", errors="replace")
        return extract_text_from_html(html)
    elif ext in {".txt", ".md"}:
        return Path(file_path).read_text(encoding="utf-8", errors="replace")
    else:
        raise ValueError(f"Unsupported file type: {ext}")


def build_raw_record_text(record: dict) -> str:
    metadata = {
        "TARGET": record.get("source", "inbox"),
        "TOPIC": "manual_upload",
        "TITLE": record.get("title", ""),
        "URL": record.get("url", ""),
        "SOURCE_TYPE": "document",
        "INGEST_SOURCE": "inbox",
        "PAGE_TYPE": "document",
        "DETECTED_LANGUAGE": "en",
        "INGESTED_AT": record.get("ingested_at", ""),
    }
    header_lines = [f"{key}: {value}" for key, value in metadata.items()]
    return "\n".join(header_lines) + f"\n\n{record.get('content', '')}"


def build_record_id(filename: str) -> str:
    name = Path(filename).stem.lower()
    name = name.replace(" ", "_").replace("-", "_")
    name = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
    name = name[:50]
    hash_suffix = hashlib.sha1(filename.encode("utf-8")).hexdigest()[:8]
    return f"inbox_{name}_{hash_suffix}"


def scan_inbox() -> list[str]:
    if not INBOX_DIR.exists():
        return []

    processed_files = set()
    if PROCESSED_DIR.exists():
        for f in PROCESSED_DIR.iterdir():
            processed_files.add(f.name)

    files = []
    for f in INBOX_DIR.iterdir():
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS:
            if f.name not in processed_files:
                files.append(str(f))
    return files


def move_to_processed(file_path: str) -> None:
    path = Path(file_path)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    dest = PROCESSED_DIR / path.name
    path.rename(dest)


def process_file(file_path: str) -> dict | None:
    path = Path(file_path)
    if not path.exists():
        return None

    filename = path.name
    ext = path.suffix.lower()

    try:
        content = extract_text(file_path)
    except Exception as e:
        print(f"  Failed to extract text from {filename}: {e}")
        return None

    record_id = build_record_id(filename)
    url = f"file://inbox/{quote(filename)}"
    ingested_at = datetime.now(timezone.utc).isoformat()

    record = {
        "title": path.stem,
        "source": "inbox",
        "url": url,
        "content": content,
        "ingested_at": ingested_at,
    }

    output_path = RAW_DIR / f"{record_id}.txt"
    output_text = build_raw_record_text(record)
    try:
        output_path.write_text(output_text, encoding="utf-8")
    except Exception as e:
        print(f"  Failed to write raw record for {filename}: {e}")
        return None

    ensure_schema()
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    upsert_seen_url(url, digest)
    set_record_map(url, record_id)
    set_record_rules(
        record_id,
        {
            "required_keywords": [],
            "blocked_keywords": [],
            "min_word_count": 50,
            "expected_language": "en",
            "allowed_page_types": ["document"],
            "target_name": "inbox",
            "topic": "manual_upload",
        },
    )
    add_fingerprint("content_fingerprints", digest, record_id)

    move_to_processed(file_path)

    print(f"  Processed: {filename} -> {record_id}.txt")
    return record


def main() -> list[str]:
    INBOX_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    files = scan_inbox()
    if not files:
        print("No files in inbox to process.")
        return []

    print(f"Found {len(files)} file(s) in inbox to process.")
    created = []

    for file_path in files:
        filename = Path(file_path).name
        print(f"Processing: {filename}")
        result = process_file(file_path)
        if result:
            record_id = build_record_id(filename)
            created.append(record_id)

    print(f"\nInbox ingest complete. Created {len(created)} record(s).")
    if created:
        print("Record IDs:")
        for record_id in created:
            print(f"  - {record_id}")
    else:
        print("  - none")

    return created


if __name__ == "__main__":
    created = main()
    print("\nJSON_OUTPUT_START")
    print(json.dumps(created))
    print("JSON_OUTPUT_END")
