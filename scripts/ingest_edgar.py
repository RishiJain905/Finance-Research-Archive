"""SEC EDGAR ingestor for Phase 4 of the Finance Research Archive pipeline.

Fetches recent 8-K, 10-K, and 10-Q filings for configured companies,
writes raw records to data/raw/<accession_number>.txt, and tracks URLs
in the manifest database to avoid duplicates.
"""

import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urljoin

import requests

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.manifest_db import (
    ensure_schema,
    get_url_content_hash,
    is_url_seen,
    upsert_seen_url,
    set_record_map,
)

CONFIG_PATH = BASE_DIR / "config" / "edgar_sources.json"
RAW_DIR = BASE_DIR / "data" / "raw"

EDGAR_API_BASE = "https://data.sec.gov/submissions"
EDGAR_DOC_BASE = "https://www.sec.gov/Archives/edgar/data"

# Seconds to wait between requests to avoid rate limiting
REQUEST_DELAY = 0.2


def load_config() -> dict:
    """Load and validate the edgar_sources.json config."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found: {CONFIG_PATH}")
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def pad_cik(cik: str) -> str:
    """Pad CIK to 10 digits for EDGAR API URL construction."""
    cik_str = str(cik).lstrip("0")
    return cik_str.zfill(10)


def make_session(user_agent: str) -> requests.Session:
    """Build a requests session with the required User-Agent header."""
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    return session


def fetch_submissions(session: requests.Session, cik_padded: str) -> dict:
    """Fetch the company submissions JSON from EDGAR."""
    url = f"{EDGAR_API_BASE}/CIK{cik_padded}.json"
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def parse_recent_filings(
    submissions: dict,
    filing_types: list[str],
    lookback_days: int,
) -> list[dict]:
    """Extract filings matching the desired types within the lookback window.

    Returns a list of dicts with keys: accession_number, form_type, filing_date,
    primary_document, accession_number_hyphenated.
    """
    recent = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    results = []

    filings = submissions.get("filings", {}).get("recent", {})
    accession_numbers = filings.get("accessionNumber", [])
    form_types = filings.get("form", [])
    filing_dates = filings.get("filingDate", [])
    primary_documents = filings.get("primaryDocument", [])

    for i in range(len(accession_numbers)):
        form_type = form_types[i] if i < len(form_types) else None
        if form_type not in filing_types:
            continue

        filing_date_str = filing_dates[i] if i < len(filing_dates) else None
        if not filing_date_str:
            continue

        try:
            filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            continue

        if filing_date < recent:
            continue

        accession_no = accession_numbers[i]
        primary_doc = primary_documents[i] if i < len(primary_documents) else ""

        results.append({
            "accession_number": accession_no,
            "accession_number_hyphenated": accession_no,
            "form_type": form_type,
            "filing_date": filing_date_str,
            "primary_document": primary_doc,
        })

    return results


def build_document_url(cik: str, accession_no: str, primary_doc: str) -> str:
    """Construct the full EDGAR document URL.

    EDGAR directories use CIK without leading zeros and accession without dashes.
    accession_no from submissions has dashes, e.g. "0000019617-24-000001".
    """
    cik_normalized = str(cik).lstrip("0") or "0"
    accession_normalized = accession_no.replace("-", "")
    return f"{EDGAR_DOC_BASE}/{cik_normalized}/{accession_normalized}/{primary_doc}"


def fetch_document_text(session: requests.Session, doc_url: str) -> str:
    """Fetch and extract readable text from a filing document URL.

    Tries .txt documents first, then falls back to HTML parsing.
    """
    response = session.get(doc_url, timeout=30)
    response.raise_for_status()
    content_type = response.headers.get("Content-Type", "")

    text = response.text

    # If it's a .txt file (content-type text/plain), return as-is
    if "text/plain" in content_type or doc_url.endswith(".txt"):
        return text

    # Otherwise try to extract from HTML
    return extract_text_from_html(text)


def extract_text_from_html(html: str) -> str:
    """Strip HTML tags and scripts to recover readable text."""
    # Remove script and style blocks
    html = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL | re.IGNORECASE)

    # Replace block elements with newlines
    html = re.sub(r"<(br|p|h[1-6])[^>]*>", "\n", html, flags=re.IGNORECASE)

    # Remove all remaining tags
    text = re.sub(r"<[^>]+>", "", html)

    # Collapse whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n", "\n\n", text)

    return text.strip()


def build_raw_record_text(
    company_name: str,
    form_type: str,
    filing_date: str,
    doc_url: str,
    content: str,
) -> str:
    """Build the raw record text file content.

    Format (matching existing pipeline):
        title: {company} {form_type} Filing — {date}
        url: full EDGAR document URL
        source: sec_edgar
        content: extracted text
    """
    title = f"{company_name} {form_type} Filing — {filing_date}"
    header = (
        f"title: {title}\n"
        f"url: {doc_url}\n"
        f"source: sec_edgar\n\n"
    )
    return header + content


def run_one_company(
    session: requests.Session,
    company: dict,
    filing_types: list[str],
    lookback_days: int,
    max_filings: int,
) -> list[str]:
    """Ingest filings for one company. Returns list of created record IDs."""
    cik = company["cik"]
    name = company["name"]
    cik_padded = pad_cik(cik)

    print(f"\n=== {name} (CIK {cik}) ===")

    submissions = fetch_submissions(session, cik_padded)
    recent_filings = parse_recent_filings(submissions, filing_types, lookback_days)

    if not recent_filings:
        print(f"  No recent filings found.")
        return []

    print(f"  Found {len(recent_filings)} recent filing(s)")

    created = []
    for filing in recent_filings[:max_filings]:
        accession_no = filing["accession_number"]
        doc_url = build_document_url(cik, filing["accession_number_hyphenated"], filing["primary_document"])

        if is_url_seen(doc_url):
            print(f"  Already seen, skipping: {accession_no}")
            continue

        try:
            content = fetch_document_text(session, doc_url)
        except Exception as e:
            print(f"  Failed to fetch document {doc_url}: {e}")
            continue

        if not content or len(content) < 100:
            print(f"  Skipping empty/too-short content for {accession_no}")
            continue

        record_id = accession_no.replace("-", "")  # Use accession without dashes as record ID
        output_path = RAW_DIR / f"{record_id}.txt"

        try:
            output_text = build_raw_record_text(
                name,
                filing["form_type"],
                filing["filing_date"],
                doc_url,
                content,
            )
            output_path.write_text(output_text, encoding="utf-8")
        except Exception as e:
            print(f"  Failed to write raw file for {record_id}: {e}")
            continue

        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        upsert_seen_url(doc_url, content_hash)
        set_record_map(doc_url, record_id)

        created.append(record_id)
        print(f"  Saved: {output_path.name} — {filing['form_type']} {filing['filing_date']}")

        time.sleep(REQUEST_DELAY)

    return created


def main() -> None:
    """Load config and ingest filings for all enabled companies."""
    ensure_schema()

    config = load_config()
    user_agent = config.get("user_agent", "FinanceResearchArchive contact@example.com")
    filing_types = config.get("filing_types", ["8-K", "10-K", "10-Q"])
    lookback_days = config.get("lookback_days", 3)
    max_filings = config.get("max_filings_per_run", 20)
    companies = [c for c in config.get("companies", []) if c.get("enabled", True)]

    if not companies:
        print("No enabled companies found in config.")
        return

    session = make_session(user_agent)

    print(f"EDGAR Ingest — {len(companies)} company(ies), looking back {lookback_days} day(s)")
    print(f"Filing types: {', '.join(filing_types)}")

    total_created = 0
    for company in companies:
        created = run_one_company(
            session,
            company,
            filing_types,
            lookback_days,
            max_filings,
        )
        total_created += len(created)

    print(f"\n=== Done. Created {total_created} new record(s) ===")


if __name__ == "__main__":
    main()