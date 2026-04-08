# Phase 4 — SEC EDGAR Filings Pipeline

**Depends on:** Phase 1 complete, Phase 3 recommended (manifest stability helps with a new high-volume source)  
**Priority:** High new source — EDGAR is free, structured, time-stamped, and one of the highest-signal financial data feeds available

---

## What This Phase Does

Adds a new ingestion pipeline that watches SEC EDGAR for fresh filings from a configurable list of companies and filing types. Filings are fetched as structured text, converted to the existing raw record format, and routed through the standard `process_record` pipeline.

---

## Why EDGAR

- Completely free REST API — no key required
- Filing timestamps are authoritative (SEC-published)
- 8-K filings (material events) arrive within hours of major corporate announcements
- 10-K and 10-Q filings contain forward guidance, risk factors, and management commentary not captured by news articles
- Deduplication is trivial: each filing has a unique accession number

---

## EDGAR API Overview

Base URL: `https://data.sec.gov/submissions/CIK{cik_padded}.json`

Returns all recent filings for a company. Fields used:

- `filings.recent.accessionNumber` — unique ID per filing
- `filings.recent.form` — filing type (8-K, 10-K, 10-Q, etc.)
- `filings.recent.filingDate` — publication date
- `filings.recent.primaryDocument` — filename of the primary document

Full document URL: `https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_dashes}/{primary_document}`

EDGAR also provides a company search API: `https://efts.sec.gov/LATEST/search-index?q=%22{company_name}%22&dateRange=custom&startdt={date}&enddt={date}&forms=8-K`

No authentication required. User-Agent header required by SEC policy: `User-Agent: YourName YourEmail@example.com`

---

## Implementation Plan

### Step 1 — Create `config/edgar_sources.json`

```json
{
  "user_agent": "FinanceResearchArchive contact@example.com",
  "filing_types": ["8-K", "10-K", "10-Q"],
  "lookback_days": 3,
  "max_filings_per_run": 20,
  "companies": [
    { "name": "JPMorgan Chase", "cik": "0000019617", "enabled": true },
    { "name": "Goldman Sachs",  "cik": "0000886982", "enabled": true },
    { "name": "Bank of America","cik": "0000070858", "enabled": true },
    { "name": "BlackRock",      "cik": "0001364742", "enabled": true },
    { "name": "Berkshire Hathaway", "cik": "0001067983", "enabled": true }
  ]
}
```

Add more companies as needed. CIK numbers are found via `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={name}&type=&dateb=&owner=include&count=10`.

### Step 2 — Create `scripts/ingest_edgar.py`

Main logic:

1. Load `config/edgar_sources.json`
2. For each enabled company, fetch `submissions/{CIK}.json` from EDGAR API
3. Filter filings to configured `filing_types` and `lookback_days`
4. Skip any accession numbers already in the manifest (SQLite `seen_urls` keyed by accession number)
5. Fetch the primary document text for new filings
6. Write to `data/raw/{accession_number}.txt` in the standard raw record format:
  - `title`: `"{company} {form_type} Filing — {date}"`
  - `url`: full EDGAR document URL
  - `source`: `"sec_edgar"`
  - `content`: extracted text from the filing document
7. Update manifest with accession number as the URL key

### Step 3 — Create `scripts/run_edgar_pipeline.py`

Orchestrator script following the same pattern as `run_ingest_and_process.py`:

1. Call `ingest_edgar()`
2. For each new raw record ID, call `process_record()`
3. Commit results

### Step 4 — Create `.github/workflows/process-edgar.yml`

```yaml
name: Process EDGAR Filings

on:
  workflow_dispatch:
  schedule:
    - cron: '30 */6 * * *'   # Every 6 hours

jobs:
  process:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: python scripts/run_edgar_pipeline.py
        env:
          OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
          OPENAI_BASE_URL: ${{ secrets.OPENAI_BASE_URL }}
      - name: Commit results
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/
          git diff --staged --quiet || git commit -m "chore: edgar pipeline run"
          git push
```

---

## New GitHub Secrets Required

**None.** The SEC EDGAR API is completely free and requires no authentication. The `User-Agent` header value (name + email) should be set in `config/edgar_sources.json` — update it with your actual contact details before first run as required by SEC fair-access policy.

---

## Files Changed in This Phase


| File                                  | Change                                                |
| ------------------------------------- | ----------------------------------------------------- |
| `config/edgar_sources.json`           | New — company CIK list and filing type config         |
| `scripts/ingest_edgar.py`             | New — EDGAR API fetch and raw record creation         |
| `scripts/run_edgar_pipeline.py`       | New — orchestrator                                    |
| `.github/workflows/process-edgar.yml` | New — GitHub Actions workflow                         |
| `requirements.txt`                    | No new dependencies (uses `requests` already present) |


---

## Acceptance Criteria

- Running `python scripts/run_edgar_pipeline.py` locally produces at least one raw record in `data/raw/` for a company that filed within the last 3 days
- Accession numbers are correctly used as dedup keys (same filing does not produce a second record on re-run)
- 8-K filings appear in `data/accepted/` or `data/review_queue/` after the full pipeline runs
- The workflow runs on schedule without errors

