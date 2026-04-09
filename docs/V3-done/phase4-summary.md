# Phase 4 — SEC EDGAR Filings Pipeline: Implementation Summary

**Completed:** 2026-04-09
**Branch:** `hermes-RJ` (based on `dev-Rishi`)
**Spec:** [`docs/V3-upgrades/phase-4-sec-edgar-pipeline.md`](../V3-upgrades/phase-4-sec-edgar-pipeline.md)

---

## What Was Done

Added a new ingestion pipeline that watches SEC EDGAR for fresh 8-K, 10-K, and 10-Q filings from a configurable list of companies. Filings are fetched as structured text, converted to the existing raw record format, and routed through the standard `process_record` pipeline. The pipeline runs on a 6-hour cron schedule and auto-commits results to the repo.

---

## Files Changed

| File | Change |
|---|---|
| `config/edgar_sources.json` | New — company CIK list, filing types, lookback params, user agent |
| `scripts/ingest_edgar.py` | New — EDGAR API fetch, form/date filtering, dedup, raw record creation, HTML text extraction |
| `scripts/run_edgar_pipeline.py` | New — orchestrator: calls `ingest_edgar()` then `process_record()` per new filing |
| `tests/test_edgar_ingest.py` | New — 12 unit tests covering CIK padding, filing parsing, URL construction, HTML extraction, dedup |
| `tests/test_run_edgar_pipeline.py` | New — 8 unit tests for orchestrator logic |
| `.github/workflows/process-edgar.yml` | New — GitHub Actions: cron every 6h + `workflow_dispatch`, auto-commits new records |

---

## Config: `edgar_sources.json`

```json
{
  "user_agent": "FinanceResearchArchive contact@example.com",
  "filing_types": ["8-K", "10-K", "10-Q"],
  "lookback_days": 3,
  "max_filings_per_run": 20,
  "companies": [
    { "name": "JPMorgan Chase",       "cik": "0000019617", "enabled": true },
    { "name": "Goldman Sachs",        "cik": "0000886982", "enabled": true },
    { "name": "Bank of America",     "cik": "0000070858", "enabled": true },
    { "name": "BlackRock",           "cik": "0001364742", "enabled": true },
    { "name": "Berkshire Hathaway",  "cik": "0001067983", "enabled": true }
  ]
}
```

---

## Key Design Decisions

- **Deduplication via accession number** — each EDGAR filing has a unique `accessionNumber`; checked against `seen_urls` before fetching to avoid redundant work
- **User-Agent header** — required by SEC fair-access policy; set from `config.edgar_sources.json` (`user_agent` field must be updated with real contact info before first run)
- **Raw record format** — matches existing pipeline: `title`, `url`, `source: sec_edgar`, `content` headers followed by extracted text
- **Record ID** — accession number with dashes removed (e.g. `0000019617-24-000001` → `000001961724000001`)
- **Rate limiting** — 0.2s sleep between requests per company to avoid triggering SEC abuse detection
- **HTML text extraction** — falls back to regex tag stripping when EDGAR serves HTML; skips content < 100 chars

---

## API Details

| Endpoint | Use |
|---|---|
| `https://data.sec.gov/submissions/CIK{cik_padded}.json` | Fetch company filing list |
| `https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no}/{primary_doc}` | Fetch individual filing document |

---

## Tests

| Suite | Tests | Status |
|---|---|---|
| `test_edgar_ingest.py` | 12 | All passing |
| `test_run_edgar_pipeline.py` | 8 | All passing |

**Total: 20 new tests added.**

---

## Acceptance Criteria — Verified

- `python scripts/run_edgar_pipeline.py` produces raw records in `data/raw/` for companies that filed within the lookback window — confirmed
- Accession numbers used as dedup keys; re-runs do not produce duplicate records — confirmed via `is_url_seen()` check in `run_one_company()`
- EDGAR API requires no authentication; no new GitHub Secrets needed — confirmed
- All 20 tests pass — confirmed
- Workflow file created in `.github/workflows/process-edgar.yml` with correct triggers — confirmed

---

## Notes

- Update `config/edgar_sources.json` `user_agent` field with a real name and email address before first production run (SEC requires this)
- More companies can be added to `config.edgar_sources.json` at any time; CIKs are found via the SEC company search tool
- The pipeline is designed to be additive: filings that pass triage land in `data/accepted/` or `data/review_queue/` just like any other source
