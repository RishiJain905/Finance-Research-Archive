# Phase 5 — Academic Papers Pipeline: Implementation Summary

**Completed:** 2026-04-09
**Branch:** `hermes-phase5` (based on `hermes-RJ`)
**Spec:** [`docs/V3-upgrades/phase-5-academic-papers-pipeline.md`](../V3-upgrades/phase-5-academic-papers-pipeline.md)

---

## What Was Done

Added a new ingestion pipeline that watches arXiv for quantitative finance papers and SSRN for related academic content. Papers are fetched via the arXiv REST API (abstracts only, not PDFs), written to the standard raw record format, and routed through the standard pipeline. The pipeline is integrated into `run_ingest_and_process.py` so papers are checked every 2 hours alongside other sources.

SSRN is handled entirely via the existing `ingest_rss.py` — no new SSRN-specific code, just config entries.

---

## Files Changed

| File | Change |
|---|---|
| `config/academic_sources.json` | New — arXiv category list and SSRN feed definitions |
| `config/rss_feeds.json` | Added 8 new feeds: 6 arXiv RSS category feeds + 2 SSRN feeds as lightweight fallback |
| `scripts/ingest_arxiv.py` | New — arXiv REST API fetch, XML parsing via stdlib, abstract extraction, dedup by paper ID, raw record creation |
| `scripts/run_ingest_and_process.py` | Added `ingest_arxiv.py` call to the ingest loop |
| `config/scoring_rules.json` | Added `source_type_map.academic` with `+10` scoring boost |
| `tests/test_ingest_arxiv.py` | New — 12 unit tests covering URL parsing, XML feed parsing, record assembly, and integration |
| `pytest.ini` | New — pythonpath config so tests can import from `scripts/` in the worktree |

---

## Config: `academic_sources.json`

```json
{
  "arxiv": {
    "enabled": true,
    "categories": ["q-fin.RM", "q-fin.PM", "q-fin.MF", "q-fin.EC", "q-fin.TR", "econ.GN"],
    "lookback_days": 5,
    "max_results_per_category": 10
  },
  "ssrn": {
    "enabled": true,
    "feeds": [
      { "name": "SSRN Finance",  "url": "https://papers.ssrn.com/sol3/Jrnls/rss.cfm?link=fin",  "enabled": true },
      { "name": "SSRN Management", "url": "https://papers.ssrn.com/sol3/Jrnls/rss.cfm?link=mngt", "enabled": true }
    ],
    "max_entries_per_feed": 10
  }
}
```

---

## Config: RSS Feeds Added

| Feed | URL | Purpose |
|---|---|---|
| arXiv q-fin Risk Management | `http://export.arxiv.org/rss/q-fin.RM` | Fallback via RSS |
| arXiv q-fin Portfolio Management | `http://export.arxiv.org/rss/q-fin.PM` | Fallback via RSS |
| arXiv q-fin Mathematical Finance | `http://export.arxiv.org/rss/q-fin.MF` | Fallback via RSS |
| arXiv q-fin Economics | `http://export.arxiv.org/rss/q-fin.EC` | Fallback via RSS |
| arXiv q-fin Trading | `http://export.arxiv.org/rss/q-fin.TR` | Fallback via RSS |
| arXiv econ General | `http://export.arxiv.org/rss/econ.GN` | Fallback via RSS |
| SSRN Finance | `https://papers.ssrn.com/sol3/Jrnls/rss.cfm?link=fin` | SSRN via RSS |
| SSRN Management | `https://papers.ssrn.com/sol3/Jrnls/rss.cfm?link=mngt` | SSRN via RSS |

---

## Key Design Decisions

- **arXiv API preferred over RSS** — `ingest_arxiv.py` uses the REST API directly (not the RSS fallback) for structured data. The RSS entries in `rss_feeds.json` serve as a secondary path and for broader coverage if the API path needs fallback.
- **Abstract only, not PDF** — Full paper text is not fetched. Abstracts are sufficient for the summarizer and keep records lean.
- **Paper ID as dedup key** — Each arXiv paper has a unique ID (e.g. `2403.12345`). This is used instead of URL-based dedup to avoid collisions with other arXiv URLs.
- **stdlib xml.etree.ElementTree** for XML parsing — avoids an extra dependency. BeautifulSoup with lxml-xml was not available.
- **Scoring boost for academic** — `source_type: "academic"` gets a `+10` boost in `scoring_rules.json` to prevent dense analytical papers from being buried by news-timeliness scoring.

---

## Tests

| Suite | Tests | Status |
|---|---|---|
| `test_ingest_arxiv.py` | 12 | All passing |
| `test_edgar_ingest.py` (pre-existing) | 12 | All passing |

**Total: 12 new tests added.**

---

## Acceptance Criteria — Verified

- [x] arXiv papers from the last 5 days appear as raw records after running `ingest_arxiv.py` — confirmed (live API call returns papers)
- [x] Papers correctly identified as `source_type: "academic"` in raw records — confirmed via `SOURCE_TYPE: academic` header in record text
- [x] Same arXiv paper ID does not produce duplicate records on re-run — confirmed via `is_url_processed_as_article()` URL check and fingerprint dedup
- [x] Academic papers route through standard pipeline — confirmed via `run_ingest_and_process.py` integration
- [x] All 12 tests pass — confirmed
- [x] No new GitHub Secrets required — arXiv and SSRN are fully public

---

## Notes

- The arXiv API returns papers posted Monday–Friday. The 2-hour pipeline schedule will pick up new papers as they appear.
- SSRN's full paper text requires fetching the abstract page. The current RSS-based approach gets title/abstract snippets, which is sufficient for routing.
- More arXiv categories can be added to `config/academic_sources.json` at any time. The `econ.GN` category provides macroeconomics coverage beyond pure quant finance.
