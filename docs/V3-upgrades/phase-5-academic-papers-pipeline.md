# Phase 5 — Academic Papers Pipeline (arXiv + SSRN)

**Depends on:** Phase 1 complete  
**Priority:** Medium-High new source — fills the gap between practitioner news and research output; arXiv q-fin papers often lead market commentary by weeks

---

## What This Phase Does

Adds a pipeline that ingests quantitative finance and economics academic papers from arXiv and SSRN. Both are free, have structured APIs or RSS feeds, and publish continuously. Papers are processed through the standard summarize/verify/route pipeline.

---

## Source Details

### arXiv

- **API:** `http://export.arxiv.org/api/query?search_query=cat:q-fin.*&sortBy=submittedDate&sortOrder=descending&max_results=20`
- **Categories to watch:**
  - `q-fin.RM` — Risk Management
  - `q-fin.PM` — Portfolio Management
  - `q-fin.MF` — Mathematical Finance
  - `q-fin.EC` — Economics
  - `q-fin.TR` — Trading and Market Microstructure
  - `econ.GN` — General Economics
- **Format:** Atom feed with abstract, authors, submission date, paper ID
- **No API key required**
- **Update frequency:** Submissions posted daily (Monday–Friday)

### SSRN

- **RSS:** SSRN provides topic RSS feeds, e.g.:
  - `https://papers.ssrn.com/sol3/Jrnls/rss.cfm?link=mngt` (Management)
  - `https://papers.ssrn.com/sol3/Jrnls/rss.cfm?link=fin` (Finance)
  - `https://papers.ssrn.com/sol3/Jrnls/rss.cfm?link=accg` (Accounting)
- **No API key required**
- **Note:** SSRN RSS gives title, abstract snippet, and link — full paper text requires fetching the abstract page or PDF

---

## Implementation Plan

### Step 1 — Add arXiv and SSRN feeds to `config/rss_feeds.json`

Rather than building a completely new pipeline, arXiv Atom feeds and SSRN RSS are structurally identical to the existing RSS ingest. Add entries to `rss_feeds.json` with:

```json
{
  "name": "arXiv q-fin Risk Management",
  "url": "http://export.arxiv.org/rss/q-fin.RM",
  "source_type": "academic",
  "topic": "quantitative_finance",
  "enabled": true,
  "max_entries": 15
}
```

Add entries for each category listed above plus SSRN finance/macro feeds.

This approach requires **zero new scripts** for basic ingestion — `ingest_rss.py` already handles arbitrary RSS feeds.

### Step 2 — Create `scripts/ingest_arxiv.py` (enhanced fetch)

The basic RSS route gets titles and abstracts but not full paper text. For higher-quality records, create a dedicated arXiv ingest script that:

1. Queries the arXiv REST API (`http://export.arxiv.org/api/query`) with category filters and `submittedDate` range matching `lookback_days`
2. For each new paper (keyed by arXiv paper ID, e.g. `2403.12345`):
  - Extracts abstract, authors, category, submission date
  - Builds a structured raw record: abstract as `content`, arXiv URL as `url`, authors as metadata
  - Does **not** fetch the full PDF (too large; the abstract is sufficient for the summarizer)
3. Skips papers already in the manifest by arXiv ID

### Step 3 — Create `config/academic_sources.json`

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
      { "name": "SSRN Finance", "url": "https://papers.ssrn.com/sol3/Jrnls/rss.cfm?link=fin", "enabled": true },
      { "name": "SSRN Macro",   "url": "https://papers.ssrn.com/sol3/Jrnls/rss.cfm?link=mngt", "enabled": true }
    ],
    "max_entries_per_feed": 10
  }
}
```

### Step 4 — Add to Existing `process-articles` Workflow

Rather than a separate workflow, academic paper ingestion runs as part of the existing `process-articles.yml`. Add a step before the main ingest call:

```yaml
- run: python scripts/ingest_arxiv.py
  env:
    OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
```

This keeps scheduling simple — papers are checked every 2 hours alongside article ingestion.

### Step 5 — Scoring Adjustment

Academic papers should receive a scoring boost for `source_type: "academic"` in `scoring_rules.json`. They tend to be dense and lower in news-timeliness but higher in analytical depth. Consider adding a `+10` modifier for academic sources in the scoring config.

---

## New GitHub Secrets Required

**None.** Both arXiv and SSRN are fully public with no authentication required.

---

## Files Changed in This Phase


| File                                     | Change                                            |
| ---------------------------------------- | ------------------------------------------------- |
| `config/academic_sources.json`           | New — arXiv category list and SSRN feed list      |
| `config/rss_feeds.json`                  | Add arXiv RSS entries as lightweight fallback     |
| `scripts/ingest_arxiv.py`                | New — structured arXiv API fetch                  |
| `scripts/run_ingest_and_process.py`      | Call `ingest_arxiv()` as part of main ingest loop |
| `config/scoring_rules.json`              | Add academic source type boost                    |
| `.github/workflows/process-articles.yml` | No change needed if integrated into main ingest   |


---

## Acceptance Criteria

- At least 5 arXiv papers from the last 5 days appear as raw records after running `ingest_arxiv.py`
- Papers are correctly identified as `source_type: "academic"` in their raw records
- Same arXiv paper ID does not produce a duplicate record on re-run
- At least one academic paper routes to `data/accepted/` per weekly period

