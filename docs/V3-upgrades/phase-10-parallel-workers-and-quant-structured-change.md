# Phase 10 — Parallel Fetch Workers and Quant Structured Change Detection

**Depends on:** All previous phases complete (or at least Phase 1 and Phase 3)  
**Priority:** Low-Medium — performance and data quality polish; not a blocker but makes the system faster and the quant pipeline genuinely useful for non-FRED data

---

## What This Phase Does

Two focused improvements:

1. **Parallel fetch workers** — moves HTTP fetching in the ingest scripts to a thread pool, reducing wall-clock time for large crawl runs
2. **Quant structured change detection** — replaces the placeholder `build_dataset_snapshot` functions in `ingest_quant_data.py` with real numeric comparison, so quant records are only generated when data actually changes

---

## Part A — Parallel Fetch Workers

### The Current Problem

`ingest_sources.py` and `ingest_rss.py` fetch article URLs sequentially. With `max_links_per_target: 15` (raised in Phase 1) across many targets, a single ingest run makes hundreds of sequential HTTP requests. On GitHub Actions' shared runners, this can push the workflow past its useful time budget.

The `--process-workers` flag passed in workflows already parallelizes LLM processing, but the **fetch** phase before it is single-threaded.

### Step 1 — Parallelize `ingest_sources.py` Fetch Loop

Replace the sequential per-target article fetch loop with `concurrent.futures.ThreadPoolExecutor`:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_article_batch(article_urls: list[str], max_workers: int = 5) -> dict[str, str]:
    """Fetches multiple article URLs concurrently. Returns {url: html_content}."""
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(fetch_html, url): url for url in article_urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                results[url] = future.result()
            except Exception as e:
                print(f"  Fetch failed for {url}: {e}")
    return results
```

Key constraint: the manifest write at the end of each fetch must remain serial (or use a lock). Only the HTTP GET calls are parallelized.

### Step 2 — Add `--fetch-workers` CLI Argument

Add `--fetch-workers N` (default `5`) to `run_ingest_and_process.py` and pass it down to `ingest_sources.py`. Update the GitHub Actions workflows to pass `--fetch-workers 5`.

### Step 3 — Rate Limiting Per Domain

With parallel fetches, it is possible to hammer a single domain (e.g. `federalreserve.gov`) with 5 simultaneous requests. Add a simple per-domain rate limiter: at most 1 request per domain per 0.5 seconds. This is a courtesy measure and avoids soft-banning.

---

## Part B — Quant Structured Change Detection

### The Current Problem

In `ingest_quant_data.py`, non-FRED data sources hit `build_dataset_snapshot()` which returns placeholder text:

```python
def build_dataset_snapshot(source_config: dict) -> str:
    return f"Placeholder snapshot for {source_config['name']} — real fetch not yet implemented."
```

This means the quant pipeline for non-FRED sources:

- Always produces a "new" record (the placeholder text changes with timestamp)
- Never contains real data
- Wastes summarizer API calls on placeholder content

### Step 1 — Implement Real Fetch for Non-FRED Quant Sources

For each `datasets` entry in `config/quant_sources.json`, implement a proper fetch handler based on `source_type`:


| Source type | Fetch method                                                               |
| ----------- | -------------------------------------------------------------------------- |
| `worldbank` | World Bank API (added in Phase 6)                                          |
| `bls`       | BLS Public Data API — `https://api.bls.gov/publicAPI/v2/timeseries/data/`  |
| `bea`       | BEA API — `https://apps.bea.gov/api/data?UserID={KEY}&method=GetData&...`  |
| `treasury`  | TreasuryDirect API or FRED mirror                                          |
| `csv_url`   | Download CSV from configured URL, parse with standard library `csv` module |


### Step 2 — Numeric Change Detection

Replace the content-hash approach for quant snapshots with numeric comparison:

1. Load the most recent snapshot for a series from the manifest (SQLite `quant_seen_series` table added in Phase 3)
2. Fetch current data as a list of `{date: value}` pairs
3. Compare: if the newest data point date is **later than** the stored most-recent date, there is genuinely new data
4. Only create a new quant record if new data points exist — otherwise skip with "no new data"
5. The record content includes a structured summary: `"New release: {series_name}. Latest value: {value} ({date}). Previous: {prev_value} ({prev_date}). Change: {delta} ({pct_change}%)"`

This makes quant records far more informative and eliminates false "new data" triggers from placeholder changes.

### Step 3 — BLS API Key (if using BLS)

The BLS Public Data API has two tiers:

- **Unregistered:** 25 queries/day, 10 years of data — may be sufficient
- **Registered:** 500 queries/day, 20 years of data — requires free registration

> **ACTION REQUIRED (only if BLS series are enabled and unregistered tier is insufficient):**
>
> Register for a free BLS API key at `https://data.bls.gov/registrationEngine/`
>
> Then add to GitHub Secrets:
>
>
> | Secret name   | Value                     |
> | ------------- | ------------------------- |
> | `BLS_API_KEY` | Your BLS registration key |
>
>
> Add to the relevant workflow's `env:` block:
>
> ```yaml
> BLS_API_KEY: ${{ secrets.BLS_API_KEY }}
> ```

### Step 4 — BEA API Key (if using BEA)

The BEA API requires a free API key.

> **ACTION REQUIRED (only if BEA data sources are enabled):**
>
> Register for a free BEA API key at `https://apps.bea.gov/API/signup/`
>
> Then add to GitHub Secrets:
>
>
> | Secret name   | Value                     |
> | ------------- | ------------------------- |
> | `BEA_API_KEY` | Your BEA registration key |
>
>
> Add to the relevant workflow's `env:` block:
>
> ```yaml
> BEA_API_KEY: ${{ secrets.BEA_API_KEY }}
> ```

FRED, BIS, IMF, World Bank, arXiv, SSRN, and EDGAR all work without any additional keys. BLS and BEA keys are only needed if you add those series to `config/quant_sources.json`.

---

## New GitHub Secrets Required


| Secret        | Required when                                               | How to obtain                                    |
| ------------- | ----------------------------------------------------------- | ------------------------------------------------ |
| `BLS_API_KEY` | BLS series added to quant config AND free tier insufficient | Free: `https://data.bls.gov/registrationEngine/` |
| `BEA_API_KEY` | BEA data series added to quant config                       | Free: `https://apps.bea.gov/API/signup/`         |


---

## Files Changed in This Phase


| File                                     | Change                                                                                    |
| ---------------------------------------- | ----------------------------------------------------------------------------------------- |
| `scripts/ingest_sources.py`              | Add `ThreadPoolExecutor` fetch loop; per-domain rate limiter                              |
| `scripts/run_ingest_and_process.py`      | Add `--fetch-workers` CLI argument                                                        |
| `.github/workflows/process-articles.yml` | Add `--fetch-workers 5` to run command                                                    |
| `scripts/ingest_quant_data.py`           | Replace `build_dataset_snapshot` with real source-type handlers; numeric change detection |
| `config/quant_sources.json`              | Add BLS and BEA series entries (if desired)                                               |


---

## Acceptance Criteria

- `ingest_sources.py` with 20+ targets completes the HTTP fetch phase at least 3x faster than sequential
- No single domain receives more than 2 simultaneous requests
- A FRED series with no new data since last run produces no new quant record ("no new data" log line)
- A FRED series with a new release produces a record with the delta value in its content
- `build_dataset_snapshot` placeholder function is removed from `ingest_quant_data.py`

