# Phase 9 — Pipeline Health Dashboard and Source Auto-Disable

**Depends on:** Phase 1 complete, Phase 3 recommended (SQLite makes querying source stats trivial)  
**Priority:** Medium — makes silent failures visible; currently a broken source just wastes CI budget forever with no indication

---

## What This Phase Does

Adds two connected systems:

1. **Health dashboard** — a weekly Markdown report committed to `docs/pipeline_health.md` showing per-source performance over the last 30 days
2. **Source auto-disable** — sources that produce zero accepted records for a configurable number of consecutive runs are automatically flagged, reducing wasted API calls and CI minutes

---

## Part A — Pipeline Health Dashboard

### What It Reports

Per-source metrics for the last 30 days:


| Source               | Raw fetched | Passed filter | Accepted | Rejected | In review | Last new record    |
| -------------------- | ----------- | ------------- | -------- | -------- | --------- | ------------------ |
| Fed RSS              | 42          | 38            | 12       | 26       | 0         | 2026-04-07         |
| arXiv q-fin          | 20          | 18            | 7        | 11       | 0         | 2026-04-06         |
| SEC EDGAR            | 15          | 14            | 9        | 5        | 1         | 2026-04-08         |
| Keyword: repo_stress | 8           | 2             | 0        | 2        | 0         | 2026-03-21 ← STALE |


Also includes:

- Total records in `data/accepted/` and `data/review_queue/`
- Quant series with no new data point in 30+ days
- Top 5 most productive sources this month
- Sources flagged for auto-disable (see Part B)

### Step 1 — Create `scripts/generate_health_report.py`

Queries the SQLite manifest database (Phase 3) or scans `data/accepted/` JSON files for `source` and `processed_at` fields. Aggregates by source name into a Markdown table. Also reads `data/candidate_manifests/query_performance.json` for keyword lane stats.

If Phase 3 is not yet complete, the script can scan `data/accepted/*.json` directly — slower but functional.

### Step 2 — Create `.github/workflows/pipeline-health.yml`

```yaml
name: Pipeline Health Report

on:
  workflow_dispatch:
  schedule:
    - cron: '0 9 * * 1'  # Every Monday at 09:00 UTC

jobs:
  health:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: python scripts/generate_health_report.py
      - name: Commit report
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add docs/pipeline_health.md
          git diff --staged --quiet || git commit -m "chore: weekly pipeline health report"
          git push
```

### Step 3 — Optional Telegram Notification

At the end of `generate_health_report.py`, send a Telegram summary using the existing notification bot:

- Total new records this week
- Number of stale sources
- Any sources newly auto-disabled (see Part B)

Uses existing `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` secrets — no new secrets needed.

---

## Part B — Source Auto-Disable

### How It Works

Each time a pipeline runs, it updates a source performance counter in the manifest (or SQLite DB):

- `consecutive_empty_runs` — incremented when a source produces zero raw records in a run
- Reset to 0 when the source produces at least one record

When `consecutive_empty_runs` exceeds a configurable threshold (default: `10`), the source is automatically set to `"auto_disabled": true` in its config file.

### Config Entry for Threshold

Add to a new `config/health_config.json`:

```json
{
  "auto_disable_after_empty_runs": 10,
  "stale_source_warning_days": 14,
  "report_lookback_days": 30,
  "notify_telegram_on_stale": true
}
```

### Step 4 — Create `scripts/source_health_tracker.py`

A module called at the end of each ingest script. For each source in the run:

- Record number of raw records fetched
- Update `consecutive_empty_runs` counter
- If threshold exceeded: write `"auto_disabled": true` to the source's config entry

Auto-disabled sources appear in the weekly health report with a reason and the date they were disabled.

### Step 5 — Re-enable Flow

Auto-disabled sources are not deleted — they just have `"auto_disabled": true`. To re-enable: set `"auto_disabled": false` and optionally reset the counter. Add a note in the health report listing how to re-enable each flagged source so the action is clear.

---

## New GitHub Secrets Required

**None.** The health report uses existing `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` secrets for optional notifications. No new secrets needed.

---

## Files Changed in This Phase


| File                                    | Change                                                      |
| --------------------------------------- | ----------------------------------------------------------- |
| `scripts/generate_health_report.py`     | New — weekly Markdown report generator                      |
| `scripts/source_health_tracker.py`      | New — per-run source counter updates                        |
| `config/health_config.json`             | New — thresholds and notification config                    |
| `.github/workflows/pipeline-health.yml` | New — Monday morning health workflow                        |
| `scripts/ingest_sources.py`             | Call `source_health_tracker.update()` at end of each target |
| `scripts/ingest_rss.py`                 | Same                                                        |
| `scripts/ingest_arxiv.py`               | Same (Phase 5)                                              |
| `scripts/ingest_edgar.py`               | Same (Phase 4)                                              |
| `docs/pipeline_health.md`               | Generated and committed weekly                              |


---

## Acceptance Criteria

- Running `generate_health_report.py` produces a valid `docs/pipeline_health.md` with per-source stats
- A source with `consecutive_empty_runs >= 10` shows `"auto_disabled": true` in its config file
- The weekly workflow commits the report to the repository every Monday
- Telegram notification (if enabled) fires with the weekly summary
- Re-enabling an auto-disabled source by setting `"auto_disabled": false` causes it to run again on the next pipeline execution

