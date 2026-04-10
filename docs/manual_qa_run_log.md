# Manual QA run log — all scripts + V3 upgrades

**Session:** 2026-04-10 (local workspace `F:\Personal\RAG\Finance-Research-Archive`)

This log records a manual QA pass: CLI `--help` sweep, targeted smokes, V3 acceptance checks, and one GitHub Actions dispatch. Prefer this over `pytest` where the plan called for manual runs.

---

## Preconditions

| Check | Result |
|--------|--------|
| `pip install -r requirements.txt` | Pass |
| `python --version` | **3.13.2** (README/plan mention 3.11; 3.13 used here) |
| API keys | Not verified globally; `search_archive` and `generate_health_report` ran without OpenAI for those steps. Telegram bots not configured locally (`TELEGRAM_INGEST_BOT_TOKEN`, `TELEGRAM_BOT_TOKEN` missing where required). |

---

## Part A — CLI scripts: `python scripts/<name>.py --help`

Each script was invoked with `--help` from repo root (120s timeout per process). Exit code **0** means argparse printed help. **1** usually means the script treats the first positional as a record ID or requires env vars.

| Script | Exit | Notes |
|--------|------|--------|
| run_verifier | 1 | No argparse; `--help` treated as `record_id` → missing `data/raw/--help.txt` |
| send_pending_reviews | 0 | |
| run_ingest_and_process | 0 | |
| ingest_edgar | 0 | |
| update_story_graph | 0 | |
| ingest_inbox | 0 | |
| ingest_arxiv | 0 | |
| ingest_sources | 0 | |
| link_article_and_quant_records | 0 | |
| generate_health_report | 0 | |
| ingest_quant_data | 0 | |
| build_daily_market_structure_digest | 0 | |
| run_edgar_pipeline | 0 | |
| build_keyword_candidates | 0 | After adding `sys.path` bootstrap; **before fix:** `ModuleNotFoundError: scripts.candidate_utils` when run as `python scripts/build_keyword_candidates.py` |
| telegram_ingest_bot | 1 | Requires `TELEGRAM_INGEST_BOT_TOKEN` at startup |
| apply_human_feedback | 0 | |
| backfill_archive_quality | 0 | |
| migrate_manifest_to_db | 0 | |
| ingest_rss | 0 | |
| source_analytics | 0 | |
| run_quant_pipeline | 0 | |
| create_record | 1 | Prints usage string (not argparse `--help`) |
| update_source_memory | 1 | Positional usage only |
| run_seed_crawl | 0 | |
| drain_review_queue | 0 | |
| assign_quality_tier | 0 | |
| backfill_linked_records | 0 | |
| backfill_vector_store | 0 | |
| route_record | 1 | No argparse; looks for `data/review_queue/--help.json` |
| run_keyword_discovery | 0 | |
| source_recommendations | 0 | |
| telegram_callback_server | TIMEOUT | `__main__` calls `app.run()`; `--help` does not exit (blocks). Use `python -c "from scripts.telegram_callback_server import app; print(app.url_map)"` for import smoke, or hit `/health` when server is running. |
| watchlist_matcher | 0 | |
| send_review_to_telegram | 1 | Requires `TELEGRAM_BOT_TOKEN` |
| backfill_quality_tiers | 0 | |
| build_daily_macro_digest | 0 | |
| build_weekly_digest | 0 | |
| link_new_record | 1 | Treats `--help` as record id → missing accepted JSON |
| update_theme_memory | 0 | After fix: `sys.path` bootstrap + `import scripts.theme_memory_persistence as …` (**before:** `ImportError` on direct script path) |
| update_feedback_memory | 0 | |
| run_summarizer | 1 | No argparse; expects real `record_id` |
| cluster_records | 0 | |
| propose_keyword_expansions | 0 | |
| filter_raw_records | 0 | |
| apply_keyword_expansions | 0 | |
| get_scheduled_records | 0 | |
| search_archive | 0 | |
| process_record | 1 | Delegates to summarizer with bogus id |

**Recommendation:** For scripts without argparse, manual verification is **invocation with a real record id** (or documented usage line), not `--help`.

---

## Part A — Manual smokes (this session)

| Step | Command / action | Result |
|------|-------------------|--------|
| Health report | `python scripts/generate_health_report.py` | Pass — updated `docs/pipeline_health.md`; Telegram skipped (no creds) |
| Semantic search | `python scripts/search_archive.py "liquidity repo"` | Pass — top results printed (Chroma / embeddings loaded) |
| Theme memory CLI | `python scripts/update_theme_memory.py --help` | Pass |
| Scheduled records | `python scripts/get_scheduled_records.py` | Pass — printed `["sample_source"]` from config |
| Inbox drop (Phase 7) | Added `data/inbox/qa_manual_smoke.txt`, ran `python scripts/ingest_inbox.py` | Pass — created `inbox_qa_manual_smoke_aa1e5160`, moved file to `data/inbox/processed/` |
| arXiv ingest (Phase 5) | `python scripts/ingest_arxiv.py` | Partial — script completed exit 0 but arXiv API returned **429** for all categories (rate limit); JSON output `[]` |

**Not run (full pass) in this session:** `run_ingest_and_process`, `run_quant_pipeline`, `process_record` chain, `filter_raw_records` over full `data/raw`, `telegram_*` bots, `finalize_review`, EDGAR pipeline — these need real secrets, record IDs, or mutate large portions of `data/`. Run them in a controlled clone when ready.

---

## Part B — Library modules (indirect)

Orchestrators were at least checked for CLI health (`run_v2_candidate_pipeline.py --help` exited 0). Full candidate lane execution was not run end-to-end here. When you run `run_v2_candidate_pipeline` or `run_keyword_discovery` with real data, modules such as `dedupe_candidates`, `score_candidate`, `candidate_utils`, `generate_ephemeral_queries`, `manifest_db`, `vector_store`, and triage helpers are exercised as described in the plan.

---

## Part C — V3 phases (manual acceptance)

| Phase | Verification | Status |
|-------|----------------|--------|
| 1 Core pipeline | `ingest_sources` / `run_ingest_and_process` not fully re-run; ingest paths exercised via `ingest_inbox`, `generate_health_report` source table | Partial |
| 2 Dynamic keywords | `run_keyword_discovery --help` OK; no full lane run | Partial |
| 3 SQLite manifest | `ingest_inbox` updated fingerprints / record map; health report read SQLite | Pass (subset) |
| 4 EDGAR | Not run (no `--help`-only smoke beyond CLI) | Pending |
| 5 Academic | `ingest_arxiv.py` executed; 429 from API | Partial |
| 6 BIS/IMF/WB | Not isolated; covered only if normal ingest runs | Pending |
| 7 Inbox + Telegram | Inbox pass; Telegram ingest bot not run (no token) | Partial |
| 8 Vector / RAG | `search_archive` pass | Pass (subset) |
| 9 Health dashboard | `generate_health_report` pass | Pass |
| 10 Parallel quant | `run_quant_pipeline --help` only | Pending |

---

## GitHub Actions (`workflow_dispatch`)

| Workflow | Action |
|----------|--------|
| `process-backlog.yml` | **Dispatched** with `max_records=1` — [run 24225004225](https://github.com/RishiJain905/Finance-Research-Archive/actions/runs/24225004225) |
| `process-edgar.yml`, `process-inbox.yml`, `process-telegram-inbox.yml`, `pipeline-health.yml` | **Not on default branch** on GitHub as of this check (only local). Dispatch after merge/push. |
| `process-articles.yml`, `process-quant.yml`, `process-keyword-discovery.yml`, `process-seed-crawl.yml`, `finalize-review.yml` | Not dispatched here (cost / inputs); trigger from Actions UI when needed. |

---

## Follow-up session (deferred runs, excluding Telegram bots)

Commands executed in a second pass:

| Step | Command / note | Result |
|------|----------------|--------|
| Filter all raw | `python scripts/filter_raw_records.py` | Pass |
| Keyword discovery | `python scripts/run_keyword_discovery.py --dry-run` | Pass (0 candidates; `TAVILY_API_KEY` / `OPENAI_API_KEY` unset) |
| Seed crawl | `python scripts/run_seed_crawl.py --dry-run --max-process-records 1 --process-workers 1` | Pass |
| V2 candidate pipeline | `python scripts/run_v2_candidate_pipeline.py --lane trusted_sources` | Pass (2/2 synthetic duplicates) |
| EDGAR pipeline | `python scripts/run_edgar_pipeline.py --max-records 1 --process-workers 1` | Pass (0 new filings in lookback) |
| Quant ingest | `python scripts/ingest_quant_data.py` | Pass (7 records; FRED skipped without key) |
| Process quant | `python scripts/process_record.py wb_gdp_growth_2026_04_10` then `repo_operations_2026_04_10` | Pass after verifier fix → both routed to `rejected/` |
| Article pipeline | `python scripts/run_ingest_and_process.py --max-records 1 --process-workers 1 --fetch-workers 2 --skip-send-reviews` | Pass (~204s); processed `bea_news_for_journalists_u_s_bureau_of_economic_analysis_bea_4e00dcdb` |
| Vector backfill | `python scripts/backfill_vector_store.py` | Pass (88 upserts) |
| Analytics | `source_analytics`, `source_recommendations`, `drain_review_queue`, `link_article_quant`, `watchlist_matcher`, `link_article_and_quant_records` | Pass |
| Clustering | `python scripts/cluster_records.py` | Pass after datetime + `sys.path` fixes |
| Digests + story graph | `build_daily_macro_digest`, `build_daily_market_structure_digest`, `build_weekly_digest`, `update_story_graph` | Pass |
| Health report | `python scripts/generate_health_report.py` | Pass |

**Skipped (by request or safety):** `telegram_ingest_bot`, `send_review_to_telegram`, `send_pending_reviews`, `telegram_callback_server` long-run, `finalize_review` (requires an explicit approve/reject decision on a real `review_queue` id).

**Inbox note:** `inbox_qa_manual_smoke_aa1e5160` raw was moved to `data/filtered_out/` by `filter_raw_records`, so `process_record` for that id could not re-run.

---

## Code changes made for QA

1. [`scripts/build_keyword_candidates.py`](../scripts/build_keyword_candidates.py) — insert repo root on `sys.path` before `from scripts.candidate_utils import …` so `python scripts/build_keyword_candidates.py` matches other scripts.
2. [`scripts/update_theme_memory.py`](../scripts/update_theme_memory.py) — same path bootstrap; replace `from scripts import theme_memory_persistence` with `import scripts.theme_memory_persistence as theme_memory_persistence` so the submodule resolves when executed as a file.
3. [`scripts/run_verifier.py`](../scripts/run_verifier.py) — load `record_rules` from SQLite via `manifest_db.get_record_rules` when `data/ingestion_manifest.json` is absent (Phase 3 migration).
4. [`scripts/process_record.py`](../scripts/process_record.py) — insert repo root on `sys.path` so in-process `scripts.watchlist_matcher` imports work when invoked as `python scripts/process_record.py`.
5. [`scripts/cluster_records.py`](../scripts/cluster_records.py) — `sys.path` bootstrap for `scripts.watchlist_matcher`; normalize parsed dates to UTC-aware in `find_recent_clusters` to avoid naive/aware subtraction errors.

---

## Artifacts touched by QA

- `docs/pipeline_health.md` — regenerated (multiple times).
- `data/raw/inbox_qa_manual_smoke_aa1e5160.txt` — new raw record from inbox smoke.
- `data/inbox/processed/qa_manual_smoke.txt` — moved from inbox after ingest.
- `data/filtered_out/inbox_qa_manual_smoke_aa1e5160.txt` — after full `filter_raw_records` run.
- `data/rejected/`, `data/review_queue/`, `data/verify/`, `data/vector_store/`, `data/digests/`, `data/events/`, `data/source_recommendations/`, and SQLite manifest — updated by pipeline and clustering steps above.

Review `git status` before committing if you do not want these data files in a PR.
