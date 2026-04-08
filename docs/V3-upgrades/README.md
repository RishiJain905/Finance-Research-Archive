# Finance Research Archive — V3 Upgrade Plan

This folder contains the sequential implementation plan for the V3 upgrade. Each phase is a self-contained document with full implementation detail, file change lists, and acceptance criteria.

---

## Phase Sequence


| Phase | File                                                                                                                 | Summary                                                                                   | Secrets needed                                                         |
| ----- | -------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| 1     | [phase-1-fix-core-pipeline-bugs.md](phase-1-fix-core-pipeline-bugs.md)                                               | Fix listing URL permanent block, re-enable RSS feeds, raise caps, fix keyword `days_back` | None                                                                   |
| 2     | [phase-2-dynamic-keyword-injection.md](phase-2-dynamic-keyword-injection.md)                                         | LLM-generated ephemeral queries + date-stamped static queries                             | None (uses existing `OPENAI_API_KEY`)                                  |
| 3     | [phase-3-sqlite-manifest-migration.md](phase-3-sqlite-manifest-migration.md)                                         | Replace JSON manifests with SQLite database                                               | None                                                                   |
| 4     | [phase-4-sec-edgar-pipeline.md](phase-4-sec-edgar-pipeline.md)                                                       | New pipeline: SEC EDGAR 8-K / 10-K / 10-Q filings                                         | None (EDGAR is free, no key)                                           |
| 5     | [phase-5-academic-papers-pipeline.md](phase-5-academic-papers-pipeline.md)                                           | New pipeline: arXiv q-fin and SSRN research papers                                        | None                                                                   |
| 6     | [phase-6-bis-imf-worldbank-pipeline.md](phase-6-bis-imf-worldbank-pipeline.md)                                       | Wire up BIS, IMF, World Bank feeds (already in sources.json, not ingested)                | None                                                                   |
| 7     | [phase-7-local-file-drop-and-telegram-ingestion.md](phase-7-local-file-drop-and-telegram-ingestion.md)               | Drop PDFs into inbox folder; send URLs via Telegram bot                                   | `TELEGRAM_INGEST_BOT_TOKEN`, `TELEGRAM_INGEST_CHAT_ID`                 |
| 8     | [phase-8-vector-store-and-rag-foundation.md](phase-8-vector-store-and-rag-foundation.md)                             | ChromaDB vector store, semantic dedup, local archive search                               | None (uses existing `OPENAI_API_KEY`)                                  |
| 9     | [phase-9-pipeline-health-dashboard.md](phase-9-pipeline-health-dashboard.md)                                         | Weekly Markdown health report + auto-disable stale sources                                | None (uses existing `TELEGRAM_BOT_TOKEN`)                              |
| 10    | [phase-10-parallel-workers-and-quant-structured-change.md](phase-10-parallel-workers-and-quant-structured-change.md) | Parallel HTTP fetching; real quant data fetch replacing placeholders                      | `BLS_API_KEY`, `BEA_API_KEY` (optional, only if those sources enabled) |


---

## GitHub Secrets Master List

Secrets you need to add **before** each phase that requires them. All secrets go to:
`GitHub repository → Settings → Secrets and variables → Actions → New repository secret`

### Already Exist (V2)

- `OPENAI_API_KEY`
- `OPENAI_BASE_URL`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `FRED_API_KEY`
- `TAVILY_API_KEY`
- `GITHUB_TRIGGER_TOKEN`

### New Secrets — Add Before Phase 7


| Secret name                 | Purpose                                                | How to get it                                                                                                |
| --------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| `TELEGRAM_INGEST_BOT_TOKEN` | Telegram bot that receives URLs you send for ingestion | Open Telegram → message @BotFather → `/newbot` → follow prompts                                              |
| `TELEGRAM_INGEST_CHAT_ID`   | Your personal chat ID for the ingest bot               | Message the new bot, then call `https://api.telegram.org/bot{TOKEN}/getUpdates` and copy the `chat.id` field |


### New Secrets — Add Before Phase 10 (Optional)

Only needed if you add BLS or BEA series to `config/quant_sources.json`:


| Secret name   | Purpose                              | How to get it                                                 |
| ------------- | ------------------------------------ | ------------------------------------------------------------- |
| `BLS_API_KEY` | Bureau of Labor Statistics data API  | Free registration: `https://data.bls.gov/registrationEngine/` |
| `BEA_API_KEY` | Bureau of Economic Analysis data API | Free registration: `https://apps.bea.gov/API/signup/`         |


---

## Dependency Graph

```
Phase 1 (critical bugs)
    ├── Phase 2 (dynamic keywords)
    ├── Phase 3 (SQLite) ──────────── Phase 8 (vector store)
    │                                 Phase 9 (health dashboard)
    │                                 Phase 10 (parallel + quant)
    ├── Phase 4 (EDGAR)
    ├── Phase 5 (academic papers) ─── Phase 6 (BIS/IMF/WB)
    └── Phase 7 (file drop + Telegram)
```

Phases 2–7 can run in any order after Phase 1 is complete.
Phases 8, 9, 10 benefit from Phase 3 but are not strictly blocked by it.

---

## Recommended Execution Order

For maximum immediate impact:

1. **Phase 1** — fixes the silent "zero new records" problem. Do this first.
2. **Phase 3** — SQLite migration, infrastructure stability before adding more sources.
3. **Phase 4** — EDGAR is the highest-signal new source with the simplest implementation.
4. **Phase 2** — dynamic keywords multiplies the value of the keyword lane.
5. **Phase 5 + 6** — academic and institutional feeds, minimal new code.
6. **Phase 9** — health dashboard so you can see everything is working.
7. **Phase 8** — vector store and semantic dedup.
8. **Phase 7** — Telegram bot and file drop for convenience.
9. **Phase 10** — parallel workers and quant cleanup, last polish.

