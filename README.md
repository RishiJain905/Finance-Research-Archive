# Finance Research Archive

A quality-controlled finance research archive for future RAG, focused on **market structure**, **macro catalysts**, and an expanding mix of **article sources** and **quantitative data sources**.

This project is built to do more than collect links. It continuously ingests finance-relevant information, filters weak inputs, summarizes and verifies records with MiniMax, routes high-confidence records into an archive, and sends uncertain items to Telegram for human approval.

---

## What this repo does

This repo runs a multi-stage research pipeline:

- ingests **article-style sources** on a frequent schedule (parallel HTTP fetch workers)
- ingests **SEC EDGAR filings** (8-K, 10-K, 10-Q) every 6 hours
- ingests **academic papers** from arXiv and SSRN
- ingests **quantitative / numeric sources** daily with real numeric change detection
- accepts **manual drops** via inbox folder (PDF/txt/html) and Telegram bot
- filters low-value or noisy inputs before spending model calls
- uses **MiniMax** to summarize and verify candidate records
- automatically routes records into `accepted`, `review_queue`, `rejected`
- sends `review_queue` items to **Telegram** for human approval
- finalizes human decisions through a callback-triggered workflow
- embeds accepted records into a **ChromaDB vector store** for RAG and semantic dedup
- runs a weekly **pipeline health dashboard** with auto-disable for stale sources

### V2.7 additions

The V2.7 layer adds intelligence on top of the base pipeline:

- **Triage & prioritization engine** — scores and routes candidates through priority buckets before processing
- **Event clustering & story graphs** — groups related records into market events with narrative + quant evidence
- **Watchlists & thesis tracking** — monitors specific topics and tracks thesis validity over time
- **Article-quant enrichment** — links narrative articles to quantitative records with deterministic scoring
- **Source performance analytics** — tracks per-source acceptance rates and generates actionable recommendations
- **Massive source expansion** — 108 additional curated sources across 7 families (central banks, regulators, exchanges, think tanks, etc.)

### V3 additions (Phases 1–10, all complete)

V3 fixes, stabilizes, and significantly extends the system:

- **Phase 1** — Fixed listing URL permanent-block bug, re-enabled RSS feeds, raised ingest caps, fixed keyword `days_back`
- **Phase 2** — LLM-generated ephemeral keyword queries from recent accepted records each run
- **Phase 3** — Replaced JSON manifests with a transactional SQLite database (`data/archive.db`)
- **Phase 4** — New SEC EDGAR pipeline: polls 8-K/10-K/10-Q filings for configured CIKs every 6 hours
- **Phase 5** — New academic papers pipeline: arXiv q-fin/econ papers ingested on every article run
- **Phase 6** — BIS, IMF, World Bank feeds activated; World Bank API quant series added
- **Phase 7** — Inbox file drop (PDF/txt/html) and Telegram bot for ad-hoc URL ingestion
- **Phase 8** — ChromaDB vector store, semantic dedup before summarization, local `search_archive.py`
- **Phase 9** — Weekly Markdown health dashboard, consecutive-empty-run auto-disable, Telegram health alert
- **Phase 10** — Parallel HTTP fetch workers (ThreadPoolExecutor + per-domain rate limiting); real TreasuryFiscalData and NY Fed quant fetchers; numeric data-date change detection replacing placeholders

The end goal is a clean, growing finance research archive that can later power:
- RAG
- dashboards
- eval sets
- finance copilots
- research digests

---

## Focus areas

This archive currently focuses on:

- **Market structure**
  - liquidity
  - repo / funding
  - Treasury issuance
  - auctions
  - yields
  - ETF / exchange / clearing / rulemaking context
- **Macro catalysts**
  - inflation
  - CPI / PPI
  - labor market
  - GDP / spending / growth
  - central bank policy
  - rates / policy path expectations

---

## High-level architecture

### Full system flow

```mermaid
flowchart TD
    subgraph Ingest
        A1[Article Pipeline<br/>every 2h] --> B1[ingest_sources.py<br/>parallel fetch workers]
        A2[Quant Pipeline<br/>daily] --> B2[ingest_quant_data.py<br/>FRED / WB / Treasury / NYFed]
        A3[EDGAR Pipeline<br/>every 6h] --> B3[ingest_edgar.py]
        A4[arXiv / SSRN<br/>per article run] --> B4[ingest_arxiv.py]
        A5[Inbox file drop<br/>on push] --> B5[ingest_inbox.py]
        A6[Telegram bot<br/>every 30min] --> B6[telegram_ingest_bot.py]
        A7[RSS feeds<br/>per article run] --> B7[ingest_rss.py]
    end

    B1 & B4 & B7 --> RAW[data/raw]
    B2 --> RAW
    B3 --> RAW
    B5 & B6 --> RAW

    RAW --> F[filter_raw_records.py<br/>+ semantic dedup]
    F -->|filtered| FO[data/filtered_out]
    F -->|kept| P[process_record.py]

    P --> S[run_summarizer.py<br/>MiniMax]
    S --> V[run_verifier.py<br/>MiniMax]
    V --> R[route_record.py]

    R -->|accepted| ACC[data/accepted]
    R -->|rejected| REJ[data/rejected]
    R -->|review_queue| RQ[data/review_queue]

    ACC --> VS[vector_store.py<br/>ChromaDB upsert]

    RQ --> TG[send_pending_reviews.py]
    TG --> TM[Telegram message<br/>Approve / Reject]
    TM --> CB[Render callback server]
    CB --> GH[finalize-review.yml]
    GH --> FIN[finalize_review.py]
    FIN -->|approve| ACC
    FIN -->|reject| REJ

    ACC --> INT[V2.7 Intelligence Layer]
    INT --> EC[cluster_records.py]
    INT --> AQL[link_article_quant.py]
    INT --> SA[source_analytics.py]
    INT --> WL[watchlist_engine.py]

    HEALTH[pipeline-health.yml<br/>weekly Monday] --> HR[generate_health_report.py<br/>docs/pipeline_health.md]
```

---

# Workflow Architecture

This section explains every active GitHub Actions workflow and how records move through the system.

## Active workflows

| Workflow | Schedule | Purpose |
|----------|----------|---------|
| `process-articles.yml` | Every 2 hours | Article + RSS + arXiv ingest, filter, summarize, review |
| `process-quant.yml` | Daily 13:15 UTC | FRED / World Bank / Treasury / NY Fed quant snapshots |
| `process-edgar.yml` | Every 6 hours | SEC EDGAR 8-K / 10-K / 10-Q filings |
| `process-keyword-discovery.yml` | Every 4 hours | Keyword-driven search discovery |
| `process-seed-crawl.yml` | Every 4 hours | Seed site link crawling |
| `process-backlog.yml` | Manual dispatch | Process pre-existing raw backlog |
| `process-inbox.yml` | On push to data/inbox | Parse dropped PDF/txt/html files |
| `process-telegram-inbox.yml` | Every 30 minutes | Drain Telegram bot URL queue |
| `pipeline-health.yml` | Monday 09:00 UTC | Weekly health report, Telegram summary |
| `finalize-review.yml` | Manual dispatch | Apply human approve/reject from Telegram |

---

## 1. Article Research Pipeline

**Workflow file:** `.github/workflows/process-articles.yml`

Runs every 2 hours. Ingests article sources, RSS, and arXiv papers; filters weak records; processes survivors through MiniMax summarize + verify + route; sends review items to Telegram.

### What happens inside `run_ingest_and_process.py`

```
ingest_sources.py   --fetch-workers 5   (parallel HTTP, per-domain rate limit)
ingest_rss.py
ingest_arxiv.py
filter_raw_records.py
  → for each surviving record:
      process_record.py
          run_summarizer.py  (MiniMax)
          run_verifier.py    (MiniMax)
          route_record.py
send_pending_reviews.py
```

### Article pipeline diagram

```mermaid
flowchart TD
    A[process-articles.yml] --> ORC[run_ingest_and_process.py]

    ORC --> IS[ingest_sources.py]
    ORC --> RSS[ingest_rss.py]
    ORC --> ARX[ingest_arxiv.py]

    subgraph ParallelFetch [Parallel HTTP Fetch - Phase 10]
        IS --> PF[fetch_article_batch<br/>ThreadPoolExecutor N=5]
        PF --> DL[Per-domain rate limiter<br/>0.5s minimum interval]
    end

    IS & RSS & ARX --> RAW[data/raw]

    ORC --> FILT[filter_raw_records.py<br/>rules + semantic dedup]
    FILT -->|weak or duplicate| FO[data/filtered_out]
    FILT -->|survives| PR[process_record.py]

    PR --> SUM[run_summarizer.py<br/>MiniMax]
    SUM --> VER[run_verifier.py<br/>MiniMax]
    VER --> RTE[route_record.py]

    RTE -->|accepted| ACC[data/accepted]
    RTE -->|review_queue| RQ[data/review_queue]
    RTE -->|rejected| REJ[data/rejected]

    ACC --> VS[vector_store.py<br/>ChromaDB upsert]

    ORC --> SPR[send_pending_reviews.py]
    SPR --> TG[Telegram review messages]

    ORC --> GIT[git commit / push data]
```

---

## 2. Quant Research Pipeline

**Workflow file:** `.github/workflows/process-quant.yml`

Runs daily. Fetches numeric data from FRED, World Bank, TreasuryFiscalData, and NY Fed. Uses **numeric date-comparison change detection** (Phase 10): a new record is only written when the fetched data point date is strictly newer than the most recently stored date for that series.

### Quant sources

| Source | Type | Secrets |
|--------|------|---------|
| FRED (SOFR, DFF, DGS2, DGS10, IORB) | `series` | `FRED_API_KEY` |
| World Bank (GDP growth, inflation, debt, trade) | `worldbank_series` | None |
| TreasuryFiscalData (auctions, upcoming auctions) | `datasets` | None |
| NY Fed (repo operations) | `datasets` | None |

### Quant pipeline diagram

```mermaid
flowchart TD
    QC[quant_sources.json] --> QM[ingest_quant_data.py main]

    QM --> FRED[build_fred_snapshot<br/>FRED observations API]
    QM --> WB[build_wb_snapshot<br/>World Bank API]
    QM --> DS[build_dataset_snapshot<br/>dispatcher]

    DS --> TR[build_treasury_auctions_snapshot<br/>FiscalData API]
    DS --> TRU[build_treasury_upcoming_snapshot<br/>FiscalData API]
    DS --> NYFED[build_nyfed_snapshot<br/>NY Fed repo API]

    FRED & WB & TR & TRU & NYFED --> CHK[Numeric change check<br/>get_quant_series_latest_data_date]

    CHK -->|fetched_date > stored_date| WRITE[write_raw_snapshot<br/>add_quant_series with latest_data_date]
    CHK -->|no new data| SKIP[log: no new data, skip]

    WRITE --> RAW[data/raw]
    RAW --> PR[run_quant_pipeline.py<br/>process_record.py]
    PR --> SUM[MiniMax summarize]
    SUM --> VER[MiniMax verify]
    VER --> RTE[route_record.py]
    RTE -->|accepted| ACC[data/accepted]
```

---

## 3. EDGAR Pipeline (Phase 4)

**Workflow file:** `.github/workflows/process-edgar.yml`

Runs every 6 hours. Polls EDGAR's full-text search for configured CIK numbers and filing types (8-K, 10-K, 10-Q). Dedupes by accession number using the SQLite manifest so the same filing is never processed twice.

### EDGAR pipeline diagram

```mermaid
flowchart TD
    EC[edgar_sources.json<br/>CIKs + filing types] --> IE[ingest_edgar.py]

    IE --> EPI[EDGAR full-text search API<br/>no key required]
    EPI --> FILINGS[filing list per CIK]

    FILINGS --> DEDUP{accession seen<br/>in archive.db?}
    DEDUP -->|yes| SKIP[skip]
    DEDUP -->|no| RAW[write data/raw record]

    RAW --> RP[run_edgar_pipeline.py]
    RP --> PR[process_record.py]
    PR --> SUM[MiniMax summarize]
    SUM --> VER[MiniMax verify]
    VER --> RTE[route_record.py]

    RTE -->|accepted| ACC[data/accepted]
    RTE -->|review_queue| RQ[data/review_queue]
    RTE -->|rejected| REJ[data/rejected]
```

---

## 4. Academic Papers Pipeline (Phase 5)

**Script:** `scripts/ingest_arxiv.py` — called inside every article pipeline run.

Ingests q-fin and econ papers from arXiv. Each paper's abstract and metadata become a raw record that passes through the same summarize/verify/route chain. Academic sources receive a scoring bonus in `scoring_rules.json`.

### Academic pipeline diagram

```mermaid
flowchart TD
    AC[academic_sources.json<br/>arXiv categories + SSRN RSS] --> IA[ingest_arxiv.py]

    IA --> ARXIV[arXiv API<br/>q-fin.* / econ.*]
    IA --> SSRN[SSRN RSS<br/>optional fallback]

    ARXIV & SSRN --> META[paper metadata<br/>title, abstract, authors, date]
    META --> DEDUP{seen in archive.db?}
    DEDUP -->|yes| SKIP[skip]
    DEDUP -->|no| RAW[data/raw record]

    RAW --> PR[process_record.py]
    PR --> SUM[MiniMax summarize]
    SUM --> VER[MiniMax verify]
    VER --> RTE[route_record.py]

    RTE -->|accepted| ACC[data/accepted<br/>with academic source bonus]
```

---

## 5. Inbox and Telegram Ingestion (Phase 7)

Two complementary manual-input channels that bypass scheduled crawls.

### Inbox file drop

Drop a PDF, `.txt`, `.html`, or `.md` file into `data/inbox/`. The `process-inbox.yml` workflow triggers on push and runs `ingest_inbox.py`, which parses the file and writes it as a standard raw record.

### Telegram ingestion bot

Send a URL (or paste text) to the Telegram ingest bot. `process-telegram-inbox.yml` runs every 30 minutes, polls the bot for new messages, queues URLs in `data/inbox_queue.json`, and the next article pipeline run drains that queue via `drain_inbox_queue()` in `run_ingest_and_process.py`.

### Ingestion channels diagram

```mermaid
flowchart TD
    subgraph Manual [Manual Input Channels]
        PDF[PDF / txt / html / md<br/>dropped in data/inbox/]
        URL[URL or text<br/>sent to Telegram bot]
    end

    PDF --> INB[ingest_inbox.py<br/>process-inbox.yml on push]
    URL --> TGB[telegram_ingest_bot.py<br/>process-telegram-inbox.yml every 30min]

    TGB --> Q[data/inbox_queue.json]
    Q --> DRAIN[drain_inbox_queue<br/>in run_ingest_and_process.py]

    INB --> RAW[data/raw]
    DRAIN --> RAW

    RAW --> PR[process_record.py]
    PR --> SUM[MiniMax summarize]
    SUM --> VER[MiniMax verify]
    VER --> RTE[route_record.py]
    RTE -->|accepted| ACC[data/accepted]
```

---

## 6. Vector Store and RAG Foundation (Phase 8)

**Scripts:** `scripts/vector_store.py`, `scripts/backfill_vector_store.py`, `scripts/search_archive.py`

Every accepted record is embedded and upserted into a ChromaDB collection at `data/vector_store/`. The same store is queried during `filter_raw_records.py` for **semantic deduplication** before the expensive MiniMax steps. `search_archive.py` provides a local CLI for querying the archive by meaning.

### Vector store diagram

```mermaid
flowchart TD
    subgraph Upsert [On Accept]
        ACC[data/accepted record] --> VS[vector_store.py]
        VS --> EMB[sentence-transformers<br/>nomic-embed-text]
        EMB --> CHROMA[ChromaDB<br/>data/vector_store/]
    end

    subgraph Dedup [During Filtering]
        RAW2[new raw record] --> FILT[filter_raw_records.py]
        FILT --> SEMQ[semantic similarity query<br/>vector_store.py]
        SEMQ --> CHROMA
        CHROMA --> SIM{similarity >= threshold?}
        SIM -->|yes| SEMSKIP[filtered out as duplicate]
        SIM -->|no| KEEP[continue to process_record]
    end

    subgraph Search [Local RAG Query]
        QUERY[search_archive.py<br/>natural language query] --> CHROMA
        CHROMA --> RESULTS[top-K accepted records]
    end

    BACKFILL[backfill_vector_store.py<br/>one-time or periodic] --> CHROMA
```

---

## 7. Pipeline Health Dashboard (Phase 9)

**Workflow file:** `.github/workflows/pipeline-health.yml` — runs every Monday at 09:00 UTC.

Generates `docs/pipeline_health.md` with 30-day per-source stats. Tracks `consecutive_empty_runs` in the `source_health` table of `archive.db`. Sources that exceed the `auto_disable_after` threshold in `config/health_config.json` are automatically flagged with `auto_disabled = 1` and skipped by ingest scripts until manually re-enabled.

### Health dashboard diagram

```mermaid
flowchart TD
    DB[archive.db<br/>source_health table] --> GHR[generate_health_report.py]
    HC[health_config.json<br/>thresholds] --> GHR

    GHR --> RPT[docs/pipeline_health.md<br/>per-source 30-day stats]
    GHR --> TGA[Telegram health alert<br/>optional summary]

    subgraph AutoDisable [Auto-disable on Every Ingest Run]
        IS[ingest_sources.py] --> SHT[source_health_tracker.py]
        RSS[ingest_rss.py] --> SHT
        SHT --> DB2[update consecutive_empty_runs<br/>in archive.db]
        DB2 --> CHK{exceeds threshold?}
        CHK -->|yes| DIS[set auto_disabled = 1]
        CHK -->|no| OK[continue normally]
        DIS --> SKIP2[source skipped next run]
    end
```

---

## 8. Three-Lane Discovery Architecture

Sources enter through three lanes that converge into the same processing pipeline:

```mermaid
flowchart TD
    L1[Lane 1: Trusted Sources<br/>federalreserve.gov, BIS, IMF, WB, etc.<br/>ingest_sources.py with parallel fetch] --> CAND[Candidate Pipeline]
    L2[Lane 2: Keyword Discovery<br/>static + LLM-generated ephemeral queries<br/>run_keyword_discovery.py] --> CAND
    L3[Lane 3: Seed Site Crawl<br/>config/seed_sites.json<br/>run_seed_crawl.py] --> CAND
    L4[Lane 4: Structured Sources<br/>EDGAR + arXiv + RSS<br/>dedicated pipelines] --> CAND

    CAND --> DEDUP[Deduplication<br/>URL / title / content hash + semantic]
    DEDUP --> SCORE[Scoring<br/>V2.5 unified weighted score]
    SCORE --> TRIAGE[Triage<br/>V2.7 priority buckets]
    TRIAGE --> PR[process_record.py]
    PR --> MMS[MiniMax summarize + verify]
    MMS --> ROUTE[route_record.py]

    ROUTE -->|accepted| ACC[data/accepted]
    ROUTE -->|rejected| REJ[data/rejected]
    ROUTE -->|review| RQ[data/review_queue]

    RQ --> TG[Telegram human review]
    TG -->|approve| ACC
    TG -->|reject| REJ
```

---

## 9. V2.5 Unified Scoring System

**Scripts:**
- `scripts/extract_candidate_features.py`
- `scripts/score_candidate.py`
- `scripts/score_candidates_batch.py`

### Scoring components

| Component | Weight | Description |
|-----------|--------|-------------|
| domain_trust | 0.25 | Trust from domain baselines (high=100, medium=50, low=10) |
| url_quality | 0.20 | URL structure hints (positive: press, report, research) |
| title_quality | 0.20 | Title keyword hints |
| keyword_match | 0.20 | Match against keyword bundles |
| freshness | 0.10 | Age decay (full score < 168 hours) |
| lane_reliability | 0.10 | Lane-based reliability (trusted=100, keyword=50, seed=30) |
| duplication_risk | -0.15 | Duplicate penalty (URL/title hash matches) |

### Scoring and priority flow

```mermaid
flowchart TD
    A[candidate record] --> B[extract_candidate_features.py]

    B --> C[Freshness]
    B --> D[URL quality]
    B --> E[Title quality]
    B --> F[Domain trust<br/>baseline + memory blend]
    B --> G[Lane reliability]
    B --> H[Duplication check]

    C & D & E & F & G & H --> I[score_candidate.py<br/>weighted sum]
    I --> J[candidate_score 0-100]

    J --> K{priority bucket}
    K -->|>= 75| L[High: process immediately]
    K -->|60-74| M[Medium: defer]
    K -->|45-59| N[Low: defer]
    K -->|< 45| O[Skip: filtered_out]
```

---

## 10. V2.5 Memory System

**Scripts:** `scripts/memory_persistence.py`, `scripts/memory_manager.py`, `scripts/update_source_memory.py`

| Memory Type | Tracks | Stored At |
|-------------|--------|-----------|
| Domain Memory | trust_score, accepted/rejected counts | `data/source_memory/domain_memory.json` |
| Path Memory | path-pattern trust (e.g. `/research/`) | `data/source_memory/path_memory.json` |
| Source Memory | per-source yield/noise ratios | `data/source_memory/source_memory.json` |

### Cold-start blending

New domains start at baseline trust and shift to learned trust as evidence accumulates:

| Samples | Baseline Weight | Learned Weight |
|---------|-----------------|----------------|
| 0–9 | 90–100% | 0–10% |
| 10–24 | blending | blending |
| 25+ | 10% | 90% |

Human decisions count 2× more than automatic decisions.

---

## 11. V2.7 Intelligence Layer

V2.7 adds six post-processing subsystems on top of the archive:

### Triage and Prioritization Engine

```mermaid
flowchart TD
    CANDS[discovered candidates] --> BG[triage_budget_gate.py]
    BG -->|over budget| DEFER[defer to next run]
    BG -->|within budget| TE[triage_engine.py]

    TE --> TS[compute triage score<br/>source_trust + topic_relevance<br/>+ content_quality + freshness]

    TS -->|>= process_now| IMM[process immediately]
    TS -->|>= defer| DQ[defer queue]
    TS -->|< defer| DISC[discard]

    IMM --> PR[process_record.py]
    DQ --> NEXT[next triage run]
    DISC --> TM[triage_metrics.py<br/>data/triage/metrics.json]
```

### V2.7 Complete System Overview

```mermaid
flowchart TD
    subgraph Discovery
        A1[Trusted Sources]
        A2[Keyword Discovery]
        A3[Seed Crawl]
        A4[EDGAR filings]
        A5[arXiv / SSRN]
        A6[Inbox / Telegram]
        A7[108 Expansion Sources]
    end

    subgraph Triage
        B1[triage_budget_gate.py]
        B2[triage_engine.py]
    end

    subgraph Processing
        C1[process_record.py]
        C2[MiniMax summarize]
        C3[MiniMax verify]
        C4[route_record.py]
    end

    subgraph Archive
        D1[data/accepted]
        D2[data/review_queue]
        D3[data/rejected]
        D4[ChromaDB vector store]
    end

    subgraph Intelligence
        E1[cluster_records.py<br/>event clustering]
        E2[link_article_quant.py<br/>article-quant linking]
        E3[source_analytics.py]
        E4[source_recommendations.py]
        E5[watchlist_engine.py]
        E6[generate_health_report.py]
    end

    A1 & A2 & A3 & A4 & A5 & A6 & A7 --> B1
    B1 --> B2 --> C1
    C1 --> C2 --> C3 --> C4
    C4 --> D1 & D2 & D3
    D1 --> D4
    D2 --> G[Telegram human review]
    G -->|approve| D1
    G -->|reject| D3
    D1 --> E1 & E2 & E3 & E5
    E3 --> E4
    E6 --> F6[docs/pipeline_health.md]
```

---

## 12. Article-Quant Enrichment

**Script:** `scripts/link_article_quant.py`

Deterministic config-driven linking between narrative article records and quantitative data records across four dimensions:

```mermaid
flowchart TD
    ART[data/accepted articles] --> LNK[link_article_quant.py]
    QNT[data/accepted quants] --> LNK
    EVT[data/events clusters] --> LNK
    RULES[quant_linking_rules.json] --> LNK

    LNK --> J[topic compatibility<br/>30%]
    LNK --> K[time window<br/>25%]
    LNK --> L[keyword overlap<br/>25%]
    LNK --> M[event alignment<br/>20%]

    J & K & L & M --> N[combined score 0-100]

    N -->|>= 80| P[supports]
    N -->|60-79| Q[context]
    N -->|40-59| R[weak_context]
    N -->|< 40| S[no link]

    P & Q & R --> T[data/article_quant_links/*.json]
```

---

## 13. Source Performance Analytics

**Scripts:** `scripts/source_analytics.py`, `scripts/source_recommendations.py`

Tracks acceptance / review / rejection / filtered-out ratios per source domain and generates actionable management recommendations.

```mermaid
flowchart TD
    ACC2[data/accepted] --> SA[source_analytics.py]
    RQ2[data/review_queue] --> SA
    REJ2[data/rejected] --> SA
    FO2[data/filtered_out] --> SA

    SA --> STATS[per-source stats<br/>data/source_analytics/*.json]
    STATS --> SR[source_recommendations.py]

    SR -->|accepted < 5%, filtered > 70%| DIS2[disable]
    SR -->|filtered high, accepted moderate| LOW[lower_max_links]
    SR -->|high review ratio| TIGHT[tighten]
    SR -->|zero accepted| INV[investigate]
    SR -->|strong accepted| KEEP[keep]

    DIS2 & LOW & TIGHT & INV & KEEP --> REC[data/source_recommendations/*.json]
```

---

## 14. V3 Phase 10 — Parallel Fetch Detail

### Parallel fetch worker architecture

```mermaid
flowchart TD
    TARGET[ingestion target<br/>config/ingestion_targets.json] --> IXFETCH[fetch_html index page<br/>sequential]
    IXFETCH --> LINKS[extract_links<br/>scored + filtered candidate URLs]

    LINKS --> PREFILTER[pre-filter already-processed URLs<br/>is_url_processed_as_article DB check]
    PREFILTER --> BATCH[fetch_article_batch<br/>ThreadPoolExecutor max_workers=N]

    BATCH --> W1[worker 1]
    BATCH --> W2[worker 2]
    BATCH --> WN[worker N]

    W1 & W2 & WN --> THROTTLE[_domain_throttle<br/>0.5s min interval per domain<br/>threading.Lock]
    THROTTLE --> HTTP[fetch_html with retries]
    HTTP --> RESULTS[dict url -> html_content]

    RESULTS --> SERIAL[Serial processing loop<br/>content hash check<br/>parse / classify / fingerprint<br/>write data/raw<br/>SQLite manifest writes]
```

### Quant numeric change detection detail

```mermaid
flowchart TD
    SERIES[quant series config] --> FETCH[fetch from API<br/>FRED / World Bank / Treasury / NYFed]
    FETCH --> LATEST[extract latest data point<br/>date + value]

    LATEST --> STORED[get_quant_series_latest_data_date<br/>archive.db]

    STORED --> CMP{fetched_date > stored_date?}
    CMP -->|yes — new data| WRITE2[write_raw_snapshot<br/>structured QUANT_SUMMARY content]
    CMP -->|no — stale| SKIP2[log: no new data, skipping]

    WRITE2 --> DB2[add_quant_series<br/>latest_data_date stored in archive.db]
    WRITE2 --> RAW2[data/raw quant record]
```

---

## GitHub Secrets reference

| Secret | Required by | How to obtain |
|--------|-------------|---------------|
| `OPENAI_API_KEY` | All MiniMax steps | MiniMax / OpenAI dashboard |
| `OPENAI_BASE_URL` | All MiniMax steps | Set to `https://api.minimax.io/v1` |
| `TELEGRAM_BOT_TOKEN` | Review send | @BotFather in Telegram |
| `TELEGRAM_CHAT_ID` | Review send | `getUpdates` API call |
| `FRED_API_KEY` | Quant pipeline | [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html) |
| `TAVILY_API_KEY` | Keyword discovery | [tavily.com](https://tavily.com) |
| `GITHUB_TRIGGER_TOKEN` | Finalize review callback | GitHub personal access token |
| `TELEGRAM_INGEST_BOT_TOKEN` | Phase 7 — Telegram ingestion | @BotFather in Telegram |
| `TELEGRAM_INGEST_CHAT_ID` | Phase 7 — Telegram ingestion | `getUpdates` API call |
| `BLS_API_KEY` | Phase 10 — optional BLS series | [data.bls.gov/registrationEngine](https://data.bls.gov/registrationEngine/) |
| `BEA_API_KEY` | Phase 10 — optional BEA series | [apps.bea.gov/API/signup](https://apps.bea.gov/API/signup/) |
