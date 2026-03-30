# Finance Research Archive

A quality-controlled finance research archive for future RAG, focused on **market structure**, **macro catalysts**, and an expanding mix of **article sources** and **quantitative data sources**.

This project is built to do more than collect links. It continuously ingests finance-relevant information, filters weak inputs, summarizes and verifies records with MiniMax, routes high-confidence records into an archive, and sends uncertain items to Telegram for human approval.

---

## What this repo does

This repo runs a multi-stage research pipeline:

- ingests **article-style sources** on a frequent schedule
- ingests **quantitative / numeric sources** on a slower schedule
- filters low-value or noisy inputs before spending model calls
- uses **MiniMax** to summarize and verify candidate records
- automatically routes records into:
  - `accepted`
  - `review_queue`
  - `rejected`
- sends `review_queue` items to **Telegram**
- lets a human approve or reject them from Telegram
- finalizes the record in GitHub through a callback-triggered workflow

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

## Full system flow

```mermaid
flowchart TD
    A[GitHub Actions: Article Pipeline<br/>every 2 hours] --> B[ingest_sources.py]
    Q[GitHub Actions: Quant Pipeline<br/>daily] --> R[ingest_quant_data.py]

    B --> C[Raw article snapshots<br/>data/raw]
    R --> S[Raw quant snapshots<br/>data/raw]

    C --> D[filter_raw_records.py]
    S --> T[process_record.py]

    D -->|keep| E[process_record.py]
    D -->|filter out| F[data/filtered_out]

    E --> G[run_summarizer.py<br/>MiniMax summary]
    T --> G
    G --> H[run_verifier.py<br/>MiniMax verification]
    H --> I[route_record.py]

    I -->|accepted| J[data/accepted]
    I -->|rejected| K[data/rejected]
    I -->|review_queue| L[data/review_queue]

    L --> M[send_pending_reviews.py]
    M --> N[send_review_to_telegram.py]
    N --> O[Telegram message<br/>Approve / Reject]

    O --> P[Render callback server<br/>app.py / telegram_callback_server.py]
    P --> X[GitHub workflow_dispatch]
    X --> Y[finalize-review.yml]
    Y --> Z[finalize_review.py]

    Z -->|approve| J
    Z -->|reject| K
```



---

# Workflow Architecture Appendix

This section explains the three main GitHub Actions workflows in the repo and visually shows how records move through the system.

   The repo currently has three main automation workflows:

    - **Article Research Pipeline**
    - **Quant Research Pipeline**
    - **Finalize Review Decision**

    Each one has a different job in the archive lifecycle.

---

## 1. Article Research Pipeline

**Workflow file:** `.github/workflows/process-articles.yml`

This workflow runs on:
- manual trigger
- schedule every 2 hours

At a high level it:

1. ingests article-style sources
2. filters weak/noisy raw records
3. processes each surviving record
4. sends any review-needed records to Telegram
5. commits resulting archive changes back to GitHub

This workflow calls:
- `scripts/run_ingest_and_process.py`
- `scripts/send_pending_reviews.py` :contentReference[oaicite:3]{index=3}

### What happens inside `run_ingest_and_process.py`

That script does:

1. `scripts/ingest_sources.py`
2. `scripts/filter_raw_records.py`
3. for each surviving record:
   - `scripts/process_record.py <record_id>`

Then `process_record.py` does:

1. `scripts/run_summarizer.py <record_id>`
2. `scripts/run_verifier.py <record_id>`
3. `scripts/route_record.py <record_id>` :contentReference[oaicite:4]{index=4}

### Where AI steps in

The AI steps are:

- **Summarization:** `run_summarizer.py`
  - reads raw source text
  - loads the summarize prompt + schema
  - calls MiniMax
  - writes the structured research record

- **Verification:** `run_verifier.py`
  - reads original source text + generated record
  - calls MiniMax again
  - decides whether the record should be accepted, reviewed, or rejected

So the article workflow uses MiniMax twice on each kept record:
- once to generate the research record
- once to verify the research record against the source :contentReference[oaicite:5]{index=5}

### Article pipeline diagram

```mermaid
flowchart TD
    A[GitHub Actions: process-articles.yml] --> B[run_ingest_and_process.py]
    B --> C[ingest_sources.py]
    C --> D[data/raw]
    B --> E[filter_raw_records.py]
    E -->|weak/noisy| F[data/filtered_out]
    E -->|survives| G[process_record.py]

    G --> H[run_summarizer.py]
    H --> H1[MiniMax summarization]
    H1 --> I[research record JSON]

    G --> J[run_verifier.py]
    J --> J1[MiniMax verification]
    J1 --> K[verified verdict]

    G --> L[route_record.py]
    L -->|accepted| M[data/accepted]
    L -->|review_queue| N[data/review_queue]
    L -->|rejected| O[data/rejected]

    A --> P[send_pending_reviews.py]
    P --> Q[send_review_to_telegram.py]
    Q --> R[Telegram review messages]

    A --> S[git add / commit / push data]
```

---

## 2. Keyword Discovery Pipeline

**Script:** `scripts/run_keyword_discovery.py`

This lane discovers candidates through keyword searches against configured queries. It feeds into the shared candidate pipeline.

Flow:
1. Load `config/keyword_queries.json`
2. For each enabled query, execute search (web or preferred domains)
3. Normalize results and build candidates
4. Shared pipeline: dedupe → scoring → filtering → convert → process_record

### Keyword discovery diagram

```mermaid
flowchart TD
    A[keyword_queries.json] --> B[run_keyword_discovery.py]
    B --> C[discovery_providers.py]
    C -->|web search| D[search results]
    C -->|preferred domains| E[domain-filtered results]
    D --> F[normalize_results.py]
    E --> F
    F --> G[build_keyword_candidates.py]
    G --> H[candidate JSON records]
    
    H --> I[dedupe_candidates.py]
    I -->|duplicates| J[data/candidates/deduped_out]
    I -->|unique| K[score_candidates.py]
    
    K --> L{score >= threshold?}
    L -->|no| M[data/candidates/filtered_out]
    L -->|yes| N[convert_candidates_to_raw.py]
    N --> O[data/raw]
    O --> P[process_record.py]
    
    P --> Q[run_summarizer.py<br/>MiniMax]
    Q --> R[run_verifier.py<br/>MiniMax]
    R --> S[route_record.py]
    
    S -->|accepted| T[data/accepted]
    S -->|review_queue| U[data/review_queue]
    S -->|rejected| V[data/rejected]
```

---

## 3. Seed Site Crawling Pipeline

**Script:** `scripts/run_seed_crawl.py`

This lane crawls configured seed sites to discover candidates. It also feeds into the shared candidate pipeline.

Flow:
1. Load `config/seed_sites.json`
2. For each enabled seed, crawl and extract links
3. Build candidates from discovered URLs
4. Shared pipeline: dedupe → scoring → filtering → convert → process_record

### Seed crawl diagram

```mermaid
flowchart TD
    A[seed_sites.json] --> B[run_seed_crawl.py]
    B --> C[crawl_seed_site.py]
    C --> D[extract_internal_links.py]
    D --> E[candidate URL list]
    E --> F[build candidates]
    
    F --> G[save to data/candidates/discovered]
    G --> H[dedupe_candidates.py]
    
    H -->|duplicates| I[data/candidates/deduped_out]
    H -->|unique| J[score_candidates.py]
    
    J --> K{score >= threshold?}
    K -->|no| L[data/candidates/filtered_out]
    K -->|yes| M[convert_candidates_to_raw.py]
    M --> N[data/raw]
    N --> O[process_record.py]
    
    O --> P[run_summarizer.py<br/>MiniMax]
    P --> Q[run_verifier.py<br/>MiniMax]
    Q --> R[route_record.py]
    
    R -->|accepted| S[data/accepted]
    R -->|review_queue| T[data/review_queue]
    R -->|rejected| U[data/rejected]
```

---

## 4. V2.5 Unified Scoring System

**Scripts:** 
- `scripts/extract_candidate_features.py`
- `scripts/score_candidate.py`
- `scripts/score_candidates_batch.py`

V2.5 introduced a unified scoring system with multiple weighted components and priority buckets.

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

### Scoring flow diagram

```mermaid
flowchart TD
    A[candidate record] --> B[extract_candidate_features.py]
    
    B --> C[Freshness extraction<br/>hours since published]
    B --> D[URL quality scoring<br/>path hints]
    B --> E[Title quality scoring<br/>keyword hints]
    B --> F[Domain trust lookup<br/>from scoring_rules.json]
    B --> G[Lane reliability lookup<br/>static lane scores]
    B --> H[Duplication check<br/>hash against index]
    
    C & D & E & F & G & H --> I[score_candidate.py]
    
    I --> J[Weighted sum calculation]
    J --> K[Score breakdown<br/>domain_trust, url_quality, etc.]
    K --> L[candidate_score: 0-100]
    
    L --> M[score_candidates_batch.py]
    M --> N{score >= 75?}
    N -->|yes| O[High Priority<br/>process immediately]
    N -->|no| P{score >= 60?}
    P -->|yes| Q[Medium Priority<br/>defer]
    P -->|no| R{score >= 45?}
    R -->|yes| S[Low Priority<br/>defer]
    R -->|no| T[Skip<br/>filtered_out]
    
    O --> U[process_list]
    Q --> V[defer_list]
    S --> V
    T --> W[skip_list]
    
    U & V --> X[convert_candidates_to_raw.py]
    W --> Y[update_source_memory.py<br/>filtered_out memory]
```

---

## 5. V2.5 Memory System

**Scripts:**
- `scripts/memory_persistence.py`
- `scripts/memory_manager.py`
- `scripts/update_source_memory.py`

Memory tracks domain, path-pattern, and source performance over time to make scoring adaptive.

### Memory types

| Memory Type | Tracks | Key Metrics |
|-------------|--------|-------------|
| Domain Memory | `data/source_memory/domain_memory.json` | trust_score, accepted/rejected counts, yield/noise |
| Path Memory | `data/source_memory/path_memory.json` | `/events/`, `/research/` pattern trust |
| Source Memory | `data/source_memory/source_memory.json` | source_id yield/noise, lane-based trust |

### Memory update flow

```mermaid
flowchart TD
    A[Candidate Outcome] --> B{Outcome Type}
    
    B -->|accepted| C[accepted_human]
    B -->|rejected| D[rejected_human]
    B -->|review_queue| E[review_human]
    B -->|filtered_out| F[filtered_out]
    
    C --> G[route_record.py]
    D --> G
    E --> G
    F --> H[score_candidates_batch.py]
    
    G --> I[update_all_memory_on_outcome]
    H --> I
    
    I --> J[memory_manager.py]
    
    J --> K[Domain Memory]
    J --> L[Path Memory]
    J --> L
    J --> M[Source Memory]
    
    K --> N[data/source_memory<br/>domain_memory.json]
    L --> O[data/source_memory<br/>path_memory.json]
    M --> P[data/source_memory<br/>source_memory.json]
    
    K --> Q[Trust Recalculation]
    L --> Q
    M --> Q
    
    Q --> R[blend baseline + learned<br/>cold-start aware]
    R --> S[trust_score updated]
    
    S --> T[compute yield/noise]
    T --> U[yield_score, noise_score]
    
    U --> V[logs/source_memory<br/>memory_updates.jsonl]
```

### Memory-influenced scoring

```mermaid
flowchart TD
    A[candidate] --> B[extract_candidate_features.py]
    
    B --> C[extract_domain_trust_score_with_memory]
    C --> D{Domain in memory?}
    D -->|yes| E[blend: 10% baseline<br/>90% memory]
    D -->|no| F[use baseline trust]
    
    B --> G[extract_path_trust_score]
    G --> H{path /events/?}
    H -->|yes, trust < 20| I[apply 10% penalty]
    H -->|no| J[no penalty]
    
    B --> K[extract_source_quality_scores]
    K --> L{yield > 0.7<br/>noise < 0.3?}
    L -->|yes| M[apply 10% bonus]
    L -->|no| N[no bonus]
    
    E & F & I & J & M & N --> O[Final score<br/>capped at 100]
```

### Cold-start blending

New domains/sources start with baseline trust and gradually shift to learned trust:

| Samples | Baseline Weight | Learned Weight |
|---------|-----------------|----------------|
| 0-9 | 90-100% | 0-10% |
| 10-24 | 90% → 10% | 10% → 90% |
| 25+ | 10% | 90% |

Human decisions are weighted 2x more than automatic decisions.

---

## 6. Three-Lane Architecture

The system uses three discovery lanes that converge into a shared processing pipeline:

```mermaid
flowchart TD
    A[Lane 1: Trusted Sources<br/>federalreserve.gov, etc.] --> X[Shared Candidate Pipeline]
    B[Lane 2: Keyword Discovery<br/>config/keyword_queries.json] --> X
    C[Lane 3: Seed Site Crawl<br/>config/seed_sites.json] --> X
    
    X --> D[Candidate Deduplication]
    D --> E[Candidate Scoring<br/>V2.5 Unified]
    E --> F[Memory-Based Trust<br/>V2.5 Part 2]
    F --> G[Priority Buckets<br/>high/medium/low/skip]
    G --> H[Convert to Raw]
    H --> I[process_record.py]
    I --> J[MiniMax Summarize]
    J --> K[MiniMax Verify]
    K --> L[Route: accept/reject/review]
    
    L -->|accepted| M[data/accepted]
    L -->|rejected| N[data/rejected]
    L -->|review| O[data/review_queue]
    
    O --> P[Telegram Human Review]
    P -->|approve| M
    P -->|reject| N
```
