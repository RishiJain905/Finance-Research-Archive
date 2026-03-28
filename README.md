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