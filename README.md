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