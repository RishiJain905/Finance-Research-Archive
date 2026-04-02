# V2.7 Part 1 — Triage and Prioritization Engine

## Objective

Build a dedicated triage layer that scores and prioritizes every candidate **before** expensive AI processing. This layer should sit between all ingestion lanes and the existing summarize/verify/archive backend.

This is the highest-leverage V2.7 upgrade because the repo now has multiple discovery paths and a much larger source universe. At this stage, the system's main problem is no longer “how do I find more content?” It is “how do I choose the best content consistently, cheaply, and safely?”

## Why this is needed

The current repo already has:
- trusted-source article pipeline
- quant pipeline
- review and finalization flows
- filtering and dedupe
- active source expansion

Without a triage layer, the system will:
- waste MiniMax calls on low-value records
- spend too much time on redundant candidates
- flood review with medium-quality items
- make source expansion harder to manage

A triage engine fixes this by assigning an explicit priority score to every candidate before it reaches the AI stage.

## Success criteria

By the end of this part, the repo should be able to:
1. assign every candidate a numeric priority score
2. assign every candidate a structured scoring explanation
3. rank candidates across all lanes using one common standard
4. process top candidates first
5. optionally defer or skip low-priority candidates
6. preserve triage metadata for later analysis

## Architecture

```text
lane output
-> candidate queue
-> triage_engine.py
-> ranked candidates
-> processing budget gate
-> process_record.py
```

## New concepts

### Candidate
Any discovered item that may become an archive record.

### Triage score
A numeric score used to decide processing priority.

### Budget gate
A configurable limit on how many candidates can be fully processed per run.

### Deferred candidate
A candidate relevant enough to keep, but not important enough to process immediately.

## New folders and files

```text
scripts/triage_engine.py
schemas/candidate.json
schemas/triage_result.json
config/triage_weights.json
data/candidates/
data/triage/
data/deferred/
```

## Candidate schema

Create `schemas/candidate.json`.

```json
{
  "candidate_id": "string",
  "lane": "trusted_sources | keyword_discovery | seed_crawling | quant",
  "source_name": "string",
  "source_domain": "string",
  "source_url": "string",
  "discovered_at": "ISO-8601",
  "topic": "string",
  "title": "string",
  "anchor_text": "string",
  "raw_path": "string",
  "source_type": "article | speech | press_release | quant_snapshot | policy_statement | blog | research_note",
  "discovery_context": {
    "query": "optional string",
    "seed_domain": "optional string",
    "parent_url": "optional string"
  }
}
```

## Triage result schema

Create `schemas/triage_result.json`.

```json
{
  "candidate_id": "string",
  "priority_score": 0,
  "priority_band": "critical | high | medium | low | discard",
  "scoring": {
    "source_trust": 0,
    "freshness": 0,
    "topic_relevance": 0,
    "title_quality": 0,
    "url_quality": 0,
    "novelty": 0,
    "quant_value": 0,
    "duplicate_risk": 0
  },
  "reasons": ["string"],
  "action": "process_now | defer | discard"
}
```

## Scoring categories

Score candidates on:
- source trust
- freshness
- topic relevance
- title quality
- URL quality
- novelty
- quant value
- duplicate risk

## Weight config

Create `config/triage_weights.json`.

```json
{
  "weights": {
    "source_trust": 0.20,
    "freshness": 0.15,
    "topic_relevance": 0.20,
    "title_quality": 0.10,
    "url_quality": 0.10,
    "novelty": 0.10,
    "quant_value": 0.10,
    "duplicate_risk": -0.15
  },
  "bands": {
    "critical": 85,
    "high": 70,
    "medium": 50,
    "low": 30
  }
}
```

## Implementation plan

### Step 1
Create `scripts/triage_engine.py`.
Responsibilities:
- read candidates from `data/candidates/`
- compute triage scores
- save triage outputs to `data/triage/`
- move/discard/defer according to score and budget

### Step 2
Add candidate production to each lane:
- trusted-source lane
- keyword-discovery lane
- seed-crawling lane
- quant lane

### Step 3
Introduce a budget gate.

Example config:
```json
{
  "article_process_limit": 25,
  "quant_process_limit": 10,
  "defer_medium": true
}
```

### Step 4
Update `run_ingest_and_process.py` and `run_quant_pipeline.py` so they:
- ingest
- filter
- create candidates
- triage candidates
- process only selected candidates

### Step 5
Persist triage metadata in accepted/review/rejected records.

Example:
```json
"triage": {
  "priority_score": 82,
  "priority_band": "high",
  "lane": "trusted_sources",
  "reasons": [
    "high trust domain",
    "strong inflation topic match",
    "fresh official release"
  ]
}
```

## Metrics

Track:
- total candidates per lane
- processed candidates per lane
- deferred candidates per lane
- discarded candidates per lane
- average priority score per lane
- accepted ratio by priority band

## Testing

### Unit tests
- candidate score calculation
- priority band thresholds
- discard/defer/process decisions
- duplicate risk penalty

### Integration tests
- article pipeline emits candidates
- quant pipeline emits candidates
- triage engine selects correct top candidates
- low-scoring candidates go to deferred/discard

## Deliverables
- candidate schema
- triage result schema
- triage weights config
- triage engine
- updated article and quant pipeline entrypoints
- persisted triage metadata
- tests

## Definition of done

Complete when:
1. all lanes emit candidate files in one shared format
2. triage scores are computed consistently
3. only top candidates are processed by default
4. lower-priority candidates are deferred or discarded
5. triage metadata is preserved
6. priority ranking improves processing quality
