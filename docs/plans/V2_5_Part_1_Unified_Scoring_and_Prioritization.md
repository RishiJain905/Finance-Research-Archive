# V2.5 Part 1 — Unified Scoring and Prioritization Layer

## Objective

Build a single scoring system that evaluates every candidate from all three V2 lanes before MiniMax processing.

This is the most important V2.5 upgrade because once the system can monitor trusted sources, discover via keywords, and crawl seed sites, the bottleneck becomes:

- which candidates deserve attention first
- which candidates should be skipped entirely
- how to reduce noise before spending model calls

The output of this part is a **unified candidate scoring layer** that sits between discovery and processing.

---

## Why this part comes first

Without a shared prioritization layer:

- noisy lanes can overwhelm strong lanes
- model spend rises too fast
- accepted quality becomes inconsistent
- downstream memory and clustering get polluted by weak inputs

With a scoring layer, the system becomes:

- more cost-efficient
- more explainable
- easier to tune
- safer to scale

---

## Scope

This part includes:

- unified candidate scoring schema
- feature extraction before processing
- score composition rules
- thresholding for skip / process / prioritize
- score logging for debugging and tuning
- integration into the shared candidate queue

This part does **not** yet include:

- source memory
- adaptive keyword learning
- clustering
- digest generation

Those come later.

---

## End state

At the end of this part, every candidate record from every lane should have:

- a `lane`
- a `source_type`
- a `candidate_score`
- a `score_breakdown`
- a `priority_bucket`
- a `process_decision`

And only candidates above threshold should move into MiniMax summarization/verification.

---

## Architecture

### Before V2.5 Part 1

```text
lane output
-> dedupe
-> filter
-> MiniMax summarizer
-> MiniMax verifier
```

### After V2.5 Part 1

```text
lane output
-> dedupe
-> candidate feature extraction
-> unified scoring
-> thresholding / priority routing
-> filter
-> MiniMax summarizer
-> MiniMax verifier
```

---

## Candidate feature model

Every candidate should expose normalized fields before scoring.

Suggested candidate feature schema:

```json
{
  "candidate_id": "string",
  "lane": "trusted_sources | keyword_discovery | seed_crawling",
  "discovery_timestamp": "ISO-8601",
  "source_domain": "string",
  "source_url": "string",
  "title": "string",
  "anchor_text": "string",
  "topic_hints": ["string"],
  "source_type": "press_release | speech | blog | research | rulemaking | quant_snapshot | unknown",
  "freshness_hours": 0,
  "url_quality_score": 0,
  "title_quality_score": 0,
  "keyword_match_score": 0,
  "domain_trust_score": 0,
  "duplication_risk_score": 0,
  "lane_reliability_score": 0,
  "candidate_score": 0,
  "priority_bucket": "high | medium | low | skip",
  "process_decision": "process | defer | skip"
}
```

---

## Scoring dimensions

### 1. Domain trust

Measures how trustworthy the domain is in general.

Examples:

- official central bank domains → high
- regulator domains → high
- large institutional research domains → medium-high
- broad think tank / blog domains → medium
- noisy commercial domains → low

Initial rule:

- assign static baseline trust by domain family
- later V2.5 Part 2 will make this dynamic

---

### 2. URL quality

Measures whether the URL structure looks like a meaningful record.

Positive signs:

- `/press/`
- `/speech/`
- `/statement/`
- `/report/`
- `/article/`
- date-like paths
- specific slug depth

Negative signs:

- `/about/`
- `/careers/`
- `/events/`
- `/category/`
- `/tag/`
- `/people/`
- root hub pages

---

### 3. Title quality

Measures whether the title looks like actual finance content.

Positive signs:

- contains inflation / policy / liquidity / repo / treasury / market / funding / yield / rates
- looks specific rather than generic
- contains event-like terms

Negative signs:

- generic navigation titles
- careers / event / registration language
- vague titles with no finance signal

---

### 4. Keyword / topic match

Measures relevance to your finance archive themes.

Core themes:

- market structure
- macro catalysts
- liquidity
n- repo / funding
- Treasury issuance
- policy expectations
- inflation / labor / growth
- volatility / rates / curve

This should use:

- target keyword bundles
- topic bundles
- lane-specific context

---

### 5. Freshness

Measures how recent the candidate is.

Guideline:

- very fresh and relevant candidates should be prioritized
- old pages should not dominate unless they are from trusted research lanes

Freshness can be a boost, not a hard requirement.

---

### 6. Duplication risk

Measures how likely this is to be a duplicate or near-duplicate.

Examples:

- same normalized title as known candidate
- same URL pattern and path family
- same event appearing from multiple mirrors

Higher duplication risk should reduce score.

---

### 7. Lane reliability

Measures how much trust to place in the lane that found the item.

Suggested initial defaults:

- trusted source monitoring → highest
- keyword discovery → medium
- seed-site crawling → medium-low until proven

This is not about domain trust. It is about the discovery method itself.

---

## Score composition

Use an additive weighted score.

Initial example:

```text
candidate_score =
    0.25 * domain_trust_score
  + 0.20 * url_quality_score
  + 0.20 * title_quality_score
  + 0.20 * keyword_match_score
  + 0.10 * freshness_score
  + 0.10 * lane_reliability_score
  - 0.15 * duplication_risk_score
```

You can normalize all component scores to 0–100 first.

---

## Priority buckets

Define clear decision buckets.

Example:

- `>= 75` → `high`, process immediately
- `60–74` → `medium`, process normally
- `45–59` → `low`, process only if budget available
- `< 45` → `skip`

This gives you a cost-control lever.

---

## Files to add

Suggested files:

```text
scripts/
  extract_candidate_features.py
  score_candidate.py
  score_candidates_batch.py
schemas/
  candidate_score.json
config/
  scoring_rules.json
logs/
  candidate_scoring/
```

---

## File responsibilities

### `config/scoring_rules.json`

Contains:

- domain trust baselines
- lane reliability baselines
- title / URL hint weights
- threshold cutoffs

### `schemas/candidate_score.json`

Defines the scored candidate object.

### `scripts/extract_candidate_features.py`

Takes a discovered candidate and extracts:

- normalized domain
- normalized title
- URL shape signals
- topic hits
- freshness estimate

### `scripts/score_candidate.py`

Scores one candidate from extracted features.

### `scripts/score_candidates_batch.py`

Scores a batch of candidates and writes:

- scored queue
- priority buckets
- skip decisions

---

## Integration points

### Trusted source lane

After discovery, before fetch-heavy processing:

```text
ingest_sources.py
-> candidate feature extraction
-> scoring
-> only keep process-worthy candidates
```

### Keyword discovery lane

After search result collection:

```text
keyword discovery
-> candidate scoring
-> keep top candidates
-> fetch
-> shared pipeline
```

### Seed crawler lane

After internal link discovery:

```text
crawler discovery
-> candidate scoring
-> keep top candidates
-> fetch
-> shared pipeline
```

---

## Logging and observability

This part should produce logs that make tuning easy.

For each candidate, log:

- domain
- title
- total score
- score breakdown
- bucket
- process decision

This is crucial because V2.5 tuning should be evidence-based.

Suggested output directory:

```text
logs/candidate_scoring/
```

Suggested file format:

- JSONL for machine analysis
- one record per candidate

---

## Acceptance criteria

This part is complete when:

- all three lanes can emit candidates into the same scoring layer
- every candidate gets a score and score breakdown
- thresholds determine whether to process or skip
- score logs exist for later tuning
- MiniMax spend drops on weak candidates
- accepted-quality consistency improves

---

## Risks

### Risk 1

Over-weighting trust may suppress new discoveries.

Mitigation:

- keep a medium bucket for exploratory candidates
- do not make domain trust dominant

### Risk 2

Over-aggressive skipping may block useful early signals.

Mitigation:

- route medium and borderline candidates through a limited daily budget

### Risk 3

Too many score features too early may make tuning confusing.

Mitigation:

- start with a small set of high-signal features
- add new features only when logs prove they help

---

## Recommended implementation order

1. define schema
2. define scoring config
3. implement feature extraction
4. implement batch scoring
5. integrate into all lanes
6. add logs
7. tune thresholds from real runs

---

## Output of this part

After this part, the archive should have a disciplined gate that decides:

- what is worth model attention
- what can be deferred
- what should be skipped entirely

That becomes the backbone for all later V2.5 improvements.
