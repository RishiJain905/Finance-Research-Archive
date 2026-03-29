# V2.5 Part 2 — Source and Domain Memory

## Objective

Build a persistent memory layer that tracks how domains, paths, and sources perform over time.

This part makes the system adaptive. Instead of treating every source the same on every run, the archive learns:

- which domains are consistently good
- which path patterns are consistently bad
- which sources tend to create accepted records
- which sources mostly create noise

---

## Why this part matters

Once V2 has three lanes and V2.5 Part 1 adds scoring, the next improvement is memory.

Without memory:

- the system repeats mistakes
- noisy sources keep getting rescored from scratch
- good sources do not gain deserved trust
- bad URL patterns keep slipping into the queue

With memory:

- domain trust evolves from evidence
- path trust evolves from evidence
- candidate scoring gets better over time
- the whole system compounds

---

## Scope

This part includes:

- domain-level memory
- source-path memory
- lane/source performance tracking
- accepted/review/rejected ratios
- dynamic trust score adjustment
- memory lookups in candidate scoring

This part does **not** yet include:

- adaptive keyword learning
- clustering
- digest generation

---

## End state

At the end of this part, the system should know things like:

- `federalreserve.gov` has a high trust score
- `brookings.edu/topic/economy/` produces mixed results
- `.../events/` style paths are almost always junk
- `libertystreeteconomics.newyorkfed.org` tends to produce high-value records
- one keyword-discovery source repeatedly leads to review/reject outcomes

That memory should feed back into the scoring layer.

---

## Memory model

There should be at least three memory entities.

### 1. Domain memory

Tracks performance at the domain level.

Example fields:

```json
{
  "domain": "newyorkfed.org",
  "trust_score": 88,
  "total_candidates": 120,
  "accepted_count": 58,
  "review_count": 20,
  "rejected_count": 18,
  "filtered_out_count": 24,
  "last_seen": "ISO-8601"
}
```

### 2. Path-pattern memory

Tracks performance for path families.

Example:

```json
{
  "domain": "brookings.edu",
  "path_pattern": "/events/",
  "trust_score": 12,
  "total_candidates": 30,
  "accepted_count": 0,
  "review_count": 1,
  "rejected_count": 7,
  "filtered_out_count": 22
}
```

### 3. Source-memory record

Tracks explicit configured sources or discovery seed sources.

Example:

```json
{
  "source_id": "brookings_economy",
  "source_type": "trusted_source_monitor",
  "trust_score": 42,
  "yield_score": 0.15,
  "noise_score": 0.72,
  "last_updated": "ISO-8601"
}
```

---

## Files to add

Suggested files:

```text
scripts/
  update_source_memory.py
  compute_domain_metrics.py
  compute_path_metrics.py
  apply_memory_adjustments.py
schemas/
  domain_memory.json
  path_memory.json
  source_memory.json
data/
  source_memory/
    domain_memory.json
    path_memory.json
    source_memory.json
```

---

## Memory update triggers

Memory should be updated after routing is finalized.

That means after a candidate becomes:

- accepted
- rejected
- review_queue
- filtered_out

The memory layer should update counters and trust signals.

### Example flow

```text
candidate processed
-> routed to accepted/rejected/review/filtered_out
-> update_source_memory.py
-> adjust domain trust and path trust
-> persist memory files
```

---

## Trust score logic

### Domain trust

Start with a baseline from static config.
Then adjust over time.

Example idea:

- accepted records increase trust
- rejected and filtered-out records reduce trust
- review_queue has neutral or mild effect

Example formula:

```text
trust_score =
  base_trust
  + accepted_weight * accepted_rate
  - filtered_weight * filtered_rate
  - rejected_weight * rejected_rate
```

### Path trust

More aggressive than domain trust.

If `/events/` repeatedly fails, it should become near-zero trust quickly.

---

## Yield and noise metrics

Introduce two helpful metrics.

### Yield score

How often does a source produce useful output?

```text
yield_score = accepted_count / total_candidates
```

### Noise score

How often does a source produce junk?

```text
noise_score = (filtered_out_count + rejected_count) / total_candidates
```

These metrics are easier to reason about than trust alone.

---

## Integration with candidate scoring

V2.5 Part 1 scoring should now use memory.

### Before memory

- domain trust was mostly static
- lane trust was mostly static

### After memory

Scoring should incorporate:

- `domain_memory.trust_score`
- `path_memory.trust_score`
- `source_memory.yield_score`
- `source_memory.noise_score`

This makes scores evidence-based.

---

## Cold start behavior

New sources and domains will not have history.

So you need a cold-start rule:

- use static baseline trust
- gradually shift toward evidence after a minimum sample size

Example:

- before 10 candidates: mostly static trust
- after 10 candidates: blended static + learned trust
- after 25 candidates: mostly learned trust

---

## Human review feedback integration

Human review decisions are valuable memory signals.

If a source often sends candidates that humans approve, that source should gain trust.

If a source often sends candidates that humans reject, that source should lose trust.

So memory should distinguish:

- rejected automatically
- rejected by human
- accepted automatically
- approved by human

Human decisions should be weighted more strongly because they are high-quality supervision.

---

## Logging and observability

For each memory update, log:

- candidate id
- source id
- domain
- path pattern
- outcome
- before/after trust score

This should be written to:

```text
logs/source_memory/
```

---

## Acceptance criteria

This part is complete when:

- domain memory persists across runs
- path-pattern memory persists across runs
- source performance metrics persist across runs
- scoring uses memory-based trust adjustments
- noisy domains and noisy path types degrade over time
- high-yield sources gain priority over time

---

## Risks

### Risk 1

A strong domain may contain weak subsections.

Mitigation:

- keep path memory separate from domain memory

### Risk 2

Too much negative feedback too early may unfairly bury a source.

Mitigation:

- use sample-size-aware smoothing
- avoid hard penalties on tiny sample counts

### Risk 3

Human approvals on weak content can bias the memory.

Mitigation:

- keep human decision history visible and auditable

---

## Recommended implementation order

1. define memory schemas
2. create persistent memory files
3. implement domain/path/source metric updates
4. hook updates into routing outcomes
5. feed trust/yield/noise into scoring
6. add logs and dashboards later

---

## Output of this part

After this part, the archive stops being a system that merely runs rules.

It becomes a system that remembers which inputs deserve more trust and which inputs deserve less, based on observed outcomes.
