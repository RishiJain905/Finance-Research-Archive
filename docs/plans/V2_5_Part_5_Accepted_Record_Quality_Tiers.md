# V2.5 Part 5 — Accepted Record Quality Tiers

## Objective

Introduce quality tiers inside `accepted` records so the archive can distinguish between:

- highly reusable records
- useful but average records
- acceptable but lower-value records

This helps retrieval, synthesis, and long-term archive quality management.

---

## Why this part matters

Without tiers, every accepted record is treated equally.

But in practice:

- some accepted records are core RAG assets
- some are just decent context
- some are acceptable but not especially important

V2.5 should separate those.

---

## Scope

This part includes:

- accepted record tier assignment
- tier criteria
- tier metadata storage
- later use in retrieval/ranking

---

## End state

Accepted records should receive a quality tier like:

- `tier_1` — highly reusable, strong signal, likely valuable for future RAG
- `tier_2` — useful context, but not core
- `tier_3` — acceptable, but lower-value or narrower utility

---

## Example tier block

```json
{
  "quality_tier": {
    "tier": "tier_1",
    "reasoning": [
      "high verification confidence",
      "clear why_it_matters",
      "strong topic relevance",
      "contains useful structured numbers"
    ]
  }
}
```

---

## Tier criteria

### Tier 1

Characteristics:

- strong verification confidence
- high relevance to core themes
- clean narrative
- strong reusability for retrieval
- useful numbers or durable concepts
- likely to remain valuable over time

### Tier 2

Characteristics:

- good but narrower
- useful supporting context
- somewhat reusable
- may be topic-specific or weaker in evidence density

### Tier 3

Characteristics:

- acceptable
- low reusability
- may be transient, narrow, or structurally weaker

---

## Files to add

Suggested files:

```text
scripts/
  assign_quality_tier.py
schemas/
  quality_tier.json
config/
  quality_tier_rules.json
```

---

## Tier inputs

Suggested features for tier assignment:

- verification confidence
- human approval status
- number of issues found
- quality of `why_it_matters`
- presence of structured quant context
- source trust
- topic centrality
- event significance

---

## Integration point

Tier assignment should happen after a record is accepted.

### Flow

```text
accepted record
-> assign_quality_tier.py
-> update accepted record with tier metadata
```

Human-approved records may receive a boost.

---

## Acceptance criteria

This part is complete when:

- all accepted records receive a tier
- the tier is stored in metadata
- tier assignment is explainable and rule-driven
- future retrieval can prefer tier_1 over tier_2 over tier_3

---

## Recommended implementation order

1. define tier schema
2. define rule config
3. implement assignment script
4. backfill tiers for existing accepted records
5. integrate into acceptance flow

---

## Output of this part

After this part, the archive can distinguish between “acceptable” and “high-value,” which is crucial for later retrieval quality.
