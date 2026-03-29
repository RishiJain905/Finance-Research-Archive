# V2.5 Part 4 — Article and Quant Linking

## Objective

Link narrative records and quantitative records so the archive can connect what happened with the measurable market state around it.

This part turns the system from a set of separate article and quant records into a joined finance knowledge base.

---

## Why this part matters

Without linking:

- article records say what happened
- quant records show values
- but the archive does not explicitly connect them

With linking:

- policy articles can reference relevant rates snapshots
- market-structure articles can reference liquidity or yield changes
- the archive becomes much more useful for RAG and synthesis

---

## Scope

This part includes:

- linking article records to nearby quant records
- temporal proximity matching
- topic-based matching
- linked-record metadata
- enrichment of article records with quant context

---

## End state

At the end of this part, an article about policy repricing or Treasury funding should be able to point to:

- nearby SOFR snapshot
- nearby Fed funds snapshot
- nearby 2Y / 10Y yield snapshot
- later Treasury auction / repo data

And quant records should be able to reference nearby event/narrative records.

---

## Linking model

Suggested linked context block:

```json
{
  "linked_quant_context": [
    {
      "record_id": "2y_yield_2026_04_01",
      "relationship": "nearest_relevant_quant_snapshot",
      "reason": "same-day rates context",
      "topic_overlap": ["rates", "macro catalysts"]
    }
  ],
  "linked_article_context": [
    {
      "record_id": "federal_reserve_press_release_xxx",
      "relationship": "narrative_context_for_quant_snapshot",
      "reason": "same-day policy event"
    }
  ]
}
```

---

## Files to add

Suggested files:

```text
scripts/
  link_article_and_quant_records.py
  find_related_quant_records.py
  find_related_article_records.py
schemas/
  linked_context.json
```

---

## Matching dimensions

### 1. Time proximity

Strongest first-pass rule.

Examples:

- same day
- previous market day
- within N hours of publication

### 2. Topic overlap

Use topic tags such as:

- macro catalysts
- market structure
- rates
- inflation
- treasury
- liquidity
- funding

### 3. Thematic overlap

Use phrases like:

- rate repricing
- funding conditions
- Treasury issuance
- inflation expectations
- curve shift

### 4. Source/event type compatibility

Examples:

- policy statement → rates snapshots
- Treasury refunding article → auction / funding snapshots
- liquidity article → SOFR / repo / reserves-related series

---

## Enrichment rules

When linking article to quant:

Add fields such as:

- nearest relevant quant snapshot ids
- latest related rates values
- basic quant movement summary

When linking quant to article:

Add fields such as:

- nearby event ids
- narrative summary ids
- related policy/news context

---

## Output usage

These links should be usable for:

- better retrieval later
- daily synthesis
- analyst-style event explanations
- more coherent RAG context assembly

---

## Acceptance criteria

This part is complete when:

- article records can reference quant snapshots
- quant records can reference nearby narrative records
- links are stored in structured metadata
- retrieved context becomes more useful and coherent

---

## Recommended implementation order

1. define linked-context schema
2. implement nearest-time matching
3. add topic overlap filtering
4. enrich accepted records with linked ids
5. later add stronger semantic linking

---

## Output of this part

After this part, the archive no longer stores narrative and numbers as separate worlds.

It becomes a joined financial knowledge graph in lightweight form.
