# V2.7 Part 4 — Article and Quant Enrichment

## Objective

Link article records and quant records so the archive becomes context-rich instead of split into separate text and number lanes.

This should let the system automatically attach quant context to narrative records and attach narrative context to quant movements when relevant.

## Why this is needed

The repo already has:
- article pipelines
- quant pipelines
- accepted records
- event clustering plans

But if article and quant live separately, the archive misses one of the biggest advantages of a finance intelligence system:

**narrative + numbers in the same unit of meaning**

## Success criteria

By the end of this part, the repo should:
1. link accepted article records to relevant quant records
2. link accepted quant records to related event clusters
3. store article-to-quant relationships persistently
4. enrich final archive records with cross-lane context
5. support later synthesis using combined narrative and numeric evidence

## New files and folders

```text
scripts/link_article_quant.py
schemas/article_quant_link.json
config/quant_linking_rules.json
data/article_quant_links/
```

## Link schema

```json
{
  "link_id": "string",
  "article_record_id": "string",
  "quant_record_id": "string",
  "event_id": "optional string",
  "relationship": "supports | context | confirms | weak_context",
  "score": 0,
  "matched_dimensions": [
    "topic",
    "time_window",
    "keyword_overlap",
    "event_alignment"
  ],
  "created_at": "ISO-8601"
}
```

## Linkable examples

### Articles to quant
- Fed policy speech -> DFF, SOFR, 2Y, 10Y
- Treasury refunding note -> yields, auctions, funding metrics
- inflation release -> front-end rates
- labor market surprise -> policy path and rates sensitivity

### Quant to articles
- sharp 2Y move -> relevant policy speech/article cluster
- funding stress snapshot -> liquidity / repo narrative cluster

## Linking dimensions

Use deterministic linking first:
- topic compatibility
- time window
- event alignment
- keyword compatibility

## Quant family mapping

Create `config/quant_linking_rules.json`.

```json
{
  "topic_to_series": {
    "macro catalysts": ["fed_funds", "sofr", "2y_yield", "10y_yield"],
    "market structure": ["sofr", "repo_operations", "treasury_auctions", "2y_yield", "10y_yield"]
  },
  "keyword_to_series": {
    "inflation": ["2y_yield", "10y_yield", "fed_funds"],
    "policy": ["fed_funds", "sofr", "2y_yield"],
    "liquidity": ["sofr", "repo_operations"],
    "auction": ["treasury_auctions", "upcoming_treasury_auctions"],
    "funding": ["sofr", "repo_operations"]
  }
}
```

## Enrichment strategy

When an accepted article record is finalized:
1. identify relevant quant families
2. search recent accepted quant records
3. choose top matches
4. create link files
5. optionally write a small enrichment block into the accepted record

Example:
```json
"quant_context": {
  "linked_quant_records": [
    "fed_funds_2026_04_05",
    "2y_yield_2026_04_05"
  ],
  "summary": "Front-end rates remained elevated around the policy communication."
}
```

## New processing stage

```text
accepted record
-> link_article_quant.py
-> article_quant_links/
-> optional accepted record enrichment
```

Only for accepted records at first.

## Relationship scoring

Suggested bands:
- 80+ = strong support
- 60–79 = contextual support
- 40–59 = weak context
- below 40 = ignore

## Testing

### Unit tests
- topic/series mapping
- time-window logic
- score calculation
- link file creation

### Integration tests
- accepted article records create quant links
- accepted quant records create article links
- event-linked records get stronger matching

## Deliverables
- article-quant link schema
- quant linking config
- linking script
- accepted record enrichment logic
- tests

## Definition of done

Complete when:
1. accepted article and quant records can be linked reliably
2. relationship files are persisted
3. accepted records can include cross-lane context
4. event clusters can carry both narrative and numeric evidence
5. the archive becomes more useful for explanatory finance retrieval
