# V2.7 Part 5 — Source Performance Analytics and Adaptive Control

## Objective

Track the real performance of every source and use that data to adapt the source universe over time.

This should answer:
- Which sources actually produce accepted records?
- Which sources mostly generate junk?
- Which sources create too much human review burden?
- Which sources deserve more links?
- Which sources should be demoted or disabled?

## Why this is needed

The repo already has many sources and may add many more.

At that scale, manual source tuning alone becomes fragile.

V2.7 needs a source analytics layer so the system can:
- learn what works
- learn what wastes time
- become easier to maintain
- support future auto-tuning

## Success criteria

By the end of this part, the repo should:
1. measure source-level outcomes
2. calculate accepted/review/rejected/filter ratios
3. rank sources by usefulness
4. surface weak sources clearly
5. recommend source actions such as keep, tighten, lower_max_links, disable, investigate

## New files and folders

```text
scripts/source_analytics.py
scripts/source_recommendations.py
schemas/source_stats.json
schemas/source_recommendation.json
data/source_analytics/
data/source_recommendations/
```

## Source stats schema

```json
{
  "source_name": "string",
  "source_domain": "string",
  "records_seen": 0,
  "accepted_count": 0,
  "review_count": 0,
  "rejected_count": 0,
  "filtered_out_count": 0,
  "accepted_ratio": 0.0,
  "review_ratio": 0.0,
  "rejected_ratio": 0.0,
  "filtered_ratio": 0.0,
  "avg_priority_score": 0.0,
  "avg_verification_confidence": 0.0,
  "last_seen_at": "ISO-8601"
}
```

## Recommendation schema

```json
{
  "source_name": "string",
  "recommended_action": "keep | tighten | lower_max_links | disable | investigate",
  "reasons": ["string"],
  "metrics_snapshot": {
    "accepted_ratio": 0.0,
    "filtered_ratio": 0.0,
    "review_ratio": 0.0
  },
  "created_at": "ISO-8601"
}
```

## Analytics dimensions

Track:
- records seen
- raw ingested count
- candidates produced
- accepted
- review_queue
- rejected
- filtered_out
- average verification confidence
- average triage score
- average human review burden
- last seen

## Recommendation logic

### Keep
- accepted ratio strong
- filtered ratio reasonable
- review burden reasonable

### Tighten
- too many review results
- filtered ratio high but not catastrophic
- some good records still exist

### Lower max links
- source is useful but too high-volume and noisy

### Disable
- accepted ratio very low
- filtered ratio very high
- repeated junk across many runs
- source repeatedly 403s or fails

### Investigate
- volatile performance
- inconsistent behavior
- mixed but unclear results

## Data sources

Build analytics by reading:
- `data/accepted/`
- `data/review_queue/`
- `data/rejected/`
- `data/filtered_out/`
- triage metadata
- verification metadata
- source metadata embedded in records

## New processing stage

Daily analytics pass:

```text
all records
-> source_analytics.py
-> source stats files
-> source_recommendations.py
-> recommendation files
```

## Adaptive control

Phase 1 should only generate recommendations.
Do not auto-edit configs yet.

## Example recommendation rules

Disable if:
- 20+ records seen
- accepted ratio < 5%
- filtered ratio > 70%

Lower max links if:
- accepted ratio moderate
- filtered ratio high
- source volume very high

Tighten if:
- accepted ratio exists
- many records go to review/filter
- source still has potential

## Testing

### Unit tests
- ratio calculation
- recommendation thresholds
- missing-data handling

### Integration tests
- accepted/review/rejected/filtered records update stats correctly
- recommendations are produced from real repo data

## Deliverables
- source stats schema
- recommendation schema
- analytics script
- recommendation script
- daily workflow or entrypoint
- tests

## Definition of done

Complete when:
1. every source has measurable performance stats
2. the system can rank sources by usefulness
3. weak sources are identified automatically
4. action recommendations are generated consistently
5. future source tuning becomes evidence-based
