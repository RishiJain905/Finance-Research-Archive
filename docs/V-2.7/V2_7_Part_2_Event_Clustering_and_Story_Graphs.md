# V2.7 Part 2 — Event Clustering and Story Graphs

## Objective

Upgrade the archive from isolated records into a **story-aware system** that groups related records into common events, narratives, and evolving finance themes.

This should cluster:
- Fed speeches
- Treasury updates
- NY Fed market notes
- quant moves in rates/funding

into one coherent event or narrative when clearly related.

## Why this is needed

The repo is already strong at:
- ingesting
- summarizing
- verifying
- routing
- reviewing

But finance research is often about **events** and **narratives**, not isolated pages.

Without clustering:
- the archive stays fragmented
- retrieval later gets repetitive
- the same macro event appears many times
- daily summaries become a list instead of a story

## Success criteria

By the end of this part, the repo should:
1. identify related records across lanes
2. group them into event clusters
3. store event-level metadata
4. support evolving clusters as new records arrive
5. use event context in summaries and later digests

## New concepts

### Event cluster
A group of related records describing the same event or narrative.

### Narrative cluster
A broader, slower-moving theme that may contain multiple event clusters.

### Story graph
A graph showing how records connect to events, themes, quant moves, and sources.

## New files and folders

```text
data/events/
data/story_graph/
scripts/cluster_records.py
scripts/update_story_graph.py
schemas/event_cluster.json
schemas/story_edge.json
config/clustering_rules.json
```

## Event cluster schema

```json
{
  "event_id": "string",
  "title": "string",
  "topic": "string",
  "event_type": "string",
  "summary": "string",
  "status": "open | stable | archived",
  "created_at": "ISO-8601",
  "updated_at": "ISO-8601",
  "record_ids": ["string"],
  "source_domains": ["string"],
  "keywords": ["string"],
  "quant_links": ["string"],
  "confidence": 0
}
```

## Story edge schema

```json
{
  "from_type": "record | event | quant | theme",
  "from_id": "string",
  "to_type": "record | event | quant | theme",
  "to_id": "string",
  "relationship": "supports | extends | quant_context | contradicts | duplicate_theme",
  "weight": 0
}
```

## Clustering dimensions

Use:
- time proximity
- topic compatibility
- key phrase overlap
- source diversity
- quant support

## Processing stage

After `route_record.py` for accepted records:

```text
accepted record
-> cluster_records.py
-> update_story_graph.py
```

## Assignment algorithm

1. Read newly accepted records.
2. Compare against recent event clusters.
3. If similarity exceeds threshold, attach to existing cluster.
4. Else create a new cluster.

## Event title generation

Use deterministic title generation:
- top keywords
- dominant event type
- strongest source title
- optional later AI cleanup

## AI usage

Start deterministic:
- phrase overlap
- metadata overlap
- time-window rules

Later AI can:
- refine titles
- merge near-duplicate clusters
- summarize cluster evolution

## Metrics

Track:
- accepted records assigned to clusters
- new clusters created
- average cluster size
- number of quant-linked clusters
- source diversity per cluster

## Testing

### Unit tests
- similarity scoring
- threshold decisions
- new cluster creation
- existing cluster attachment

### Integration tests
- accepted records update clusters correctly
- quant records attach as context
- duplicate cluster creation is limited

## Deliverables
- event cluster schema
- story edge schema
- clustering config
- cluster assignment script
- story graph update script
- accepted-record integration
- tests

## Definition of done

Complete when:
1. accepted records can be grouped into event clusters
2. event clusters are stored persistently
3. the repo maintains graph-style relationships
4. quant context can attach to narrative clusters
5. cluster output supports later synthesis
