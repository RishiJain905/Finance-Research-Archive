# V2.7 Part 3 — Watchlists and Thesis Tracking

## Objective

Add explicit watchlists and thesis-tracking logic so the system can monitor what the operator actually cares about, not just what sources publish.

This should let the repo track themes like:
- Treasury refunding pressure
- repo stress
- inflation re-acceleration
- yield curve steepening
- ETF structure stress
- labor market cooling
- policy path repricing

## Why this is needed

The repo can already discover and process finance content, but it is still mostly source-driven.

V2.7 should make it **thesis-driven**.

That means:
- incoming records are evaluated against active watchlists
- event clusters get mapped to user-defined themes
- the system can tell you not just what is new, but what is relevant to active macro/market structure views

## Success criteria

By the end of this part, the repo should:
1. define multiple watchlists
2. score new records against watchlists
3. attach watchlist hits to accepted/reviewed records
4. track watchlist activity over time
5. support thesis states like strengthening / weakening / neutral

## New files and folders

```text
config/watchlists_v27.json
scripts/watchlist_matcher.py
scripts/update_thesis_state.py
schemas/watchlist.json
schemas/watchlist_hit.json
data/watchlists/
data/watchlist_hits/
data/theses/
```

## Watchlist schema

```json
{
  "watchlist_id": "string",
  "title": "string",
  "topic": "string",
  "description": "string",
  "keywords": ["string"],
  "required_terms": ["string"],
  "blocked_terms": ["string"],
  "priority": "high | medium | low",
  "enabled": true
}
```

## Watchlist hit schema

```json
{
  "watchlist_id": "string",
  "record_id": "string",
  "event_id": "optional string",
  "match_score": 0,
  "matched_terms": ["string"],
  "thesis_signal": "strengthening | weakening | neutral",
  "created_at": "ISO-8601"
}
```

## Example watchlists
- repo stress
- Treasury refunding / issuance pressure
- inflation persistence
- policy repricing
- labor market cooling
- yield curve steepening
- fixed-income liquidity strain

## Matching logic

Use a deterministic matcher with:
- keyword overlap
- required term presence
- blocked term penalties
- topic compatibility
- event type compatibility

Inputs:
- title
- summary
- why_it_matters
- tags
- important_numbers
- cluster title later
- quant-linked metadata later

## Thesis states
Minimal model:
- strengthening
- weakening
- neutral

## New processing stage

After a record is accepted or routed to review:

```text
record
-> watchlist_matcher.py
-> watchlist hit files
-> optional thesis state update
```

For accepted records, hits should be persisted.
For review records, hits can exist but be marked unconfirmed.

## Event-level matching

Do not only match individual records. Also match event clusters from Part 2.

## Operator control

The user should be able to edit:
- watchlist keywords
- required terms
- blocked terms
- priority
- enabled/disabled

Store all of this in config.

## Metrics

Track:
- hits per watchlist per day
- accepted hits per watchlist
- rejected hits per watchlist
- review hits per watchlist
- thesis state changes over time
- source domains contributing to each watchlist

## Implementation order

1. Create watchlist config and schemas
2. Build deterministic matcher
3. Persist watchlist hit files
4. Attach watchlist metadata to records
5. Build thesis state updater

## Testing

### Unit tests
- keyword overlap
- required term logic
- blocked term logic
- thesis state rules

### Integration tests
- accepted records create watchlist hits
- event clusters can generate watchlist hits
- thesis state updates when evidence accumulates

## Deliverables
- watchlist schema
- watchlist hit schema
- watchlist config
- matcher
- thesis state updater
- record integration
- tests

## Definition of done

Complete when:
1. the repo can define multiple watchlists
2. records and event clusters can be matched against them
3. watchlist hits are persisted
4. simple thesis state changes are tracked
5. the archive reflects user intent, not just source output
