# V2.7 Part 6 — Massive Article Source Expansion Program

## Objective

Expand the article research pipeline by **at least 100 additional sources** in a controlled, lane-aware way that does not destroy quality.

This is not just “add more URLs.” It is a structured source expansion program with categories, intake rules, target classes, and phased rollout.

## Why this is needed

The current repo already has a broad set of official and semi-official sources, but V2.7 should become a much deeper finance intelligence engine.

To do that, the article lane must widen across:
- official institutions
- regulators
- exchanges
- market infrastructure
- regional Fed research
- central bank speech/news hubs
- high-signal institutional research
- macro commentary sources

## Success criteria

By the end of this part, the repo should have:
1. a curated list of at least 100 additional article sources
2. source metadata attached to each target
3. target-class annotations
4. phased rollout guidance
5. source family grouping for later analytics

## New concepts

### Source family
A grouping such as:
- central banks
- regulators
- exchanges
- market infrastructure
- research institutions
- regional Federal Reserve Banks

### Target class
A content category such as:
- press_release
- speech
- policy_statement
- research_article
- blog
- market_notice
- infrastructure_update

## Config additions

Every new target should include:
```json
{
  "family": "central_bank",
  "target_class": "speech",
  "enabled": false,
  "priority_tier": "A"
}
```

Recommended new fields:
- `family`
- `target_class`
- `priority_tier`
- `enabled`
- `url_blocklist_fragments`
- `max_links`

## Rollout strategy

Do **not** enable 100+ new sources at once.

### Phase 1
Add them all to config but keep most disabled.

### Phase 2
Enable one family at a time.

### Phase 3
Use source analytics to promote/demote.

## Source families to add

### Family 1 — Additional central bank press/speech/news sources
Target count: 20+
Examples:
- Bank of Japan
- Sveriges Riksbank
- Swiss National Bank
- Norges Bank
- Reserve Bank of India
- Banco de Mexico
- Monetary Authority of Singapore
- Hong Kong Monetary Authority
- additional Bank of Canada / BoE / ECB subpages

### Family 2 — Additional regional Federal Reserve research/publication sources
Target count: 15+
Examples:
- Minneapolis Fed
- Richmond Fed economic briefs
- Cleveland Fed commentary
- Dallas Fed economics
- Boston Fed policy publications
- Kansas City Fed economic bulletin
- San Francisco Fed economic letters
- St. Louis Fed blogs
- Philadelphia Fed research streams
- Chicago Fed publications
- Atlanta Fed macro research subpages

### Family 3 — Regulators and policy infrastructure
Target count: 15+
Examples:
- CFTC
- OCC
- FDIC
- FINRA
- ESMA
- FCA
- OSFI
- CIRO
- IOSCO
- BIS committees
- SEC subpages

### Family 4 — Exchanges, clearing, and market infrastructure
Target count: 15+
Examples:
- Eurex
- LSEG / Refinitiv
- ICE
- CME sub-insights
- DTCC notices
- CLS
- clearinghouse public notices
- SIFMA support pages
- ISDA research/news
- FIX Trading Community insights

### Family 5 — Institutional and think-tank research
Target count: 20+
Examples:
- Peterson Institute
- Brookings substreams
- IMF finance/macro pages
- BIS blogs/publications subsets
- OECD macro pages
- NBER summaries/news
- CEPR policy portal
- Bruegel
- tightly filtered AEI / CFR / Hoover later if useful

### Family 6 — Treasury and fiscal / debt context
Target count: 10+
Examples:
- Treasury supporting financing pages
- Fiscal Data subdatasets and notes
- debt management pages
- auction announcements
- debt ceiling / cash balance informational pages
- official debt pages

### Family 7 — Global macro institutions
Target count: 10+
Examples:
- World Bank macro commentary
- OECD macro publications
- IMF fiscal monitor pages
- BIS quarterly review subsets
- ECB blog substreams
- BoE speeches/news substreams
- Bank of Canada publication substreams

## Source metadata requirements

Every added source should specify:
- `name`
- `topic`
- `url`
- `allowed_prefixes`
- `url_blocklist_fragments`
- `max_links`
- `required_keywords`
- `blocked_keywords`
- `min_word_count`
- `family`
- `target_class`
- `priority_tier`
- `enabled`

## Priority tiers

### Tier S
Core trusted official sources

### Tier A
Strong central-bank / regulator / infrastructure pages

### Tier B
Broader research and commentary pages

### Tier C
Experimental or noisier sources, disabled by default

## New files

```text
config/article_source_families.json
config/article_source_expansion_batch_1.json
config/article_source_expansion_batch_2.json
config/article_source_expansion_batch_3.json
```

## Analytics integration

All new sources should be compatible with Part 5 source analytics from day one.

Track:
- family
- target class
- acceptance rates by family

## Testing

### Config validation
- valid JSON
- required fields present
- target metadata present

### Lane validation
- disabled sources do not run
- enabled subset works safely
- broad blocklists reduce structural junk

### Manual review
Enable one family at a time and inspect:
- accepted ratio
- filtered ratio
- review burden

## Deliverables
- a structured 100+ additional article source expansion plan
- updated config schema with source family metadata
- expansion batch files or integrated config
- target classes and priority tiers
- rollout guidance
- tests/validation scripts if needed

## Definition of done

Complete when:
1. the repo has at least 100 additional article sources defined
2. those sources are categorized by family and class
3. rollout can happen safely in phases
4. the source expansion is compatible with analytics and triage
5. the article lane has enough depth to support a much more powerful finance archive
