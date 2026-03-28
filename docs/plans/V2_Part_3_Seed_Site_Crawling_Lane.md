# V2 Part 3 — Seed-Site Crawling Lane

## Purpose

This spec defines Lane 3 of V2: seed-site crawling.

This lane explores trusted domains more deeply than Lane 1.
It is meant to find valuable internal pages you did not explicitly whitelist.

This part covers:
- seed site config
- crawl scope rules
- crawl depth
- allowed/disallowed URL patterns
- page scoring
- candidate generation
- dedupe and conversion into the shared candidate layer
- acceptance criteria

---

## Why this lane exists

Current source monitoring only checks pages you already listed.

Seed-site crawling lets you:
- discover new subpages on trusted domains
- find overlooked sections
- pick up deeper pages without manually enumerating everything

Examples:
- a new NY Fed markets note
- a Treasury page not directly linked from your current monitored page
- an ECB subpage that fits your themes but is not in the target list

---

## Core principle

This lane should only crawl **trusted seed domains**.

It is not general web crawling.

That is how you get exploratory coverage without losing trust discipline.

---

## Seed site config

Create:

`config/seed_sites.json`

Suggested structure:

```json
{
  "seeds": [
    {
      "id": "fed",
      "enabled": true,
      "domain": "federalreserve.gov",
      "start_urls": [
        "https://www.federalreserve.gov/newsevents.htm",
        "https://www.federalreserve.gov/monetarypolicy.htm"
      ],
      "allowed_prefixes": [
        "https://www.federalreserve.gov/newsevents/",
        "https://www.federalreserve.gov/monetarypolicy/"
      ],
      "blocked_fragments": [
        "about",
        "careers",
        "education",
        "calendar"
      ],
      "max_depth": 2,
      "max_pages": 20,
      "topic": "macro catalysts"
    },
    {
      "id": "nyfed",
      "enabled": true,
      "domain": "newyorkfed.org",
      "start_urls": [
        "https://www.newyorkfed.org/press",
        "https://www.newyorkfed.org/markets/data-hub"
      ],
      "allowed_prefixes": [
        "https://www.newyorkfed.org/newsevents/",
        "https://www.newyorkfed.org/markets/",
        "https://www.newyorkfed.org/research/"
      ],
      "blocked_fragments": [
        "about",
        "museum",
        "community",
        "education",
        "people"
      ],
      "max_depth": 2,
      "max_pages": 20,
      "topic": "market structure"
    }
  ]
}
```

---

## Crawl rules

### 1. Respect domain boundaries
Do not leave the configured domain family.

### 2. Respect allowed prefixes
Even inside a domain, only crawl allowed sections.

### 3. Respect blocked fragments
Do not enqueue URLs containing blocked fragments.

### 4. Limit crawl depth
Keep it shallow at first.
Recommended:
- depth 1 or 2

### 5. Limit page count per seed
Recommended:
- 10 to 25 pages per seed site per run

---

## Page selection philosophy

This crawler is not trying to mirror the site.
It is trying to discover finance-relevant candidate pages.

That means:
- score links before enqueueing them
- prioritize article-like paths
- heavily penalize navigation pages

---

## Link scoring during crawl

Reuse the same concept as improved article ingestion.

### Positive hints
- press
- release
- statement
- speech
- testimony
- report
- bulletin
- commentary
- research
- staff-report
- market notice
- policy
- article

### Negative hints
- about
- careers
- events
- experts
- people
- education
- programs
- museum
- archive
- category
- tag
- subscribe

The crawler should only enqueue high-scoring links.

---

## Crawl queue model

Use a BFS-style queue with scoring.

Queue item structure:

```json
{
  "url": "string",
  "depth": 0,
  "score": 0,
  "parent_url": "string",
  "anchor_text": "string"
}
```

Suggested policy:
- sort by score descending inside same depth
- stop when `max_pages` reached
- stop when `max_depth` exceeded

---

## Candidate generation

For each crawled page that looks promising:
1. fetch HTML
2. extract visible text
3. create shared candidate record
4. include crawl metadata:
   - seed ID
   - parent URL
   - depth
   - discovery method = crawl

Suggested extra metadata block inside the candidate:

```json
"crawl": {
  "seed_id": "nyfed",
  "parent_url": "https://www.newyorkfed.org/press",
  "depth": 1
}
```

---

## Suggested scripts

### New files
- `config/seed_sites.json`
- `scripts/run_seed_crawl.py`
- `scripts/crawl_seed_site.py`
- `scripts/crawl_queue.py`
- `scripts/extract_internal_links.py`

### `run_seed_crawl.py` responsibilities
1. load seed config
2. run each enabled seed site crawl
3. emit candidate records to shared candidate folders
4. call shared dedupe/filter/convert steps
5. pass survivors to `process_record.py`

### `crawl_seed_site.py` responsibilities
1. initialize queue from start URLs
2. fetch pages up to max depth/page limits
3. score internal links
4. enqueue only allowed links
5. emit candidate pages

---

## Dedupe strategy

This lane must dedupe aggressively because many seed pages are closely related.

Use shared candidate dedupe plus extra crawl-level controls:
- do not revisit same URL in one run
- do not enqueue same normalized URL twice
- skip pages already seen recently in the candidate manifest

---

## Lane-specific filtering

Seed crawl content should be filtered a bit more strictly than trusted-source monitoring.

Why:
- it is easier for crawlers to drift into low-value site structure
- trusted domain does not guarantee useful page

Suggested stricter checks:
- minimum score threshold before fetch
- minimum score threshold before conversion
- stronger blocklist fragments

---

## Example flow

```text
run_seed_crawl.py
-> load seed_sites.json
-> for each seed
   -> initialize crawl queue
   -> fetch seed page
   -> score internal links
   -> enqueue top allowed links
   -> crawl up to depth/page limits
   -> emit candidate records
-> shared dedupe
-> shared scoring/filtering
-> convert survivors to raw records
-> process_record.py
-> send pending reviews
```

---

## Safety and control

This lane should start with:
- a small number of seeds
- shallow depth
- low page limits

Recommended initial seeds:
- federalreserve.gov
- newyorkfed.org
- treasury.gov
- bankofcanada.ca
- ecb.europa.eu

Do not start with 20 seed domains at once.

---

## Monitoring stats

Create:

`data/candidate_manifests/seed_crawl_stats.json`

Suggested metrics per seed:
- pages visited
- links scored
- links enqueued
- candidates emitted
- deduped out
- filtered out
- converted
- accepted
- review
- rejected

This will tell you which seeds are worth keeping.

---

## Acceptance criteria

Part 3 is complete when:
- seed sites are configurable in JSON
- crawler respects allowed prefixes and blocked fragments
- crawler respects depth/page limits
- discovered pages are emitted as shared candidate records
- candidates go through shared dedupe/filter/convert flow
- survivors enter the existing archive backend

---

## Non-goals for Part 3

This part does not build:
- open-web crawling
- autonomous domain trust promotion
- semantic crawl prioritization with AI

That can come later.

---

## Final outcome of Part 3

When complete, V2 will be able to discover deeper relevant pages on trusted domains without manually whitelisting every single path.

That gives the archive exploratory depth while preserving a trust-first architecture.
