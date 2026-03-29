# V2.5 Part 3 — Adaptive Keyword Expansion and Theme Memory

## Objective

Build a theme memory layer that learns from accepted records and improves discovery over time by expanding keywords and topic bundles automatically.

This part makes the system less static. Instead of relying only on manually chosen keywords, the archive begins to learn:

- which themes recur in accepted records
- which phrases signal useful candidates
- which topic bundles deserve more discovery coverage

---

## Why this part matters

Once you have:

- unified scoring
- source/domain memory

then the next best upgrade is to make discovery itself smarter.

Without adaptive keyword expansion:

- keyword discovery stays static
- the archive misses emerging language shifts
- new themes are discovered too slowly

With theme memory:

- the system learns what good finance records look like
- keyword queries improve over time
- discovery gets better without manual intervention every week

---

## Scope

This part includes:

- theme extraction from accepted records
- recurring phrase memory
- keyword bundle expansion
- negative keyword learning
- topic family tracking
- feedback loop into keyword discovery lane

This part does **not** yet include:

- full event clustering
- digest generation
- cross-record synthesis

---

## End state

At the end of this part, the system should be able to say:

- accepted records often mention `funding conditions`, so expand that bundle
- accepted records often link `repo` + `liquidity` + `Treasury`, so prioritize those combinations
- rejected records often contain certain broad non-finance phrases, so suppress those in discovery
- new keyword bundles should be proposed automatically for lane 2

---

## Theme memory model

Suggested theme-memory object:

```json
{
  "theme_id": "repo_liquidity_treasury",
  "theme_label": "Repo / Liquidity / Treasury",
  "positive_terms": ["repo", "liquidity", "treasury", "funding conditions"],
  "negative_terms": ["conference registration", "career center"],
  "accepted_count": 18,
  "review_count": 4,
  "rejected_count": 2,
  "last_seen": "ISO-8601",
  "priority_score": 82
}
```

---

## Files to add

Suggested files:

```text
scripts/
  extract_theme_terms.py
  update_theme_memory.py
  propose_keyword_expansions.py
  apply_keyword_expansions.py
schemas/
  theme_memory.json
config/
  keyword_bundles.json
  negative_keyword_bundles.json
data/
  theme_memory/
    themes.json
    expansions.json
```

---

## Theme extraction inputs

Theme extraction should use accepted records first.

Best fields to inspect:

- title
- summary
- why_it_matters
- market_structure_context
- macro_context
- tags
- important_numbers labels if present

This keeps the learning signal focused on what the archive already trusts.

---

## Positive term extraction

The system should extract terms and phrases that show up repeatedly in accepted records.

Examples:

- funding conditions
- term premium
- policy repricing
- Treasury issuance
- reserve balances
- ON RRP
- repo market stress
- inflation expectations
- labor market cooling
- curve steepening

Start with frequency + co-occurrence rules.

---

## Negative term extraction

The system should also learn phrases that repeatedly correlate with noise.

Examples:

- event registration
- career center
- donate now
- webinar replay
- museum
- community outreach

These should feed back into keyword discovery suppression.

---

## Topic bundles

Keyword discovery should not operate on individual terms only.

Use bundles like:

- `repo + liquidity + treasury`
- `inflation + expectations + rates`
- `policy + repricing + curve`
- `auction + funding + issuance`

This is much stronger than flat keyword lists.

Suggested file:

```text
config/keyword_bundles.json
```

Each bundle can have:

- label
- required terms
- optional terms
- exclusions
- current priority score

---

## Expansion logic

The system should propose expansions, not blindly auto-activate all of them.

Suggested process:

1. extract candidate positive phrases from accepted records
2. rank by recurrence and usefulness
3. compare against existing bundles
4. propose:
   - add new term to existing bundle
   - create new bundle
   - raise priority of existing bundle
5. optionally require human approval before activation

---

## Negative bundle logic

Similarly, build negative bundles from repeatedly bad candidates.

Example:

```json
{
  "bundle_id": "broad_event_noise",
  "terms": ["event registration", "conference", "careers", "webinar"],
  "suppression_strength": 0.8
}
```

These should reduce the score of discovery candidates that match them.

---

## Integration with Lane 2 keyword discovery

This part mainly improves keyword discovery.

### Before

- lane 2 uses static keyword bundles

### After

- lane 2 uses static bundles plus learned bundles
- discovery queries evolve over time
- bundle priority is adjusted by success rates

Example:

```text
static bundle: treasury funding
learned expansion: reserve balances
new search bundle: treasury funding reserve balances liquidity
```

---

## Integration with scoring

The adaptive keyword layer should feed into candidate scoring.

If a candidate matches a high-priority learned theme, it should receive a stronger keyword relevance score.

If it matches a negative learned theme, it should lose score.

---

## Human-in-the-loop option

Recommended for early V2.5:

- auto-generate suggested expansions
- store them in a queue
- optionally approve them manually

This prevents drift.

Later, once stable, some low-risk bundle promotions can become automatic.

---

## Acceptance criteria

This part is complete when:

- accepted records update theme memory
- recurring useful phrases are tracked
- recurring noisy phrases are tracked
- keyword discovery can consume learned bundle expansions
- candidate scoring uses learned themes
- discovery quality improves over several runs

---

## Risks

### Risk 1

The system may overfit to currently dominant themes and miss new ones.

Mitigation:

- preserve some static discovery bundles
- keep a percentage of exploration queries

### Risk 2

Weak accepted records may teach bad terms.

Mitigation:

- build expansions primarily from high-confidence accepted records
- optionally weight human-approved records more heavily

### Risk 3

Bundle explosion can make discovery hard to manage.

Mitigation:

- require minimum recurrence thresholds
- merge similar bundles instead of duplicating them

---

## Recommended implementation order

1. define theme memory schema
2. extract positive/negative phrase candidates from accepted/rejected records
3. persist theme memory
4. generate keyword expansion proposals
5. integrate approved expansions into lane 2
6. feed learned themes into candidate scoring

---

## Output of this part

After this part, the archive starts to teach the discovery system what to look for next.

That is one of the major steps toward a self-improving finance RAG pipeline.
