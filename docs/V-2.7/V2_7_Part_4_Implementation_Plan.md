# V2.7 Part 4 — Article and Quant Enrichment: Implementation Plan

## Plan Overview

This plan implements the complete V2.7 Part 4 spec for linking article records and quant records. The existing codebase already has bidirectional linking infrastructure (`link_article_and_quant_records.py`) that embeds links directly in records. This plan adds a **new deterministic, config-driven linking system** with standalone persistent link files, quant family mapping rules, and accepted record enrichment.

**Key Design Decision:** The new system complements rather than replaces the existing linking. The new `link_article_quant.py` uses config-driven rules (`quant_linking_rules.json`) for deterministic topic/keyword-to-series mapping, creates standalone link files in `data/article_quant_links/`, and enriches accepted records with `quant_context` blocks.

---

## 1. Plan Intake (ocExecution.md §2)

### Shared Files
- `config/quant_sources.json` — Read-only reference for quant series definitions
- `data/accepted/*.json` — Read accepted records, write enrichment blocks
- `data/events/*.json` — Read event clusters, add narrative+numeric evidence
- `scripts/link_article_and_quant_records.py` — Existing linking (not modified, coexists)

### Overlap/Conflict Zones
- **None expected** — New files are additive: new script, new schema, new config, new data directory
- Event cluster schema may need `article_links` field added (currently only has `quant_links`)

### Ordering Dependencies
1. Schema and config must exist before linking script
2. Linking script must exist before tests
3. Tests must pass before integration
4. Enrichment logic depends on linking script output

### Verification Strategy
- Unit tests for each component (mapping, time-window, scoring, link creation)
- Integration tests for end-to-end linking and enrichment
- Full test suite run after integration

---

## 2. Implementation Streams (ocExecution.md §3)

### Stream A: Schema, Config, and Data Structure
**Owner:** `test-automation-engineer` (TDD-first: schema/config tests → implementation)
**Files:**
- `schemas/article_quant_link.json`
- `config/quant_linking_rules.json`
- `data/article_quant_links/.gitkeep`

### Stream B: Core Linking Script
**Owner:** `reliable-backend-architect` (TDD-first: tests → implementation)
**Files:**
- `scripts/link_article_quant.py`

### Stream C: Enrichment and Event Cluster Integration
**Owner:** `reliable-backend-architect` (depends on Stream B)
**Files:**
- `scripts/link_article_quant.py` (enrichment functions)
- `schemas/event_cluster.json` (add `article_links` field)

### Stream D: Test Suite
**Owner:** `test-automation-engineer` (parallel with B and C, TDD enforcement)
**Files:**
- `tests/test_link_article_quant_v2.py`

---

## 3. Detailed Task Breakdown

### Task A1: Create Article-Quant Link Schema
**File:** `schemas/article_quant_link.json`
**Spec:**
```json
{
  "link_id": "string (UUID or deterministic hash)",
  "article_record_id": "string",
  "quant_record_id": "string",
  "event_id": "optional string",
  "relationship": "enum: supports | context | confirms | weak_context",
  "score": "number (0-100)",
  "matched_dimensions": ["topic", "time_window", "keyword_overlap", "event_alignment"],
  "created_at": "ISO-8601"
}
```
**TDD:** Test schema validity, required fields, enum validation

### Task A2: Create Quant Linking Rules Config
**File:** `config/quant_linking_rules.json`
**Content (from spec):**
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
  },
  "scoring_bands": {
    "strong": {"min": 80, "relationship": "supports"},
    "contextual": {"min": 60, "relationship": "context"},
    "weak": {"min": 40, "relationship": "weak_context"},
    "ignore_below": 40
  },
  "dimension_weights": {
    "topic": 0.30,
    "time_window": 0.25,
    "keyword_overlap": 0.25,
    "event_alignment": 0.20
  },
  "time_window_days": 7
}
```

### Task A3: Create Data Directory
**File:** `data/article_quant_links/.gitkeep`

### Task B1: Core Linking Script — Record Loading and Classification
**File:** `scripts/link_article_quant.py`
**Functions:**
- `load_accepted_records(base_dir)` — Reuse pattern from existing script
- `is_quant_record(record)` — Reuse or adapt from existing
- `is_article_record(record)` — Negation of is_quant_record

### Task B2: Deterministic Linking — Topic Compatibility
**Functions:**
- `get_topic_series(topic, config)` — Look up topic_to_series mapping
- `compute_topic_score(article, quant, config)` — Score based on topic-to-series match

### Task B3: Deterministic Linking — Time Window
**Functions:**
- `compute_time_window_score(article_date, quant_date, config)` — Score based on time proximity within configured window
- Reuse/adapt `parse_date` from existing script

### Task B4: Deterministic Linking — Keyword Overlap
**Functions:**
- `get_keyword_series(keywords, config)` — Look up keyword_to_series mapping
- `compute_keyword_overlap_score(article, quant, config)` — Score based on shared keyword-to-series mappings

### Task B5: Deterministic Linking — Event Alignment
**Functions:**
- `compute_event_alignment_score(article, quant, events)` — Score based on shared event cluster membership
- `load_events(event_dir)` — Load event clusters

### Task B6: Combined Scoring and Relationship Classification
**Functions:**
- `compute_link_score(article, quant, events, config)` — Weighted sum of all dimensions
- `classify_relationship(score, config)` — Map score to relationship type using scoring_bands
- `create_link(article_id, quant_id, event_id, relationship, score, dimensions)` — Create link dict conforming to schema

### Task B7: Link Persistence
**Functions:**
- `save_link(link, output_dir)` — Write standalone link JSON file
- `load_links(output_dir)` — Load existing links (for dedup)
- `generate_link_id(article_id, quant_id)` — Deterministic ID

### Task B8: Main Orchestration
**Functions:**
- `link_all_records(articles, quants, events, config, output_dir)` — Main linking loop
- `main()` — CLI entry point

### Task C1: Accepted Record Enrichment
**Functions:**
- `enrich_accepted_record(record, links, link_type)` — Add quant_context or article_context block
- `write_enriched_record(record, base_dir)` — Write updated record back

**Enrichment format (from spec):**
```json
"quant_context": {
  "linked_quant_records": ["fed_funds_2026_04_05", "2y_yield_2026_04_05"],
  "summary": "Front-end rates remained elevated around the policy communication."
}
```

### Task C2: Event Cluster Enrichment
**Functions:**
- `enrich_event_cluster(event, article_links, quant_links)` — Add both narrative and numeric evidence
- Update `schemas/event_cluster.json` to add `article_links` field

### Task D1: Unit Tests
**File:** `tests/test_link_article_quant_v2.py`
**Test classes:**
- `TestLinkSchema` — Schema validation
- `TestQuantLinkingRulesConfig` — Config loading and structure
- `TestTopicSeriesMapping` — topic_to_series lookups
- `TestKeywordSeriesMapping` — keyword_to_series lookups
- `TestTimeWindowScoring` — Time window logic
- `TestKeywordOverlapScoring` — Keyword overlap logic
- `TestEventAlignmentScoring` — Event alignment logic
- `TestCombinedScoring` — Weighted score calculation
- `TestRelationshipClassification` — Score band mapping
- `TestLinkCreation` — Link dict structure
- `TestLinkPersistence` — File creation and loading
- `TestLinkDeduplication` — No duplicate links on re-run

### Task D2: Integration Tests
**Test classes:**
- `TestArticleToQuantLinking` — Accepted articles create quant links
- `TestQuantToArticleLinking` — Accepted quants create article links
- `TestEventLinkedRecords` — Event-clustered records get stronger matching
- `TestRecordEnrichment` — Accepted records receive enrichment blocks
- `TestEventClusterEnrichment` — Event clusters carry narrative + numeric evidence
- `TestEndToEnd` — Full pipeline: load → link → persist → enrich → verify

---

## 4. Subagent Delegation Strategy (ocExecution.md §4)

### Delegation Map

| Task | Subagent | Rationale |
|------|----------|-----------|
| A1, A2, A3 (Schema/Config/Data) | `test-automation-engineer` | TDD-first: write schema/config tests, then create files |
| B1-B8 (Core Linking Script) | `reliable-backend-architect` | Backend logic, data integrity, deterministic algorithms |
| C1-C2 (Enrichment) | `reliable-backend-architect` | Sequential dependency on B, same domain owner |
| D1-D2 (Test Suite) | `test-automation-engineer` | Comprehensive test creation, TDD enforcement |

### Execution Order

```
Phase 1 (Parallel): A1+A2+A3 (Schema/Config) + D1 (Test scaffolding)
Phase 2 (Sequential): B1-B4 (Core linking dimensions)
Phase 3 (Sequential): B5-B8 (Event alignment + orchestration + persistence)
Phase 4 (Sequential): C1-C2 (Enrichment logic)
Phase 5 (Parallel): D2 (Integration tests) + verification
Phase 6 (Orchestrator): Integration, merge, final verification
```

### Parallel Investigation Opportunities
- Phase 1: Schema/Config creation and test scaffolding can run in parallel
- Phase 5: Integration tests can run in parallel with final enrichment implementation

---

## 5. Integration Strategy (ocExecution.md §5)

### Orchestrator Responsibilities
1. **Merge ordering:** A → B → C → D (schema/config first, then script, then enrichment, then tests)
2. **Conflict resolution:** No conflicts expected (all new files), but verify no overlap with existing `link_article_and_quant_records.py`
3. **Integrated verification:** Run full test suite after all streams merged
4. **Behavior preservation:** Existing linking (`link_article_and_quant_records.py`) continues to work unchanged

### Merge Strategy
- **Cherry-pick** per task group (A, B, C, D) to maintain clean history
- Each task group merged only after its tests pass
- Final integration: run `pytest tests/` to confirm no regressions

---

## 6. Verification Plan (ocExecution.md §6)

### Per-Stream Verification
- **Stream A:** Schema validation tests, config structure tests
- **Stream B:** Unit tests for each scoring dimension, combined scoring, link creation
- **Stream C:** Enrichment format tests, event cluster update tests
- **Stream D:** All tests must pass before claiming completion

### Integration Verification Commands
```bash
# Run new tests only
pytest tests/test_link_article_quant_v2.py -v

# Run all tests to confirm no regressions
pytest tests/ -v

# Run linking script manually to verify output
python scripts/link_article_quant.py --dry-run
python scripts/link_article_quant.py
```

### Verification Reports Must Include
- Tests added: count and names
- Failing test evidence (if any, before fixes)
- Passing test confirmation (all green)
- Full suite results after integration
- Manual verification of link files in `data/article_quant_links/`
- Manual verification of enrichment blocks in `data/accepted/`

---

## 7. Cleanup Plan (ocExecution.md §7)

### Post-Integration
1. Remove any temporary worktrees
2. Delete merged branches
3. Verify `data/article_quant_links/` contains expected link files
4. Verify enriched accepted records have `quant_context` or `article_context` blocks
5. Verify event clusters have both `article_links` and `quant_links`

### Quality Review Checklist
- No duplicated logic between new and existing linking scripts
- Consistent naming conventions (snake_case for scripts, tests, configs)
- All new files follow existing patterns (BASE_DIR, load_json, save_json)
- No leftover debug code or temporary scaffolding
- Schema files are valid JSON Schema draft-07
- Config files are valid JSON

---

## 8. Definition of Done (per spec)

Complete when:
1. ✅ Accepted article and quant records can be linked reliably (via `link_article_quant.py`)
2. ✅ Relationship files are persisted in `data/article_quant_links/`
3. ✅ Accepted records include cross-lane context (`quant_context` / `article_context` blocks)
4. ✅ Event clusters carry both narrative and numeric evidence (`article_links` + `quant_links`)
5. ✅ The archive is more useful for explanatory finance retrieval (links enable narrative+quant queries)

### Deliverables Checklist
- [ ] `schemas/article_quant_link.json` — Link schema
- [ ] `config/quant_linking_rules.json` — Quant linking config
- [ ] `scripts/link_article_quant.py` — Linking script
- [ ] `data/article_quant_links/` — Persistent link storage
- [ ] Accepted record enrichment logic
- [ ] Event cluster enrichment (article_links field)
- [ ] `tests/test_link_article_quant_v2.py` — Comprehensive test suite
- [ ] All tests passing
- [ ] No regressions in existing tests

---

## 9. Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Existing linking script conflicts | New script uses different output (standalone files vs embedded), no modification to existing |
| Event cluster schema change breaks existing | Additive change only (`article_links` field), `additionalProperties: true` already set |
| Config-driven rules don't match real data | Start with spec-provided rules, make configurable, add dry-run mode |
| Performance with many records | Use lookup maps (dict by ID), limit to recent records within time window |
| Link deduplication on re-run | Deterministic link_id, check existing links before creating new |

---

## 10. Execution Commands (for reference)

```bash
# Create new branch for work
git checkout -b v2.7-part4-article-quant-enrichment

# Run tests during development
pytest tests/test_link_article_quant_v2.py -v

# Run full suite before integration
pytest tests/ -v

# Run linking script
python scripts/link_article_quant.py

# Dry run (preview links without writing)
python scripts/link_article_quant.py --dry-run
```
