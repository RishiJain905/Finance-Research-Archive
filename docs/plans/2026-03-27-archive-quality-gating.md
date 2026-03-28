# Archive Quality Gating Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the article pipeline pass real finance research into the archive and reject container pages, thin commentary, and low-signal noise before they reach `accepted`.

**Architecture:** Push quality control to the edges. Ingest should classify pages and pass structured metadata, filtering should reject weak or non-article records deterministically, and verifier output should be gated by hard rules instead of model optimism. The result is fewer records, but materially better ones.

**Tech Stack:** Python, BeautifulSoup, requests, MiniMax via OpenAI-compatible client, JSON config files, GitHub Actions.

---

### Task 1: Add structured source diagnostics at ingest time

**Files:**
- Modify: `scripts/ingest_sources.py`
- Modify: `config/ingestion_targets.json`
- Test: `tests/test_archive_quality.py`

**Step 1: Write the failing test**

Add tests that prove:
- root URLs and `/index` pages classify as `homepage` or `navigation_page`
- article-level pages classify as `article`
- link selection prefers dated/article URLs over site roots and hub pages
- raw output includes metadata fields separate from body text

**Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_archive_quality -v`

Expected: failures for missing classification and metadata behavior.

**Step 3: Write minimal implementation**

Implement:
- `extract_main_text()` that prefers `article`, `main`, and common content containers
- `classify_page_type()` for `homepage`, `navigation_page`, `listing_page`, `search_page`, and `article`
- `extract_published_at()`, `extract_canonical_url()`, and language detection helpers
- stricter link scoring that penalizes roots, index pages, and hub pages
- raw file output that includes a structured metadata header plus body text

Update target config so the broadest sources use tighter allowlists and page-type expectations.

**Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_archive_quality -v`

Expected: pass.

**Step 5: Commit**

```bash
git add scripts/ingest_sources.py config/ingestion_targets.json tests/test_archive_quality.py
git commit -m "Improve ingest diagnostics and page classification"
```

### Task 2: Reject weak records before summarization

**Files:**
- Modify: `scripts/filter_raw_records.py`
- Test: `tests/test_archive_quality.py`

**Step 1: Write the failing test**

Add tests that prove:
- container-page metadata causes a deterministic reject
- language mismatches are rejected
- low-signal pages do not survive just because they are long

**Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_archive_quality -v`

Expected: failures for missing page-type and language gating behavior.

**Step 3: Write minimal implementation**

Implement:
- `parse_raw_record()` to split metadata from body
- article evaluation that uses metadata, not only raw text
- explicit rejection reasons for `non_article_page_type`, `language_mismatch`, and `container_page_warning`
- body-only evaluation so header text does not distort word-count or keyword scoring

Keep the filter conservative:
- low-quality container pages are filtered out
- article-level pages still need minimum substance and relevance

**Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_archive_quality -v`

Expected: pass.

**Step 5: Commit**

```bash
git add scripts/filter_raw_records.py tests/test_archive_quality.py
git commit -m "Reject weak records earlier in the pipeline"
```

### Task 3: Pass metadata into summarizer and verifier prompts

**Files:**
- Modify: `scripts/run_summarizer.py`
- Modify: `scripts/run_verifier.py`
- Modify: `prompts/summarize.txt`
- Modify: `prompts/verify.txt`
- Test: `tests/test_archive_quality.py`

**Step 1: Write the failing test**

Add tests that prove:
- prompt input separates `SOURCE METADATA` and `SOURCE BODY`
- record templates are prefilled from source metadata when available
- generated records inherit source attribution fields instead of leaving them blank

**Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_archive_quality -v`

Expected: prompt/template tests fail until metadata is wired through.

**Step 3: Write minimal implementation**

Implement:
- prompt builders that pass metadata and body as separate blocks
- record template hydration from metadata fields such as `TARGET`, `URL`, `PUBLISHED_AT`, and `PAGE_TYPE`
- prompt rules that forbid inference on unsupported market reactions and treat container pages as non-archiveable

Keep the prompt changes narrow and explicit. The goal is to improve signal, not add new schema complexity.

**Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_archive_quality -v`

Expected: pass.

**Step 5: Commit**

```bash
git add scripts/run_summarizer.py scripts/run_verifier.py prompts/summarize.txt prompts/verify.txt tests/test_archive_quality.py
git commit -m "Pass structured source metadata into LLM stages"
```

### Task 4: Add a hard verifier gate and archive routing rules

**Files:**
- Modify: `scripts/run_verifier.py`
- Test: `tests/test_archive_quality.py`

**Step 1: Write the failing test**

Add tests that prove:
- container pages cannot be accepted even if the verifier is optimistic
- missing source attribution pushes the record to review or reject
- acceptance requires high verifier confidence and no outstanding issues

**Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_archive_quality -v`

Expected: verifier gate tests fail until the hard rules exist.

**Step 3: Write minimal implementation**

Implement:
- `apply_archive_quality_gate()` or equivalent deterministic gate
- acceptance only when the verifier is high confidence, issue-free, source-attributed, and article-like
- reject container pages immediately
- route borderline but real sources to review instead of accepting them

Do not make the model responsible for final acceptance on its own.

**Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_archive_quality -v`

Expected: pass.

**Step 5: Commit**

```bash
git add scripts/run_verifier.py tests/test_archive_quality.py
git commit -m "Gate archive acceptance on source quality"
```

### Task 5: Backfill and verify current archive quality

**Files:**
- Modify if needed: `scripts/process_record.py`
- Modify if needed: `scripts/run_ingest_and_process.py`
- Modify if needed: `scripts/route_record.py`
- Inspect: `data/accepted`, `data/review_queue`, `data/filtered_out`

**Step 1: Write the failing test**

Add a regression test or fixture check for one known bad source from `data/accepted`, such as:
- NYSE homepage recap-style content
- ECB navigation/menu pages

The test should prove the new gates classify them as non-archiveable.

**Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_archive_quality -v`

Expected: fixture test fails until the backfill logic or classification rule is present.

**Step 3: Write minimal implementation**

Implement the smallest backfill path needed:
- reprocess current `data/raw`
- move stale low-signal records out of `accepted`
- preserve real article-level and data-release records

If a one-time script is needed, keep it simple and local to the repo.

**Step 4: Run test to verify it passes**

Run:
- `python -m unittest tests.test_archive_quality -v`
- `python -m compileall scripts tests`

Expected: pass.

**Step 5: Commit**

```bash
git add scripts/process_record.py scripts/run_ingest_and_process.py scripts/route_record.py data/*
git commit -m "Backfill archive quality rules"
```

## Test Plan

- Unit tests for page classification, metadata parsing, prompt separation, and verifier gating.
- Compile check for `scripts/` and `tests/`.
- Smoke-check known noisy samples from `data/accepted` and `data/raw` after the gating changes.
- Optional one-time archive backfill to reclassify stale low-value records.

## Assumptions

- Better precision is worth fewer accepted records.
- Secondary commentary is allowed only when it is article-level and concrete.
- Container pages, site roots, and navigation pages should be rejected by default.
- No new heavyweight dependencies are needed; current Python stack is sufficient.
