# Phase 8 — Vector Store and RAG Foundation

**Depends on:** Phase 1, Phase 3 (SQLite manifest for stable record IDs)  
**Priority:** Medium — this is what the README calls "for future RAG"; Phase 8 makes it real

---

## What This Phase Does

Embeds every accepted record into a local vector store. This enables:

1. **Semantic deduplication** — reject candidates that are too similar to already-accepted records (better than hash-based dedup alone)
2. **Local semantic search** over the full archive
3. **RAG-ready foundation** — downstream Q&A, clustering, and synthesis tools can query the vector store instead of scanning flat files

---

## Vector Store Choice: ChromaDB (local)

[ChromaDB](https://www.trychroma.com/) is the recommended choice because:

- Runs entirely locally with no external service
- Persists to a directory (`data/vector_store/`) that commits to git like other data
- Pure Python, single pip install
- Supports both embedding storage and similarity search
- No API key required

The embedding calls use the **existing MiniMax/OpenAI-compatible endpoint** already wired in the codebase — no new LLM provider needed.

---

## Implementation Plan

### Step 1 — Add ChromaDB to `requirements.txt`

```
chromadb>=0.4.0
```

### Step 2 — Create `scripts/vector_store.py`

A module that wraps ChromaDB and exposes three functions used by the rest of the pipeline:

```python
def get_collection() -> chromadb.Collection:
    """Returns the persistent ChromaDB collection from data/vector_store/."""

def upsert_record(record_id: str, content: str, metadata: dict) -> None:
    """Embeds content and upserts into the vector store."""

def search_similar(query_text: str, n_results: int = 5) -> list[dict]:
    """Returns the n most similar records to query_text."""

def is_semantically_duplicate(content: str, threshold: float = 0.92) -> bool:
    """Returns True if content is above similarity threshold vs any stored record."""
```

Embedding calls go through the existing `openai.Embedding.create()` pattern (or `client.embeddings.create()` for the newer SDK style) using `OPENAI_API_KEY` and `OPENAI_BASE_URL`.

### Step 3 — Embed Records at Route Time

In `scripts/route_record.py`, after a record is written to `data/accepted/`, call `vector_store.upsert_record(record_id, content, metadata)`. This keeps the vector store in sync with accepted records automatically on every pipeline run.

### Step 4 — Semantic Deduplication in `filter_raw_records.py`

Add a semantic dedup check as a final filter step: if `vector_store.is_semantically_duplicate(content, threshold=0.92)` returns True, the record is rejected with reason `"semantic_duplicate"` before it reaches the expensive summarizer step. The threshold of 0.92 is a starting point — tune based on false positive rate.

### Step 5 — Backfill Existing Accepted Records

Create `scripts/backfill_vector_store.py` — a one-time script that iterates all existing records in `data/accepted/` and calls `upsert_record` for each. Run once after ChromaDB is set up.

### Step 6 — Update `.gitignore`

ChromaDB creates several files in the persistence directory. Add:

```
data/vector_store/chroma.sqlite3-wal
data/vector_store/chroma.sqlite3-shm
```

But keep `data/vector_store/` itself tracked so the vector store persists across CI runs.

### Step 7 — Simple Search CLI (Bonus)

Create `scripts/search_archive.py` — a simple command-line tool:

```bash
python scripts/search_archive.py "yield curve inversion recession signal"
```

Prints the top 5 semantically similar records with title, date, and similarity score. Useful for local exploration.

---

## Embedding Model Consideration

The MiniMax endpoint you use for summarization also supports embeddings (MiniMax `embo-01` or the OpenAI-compatible `text-embedding-ada-002` equivalent). Check your MiniMax plan for embedding API availability. If embeddings are not available on your current plan:

> **Fallback option:** Use the free [Sentence Transformers](https://www.sbert.net/) library with a local model like `all-MiniLM-L6-v2`. This runs entirely offline with no API calls. Add `sentence-transformers` to `requirements.txt` and replace the `openai.embeddings` call in `vector_store.py` with `model.encode(content)`.

---

## New GitHub Secrets Required

**Likely none** — embeddings use the existing `OPENAI_API_KEY` and `OPENAI_BASE_URL` secrets already in the workflows.

If you switch to a different embedding provider, secrets for that provider would be needed. Add them the same way as `OPENAI_API_KEY`:

- `Settings → Secrets and variables → Actions → New repository secret`

---

## Files Changed in This Phase


| File                               | Change                                |
| ---------------------------------- | ------------------------------------- |
| `requirements.txt`                 | Add `chromadb`                        |
| `scripts/vector_store.py`          | New — ChromaDB wrapper module         |
| `scripts/route_record.py`          | Call `upsert_record` after acceptance |
| `scripts/filter_raw_records.py`    | Add semantic dedup check              |
| `scripts/backfill_vector_store.py` | New — one-time backfill script        |
| `scripts/search_archive.py`        | New — CLI search tool                 |
| `.gitignore`                       | Add ChromaDB temp files               |


---

## Acceptance Criteria

- Running `backfill_vector_store.py` produces a populated `data/vector_store/` directory
- `search_archive.py "federal reserve rate decision"` returns relevant records with scores above 0.7
- A record with >92% cosine similarity to an existing accepted record is rejected as `semantic_duplicate` before reaching the summarizer
- The vector store size grows correctly after each pipeline run (one new embedding per newly accepted record)
- CI runs complete without errors related to the vector store

