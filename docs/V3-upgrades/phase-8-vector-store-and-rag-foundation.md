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

The embedding calls use a **local sentence-transformers model** (recommended — see "Where the DB Lives and Mac Mini Handoff" below for why this matters more than it might seem).

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

## Where the DB Lives and Mac Mini Handoff

### Storage location

ChromaDB persists to `data/vector_store/` inside this git repository. It is a directory of binary vector files and a small SQLite index. GitHub Actions builds it incrementally — every pipeline run adds new embeddings and pushes the updated directory to the repo alongside `data/accepted/`.

On your Mac mini, all you do is `git pull`. The full vector store is right there in `data/vector_store/`, ready to use with zero migration steps.

### The critical embedding model constraint

This is the most important design decision in this entire phase:

**The embedding model used to build the DB in CI must be the exact same model used to query it on the Mac mini.** ChromaDB stores raw float vectors. If CI generated them with MiniMax's cloud embedding API and your Mac mini queries with a different local model, the dot products are meaningless — you will get completely wrong similarity results.

There are two valid approaches:


| Approach                                 | CI embeddings                                    | Mac mini embeddings                                          | Verdict                                                            |
| ---------------------------------------- | ------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------------ |
| **Cloud API everywhere**                 | MiniMax API                                      | MiniMax API (called from Mac mini)                           | Works, but requires internet + API calls on Mac mini at query time |
| **Local model everywhere (recommended)** | `sentence-transformers` on GitHub Actions runner | Same model via Ollama or `sentence-transformers` on Mac mini | Fully offline on Mac mini, zero API cost, portable                 |


### Recommended embedding model: `nomic-embed-text`

Use `nomic-embed-text` as the single embedding model for both CI and Mac mini:

- GitHub Actions: install via `sentence-transformers` — runs on CPU, no GPU needed, fits in the free runner
- Mac mini: available natively in [Ollama](https://ollama.com) — `ollama pull nomic-embed-text`
- Same vectors, same cosine space, full compatibility
- 768-dimension embeddings, strong performance on technical/financial text
- Completely free and offline on both ends

In `scripts/vector_store.py`, the embedding function looks like:

```python
from sentence_transformers import SentenceTransformer

_model = None

def _get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer("nomic-ai/nomic-embed-text-v1", trust_remote_code=True)
    return _model

def embed(text: str) -> list[float]:
    return _get_model().encode(text, normalize_embeddings=True).tolist()
```

Add `sentence-transformers` and `torch` (CPU-only) to `requirements.txt`.

### Mac mini RAG setup (future, no code needed now)

When you get the Mac mini and have Ollama running with your chosen local model (e.g. `llama3`, `mistral`, `phi-3`):

1. `git clone` or `git pull` this repository — you now have `data/vector_store/` locally
2. Point ChromaDB at that directory: `chromadb.PersistentClient(path="data/vector_store/")`
3. Use the same `nomic-embed-text` model to embed your query at ask-time
4. ChromaDB returns the top-N most relevant records from `data/accepted/`
5. Pass those records as context to your local model — standard RAG prompt injection

Any RAG framework works for step 3–5: [LlamaIndex](https://www.llamaindex.ai/), [LangChain](https://python.langchain.com/), or a simple hand-rolled prompt. The DB is framework-agnostic.

## Embedding Model Consideration (Legacy Note)

The original plan mentioned using MiniMax for embeddings. **Do not do this** if you plan to use the DB on the Mac mini with a local model. Use `nomic-embed-text` via `sentence-transformers` as described above from the start — re-embedding the entire archive later to switch models is painful.

---

## New GitHub Secrets Required

**Likely none** — embeddings use the existing `OPENAI_API_KEY` and `OPENAI_BASE_URL` secrets already in the workflows.

If you switch to a different embedding provider, secrets for that provider would be needed. Add them the same way as `OPENAI_API_KEY`:

- `Settings → Secrets and variables → Actions → New repository secret`

---

## Files Changed in This Phase


| File                               | Change                                                 |
| ---------------------------------- | ------------------------------------------------------ |
| `requirements.txt`                 | Add `chromadb`, `sentence-transformers`, `torch` (CPU) |
| `scripts/vector_store.py`          | New — ChromaDB wrapper module                          |
| `scripts/route_record.py`          | Call `upsert_record` after acceptance                  |
| `scripts/filter_raw_records.py`    | Add semantic dedup check                               |
| `scripts/backfill_vector_store.py` | New — one-time backfill script                         |
| `scripts/search_archive.py`        | New — CLI search tool                                  |
| `.gitignore`                       | Add ChromaDB temp files                                |


---

## Acceptance Criteria

- Running `backfill_vector_store.py` produces a populated `data/vector_store/` directory
- `search_archive.py "federal reserve rate decision"` returns relevant records with scores above 0.7
- A record with >92% cosine similarity to an existing accepted record is rejected as `semantic_duplicate` before reaching the summarizer
- The vector store size grows correctly after each pipeline run (one new embedding per newly accepted record)
- CI runs complete without errors related to the vector store

