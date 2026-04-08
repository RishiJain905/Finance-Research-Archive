# Mac Mini RAG Setup Guide

**For the AI assistant reading this:** This document gives you everything you need to set up and run a local RAG (Retrieval-Augmented Generation) system against the Finance Research Archive on this machine. Read it fully before taking any action. All commands are for macOS with Apple Silicon (M-series chip).

---

## What This Repo Is

This is a continuously updated financial research archive. A GitHub Actions pipeline runs on a schedule and ingests articles, central bank publications, SEC filings, academic papers, and macroeconomic data from dozens of sources. Each piece of content is summarized, scored, and stored as a structured JSON record.

The archive has two layers relevant to RAG:

1. **`data/accepted/`** — structured JSON records, one file per accepted research item. These are the source documents for RAG context.
2. **`data/vector_store/`** — a ChromaDB persistent vector store containing embeddings of every accepted record. This is what you query for semantic search.

Your job is to wire these two layers to a local LLM so the user can ask finance questions and get answers grounded in the archive.

---

## Record Format

Every file in `data/accepted/` is a JSON object. Here are the fields you will use as RAG context:

```json
{
  "id": "unique_record_id",
  "title": "Full title of the publication or article",
  "summary": "2-4 paragraph LLM-generated summary of the content",
  "key_points": ["Bullet 1", "Bullet 2", "..."],
  "why_it_matters": "1-2 sentences on market/macro relevance",
  "source": {
    "name": "Federal Reserve Board",
    "url": "https://...",
    "published_at": "2026-03-21",
    "source_type": "speech"
  },
  "topic": "macro catalysts",
  "event_type": "speech_neutral",
  "tags": ["fed", "powell", "rates"],
  "market_impact": {
    "directional_bias": "neutral",
    "confidence": 0
  },
  "important_numbers": [],
  "quality_tier": {
    "tier": "tier_1",
    "score": 82.5
  }
}
```

When you build RAG context to inject into the prompt, format each retrieved record like this:

```
[Source: {source.name} | {source.published_at} | {source.source_type}]
Title: {title}
Summary: {summary}
Key points: {key_points joined with newlines}
Why it matters: {why_it_matters}
URL: {source.url}
```

---

## Prerequisites

- Mac with Apple Silicon (M1/M2/M3/M4)
- macOS Sonoma or later
- Python 3.11+ — check with `python3 --version`
- At least 32GB RAM recommended for a 27–32B parameter model at 4-bit quantization
- The repo is already cloned or pulled locally

If Python 3.11+ is not installed:
```bash
brew install python@3.11
```

---

## Step 1 — Install Python Dependencies

From the repo root:

```bash
pip install -r requirements.txt
```

Then install the additional packages needed only for local RAG (not needed in CI):

```bash
pip install chromadb sentence-transformers mlx-lm
```

- `chromadb` — reads the vector store in `data/vector_store/`
- `sentence-transformers` — runs the embedding model locally (same model used to build the DB in CI)
- `mlx-lm` — loads and runs LLMs in MLX format on Apple Silicon

---

## Step 2 — Verify the Vector Store Exists

```bash
ls data/vector_store/
```

You should see a `chroma.sqlite3` file and one or more UUID-named subdirectories. If the directory is empty or missing, the CI pipeline has not yet run Phase 8. In that case, run the backfill script to build the vector store from existing accepted records:

```bash
python scripts/backfill_vector_store.py
```

This will take a few minutes on first run as it embeds every record in `data/accepted/`.

---

## Step 3 — The Embedding Model (Critical — Do Not Change)

The vector store was built using `nomic-ai/nomic-embed-text-v1` via the `sentence-transformers` library. **You must use this exact same model to embed queries** — otherwise similarity search returns meaningless results because the vectors live in different geometric spaces.

Verify the model downloads correctly:

```python
from sentence_transformers import SentenceTransformer
model = SentenceTransformer("nomic-ai/nomic-embed-text-v1", trust_remote_code=True)
test = model.encode("federal reserve interest rate", normalize_embeddings=True)
print(f"Embedding shape: {test.shape}")  # Should print: Embedding shape: (768,)
```

The model is ~270MB and downloads once to `~/.cache/huggingface/`.

---

## Step 4 — Download the Local LLM

### Option A — Qwen (MLX, recommended for Apple Silicon)

MLX models run natively on the Apple Silicon Neural Engine and are significantly faster than standard HuggingFace models on Mac.

Find the latest Qwen MLX model on HuggingFace by searching `mlx-community/Qwen`. As of writing, good options are:

```bash
# Qwen 2.5 32B at 4-bit (fits in 32GB RAM, fast)
python -c "from mlx_lm import load; load('mlx-community/Qwen2.5-32B-Instruct-4bit')"

# Qwen 3 27B or 32B dense (use whatever mlx-community has published)
python -c "from mlx_lm import load; load('mlx-community/Qwen3-30B-A3B-4bit')"
```

The first call downloads the model weights to `~/.cache/huggingface/`. This is a one-time download of ~18–20GB. Pick the version that fits your RAM:

| Model | Approx size (4-bit) | Min RAM |
|-------|---------------------|---------|
| Qwen2.5-14B-Instruct-4bit | ~9GB | 16GB |
| Qwen2.5-32B-Instruct-4bit | ~18GB | 32GB |
| Qwen3-30B-A3B-4bit | ~18GB | 32GB |

To find the exact latest model name, browse: `https://huggingface.co/mlx-community?search=Qwen`

### Option B — Ollama (simpler setup, slightly slower)

If you prefer Ollama:

```bash
brew install ollama
ollama serve &
ollama pull qwen2.5:32b
```

Ollama also supports `nomic-embed-text` for embeddings if you want everything through one tool:
```bash
ollama pull nomic-embed-text
```

Note: if you use Ollama for embeddings instead of `sentence-transformers`, the vector store in `data/vector_store/` must have been built with Ollama's `nomic-embed-text` too. The CI pipeline uses `sentence-transformers` — so stick with `sentence-transformers` for querying unless you rebuild the store.

### Option C — HuggingFace Transformers (non-MLX)

Only use this if you need a model that does not have an MLX community version:

```bash
pip install transformers accelerate bitsandbytes
```

```python
from transformers import AutoModelForCausalLM, AutoTokenizer
model = AutoModelForCausalLM.from_pretrained("Qwen/Qwen2.5-32B-Instruct", device_map="auto", load_in_4bit=True)
```

This is slower than MLX on Apple Silicon. Use MLX when available.

---

## Step 5 — The RAG Script

Save this as `scripts/rag_query.py` in the repo. It is a complete, runnable RAG interface:

```python
"""
Local RAG query interface for the Finance Research Archive.
Usage: python scripts/rag_query.py "your finance question here"
       python scripts/rag_query.py  (interactive mode)
"""

import json
import sys
from pathlib import Path

import chromadb
from sentence_transformers import SentenceTransformer

# ── Configuration ──────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).parent.parent
VECTOR_STORE_PATH = REPO_ROOT / "data" / "vector_store"
ACCEPTED_PATH = REPO_ROOT / "data" / "accepted"
EMBEDDING_MODEL = "nomic-ai/nomic-embed-text-v1"
N_RESULTS = 5          # number of records to retrieve per query
COLLECTION_NAME = "finance_archive"

# Set your model backend here: "mlx", "ollama", or "transformers"
MODEL_BACKEND = "mlx"
MLX_MODEL_ID = "mlx-community/Qwen2.5-32B-Instruct-4bit"   # change to your downloaded model
OLLAMA_MODEL = "qwen2.5:32b"


# ── Embedding ──────────────────────────────────────────────────────────────────

_embed_model = None

def get_embedder() -> SentenceTransformer:
    global _embed_model
    if _embed_model is None:
        print("Loading embedding model...")
        _embed_model = SentenceTransformer(EMBEDDING_MODEL, trust_remote_code=True)
    return _embed_model

def embed_query(text: str) -> list[float]:
    return get_embedder().encode(text, normalize_embeddings=True).tolist()


# ── Vector Store ───────────────────────────────────────────────────────────────

_chroma_client = None
_collection = None

def get_collection():
    global _chroma_client, _collection
    if _collection is None:
        _chroma_client = chromadb.PersistentClient(path=str(VECTOR_STORE_PATH))
        _collection = _chroma_client.get_collection(COLLECTION_NAME)
    return _collection

def retrieve(query: str, n: int = N_RESULTS) -> list[dict]:
    """Embed the query and return the top-n most relevant records."""
    query_vec = embed_query(query)
    results = get_collection().query(query_embeddings=[query_vec], n_results=n)

    records = []
    for i, record_id in enumerate(results["ids"][0]):
        # Load the full JSON record from data/accepted/
        record_path = ACCEPTED_PATH / f"{record_id}.json"
        if record_path.exists():
            with open(record_path, encoding="utf-8") as f:
                records.append(json.load(f))
        else:
            # Fallback to metadata stored in ChromaDB
            records.append({"id": record_id, **results["metadatas"][0][i]})
    return records

def format_context(records: list[dict]) -> str:
    """Format retrieved records into a context block for the LLM prompt."""
    parts = []
    for r in records:
        src = r.get("source", {})
        key_points = r.get("key_points", [])
        kp_text = "\n".join(f"  - {p}" for p in key_points) if key_points else "  (none)"
        parts.append(
            f"[Source: {src.get('name','Unknown')} | {src.get('published_at','?')} | {src.get('source_type','?')}]\n"
            f"Title: {r.get('title','')}\n"
            f"Summary: {r.get('summary','')}\n"
            f"Key points:\n{kp_text}\n"
            f"Why it matters: {r.get('why_it_matters','')}\n"
            f"URL: {src.get('url','')}"
        )
    return "\n\n---\n\n".join(parts)


# ── LLM Backends ──────────────────────────────────────────────────────────────

def ask_mlx(prompt: str) -> str:
    from mlx_lm import load, generate
    print(f"Loading MLX model: {MLX_MODEL_ID} ...")
    model, tokenizer = load(MLX_MODEL_ID)
    response = generate(model, tokenizer, prompt=prompt, max_tokens=1024, verbose=False)
    return response

def ask_ollama(prompt: str) -> str:
    import requests
    response = requests.post(
        "http://localhost:11434/api/generate",
        json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
        timeout=120,
    )
    response.raise_for_status()
    return response.json()["response"]

def ask_transformers(prompt: str) -> str:
    from transformers import pipeline
    pipe = pipeline("text-generation", model="Qwen/Qwen2.5-32B-Instruct",
                    device_map="auto", max_new_tokens=1024)
    return pipe(prompt)[0]["generated_text"][len(prompt):]

def ask_llm(prompt: str) -> str:
    if MODEL_BACKEND == "mlx":
        return ask_mlx(prompt)
    elif MODEL_BACKEND == "ollama":
        return ask_ollama(prompt)
    elif MODEL_BACKEND == "transformers":
        return ask_transformers(prompt)
    else:
        raise ValueError(f"Unknown MODEL_BACKEND: {MODEL_BACKEND}")


# ── RAG Pipeline ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a financial research assistant with access to a curated archive of central bank publications, economic research, SEC filings, and macroeconomic analysis. Answer the user's question using ONLY the provided archive context. If the context does not contain enough information to answer confidently, say so clearly. Always cite the source name and date for any claim you make."""

def rag_query(question: str) -> str:
    print(f"\nRetrieving relevant records for: \"{question}\"")
    records = retrieve(question)
    print(f"Retrieved {len(records)} records.")

    context = format_context(records)

    full_prompt = (
        f"{SYSTEM_PROMPT}\n\n"
        f"=== ARCHIVE CONTEXT ===\n\n{context}\n\n"
        f"=== END CONTEXT ===\n\n"
        f"Question: {question}\n\n"
        f"Answer:"
    )

    print("Querying local model...\n")
    answer = ask_llm(full_prompt)
    return answer


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
        print(rag_query(question))
    else:
        print("Finance Research Archive — Local RAG")
        print(f"Model: {MODEL_BACKEND} | Embedding: {EMBEDDING_MODEL}")
        print("Type 'quit' to exit.\n")
        while True:
            try:
                question = input("Question: ").strip()
            except (KeyboardInterrupt, EOFError):
                break
            if question.lower() in ("quit", "exit", "q"):
                break
            if question:
                print(rag_query(question))
                print()
```

---

## Step 6 — First Run

```bash
# Single question
python scripts/rag_query.py "What is the Federal Reserve's current stance on interest rates?"

# Interactive mode
python scripts/rag_query.py
```

Expected output flow:
1. "Loading embedding model..." — downloads/loads `nomic-embed-text-v1` (~5s first time)
2. "Retrieving relevant records..." — ChromaDB similarity search (~1s)
3. "Retrieved 5 records." — shows how many context docs were found
4. "Loading MLX model: ..." — loads model weights into unified memory (~20-40s first time, faster after)
5. "Querying local model..." — inference begins
6. Answer printed to terminal

---

## Step 7 — Switching Model Versions

Edit the top of `scripts/rag_query.py`:

```python
# For a different MLX model — find the exact ID at https://huggingface.co/mlx-community
MODEL_BACKEND = "mlx"
MLX_MODEL_ID = "mlx-community/Qwen3-30B-A3B-4bit"   # or whatever you downloaded

# For Ollama
MODEL_BACKEND = "ollama"
OLLAMA_MODEL = "qwen2.5:32b"
```

No other changes needed.

---

## Step 8 — Keeping the Archive Fresh

The archive is updated by GitHub Actions automatically. To get the latest records and embeddings on your Mac mini:

```bash
git pull origin main
```

That's it. The `data/vector_store/` directory is committed to the repo and updates alongside `data/accepted/`.

If you want to verify the store is in sync after pulling:

```bash
python -c "
import chromadb
db = chromadb.PersistentClient(path='data/vector_store/')
col = db.get_collection('finance_archive')
print(f'Vector store contains {col.count()} embedded records')
"
```

---

## Troubleshooting

### "Collection 'finance_archive' does not exist"

The vector store has not been built yet. Run:
```bash
python scripts/backfill_vector_store.py
```

### Similarity scores are very low / answers are irrelevant

The vector store was built with a different embedding model than what `rag_query.py` is using. Check that `EMBEDDING_MODEL` in `rag_query.py` matches exactly what was used in `scripts/vector_store.py` in the CI pipeline. Both must be `"nomic-ai/nomic-embed-text-v1"`.

### MLX model runs out of memory

Switch to a smaller quantization or smaller parameter count:
```python
MLX_MODEL_ID = "mlx-community/Qwen2.5-14B-Instruct-4bit"   # half the size
```

### Ollama connection refused

Make sure Ollama is running:
```bash
ollama serve
```

### Model is slow on first token

This is normal — the model weights are being paged into unified memory. After the first query, subsequent queries are faster. MLX keeps the model resident in memory for the duration of the process.

---

## Archive Statistics (for context)

| Location | Contents |
|----------|----------|
| `data/accepted/` | Accepted research records (JSON, one file per record) |
| `data/review_queue/` | Records pending human review |
| `data/rejected/` | Rejected records (kept for audit) |
| `data/raw/` | Pre-processed raw fetches |
| `data/vector_store/` | ChromaDB persistent vector index |
| `config/` | Pipeline configuration (sources, scoring, keywords) |
| `scripts/` | All pipeline and utility scripts |

The archive covers: Federal Reserve, ECB, Bank of England, Bank of Canada, RBA, Bank of Japan, BIS, IMF, World Bank, US Treasury, SEC EDGAR filings, arXiv quantitative finance papers, SSRN working papers, and major financial news sources.
