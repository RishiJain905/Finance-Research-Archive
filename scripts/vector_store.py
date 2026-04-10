"""Vector Store module — ChromaDB wrapper for the Finance Research Archive.

Provides semantic embedding and similarity search over accepted records using
nomic-ai/nomic-embed-text-v1 (sentence-transformers) and a local ChromaDB
persistent store at data/vector_store/.

The same embedding model must be used for both writes (CI / pipeline) and reads
(Mac mini RAG queries). nomic-embed-text-v1 is available via sentence-transformers
here and via `ollama pull nomic-embed-text` on the Mac mini — identical vector space.

Usage:
    from scripts.vector_store import upsert_record, search_similar, is_semantically_duplicate
"""

import sys
from pathlib import Path
from typing import Any

import chromadb

BASE_DIR = Path(__file__).resolve().parent.parent
VECTOR_STORE_DIR = BASE_DIR / "data" / "vector_store"
COLLECTION_NAME = "finance_archive"

_model = None
_client = None
_collection = None


def _get_model():
    """Lazily load the nomic-embed-text-v1 sentence-transformers model."""
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer

        _model = SentenceTransformer(
            "nomic-ai/nomic-embed-text-v1", trust_remote_code=True
        )
    return _model


def embed(text: str) -> list[float]:
    """Embed a single text string into a normalised float vector.

    Args:
        text: The text to embed.

    Returns:
        List of floats representing the normalised embedding vector.
    """
    model = _get_model()
    return model.encode(text, normalize_embeddings=True).tolist()


def get_collection() -> chromadb.Collection:
    """Return the persistent ChromaDB collection from data/vector_store/.

    Creates the directory and collection on first call. Subsequent calls return
    the cached collection.

    Returns:
        The ChromaDB Collection object for the finance archive.
    """
    global _client, _collection
    if _collection is not None:
        return _collection

    VECTOR_STORE_DIR.mkdir(parents=True, exist_ok=True)
    _client = chromadb.PersistentClient(path=str(VECTOR_STORE_DIR))
    _collection = _client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )
    return _collection


def upsert_record(record_id: str, content: str, metadata: dict[str, Any]) -> None:
    """Embed content and upsert the record into the vector store.

    If a record with the same record_id already exists it is overwritten, so
    this function is safe to call on every pipeline run (idempotent).

    Args:
        record_id: Stable unique identifier for the record (matches the JSON filename stem).
        content:   Text to embed — typically ``title + "\\n\\n" + summary``.
        metadata:  Flat dict of string values stored alongside the embedding
                   (title, domain, published_at, event_type, url).
    """
    if not content.strip():
        return

    # ChromaDB metadata values must be str, int, float, or bool
    clean_meta: dict[str, Any] = {
        k: (v if isinstance(v, (str, int, float, bool)) else str(v))
        for k, v in metadata.items()
        if v is not None
    }

    collection = get_collection()
    embedding = embed(content)
    collection.upsert(
        ids=[record_id],
        embeddings=[embedding],
        documents=[content],
        metadatas=[clean_meta],
    )


def search_similar(query_text: str, n_results: int = 5) -> list[dict[str, Any]]:
    """Return the n most similar records to query_text.

    Args:
        query_text: The search query to embed and compare against the store.
        n_results:  Number of results to return (default 5).

    Returns:
        List of dicts, each containing:
            - ``id``       — record ID
            - ``document`` — stored content string
            - ``metadata`` — stored metadata dict
            - ``distance`` — cosine distance (0 = identical, 1 = orthogonal)
            - ``score``    — similarity score (1 - distance)
    """
    collection = get_collection()
    count = collection.count()
    if count == 0:
        return []

    actual_n = min(n_results, count)
    query_embedding = embed(query_text)
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=actual_n,
        include=["documents", "metadatas", "distances"],
    )

    output = []
    ids = results["ids"][0]
    documents = results["documents"][0]
    metadatas = results["metadatas"][0]
    distances = results["distances"][0]

    for rec_id, doc, meta, dist in zip(ids, documents, metadatas, distances):
        output.append(
            {
                "id": rec_id,
                "document": doc,
                "metadata": meta,
                "distance": dist,
                "score": round(1.0 - dist, 4),
            }
        )

    return output


def is_semantically_duplicate(content: str, threshold: float = 0.92) -> bool:
    """Return True if content is above the similarity threshold vs any stored record.

    Used in filter_raw_records.py to reject candidates that are too similar to
    already-accepted records before they reach the expensive summariser step.

    Args:
        content:   Text to check (raw article body).
        threshold: Cosine similarity threshold (default 0.92). A value of 1.0
                   means identical; lower values cast a wider dedup net.

    Returns:
        True if the most similar stored record has similarity >= threshold.
    """
    collection = get_collection()
    if collection.count() == 0:
        return False

    query_embedding = embed(content)
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=1,
        include=["distances"],
    )

    distances = results["distances"]
    if not distances or not distances[0]:
        return False

    top_distance = distances[0][0]
    similarity = 1.0 - top_distance
    return similarity >= threshold
