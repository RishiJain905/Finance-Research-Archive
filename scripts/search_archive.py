"""Search Archive — semantic search CLI over the Finance Research Archive.

Queries the ChromaDB vector store using nomic-embed-text-v1 embeddings and
prints the top-N most semantically similar accepted records.

Usage:
    python scripts/search_archive.py "yield curve inversion recession signal"
    python scripts/search_archive.py "federal reserve rate decision" --n 10
"""

import argparse
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Semantic search over the Finance Research Archive."
    )
    parser.add_argument("query", help="Search query text")
    parser.add_argument(
        "--n",
        type=int,
        default=5,
        metavar="N",
        help="Number of results to return (default: 5)",
    )
    args = parser.parse_args()

    from scripts.vector_store import search_similar, get_collection

    collection = get_collection()
    count = collection.count()

    if count == 0:
        print(
            "Vector store is empty. Run `python scripts/backfill_vector_store.py` first."
        )
        sys.exit(1)

    print(f'Searching {count} records for: "{args.query}"\n')

    results = search_similar(args.query, n_results=args.n)

    if not results:
        print("No results found.")
        return

    for rank, result in enumerate(results, start=1):
        meta = result["metadata"]
        title = meta.get("title") or result["id"]
        domain = meta.get("domain", "unknown")
        published_at = meta.get("published_at", "unknown date")
        score = result["score"]

        print(f"{rank}. [{score:.4f}] {title}")
        print(f"   {domain} — {published_at}")
        print(f"   ID: {result['id']}")
        print()


if __name__ == "__main__":
    main()
