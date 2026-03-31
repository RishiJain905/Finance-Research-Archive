"""
Backfill script for bidirectional linking of all existing accepted records.

Loads all accepted records, separates articles vs quants, runs bidirectional
linking, and updates each record file in-place with linked context fields.

CLI usage:
    python scripts/backfill_linked_records.py
    python scripts/backfill_linked_records.py --dry-run
    python scripts/backfill_linked_records.py --min-score 60.0
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from scripts.link_article_and_quant_records import (
    load_accepted_records,
    link_all_records,
    is_quant_record,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill bidirectional links for all accepted records."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be linked without writing files",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=50.0,
        help="Minimum link score threshold (default: 50.0)",
    )
    args = parser.parse_args()

    print("=== Backfill Linked Records ===\n")
    print("Loading accepted records...")

    articles, quants = load_accepted_records(BASE_DIR)

    print(f"Found {len(articles)} article records")
    print(f"Found {len(quants)} quant records\n")

    # Handle edge cases
    if not articles and not quants:
        print("No records found. Exiting.")
        return

    if not articles:
        print("No article records found. Nothing to link.")
        return

    if not quants:
        print("No quant records found. Links will appear after quant pipeline runs.")
        print("\n=== Results ===")
        print(f"Articles processed: {len(articles)}")
        print(f"Quants processed: 0")
        print(f"Articles with new links: 0")
        print(f"Quants with new links: 0")
        print(f"Total new links added: 0")
        return

    print("Running bidirectional linking...\n")

    # Build per-record statistics for dry-run
    if args.dry_run:
        _dry_run_report(articles, quants, args.min_score)
    else:
        _run_linking(articles, quants)


def _dry_run_report(articles: list[dict], quants: list[dict], min_score: float) -> None:
    """Show what links would be created without writing files."""
    from scripts.link_article_and_quant_records import (
        find_related_quant_records_for_article,
        find_related_articles_for_quant_record,
    )

    print("=== Dry Run - Links to be created ===\n")

    total_article_links = 0
    total_quant_links = 0
    articles_with_links = 0
    quants_with_links = 0

    # Check article -> quant links
    print("Article -> Quant links:")
    for article in articles:
        related = find_related_quant_records_for_article(
            article, quants, top_n=3, min_score=min_score
        )
        if related:
            articles_with_links += 1
            total_article_links += len(related)
            print(f"  {article['id']} -> {len(related)} quant(s)")
            for link in related:
                print(f"    -> {link['record_id']} (score: {link['link_score']})")

    print()

    # Check quant -> article links
    print("Quant -> Article links:")
    for quant in quants:
        related = find_related_articles_for_quant_record(
            quant, articles, top_n=3, min_score=min_score
        )
        if related:
            quants_with_links += 1
            total_quant_links += len(related)
            print(f"  {quant['id']} -> {len(related)} article(s)")
            for link in related:
                print(f"    -> {link['record_id']} (score: {link['link_score']})")

    print("\n=== Summary ===")
    print(f"Articles with links: {articles_with_links}/{len(articles)}")
    print(f"Quants with links: {quants_with_links}/{len(quants)}")
    print(f"Total links: {total_article_links + total_quant_links}")


def _run_linking(articles: list[dict], quants: list[dict]) -> None:
    """Run the actual linking and update files."""
    stats = link_all_records(articles, quants, BASE_DIR)

    print("=== Results ===")
    print(f"Articles processed: {stats['total_articles']}")
    print(f"Quants processed: {stats['total_quants']}")
    print(f"Articles with new links: {stats['articles_with_links']}")
    print(f"Quants with new links: {stats['quants_with_links']}")
    print(f"Total new links added: {stats['total_links_added']}")


if __name__ == "__main__":
    main()
