"""
Link a single newly accepted record to existing records.

Loads the new record and all other accepted records, finds related records
in the opposite category (article↔quant), and updates both the new record
and any existing records that should link to it.

CLI usage:
    python scripts/link_new_record.py <record_id>
"""

import sys
import json
from pathlib import Path

# Add parent directory to path for imports
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from scripts.link_article_and_quant_records import (
    load_accepted_records,
    is_quant_record,
    find_related_quant_records_for_article,
    find_related_articles_for_quant_record,
    save_json,
)


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python scripts/link_new_record.py <record_id>")

    record_id = sys.argv[1]

    print(f"=== Link New Record: {record_id} ===\n")

    # Load the new record
    accepted_dir = BASE_DIR / "data" / "accepted"
    new_record_path = accepted_dir / f"{record_id}.json"

    if not new_record_path.exists():
        raise SystemExit(f"Record not found: {new_record_path}")

    with new_record_path.open("r", encoding="utf-8") as f:
        new_record = json.load(f)

    # Load all other accepted records
    all_articles, all_quants = load_accepted_records(BASE_DIR)

    # Separate other records from the new one
    other_articles = [a for a in all_articles if a.get("id") != record_id]
    other_quants = [q for q in all_quants if q.get("id") != record_id]

    print(f"Found {len(other_articles)} other accepted articles")
    print(f"Found {len(other_quants)} other accepted quants")

    if not other_articles and not other_quants:
        print("\nNo other records to link against. Exiting.")
        return

    # Determine record type
    if is_quant_record(new_record):
        record_type = "quant"
        print(f"\nNew record type: quant")
    else:
        record_type = "article"
        print(f"\nNew record type: article")

    # Find related records and track updates
    updated_records = []
    new_links_from_new = []
    new_links_to_new = []

    if record_type == "article":
        # Find quant records related to the new article
        if other_quants:
            related_quants = find_related_quant_records_for_article(
                new_record, other_quants, top_n=3, min_score=50.0
            )
            new_links_from_new = related_quants

            if related_quants:
                # Initialize linked_quant_context on new record
                if "linked_quant_context" not in new_record:
                    new_record["linked_quant_context"] = []

                existing_ids = {
                    link["record_id"] for link in new_record["linked_quant_context"]
                }
                for link in related_quants:
                    if link["record_id"] not in existing_ids:
                        new_record["linked_quant_context"].append(link)
                        updated_records.append(new_record)

            # Find which existing quants should link back to this new article
            for quant in all_quants:
                if quant.get("id") == record_id:
                    continue

                # Check if this quant would link to the new article
                related_articles = find_related_articles_for_quant_record(
                    quant, [new_record] + other_articles, top_n=3, min_score=50.0
                )

                # Check if the new article is in the top results for this quant
                new_article_links = [
                    link for link in related_articles if link["record_id"] == record_id
                ]

                if new_article_links and quant.get("id") != record_id:
                    # Initialize linked_article_context if needed
                    if "linked_article_context" not in quant:
                        quant["linked_article_context"] = []

                    existing_ids = {
                        link["record_id"] for link in quant["linked_article_context"]
                    }
                    for link in new_article_links:
                        if link["record_id"] not in existing_ids:
                            quant["linked_article_context"].append(link)
                            if quant not in updated_records:
                                updated_records.append(quant)
                            new_links_to_new.append(link)

    else:  # quant record
        # Find article records related to the new quant
        if other_articles:
            related_articles = find_related_articles_for_quant_record(
                new_record, other_articles, top_n=3, min_score=50.0
            )
            new_links_from_new = related_articles

            if related_articles:
                # Initialize linked_article_context on new record
                if "linked_article_context" not in new_record:
                    new_record["linked_article_context"] = []

                existing_ids = {
                    link["record_id"] for link in new_record["linked_article_context"]
                }
                for link in related_articles:
                    if link["record_id"] not in existing_ids:
                        new_record["linked_article_context"].append(link)
                        updated_records.append(new_record)

            # Find which existing articles should link back to this new quant
            for article in all_articles:
                if article.get("id") == record_id:
                    continue

                # Check if this article would link to the new quant
                related_quants = find_related_quant_records_for_article(
                    article, [new_record] + other_quants, top_n=3, min_score=50.0
                )

                # Check if the new quant is in the top results for this article
                new_quant_links = [
                    link for link in related_quants if link["record_id"] == record_id
                ]

                if new_quant_links and article.get("id") != record_id:
                    # Initialize linked_quant_context if needed
                    if "linked_quant_context" not in article:
                        article["linked_quant_context"] = []

                    existing_ids = {
                        link["record_id"] for link in article["linked_quant_context"]
                    }
                    for link in new_quant_links:
                        if link["record_id"] not in existing_ids:
                            article["linked_quant_context"].append(link)
                            if article not in updated_records:
                                updated_records.append(article)
                            new_links_to_new.append(link)

    # Report linking results
    if record_type == "article":
        link_type = "quant"
    else:
        link_type = "article"

    if new_links_from_new:
        print(
            f"\nCreated {len(new_links_from_new)} links from {record_id} to {link_type} records"
        )
        for link in new_links_from_new:
            print(
                f"  → {link['record_id']} (score: {link['link_score']}, {link['relationship']})"
            )
    else:
        print(f"\nCreated 0 links from {record_id} to {link_type} records")

    if new_links_to_new:
        print(
            f"\nCreated {len(new_links_to_new)} links from existing records to {record_id}"
        )
        for link in new_links_to_new:
            print(
                f"  ← {link['record_id']} (score: {link['link_score']}, {link['relationship']})"
            )
    else:
        print(f"\nCreated 0 links from existing records to {record_id}")

    # Write updated records
    if updated_records:
        print("\nUpdated files:")
        for record in updated_records:
            record_path = accepted_dir / f"{record['id']}.json"
            save_json(record_path, record)
            print(f"  - {record_path.relative_to(BASE_DIR)}")
    else:
        print("\nNo files updated (no new links found).")


if __name__ == "__main__":
    main()
