"""Extract Theme Terms Module.

Extracts positive and negative phrases from accepted/rejected records
for theme memory learning.
"""

import re
import string
from collections import Counter
from typing import Any

# Common English stopwords
STOPWORDS = {
    "a",
    "an",
    "the",
    "and",
    "or",
    "but",
    "in",
    "on",
    "at",
    "to",
    "for",
    "of",
    "with",
    "by",
    "from",
    "as",
    "is",
    "was",
    "are",
    "were",
    "been",
    "be",
    "have",
    "has",
    "had",
    "do",
    "does",
    "did",
    "will",
    "would",
    "could",
    "should",
    "may",
    "might",
    "must",
    "shall",
    "can",
    "need",
    "dare",
    "ought",
    "used",
    "it",
    "its",
    "this",
    "that",
    "these",
    "those",
    "i",
    "you",
    "he",
    "she",
    "we",
    "they",
    "what",
    "which",
    "who",
    "whom",
    "whose",
    "where",
    "when",
    "why",
    "how",
    "all",
    "each",
    "every",
    "both",
    "few",
    "more",
    "most",
    "other",
    "some",
    "such",
    "no",
    "nor",
    "not",
    "only",
    "own",
    "same",
    "so",
    "than",
    "too",
    "very",
    "just",
    "also",
    "now",
    "here",
    "there",
    "then",
    "once",
    "if",
    "because",
    "until",
    "while",
    "about",
    "against",
    "between",
    "into",
    "through",
    "during",
    "before",
    "after",
    "above",
    "below",
    "up",
    "down",
    "out",
    "off",
    "over",
    "under",
    "again",
    "further",
    "any",
    "being",
    "having",
    "doing",
    "etc",
    "via",
    "ie",
    "eg",
}

# Fields to extract text from in each record
TEXT_FIELDS = [
    "title",
    "summary",
    "why_it_matters",
    "market_structure_context",
    "macro_context",
    "tags",
]


def extract_text_from_record(record: dict[str, Any]) -> str:
    """Extract relevant text from accepted/rejected record.

    Args:
        record: A record dictionary containing relevant fields

    Returns:
        Concatenated text from all relevant fields
    """
    text_parts = []

    for field in TEXT_FIELDS:
        if field in record:
            value = record[field]
            if isinstance(value, list):
                # Tags field is a list
                text_parts.append(" ".join(value))
            elif isinstance(value, str):
                text_parts.append(value)

    return " ".join(text_parts)


def tokenize_and_clean(text: str) -> list[str]:
    """Tokenize and clean text.

    Args:
        text: Input text to tokenize

    Returns:
        List of cleaned tokens (lowercase, no punctuation, no stopwords)
    """
    # Lowercase
    text = text.lower()

    # Remove punctuation except hyphens and underscores (for multi-word terms)
    translator = str.maketrans(
        string.punctuation, " " + " " * (len(string.punctuation) - 1)
    )
    text = text.translate(translator)

    # Split into tokens
    tokens = text.split()

    # Remove stopwords and single characters
    tokens = [t for t in tokens if t not in STOPWORDS and len(t) > 1]

    return tokens


def extract_ngrams(tokens: list[str], n_range: tuple[int, int] = (1, 3)) -> set[str]:
    """Extract n-grams from tokens.

    Args:
        tokens: List of tokens
        n_range: Tuple of (min_n, max_n) for n-gram sizes

    Returns:
        Set of n-gram strings
    """
    min_n, max_n = n_range
    ngrams = set()

    # Ensure min_n is at least 1
    min_n = max(1, min_n)

    for n in range(min_n, max_n + 1):
        for i in range(len(tokens) - n + 1):
            ngram = " ".join(tokens[i : i + n])
            ngrams.add(ngram)

    return ngrams


def calculate_term_frequency(
    records: list[dict[str, Any]], term_set: set[str]
) -> dict[str, int]:
    """Calculate frequency of terms across records.

    Args:
        records: List of records to analyze (each with fields like title, summary, etc.
                 OR simple records with a 'text' field)
        term_set: Set of terms to count

    Returns:
        Dictionary mapping term to frequency count
    """
    freq = {term: 0 for term in term_set}

    for record in records:
        # Support both structured records and simple text records
        if "text" in record:
            text = record["text"]
        else:
            text = extract_text_from_record(record)

        tokens = tokenize_and_clean(text)
        text_lower = " ".join(tokens).lower()

        for term in term_set:
            # Count occurrences of term (as whole word)
            pattern = r"\b" + re.escape(term) + r"\b"
            freq[term] += len(re.findall(pattern, text_lower))

    return freq


def calculate_cooccurrence(
    records: list[dict[str, Any]], terms: list[str]
) -> dict[str, dict[str, int]]:
    """Calculate co-occurrence matrix for terms.

    Args:
        records: List of records to analyze (each with fields like title, summary, etc.
                 OR simple records with a 'text' field)
        terms: List of terms to check for co-occurrence

    Returns:
        2D dictionary where matrix[a][b] = count of co-occurrences of a and b
    """
    # Initialize matrix
    matrix = {term: {t: 0 for t in terms} for term in terms}

    for record in records:
        # Support both structured records and simple text records
        if "text" in record:
            text = record["text"]
        else:
            text = extract_text_from_record(record)

        tokens = tokenize_and_clean(text)
        ngrams = extract_ngrams(tokens, n_range=(1, 3))
        text_lower = text.lower()

        # Find which terms appear in this record
        present_terms = set()
        for term in terms:
            if term in ngrams or term in text_lower:
                present_terms.add(term)

        # Increment co-occurrence counts
        for term_a in present_terms:
            for term_b in present_terms:
                matrix[term_a][term_b] += 1

    return matrix


def extract_positive_candidates(
    records: list[dict[str, Any]], min_occurrences: int = 5
) -> list[str]:
    """Extract terms that appear frequently in accepted records.

    Args:
        records: List of accepted records
        min_occurrences: Minimum number of occurrences to include term

    Returns:
        List of candidate positive terms (unigrams and bigrams)
    """
    if not records:
        return []

    # Collect all n-grams from all records
    all_ngrams = Counter()

    for record in records:
        text = extract_text_from_record(record)
        tokens = tokenize_and_clean(text)
        ngrams = extract_ngrams(tokens, n_range=(1, 3))
        all_ngrams.update(ngrams)

    # Filter by minimum occurrences
    candidates = [
        term for term, count in all_ngrams.items() if count >= min_occurrences
    ]

    # Sort by frequency (descending)
    candidates.sort(key=lambda t: all_ngrams[t], reverse=True)

    return candidates


def extract_negative_candidates(
    records: list[dict[str, Any]], min_occurrences: int = 3
) -> list[str]:
    """Extract terms that appear frequently in rejected records.

    Args:
        records: List of rejected records
        min_occurrences: Minimum number of occurrences to include term

    Returns:
        List of candidate negative terms
    """
    if not records:
        return []

    # Collect all n-grams from all records
    all_ngrams = Counter()

    for record in records:
        text = extract_text_from_record(record)
        tokens = tokenize_and_clean(text)
        ngrams = extract_ngrams(tokens, n_range=(1, 2))
        all_ngrams.update(ngrams)

    # Filter by minimum occurrences
    candidates = [
        term for term, count in all_ngrams.items() if count >= min_occurrences
    ]

    # Sort by frequency (descending)
    candidates.sort(key=lambda t: all_ngrams[t], reverse=True)

    return candidates


# =============================================================================
# CLI Interface
# =============================================================================


def main():
    """Main entry point for CLI usage."""
    import argparse
    import json
    from pathlib import Path

    parser = argparse.ArgumentParser(
        description="Extract theme terms from accepted/rejected records"
    )
    parser.add_argument(
        "--accepted-dir",
        type=Path,
        help="Directory containing accepted records JSON files",
    )
    parser.add_argument(
        "--rejected-dir",
        type=Path,
        help="Directory containing rejected records JSON files",
    )
    parser.add_argument(
        "--min-positive-occurrences",
        type=int,
        default=5,
        help="Minimum occurrences for positive candidates",
    )
    parser.add_argument(
        "--min-negative-occurrences",
        type=int,
        default=3,
        help="Minimum occurrences for negative candidates",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output file for extracted terms",
    )

    args = parser.parse_args()

    # Load records
    accepted_records = []
    rejected_records = []

    if args.accepted_dir and args.accepted_dir.exists():
        for json_file in args.accepted_dir.glob("*.json"):
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    accepted_records.extend(data)
                elif isinstance(data, dict) and "records" in data:
                    accepted_records.extend(data["records"])

    if args.rejected_dir and args.rejected_dir.exists():
        for json_file in args.rejected_dir.glob("*.json"):
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    rejected_records.extend(data)
                elif isinstance(data, dict) and "records" in data:
                    rejected_records.extend(data["records"])

    # Extract candidates
    positive = extract_positive_candidates(
        accepted_records, min_occurrences=args.min_positive_occurrences
    )
    negative = extract_negative_candidates(
        rejected_records, min_occurrences=args.min_negative_occurrences
    )

    result = {
        "positive_candidates": positive,
        "negative_candidates": negative,
        "stats": {
            "accepted_records_analyzed": len(accepted_records),
            "rejected_records_analyzed": len(rejected_records),
            "positive_candidates_found": len(positive),
            "negative_candidates_found": len(negative),
        },
    }

    # Output
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)
        print(f"Results written to {args.output}")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
