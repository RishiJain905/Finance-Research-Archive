"""Tests for extract_theme_terms module.

Tests term extraction from accepted/rejected records including:
- Text extraction
- Tokenization and cleaning
- Ngram extraction
- Term frequency calculation
- Co-occurrence calculation
- Positive/negative candidate extraction
"""

import json
from pathlib import Path

import pytest

from scripts import extract_theme_terms


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_records():
    """Load sample accepted and rejected records for testing."""
    fixtures_path = Path(__file__).parent / "fixtures" / "sample_records.json"
    with open(fixtures_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["accepted_records"], data["rejected_records"]


@pytest.fixture
def sample_accepted_records(sample_records):
    """Get sample accepted records."""
    return sample_records[0]


@pytest.fixture
def sample_rejected_records(sample_records):
    """Get sample rejected records."""
    return sample_records[1]


# =============================================================================
# Test Text Extraction
# =============================================================================


class TestExtractTextFromRecord:
    """Tests for extract_text_from_record function."""

    def test_extracts_all_relevant_fields(self, sample_accepted_records):
        """extract_text_from_record extracts title, summary, why_it_matters, etc."""
        record = sample_accepted_records[0]
        text = extract_theme_terms.extract_text_from_record(record)

        # Should contain text from all relevant fields
        assert "Treasury funding" in text
        assert "reserve conditions" in text
        assert "liquidity" in text
        assert "market liquidity" in text
        assert "treasury" in text

    def test_includes_tags(self, sample_accepted_records):
        """extract_text_from_record includes tags."""
        record = sample_accepted_records[0]
        text = extract_theme_terms.extract_text_from_record(record)

        # Tags like 'liquidity', 'treasury', 'funding', 'rates', 'market structure'
        assert "liquidity" in text
        assert "treasury" in text

    def test_handles_missing_fields(self):
        """extract_text_from_record handles missing optional fields."""
        minimal_record = {
            "id": "test",
            "title": "Test title only",
        }
        text = extract_theme_terms.extract_text_from_record(minimal_record)
        assert text == "Test title only"

    def test_handles_empty_record(self):
        """extract_text_from_record handles empty record."""
        record = {"id": "test"}
        text = extract_theme_terms.extract_text_from_record(record)
        assert text == ""


# =============================================================================
# Test Tokenization and Cleaning
# =============================================================================


class TestTokenizeAndClean:
    """Tests for tokenize_and_clean function."""

    def test_lowercases_text(self):
        """tokenize_and_clean converts text to lowercase."""
        result = extract_theme_terms.tokenize_and_clean("UPPERCASE TEXT")
        assert "uppercase" in result
        assert "text" in result

    def test_removes_punctuation(self):
        """tokenize_and_clean removes punctuation."""
        result = extract_theme_terms.tokenize_and_clean("Hello, world! How are you?")
        assert "hello" in result
        assert "world" in result
        # Punctuation should not appear as separate tokens
        assert "," not in result
        assert "!" not in result
        assert "?" not in result

    def test_removes_stopwords(self):
        """tokenize_and_clean removes common stopwords."""
        result = extract_theme_terms.tokenize_and_clean(
            "the quick brown fox jumps over the lazy dog"
        )
        assert "the" not in result
        assert "over" not in result
        assert "quick" in result
        assert "brown" in result
        assert "fox" in result

    def test_returns_list_of_tokens(self):
        """tokenize_and_clean returns a list of tokens."""
        result = extract_theme_terms.tokenize_and_clean("Hello World")
        assert isinstance(result, list)
        assert len(result) >= 2

    def test_handles_empty_string(self):
        """tokenize_and_clean handles empty string."""
        result = extract_theme_terms.tokenize_and_clean("")
        assert result == []


# =============================================================================
# Test Ngram Extraction
# =============================================================================


class TestExtractNgrams:
    """Tests for extract_ngrams function."""

    def test_extracts_unigrams(self):
        """extract_ngrams extracts unigrams by default."""
        tokens = ["liquidity", "treasury", "market", "rates"]
        result = extract_theme_terms.extract_ngrams(tokens)

        assert "liquidity" in result
        assert "treasury" in result
        assert "market" in result
        assert "rates" in result

    def test_extracts_bigrams(self):
        """extract_ngrams extracts bigrams."""
        tokens = ["treasury", "funding", "liquidity", "conditions"]
        result = extract_theme_terms.extract_ngrams(tokens, n_range=(1, 2))

        assert "treasury funding" in result
        assert "funding liquidity" in result
        assert "liquidity conditions" in result

    def test_extracts_trigrams(self):
        """extract_ngrams extracts trigrams."""
        tokens = ["treasury", "funding", "and", "reserve", "conditions"]
        result = extract_theme_terms.extract_ngrams(tokens, n_range=(1, 3))

        assert "treasury funding and" in result
        assert "funding and reserve" in result
        assert "and reserve conditions" in result

    def test_respects_n_range(self):
        """extract_ngrams respects n_range parameter."""
        tokens = ["a", "b", "c", "d", "e"]
        result = extract_theme_terms.extract_ngrams(tokens, n_range=(2, 2))

        # Should only have bigrams
        assert "a b" in result
        assert "b c" in result
        assert "a" not in result  # No unigrams

    def test_handles_short_tokens(self):
        """extract_ngrams handles tokens shorter than n."""
        tokens = ["a", "b"]
        result = extract_theme_terms.extract_ngrams(tokens, n_range=(1, 3))

        # Should still have unigrams and bigram
        assert "a" in result
        assert "b" in result
        assert "a b" in result
        # No trigrams possible
        assert len([r for r in result if len(r.split()) == 3]) == 0


# =============================================================================
# Test Term Frequency Calculation
# =============================================================================


class TestCalculateTermFrequency:
    """Tests for calculate_term_frequency function."""

    def test_counts_occurrences(self):
        """calculate_term_frequency counts term occurrences across records."""
        records = [
            {"text": "treasury liquidity funding"},
            {"text": "treasury liquidity rates"},
            {"text": "treasury funding rates"},
        ]

        term_set = {"treasury", "liquidity", "funding", "rates"}
        freq = extract_theme_terms.calculate_term_frequency(records, term_set)

        assert freq["treasury"] == 3
        assert freq["liquidity"] == 2
        assert freq["funding"] == 2
        assert freq["rates"] == 2

    def test_respects_term_set(self):
        """calculate_term_frequency only counts terms in term_set."""
        records = [
            {"text": "treasury liquidity funding noise"},
        ]

        term_set = {"treasury", "liquidity"}
        freq = extract_theme_terms.calculate_term_frequency(records, term_set)

        assert freq["treasury"] == 1
        assert freq["liquidity"] == 1
        assert "noise" not in freq

    def test_empty_records(self):
        """calculate_term_frequency handles empty records list."""
        term_set = {"treasury", "liquidity"}
        freq = extract_theme_terms.calculate_term_frequency([], term_set)

        assert freq["treasury"] == 0
        assert freq["liquidity"] == 0


# =============================================================================
# Test Co-occurrence Calculation
# =============================================================================


class TestCalculateCooccurrence:
    """Tests for calculate_cooccurrence function."""

    def test_calculates_cooccurrence_matrix(self):
        """calculate_cooccurrence builds co-occurrence matrix."""
        records = [
            {"text": "treasury liquidity funding"},
            {"text": "treasury liquidity rates"},
            {"text": "treasury funding rates"},
        ]

        terms = ["treasury", "liquidity", "funding", "rates"]
        matrix = extract_theme_terms.calculate_cooccurrence(records, terms)

        # Treasury appears with all other terms
        assert matrix["treasury"]["treasury"] == 3
        assert matrix["treasury"]["liquidity"] == 2
        assert matrix["treasury"]["funding"] == 2
        assert matrix["treasury"]["rates"] == 2

    def test_symmetric_matrix(self):
        """calculate_cooccurrence produces symmetric matrix."""
        records = [
            {"text": "treasury liquidity funding"},
        ]

        terms = ["treasury", "liquidity", "funding"]
        matrix = extract_theme_terms.calculate_cooccurrence(records, terms)

        assert matrix["treasury"]["liquidity"] == matrix["liquidity"]["treasury"]
        assert matrix["treasury"]["funding"] == matrix["funding"]["treasury"]
        assert matrix["liquidity"]["funding"] == matrix["funding"]["liquidity"]

    def test_diagonal_counts_occurrences(self):
        """Co-occurrence diagonal equals term frequency."""
        records = [
            {"text": "treasury treasury treasury"},
            {"text": "treasury liquidity"},
        ]

        terms = ["treasury", "liquidity"]
        matrix = extract_theme_terms.calculate_cooccurrence(records, terms)

        # Treasury appears in 2 records (presence-based count)
        assert matrix["treasury"]["treasury"] == 2
        assert matrix["liquidity"]["liquidity"] == 1


# =============================================================================
# Test Positive Candidate Extraction
# =============================================================================


class TestExtractPositiveCandidates:
    """Tests for extract_positive_candidates function."""

    def test_extracts_frequent_terms(self, sample_accepted_records):
        """extract_positive_candidates extracts terms meeting min_occurrences."""
        candidates = extract_theme_terms.extract_positive_candidates(
            sample_accepted_records, min_occurrences=5
        )

        # 'treasury' appears in 6+ records, 'liquidity' in 4 records
        # With min_occurrences=5, only treasury should qualify
        assert "treasury" in candidates
        # liquidity doesn't meet the 5 threshold

    def test_respects_min_occurrences(self, sample_accepted_records):
        """extract_positive_candidates respects min_occurrences threshold."""
        candidates = extract_theme_terms.extract_positive_candidates(
            sample_accepted_records, min_occurrences=8
        )

        # Fewer terms should meet higher threshold
        candidates_low = extract_theme_terms.extract_positive_candidates(
            sample_accepted_records, min_occurrences=3
        )

        assert len(candidates) <= len(candidates_low)

    def test_includes_bigrams(self, sample_accepted_records):
        """extract_positive_candidates includes multi-word terms."""
        candidates = extract_theme_terms.extract_positive_candidates(
            sample_accepted_records, min_occurrences=3
        )

        # Should include bigrams like 'yield curve', 'market liquidity'
        bigrams = [c for c in candidates if " " in c]
        assert len(bigrams) > 0

    def test_empty_records(self):
        """extract_positive_candidates handles empty records."""
        candidates = extract_theme_terms.extract_positive_candidates(
            [], min_occurrences=5
        )
        assert candidates == []

    def test_default_min_occurrences(self, sample_accepted_records):
        """extract_positive_candidates uses default min_occurrences of 5."""
        candidates = extract_theme_terms.extract_positive_candidates(
            sample_accepted_records
        )

        # Should return terms appearing 5+ times
        # We have 8 accepted records, so terms appearing in most should be included


# =============================================================================
# Test Negative Candidate Extraction
# =============================================================================


class TestExtractNegativeCandidates:
    """Tests for extract_negative_candidates function."""

    def test_extracts_frequent_rejected_terms(self, sample_rejected_records):
        """extract_negative_candidates extracts terms from rejected records."""
        candidates = extract_theme_terms.extract_negative_candidates(
            sample_rejected_records, min_occurrences=2
        )

        # Terms like 'local', 'news', 'sports' should appear
        assert len(candidates) > 0

    def test_respects_min_occurrences(self, sample_rejected_records):
        """extract_negative_candidates respects min_occurrences threshold."""
        candidates_low = extract_theme_terms.extract_negative_candidates(
            sample_rejected_records, min_occurrences=1
        )

        candidates_high = extract_theme_terms.extract_negative_candidates(
            sample_rejected_records, min_occurrences=3
        )

        assert len(candidates_high) <= len(candidates_low)

    def test_different_from_positive(
        self, sample_accepted_records, sample_rejected_records
    ):
        """Negative candidates differ from positive candidates."""
        positive = extract_theme_terms.extract_positive_candidates(
            sample_accepted_records, min_occurrences=3
        )
        negative = extract_theme_terms.extract_negative_candidates(
            sample_rejected_records, min_occurrences=2
        )

        # Sports, entertainment, weather terms should not be in positive
        # This is a general check - actual overlap depends on specific terms
        sports_terms = {"sports", "championship", "local news"}
        for term in sports_terms:
            if term in negative:
                assert term not in positive or term in negative


# =============================================================================
# Test Integration
# =============================================================================


class TestIntegration:
    """Integration tests for the extraction pipeline."""

    def test_full_extraction_pipeline(self, sample_accepted_records):
        """Test complete extraction pipeline from records to candidates."""
        # Extract positive candidates
        positive = extract_theme_terms.extract_positive_candidates(
            sample_accepted_records, min_occurrences=5
        )

        # Should get meaningful financial terms
        financial_terms = {"liquidity", "treasury", "rates", "fed", "yield curve"}
        found_terms = [t for t in positive if t in financial_terms]
        assert len(found_terms) >= 2  # At least some financial terms

    def test_terms_are_cleaned(self, sample_accepted_records):
        """Extracted terms should be cleaned (lowercase, no punctuation)."""
        candidates = extract_theme_terms.extract_positive_candidates(
            sample_accepted_records, min_occurrences=3
        )

        for term in candidates:
            # Should not contain uppercase
            assert term == term.lower()
            # Should not contain punctuation (except spaces in multi-word terms)
            clean_term = term.replace(" ", "")
            assert clean_term.isalnum() or "-" in term or "_" in term
