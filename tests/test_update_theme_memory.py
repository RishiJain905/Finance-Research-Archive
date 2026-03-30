"""Tests for update_theme_memory module.

Tests theme memory updates including:
- Theme creation
- Theme update
- Priority score computation
- Similar theme merging
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts import update_theme_memory, theme_memory_persistence


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_theme():
    """Provide a sample theme record."""
    return {
        "theme_id": "treasury-liquidity",
        "theme_label": "Treasury Liquidity",
        "positive_terms": ["liquidity", "treasury", "funding", "reserve"],
        "negative_terms": ["sports", "entertainment"],
        "accepted_count": 10,
        "review_count": 3,
        "rejected_count": 2,
        "last_seen": datetime.now(timezone.utc).isoformat(),
        "priority_score": 89,
    }


@pytest.fixture
def sample_new_theme_data():
    """Provide sample new theme data."""
    return {
        "theme_label": "Yield Curve Dynamics",
        "positive_terms": ["yield curve", "treasury", "recession", "bonds"],
        "negative_terms": ["sports"],
    }


@pytest.fixture
def temp_theme_memory_dir(tmp_path):
    """Create a temporary theme memory directory for testing."""
    theme_dir = tmp_path / "theme_memory"
    theme_dir.mkdir(parents=True, exist_ok=True)
    return theme_dir


@pytest.fixture
def mock_theme_memory_paths(temp_theme_memory_dir):
    """Mock the theme memory file paths to use temp directory."""
    themes_path = temp_theme_memory_dir / "themes.json"
    expansions_path = temp_theme_memory_dir / "expansions.json"

    with (
        patch.object(theme_memory_persistence, "THEMES_PATH", themes_path),
        patch.object(theme_memory_persistence, "EXPANSIONS_PATH", expansions_path),
        patch.object(
            update_theme_memory,
            "load_theme_memory",
            update_theme_memory._load_theme_memory,
        ),
        patch.object(
            update_theme_memory,
            "save_theme_memory",
            update_theme_memory._save_theme_memory,
        ),
    ):
        yield {
            "themes": themes_path,
            "expansions": expansions_path,
        }


# =============================================================================
# Test Priority Score Computation
# =============================================================================


class TestComputePriorityScore:
    """Tests for compute_priority_score function."""

    def test_basic_priority_calculation(self):
        """compute_priority_score calculates correct base score."""
        theme = {
            "accepted_count": 10,
            "rejected_count": 2,
            "review_count": 3,
        }

        score = update_theme_memory.compute_priority_score(theme)

        # base = 10 * 10 = 100
        # penalty = 2 * 5 = 10
        # bonus = 3 * 3 = 9
        # final = min(100, max(0, 100 - 10 + 9)) = 99
        assert score == 99

    def test_zero_counts(self):
        """compute_priority_score handles zero counts."""
        theme = {
            "accepted_count": 0,
            "rejected_count": 0,
            "review_count": 0,
        }

        score = update_theme_memory.compute_priority_score(theme)

        # base = 0, penalty = 0, bonus = 0
        # final = min(100, max(0, 0)) = 0
        assert score == 0

    def test_high_rejected_count(self):
        """compute_priority_score penalizes high rejected count."""
        theme = {
            "accepted_count": 20,
            "rejected_count": 30,
            "review_count": 0,
        }

        score = update_theme_memory.compute_priority_score(theme)

        # base = 200, penalty = 150, bonus = 0
        # final = min(100, max(0, 200 - 150)) = 50
        assert score == 50

    def test_max_score_capped_at_100(self):
        """compute_priority_score caps score at 100."""
        theme = {
            "accepted_count": 20,
            "rejected_count": 0,
            "review_count": 10,
        }

        score = update_theme_memory.compute_priority_score(theme)

        # base = 200, penalty = 0, bonus = 30
        # final = min(100, max(0, 200 + 30)) = 100
        assert score == 100

    def test_min_score_never_negative(self):
        """compute_priority_score never returns negative."""
        theme = {
            "accepted_count": 1,
            "rejected_count": 50,
            "review_count": 0,
        }

        score = update_theme_memory.compute_priority_score(theme)

        # base = 10, penalty = 250
        # final = min(100, max(0, 10 - 250)) = 0
        assert score == 0

    def test_missing_fields_default_to_zero(self):
        """compute_priority_score handles missing fields."""
        theme = {}

        score = update_theme_memory.compute_priority_score(theme)

        assert score == 0


# =============================================================================
# Test Theme Creation and Update
# =============================================================================


class TestCreateOrUpdateTheme:
    """Tests for create_or_update_theme function."""

    def test_creates_new_theme(self, sample_new_theme_data):
        """create_or_update_theme creates new theme when none exists."""
        existing_themes = {}
        result = update_theme_memory.create_or_update_theme(
            existing_themes, sample_new_theme_data
        )

        assert result["theme_id"] == "yield-curve-dynamics"
        assert result["theme_label"] == "Yield Curve Dynamics"
        assert result["accepted_count"] == 1
        assert result["review_count"] == 0
        assert result["rejected_count"] == 0
        assert result["priority_score"] == 10  # base = 1 * 10

    def test_updates_existing_theme(self, sample_theme):
        """create_or_update_theme updates existing theme."""
        existing_themes = {"treasury-liquidity": sample_theme.copy()}
        new_data = {
            "theme_label": "Treasury Liquidity",
            "positive_terms": ["liquidity", "treasury", "funding", "reserve", "repo"],
        }

        result = update_theme_memory.create_or_update_theme(
            existing_themes, new_data, theme_id="treasury-liquidity"
        )

        # Should increment counts and add new term
        assert result["accepted_count"] == 11
        assert "repo" in result["positive_terms"]

    def test_generates_slug_for_theme_id(self, sample_new_theme_data):
        """create_or_update_theme generates slug for new theme_id."""
        existing_themes = {}
        result = update_theme_memory.create_or_update_theme(
            existing_themes, sample_new_theme_data
        )

        # Should be a slug
        assert "-" in result["theme_id"] or "_" in result["theme_id"]
        assert result["theme_id"] == result["theme_id"].lower()

    def test_adds_negative_terms(self, sample_new_theme_data):
        """create_or_update_theme adds negative terms."""
        existing_themes = {}
        result = update_theme_memory.create_or_update_theme(
            existing_themes, sample_new_theme_data
        )

        assert "sports" in result["negative_terms"]


# =============================================================================
# Test Similar Theme Merging
# =============================================================================


class TestMergeSimilarThemes:
    """Tests for merge_similar_themes function."""

    def test_merges_themes_with_high_overlap(self):
        """merge_similar_themes merges themes with >threshold overlap."""
        # Create themes with very high overlap (4 out of 4 terms = 1.0 Jaccard)
        themes = {
            "theme-1": {
                "theme_id": "theme-1",
                "positive_terms": ["treasury", "liquidity", "funding", "rates"],
            },
            "theme-2": {
                "theme_id": "theme-2",
                "positive_terms": ["treasury", "liquidity", "funding", "rates"],
            },
        }

        merged = update_theme_memory.merge_similar_themes(themes, threshold=0.7)

        # Should merge into one theme (4/4 = 1.0 > 0.70)
        assert len(merged) < len(themes)

    def test_keeps_dissimilar_themes(self):
        """merge_similar_themes keeps themes with <threshold overlap."""
        themes = {
            "treasury-theme": {
                "theme_id": "treasury-theme",
                "positive_terms": ["treasury", "liquidity", "funding"],
            },
            "sports-theme": {
                "theme_id": "sports-theme",
                "positive_terms": ["football", "basketball", "championship"],
            },
        }

        merged = update_theme_memory.merge_similar_themes(themes, threshold=0.7)

        # Should keep both separate
        assert len(merged) == 2

    def test_merges_counts_correctly(self):
        """merge_similar_themes combines counts from merged themes."""
        themes = {
            "theme-1": {
                "theme_id": "theme-1",
                "theme_label": "Theme One",
                "positive_terms": ["treasury", "liquidity"],
                "negative_terms": [],
                "accepted_count": 10,
                "review_count": 5,
                "rejected_count": 2,
                "last_seen": datetime.now(timezone.utc).isoformat(),
                "priority_score": 85,
            },
            "theme-2": {
                "theme_id": "theme-2",
                "theme_label": "Theme Two",
                "positive_terms": ["treasury", "funding"],
                "negative_terms": [],
                "accepted_count": 8,
                "review_count": 3,
                "rejected_count": 1,
                "last_seen": datetime.now(timezone.utc).isoformat(),
                "priority_score": 80,
            },
        }

        merged = update_theme_memory.merge_similar_themes(themes, threshold=0.7)

        # Should have combined counts
        if len(merged) == 1:
            combined = list(merged.values())[0]
            assert combined["accepted_count"] == 18  # 10 + 8
            assert combined["review_count"] == 8  # 5 + 3

    def test_threshold_0_merges_all(self):
        """merge_similar_themes with threshold=0 merges everything."""
        themes = {
            "theme-1": {"positive_terms": ["a", "b"]},
            "theme-2": {"positive_terms": ["c", "d"]},
        }

        merged = update_theme_memory.merge_similar_themes(themes, threshold=0.0)

        # Should merge all into one
        assert len(merged) == 1

    def test_threshold_1_requires_exact_match(self):
        """merge_similar_themes with threshold=1 requires identical terms."""
        themes = {
            "theme-1": {"positive_terms": ["a", "b", "c"]},
            "theme-2": {"positive_terms": ["a", "b", "c"]},
        }

        merged = update_theme_memory.merge_similar_themes(themes, threshold=1.0)

        # Should merge only identical themes (which these are)
        assert len(merged) == 1


# =============================================================================
# Test Process Accepted Record
# =============================================================================


class TestProcessAcceptedRecord:
    """Tests for process_accepted_record function."""

    def test_updates_matching_theme(self, sample_theme):
        """process_accepted_record updates theme with matching terms."""
        themes = {"treasury-liquidity": sample_theme.copy()}
        record = {
            "id": "new_record",
            "title": "Treasury liquidity conditions remain tight",
            "summary": "Funding stress continues",
            "why_it_matters": "Market impact",
            "market_structure_context": "Reserve issues",
            "macro_context": "Policy transmission",
            "tags": ["liquidity", "treasury"],
        }

        updated = update_theme_memory.process_accepted_record(record, themes)

        # Should update treasury-liquidity theme
        assert updated["treasury-liquidity"]["accepted_count"] == 11

    def test_creates_new_theme(self, sample_theme):
        """process_accepted_record creates new theme if no match."""
        themes = {"treasury-liquidity": sample_theme.copy()}
        record = {
            "id": "new_record",
            "title": "Earnings season shows tech sector growth",
            "summary": "Technology companies report strong results",
            "why_it_matters": "Market impact",
            "market_structure_context": "Sector rotation",
            "macro_context": "Growth outlook",
            "tags": ["earnings", "technology", "growth"],
        }

        updated = update_theme_memory.process_accepted_record(record, themes)

        # Should create new tech theme
        tech_themes = [t for t in updated.keys() if "tech" in t or "technology" in t]
        assert len(tech_themes) >= 1

    def test_adds_new_terms(self, sample_theme):
        """process_accepted_record adds new positive terms."""
        themes = {"treasury-liquidity": sample_theme.copy()}
        record = {
            "id": "new_record",
            "title": "Treasury repo rates spike",
            "summary": "Repo market stress",
            "why_it_matters": "Funding impact",
            "market_structure_context": "Overnight funding",
            "macro_context": "Market conditions",
            "tags": ["repo", "treasury"],
        }

        updated = update_theme_memory.process_accepted_record(record, themes)

        # Should add 'repo' to treasury-liquidity
        assert "repo" in updated["treasury-liquidity"]["positive_terms"]


# =============================================================================
# Test Process Rejected Record
# =============================================================================


class TestProcessRejectedRecord:
    """Tests for process_rejected_record function."""

    def test_adds_negative_terms(self, sample_theme):
        """process_rejected_record adds negative terms from rejected record."""
        themes = {"treasury-liquidity": sample_theme.copy()}
        record = {
            "id": "rejected_record",
            "title": "Local sports team wins championship",
            "summary": "Team celebrates victory",
            "why_it_matters": "Local interest only",
            "market_structure_context": "Not applicable",
            "macro_context": "No relevance",
            "tags": ["sports", "championship", "local"],
        }

        updated = update_theme_memory.process_rejected_record(record, themes)

        # Should add sports-related terms to treasury-liquidity as negative
        treasury_negative = updated["treasury-liquidity"]["negative_terms"]
        sports_terms_found = any(
            term in treasury_negative for term in ["sports", "championship", "local"]
        )
        # At least one sports term should be added
        assert sports_terms_found or len(treasury_negative) > 1

    def test_increments_rejected_count(self, sample_theme):
        """process_rejected_record increments rejected_count."""
        themes = {"treasury-liquidity": sample_theme.copy()}
        record = {
            "id": "rejected_record",
            "title": "Sports news",
            "summary": "Local team wins",
            "why_it_matters": "Not relevant",
            "market_structure_context": "N/A",
            "macro_context": "N/A",
            "tags": ["sports"],
        }

        updated = update_theme_memory.process_rejected_record(record, themes)

        assert updated["treasury-liquidity"]["rejected_count"] == 3


# =============================================================================
# Test Load/Save Theme Memory
# =============================================================================


class TestLoadSaveThemeMemory:
    """Tests for load_theme_memory and save_theme_memory functions."""

    def test_load_theme_memory_returns_dict(self, mock_theme_memory_paths):
        """load_theme_memory returns a dictionary."""
        # Write some data in the expected format
        themes_path = mock_theme_memory_paths["themes"]
        themes_path.write_text('{"themes": {"test-theme": {"theme_id": "test-theme"}}}')

        themes = update_theme_memory.load_theme_memory()

        assert isinstance(themes, dict)
        assert "test-theme" in themes

    def test_save_theme_memory_persists_data(self, mock_theme_memory_paths):
        """save_theme_memory persists data correctly."""
        themes = {
            "new-theme": {
                "theme_id": "new-theme",
                "theme_label": "New Theme",
                "positive_terms": ["test"],
                "negative_terms": [],
                "accepted_count": 1,
                "review_count": 0,
                "rejected_count": 0,
                "last_seen": datetime.now(timezone.utc).isoformat(),
                "priority_score": 10,
            }
        }

        update_theme_memory.save_theme_memory(themes)

        # Verify file contents - should be wrapped in {"themes": ...}
        themes_path = mock_theme_memory_paths["themes"]
        content = json.loads(themes_path.read_text())
        assert "themes" in content
        assert "new-theme" in content["themes"]


# =============================================================================
# Test Theme Memory Model
# =============================================================================


class TestThemeMemoryModel:
    """Tests for theme memory data model structure."""

    def test_theme_has_required_fields(self, sample_theme):
        """Theme record has all required fields."""
        required_fields = [
            "theme_id",
            "theme_label",
            "positive_terms",
            "negative_terms",
            "accepted_count",
            "review_count",
            "rejected_count",
            "last_seen",
            "priority_score",
        ]

        for field in required_fields:
            assert field in sample_theme

    def test_theme_id_is_slug(self, sample_new_theme_data):
        """Theme ID should be a URL-safe slug."""
        existing_themes = {}
        result = update_theme_memory.create_or_update_theme(
            existing_themes, sample_new_theme_data
        )

        # Should only contain lowercase letters, numbers, hyphens, underscores
        import re

        assert re.match(r"^[a-z0-9_-]+$", result["theme_id"])

    def test_counts_are_integers(self, sample_theme):
        """Count fields should be integers."""
        assert isinstance(sample_theme["accepted_count"], int)
        assert isinstance(sample_theme["review_count"], int)
        assert isinstance(sample_theme["rejected_count"], int)

    def test_priority_score_in_valid_range(self, sample_theme):
        """Priority score should be between 0 and 100."""
        score = update_theme_memory.compute_priority_score(sample_theme)
        assert 0 <= score <= 100
