"""
Tests for build_daily_macro_digest.py module.

Tests the daily macro digest building functionality including:
- Date parsing
- Macro record filtering
- Digest building with mocked AI synthesis
- Empty records handling
- File output
"""

import json
from datetime import date
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from scripts.build_daily_macro_digest import (
    parse_date,
    filter_macro_records,
    build_macro_digest,
    DIGEST_TYPE,
    OUTPUT_DIR,
)


class TestParseDate:
    """Tests for parse_date function."""

    def test_parses_valid_date_string(self):
        """Should parse valid YYYY-MM-DD date string."""
        result = parse_date("2026-03-25")
        assert result == date(2026, 3, 25)

    def test_returns_today_when_date_string_is_none(self):
        """Should return today's date when input is None."""
        result = parse_date(None)
        assert result == date.today()

    def test_raises_system_exit_for_invalid_format(self):
        """Should raise SystemExit for invalid date format."""
        with pytest.raises(SystemExit, match="Invalid date format"):
            parse_date("03-25-2026")

    def test_raises_system_exit_for_invalid_date(self):
        """Should raise SystemExit for invalid date values."""
        with pytest.raises(SystemExit, match="Invalid date format"):
            parse_date("2026-13-45")

    def test_raises_system_exit_for_non_date_string(self):
        """Should raise SystemExit for non-date string."""
        with pytest.raises(SystemExit, match="Invalid date format"):
            parse_date("not-a-date")


class TestFilterMacroRecords:
    """Tests for filter_macro_records function."""

    def test_filters_inflation_records(self):
        """Should filter records with inflation-related keywords."""
        records = [
            {
                "id": "inf_001",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "CPI data",
                "title": "CPI Report",
            },
            {
                "id": "tech_001",
                "topic": "technology",
                "tags": ["gadget"],
                "summary": "New phone",
                "title": "Tech News",
            },
        ]
        result = filter_macro_records(records)
        assert len(result) == 1
        assert result[0]["id"] == "inf_001"

    def test_filters_labor_records(self):
        """Should filter records with labor-related keywords."""
        records = [
            {
                "id": "labor_001",
                "topic": "employment",
                "tags": ["jobs"],
                "summary": "Jobs report",
                "title": "Employment Data",
            },
            {
                "id": "sports_001",
                "topic": "sports",
                "tags": ["game"],
                "summary": "Game results",
                "title": "Sports",
            },
        ]
        result = filter_macro_records(records)
        assert len(result) == 1
        assert result[0]["id"] == "labor_001"

    def test_filters_central_bank_records(self):
        """Should filter records with central bank keywords."""
        records = [
            {
                "id": "fed_001",
                "topic": "monetary policy",
                "tags": ["fed", "fomc"],
                "summary": "Fed statement",
                "title": "FOMC",
            },
            {
                "id": "ent_001",
                "topic": "entertainment",
                "tags": ["movie"],
                "summary": "Movie news",
                "title": "Films",
            },
        ]
        result = filter_macro_records(records)
        assert len(result) == 1
        assert result[0]["id"] == "fed_001"

    def test_filters_rates_curve_records(self):
        """Should filter records with rate/yield keywords."""
        records = [
            {
                "id": "rates_001",
                "topic": "rates",
                "tags": ["yield", "curve"],
                "summary": "Yield analysis",
                "title": "Curve",
            },
            {
                "id": "food_001",
                "topic": "food",
                "tags": ["recipe"],
                "summary": "Cooking tips",
                "title": "Recipes",
            },
        ]
        result = filter_macro_records(records)
        assert len(result) == 1
        assert result[0]["id"] == "rates_001"

    def test_returns_empty_list_when_no_macro_records(self):
        """Should return empty list when no records match macro themes."""
        records = [
            {
                "id": "tech_001",
                "topic": "technology",
                "tags": ["gadget"],
                "summary": "Tech",
                "title": "Tech",
            },
            {
                "id": "sports_001",
                "topic": "sports",
                "tags": ["game"],
                "summary": "Sports",
                "title": "Sports",
            },
        ]
        result = filter_macro_records(records)
        assert result == []

    def test_handles_empty_records_list(self):
        """Should return empty list for empty input."""
        result = filter_macro_records([])
        assert result == []

    def test_returns_all_records_when_all_match_macro(self):
        """Should return all records when all are macro-related."""
        records = [
            {
                "id": "inf_001",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "CPI",
                "title": "CPI",
            },
            {
                "id": "labor_001",
                "topic": "employment",
                "tags": ["jobs"],
                "summary": "Jobs",
                "title": "Jobs",
            },
            {
                "id": "fed_001",
                "topic": "monetary policy",
                "tags": ["fed"],
                "summary": "Fed",
                "title": "Fed",
            },
        ]
        result = filter_macro_records(records)
        assert len(result) == 3

    def test_classifies_by_topic_not_just_tags(self):
        """Should classify by topic content, not just tags."""
        records = [
            {
                "id": "inf_001",
                "topic": "inflation",
                "tags": [],
                "summary": "",
                "title": "",
            },
            {
                "id": "labor_001",
                "topic": "employment",
                "tags": [],
                "summary": "",
                "title": "",
            },
        ]
        result = filter_macro_records(records)
        assert len(result) == 2


class TestBuildMacroDigest:
    """Tests for build_macro_digest function."""

    @patch("scripts.build_daily_macro_digest.save_digest_record")
    @patch("scripts.build_daily_macro_digest.load_accepted_records_for_date")
    def test_returns_digest_for_empty_records(self, mock_load, mock_save):
        """Should return digest with empty message when no records."""
        mock_load.return_value = []

        result = build_macro_digest(date(2026, 3, 25))

        assert result is not None
        assert result["digest_type"] == DIGEST_TYPE
        assert result["summary"] == "No accepted records available for this date."
        assert result["confidence"] == 0
        assert result["key_themes"] == []

    @patch("scripts.build_daily_macro_digest.save_digest_record")
    @patch("scripts.build_daily_macro_digest.load_accepted_records_for_date")
    def test_returns_digest_when_no_macro_records(self, mock_load, mock_save):
        """Should return digest noting absence of macro content when no macro records."""
        mock_load.return_value = [
            {
                "id": "tech_001",
                "topic": "technology",
                "tags": ["gadget"],
                "summary": "Tech",
                "title": "Tech",
            },
        ]

        result = build_macro_digest(date(2026, 3, 25))

        assert result is not None
        assert result["digest_type"] == DIGEST_TYPE
        assert "none were classified as macro-relevant" in result["summary"]
        assert result["confidence"] == 0

    @patch("scripts.build_daily_macro_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_macro_digest.save_digest_record")
    @patch("scripts.build_daily_macro_digest.load_accepted_records_for_date")
    def test_builds_digest_with_mocked_synthesis(
        self, mock_load, mock_save, mock_synthesis
    ):
        """Should build digest with AI synthesis when records available."""
        mock_load.return_value = [
            {
                "id": "inf_001",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "CPI data shows inflation rising",
                "title": "CPI Report",
                "key_points": ["CPI +0.3%"],
                "why_it_matters": "Inflation matters",
                "macro_context": "Macro context",
                "market_structure_context": "MS context",
                "quality_tier": {"tier": "tier_1", "score": 85},
            }
        ]

        mock_synthesis.return_value = {
            "summary": "Synthesized macro summary for the day.",
            "key_themes": ["inflation", "central_bank"],
            "confidence": 80,
        }

        result = build_macro_digest(date(2026, 3, 25))

        assert result is not None
        assert result["digest_type"] == DIGEST_TYPE
        assert result["summary"] == "Synthesized macro summary for the day."
        assert result["key_themes"] == ["inflation", "central_bank"]
        assert result["confidence"] == 80
        assert "inf_001" in result["linked_record_ids"]

    @patch("scripts.build_daily_macro_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_macro_digest.save_digest_record")
    @patch("scripts.build_daily_macro_digest.load_accepted_records_for_date")
    def test_uses_fallback_when_synthesis_fails(
        self, mock_load, mock_save, mock_synthesis
    ):
        """Should use fallback summary when AI synthesis fails."""
        mock_load.return_value = [
            {
                "id": "inf_001",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "CPI data",
                "title": "CPI Report",
                "key_points": ["CPI +0.3%"],
                "why_it_matters": "Inflation matters",
                "macro_context": "Macro context",
                "market_structure_context": "MS context",
                "quality_tier": {"tier": "tier_1", "score": 85},
            }
        ]

        mock_synthesis.side_effect = ValueError("API Error")

        result = build_macro_digest(date(2026, 3, 25))

        assert result is not None
        assert result["confidence"] == 50
        assert "inflation" in result["key_themes"]

    @patch("scripts.build_daily_macro_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_macro_digest.save_digest_record")
    @patch("scripts.build_daily_macro_digest.load_accepted_records_for_date")
    def test_extracts_linked_quant_ids(self, mock_load, mock_save, mock_synthesis):
        """Should extract and include linked quant IDs in digest."""
        mock_load.return_value = [
            {
                "id": "article_001",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "CPI data",
                "title": "CPI Report",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 85},
                "linked_quant_context": [
                    {"record_id": "cpi_quant_2026_03", "link_score": 90},
                ],
            }
        ]

        mock_synthesis.return_value = {
            "summary": "Summary",
            "key_themes": ["inflation"],
            "confidence": 75,
        }

        result = build_macro_digest(date(2026, 3, 25))

        assert "cpi_quant_2026_03" in result["linked_quant_ids"]

    @patch("scripts.build_daily_macro_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_macro_digest.save_digest_record")
    @patch("scripts.build_daily_macro_digest.load_accepted_records_for_date")
    def test_groups_records_by_theme(self, mock_load, mock_save, mock_synthesis):
        """Should group records by macro themes."""
        mock_load.return_value = [
            {
                "id": "inf_001",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "CPI data",
                "title": "CPI",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 85},
            },
            {
                "id": "labor_001",
                "topic": "employment",
                "tags": ["jobs"],
                "summary": "Jobs data",
                "title": "Jobs",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 80},
            },
        ]

        mock_synthesis.return_value = {
            "summary": "Summary",
            "key_themes": ["inflation", "labor_growth"],
            "confidence": 80,
        }

        result = build_macro_digest(date(2026, 3, 25))

        # Both record IDs should be in linked_record_ids
        assert "inf_001" in result["linked_record_ids"]
        assert "labor_001" in result["linked_record_ids"]


class TestDigestOutput:
    """Tests for digest output file creation."""

    @patch("scripts.build_daily_macro_digest.save_digest_record")
    @patch("scripts.build_daily_macro_digest.load_accepted_records_for_date")
    def test_digest_has_correct_structure(self, mock_load, mock_save, tmp_path):
        """Should produce digest with all required fields."""
        mock_load.return_value = [
            {
                "id": "inf_001",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "CPI data",
                "title": "CPI",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 85},
            }
        ]

        output_dir = tmp_path / "digests" / "daily_macro"
        output_dir.mkdir(parents=True)

        mock_save.return_value = output_dir / "daily_macro_2026_03_25.json"

        from scripts.build_daily_macro_digest import build_macro_digest

        with patch("scripts.build_daily_macro_digest.OUTPUT_DIR", output_dir):
            result = build_macro_digest(date(2026, 3, 25))

        # Verify all required fields are present
        assert "digest_id" in result
        assert "digest_type" in result
        assert "created_at" in result
        assert "date_range" in result
        assert "summary" in result
        assert "key_themes" in result
        assert "linked_record_ids" in result
        assert "linked_quant_ids" in result
        assert "confidence" in result
        assert "notes" in result

        # Verify date_range structure
        assert "start" in result["date_range"]
        assert "end" in result["date_range"]
        assert result["date_range"]["start"] == "2026-03-25"
        assert result["date_range"]["end"] == "2026-03-25"
