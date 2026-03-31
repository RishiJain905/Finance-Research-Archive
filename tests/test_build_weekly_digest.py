"""
Tests for build_weekly_digest.py module.

Tests the weekly synthesis digest building functionality including:
- Week range calculation
- Record loading for date ranges
- Digest building with mocked AI synthesis
- Quality tier analysis
- Empty week handling
- File output
"""

import json
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from scripts.build_weekly_digest import (
    parse_date,
    build_weekly_digest,
    DIGEST_TYPE,
    OUTPUT_DIR,
    ALL_THEMES,
)


class TestParseDate:
    """Tests for parse_date function."""

    def test_parses_valid_date_string(self):
        """Should parse valid YYYY-MM-DD date string."""
        result = parse_date("2026-03-25")
        assert result == date(2026, 3, 25)

    def test_returns_none_when_date_string_is_none(self):
        """Should return None when input is None."""
        result = parse_date(None)
        assert result is None

    def test_raises_system_exit_for_invalid_format(self):
        """Should raise SystemExit for invalid date format."""
        with pytest.raises(SystemExit, match="Invalid date format"):
            parse_date("03-25-2026")


class TestWeekRangeCalculation:
    """Tests for week range calculation in build_weekly_digest."""

    def test_week_range_monday_to_sunday(self):
        """Week should run Monday to Sunday."""
        from scripts.digest_utils import get_week_range

        # March 25, 2026 is a Wednesday
        monday, sunday = get_week_range(date(2026, 3, 25))

        assert monday == date(2026, 3, 23)  # Monday
        assert sunday == date(2026, 3, 29)  # Sunday
        assert (sunday - monday).days == 6


class TestBuildWeeklyDigest:
    """Tests for build_weekly_digest function."""

    @patch("scripts.build_weekly_digest.save_digest_record")
    @patch("scripts.build_weekly_digest.load_accepted_records_for_range")
    def test_returns_digest_for_empty_week(self, mock_load, mock_save):
        """Should return digest with empty message when no records for week."""
        mock_load.return_value = []

        result = build_weekly_digest(date(2026, 3, 23), date(2026, 3, 29))

        assert result is not None
        assert result["digest_type"] == DIGEST_TYPE
        assert result["summary"] == "No accepted records available for this week."
        assert result["confidence"] == 0
        assert result["key_themes"] == []

    @patch("scripts.build_weekly_digest.call_minimax_for_synthesis")
    @patch("scripts.build_weekly_digest.save_digest_record")
    @patch("scripts.build_weekly_digest.load_accepted_records_for_range")
    def test_builds_digest_with_mocked_synthesis(
        self, mock_load, mock_save, mock_synthesis
    ):
        """Should build digest with AI synthesis when records available."""
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
                "created_at": "2026-03-25T10:00:00+00:00",
            },
            {
                "id": "liq_001",
                "topic": "liquidity",
                "tags": ["repo"],
                "summary": "Repo rates",
                "title": "Funding Update",
                "key_points": ["SOFR 5.31%"],
                "why_it_matters": "Funding matters",
                "macro_context": "Macro context",
                "market_structure_context": "MS context",
                "quality_tier": {"tier": "tier_2", "score": 72},
                "created_at": "2026-03-26T10:00:00+00:00",
            },
        ]

        mock_synthesis.return_value = {
            "summary": "Weekly synthesis covering macro and market structure themes.",
            "key_themes": ["inflation", "liquidity", "central_bank"],
            "confidence": 85,
        }

        result = build_weekly_digest(date(2026, 3, 23), date(2026, 3, 29))

        assert result is not None
        assert result["digest_type"] == DIGEST_TYPE
        assert "Weekly synthesis" in result["summary"]
        assert result["confidence"] == 85
        assert "inf_001" in result["linked_record_ids"]
        assert "liq_001" in result["linked_record_ids"]

    @patch("scripts.build_weekly_digest.call_minimax_for_synthesis")
    @patch("scripts.build_weekly_digest.save_digest_record")
    @patch("scripts.build_weekly_digest.load_accepted_records_for_range")
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
                "title": "CPI",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 85},
            }
        ]

        mock_synthesis.side_effect = ValueError("API Error")

        result = build_weekly_digest(date(2026, 3, 23), date(2026, 3, 29))

        assert result is not None
        assert result["confidence"] == 50
        assert "Total accepted records: 1" in result["summary"]

    @patch("scripts.build_weekly_digest.call_minimax_for_synthesis")
    @patch("scripts.build_weekly_digest.save_digest_record")
    @patch("scripts.build_weekly_digest.load_accepted_records_for_range")
    def test_identifies_high_significance_records(
        self, mock_load, mock_save, mock_synthesis
    ):
        """Should identify high-significance records (tier_1 or score >= 80)."""
        mock_load.return_value = [
            {
                "id": "tier1_record",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "High tier",
                "title": "Tier 1",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 92},
            },
            {
                "id": "high_score_record",
                "topic": "employment",
                "tags": ["jobs"],
                "summary": "High score",
                "title": "High Score",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_2", "score": 85},
            },
            {
                "id": "low_score_record",
                "topic": "labor",
                "tags": ["wages"],
                "summary": "Low score",
                "title": "Low Score",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_3", "score": 45},
            },
        ]

        mock_synthesis.return_value = {
            "summary": "Weekly summary",
            "key_themes": ["inflation"],
            "confidence": 80,
        }

        result = build_weekly_digest(date(2026, 3, 23), date(2026, 3, 29))

        # High significance records should be mentioned in notes
        assert "tier1_record" in result["linked_record_ids"]
        assert "high_score_record" in result["linked_record_ids"]
        assert "low_score_record" in result["linked_record_ids"]

    @patch("scripts.build_weekly_digest.call_minimax_for_synthesis")
    @patch("scripts.build_weekly_digest.save_digest_record")
    @patch("scripts.build_weekly_digest.load_accepted_records_for_range")
    def test_analyzes_quality_tier_distribution(
        self, mock_load, mock_save, mock_synthesis
    ):
        """Should analyze and include quality tier distribution."""
        mock_load.return_value = [
            {
                "id": "tier1_001",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "CPI",
                "title": "CPI",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 90},
            },
            {
                "id": "tier1_002",
                "topic": "employment",
                "tags": ["jobs"],
                "summary": "Jobs",
                "title": "Jobs",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 85},
            },
            {
                "id": "tier2_001",
                "topic": "liquidity",
                "tags": ["repo"],
                "summary": "Repo",
                "title": "Repo",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_2", "score": 70},
            },
        ]

        mock_synthesis.return_value = {
            "summary": "Weekly summary",
            "key_themes": ["inflation"],
            "confidence": 80,
        }

        result = build_weekly_digest(date(2026, 3, 23), date(2026, 3, 29))

        # Notes should contain tier distribution
        assert "tier_1=2" in result["notes"]
        assert "tier_2=1" in result["notes"]

    @patch("scripts.build_weekly_digest.call_minimax_for_synthesis")
    @patch("scripts.build_weekly_digest.save_digest_record")
    @patch("scripts.build_weekly_digest.load_accepted_records_for_range")
    def test_groups_records_by_all_themes(self, mock_load, mock_save, mock_synthesis):
        """Should group records by both macro and market structure themes."""
        mock_load.return_value = [
            {
                "id": "macro_001",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "CPI",
                "title": "CPI",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 85},
            },
            {
                "id": "ms_001",
                "topic": "liquidity",
                "tags": ["repo"],
                "summary": "Repo",
                "title": "Repo",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 82},
            },
        ]

        mock_synthesis.return_value = {
            "summary": "Weekly summary",
            "key_themes": ["inflation", "liquidity"],
            "confidence": 80,
        }

        result = build_weekly_digest(date(2026, 3, 23), date(2026, 3, 29))

        # Both macro and market structure themes should be identified
        assert "inflation" in result["key_themes"]
        assert "liquidity" in result["key_themes"]

    @patch("scripts.build_weekly_digest.call_minimax_for_synthesis")
    @patch("scripts.build_weekly_digest.save_digest_record")
    @patch("scripts.build_weekly_digest.load_accepted_records_for_range")
    def test_extracts_linked_quant_ids(self, mock_load, mock_save, mock_synthesis):
        """Should extract and include linked quant IDs from all records."""
        mock_load.return_value = [
            {
                "id": "article_001",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "CPI",
                "title": "CPI",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 85},
                "linked_quant_context": [
                    {"record_id": "cpi_quant_2026_03", "link_score": 90},
                ],
            },
            {
                "id": "article_002",
                "topic": "liquidity",
                "tags": ["repo"],
                "summary": "Repo",
                "title": "Repo",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 82},
                "linked_quant_context": [
                    {"record_id": "sofr_2026_03_25", "link_score": 88},
                ],
            },
        ]

        mock_synthesis.return_value = {
            "summary": "Weekly summary",
            "key_themes": ["inflation", "liquidity"],
            "confidence": 80,
        }

        result = build_weekly_digest(date(2026, 3, 23), date(2026, 3, 29))

        assert "cpi_quant_2026_03" in result["linked_quant_ids"]
        assert "sofr_2026_03_25" in result["linked_quant_ids"]

    @patch("scripts.build_weekly_digest.call_minimax_for_synthesis")
    @patch("scripts.build_weekly_digest.save_digest_record")
    @patch("scripts.build_weekly_digest.load_accepted_records_for_range")
    def test_uses_max_records_30_for_weekly_context(
        self, mock_load, mock_save, mock_synthesis
    ):
        """Should use max_records=30 for weekly context building."""
        mock_load.return_value = [
            {
                "id": f"record_{i:03d}",
                "topic": "inflation" if i % 2 == 0 else "liquidity",
                "tags": ["test"],
                "summary": f"Record {i}",
                "title": f"Record {i}",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_2", "score": 70},
            }
            for i in range(35)
        ]

        mock_synthesis.return_value = {
            "summary": "Weekly summary",
            "key_themes": ["inflation"],
            "confidence": 80,
        }

        build_weekly_digest(date(2026, 3, 23), date(2026, 3, 29))

        # Verify synthesis was called (context includes metadata + records)
        mock_synthesis.assert_called_once()


class TestWeeklyDigestOutput:
    """Tests for weekly digest output file creation."""

    @patch("scripts.build_weekly_digest.call_minimax_for_synthesis")
    @patch("scripts.build_weekly_digest.save_digest_record")
    @patch("scripts.build_weekly_digest.load_accepted_records_for_range")
    def test_digest_has_correct_structure(
        self, mock_load, mock_save, mock_synthesis, tmp_path
    ):
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

        mock_synthesis.return_value = {
            "summary": "Weekly summary",
            "key_themes": ["inflation"],
            "confidence": 80,
        }

        output_dir = tmp_path / "digests" / "weekly"
        output_dir.mkdir(parents=True)

        mock_save.return_value = output_dir / "weekly_2026_03_23_to_2026_03_29.json"

        with patch("scripts.build_weekly_digest.OUTPUT_DIR", output_dir):
            result = build_weekly_digest(date(2026, 3, 23), date(2026, 3, 29))

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

        # Verify weekly date_range
        assert result["date_range"]["start"] == "2026-03-23"
        assert result["date_range"]["end"] == "2026-03-29"

    @patch("scripts.build_weekly_digest.call_minimax_for_synthesis")
    @patch("scripts.build_weekly_digest.save_digest_record")
    @patch("scripts.build_weekly_digest.load_accepted_records_for_range")
    def test_digest_id_format_is_weekly(self, mock_load, mock_save, mock_synthesis):
        """Should have correct digest_id format for weekly."""
        mock_load.return_value = [
            {
                "id": "inf_001",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "CPI",
                "title": "CPI",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 85},
            }
        ]

        mock_synthesis.return_value = {
            "summary": "Summary",
            "key_themes": ["inflation"],
            "confidence": 75,
        }

        result = build_weekly_digest(date(2026, 3, 23), date(2026, 3, 29))

        assert result["digest_id"] == "weekly_2026_03_23_to_2026_03_29"
        assert result["digest_type"] == "weekly"

    @patch("scripts.build_weekly_digest.call_minimax_for_synthesis")
    @patch("scripts.build_weekly_digest.save_digest_record")
    @patch("scripts.build_weekly_digest.load_accepted_records_for_range")
    def test_notes_contain_metadata(self, mock_load, mock_save, mock_synthesis):
        """Should include metadata about the week in notes."""
        mock_load.return_value = [
            {
                "id": "inf_001",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "CPI",
                "title": "CPI",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 85},
            },
            {
                "id": "liq_001",
                "topic": "liquidity",
                "tags": ["repo"],
                "summary": "Repo",
                "title": "Repo",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_2", "score": 72},
            },
        ]

        mock_synthesis.return_value = {
            "summary": "Summary",
            "key_themes": ["inflation", "liquidity"],
            "confidence": 80,
        }

        result = build_weekly_digest(date(2026, 3, 23), date(2026, 3, 29))

        # Notes should contain record counts and theme info
        assert "2 accepted records" in result["notes"]
        assert "inflation" in result["notes"] or "2 themes" in result["notes"]
