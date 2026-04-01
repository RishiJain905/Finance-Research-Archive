"""
Tests for build_daily_market_structure_digest.py module.

Tests the daily market structure digest building functionality including:
- Market structure record filtering
- Digest building with mocked AI synthesis
- Empty records handling
- File output
"""

import json
from datetime import date
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from scripts.build_daily_market_structure_digest import (
    parse_date,
    filter_market_structure_records,
    build_market_structure_digest,
    group_records_by_theme,
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
            parse_date("25-03-2026")


class TestFilterMarketStructureRecords:
    """Tests for filter_market_structure_records function."""

    def test_filters_liquidity_records(self):
        """Should filter records with liquidity-related keywords."""
        records = [
            {
                "id": "liq_001",
                "topic": "liquidity",
                "tags": ["repo", "sofr"],
                "summary": "Repo rates",
                "title": "Funding",
            },
            {
                "id": "tech_001",
                "topic": "technology",
                "tags": ["gadget"],
                "summary": "New phone",
                "title": "Tech",
            },
        ]
        result = filter_market_structure_records(records)
        assert len(result) == 1
        assert result[0]["id"] == "liq_001"

    def test_filters_treasury_issuance_records(self):
        """Should filter records with treasury issuance keywords."""
        records = [
            {
                "id": "treas_001",
                "topic": "treasury",
                "tags": ["auction", "debt"],
                "summary": "Auction results",
                "title": "Treasury",
            },
            {
                "id": "food_001",
                "topic": "food",
                "tags": ["recipe"],
                "summary": "Cooking tips",
                "title": "Recipes",
            },
        ]
        result = filter_market_structure_records(records)
        assert len(result) == 1
        assert result[0]["id"] == "treas_001"

    def test_filters_clearing_exchange_records(self):
        """Should filter records with clearing/exchange keywords."""
        records = [
            {
                "id": "clearing_001",
                "topic": "clearing",
                "tags": ["dtcc", "settlement"],
                "summary": "Clearing updates",
                "title": "DTCC",
            },
            {
                "id": "sports_001",
                "topic": "sports",
                "tags": ["game"],
                "summary": "Game results",
                "title": "Sports",
            },
        ]
        result = filter_market_structure_records(records)
        assert len(result) == 1
        assert result[0]["id"] == "clearing_001"

    def test_filters_market_plumbing_records(self):
        """Should filter records with market plumbing keywords."""
        records = [
            {
                "id": "plumbing_001",
                "topic": "market structure",
                "tags": ["infrastructure", "operations"],
                "summary": "Market ops",
                "title": "Infrastructure",
            },
            {
                "id": "ent_001",
                "topic": "entertainment",
                "tags": ["movie"],
                "summary": "Movie news",
                "title": "Films",
            },
        ]
        result = filter_market_structure_records(records)
        assert len(result) == 1
        assert result[0]["id"] == "plumbing_001"

    def test_returns_empty_list_when_no_market_structure_records(self):
        """Should return empty list when no records match market structure themes."""
        records = [
            {
                "id": "macro_001",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "Inflation",
                "title": "CPI",
            },
            {
                "id": "sports_001",
                "topic": "sports",
                "tags": ["game"],
                "summary": "Game",
                "title": "Sports",
            },
        ]
        result = filter_market_structure_records(records)
        assert result == []

    def test_handles_empty_records_list(self):
        """Should return empty list for empty input."""
        result = filter_market_structure_records([])
        assert result == []

    def test_classifies_by_topic_and_tags(self):
        """Should classify by topic content, not just tags."""
        records = [
            {
                "id": "liq_001",
                "topic": "liquidity",
                "tags": [],
                "summary": "",
                "title": "",
            },
            {
                "id": "treas_001",
                "topic": "treasury",
                "tags": [],
                "summary": "",
                "title": "",
            },
        ]
        result = filter_market_structure_records(records)
        assert len(result) == 2


class TestGroupRecordsByTheme:
    """Tests for local group_records_by_theme function."""

    def test_groups_records_by_theme(self):
        """Should group records under their matching themes."""
        records = [
            {
                "id": "liq_001",
                "topic": "liquidity",
                "tags": ["repo"],
                "summary": "Repo data",
                "title": "Repo",
            },
            {
                "id": "treas_001",
                "topic": "treasury",
                "tags": ["auction"],
                "summary": "Auction data",
                "title": "Auction",
            },
        ]

        from scripts.digest_utils import MARKET_STRUCTURE_THEMES

        groups = group_records_by_theme(records, MARKET_STRUCTURE_THEMES)

        assert "liquidity" in groups
        assert "treasury_issuance" in groups
        assert len(groups["liquidity"]) == 1
        assert len(groups["treasury_issuance"]) == 1

    def test_handles_multiple_themes_per_record(self):
        """Should handle records matching multiple themes."""
        records = [
            {
                "id": "combo_001",
                "topic": "liquidity",
                "tags": ["repo", "treasury"],
                "summary": "Repo and treasury",
                "title": "Combo",
            },
        ]

        from scripts.digest_utils import MARKET_STRUCTURE_THEMES

        groups = group_records_by_theme(records, MARKET_STRUCTURE_THEMES)

        assert "liquidity" in groups
        assert "treasury_issuance" in groups


class TestBuildMarketStructureDigest:
    """Tests for build_market_structure_digest function."""

    @patch("scripts.build_daily_market_structure_digest.save_digest_record")
    @patch("scripts.build_daily_market_structure_digest.load_accepted_records_for_date")
    def test_returns_digest_for_empty_records(self, mock_load, mock_save):
        """Should return digest with empty message when no records."""
        mock_load.return_value = []

        result = build_market_structure_digest(date(2026, 3, 25))

        assert result is not None
        assert result["digest_type"] == DIGEST_TYPE
        assert result["summary"] == "No accepted records available for this date."
        assert result["confidence"] == 0
        assert result["key_themes"] == []

    @patch("scripts.build_daily_market_structure_digest.save_digest_record")
    @patch("scripts.build_daily_market_structure_digest.load_accepted_records_for_date")
    def test_returns_digest_when_no_market_structure_records(
        self, mock_load, mock_save
    ):
        """Should return digest noting absence of market structure content."""
        mock_load.return_value = [
            {
                "id": "macro_001",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "Inflation",
                "title": "CPI",
            },
        ]

        result = build_market_structure_digest(date(2026, 3, 25))

        assert result is not None
        assert result["digest_type"] == DIGEST_TYPE
        assert "none were classified as market structure-relevant" in result["summary"]
        assert result["confidence"] == 0

    @patch("scripts.build_daily_market_structure_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_market_structure_digest.save_digest_record")
    @patch("scripts.build_daily_market_structure_digest.load_accepted_records_for_date")
    def test_builds_digest_with_mocked_synthesis(
        self, mock_load, mock_save, mock_synthesis
    ):
        """Should build digest with AI synthesis when records available."""
        mock_load.return_value = [
            {
                "id": "liq_001",
                "topic": "liquidity",
                "tags": ["repo", "sofr"],
                "summary": "Repo rates steady",
                "title": "Funding Update",
                "key_points": ["SOFR 5.31%"],
                "why_it_matters": "Funding conditions matter",
                "macro_context": "Macro context",
                "market_structure_context": "MS context",
                "quality_tier": {"tier": "tier_1", "score": 82},
            }
        ]

        mock_synthesis.return_value = {
            "summary": "Synthesized market structure summary.",
            "key_themes": ["liquidity", "treasury_issuance"],
            "confidence": 80,
        }

        result = build_market_structure_digest(date(2026, 3, 25))

        assert result is not None
        assert result["digest_type"] == DIGEST_TYPE
        assert result["summary"] == "Synthesized market structure summary."
        assert result["key_themes"] == ["liquidity", "treasury_issuance"]
        assert result["confidence"] == 80
        assert "liq_001" in result["linked_record_ids"]

    @patch("scripts.build_daily_market_structure_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_market_structure_digest.save_digest_record")
    @patch("scripts.build_daily_market_structure_digest.load_accepted_records_for_date")
    def test_uses_fallback_when_synthesis_fails(
        self, mock_load, mock_save, mock_synthesis
    ):
        """Should use fallback summary when AI synthesis fails."""
        mock_load.return_value = [
            {
                "id": "liq_001",
                "topic": "liquidity",
                "tags": ["repo"],
                "summary": "Repo rates",
                "title": "Funding",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 82},
            }
        ]

        mock_synthesis.side_effect = ValueError("API Error")

        result = build_market_structure_digest(date(2026, 3, 25))

        assert result is not None
        assert result["confidence"] == 50
        assert "liquidity" in result["key_themes"]

    @patch("scripts.build_daily_market_structure_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_market_structure_digest.save_digest_record")
    @patch("scripts.build_daily_market_structure_digest.load_accepted_records_for_date")
    def test_extracts_linked_quant_ids(self, mock_load, mock_save, mock_synthesis):
        """Should extract and include linked quant IDs in digest."""
        mock_load.return_value = [
            {
                "id": "liq_001",
                "topic": "liquidity",
                "tags": ["repo"],
                "summary": "Repo data",
                "title": "Funding",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 82},
                "linked_quant_context": [
                    {"record_id": "sofr_2026_03_25", "link_score": 90},
                ],
            }
        ]

        mock_synthesis.return_value = {
            "summary": "Summary",
            "key_themes": ["liquidity"],
            "confidence": 75,
        }

        result = build_market_structure_digest(date(2026, 3, 25))

        assert "sofr_2026_03_25" in result["linked_quant_ids"]

    @patch("scripts.build_daily_market_structure_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_market_structure_digest.save_digest_record")
    @patch("scripts.build_daily_market_structure_digest.load_accepted_records_for_date")
    def test_groups_records_by_theme(self, mock_load, mock_save, mock_synthesis):
        """Should group records by market structure themes."""
        mock_load.return_value = [
            {
                "id": "liq_001",
                "topic": "liquidity",
                "tags": ["repo"],
                "summary": "Repo data",
                "title": "Funding",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 82},
            },
            {
                "id": "treas_001",
                "topic": "treasury",
                "tags": ["auction"],
                "summary": "Auction data",
                "title": "Auction",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_2", "score": 75},
            },
        ]

        mock_synthesis.return_value = {
            "summary": "Summary",
            "key_themes": ["liquidity", "treasury_issuance"],
            "confidence": 80,
        }

        result = build_market_structure_digest(date(2026, 3, 25))

        # Both record IDs should be in linked_record_ids
        assert "liq_001" in result["linked_record_ids"]
        assert "treas_001" in result["linked_record_ids"]


class TestDigestOutput:
    """Tests for digest output file creation."""

    @patch("scripts.build_daily_market_structure_digest.save_digest_record")
    @patch("scripts.build_daily_market_structure_digest.load_accepted_records_for_date")
    def test_digest_has_correct_structure(self, mock_load, mock_save, tmp_path):
        """Should produce digest with all required fields."""
        mock_load.return_value = [
            {
                "id": "liq_001",
                "topic": "liquidity",
                "tags": ["repo"],
                "summary": "Repo data",
                "title": "Funding",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 82},
            }
        ]

        output_dir = tmp_path / "digests" / "daily_market_structure"
        output_dir.mkdir(parents=True)

        mock_save.return_value = output_dir / "daily_market_structure_2026_03_25.json"

        with patch(
            "scripts.build_daily_market_structure_digest.OUTPUT_DIR", output_dir
        ):
            result = build_market_structure_digest(date(2026, 3, 25))

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

    @patch("scripts.build_daily_market_structure_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_market_structure_digest.save_digest_record")
    @patch("scripts.build_daily_market_structure_digest.load_accepted_records_for_date")
    def test_digest_id_format_is_correct(self, mock_load, mock_save, mock_synthesis):
        """Should have correct digest_id format for market structure."""
        mock_load.return_value = [
            {
                "id": "liq_001",
                "topic": "liquidity",
                "tags": ["repo"],
                "summary": "Repo data",
                "title": "Funding",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 82},
            }
        ]

        mock_synthesis.return_value = {
            "summary": "Summary",
            "key_themes": ["liquidity"],
            "confidence": 75,
        }

        result = build_market_structure_digest(date(2026, 3, 25))

        assert result["digest_id"] == "daily_market_structure_2026_03_25"
        assert result["digest_type"] == "daily_market_structure"
