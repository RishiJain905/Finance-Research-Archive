"""
Integration tests for digest generation system.

Tests the complete digest pipeline from loading records to producing
valid digest files with proper schema compliance and cross-references.

Tests cover:
- Full pipeline execution with mocked AI
- Digest file validity (valid JSON)
- Schema compliance
- Cross-reference validation
- All three digest types end-to-end
"""

import json
from datetime import date
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from scripts.digest_utils import (
    MACRO_THEMES,
    MARKET_STRUCTURE_THEMES,
    load_digest_schema,
    build_digest_record,
    save_digest_record,
    build_records_context,
    classify_record_theme,
    extract_linked_quant_ids,
    extract_linked_record_ids,
    get_week_range,
)
from scripts.build_daily_macro_digest import (
    build_macro_digest as build_daily_macro,
    filter_macro_records,
)
from scripts.build_daily_market_structure_digest import (
    build_market_structure_digest as build_daily_market_structure,
    filter_market_structure_records,
)
from scripts.build_weekly_digest import (
    build_weekly_digest as build_weekly,
)


class TestDigestSchemaCompliance:
    """Tests that all digest types produce schema-compliant records."""

    @patch("scripts.build_daily_macro_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_macro_digest.load_accepted_records_for_date")
    def test_daily_macro_digest_schema_compliance(
        self, mock_load, mock_synthesis, tmp_path
    ):
        """Daily macro digest should comply with digest schema."""
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
                "linked_quant_context": [
                    {"record_id": "cpi_quant_2026_03", "link_score": 90},
                ],
            }
        ]

        mock_synthesis.return_value = {
            "summary": "Synthesized macro summary for the day covering inflation developments.",
            "key_themes": ["inflation", "central_bank"],
            "confidence": 85,
        }

        # Load the actual schema for validation
        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()
        schema = {
            "digest_id": "",
            "digest_type": "",
            "created_at": "",
            "date_range": {"start": "", "end": ""},
            "summary": "",
            "key_themes": [],
            "linked_record_ids": [],
            "linked_quant_ids": [],
            "confidence": 0,
            "notes": "",
        }
        (schemas_dir / "digest_record.json").write_text(
            json.dumps(schema), encoding="utf-8"
        )

        with patch("scripts.digest_utils.SCHEMAS_DIR", schemas_dir):
            digest = build_daily_macro(date(2026, 3, 25))

        # Validate schema compliance
        assert "digest_id" in digest
        assert "digest_type" in digest
        assert "created_at" in digest
        assert "date_range" in digest
        assert "summary" in digest
        assert "key_themes" in digest
        assert "linked_record_ids" in digest
        assert "linked_quant_ids" in digest
        assert "confidence" in digest
        assert "notes" in digest

        # Validate types
        assert isinstance(digest["key_themes"], list)
        assert isinstance(digest["linked_record_ids"], list)
        assert isinstance(digest["linked_quant_ids"], list)
        assert isinstance(digest["confidence"], int)
        assert 0 <= digest["confidence"] <= 100

    @patch("scripts.build_daily_market_structure_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_market_structure_digest.load_accepted_records_for_date")
    def test_daily_market_structure_digest_schema_compliance(
        self, mock_load, mock_synthesis, tmp_path
    ):
        """Daily market structure digest should comply with digest schema."""
        mock_load.return_value = [
            {
                "id": "liq_001",
                "topic": "liquidity",
                "tags": ["repo"],
                "summary": "Repo rates steady",
                "title": "Funding Update",
                "key_points": ["SOFR 5.31%"],
                "why_it_matters": "Funding matters",
                "macro_context": "Macro context",
                "market_structure_context": "MS context",
                "quality_tier": {"tier": "tier_1", "score": 82},
                "linked_quant_context": [
                    {"record_id": "sofr_2026_03_25", "link_score": 90},
                ],
            }
        ]

        mock_synthesis.return_value = {
            "summary": "Synthesized market structure summary for the day.",
            "key_themes": ["liquidity", "treasury_issuance"],
            "confidence": 80,
        }

        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()
        schema = {
            "digest_id": "",
            "digest_type": "",
            "created_at": "",
            "date_range": {"start": "", "end": ""},
            "summary": "",
            "key_themes": [],
            "linked_record_ids": [],
            "linked_quant_ids": [],
            "confidence": 0,
            "notes": "",
        }
        (schemas_dir / "digest_record.json").write_text(
            json.dumps(schema), encoding="utf-8"
        )

        with patch("scripts.digest_utils.SCHEMAS_DIR", schemas_dir):
            digest = build_daily_market_structure(date(2026, 3, 25))

        # Validate schema compliance
        assert "digest_id" in digest
        assert "digest_type" in digest
        assert digest["digest_type"] == "daily_market_structure"
        assert "key_themes" in digest
        assert "confidence" in digest

    @patch("scripts.build_weekly_digest.call_minimax_for_synthesis")
    @patch("scripts.build_weekly_digest.load_accepted_records_for_range")
    def test_weekly_digest_schema_compliance(self, mock_load, mock_synthesis, tmp_path):
        """Weekly digest should comply with digest schema."""
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
            },
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
                "quality_tier": {"tier": "tier_2", "score": 72},
            },
        ]

        mock_synthesis.return_value = {
            "summary": "Weekly synthesis covering macro and market structure.",
            "key_themes": ["inflation", "liquidity", "central_bank"],
            "confidence": 85,
        }

        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()
        schema = {
            "digest_id": "",
            "digest_type": "",
            "created_at": "",
            "date_range": {"start": "", "end": ""},
            "summary": "",
            "key_themes": [],
            "linked_record_ids": [],
            "linked_quant_ids": [],
            "confidence": 0,
            "notes": "",
        }
        (schemas_dir / "digest_record.json").write_text(
            json.dumps(schema), encoding="utf-8"
        )

        with patch("scripts.digest_utils.SCHEMAS_DIR", schemas_dir):
            digest = build_weekly(date(2026, 3, 23), date(2026, 3, 29))

        # Validate schema compliance
        assert "digest_id" in digest
        assert "digest_type" in digest
        assert digest["digest_type"] == "weekly"
        assert digest["date_range"]["start"] == "2026-03-23"
        assert digest["date_range"]["end"] == "2026-03-29"


class TestCrossReferenceValidation:
    """Tests that digest cross-references are valid."""

    @patch("scripts.build_daily_macro_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_macro_digest.load_accepted_records_for_date")
    def test_linked_record_ids_exist_in_records(self, mock_load, mock_synthesis):
        """All linked_record_ids in digest should exist in records."""
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
                "id": "labor_001",
                "topic": "employment",
                "tags": ["jobs"],
                "summary": "Jobs",
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

        digest = build_daily_macro(date(2026, 3, 25))

        # All linked_record_ids should be from the input records
        record_ids = {r["id"] for r in mock_load.return_value}
        for linked_id in digest["linked_record_ids"]:
            assert linked_id in record_ids

    @patch("scripts.build_daily_macro_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_macro_digest.load_accepted_records_for_date")
    def test_linked_quant_ids_come_from_records(self, mock_load, mock_synthesis):
        """All linked_quant_ids in digest should come from records."""
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
                    {"record_id": "cpi_quant_001", "link_score": 90},
                    {"record_id": "ppi_quant_001", "link_score": 85},
                ],
            }
        ]

        mock_synthesis.return_value = {
            "summary": "Summary",
            "key_themes": ["inflation"],
            "confidence": 80,
        }

        digest = build_daily_macro(date(2026, 3, 25))

        # Extract quant IDs from all records' linked_quant_context
        all_quant_ids = set()
        for record in mock_load.return_value:
            for link in record.get("linked_quant_context", []):
                all_quant_ids.add(link["record_id"])

        # All linked_quant_ids in digest should be from records
        for quant_id in digest["linked_quant_ids"]:
            assert quant_id in all_quant_ids


class TestFileOutputValidation:
    """Tests that digest files are valid JSON and properly structured."""

    @patch("scripts.build_daily_macro_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_macro_digest.load_accepted_records_for_date")
    def test_saved_digest_is_valid_json(self, mock_load, mock_synthesis, tmp_path):
        """Saved digest file should be valid JSON."""
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
            "summary": "Summary",
            "key_themes": ["inflation"],
            "confidence": 80,
        }

        output_dir = tmp_path / "digests" / "daily_macro"
        output_dir.mkdir(parents=True)

        with patch("scripts.build_daily_macro_digest.OUTPUT_DIR", output_dir):
            digest = build_daily_macro(date(2026, 3, 25))
            output_path = save_digest_record(digest, output_dir)

        # File should exist and be valid JSON
        assert output_path.exists()
        with open(output_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)

        assert loaded["digest_id"] == digest["digest_id"]
        assert loaded["summary"] == digest["summary"]

    @patch("scripts.build_daily_market_structure_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_market_structure_digest.load_accepted_records_for_date")
    def test_saved_market_structure_digest_is_valid_json(
        self, mock_load, mock_synthesis, tmp_path
    ):
        """Saved market structure digest should be valid JSON."""
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
            "confidence": 80,
        }

        output_dir = tmp_path / "digests" / "daily_market_structure"
        output_dir.mkdir(parents=True)

        with patch(
            "scripts.build_daily_market_structure_digest.OUTPUT_DIR", output_dir
        ):
            digest = build_daily_market_structure(date(2026, 3, 25))
            output_path = save_digest_record(digest, output_dir)

        assert output_path.exists()
        with open(output_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)

        assert loaded["digest_type"] == "daily_market_structure"

    @patch("scripts.build_weekly_digest.call_minimax_for_synthesis")
    @patch("scripts.build_weekly_digest.load_accepted_records_for_range")
    def test_saved_weekly_digest_is_valid_json(
        self, mock_load, mock_synthesis, tmp_path
    ):
        """Saved weekly digest should be valid JSON."""
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
            "confidence": 80,
        }

        output_dir = tmp_path / "digests" / "weekly"
        output_dir.mkdir(parents=True)

        with patch("scripts.build_weekly_digest.OUTPUT_DIR", output_dir):
            digest = build_weekly(date(2026, 3, 23), date(2026, 3, 29))
            output_path = save_digest_record(digest, output_dir)

        assert output_path.exists()
        with open(output_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)

        assert loaded["digest_type"] == "weekly"
        assert loaded["date_range"]["start"] == "2026-03-23"
        assert loaded["date_range"]["end"] == "2026-03-29"


class TestEndToEndDigestTypes:
    """End-to-end tests for all three digest types."""

    @patch("scripts.build_daily_macro_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_macro_digest.load_accepted_records_for_date")
    def test_daily_macro_end_to_end(self, mock_load, mock_synthesis):
        """Test complete daily macro digest pipeline."""
        records = [
            {
                "id": "macro_001",
                "topic": "inflation",
                "tags": ["cpi", "price"],
                "summary": "CPI rises",
                "title": "CPI Report",
                "key_points": ["+0.3% MoM"],
                "why_it_matters": "Inflation",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 88},
                "linked_quant_context": [
                    {"record_id": "cpi_quant", "link_score": 90},
                ],
            },
            {
                "id": "macro_002",
                "topic": "employment",
                "tags": ["jobs", "labor"],
                "summary": "Jobs growth",
                "title": "Jobs Report",
                "key_points": ["+275k jobs"],
                "why_it_matters": "Labor",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 85},
            },
            {
                "id": "tech_001",
                "topic": "technology",
                "tags": ["gadget"],
                "summary": "New phone",
                "title": "Tech News",
                "key_points": [],
                "why_it_matters": "Not macro",
                "macro_context": "Not macro",
                "market_structure_context": "Not MS",
                "quality_tier": {"tier": "tier_3", "score": 40},
            },
        ]
        mock_load.return_value = records
        mock_synthesis.return_value = {
            "summary": "Daily macro synthesis covering inflation and labor market developments.",
            "key_themes": ["inflation", "labor_growth"],
            "confidence": 85,
        }

        digest = build_daily_macro(date(2026, 3, 25))

        # Should filter to macro records only
        assert len(digest["linked_record_ids"]) == 2
        assert "macro_001" in digest["linked_record_ids"]
        assert "macro_002" in digest["linked_record_ids"]
        assert "tech_001" not in digest["linked_record_ids"]

        # Should have quant links
        assert "cpi_quant" in digest["linked_quant_ids"]

        # Should have correct themes
        assert "inflation" in digest["key_themes"]
        assert "labor_growth" in digest["key_themes"]

    @patch("scripts.build_daily_market_structure_digest.call_minimax_for_synthesis")
    @patch("scripts.build_daily_market_structure_digest.load_accepted_records_for_date")
    def test_daily_market_structure_end_to_end(self, mock_load, mock_synthesis):
        """Test complete daily market structure digest pipeline."""
        records = [
            {
                "id": "ms_001",
                "topic": "liquidity",
                "tags": ["repo", "sofr"],
                "summary": "Repo rates",
                "title": "Funding",
                "key_points": ["SOFR 5.31%"],
                "why_it_matters": "Funding",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 82},
                "linked_quant_context": [
                    {"record_id": "sofr_quant", "link_score": 90},
                ],
            },
            {
                "id": "ms_002",
                "topic": "treasury",
                "tags": ["auction", "debt"],
                "summary": "Auction results",
                "title": "Treasury Auction",
                "key_points": ["$39B", "4.42%"],
                "why_it_matters": "Issuance",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_2", "score": 75},
            },
            {
                "id": "macro_001",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "CPI",
                "title": "CPI",
                "key_points": [],
                "why_it_matters": "Inflation",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 85},
            },
        ]
        mock_load.return_value = records
        mock_synthesis.return_value = {
            "summary": "Daily market structure synthesis covering liquidity and Treasury issuance.",
            "key_themes": ["liquidity", "treasury_issuance"],
            "confidence": 80,
        }

        digest = build_daily_market_structure(date(2026, 3, 25))

        # Should filter to market structure records only
        assert len(digest["linked_record_ids"]) == 2
        assert "ms_001" in digest["linked_record_ids"]
        assert "ms_002" in digest["linked_record_ids"]
        assert "macro_001" not in digest["linked_record_ids"]

        # Should have correct themes
        assert "liquidity" in digest["key_themes"]
        assert "treasury_issuance" in digest["key_themes"]

    @patch("scripts.build_weekly_digest.call_minimax_for_synthesis")
    @patch("scripts.build_weekly_digest.load_accepted_records_for_range")
    def test_weekly_end_to_end(self, mock_load, mock_synthesis):
        """Test complete weekly digest pipeline."""
        records = [
            {
                "id": "mon_macro",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "Monday CPI",
                "title": "CPI Mon",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 90},
                "created_at": "2026-03-23T10:00:00+00:00",
            },
            {
                "id": "tue_ms",
                "topic": "liquidity",
                "tags": ["repo"],
                "summary": "Tuesday Repo",
                "title": "Repo Tue",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_2", "score": 72},
                "created_at": "2026-03-24T10:00:00+00:00",
            },
            {
                "id": "wed_macro",
                "topic": "employment",
                "tags": ["jobs"],
                "summary": "Wednesday Jobs",
                "title": "Jobs Wed",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 88},
                "created_at": "2026-03-25T10:00:00+00:00",
            },
            {
                "id": "thu_ms",
                "topic": "treasury",
                "tags": ["auction"],
                "summary": "Thursday Auction",
                "title": "Auction Thu",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_2", "score": 70},
                "created_at": "2026-03-26T10:00:00+00:00",
            },
            {
                "id": "fri_macro",
                "topic": "central bank",
                "tags": ["fed", "fomc"],
                "summary": "Friday Fed",
                "title": "Fed Fri",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_1", "score": 92},
                "created_at": "2026-03-27T10:00:00+00:00",
            },
        ]
        mock_load.return_value = records
        mock_synthesis.return_value = {
            "summary": "Weekly synthesis covering major macro and market structure themes.",
            "key_themes": [
                "inflation",
                "labor_growth",
                "liquidity",
                "treasury_issuance",
                "central_bank",
            ],
            "confidence": 88,
        }

        digest = build_weekly(date(2026, 3, 23), date(2026, 3, 29))

        # Should include all records
        assert len(digest["linked_record_ids"]) == 5

        # Should identify multiple themes
        assert len(digest["key_themes"]) >= 3

        # Should have high confidence with good data
        assert digest["confidence"] >= 80


class TestEmptyAndEdgeCases:
    """Tests for edge cases in digest generation."""

    @patch("scripts.build_daily_macro_digest.load_accepted_records_for_date")
    def test_daily_macro_with_no_records(self, mock_load):
        """Should handle day with no accepted records gracefully."""
        mock_load.return_value = []

        digest = build_daily_macro(date(2026, 3, 25))

        assert digest["confidence"] == 0
        assert digest["key_themes"] == []
        assert "No accepted records" in digest["summary"]

    @patch("scripts.build_daily_macro_digest.load_accepted_records_for_date")
    def test_daily_macro_with_no_macro_records(self, mock_load):
        """Should handle day with no macro-relevant records gracefully."""
        mock_load.return_value = [
            {
                "id": "tech_001",
                "topic": "technology",
                "tags": ["gadget"],
                "summary": "Tech",
                "title": "Tech",
                "key_points": [],
                "why_it_matters": "Why",
                "macro_context": "Macro",
                "market_structure_context": "MS",
                "quality_tier": {"tier": "tier_3", "score": 40},
            }
        ]

        digest = build_daily_macro(date(2026, 3, 25))

        assert digest["confidence"] == 0
        assert digest["key_themes"] == []
        assert "none were classified as macro-relevant" in digest["summary"]

    @patch("scripts.build_weekly_digest.load_accepted_records_for_range")
    def test_weekly_with_no_records(self, mock_load):
        """Should handle week with no accepted records gracefully."""
        mock_load.return_value = []

        digest = build_weekly(date(2026, 3, 23), date(2026, 3, 29))

        assert digest["confidence"] == 0
        assert digest["key_themes"] == []
        assert "No accepted records" in digest["summary"]

    @patch("scripts.build_daily_market_structure_digest.load_accepted_records_for_date")
    def test_daily_market_structure_with_no_records(self, mock_load):
        """Should handle day with no market structure records gracefully."""
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
            }
        ]

        digest = build_daily_market_structure(date(2026, 3, 25))

        assert digest["confidence"] == 0
        assert digest["key_themes"] == []
        assert "none were classified as market structure-relevant" in digest["summary"]
