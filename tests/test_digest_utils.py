"""
Comprehensive tests for digest_utils module.

Tests all shared utility functions for digest generation including:
- Record loading and filtering
- Theme classification and grouping
- ID extraction
- Digest ID building
- Schema loading and record building
- Context building
- Week range calculation
- JSON extraction from responses
"""

import json
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from scripts.digest_utils import (
    MACRO_THEMES,
    MARKET_STRUCTURE_THEMES,
    load_accepted_records,
    load_accepted_records_for_date,
    load_accepted_records_for_range,
    classify_record_theme,
    group_records_by_theme,
    extract_linked_quant_ids,
    extract_linked_record_ids,
    build_digest_id,
    build_weekly_digest_id,
    load_digest_schema,
    build_digest_record,
    save_digest_record,
    build_records_context,
    get_week_range,
    format_theme_summary,
    extract_json_from_response,
    call_minimax_for_synthesis,
)


class TestLoadAcceptedRecords:
    """Tests for load_accepted_records function."""

    def test_returns_empty_list_when_directory_missing(self, tmp_path):
        """Should return empty list when accepted directory does not exist."""
        # Patch the ACCEPTED_DIR to point to tmp_path
        with patch("scripts.digest_utils.ACCEPTED_DIR", tmp_path / "nonexistent"):
            result = load_accepted_records()
        assert result == []

    def test_returns_empty_list_when_directory_empty(self, tmp_path):
        """Should return empty list when accepted directory is empty."""
        accepted_dir = tmp_path / "accepted"
        accepted_dir.mkdir()
        with patch("scripts.digest_utils.ACCEPTED_DIR", accepted_dir):
            result = load_accepted_records()
        assert result == []

    def test_loads_single_record(self, tmp_path):
        """Should load a single accepted record."""
        accepted_dir = tmp_path / "accepted"
        accepted_dir.mkdir()

        record = {
            "id": "test_record_001",
            "status": "accepted",
            "topic": "macro",
            "title": "Test Record",
            "summary": "A test record for unit testing.",
        }
        record_path = accepted_dir / "test_record_001.json"
        record_path.write_text(json.dumps(record), encoding="utf-8")

        with patch("scripts.digest_utils.ACCEPTED_DIR", accepted_dir):
            result = load_accepted_records()

        assert len(result) == 1
        assert result[0]["id"] == "test_record_001"

    def test_loads_multiple_records(self, tmp_path):
        """Should load multiple accepted records."""
        accepted_dir = tmp_path / "accepted"
        accepted_dir.mkdir()

        for i in range(3):
            record = {
                "id": f"test_record_{i:03d}",
                "status": "accepted",
                "title": f"Test Record {i}",
            }
            (accepted_dir / f"test_record_{i:03d}.json").write_text(
                json.dumps(record), encoding="utf-8"
            )

        with patch("scripts.digest_utils.ACCEPTED_DIR", accepted_dir):
            result = load_accepted_records()

        assert len(result) == 3

    def test_skips_invalid_json_files(self, tmp_path):
        """Should skip files that cannot be parsed as JSON."""
        accepted_dir = tmp_path / "accepted"
        accepted_dir.mkdir()

        # Valid record
        valid_record = {"id": "valid_001", "status": "accepted"}
        (accepted_dir / "valid_001.json").write_text(
            json.dumps(valid_record), encoding="utf-8"
        )

        # Invalid JSON
        (accepted_dir / "invalid.json").write_text("not valid json {", encoding="utf-8")

        with patch("scripts.digest_utils.ACCEPTED_DIR", accepted_dir):
            result = load_accepted_records()

        assert len(result) == 1
        assert result[0]["id"] == "valid_001"


class TestLoadAcceptedRecordsForDate:
    """Tests for load_accepted_records_for_date function."""

    def test_filters_records_by_exact_date(self, tmp_path):
        """Should filter records to only those created on the target date."""
        accepted_dir = tmp_path / "accepted"
        accepted_dir.mkdir()

        records = [
            {
                "id": "record_2026_03_25",
                "created_at": "2026-03-25T10:00:00+00:00",
                "title": "Record on March 25",
            },
            {
                "id": "record_2026_03_26",
                "created_at": "2026-03-26T10:00:00+00:00",
                "title": "Record on March 26",
            },
            {
                "id": "record_no_date",
                "title": "Record without date",
            },
        ]
        for r in records:
            (accepted_dir / f"{r['id']}.json").write_text(
                json.dumps(r), encoding="utf-8"
            )

        with patch("scripts.digest_utils.ACCEPTED_DIR", accepted_dir):
            result = load_accepted_records_for_date(date(2026, 3, 25))

        assert len(result) == 1
        assert result[0]["id"] == "record_2026_03_25"

    def test_returns_empty_list_when_no_records_for_date(self, tmp_path):
        """Should return empty list when no records exist for the date."""
        accepted_dir = tmp_path / "accepted"
        accepted_dir.mkdir()

        record = {
            "id": "record_2026_03_25",
            "created_at": "2026-03-25T10:00:00+00:00",
        }
        (accepted_dir / "record_2026_03_25.json").write_text(
            json.dumps(record), encoding="utf-8"
        )

        with patch("scripts.digest_utils.ACCEPTED_DIR", accepted_dir):
            result = load_accepted_records_for_date(date(2026, 3, 30))

        assert result == []


class TestLoadAcceptedRecordsForRange:
    """Tests for load_accepted_records_for_range function."""

    def test_filters_records_within_inclusive_range(self, tmp_path):
        """Should include records on both start and end dates."""
        accepted_dir = tmp_path / "accepted"
        accepted_dir.mkdir()

        records = [
            {"id": "record_03_23", "created_at": "2026-03-23T10:00:00+00:00"},
            {"id": "record_03_24", "created_at": "2026-03-24T10:00:00+00:00"},
            {"id": "record_03_25", "created_at": "2026-03-25T10:00:00+00:00"},
            {"id": "record_03_26", "created_at": "2026-03-26T10:00:00+00:00"},
            {"id": "record_03_27", "created_at": "2026-03-27T10:00:00+00:00"},
        ]
        for r in records:
            (accepted_dir / f"{r['id']}.json").write_text(
                json.dumps(r), encoding="utf-8"
            )

        with patch("scripts.digest_utils.ACCEPTED_DIR", accepted_dir):
            result = load_accepted_records_for_range(
                date(2026, 3, 24), date(2026, 3, 26)
            )

        assert len(result) == 3
        result_ids = [r["id"] for r in result]
        assert "record_03_24" in result_ids
        assert "record_03_25" in result_ids
        assert "record_03_26" in result_ids

    def test_returns_all_records_when_range_covers_all(self, tmp_path):
        """Should return all records when range covers all dates."""
        accepted_dir = tmp_path / "accepted"
        accepted_dir.mkdir()

        records = [
            {"id": "record_03_23", "created_at": "2026-03-23T10:00:00+00:00"},
            {"id": "record_03_24", "created_at": "2026-03-24T10:00:00+00:00"},
        ]
        for r in records:
            (accepted_dir / f"{r['id']}.json").write_text(
                json.dumps(r), encoding="utf-8"
            )

        with patch("scripts.digest_utils.ACCEPTED_DIR", accepted_dir):
            result = load_accepted_records_for_range(
                date(2026, 3, 1), date(2026, 3, 31)
            )

        assert len(result) == 2


class TestClassifyRecordTheme:
    """Tests for classify_record_theme function."""

    def test_classifies_inflation_theme(self):
        """Should classify record as inflation theme when CPI keyword present."""
        record = {
            "topic": "inflation",
            "tags": ["cpi", "price"],
            "summary": "CPI data shows inflation rising",
            "title": "Consumer Price Index Report",
        }
        themes = classify_record_theme(record, MACRO_THEMES)
        assert "inflation" in themes

    def test_classifies_labor_growth_theme(self):
        """Should classify record as labor_growth theme when employment keywords present."""
        record = {
            "topic": "employment",
            "tags": ["jobs", "gdp"],
            "summary": "Nonfarm payrolls exceed expectations",
            "title": "Employment Report",
        }
        themes = classify_record_theme(record, MACRO_THEMES)
        assert "labor_growth" in themes

    def test_classifies_central_bank_theme(self):
        """Should classify record as central_bank theme when Fed keywords present."""
        record = {
            "topic": "monetary policy",
            "tags": ["fomc", "powell"],
            "summary": "Fed holds rates steady",
            "title": "FOMC Statement",
        }
        themes = classify_record_theme(record, MACRO_THEMES)
        assert "central_bank" in themes

    def test_classifies_rates_curve_theme(self):
        """Should classify record as rates_curve theme when yield keywords present."""
        record = {
            "topic": "rates",
            "tags": ["yield", "bond"],
            "summary": "Yield curve steepens",
            "title": "Treasury Yield Analysis",
        }
        themes = classify_record_theme(record, MACRO_THEMES)
        assert "rates_curve" in themes

    def test_classifies_multiple_themes(self):
        """Should classify record into multiple themes when keywords match."""
        record = {
            "topic": "inflation",
            "tags": ["fed", "cpi", "rates"],
            "summary": "Fed watches inflation as rates rise",
            "title": "Fed Inflation Watch",
        }
        themes = classify_record_theme(record, MACRO_THEMES)
        assert "inflation" in themes
        assert "central_bank" in themes
        assert "rates_curve" in themes

    def test_returns_empty_list_when_no_theme_matches(self):
        """Should return empty list when no theme keywords match."""
        record = {
            "topic": "sports",
            "tags": ["football", "game"],
            "summary": "Local team wins championship",
            "title": "Sports News",
        }
        themes = classify_record_theme(record, MACRO_THEMES)
        assert themes == []

    def test_is_case_insensitive(self):
        """Should match keywords regardless of case."""
        record = {
            "topic": "INFLATION",
            "tags": ["CPI"],
            "summary": "Consumer Price Index rises",
            "title": "CPI Report",
        }
        themes = classify_record_theme(record, MACRO_THEMES)
        assert "inflation" in themes

    def test_handles_empty_record_fields(self):
        """Should handle records with missing or empty fields."""
        record = {"id": "test_001"}
        themes = classify_record_theme(record, MACRO_THEMES)
        assert themes == []

    def test_handles_none_values(self):
        """Should handle None values in topic and tags."""
        record = {
            "topic": None,
            "tags": None,
            "summary": None,
            "title": None,
        }
        themes = classify_record_theme(record, MACRO_THEMES)
        assert themes == []

    def test_classifies_market_structure_liquidity_theme(self):
        """Should classify record as liquidity theme for repo-related content."""
        record = {
            "topic": "funding",
            "tags": ["repo", "sofr"],
            "summary": "Repo rates steady",
            "title": "Funding Market Update",
        }
        themes = classify_record_theme(record, MARKET_STRUCTURE_THEMES)
        assert "liquidity" in themes

    def test_classifies_market_structure_treasury_issuance_theme(self):
        """Should classify record as treasury_issuance theme."""
        record = {
            "topic": "debt",
            "tags": ["treasury", "auction"],
            "summary": "Treasury announces auction",
            "title": "Auction Schedule",
        }
        themes = classify_record_theme(record, MARKET_STRUCTURE_THEMES)
        assert "treasury_issuance" in themes


class TestGroupRecordsByTheme:
    """Tests for group_records_by_theme function."""

    def test_groups_records_by_theme(self):
        """Should group records under their matching themes."""
        records = [
            {
                "id": "inflation_record",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "CPI data",
                "title": "CPI Report",
            },
            {
                "id": "labor_record",
                "topic": "employment",
                "tags": ["jobs"],
                "summary": "Jobs report",
                "title": "Employment Data",
            },
            {
                "id": "inflation_labor_record",
                "topic": "inflation",
                "tags": ["jobs"],
                "summary": "Wage inflation",
                "title": "Wage Pressures",
            },
        ]
        groups = group_records_by_theme(records, MACRO_THEMES)

        assert "inflation" in groups
        assert "labor_growth" in groups
        assert len(groups["inflation"]) == 2
        assert len(groups["labor_growth"]) == 2

    def test_returns_empty_dict_for_empty_records(self):
        """Should return empty dict when no records provided."""
        groups = group_records_by_theme([], MACRO_THEMES)
        assert groups == {}

    def test_omits_themes_with_no_matches(self):
        """Should only include themes that have matching records."""
        records = [
            {
                "id": "inflation_record",
                "topic": "inflation",
                "tags": ["cpi"],
                "summary": "CPI data",
                "title": "CPI Report",
            }
        ]
        groups = group_records_by_theme(records, MACRO_THEMES)

        assert "inflation" in groups
        assert "labor_growth" not in groups
        assert "central_bank" not in groups
        assert "rates_curve" not in groups


class TestExtractLinkedQuantIds:
    """Tests for extract_linked_quant_ids function."""

    def test_extracts_quant_ids_from_linked_quant_context(self):
        """Should extract unique quant IDs from linked_quant_context."""
        records = [
            {
                "id": "article_001",
                "linked_quant_context": [
                    {"record_id": "sofr_2026_03_25", "link_score": 85},
                    {"record_id": "fed_funds_2026_03_25", "link_score": 90},
                ],
            },
            {
                "id": "article_002",
                "linked_quant_context": [
                    {"record_id": "sofr_2026_03_25", "link_score": 80},
                ],
            },
        ]
        quant_ids = extract_linked_quant_ids(records)

        assert len(quant_ids) == 2
        assert "fed_funds_2026_03_25" in quant_ids
        assert "sofr_2026_03_25" in quant_ids

    def test_returns_sorted_unique_ids(self):
        """Should return sorted list with no duplicates."""
        records = [
            {
                "id": "article_001",
                "linked_quant_context": [
                    {"record_id": "z_record"},
                    {"record_id": "a_record"},
                ],
            }
        ]
        quant_ids = extract_linked_quant_ids(records)

        assert quant_ids == ["a_record", "z_record"]

    def test_handles_missing_linked_quant_context(self):
        """Should handle records without linked_quant_context."""
        records = [
            {"id": "article_001"},
            {"id": "article_002", "linked_quant_context": []},
        ]
        quant_ids = extract_linked_quant_ids(records)
        assert quant_ids == []

    def test_handles_non_dict_entries_in_linked_quant_context(self):
        """Should handle non-dict entries in linked_quant_context."""
        records = [
            {
                "id": "article_001",
                "linked_quant_context": [
                    {"record_id": "valid_quant"},
                    "invalid_entry",
                    123,
                    None,
                ],
            }
        ]
        quant_ids = extract_linked_quant_ids(records)
        assert quant_ids == ["valid_quant"]

    def test_handles_empty_records_list(self):
        """Should return empty list for empty records list."""
        quant_ids = extract_linked_quant_ids([])
        assert quant_ids == []


class TestExtractLinkedRecordIds:
    """Tests for extract_linked_record_ids function."""

    def test_extracts_ids_from_records(self):
        """Should extract record IDs from the records list."""
        records = [
            {"id": "record_001"},
            {"id": "record_002"},
            {"id": "record_003"},
        ]
        record_ids = extract_linked_record_ids(records)

        assert record_ids == ["record_001", "record_002", "record_003"]

    def test_skips_records_without_id(self):
        """Should skip records without an id field."""
        records = [
            {"id": "record_001"},
            {"title": "No ID record"},
            {"id": "record_003"},
        ]
        record_ids = extract_linked_record_ids(records)

        assert record_ids == ["record_001", "record_003"]

    def test_returns_sorted_ids(self):
        """Should return sorted list of IDs."""
        records = [
            {"id": "z_record"},
            {"id": "a_record"},
            {"id": "m_record"},
        ]
        record_ids = extract_linked_record_ids(records)

        assert record_ids == ["a_record", "m_record", "z_record"]

    def test_handles_empty_records_list(self):
        """Should return empty list for empty records list."""
        record_ids = extract_linked_record_ids([])
        assert record_ids == []


class TestBuildDigestId:
    """Tests for build_digest_id function."""

    def test_generates_correct_format(self):
        """Should generate digest ID with correct format."""
        digest_id = build_digest_id("daily_macro", date(2026, 3, 25))
        assert digest_id == "daily_macro_2026_03_25"

    def test_date_format_is_YYYY_MM_DD(self):
        """Should format date as YYYY_MM_DD."""
        digest_id = build_digest_id("weekly", date(2026, 1, 5))
        assert digest_id == "weekly_2026_01_05"


class TestBuildWeeklyDigestId:
    """Tests for build_weekly_digest_id function."""

    def test_generates_correct_format(self):
        """Should generate weekly digest ID with start and end dates."""
        digest_id = build_weekly_digest_id(date(2026, 3, 23), date(2026, 3, 29))
        assert digest_id == "weekly_2026_03_23_to_2026_03_29"

    def test_date_format_is_YYYY_MM_DD(self):
        """Should format dates as YYYY_MM_DD."""
        digest_id = build_weekly_digest_id(date(2026, 1, 1), date(2026, 1, 7))
        assert digest_id == "weekly_2026_01_01_to_2026_01_07"


class TestLoadDigestSchema:
    """Tests for load_digest_schema function."""

    def test_loads_schema_successfully(self, tmp_path):
        """Should load digest schema from file."""
        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()

        schema = {
            "digest_id": "",
            "digest_type": "",
            "date_range": {"start": "", "end": ""},
            "summary": "",
            "key_themes": [],
        }
        (schemas_dir / "digest_record.json").write_text(
            json.dumps(schema), encoding="utf-8"
        )

        with patch("scripts.digest_utils.SCHEMAS_DIR", schemas_dir):
            result = load_digest_schema()

        assert result["digest_id"] == ""
        assert result["digest_type"] == ""

    def test_raises_error_when_schema_missing(self, tmp_path):
        """Should raise FileNotFoundError when schema file is missing."""
        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()

        with patch("scripts.digest_utils.SCHEMAS_DIR", schemas_dir):
            with pytest.raises(FileNotFoundError):
                load_digest_schema()


class TestBuildDigestRecord:
    """Tests for build_digest_record function."""

    def test_builds_valid_digest_record(self, tmp_path):
        """Should build a complete digest record."""
        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()
        (schemas_dir / "digest_record.json").write_text(
            json.dumps(
                {
                    "digest_id": "",
                    "digest_type": "",
                    "date_range": {"start": "", "end": ""},
                    "summary": "",
                    "key_themes": [],
                    "linked_record_ids": [],
                    "linked_quant_ids": [],
                    "confidence": 0,
                    "notes": "",
                }
            ),
            encoding="utf-8",
        )

        with patch("scripts.digest_utils.SCHEMAS_DIR", schemas_dir):
            digest = build_digest_record(
                digest_type="daily_macro",
                start_date=date(2026, 3, 25),
                end_date=date(2026, 3, 25),
                summary="Test summary",
                key_themes=["inflation", "labor"],
                linked_record_ids=["rec_001", "rec_002"],
                linked_quant_ids=["quant_001"],
                confidence=80,
                notes="Test notes",
            )

        assert digest["digest_type"] == "daily_macro"
        assert digest["summary"] == "Test summary"
        assert digest["key_themes"] == ["inflation", "labor"]
        assert digest["linked_record_ids"] == ["rec_001", "rec_002"]
        assert digest["linked_quant_ids"] == ["quant_001"]
        assert digest["confidence"] == 80
        assert digest["notes"] == "Test notes"
        assert digest["date_range"]["start"] == "2026-03-25"
        assert digest["date_range"]["end"] == "2026-03-25"

    def test_uses_weekly_digest_id_for_weekly_type(self, tmp_path):
        """Should use weekly digest ID format for weekly digest type."""
        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()
        (schemas_dir / "digest_record.json").write_text(
            json.dumps(
                {
                    "digest_id": "",
                    "digest_type": "",
                    "date_range": {"start": "", "end": ""},
                    "summary": "",
                    "key_themes": [],
                    "linked_record_ids": [],
                    "linked_quant_ids": [],
                    "confidence": 0,
                    "notes": "",
                }
            ),
            encoding="utf-8",
        )

        with patch("scripts.digest_utils.SCHEMAS_DIR", schemas_dir):
            digest = build_digest_record(
                digest_type="weekly",
                start_date=date(2026, 3, 23),
                end_date=date(2026, 3, 29),
                summary="Weekly summary",
                key_themes=["inflation"],
                linked_record_ids=[],
                linked_quant_ids=[],
                confidence=75,
            )

        assert digest["digest_id"] == "weekly_2026_03_23_to_2026_03_29"

    def test_uses_daily_digest_id_for_daily_types(self, tmp_path):
        """Should use daily digest ID format for non-weekly types."""
        schemas_dir = tmp_path / "schemas"
        schemas_dir.mkdir()
        (schemas_dir / "digest_record.json").write_text(
            json.dumps(
                {
                    "digest_id": "",
                    "digest_type": "",
                    "date_range": {"start": "", "end": ""},
                    "summary": "",
                    "key_themes": [],
                    "linked_record_ids": [],
                    "linked_quant_ids": [],
                    "confidence": 0,
                    "notes": "",
                }
            ),
            encoding="utf-8",
        )

        with patch("scripts.digest_utils.SCHEMAS_DIR", schemas_dir):
            digest = build_digest_record(
                digest_type="daily_market_structure",
                start_date=date(2026, 3, 25),
                end_date=date(2026, 3, 25),
                summary="Daily summary",
                key_themes=["liquidity"],
                linked_record_ids=[],
                linked_quant_ids=[],
                confidence=70,
            )

        assert digest["digest_id"] == "daily_market_structure_2026_03_25"


class TestSaveDigestRecord:
    """Tests for save_digest_record function."""

    def test_saves_digest_to_json_file(self, tmp_path):
        """Should save digest record to JSON file."""
        output_dir = tmp_path / "digests" / "daily_macro"
        digest = {
            "digest_id": "daily_macro_2026_03_25",
            "digest_type": "daily_macro",
            "summary": "Test summary",
        }

        result = save_digest_record(digest, output_dir)

        assert result == output_dir / "daily_macro_2026_03_25.json"
        assert result.exists()

        saved = json.loads(result.read_text(encoding="utf-8"))
        assert saved["digest_id"] == "daily_macro_2026_03_25"

    def test_creates_parent_directories(self, tmp_path):
        """Should create parent directories if they don't exist."""
        output_dir = tmp_path / "nested" / "path" / "digests"
        digest = {"digest_id": "test_digest"}

        result = save_digest_record(digest, output_dir)

        assert result.exists()

    def test_uses_digest_id_for_filename(self, tmp_path):
        """Should use digest_id field for filename."""
        output_dir = tmp_path / "digests"
        digest = {"digest_id": "custom_id_123"}

        result = save_digest_record(digest, output_dir)

        assert result.name == "custom_id_123.json"


class TestBuildRecordsContext:
    """Tests for build_records_context function."""

    def test_returns_no_records_message_when_empty(self):
        """Should return 'No records available' message for empty list."""
        context = build_records_context([])
        assert context == "No records available for this period."

    def test_includes_record_details(self):
        """Should include record ID, title, topic, source, summary."""
        records = [
            {
                "id": "test_001",
                "title": "Test Title",
                "topic": "macro",
                "source": {"name": "Test Source"},
                "summary": "Test summary",
                "key_points": ["Point 1", "Point 2"],
                "why_it_matters": "Why it matters text",
                "macro_context": "Macro context text",
                "market_structure_context": "MS context text",
                "tags": ["tag1", "tag2"],
            }
        ]

        context = build_records_context(records)

        assert "Record 1" in context
        assert "test_001" in context
        assert "Test Title" in context
        assert "macro" in context
        assert "Test Source" in context
        assert "Test summary" in context
        assert "Point 1" in context
        assert "Why it matters text" in context

    def test_respects_max_records_limit(self):
        """Should limit records to max_records."""
        records = [
            {
                "id": f"record_{i:03d}",
                "title": f"Record {i}",
                "quality_tier": {"score": i * 10},
            }
            for i in range(25)
        ]

        context = build_records_context(records, max_records=10)

        # Should contain exactly 10 records (highest quality scores first)
        # Sorted by score descending: record_024 (240), record_023 (230), ..., record_015 (150)
        assert "record_024" in context  # Highest score first
        assert "record_015" in context  # 10th highest (score 150)
        # record_014 (score 140, 11th highest) should NOT be present
        assert "record_014" not in context

    def test_sorts_by_quality_tier_score_descending(self):
        """Should sort records by quality_tier.score descending."""
        records = [
            {"id": "low", "title": "Low Quality", "quality_tier": {"score": 30}},
            {"id": "high", "title": "High Quality", "quality_tier": {"score": 90}},
            {"id": "medium", "title": "Medium Quality", "quality_tier": {"score": 60}},
        ]

        context = build_records_context(records)

        high_pos = context.find("High Quality")
        medium_pos = context.find("Medium Quality")
        low_pos = context.find("Low Quality")

        assert high_pos < medium_pos < low_pos

    def test_includes_linked_quants_when_present(self):
        """Should include linked quant IDs in context."""
        records = [
            {
                "id": "article_001",
                "title": "Article with Quants",
                "linked_quant_context": [
                    {"record_id": "quant_001"},
                    {"record_id": "quant_002"},
                ],
            }
        ]

        context = build_records_context(records)

        assert "Linked Quants:" in context
        assert "quant_001" in context
        assert "quant_002" in context


class TestGetWeekRange:
    """Tests for get_week_range function."""

    def test_monday_to_sunday_week(self):
        """Should return Monday as start and Sunday as end."""
        # March 25, 2026 is a Wednesday
        monday, sunday = get_week_range(date(2026, 3, 25))

        assert monday == date(2026, 3, 23)
        assert sunday == date(2026, 3, 29)

    def test_week_starting_monday(self):
        """Should return same Monday when input is Monday."""
        monday, sunday = get_week_range(date(2026, 3, 23))

        assert monday == date(2026, 3, 23)
        assert sunday == date(2026, 3, 29)

    def test_week_ending_sunday(self):
        """Should return same Sunday when input is Sunday."""
        monday, sunday = get_week_range(date(2026, 3, 29))

        assert monday == date(2026, 3, 23)
        assert sunday == date(2026, 3, 29)

    def test_defaults_to_today_when_none_provided(self):
        """Should use today's date when ref_date is None."""
        today = date.today()
        monday, sunday = get_week_range()

        assert monday <= today <= sunday
        assert (sunday - monday).days == 6


class TestFormatThemeSummary:
    """Tests for format_theme_summary function."""

    def test_returns_no_themes_message_when_empty(self):
        """Should return 'No themes identified' for empty groups."""
        result = format_theme_summary({})
        assert result == "No themes identified."

    def test_formats_theme_with_records(self):
        """Should format theme with its record titles."""
        groups = {
            "inflation": [
                {"id": "inf_001", "title": "CPI Report"},
                {"id": "inf_002", "title": "PPI Data"},
            ]
        }

        result = format_theme_summary(groups)

        assert "Theme: Inflation" in result
        assert "Records: 2" in result
        assert "CPI Report" in result
        assert "inf_001" in result

    def test_formats_multiple_themes(self):
        """Should format multiple themes."""
        groups = {
            "inflation": [{"id": "inf_001", "title": "CPI Report"}],
            "labor_growth": [{"id": "lab_001", "title": "Jobs Report"}],
        }

        result = format_theme_summary(groups)

        assert "Inflation" in result
        assert "Labor Growth" in result

    def test_sorts_themes_alphabetically(self):
        """Should sort themes alphabetically."""
        groups = {
            "labor_growth": [{"id": "lab_001", "title": "Jobs Report"}],
            "inflation": [{"id": "inf_001", "title": "CPI Data"}],
            "central_bank": [{"id": "cb_001", "title": "Fed Statement"}],
        }

        result = format_theme_summary(groups)

        # Extract theme header lines to verify order
        lines = result.split("\n")
        theme_lines = [l for l in lines if l.startswith("## Theme:")]

        # Should be in alphabetical order: Central Bank, Inflation, Labor Growth
        assert len(theme_lines) == 3
        assert theme_lines[0] == "## Theme: Central Bank"
        assert theme_lines[1] == "## Theme: Inflation"
        assert theme_lines[2] == "## Theme: Labor Growth"


class TestExtractJsonFromResponse:
    """Tests for extract_json_from_response function."""

    def test_parses_simple_json(self):
        """Should parse a simple JSON object."""
        text = '{"summary": "Test summary", "key_themes": ["theme1"]}'
        result = extract_json_from_response(text)

        assert result == {"summary": "Test summary", "key_themes": ["theme1"]}

    def test_parses_json_with_whitespace(self):
        """Should parse JSON with leading/trailing whitespace."""
        text = '   \n{"summary": "Test"}   \n'
        result = extract_json_from_response(text)

        assert result == {"summary": "Test"}

    def test_extracts_json_from_markdown_fence(self):
        """Should extract JSON from markdown code fence."""
        text = '```json\n{"summary": "Test"}\n```'
        result = extract_json_from_response(text)

        assert result == {"summary": "Test"}

    def test_extracts_json_from_text_with_prefix(self):
        """Should extract JSON from text with prefix content."""
        text = 'Here is the result:\n{"summary": "Test", "value": 123}'
        result = extract_json_from_response(text)

        assert result == {"summary": "Test", "value": 123}

    def test_extracts_json_from_text_with_suffix(self):
        """Should extract JSON from text with suffix content."""
        text = '{"summary": "Test"}\n\nThis is the end.'
        result = extract_json_from_response(text)

        assert result == {"summary": "Test"}

    def test_raises_error_for_invalid_json(self):
        """Should raise ValueError for invalid JSON."""
        text = "This is not JSON at all { invalid"

        with pytest.raises(ValueError, match="did not contain valid JSON"):
            extract_json_from_response(text)

    def test_raises_error_for_array_response(self):
        """Should raise ValueError when JSON is an array, not object."""
        text = '["item1", "item2", "item3"]'

        with pytest.raises(ValueError, match="did not contain valid JSON"):
            extract_json_from_response(text)


class TestCallMinimaxForSynthesis:
    """Tests for call_minimax_for_synthesis function."""

    @patch("scripts.digest_utils.OpenAI")
    @patch("scripts.digest_utils.load_dotenv")
    def test_calls_minimax_with_correct_params(self, mock_load_dotenv, mock_openai):
        """Should call MiniMax API with correct parameters."""
        # Set up mock
        mock_client = MagicMock()
        mock_openai.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[
            0
        ].message.content = '{"summary": "Test", "key_themes": ["test"]}'
        mock_client.chat.completions.create.return_value = mock_response

        # Mock environment
        with patch.dict(
            "os.environ",
            {
                "OPENAI_API_KEY": "test_key",
                "OPENAI_BASE_URL": "https://api.minimax.io/v1",
                "MINIMAX_MODEL": "MiniMax-M2.7",
            },
        ):
            result = call_minimax_for_synthesis("Test prompt", "Test context")

        assert result == {"summary": "Test", "key_themes": ["test"]}
        mock_client.chat.completions.create.assert_called_once()

    @patch("scripts.digest_utils.load_dotenv")
    def test_raises_error_when_api_key_missing(self, mock_load_dotenv):
        """Should raise EnvironmentError when OPENAI_API_KEY is missing."""
        with patch.dict("os.environ", {"OPENAI_API_KEY": ""}, clear=True):
            with pytest.raises(EnvironmentError, match="OPENAI_API_KEY is missing"):
                call_minimax_for_synthesis("Test prompt", "Test context")

    @patch("scripts.digest_utils.OpenAI")
    @patch("scripts.digest_utils.load_dotenv")
    def test_retries_on_invalid_json(self, mock_load_dotenv, mock_openai):
        """Should retry when initial response is not valid JSON."""
        mock_client = MagicMock()
        mock_openai.return_value = mock_client

        # First response is invalid JSON, second is valid
        invalid_response = MagicMock()
        invalid_response.choices = [MagicMock()]
        invalid_response.choices[0].message.content = "Not valid JSON"

        valid_response = MagicMock()
        valid_response.choices = [MagicMock()]
        valid_response.choices[
            0
        ].message.content = '{"summary": "Valid", "key_themes": []}'

        mock_client.chat.completions.create.side_effect = [
            invalid_response,
            valid_response,
        ]

        with patch.dict(
            "os.environ",
            {
                "OPENAI_API_KEY": "test_key",
                "OPENAI_BASE_URL": "https://api.minimax.io/v1",
                "MINIMAX_MODEL": "MiniMax-M2.7",
            },
        ):
            result = call_minimax_for_synthesis(
                "Test prompt", "Test context", max_retries=1
            )

        assert result == {"summary": "Valid", "key_themes": []}
