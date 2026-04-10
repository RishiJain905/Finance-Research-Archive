"""Tests for the SEC EDGAR ingestor (scripts/ingest_edgar.py)."""

import importlib.util
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

BASE_DIR = Path(__file__).resolve().parent.parent
MODULE_PATH = BASE_DIR / "scripts" / "ingest_edgar.py"
MODULE_SPEC = importlib.util.spec_from_file_location("ingest_edgar", MODULE_PATH)
assert MODULE_SPEC and MODULE_SPEC.loader
ingest_edgar = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(ingest_edgar)


class TestPadCik:
    def test_pads_to_10_digits(self):
        assert ingest_edgar.pad_cik("19617") == "0000019617"
        assert ingest_edgar.pad_cik("886982") == "0000886982"

    def test_preserves_leading_zeros(self):
        assert ingest_edgar.pad_cik("0000019617") == "0000019617"

    def test_strips_extraneous_leading_zeros_before_padding(self):
        assert ingest_edgar.pad_cik("000000019617") == "0000019617"


class TestParseRecentFilings:
    def _make_submissions(self, filings):
        return {"filings": {"recent": filings}}

    def test_keeps_matching_form_types(self):
        submissions = self._make_submissions({
            "accessionNumber": ["0000019617-24-000001"],
            "form": ["8-K"],
            "filingDate": [datetime.now(timezone.utc).strftime("%Y-%m-%d")],
            "primaryDocument": ["d8k.htm"],
        })
        result = ingest_edgar.parse_recent_filings(submissions, ["8-K", "10-K"], lookback_days=3)
        assert len(result) == 1
        assert result[0]["form_type"] == "8-K"

    def test_rejects_non_matching_form_types(self):
        submissions = self._make_submissions({
            "accessionNumber": ["0000019617-24-000001"],
            "form": ["SC 13G"],
            "filingDate": [datetime.now(timezone.utc).strftime("%Y-%m-%d")],
            "primaryDocument": ["sc13g.htm"],
        })
        result = ingest_edgar.parse_recent_filings(submissions, ["8-K", "10-K"], lookback_days=3)
        assert result == []

    def test_respects_lookback_days(self):
        old_date = (datetime.now(timezone.utc) - timedelta(days=10)).strftime("%Y-%m-%d")
        submissions = self._make_submissions({
            "accessionNumber": ["0000019617-24-000001"],
            "form": ["8-K"],
            "filingDate": [old_date],
            "primaryDocument": ["d8k.htm"],
        })
        result = ingest_edgar.parse_recent_filings(submissions, ["8-K"], lookback_days=3)
        assert result == []


class TestBuildDocumentUrl:
    def test_standard_url_format(self):
        url = ingest_edgar.build_document_url(
            "0000019617", "0000019617-24-000001", "d8k.htm"
        )
        # EDGAR uses CIK without leading zeros and accession without dashes
        assert url == (
            "https://www.sec.gov/Archives/edgar/data/"
            "19617/000001961724000001/d8k.htm"
        )


class TestBuildRawRecordText:
    def test_title_format(self):
        text = ingest_edgar.build_raw_record_text(
            company_name="JPMorgan Chase",
            form_type="8-K",
            filing_date="2024-01-15",
            doc_url="https://www.sec.gov/Archives/edgar/data/0000019617/0000019617-24-000001/d8k.htm",
            content="Some filing content.",
        )
        assert "JPMorgan Chase 8-K Filing — 2024-01-15" in text

    def test_source_field(self):
        text = ingest_edgar.build_raw_record_text(
            "JPMorgan Chase", "8-K", "2024-01-15", "http://example.com", "content"
        )
        lines = text.split("\n")
        assert any(line.startswith("source:") for line in lines)


class TestExtractTextFromHtml:
    def test_removes_html_tags(self):
        html = "<p>Hello <strong>World</strong></p>"
        text = ingest_edgar.extract_text_from_html(html)
        assert "<" not in text
        assert "Hello" in text
        assert "World" in text

    def test_preserves_paragraph_breaks(self):
        html = "<p>Para 1</p><p>Para 2</p>"
        text = ingest_edgar.extract_text_from_html(html)
        assert "Para 1" in text
        assert "Para 2" in text


class TestRunOneCompany:
    """Tests for the per-company ingestion logic (no network)."""

    @pytest.fixture
    def mock_session(self):
        return MagicMock()

    @pytest.fixture
    def sample_submissions(self):
        return {
            "filings": {
                "recent": {
                    "accessionNumber": ["0000019617-24-000001"],
                    "form": ["8-K"],
                    "filingDate": [datetime.now(timezone.utc).strftime("%Y-%m-%d")],
                    "primaryDocument": ["d8k.htm"],
                }
            }
        }

    @patch("scripts.ingest_edgar.is_url_seen")
    @patch("scripts.ingest_edgar.fetch_submissions")
    def test_skips_already_seen_url(self, mock_fetch, mock_is_seen, mock_session, sample_submissions):
        mock_is_seen.return_value = True
        mock_fetch.return_value = sample_submissions

        result = ingest_edgar.run_one_company(
            mock_session,
            {"name": "JPMorgan Chase", "cik": "0000019617"},
            ["8-K"],
            lookback_days=3,
            max_filings=5,
        )
        assert result == []