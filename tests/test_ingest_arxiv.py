"""Tests for the arXiv ingestion script (scripts/ingest_arxiv.py)."""

import importlib.util
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

BASE_DIR = Path(__file__).resolve().parent.parent
MODULE_PATH = BASE_DIR / "scripts" / "ingest_arxiv.py"
MODULE_SPEC = importlib.util.spec_from_file_location("ingest_arxiv", MODULE_PATH)
assert MODULE_SPEC and MODULE_SPEC.loader
ingest_arxiv = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(ingest_arxiv)


# ---------------------------------------------------------------------------
# parse_arxiv_id_from_url
# ---------------------------------------------------------------------------

class TestParseArxivIdFromUrl:
    def test_parses_standard_url(self):
        url = "https://arxiv.org/abs/2403.12345"
        assert ingest_arxiv.parse_arxiv_id_from_url(url) == "2403.12345"

    def test_parses_url_with_version(self):
        url = "https://arxiv.org/abs/2301.01234v2"
        assert ingest_arxiv.parse_arxiv_id_from_url(url) == "2301.01234"

    def test_parses_url_with_trailing_slash(self):
        url = "https://arxiv.org/abs/2401.00001/"
        assert ingest_arxiv.parse_arxiv_id_from_url(url) == "2401.00001"

    def test_returns_empty_for_unknown_format(self):
        assert ingest_arxiv.parse_arxiv_id_from_url("https://arxiv.org/abs/") == ""
        assert ingest_arxiv.parse_arxiv_id_from_url("https://example.com/2403.12345") == ""


# ---------------------------------------------------------------------------
# parse_atom_feed
# ---------------------------------------------------------------------------

class TestParseAtomFeed:
    def _make_atom_entry(self, paper_id, title, abstract, published, category="q-fin.RM"):
        published_str = published.isoformat()
        return f"""<entry xmlns="http://www.w3.org/2005/Atom">
  <id>https://arxiv.org/abs/{paper_id}</id>
  <title>{title}</title>
  <summary>{abstract}</summary>
  <published>{published_str}</published>
  <author><name>John Doe</name></author>
  <author><name>Jane Smith</name></author>
  <arxiv:primary_category term="{category}" xmlns:arxiv="http://arxiv.org/schemas/atom"/>
</entry>"""

    def _make_atom_feed(self, entries_xml: str) -> str:
        return f'''<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
{entries_xml}
</feed>'''

    def test_parses_valid_entry(self):
        recent = datetime.now(timezone.utc)
        entry = self._make_atom_entry(
            "2403.12345",
            "Risk Management in Portfolio Theory",
            "This paper studies risk management.",
            recent,
        )
        feed = self._make_atom_feed(entry)
        result = ingest_arxiv.parse_atom_feed(feed, lookback_days=5)
        assert len(result) == 1
        assert result[0]["id"] == "2403.12345"
        assert result[0]["title"] == "Risk Management in Portfolio Theory"
        assert result[0]["summary"] == "This paper studies risk management."
        assert result[0]["authors"] == ["John Doe", "Jane Smith"]
        assert result[0]["primary_category"] == "q-fin.RM"
        assert result[0]["url"] == "https://arxiv.org/abs/2403.12345"

    def test_filters_entries_outside_lookback(self):
        old = datetime.now(timezone.utc) - timedelta(days=10)
        entry = self._make_atom_entry("2403.99999", "Old Paper", "Abstract.", old)
        feed = self._make_atom_feed(entry)
        result = ingest_arxiv.parse_atom_feed(feed, lookback_days=5)
        assert result == []

    def test_skips_entries_without_id(self):
        recent = datetime.now(timezone.utc)
        feed = self._make_atom_feed(f"""<entry>
  <title>No ID Paper</title>
  <summary>Abstract.</summary>
  <published>{recent.isoformat()}</published>
</entry>""")
        result = ingest_arxiv.parse_atom_feed(feed, lookback_days=5)
        assert result == []

    def test_handles_multiple_entries(self):
        recent = datetime.now(timezone.utc)
        entries = (
            self._make_atom_entry("2403.11111", "Paper One", "Abstract one.", recent)
            + self._make_atom_entry("2403.22222", "Paper Two", "Abstract two.", recent)
        )
        feed = self._make_atom_feed(entries)
        result = ingest_arxiv.parse_atom_feed(feed, lookback_days=5)
        assert len(result) == 2
        ids = {r["id"] for r in result}
        assert ids == {"2403.11111", "2403.22222"}


# ---------------------------------------------------------------------------
# build_arxiv_record_text
# ---------------------------------------------------------------------------

class TestBuildArxivRecordText:
    def test_includes_required_headers(self):
        paper = {
            "id": "2403.12345",
            "title": "A Study of Portfolio Optimization",
            "summary": "This paper investigates portfolio optimization under uncertainty.",
            "authors": ["John Doe", "Jane Smith"],
            "published_date": datetime.now(timezone.utc).isoformat(),
            "primary_category": "q-fin.PM",
            "url": "https://arxiv.org/abs/2403.12345",
        }
        output = ingest_arxiv.build_arxiv_record_text(
            paper, "quantitative_finance", "arXiv q-fin.PM"
        )
        assert "TARGET: arXiv q-fin.PM" in output
        assert "TOPIC: quantitative_finance" in output
        assert "PAPER_ID: 2403.12345" in output
        assert "PRIMARY_CATEGORY: q-fin.PM" in output
        assert "AUTHORS: John Doe; Jane Smith" in output
        assert "SOURCE_TYPE: academic" in output
        assert "INGEST_SOURCE: arxiv" in output
        assert "PAGE_TYPE: academic_paper" in output
        assert "A Study of Portfolio Optimization" in output
        assert "This paper investigates portfolio optimization under uncertainty." in output


# ---------------------------------------------------------------------------
# Integration-level: _run_arxiv with mocked API
# ---------------------------------------------------------------------------

class TestRunArxiv:
    def test_creates_records_for_new_papers(self, tmp_path):
        """When fetch returns fresh papers, _run_arxiv writes raw records and returns their IDs."""
        paper = {
            "id": "2403.12345",
            "title": "Quantitative Finance Strategies",
            "summary": "Paper abstract here.",
            "authors": ["Jane Doe"],
            "published_date": datetime.now(timezone.utc).isoformat(),
            "primary_category": "q-fin.RM",
            "url": "https://arxiv.org/abs/2403.12345",
        }

        raw_dir = tmp_path / "data" / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        with patch.object(ingest_arxiv, "fetch_arxiv_feed", return_value=[paper]):
            with patch.object(ingest_arxiv, "is_url_processed_as_article", return_value=False):
                with patch.object(ingest_arxiv, "upsert_seen_url"):
                    with patch.object(ingest_arxiv, "set_record_map"):
                        with patch.object(ingest_arxiv, "set_record_rules"):
                            with patch.object(ingest_arxiv, "add_fingerprint"):
                                with patch.object(ingest_arxiv, "get_fingerprint_record_id", return_value=None):
                                    with patch.object(ingest_arxiv, "content_hash", return_value="hash123"):
                                        with patch.object(ingest_arxiv, "title_fingerprint", return_value="fptitle"):
                                            with patch.object(ingest_arxiv, "RAW_DIR", raw_dir):
                                                config = {
                                                    "lookback_days": 5,
                                                    "max_results_per_category": 10,
                                                    "categories": ["q-fin.RM"],
                                                }
                                                created = ingest_arxiv._run_arxiv(config)
                                                assert len(created) == 1
                                                assert (raw_dir / f"{created[0]}.txt").exists()

    def test_returns_empty_when_fetch_returns_nothing(self):
        """When fetch returns an empty list, _run_arxiv returns no records."""
        with patch.object(ingest_arxiv, "fetch_arxiv_feed", return_value=[]):
            config = {
                "lookback_days": 5,
                "max_results_per_category": 10,
                "categories": ["q-fin.RM"],
            }
            created = ingest_arxiv._run_arxiv(config)
            assert created == []


# ---------------------------------------------------------------------------
# Config loading (academic_sources.json)
# ---------------------------------------------------------------------------

class TestAcademicSourcesConfig:
    def test_config_structure(self, tmp_path):
        config_path = tmp_path / "academic_sources.json"
        config_path.write_text(json.dumps({
            "arxiv": {
                "enabled": True,
                "categories": ["q-fin.RM"],
                "lookback_days": 5,
                "max_results_per_category": 10
            },
            "ssrn": {
                "enabled": True,
                "feeds": [{"name": "SSRN Finance", "url": "https://example.com/rss", "enabled": True}],
                "max_entries_per_feed": 10
            }
        }))

        with patch.object(ingest_arxiv, "CONFIG_PATH", config_path):
            from ingest_sources import load_json
            config = load_json(config_path, {"arxiv": {}, "ssrn": {}})
            assert config["arxiv"]["enabled"] is True
            assert "q-fin.RM" in config["arxiv"]["categories"]
            assert config["ssrn"]["feeds"][0]["name"] == "SSRN Finance"
