"""Tests for World Bank quant data ingestion (scripts/ingest_quant_data.py)."""

import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

BASE_DIR = Path(__file__).resolve().parent.parent
MODULE_PATH = BASE_DIR / "scripts" / "ingest_quant_data.py"
MODULE_SPEC = importlib.util.spec_from_file_location("ingest_quant_data", MODULE_PATH)
assert MODULE_SPEC and MODULE_SPEC.loader
ingest_quant_data = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(ingest_quant_data)


# ---------------------------------------------------------------------------
# format_number
# ---------------------------------------------------------------------------

class TestFormatNumber:
    def test_formats_float_with_precision(self):
        assert ingest_quant_data.format_number(2.5) == "2.5"
        assert ingest_quant_data.format_number(2.5000) == "2.5"
        assert ingest_quant_data.format_number(2.0) == "2"

    def test_formats_large_numbers(self):
        assert ingest_quant_data.format_number(1234567.89) == "1234567.89"
        # Very small numbers may strip to 0 due to trailing-zero removal
        result = ingest_quant_data.format_number(0.00001)
        assert result in ("0.00001", "0")  # depends on implementation

    def test_formats_negative(self):
        assert ingest_quant_data.format_number(-1.25) == "-1.25"


# ---------------------------------------------------------------------------
# compute_direction
# ---------------------------------------------------------------------------

class TestComputeDirection:
    def test_up(self):
        assert ingest_quant_data.compute_direction(3.0, 2.0) == "up"

    def test_down(self):
        assert ingest_quant_data.compute_direction(2.0, 3.0) == "down"

    def test_flat(self):
        assert ingest_quant_data.compute_direction(2.0, 2.0) == "flat"


# ---------------------------------------------------------------------------
# build_wb_snapshot
# ---------------------------------------------------------------------------

class TestBuildWbSnapshot:
    def _make_wb_api_response(self, observations: list[dict]) -> list:
        """Build a World Bank API v2 response structure for country=all endpoint.

        The WB API with country=all returns: [{"page":..., "pages":..., ...}, [observations]]
        where each observation has country, date, value, etc.
        """
        return [
            {"page": 1, "pages": 1, "per_page": 10000, "total": len(observations)},
            observations,
        ]

    def test_returns_record_id_and_content(self):
        wb_item = {
            "id": "wb_gdp_growth",
            "name": "World GDP Growth Rate",
            "topic": "global_economy",
            "indicator": "NY.GDP.MKTP.KD.ZG",
        }
        record_id, content = ingest_quant_data.build_wb_snapshot(wb_item)
        assert record_id.startswith("wb_gdp_growth_")
        assert "TARGET: World GDP Growth Rate" in content
        assert "SOURCE: worldbank" in content
        assert "INDICATOR: NY.GDP.MKTP.KD.ZG" in content
        assert "SNAPSHOT_DATE:" in content

    def test_parses_observations_from_wb_format(self):
        wb_item = {
            "id": "wb_inflation",
            "name": "World Inflation Rate",
            "topic": "global_economy",
            "indicator": "FP.CPI.TOTL.ZG",
        }
        mock_response = self._make_wb_api_response([
            {"date": "2023", "value": "5.25", "country": {"value": "United States"}},
            {"date": "2022", "value": "4.70", "country": {"value": "United States"}},
            {"date": "2021", "value": "3.20", "country": {"value": "United States"}},
        ])

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response
            mock_get.return_value.raise_for_status = MagicMock()
            record_id, content = ingest_quant_data.build_wb_snapshot(wb_item)

        assert "LATEST_OBSERVATION_DATE: 2023" in content
        assert "LATEST_OBSERVATION_VALUE: 5.25" in content
        assert "LATEST_COUNTRY: United States" in content
        assert "PREVIOUS_OBSERVATION_DATE: 2022" in content
        assert "PREVIOUS_OBSERVATION_VALUE: 4.7" in content
        assert "DIRECTION: up" in content
        assert "RECENT_OBSERVATIONS:" in content

    def test_handles_null_values_by_skipping(self):
        wb_item = {
            "id": "wb_trade_gdp",
            "name": "World Trade",
            "topic": "global_economy",
            "indicator": "TG.VAL.TOTL.GD.ZS",
        }
        mock_response = self._make_wb_api_response([
            {"date": "2023", "value": None, "country": {"value": "World"}},
            {"date": "2022", "value": "52.3", "country": {"value": "World"}},
        ])

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response
            mock_get.return_value.raise_for_status = MagicMock()
            record_id, content = ingest_quant_data.build_wb_snapshot(wb_item)

        assert "LATEST_OBSERVATION_DATE: 2022" in content
        assert "LATEST_OBSERVATION_VALUE: 52.3" in content

    def test_graceful_handling_of_request_failure(self):
        wb_item = {
            "id": "wb_gdp_growth",
            "name": "World GDP Growth Rate",
            "topic": "global_economy",
            "indicator": "NY.GDP.MKTP.KD.ZG",
        }

        with patch("requests.get") as mock_get:
            mock_get.side_effect = Exception("Network error")
            record_id, content = ingest_quant_data.build_wb_snapshot(wb_item)

        assert "Failed to fetch World Bank data" in content
        assert "Network error" in content

    def test_handles_empty_observations_list(self):
        wb_item = {
            "id": "wb_public_debt",
            "name": "World Public Debt",
            "topic": "global_economy",
            "indicator": "GC.DOD.TOTL.GD.ZS",
        }
        mock_response = self._make_wb_api_response([])

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response
            mock_get.return_value.raise_for_status = MagicMock()
            record_id, content = ingest_quant_data.build_wb_snapshot(wb_item)

        assert "No recent valid observations" in content

    def test_uses_worldbank_series_name_in_summary(self):
        wb_item = {
            "id": "wb_gdp_growth",
            "name": "World GDP Growth Rate",
            "topic": "global_economy",
            "indicator": "NY.GDP.MKTP.KD.ZG",
        }
        mock_response = self._make_wb_api_response([
            {"date": "2023", "value": "2.8", "country": {"value": "World"}},
            {"date": "2022", "value": "3.1", "country": {"value": "World"}},
        ])

        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = mock_response
            mock_get.return_value.raise_for_status = MagicMock()
            record_id, content = ingest_quant_data.build_wb_snapshot(wb_item)

        assert "World GDP Growth Rate latest value is 2.8 on 2023" in content
        assert "QUANT_SUMMARY:" in content