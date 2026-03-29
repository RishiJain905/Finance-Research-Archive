"""Tests for seed_sites.json configuration validation.

Tests cover:
- Configuration file structure validation
- Required fields presence
- Seed entry validation
"""

import json
from pathlib import Path
from unittest.mock import patch, mock_open

import pytest


class TestSeedSitesConfig:
    """Test suite for seed_sites.json configuration validation."""

    def test_valid_seed_sites_config_structure(self, tmp_path):
        """Test that a valid seed_sites.json has correct structure."""
        config = {
            "seeds": [
                {
                    "id": "newyorkfed",
                    "domain": "newyorkfed.org",
                    "enabled": True,
                    "priority": "high",
                    "start_urls": [
                        "https://www.newyorkfed.org/markets",
                        "https://www.newyorkfed.org/newsevents",
                    ],
                    "crawl_patterns": ["/markets/", "/newsevents/", "/research/"],
                    "topic": "monetary policy",
                    "trust_tier": "high",
                },
                {
                    "id": "bis",
                    "domain": "bis.org",
                    "enabled": True,
                    "priority": "medium",
                    "start_urls": ["https://www.bis.org/publ/"],
                    "crawl_patterns": ["/publ/", "/research/"],
                    "topic": "market structure",
                    "trust_tier": "medium",
                },
            ]
        }

        config_file = tmp_path / "seed_sites.json"
        config_file.write_text(json.dumps(config))

        # Import after file exists
        import sys

        sys.path.insert(0, str(tmp_path.parent.parent))

        # Test validation logic directly
        from scripts.run_seed_crawl import validate_seed_config

        errors = validate_seed_config(config)
        assert len(errors) == 0

    def test_missing_seeds_field(self, tmp_path):
        """Test that config without 'seeds' field fails validation."""
        config = {
            "queries": []  # Wrong field name
        }

        from scripts.run_seed_crawl import validate_seed_config

        errors = validate_seed_config(config)
        assert len(errors) > 0
        assert any("seeds" in err.lower() for err in errors)

    def test_seed_missing_required_fields(self, tmp_path):
        """Test that seed entry missing required fields fails validation."""
        config = {
            "seeds": [
                {
                    "id": "incomplete",
                    # missing domain, start_urls, etc.
                }
            ]
        }

        from scripts.run_seed_crawl import validate_seed_config

        errors = validate_seed_config(config)
        assert len(errors) > 0

    def test_seed_invalid_domain_format(self, tmp_path):
        """Test that seed with invalid domain format fails validation."""
        config = {
            "seeds": [
                {
                    "id": "bad_domain",
                    "domain": "not-a-valid-domain",  # Invalid
                    "enabled": True,
                    "start_urls": ["https://example.com"],
                    "crawl_patterns": [],
                    "topic": "test",
                    "trust_tier": "low",
                }
            ]
        }

        from scripts.run_seed_crawl import validate_seed_config

        errors = validate_seed_config(config)
        assert len(errors) > 0
        assert any("domain" in err.lower() for err in errors)

    def test_seed_invalid_url_in_start_urls(self, tmp_path):
        """Test that seed with invalid start_urls fails validation."""
        config = {
            "seeds": [
                {
                    "id": "bad_url",
                    "domain": "example.com",
                    "enabled": True,
                    "start_urls": ["not-a-valid-url"],
                    "crawl_patterns": [],
                    "topic": "test",
                    "trust_tier": "low",
                }
            ]
        }

        from scripts.run_seed_crawl import validate_seed_config

        errors = validate_seed_config(config)
        assert len(errors) > 0
        assert any("url" in err.lower() for err in errors)

    def test_disabled_seed_passes_validation(self, tmp_path):
        """Test that disabled seeds still pass validation (they're just skipped)."""
        config = {
            "seeds": [
                {
                    "id": "disabled_seed",
                    "domain": "example.com",
                    "enabled": False,  # Disabled
                    "start_urls": ["https://example.com"],
                    "crawl_patterns": [],
                    "topic": "test",
                    "trust_tier": "low",
                }
            ]
        }

        from scripts.run_seed_crawl import validate_seed_config

        errors = validate_seed_config(config)
        # Disabled seeds should still be valid structure
        assert len(errors) == 0

    def test_empty_seeds_list(self, tmp_path):
        """Test that empty seeds list passes validation (just means nothing to crawl)."""
        config = {"seeds": []}

        from scripts.run_seed_crawl import validate_seed_config

        errors = validate_seed_config(config)
        assert len(errors) == 0


class TestSeedSitesConfigLoading:
    """Tests for loading seed_sites.json configuration."""

    def test_load_seed_config_success(self, tmp_path):
        """Test successfully loading a valid seed config."""
        config_data = {
            "seeds": [
                {
                    "id": "test_seed",
                    "domain": "example.com",
                    "enabled": True,
                    "start_urls": ["https://example.com"],
                    "crawl_patterns": ["/page/"],
                    "topic": "test",
                    "trust_tier": "low",
                }
            ]
        }

        # Mock the file read
        mock_file_content = json.dumps(config_data)

        with patch("builtins.open", mock_open(read_data=mock_file_content)):
            with patch("pathlib.Path.exists", return_value=True):
                from scripts.run_seed_crawl import load_seed_config

                config = load_seed_config(Path("config/seed_sites.json"))
                assert "seeds" in config
                assert len(config["seeds"]) == 1
                assert config["seeds"][0]["id"] == "test_seed"

    def test_load_seed_config_file_not_found(self):
        """Test that FileNotFoundError is raised when config missing."""
        from scripts.run_seed_crawl import load_seed_config

        with pytest.raises(FileNotFoundError):
            load_seed_config(Path("/nonexistent/path/seed_sites.json"))

    def test_load_seed_config_invalid_json(self):
        """Test that ValueError is raised for invalid JSON."""
        from scripts.run_seed_crawl import load_seed_config

        with patch("builtins.open", mock_open(read_data="not valid json {")):
            with patch("pathlib.Path.exists", return_value=True):
                with pytest.raises(ValueError):
                    load_seed_config(Path("config/seed_sites.json"))
