"""Tests for seed crawl lane orchestration script.

Tests cover:
- run_seed_crawl module loading and configuration
- Seed crawling orchestration
- Stats tracking and logging
- Integration with shared candidate pipeline
"""

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open

import pytest


class TestRunSeedCrawlImports:
    """Test that run_seed_crawl module can be imported."""

    def test_run_seed_crawl_module_exists(self):
        """Test that run_seed_crawl module can be imported."""
        # Should not raise ImportError
        import scripts.run_seed_crawl

    def test_run_seed_crawl_has_main_function(self):
        """Test that run_seed_crawl has a main() function."""
        from scripts.run_seed_crawl import main

        assert callable(main)

    def test_run_seed_crawl_has_run_seed_crawl_function(self):
        """Test that run_seed_crawl has run_seed_crawl() function."""
        from scripts.run_seed_crawl import run_seed_crawl

        assert callable(run_seed_crawl)


class TestSeedCrawlOrchestration:
    """Test seed crawl orchestration logic."""

    @patch("scripts.run_seed_crawl.load_seed_config")
    @patch("scripts.run_seed_crawl.crawl_seed_site")
    @patch("scripts.run_seed_crawl.process_dedupe")
    @patch("scripts.run_seed_crawl.score_candidate")
    @patch("scripts.run_seed_crawl.filter_by_score")
    @patch("scripts.run_seed_crawl.fetch_candidate_contents")
    @patch("scripts.run_seed_crawl.convert_candidates")
    def test_run_seed_crawl_calls_pipeline_steps(
        self,
        mock_convert,
        mock_fetch,
        mock_filter,
        mock_score,
        mock_dedupe,
        mock_crawl,
        mock_load_config,
    ):
        """Test that run_seed_crawl calls all pipeline steps in order."""
        from scripts.run_seed_crawl import run_seed_crawl

        # Setup mock config
        mock_load_config.return_value = {
            "seeds": [
                {
                    "id": "test_seed",
                    "domain": "example.com",
                    "enabled": True,
                    "start_urls": ["https://example.com"],
                    "crawl_patterns": [],
                    "topic": "test",
                    "trust_tier": "low",
                }
            ]
        }

        # Mock crawl to return candidates
        mock_crawl.return_value = [
            {
                "candidate_id": "seed_crawl_example_test_abc123",
                "lane": "seed_crawl",
                "title": "Test Page",
                "url": "https://example.com/test",
                "source": {"domain": "example.com", "trust_tier": "low"},
                "candidate_scores": {"total_score": 50},
            }
        ]

        # Mock dedupe to return survivors and duplicates
        mock_dedupe.return_value = (
            [{"candidate_id": "seed_crawl_example_test_abc123", "lane": "seed_crawl"}],
            [],  # duplicates
        )

        # Mock filter to return survivors and filtered
        mock_filter.return_value = (
            [
                {
                    "candidate_id": "seed_crawl_example_test_abc123",
                    "candidate_scores": {"total_score": 50},
                }
            ],
            [],  # filtered
        )

        # Mock fetch to return the same candidates (content fetched successfully)
        mock_fetch.return_value = (
            [
                {
                    "candidate_id": "seed_crawl_example_test_abc123",
                    "candidate_scores": {"total_score": 50},
                }
            ],
            [],  # failed
        )

        # Mock convert to return record paths
        mock_convert.return_value = [Path("/fake/path/record.txt")]

        # Run seed crawl
        run_seed_crawl()

        # Verify all steps were called
        mock_load_config.assert_called_once()
        mock_crawl.assert_called()
        mock_dedupe.assert_called()
        mock_score.assert_called()
        mock_filter.assert_called()
        mock_fetch.assert_called()
        mock_convert.assert_called()

    @patch("scripts.run_seed_crawl.load_seed_config")
    def test_run_seed_crawl_skips_disabled_seeds(self, mock_load_config):
        """Test that disabled seeds are skipped."""
        from scripts.run_seed_crawl import run_seed_crawl

        mock_load_config.return_value = {
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

        with patch("scripts.run_seed_crawl.crawl_seed_site") as mock_crawl:
            run_seed_crawl()
            # crawl_seed_site should not be called for disabled seeds
            mock_crawl.assert_not_called()

    @patch("scripts.run_seed_crawl.load_seed_config")
    def test_run_seed_crawl_handles_no_seeds(self, mock_load_config):
        """Test that run_seed_crawl handles empty seeds list gracefully."""
        from scripts.run_seed_crawl import run_seed_crawl

        mock_load_config.return_value = {"seeds": []}

        # Should not raise, just return
        run_seed_crawl()

    @patch("scripts.run_seed_crawl.load_seed_config")
    @patch("scripts.run_seed_crawl.crawl_seed_site")
    def test_run_seed_crawl_handles_crawl_error(self, mock_crawl, mock_load_config):
        """Test that run_seed_crawl handles crawl errors gracefully."""
        from scripts.run_seed_crawl import run_seed_crawl

        mock_load_config.return_value = {
            "seeds": [
                {
                    "id": "error_seed",
                    "domain": "example.com",
                    "enabled": True,
                    "start_urls": ["https://example.com"],
                    "crawl_patterns": [],
                    "topic": "test",
                    "trust_tier": "low",
                }
            ]
        }

        # Simulate crawl raising an exception
        mock_crawl.side_effect = Exception("Crawl failed")

        # Should not raise, just log the error
        run_seed_crawl()


class TestSeedCrawlStats:
    """Test stats tracking for seed crawl."""

    @patch("scripts.run_seed_crawl.load_seed_config")
    @patch("scripts.run_seed_crawl.crawl_seed_site")
    def test_run_seed_crawl_updates_stats(self, mock_crawl, mock_load_config):
        """Test that run_seed_crawl updates lane stats."""
        from scripts.run_seed_crawl import run_seed_crawl

        mock_load_config.return_value = {
            "seeds": [
                {
                    "id": "stats_test",
                    "domain": "example.com",
                    "enabled": True,
                    "start_urls": ["https://example.com"],
                    "crawl_patterns": [],
                    "topic": "test",
                    "trust_tier": "low",
                }
            ]
        }

        mock_crawl.return_value = [
            {
                "candidate_id": "seed_crawl_example_stats_123",
                "lane": "seed_crawl",
                "title": "Test",
                "url": "https://example.com/test",
                "source": {"domain": "example.com", "trust_tier": "low"},
                "candidate_scores": {"total_score": 50},
            }
        ]

        with patch("scripts.run_seed_crawl.process_dedupe") as mock_dedupe:
            with patch("scripts.run_seed_crawl.score_candidate") as mock_score:
                with patch("scripts.run_seed_crawl.filter_by_score") as mock_filter:
                    with patch(
                        "scripts.run_seed_crawl.convert_candidates"
                    ) as mock_convert:
                        with patch(
                            "scripts.run_seed_crawl.update_lane_stats"
                        ) as mock_stats:
                            mock_dedupe.return_value = ([], [])
                            mock_filter.return_value = ([], [])
                            mock_convert.return_value = []

                            run_seed_crawl()

                            # Should call update_lane_stats for discovered
                            assert mock_stats.called


class TestSeedCrawlCLI:
    """Test CLI interface for seed crawl."""

    @patch("scripts.run_seed_crawl.load_seed_config")
    def test_seed_crawl_cli_runs(self, mock_load_config):
        """Test that the CLI runs without error."""
        from scripts.run_seed_crawl import main

        # Use a minimal config so the CLI exits quickly without network calls
        mock_load_config.return_value = {"seeds": []}

        # Mock sys.argv so argparse doesn't pick up pytest's CLI arguments
        with patch("sys.argv", ["run_seed_crawl"]):
            # Capture SystemExit from sys.exit(0)
            try:
                main()
            except SystemExit as e:
                assert e.code == 0

    def test_seed_crawl_cli_help(self):
        """Test that --help works."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.run_seed_crawl",
                "--help",
            ],
            cwd=Path(__file__).resolve().parent.parent,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "seed" in result.stdout.lower() or "help" in result.stdout.lower()
