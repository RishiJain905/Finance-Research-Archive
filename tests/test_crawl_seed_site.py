"""
Tests for crawl_seed_site module.
Verifies seed site crawling and candidate generation.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path


class TestCrawlSeed:
    """Test seed site crawling."""

    def test_crawl_seed_returns_list(self):
        """crawl_seed should return a list of candidates."""
        from scripts.crawl_seed_site import crawl_seed

        seed_config = {
            "id": "federalreserve.gov",
            "enabled": True,
            "domain": "federalreserve.gov",
            "start_urls": ["https://federalreserve.gov/"],
            "allowed_prefixes": ["https://federalreserve.gov/"],
            "blocked_fragments": ["/about/", "/careers/"],
            "max_depth": 2,
            "max_pages": 5,
            "topic": "macro catalysts",
        }

        # Mock the crawl components
        with patch("scripts.crawl_seed_site.CrawlQueue") as MockQueue:
            with patch("scripts.crawl_seed_site.fetch_page") as mock_fetch:
                with patch(
                    "scripts.extract_internal_links.extract_links"
                ) as mock_extract:
                    # Setup mocks
                    mock_queue = MockQueue.return_value
                    mock_queue.is_empty.side_effect = [False, False, True]
                    mock_queue.dequeue.side_effect = [
                        {
                            "url": "https://federalreserve.gov/",
                            "depth": 0,
                            "score": 10.0,
                            "parent_url": "",
                            "anchor_text": "Home",
                        },
                        {
                            "url": "https://federalreserve.gov/pressreleases/",
                            "depth": 1,
                            "score": 15.0,
                            "parent_url": "https://federalreserve.gov/",
                            "anchor_text": "Press Releases",
                        },
                    ]

                    mock_soup = Mock()
                    mock_fetch.return_value = mock_soup
                    mock_extract.return_value = [
                        {
                            "url": "https://federalreserve.gov/pressreleases/",
                            "anchor_text": "Press Releases",
                        }
                    ]

                    candidates = crawl_seed(seed_config)
                    assert isinstance(candidates, list)

    def test_crawl_seed_respects_max_pages(self):
        """crawl_seed should stop when max_pages is reached."""
        from scripts.crawl_seed_site import crawl_seed

        seed_config = {
            "id": "test",
            "enabled": True,
            "domain": "test.gov",
            "start_urls": ["https://test.gov/"],
            "allowed_prefixes": ["https://test.gov/"],
            "blocked_fragments": [],
            "max_depth": 2,
            "max_pages": 3,
            "topic": "macro catalysts",
        }

        with patch("scripts.crawl_seed_site.CrawlQueue") as MockQueue:
            with patch("scripts.crawl_seed_site.fetch_page") as mock_fetch:
                with patch(
                    "scripts.extract_internal_links.extract_links"
                ) as mock_extract:
                    mock_queue = MockQueue.return_value
                    # Simulate queue running out after 3 dequeues
                    mock_queue.is_empty.side_effect = [False, False, False, True]
                    mock_queue.dequeue.side_effect = [
                        {
                            "url": "https://test.gov/",
                            "depth": 0,
                            "score": 10.0,
                            "parent_url": "",
                            "anchor_text": "Home",
                        },
                        {
                            "url": "https://test.gov/page1",
                            "depth": 1,
                            "score": 8.0,
                            "parent_url": "https://test.gov/",
                            "anchor_text": "Page 1",
                        },
                        {
                            "url": "https://test.gov/page2",
                            "depth": 1,
                            "score": 6.0,
                            "parent_url": "https://test.gov/",
                            "anchor_text": "Page 2",
                        },
                    ]

                    mock_soup = Mock()
                    mock_fetch.return_value = mock_soup
                    mock_extract.return_value = []

                    candidates = crawl_seed(seed_config)
                    # Should have at most max_pages candidates
                    assert len(candidates) <= 3

    def test_crawl_seed_initializes_queue_from_start_urls(self):
        """crawl_seed should initialize queue with start URLs at depth 0."""
        from scripts.crawl_seed_site import crawl_seed
        from scripts.crawl_queue import CrawlQueue

        seed_config = {
            "id": "test",
            "enabled": True,
            "domain": "test.gov",
            "start_urls": ["https://test.gov/", "https://test.gov/research/"],
            "allowed_prefixes": ["https://test.gov/"],
            "blocked_fragments": [],
            "max_depth": 2,
            "max_pages": 10,
            "topic": "market structure",
        }

        with patch("scripts.crawl_seed_site.CrawlQueue") as MockQueue:
            with patch("scripts.crawl_seed_site.fetch_page") as mock_fetch:
                with patch(
                    "scripts.extract_internal_links.extract_links"
                ) as mock_extract:
                    mock_queue = MockQueue.return_value
                    mock_queue.is_empty.return_value = True

                    candidates = crawl_seed(seed_config)

                    # Verify CrawlQueue was called with correct max_pages and max_depth
                    MockQueue.assert_called_once_with(max_pages=10, max_depth=2)

    def test_crawl_seed_disabled_seed_returns_empty(self):
        """Disabled seed should return empty list without crawling."""
        from scripts.crawl_seed_site import crawl_seed

        seed_config = {
            "id": "test",
            "enabled": False,
            "domain": "test.gov",
            "start_urls": ["https://test.gov/"],
            "allowed_prefixes": [],
            "blocked_fragments": [],
            "max_depth": 2,
            "max_pages": 10,
            "topic": "macro catalysts",
        }

        candidates = crawl_seed(seed_config)
        assert candidates == []


class TestCandidateRecordStructure:
    """Test that generated candidates follow the schema."""

    def test_candidate_has_required_fields(self):
        """Generated candidates should have all required fields."""
        from scripts.crawl_seed_site import create_candidate_record
        from datetime import datetime

        crawl_item = {
            "url": "https://federalreserve.gov/pressreleases/2024-statement.html",
            "depth": 1,
            "score": 15.0,
            "parent_url": "https://federalreserve.gov/",
            "anchor_text": "Statement on Interest Rates",
        }

        seed_config = {
            "id": "federalreserve.gov",
            "domain": "federalreserve.gov",
            "topic": "macro catalysts",
        }

        candidate = create_candidate_record(crawl_item, seed_config)

        # Check required fields per schema
        assert "candidate_id" in candidate
        assert candidate["lane"] == "seed_crawl"
        assert "discovered_at" in candidate
        assert candidate["topic"] == "macro catalysts"
        assert "source" in candidate
        assert "title" in candidate
        assert "dedupe" in candidate
        assert "status" in candidate

    def test_candidate_source_structure(self):
        """Candidate source field should have correct structure."""
        from scripts.crawl_seed_site import create_candidate_record

        crawl_item = {
            "url": "https://federalreserve.gov/page",
            "depth": 0,
            "score": 10.0,
            "parent_url": "",
            "anchor_text": "Home",
        }

        seed_config = {
            "id": "federalreserve.gov",
            "domain": "federalreserve.gov",
            "topic": "macro catalysts",
        }

        candidate = create_candidate_record(crawl_item, seed_config)

        source = candidate["source"]
        assert "domain" in source
        assert "source_name" in source
        assert "url" in source
        assert "discovery_url" in source
        assert "discovery_method" in source
        assert "trust_tier" in source
        assert source["discovery_method"] == "crawl"

    def test_candidate_discovery_method_is_crawl(self):
        """Discovery method should be 'crawl' for seed crawl lane."""
        from scripts.crawl_seed_site import create_candidate_record

        crawl_item = {
            "url": "https://federalreserve.gov/page",
            "depth": 0,
            "score": 10.0,
            "parent_url": "",
            "anchor_text": "Test",
        }

        seed_config = {
            "id": "federalreserve.gov",
            "domain": "federalreserve.gov",
            "topic": "macro catalysts",
        }

        candidate = create_candidate_record(crawl_item, seed_config)
        assert candidate["source"]["discovery_method"] == "crawl"

    def test_candidate_lane_is_seed_crawl(self):
        """Candidate lane should be 'seed_crawl'."""
        from scripts.crawl_seed_site import create_candidate_record

        crawl_item = {
            "url": "https://federalreserve.gov/page",
            "depth": 0,
            "score": 10.0,
            "parent_url": "",
            "anchor_text": "Test",
        }

        seed_config = {
            "id": "federalreserve.gov",
            "domain": "federalreserve.gov",
            "topic": "macro catalysts",
        }

        candidate = create_candidate_record(crawl_item, seed_config)
        assert candidate["lane"] == "seed_crawl"


class TestScopeRules:
    """Test URL scope checking for crawling."""

    def test_url_within_allowed_prefix(self):
        """URL matching allowed prefix should pass scope check."""
        from scripts.crawl_seed_site import is_url_in_scope

        result = is_url_in_scope(
            "https://federalreserve.gov/pressreleases/2024/",
            ["https://federalreserve.gov/"],
            [],
        )
        assert result is True

    def test_url_outside_allowed_prefix(self):
        """URL not matching allowed prefix should fail scope check."""
        from scripts.crawl_seed_site import is_url_in_scope

        result = is_url_in_scope(
            "https://external.com/page", ["https://federalreserve.gov/"], []
        )
        assert result is False

    def test_url_with_blocked_fragment_rejected(self):
        """URL with blocked fragment should fail scope check."""
        from scripts.crawl_seed_site import is_url_in_scope

        result = is_url_in_scope(
            "https://federalreserve.gov/about/careers/",
            ["https://federalreserve.gov/"],
            ["/about/", "/careers/"],
        )
        assert result is False

    def test_url_with_allowed_prefix_and_no_blocked_fragment(self):
        """URL with allowed prefix and no blocked fragment should pass."""
        from scripts.crawl_seed_site import is_url_in_scope

        result = is_url_in_scope(
            "https://federalreserve.gov/pressreleases/2024/",
            ["https://federalreserve.gov/"],
            ["/about/", "/careers/"],
        )
        assert result is True
