"""
Tests for crawl_queue module.
Verifies BFS-style queue with scoring functionality.
"""

import pytest
from pathlib import Path


class TestCrawlQueueItem:
    """Test crawl queue item structure."""

    def test_queue_item_structure(self):
        """Queue items should have required fields."""
        from scripts.crawl_queue import CrawlQueue

        queue = CrawlQueue(max_pages=20, max_depth=2)

        item = {
            "url": "https://example.com/page",
            "depth": 0,
            "score": 10.0,
            "parent_url": "",
            "anchor_text": "Example Page",
        }

        queue.enqueue(item)
        assert queue.size() == 1

    def test_queue_item_ordering_by_score(self):
        """Higher scored items should be dequeued first within same depth."""
        from scripts.crawl_queue import CrawlQueue

        queue = CrawlQueue(max_pages=20, max_depth=2)

        # Add items with different scores at same depth
        queue.enqueue(
            {
                "url": "https://example.com/low",
                "depth": 0,
                "score": 5.0,
                "parent_url": "",
                "anchor_text": "Low score",
            }
        )
        queue.enqueue(
            {
                "url": "https://example.com/high",
                "depth": 0,
                "score": 20.0,
                "parent_url": "",
                "anchor_text": "High score",
            }
        )
        queue.enqueue(
            {
                "url": "https://example.com/medium",
                "depth": 0,
                "score": 10.0,
                "parent_url": "",
                "anchor_text": "Medium score",
            }
        )

        # Dequeue should return highest score first
        first = queue.dequeue()
        assert first is not None
        assert first["url"] == "https://example.com/high"
        assert first["score"] == 20.0

    def test_queue_respects_max_depth(self):
        """Items beyond max_depth should not be enqueued."""
        from scripts.crawl_queue import CrawlQueue

        queue = CrawlQueue(max_pages=20, max_depth=2)

        # Should accept depth 0, 1, 2
        assert queue._is_within_depth_limit(0) is True
        assert queue._is_within_depth_limit(1) is True
        assert queue._is_within_depth_limit(2) is True

        # Should reject depth 3
        assert queue._is_within_depth_limit(3) is False

    def test_queue_tracks_visited_urls(self):
        """Same URL should not be enqueued twice."""
        from scripts.crawl_queue import CrawlQueue

        queue = CrawlQueue(max_pages=20, max_depth=2)

        queue.enqueue(
            {
                "url": "https://example.com/page1",
                "depth": 0,
                "score": 10.0,
                "parent_url": "",
                "anchor_text": "Page 1",
            }
        )

        # Try to enqueue same URL again
        result = queue.enqueue(
            {
                "url": "https://example.com/page1",
                "depth": 0,
                "score": 15.0,
                "parent_url": "",
                "anchor_text": "Page 1 again",
            }
        )

        assert result is False
        assert queue.size() == 1

    def test_queue_stops_at_max_pages(self):
        """Queue should stop returning items when max_pages reached."""
        from scripts.crawl_queue import CrawlQueue

        queue = CrawlQueue(max_pages=3, max_depth=2)

        for i in range(5):
            queue.enqueue(
                {
                    "url": f"https://example.com/page{i}",
                    "depth": 0,
                    "score": 10.0 - i,
                    "parent_url": "",
                    "anchor_text": f"Page {i}",
                }
            )

        # Should only dequeue 3 items
        count = 0
        while not queue.is_empty():
            item = queue.dequeue()
            if item is None:
                break
            count += 1

        assert count == 3

    def test_queue_empty_initially(self):
        """Queue should start empty."""
        from scripts.crawl_queue import CrawlQueue

        queue = CrawlQueue(max_pages=20, max_depth=2)
        assert queue.is_empty() is True
        assert queue.size() == 0

    def test_queue_sort_within_same_depth(self):
        """Items should be sorted by score descending within same depth."""
        from scripts.crawl_queue import CrawlQueue

        queue = CrawlQueue(max_pages=20, max_depth=2)

        # Add depth 0 items
        queue.enqueue(
            {
                "url": "d0_low",
                "depth": 0,
                "score": 5.0,
                "parent_url": "",
                "anchor_text": "",
            }
        )
        queue.enqueue(
            {
                "url": "d0_high",
                "depth": 0,
                "score": 15.0,
                "parent_url": "",
                "anchor_text": "",
            }
        )
        queue.enqueue(
            {
                "url": "d0_mid",
                "depth": 0,
                "score": 10.0,
                "parent_url": "",
                "anchor_text": "",
            }
        )

        # Dequeue all
        items = []
        while not queue.is_empty():
            item = queue.dequeue()
            if item:
                items.append(item)

        # Verify order: high -> mid -> low
        assert items[0]["url"] == "d0_high"
        assert items[1]["url"] == "d0_mid"
        assert items[2]["url"] == "d0_low"


class TestCrawlQueueDepthLimit:
    """Test queue depth limiting."""

    def test_cannot_enqueue_beyond_max_depth(self):
        """Items beyond max_depth should be rejected."""
        from scripts.crawl_queue import CrawlQueue

        queue = CrawlQueue(max_pages=20, max_depth=2)

        # This should be rejected (depth 3 > max_depth 2)
        result = queue.enqueue(
            {
                "url": "https://example.com/deep",
                "depth": 3,
                "score": 10.0,
                "parent_url": "",
                "anchor_text": "",
            }
        )

        assert result is False
        assert queue.size() == 0
