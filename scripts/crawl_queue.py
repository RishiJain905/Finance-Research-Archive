"""
BFS-style crawl queue with scoring for seed-site crawling lane.

Queue items contain:
- url: Target URL
- depth: Current crawl depth
- score: Computed link score
- parent_url: URL where this was discovered
- anchor_text: Anchor text from discovery

Items are sorted by score descending within each depth level.
"""

import heapq
from typing import Optional, Dict, Any, List


class CrawlQueue:
    """
    Priority queue for BFS crawling with scoring.

    Items are sorted by score descending within each depth.
    Tracks visited URLs to avoid duplicates.
    Respects max_depth and max_pages limits.
    """

    def __init__(self, max_pages: int = 20, max_depth: int = 2):
        """
        Initialize crawl queue.

        Args:
            max_pages: Maximum pages to crawl before stopping
            max_depth: Maximum crawl depth
        """
        self.max_pages = max_pages
        self.max_depth = max_depth
        self._visited: set[str] = set()
        # Heap stores tuples: (depth, -score, url, item_dict)
        self._heap: list = []
        self._pages_processed = 0

    def enqueue(self, item: Dict[str, Any]) -> bool:
        """
        Add an item to the queue if within limits and not already visited.

        Args:
            item: Queue item with url, depth, score, parent_url, anchor_text

        Returns:
            True if item was enqueued, False otherwise
        """
        url = item.get("url", "")
        depth = item.get("depth", 0)

        # Check depth limit
        if not self._is_within_depth_limit(depth):
            return False

        # Check if already visited
        if url in self._visited:
            return False

        # Mark as visited
        self._visited.add(url)

        # Use negative depth for max-heap behavior (python heapq is min-heap)
        # Use negative score for score ordering within same depth
        # Tuple: (depth, -score, url) - sorts by depth asc, then score desc
        heapq.heappush(self._heap, (depth, -item.get("score", 0), url, item))  # type: ignore

        return True

    def dequeue(self) -> Optional[Dict[str, Any]]:
        """
        Remove and return the highest priority item.

        Highest priority = lowest depth, then highest score.

        Returns:
            Queue item dict, or None if queue is empty or max_pages reached
        """
        if self.is_empty():
            return None

        if self._pages_processed >= self.max_pages:
            return None

        # Get item, ignoring depth/score fields in returned dict
        depth, neg_score, url, item = heapq.heappop(self._heap)  # type: ignore
        self._pages_processed += 1

        return item

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self._heap) == 0

    def size(self) -> int:
        """Return current queue size."""
        return len(self._heap)

    def _is_within_depth_limit(self, depth: int) -> bool:
        """Check if depth is within allowed limit."""
        return depth <= self.max_depth

    def get_pages_processed(self) -> int:
        """Return number of pages processed so far."""
        return self._pages_processed

    def has_reached_page_limit(self) -> bool:
        """Check if max pages has been reached."""
        return self._pages_processed >= self.max_pages


def create_queue_item(
    url: str, depth: int, score: float, parent_url: str = "", anchor_text: str = ""
) -> Dict[str, Any]:
    """
    Create a queue item dictionary.

    Args:
        url: Target URL
        depth: Current crawl depth
        score: Computed link score
        parent_url: URL where this was discovered
        anchor_text: Anchor text from discovery

    Returns:
        Queue item dictionary
    """
    return {
        "url": url,
        "depth": depth,
        "score": score,
        "parent_url": parent_url,
        "anchor_text": anchor_text,
    }
