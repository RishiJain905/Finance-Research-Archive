"""
Tests for candidate_utils module.
Verifies candidate ID format, hash consistency, and utility functions.
"""

import json
import hashlib
import pytest
from pathlib import Path


class TestCandidateIdFormat:
    """Test candidate ID format validation."""

    def test_candidate_id_format_trusted_sources(self):
        """Test candidate ID format for trusted_sources lane."""
        from scripts.candidate_utils import build_candidate_id

        candidate_id = build_candidate_id(
            lane="trusted_sources",
            domain="federalreserve.gov",
            title="Federal Reserve Issues Statement on Interest Rates",
            url="https://www.federalreserve.gov/pressreleases/2024-01.htm",
        )
        # Should be format: lane_normalized_domain_short_title_slug_short_hash
        assert candidate_id.startswith("trusted_sources_federalreserve_")
        parts = candidate_id.split("_")
        assert len(parts) >= 4  # lane, domain, title_slug, hash

    def test_candidate_id_format_keyword_discovery(self):
        """Test candidate ID format for keyword_discovery lane."""
        from scripts.candidate_utils import build_candidate_id

        candidate_id = build_candidate_id(
            lane="keyword_discovery",
            domain="brookings.edu",
            title="The Fed's New Monetary Policy Framework",
            url="https://www.brookings.edu/articles/fed-framework.html",
        )
        assert candidate_id.startswith("keyword_discovery_brookings_")

    def test_candidate_id_format_seed_crawl(self):
        """Test candidate ID format for seed_crawl lane."""
        from scripts.candidate_utils import build_candidate_id

        candidate_id = build_candidate_id(
            lane="seed_crawl",
            domain="newyorkfed.org",
            title="Understanding Repo Market Dynamics",
            url="https://www.newyorkfed.org/markets/ liquidity-repo",
        )
        assert candidate_id.startswith("seed_crawl_newyorkfed_")

    def test_candidate_id_stable_for_same_inputs(self):
        """Same inputs should produce same candidate ID."""
        from scripts.candidate_utils import build_candidate_id

        id1 = build_candidate_id(
            lane="trusted_sources",
            domain="federalreserve.gov",
            title="Test Title",
            url="https://federalreserve.gov/test",
        )
        id2 = build_candidate_id(
            lane="trusted_sources",
            domain="federalreserve.gov",
            title="Test Title",
            url="https://federalreserve.gov/test",
        )
        assert id1 == id2


class TestHashFunctions:
    """Test hash function consistency."""

    def test_hash_url_consistency(self):
        """Same URL should produce same hash."""
        from scripts.candidate_utils import hash_url

        url = "https://www.federalreserve.gov/pressreleases/2024-01.htm"
        hash1 = hash_url(url)
        hash2 = hash_url(url)
        assert hash1 == hash2
        assert len(hash1) > 0

    def test_hash_url_different_urls_different_hashes(self):
        """Different URLs should produce different hashes."""
        from scripts.candidate_utils import hash_url

        url1 = "https://www.federalreserve.gov/page1"
        url2 = "https://www.federalreserve.gov/page2"
        hash1 = hash_url(url1)
        hash2 = hash_url(url2)
        assert hash1 != hash2

    def test_hash_title_consistency(self):
        """Same title should produce same hash."""
        from scripts.candidate_utils import hash_title

        title = "Federal Reserve Issues Statement on Interest Rates"
        hash1 = hash_title(title)
        hash2 = hash_title(title)
        assert hash1 == hash2

    def test_hash_title_normalized(self):
        """Title should be normalized before hashing."""
        from scripts.candidate_utils import hash_title

        title1 = "Federal Reserve Issues Statement on Interest Rates"
        title2 = "federal reserve issues statement on interest rates"  # lowercase
        hash1 = hash_title(title1)
        hash2 = hash_title(title2)
        assert hash1 == hash2  # Should be same after normalization

    def test_hash_content_consistency(self):
        """Same content should produce same fingerprint."""
        from scripts.candidate_utils import hash_content

        content = "This is some sample content for the research archive."
        hash1 = hash_content(content)
        hash2 = hash_content(content)
        assert hash1 == hash2
        assert len(hash1) > 0

    def test_hash_content_different_content_different_hashes(self):
        """Different content should produce different fingerprints."""
        from scripts.candidate_utils import hash_content

        content1 = "This is the first piece of content."
        content2 = "This is a different piece of content."
        hash1 = hash_content(content1)
        hash2 = hash_content(content2)
        assert hash1 != hash2


class TestNormalizeTitle:
    """Test title normalization."""

    def test_normalize_title_lowercase(self):
        """Title should be converted to lowercase."""
        from scripts.candidate_utils import normalize_title

        result = normalize_title("Federal Reserve Issues Statement")
        assert result == "federal reserve issues statement"

    def test_normalize_title_whitespace(self):
        """Extra whitespace should be collapsed."""
        from scripts.candidate_utils import normalize_title

        result = normalize_title("Federal   Reserve  Issues  Statement")
        assert result == "federal reserve issues statement"

    def test_normalize_title_strip(self):
        """Title should be stripped of leading/trailing whitespace."""
        from scripts.candidate_utils import normalize_title

        result = normalize_title("  Federal Reserve Statement  ")
        assert result == "federal reserve statement"


class TestSaveLoadCandidate:
    """Test candidate save/load roundtrip."""

    def test_save_and_load_candidate(self, tmp_path):
        """Candidate JSON should be saved and loaded correctly."""
        from scripts.candidate_utils import save_candidate, load_candidate, BASE_DIR

        # Create a minimal candidate
        candidate = {
            "candidate_id": "test_candidate_123",
            "lane": "trusted_sources",
            "discovered_at": "2024-01-15T10:00:00Z",
            "topic": "macro catalysts",
            "title": "Test Candidate",
            "status": "discovered",
        }

        # Save to temp path
        save_path = tmp_path / "test_candidate.json"
        save_candidate(candidate, save_path)

        # Load and verify
        loaded = load_candidate(save_path)
        assert loaded["candidate_id"] == candidate["candidate_id"]
        assert loaded["lane"] == candidate["lane"]
        assert loaded["discovered_at"] == candidate["discovered_at"]


class TestLaneStats:
    """Test lane statistics tracking."""

    def test_update_lane_stats_increments(self):
        """Lane stats should be incremented correctly."""
        from scripts.candidate_utils import update_lane_stats, get_lane_stats, BASE_DIR
        import json

        # Get initial stats
        initial_stats = get_lane_stats()

        # Update a stat
        update_lane_stats("trusted_sources", "discovered")

        # Verify the increment happened
        updated_stats = get_lane_stats()
        assert (
            updated_stats["trusted_sources"]["discovered"]
            == initial_stats["trusted_sources"]["discovered"] + 1
        )

    def test_get_lane_stats_returns_all_lanes(self):
        """Lane stats should include all three lanes."""
        from scripts.candidate_utils import get_lane_stats

        stats = get_lane_stats()

        assert "trusted_sources" in stats
        assert "keyword_discovery" in stats
        assert "seed_crawl" in stats

        # Each lane should have all stat types
        for lane in ["trusted_sources", "keyword_discovery", "seed_crawl"]:
            assert "discovered" in stats[lane]
            assert "deduped_out" in stats[lane]
            assert "filtered_out" in stats[lane]
            assert "converted" in stats[lane]


class TestCandidateIndex:
    """Test candidate index manifest."""

    def test_get_candidate_index_returns_structure(self):
        """Candidate index should have expected structure."""
        from scripts.candidate_utils import get_candidate_index

        index = get_candidate_index()

        assert "seen_url_hashes" in index
        assert "seen_title_hashes" in index
        assert "seen_content_hashes" in index
        assert "candidate_map" in index

    def test_candidate_index_is_dict(self):
        """All index fields should be dictionaries."""
        from scripts.candidate_utils import get_candidate_index

        index = get_candidate_index()

        assert isinstance(index["seen_url_hashes"], dict)
        assert isinstance(index["seen_title_hashes"], dict)
        assert isinstance(index["seen_content_hashes"], dict)
        assert isinstance(index["candidate_map"], dict)
