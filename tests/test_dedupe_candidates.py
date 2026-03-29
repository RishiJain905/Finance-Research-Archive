"""Tests for dedupe_candidates module."""

import json
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.candidate_utils import (
    BASE_DIR,
    compute_content_hash,
    compute_title_hash,
    compute_url_hash,
    load_candidate_index,
    normalize_title,
    save_candidate_index,
)
from scripts.dedupe_candidates import (
    check_content_dedupe,
    check_title_dedupe,
    check_url_dedupe,
    process_dedupe,
    register_candidate,
)


class TestNormalizeTitle:
    """Tests for title normalization."""

    def test_normalize_lowercase(self):
        assert (
            normalize_title("Federal Reserve Press Release")
            == "federal reserve press release"
        )

    def test_normalize_whitespace(self):
        assert normalize_title("  Federal   Reserve  ") == "federal reserve"

    def test_normalize_punctuation(self):
        assert normalize_title('"Federal Reserve"') == "federal reserve"

    def test_normalize_empty(self):
        assert normalize_title("") == ""
        assert normalize_title(None) == ""


class TestComputeHashes:
    """Tests for hash computation functions."""

    def test_compute_url_hash_deterministic(self):
        url = "https://www.federalreserve.gov/newsevents/pressreleases.htm"
        hash1 = compute_url_hash(url)
        hash2 = compute_url_hash(url)
        assert hash1 == hash2
        assert len(hash1) == 32  # MD5 hex

    def test_compute_url_hash_different_urls(self):
        hash1 = compute_url_hash("https://example.com/page1")
        hash2 = compute_url_hash("https://example.com/page2")
        assert hash1 != hash2

    def test_compute_title_hash(self):
        title = "Federal Reserve Announces New Policy"
        hash1 = compute_title_hash(title)
        hash2 = compute_title_hash(title.lower())
        assert hash1 == hash2

    def test_compute_content_hash(self):
        content = "This is sample content for testing."
        hash1 = compute_content_hash(content)
        hash2 = compute_content_hash(content)
        assert hash1 == hash2


class TestCheckUrlDedup:
    """Tests for URL deduplication check."""

    def test_url_not_duplicate_when_index_empty(self):
        candidate = {"url": "https://example.com/article"}
        index = {"seen_url_hashes": {}}
        assert check_url_dedupe(candidate, index) is False

    def test_url_duplicate_when_exists(self):
        url = "https://example.com/article"
        url_hash = compute_url_hash(url)
        candidate = {"url": url}
        index = {"seen_url_hashes": {url_hash: {"candidate_id": "prev_id"}}}
        assert check_url_dedupe(candidate, index) is True

    def test_url_not_duplicate_when_different(self):
        candidate = {"url": "https://example.com/new-article"}
        index = {
            "seen_url_hashes": {compute_url_hash("https://example.com/old-article"): {}}
        }
        assert check_url_dedupe(candidate, index) is False


class TestCheckTitleDedup:
    """Tests for title deduplication check."""

    def test_title_not_duplicate_when_index_empty(self):
        candidate = {
            "title": "Federal Reserve Policy Statement",
            "source": {"domain": "federalreserve.gov"},
        }
        index = {"seen_title_hashes": {}}
        assert check_title_dedupe(candidate, index) is False

    def test_title_duplicate_same_domain(self):
        title = "Federal Reserve Policy Statement"
        domain = "federalreserve.gov"
        title_hash = compute_title_hash(normalize_title(title))

        candidate = {"title": title, "source": {"domain": domain}}
        index = {
            "seen_title_hashes": {
                domain: {
                    title_hash: {"candidate_id": "prev_id", "timestamp": time.time()}
                }
            }
        }
        assert check_title_dedupe(candidate, index) is True


class TestCheckContentDedup:
    """Tests for content deduplication check."""

    def test_content_not_duplicate_when_index_empty(self):
        candidate = {"raw_text_path": "/nonexistent/path.txt"}
        index = {"seen_content_hashes": {}}
        assert check_content_dedupe(candidate, index) is False

    def test_content_duplicate_when_exists(self, tmp_path):
        # Create a temp content file
        content_file = tmp_path / "test_content.txt"
        content_file.write_text("This is test content for deduplication testing.")

        content_hash = compute_content_hash(content_file.read_text())

        candidate = {"raw_text_path": str(content_file)}
        index = {"seen_content_hashes": {content_hash: {"candidate_id": "prev_id"}}}

        assert check_content_dedupe(candidate, index) is True


class TestRegisterCandidate:
    """Tests for candidate registration."""

    def test_register_candidate_url(self):
        candidate = {
            "candidate_id": "test_123",
            "url": "https://example.com/article",
            "source": {"domain": "example.com"},
        }
        index = {}
        result = register_candidate(candidate, index)

        url_hash = compute_url_hash("https://example.com/article")
        assert url_hash in result["seen_url_hashes"]
        assert result["seen_url_hashes"][url_hash]["candidate_id"] == "test_123"

    def test_register_candidate_title(self):
        candidate = {
            "candidate_id": "test_123",
            "title": "Test Article Title",
            "source": {"domain": "example.com"},
        }
        index = {}
        result = register_candidate(candidate, index)

        title_hash = compute_title_hash(normalize_title("Test Article Title"))
        assert "example.com" in result["seen_title_hashes"]
        assert title_hash in result["seen_title_hashes"]["example.com"]

    def test_register_candidate_content(self, tmp_path):
        content_file = tmp_path / "test.txt"
        content_file.write_text("Test content for registration.")

        candidate = {
            "candidate_id": "test_123",
            "raw_text_path": str(content_file),
            "url": "https://example.com/article",
            "source": {"domain": "example.com"},
            "title": "Test",
        }
        index = {}
        result = register_candidate(candidate, index)

        content_hash = compute_content_hash("Test content for registration.")
        assert content_hash in result["seen_content_hashes"]


class TestProcessDedup:
    """Tests for main dedupe processing."""

    def test_process_dedupe_all_unique(self, tmp_path):
        # Create temp content files
        content1 = tmp_path / "content1.txt"
        content1.write_text("Content of article one.")
        content2 = tmp_path / "content2.txt"
        content2.write_text("Content of article two.")

        candidates = [
            {
                "candidate_id": "cand_1",
                "url": "https://example.com/article1",
                "title": "Article One",
                "raw_text_path": str(content1),
            },
            {
                "candidate_id": "cand_2",
                "url": "https://example.com/article2",
                "title": "Article Two",
                "raw_text_path": str(content2),
            },
        ]

        with patch("scripts.dedupe_candidates.load_candidate_index", return_value={}):
            with patch("scripts.dedupe_candidates.save_candidate_index") as mock_save:
                survivors, duplicates = process_dedupe(candidates)

        assert len(survivors) == 2
        assert len(duplicates) == 0
        mock_save.assert_called_once()

    def test_process_dedupe_url_duplicate(self):
        # Both candidates have the same URL, and that URL is already in the index
        # This means both should be flagged as duplicates
        candidates = [
            {
                "candidate_id": "cand_1",
                "url": "https://example.com/article",
                "title": "Article One",
                "raw_text_path": "/path/to/content1.txt",
            },
            {
                "candidate_id": "cand_2",
                "url": "https://example.com/article",  # Same URL
                "title": "Article Two",
                "raw_text_path": "/path/to/content2.txt",
            },
        ]

        existing_hash = compute_url_hash("https://example.com/article")
        existing_index = {
            "seen_url_hashes": {existing_hash: {"candidate_id": "cand_1"}},
            "seen_title_hashes": {},
            "seen_content_hashes": {},
            "candidate_map": {},
        }

        with patch(
            "scripts.dedupe_candidates.load_candidate_index",
            return_value=existing_index,
        ):
            with patch("scripts.dedupe_candidates.save_candidate_index"):
                survivors, duplicates = process_dedupe(candidates)

        # Both should be duplicates since URL already exists in index
        assert len(survivors) == 0
        assert len(duplicates) == 2
        assert all(d["dedupe_status"] == "url_duplicate" for d in duplicates)
