"""Tests for convert_candidates_to_raw module."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.convert_candidates_to_raw import (
    candidate_to_raw_header,
    convert_candidate,
    convert_candidates,
)


class TestCandidateToRawHeader:
    """Tests for V1 header generation."""

    def test_header_basic_fields(self):
        candidate = {
            "topic": "macro catalysts",
            "title": "Federal Reserve Policy Statement",
            "url": "https://federalreserve.gov/press",
            "lane": "trusted_sources",
            "source": {"discovery_method": "monitor", "trust_tier": "high"},
        }

        header = candidate_to_raw_header(candidate)

        assert "TARGET: macro catalysts" in header
        assert "TOPIC: macro catalysts" in header
        assert "TITLE: Federal Reserve Policy Statement" in header
        assert "URL: https://federalreserve.gov/press" in header
        assert "LANE: trusted_sources" in header
        assert "DISCOVERY_METHOD: monitor" in header
        assert "DOMAIN_TRUST_TIER: high" in header

    def test_header_defaults(self):
        candidate = {"topic": "", "title": "", "url": "", "lane": "", "source": {}}

        header = candidate_to_raw_header(candidate)

        assert "TOPIC: other" in header  # Default topic
        assert "TITLE: Untitled" in header  # Default title
        assert "LANE: unknown" in header
        assert "DISCOVERY_METHOD: unknown" in header
        assert "DOMAIN_TRUST_TIER: low" in header  # Default trust

    def test_header_format(self):
        candidate = {
            "topic": "test",
            "title": "Test",
            "url": "https://test.com",
            "lane": "test_lane",
            "source": {"discovery_method": "test", "trust_tier": "medium"},
        }

        header = candidate_to_raw_header(candidate)
        lines = header.strip().split("\n")

        # Should have 7 lines followed by blank line
        assert len(lines) >= 7
        assert header.endswith("\n\n")  # Ends with double newline


class TestConvertCandidate:
    """Tests for single candidate conversion."""

    def test_convert_creates_file(self, tmp_path):
        # Create a temp content file
        content_file = tmp_path / "content.txt"
        content_file.write_text("This is the body content of the article.")

        candidate = {
            "candidate_id": "test_federalreserve_press_abc123",
            "topic": "macro catalysts",
            "title": "Federal Reserve Press Release",
            "url": "https://federalreserve.gov/press",
            "lane": "trusted_sources",
            "raw_text_path": str(content_file),
            "source": {"discovery_method": "monitor", "trust_tier": "high"},
        }

        output_path = convert_candidate(candidate)

        assert output_path.exists()
        assert output_path.name == "test_federalreserve_press_abc123.txt"

    def test_convert_file_content(self, tmp_path):
        content_file = tmp_path / "content.txt"
        content_file.write_text("Article body content here.")

        candidate = {
            "candidate_id": "test_123",
            "topic": "market structure",
            "title": "Test Title",
            "url": "https://test.com/article",
            "lane": "keyword_discovery",
            "raw_text_path": str(content_file),
            "source": {"discovery_method": "search", "trust_tier": "medium"},
        }

        output_path = convert_candidate(candidate)
        content = output_path.read_text()

        assert "TARGET: market structure" in content
        assert "TOPIC: market structure" in content
        assert "TITLE: Test Title" in content
        assert "LANE: keyword_discovery" in content
        assert "DISCOVERY_METHOD: search" in content
        assert "Article body content here." in content

    def test_convert_missing_candidate_id(self, tmp_path):
        candidate = {"raw_text_path": str(tmp_path / "content.txt")}

        with pytest.raises(ValueError, match="candidate_id"):
            convert_candidate(candidate)

    def test_convert_missing_raw_text_path(self):
        candidate = {"candidate_id": "test_123"}

        with pytest.raises(ValueError, match="raw_text_path"):
            convert_candidate(candidate)

    def test_convert_nonexistent_content_file(self):
        candidate = {
            "candidate_id": "test_123",
            "raw_text_path": "/nonexistent/path.txt",
        }

        with pytest.raises(FileNotFoundError):
            convert_candidate(candidate)


class TestConvertCandidates:
    """Tests for batch candidate conversion."""

    def test_convert_multiple_candidates(self, tmp_path):
        # Create multiple content files
        content1 = tmp_path / "content1.txt"
        content1.write_text("Content of article one.")
        content2 = tmp_path / "content2.txt"
        content2.write_text("Content of article two.")

        candidates = [
            {
                "candidate_id": "cand_1",
                "topic": "macro catalysts",
                "title": "Article One",
                "url": "https://example.com/1",
                "lane": "trusted_sources",
                "raw_text_path": str(content1),
                "source": {"discovery_method": "monitor", "trust_tier": "high"},
            },
            {
                "candidate_id": "cand_2",
                "topic": "macro catalysts",
                "title": "Article Two",
                "url": "https://example.com/2",
                "lane": "keyword_discovery",
                "raw_text_path": str(content2),
                "source": {"discovery_method": "search", "trust_tier": "low"},
            },
        ]

        paths = convert_candidates(candidates)

        assert len(paths) == 2
        assert all(p.exists() for p in paths)

    def test_convert_candidates_handles_partial_failure(self, tmp_path):
        # Create one valid file and one invalid
        content1 = tmp_path / "content1.txt"
        content1.write_text("Valid content.")

        candidates = [
            {
                "candidate_id": "valid_1",
                "topic": "test",
                "title": "Valid",
                "url": "https://test.com/1",
                "lane": "test",
                "raw_text_path": str(content1),
                "source": {"discovery_method": "test", "trust_tier": "low"},
            },
            {
                "candidate_id": "invalid_1",
                "topic": "test",
                "title": "Invalid",
                "url": "https://test.com/2",
                "lane": "test",
                "raw_text_path": "/nonexistent/file.txt",
                "source": {"discovery_method": "test", "trust_tier": "low"},
            },
        ]

        paths = convert_candidates(candidates)

        # Should return 1 path (one succeeded)
        assert len(paths) == 1

        # Check statuses
        assert candidates[0]["conversion_status"] == "converted"
        assert "failed" in candidates[1]["conversion_status"]

    def test_convert_candidates_updates_status(self, tmp_path):
        content_file = tmp_path / "content.txt"
        content_file.write_text("Test content.")

        candidates = [
            {
                "candidate_id": "test_123",
                "topic": "test",
                "title": "Test",
                "url": "https://test.com",
                "lane": "test",
                "raw_text_path": str(content_file),
                "source": {"discovery_method": "test", "trust_tier": "low"},
            }
        ]

        convert_candidates(candidates)

        assert candidates[0]["conversion_status"] == "converted"
        assert "raw_record_path" in candidates[0]
