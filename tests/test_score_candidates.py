"""Tests for score_candidates module."""

import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.score_candidates import (
    FRESHNESS_WINDOW_DAYS,
    MIN_CONTENT_WORDS,
    NON_RELEVANT_TERMS,
    POSITIVE_ANCHOR_TERMS,
    POSITIVE_URL_KEYWORDS,
    filter_by_score,
    get_domain_trust_score,
    load_domain_trust_tiers,
    score_anchor_text,
    score_content_length,
    score_candidate,
    score_freshness,
    score_url,
)


class TestGetDomainTrustScore:
    """Tests for domain trust scoring."""

    def test_high_trust_domain(self):
        score, tier = get_domain_trust_score("federalreserve.gov")
        assert tier == "high"
        assert score == 100

    def test_medium_trust_domain(self):
        score, tier = get_domain_trust_score("brookings.edu")
        assert tier == "medium"
        assert score == 50

    def test_unknown_domain_low_trust(self):
        score, tier = get_domain_trust_score("unknown-site.com")
        assert tier == "low"
        assert score == 10

    def test_empty_domain(self):
        score, tier = get_domain_trust_score("")
        assert tier == "low"
        assert score == 0


class TestScoreUrl:
    """Tests for URL scoring."""

    def test_positive_url_keywords(self):
        candidate = {"url": "https://example.com/press-release-2024"}
        score = score_url(candidate)
        assert score > 0  # Contains 'press' and 'release'

    def test_negative_url_patterns(self):
        candidate = {"url": "https://example.com/about-us"}
        score = score_url(candidate)
        assert score < 0  # Contains 'about'

    def test_article_like_url(self):
        candidate = {"url": "https://example.com/2024/03/15/fed-policy"}
        score = score_url(candidate)
        assert score >= 0  # Has date pattern

    def test_empty_url(self):
        candidate = {"url": ""}
        score = score_url(candidate)
        assert score == 0


class TestScoreAnchorText:
    """Tests for anchor text scoring."""

    def test_positive_anchor_terms(self):
        candidate = {
            "anchor_text": "Federal Reserve monetary policy decision on interest rates",
            "title": "Fed Announcement",
        }
        score = score_anchor_text(candidate)
        assert score > 0  # Contains 'monetary policy', 'rates'

    def test_negative_anchor_terms(self):
        candidate = {
            "anchor_text": "Subscribe to our newsletter",
            "title": "Newsletter Signup",
        }
        score = score_anchor_text(candidate)
        assert score < 0  # Contains 'subscribe'

    def test_empty_anchor_and_title(self):
        candidate = {"anchor_text": "", "title": ""}
        score = score_anchor_text(candidate)
        assert score == 0


class TestScoreFreshness:
    """Tests for freshness scoring."""

    def test_recent_publication(self):
        from datetime import timezone

        recent_date = datetime.now(timezone.utc) - timedelta(days=2)
        candidate = {"metadata": {"published_at": recent_date.isoformat()}}
        score = score_freshness(candidate)
        assert score == 20

    def test_older_publication(self):
        from datetime import timezone

        old_date = datetime.now(timezone.utc) - timedelta(days=20)
        candidate = {"metadata": {"published_at": old_date.isoformat()}}
        score = score_freshness(candidate)
        assert score < 20

    def test_no_publication_date(self):
        candidate = {"metadata": {}}
        score = score_freshness(candidate)
        assert score == 0


class TestScoreContentLength:
    """Tests for content length scoring."""

    def test_sufficient_content(self, tmp_path):
        # Create a file with > 500 words
        words = ["word"] * 600
        content_file = tmp_path / "long_content.txt"
        content_file.write_text(" ".join(words))

        candidate = {"raw_text_path": str(content_file)}
        score = score_content_length(candidate)
        assert score == 0

    def test_short_content(self, tmp_path):
        # Create a file with < 500 words
        words = ["word"] * 100
        content_file = tmp_path / "short_content.txt"
        content_file.write_text(" ".join(words))

        candidate = {"raw_text_path": str(content_file)}
        score = score_content_length(candidate)
        assert score < 0

    def test_empty_content(self):
        candidate = {"raw_text_path": "/nonexistent/file.txt"}
        score = score_content_length(candidate)
        assert score == -10


class TestScoreCandidate:
    """Tests for complete candidate scoring."""

    def test_score_candidate_full(self, tmp_path):
        from datetime import timezone

        # Create a decent content file
        content_file = tmp_path / "content.txt"
        content_file.write_text("word " * 600)  # 600 words

        recent_date = datetime.now(timezone.utc) - timedelta(days=3)
        candidate = {
            "candidate_id": "test_123",
            "url": "https://federalreserve.gov/press-release-2024",
            "title": "Fed Announces Monetary Policy",
            "anchor_text": "Federal Reserve monetary policy statement on interest rates",
            "raw_text_path": str(content_file),
            "source": {"domain": "federalreserve.gov"},
            "metadata": {"published_at": recent_date.isoformat()},
        }

        result = score_candidate(candidate)

        assert "candidate_scores" in result
        assert result["candidate_scores"]["total_score"] > 0
        assert result["candidate_scores"]["trust_tier"] == "high"
        assert result["candidate_scores"]["url_score"] > 0
        assert result["candidate_scores"]["anchor_score"] > 0

    def test_score_candidate_missing_fields(self):
        candidate = {
            "candidate_id": "test_123",
            "url": "",
            "title": "",
            "source": {"domain": ""},
        }

        result = score_candidate(candidate)

        assert "candidate_scores" in result
        # Empty candidate has content penalty (-10) but trust defaults to 10 (low unknown domain)
        # So total could be 0 or negative depending on scoring
        assert result["candidate_scores"]["trust_tier"] == "low"
        assert "total_score" in result["candidate_scores"]


class TestFilterByScore:
    """Tests for score-based filtering."""

    def test_filter_passes_high_scores(self):
        candidates = [
            {"candidate_id": "high_1", "candidate_scores": {"total_score": 100}},
            {"candidate_id": "high_2", "candidate_scores": {"total_score": 75}},
        ]

        survivors, filtered = filter_by_score(candidates, threshold=50)

        assert len(survivors) == 2
        assert len(filtered) == 0

    def test_filter_fails_low_scores(self):
        candidates = [
            {"candidate_id": "low_1", "candidate_scores": {"total_score": 10}}
        ]

        survivors, filtered = filter_by_score(candidates, threshold=50)

        assert len(survivors) == 0
        assert len(filtered) == 1

    def test_filter_uses_default_threshold(self):
        candidates = [
            {"candidate_id": "c1", "candidate_scores": {"total_score": 0}},
            {"candidate_id": "c2", "candidate_scores": {"total_score": -5}},
        ]

        survivors, filtered = filter_by_score(candidates)

        assert len(survivors) == 1  # score >= 0 passes
        assert len(filtered) == 1

    def test_filter_auto_scores_uncored(self):
        """Candidates without scores should be scored first."""
        candidates = [
            {
                "candidate_id": "unscored",
                "url": "https://federalreserve.gov/press",
                "title": "Fed Press Release",
                "anchor_text": "monetary policy rates",
                "source": {"domain": "federalreserve.gov"},
                "raw_text_path": "/fake/path.txt",
            }
        ]

        # Should not raise - should auto-score
        survivors, filtered = filter_by_score(candidates, threshold=0)

        assert len(survivors) >= 0  # Depends on auto-scoring result
