"""Tests for extract_candidate_features module."""

import json
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Import the module
from scripts.extract_candidate_features import (
    load_scoring_rules,
    extract_freshness_hours,
    extract_url_quality_score,
    extract_title_quality_score,
    extract_keyword_match_score,
    extract_domain_trust_score,
    extract_lane_reliability_score,
    derive_source_type,
    extract_candidate_features,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def scoring_rules():
    """Load actual scoring rules for tests."""
    return load_scoring_rules()


@pytest.fixture
def mock_scoring_rules():
    """Provide mock scoring rules for isolated testing."""
    return {
        "url_hints": {
            "positive": ["press", "statement", "report", "speech"],
            "negative": ["about", "careers", "subscribe"],
        },
        "title_hints": {
            "positive": ["inflation", "rates", "monetary policy", "interest rate"],
            "negative": ["subscribe", "careers", "advertisement"],
        },
        "domain_trust_baselines": {
            "high": ["federalreserve.gov", "treasury.gov"],
            "medium": ["brookings.edu", "imf.org"],
            "low": [],
        },
        "lane_reliability": {
            "trusted_sources": 100,
            "keyword_discovery": 50,
            "seed_crawl": 30,
        },
        "source_type_map": {
            "federalreserve.gov": "rulemaking",
        },
        "freshness": {
            "max_hours_for_full_score": 168,
        },
    }


@pytest.fixture
def sample_candidate():
    """Provide a basic sample candidate for testing."""
    return {
        "candidate_id": "test_123",
        "url": "https://federalreserve.gov/press-release-2024",
        "title": "Federal Reserve Announces Interest Rate Decision",
        "anchor_text": "Fed monetary policy statement",
        "source_domain": "federalreserve.gov",
        "lane": "trusted_sources",
        "discovered_at": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# TestLoadScoringRules
# =============================================================================


class TestLoadScoringRules:
    """Tests for load_scoring_rules function."""

    def test_loads_scoring_rules_json(self):
        """Verify scoring_rules.json is loaded."""
        rules = load_scoring_rules()

        # Should return a non-empty dictionary
        assert isinstance(rules, dict)
        assert len(rules) > 0

    def test_scoring_rules_has_required_keys(self):
        """Verify all required keys exist in scoring rules."""
        rules = load_scoring_rules()

        # Verify required top-level keys
        required_keys = [
            "weights",
            "thresholds",
            "priority_buckets",
            "lane_reliability",
            "domain_trust_baselines",
            "url_hints",
            "title_hints",
            "source_type_map",
            "freshness",
        ]

        for key in required_keys:
            assert key in rules, f"Missing required key: {key}"


# =============================================================================
# TestExtractFreshnessHours
# =============================================================================


class TestExtractFreshnessHours:
    """Tests for extract_freshness_hours function."""

    def test_freshness_hours_recent(self):
        """Published 1 hour ago → freshness_hours ≈ 1."""
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        candidate = {"metadata": {"published_at": one_hour_ago.isoformat()}}

        freshness_hours = extract_freshness_hours(candidate)

        # Should be approximately 1 hour (allow some tolerance)
        assert 0.5 <= freshness_hours <= 2.0

    def test_freshness_hours_old(self):
        """Published 200 hours ago → freshness_hours > 168 (over max)."""
        old_date = datetime.now(timezone.utc) - timedelta(hours=200)
        candidate = {"metadata": {"published_at": old_date.isoformat()}}

        freshness_hours = extract_freshness_hours(candidate)

        # Should be > 168 hours
        assert freshness_hours > 168

    def test_freshness_hours_uses_discovered_at_fallback(self):
        """No published_at uses discovered_at."""
        two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
        candidate = {"metadata": {}, "discovered_at": two_hours_ago.isoformat()}

        freshness_hours = extract_freshness_hours(candidate)

        # Should be approximately 2 hours (using discovered_at fallback)
        assert 1.5 <= freshness_hours <= 3.0

    def test_freshness_hours_returns_0_for_missing_dates(self):
        """No dates returns 0."""
        candidate = {
            "metadata": {},
        }

        freshness_hours = extract_freshness_hours(candidate)

        assert freshness_hours == 0.0

    def test_freshness_hours_with_iso_format_string(self):
        """Handles ISO format string with Z suffix."""
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        iso_string = one_hour_ago.isoformat() + "Z"
        candidate = {"metadata": {"published_at": iso_string}}

        freshness_hours = extract_freshness_hours(candidate)

        assert 0.5 <= freshness_hours <= 2.0

    def test_freshness_hours_with_naive_datetime(self):
        """Handles naive datetime by assuming UTC."""
        one_hour_ago = datetime.now() - timedelta(hours=1)
        candidate = {"metadata": {"published_at": one_hour_ago.isoformat()}}

        freshness_hours = extract_freshness_hours(candidate)

        # Should still calculate (timezone naive is treated as UTC)
        assert freshness_hours >= 0


# =============================================================================
# TestExtractUrlQualityScore
# =============================================================================


class TestExtractUrlQualityScore:
    """Tests for extract_url_quality_score function."""

    def test_positive_url_hints(self, mock_scoring_rules):
        """URL with 'press' and 'statement' scores high."""
        candidate = {"url": "https://example.com/press/statement-2024"}

        score = extract_url_quality_score(candidate, mock_scoring_rules)

        # Should have points for 'press' (+10) and 'statement' (+10) = 20
        assert score >= 20

    def test_negative_url_hints(self, mock_scoring_rules):
        """URL with 'about' and 'careers' scores low."""
        candidate = {"url": "https://example.com/about/careers"}

        score = extract_url_quality_score(candidate, mock_scoring_rules)

        # Should have negative points: 'about' (-10) and 'careers' (-10) = -20 → floored to 0
        assert score == 0

    def test_mixed_url_signals(self, mock_scoring_rules):
        """URL with both positive and negative."""
        candidate = {"url": "https://example.com/press/about"}

        score = extract_url_quality_score(candidate, mock_scoring_rules)

        # 'press' (+10) and 'about' (-10) = 0
        assert score == 0

    def test_empty_url_returns_0(self, mock_scoring_rules):
        """No URL returns 0."""
        candidate = {}

        score = extract_url_quality_score(candidate, mock_scoring_rules)

        assert score == 0

    def test_url_score_capped_at_100(self, mock_scoring_rules):
        """Score is capped at 100."""
        # Create a URL with many positive hints
        candidate = {
            "url": "https://example.com/press/statement/report/speech/press/statement/report/speech"
        }

        score = extract_url_quality_score(candidate, mock_scoring_rules)

        assert score <= 100


# =============================================================================
# TestExtractTitleQualityScore
# =============================================================================


class TestExtractTitleQualityScore:
    """Tests for extract_title_quality_score function."""

    def test_positive_title_terms(self, mock_scoring_rules):
        """Title with 'inflation' and 'rates' scores high."""
        candidate = {"title": "Fed's Inflation Report and Interest Rates Analysis"}

        score = extract_title_quality_score(candidate, mock_scoring_rules)

        # 'inflation' (+10) and 'rates' (+10) = 20, possibly more if partial matches
        assert score >= 20

    def test_negative_title_terms(self, mock_scoring_rules):
        """Title with 'subscribe' and 'careers' scores low."""
        candidate = {"title": "Subscribe to our newsletter - Careers at our company"}

        score = extract_title_quality_score(candidate, mock_scoring_rules)

        # Should have negative points
        assert score < 10

    def test_empty_title_returns_0(self, mock_scoring_rules):
        """No title returns 0."""
        candidate = {}

        score = extract_title_quality_score(candidate, mock_scoring_rules)

        assert score == 0

    def test_title_score_floored_at_0(self, mock_scoring_rules):
        """Negative scores are floored at 0."""
        candidate = {"title": "Subscribe about careers advertisement"}

        score = extract_title_quality_score(candidate, mock_scoring_rules)

        assert score >= 0


# =============================================================================
# TestExtractKeywordMatchScore
# =============================================================================


class TestExtractKeywordMatchScore:
    """Tests for extract_keyword_match_score function."""

    def test_keyword_match_in_title(self, mock_scoring_rules):
        """Matching keywords in title."""
        candidate = {
            "title": "Inflation and interest rates monetary policy",
            "anchor_text": "",
            "url": "",
        }

        score = extract_keyword_match_score(candidate, mock_scoring_rules)

        # Should match 'inflation', 'rates', 'monetary policy', 'interest rate'
        assert score > 0

    def test_keyword_match_in_anchor(self, mock_scoring_rules):
        """Matching keywords in anchor text."""
        candidate = {
            "title": "",
            "anchor_text": "Fed announces inflation interest rates decision",
            "url": "",
        }

        score = extract_keyword_match_score(candidate, mock_scoring_rules)

        # Should match keywords in anchor
        assert score > 0

    def test_no_keyword_match(self, mock_scoring_rules):
        """No matches returns 0 or low score."""
        candidate = {
            "title": "Hello World",
            "anchor_text": "Click here for more",
            "url": "https://example.com/page",
        }

        score = extract_keyword_match_score(candidate, mock_scoring_rules)

        # No positive keywords matched
        assert score == 0

    def test_keyword_match_combined_text(self, mock_scoring_rules):
        """Keywords matched across title, anchor, and url combined."""
        candidate = {
            "title": "Inflation report",
            "anchor_text": "Interest rates",
            "url": "https://example.com/monetary-policy",
        }

        score = extract_keyword_match_score(candidate, mock_scoring_rules)

        # Should match across all text sources
        assert score > 0


# =============================================================================
# TestExtractDomainTrustScore
# =============================================================================


class TestExtractDomainTrustScore:
    """Tests for extract_domain_trust_score function."""

    def test_high_trust_domain(self, scoring_rules):
        """federalreserve.gov → (100, 'high')."""
        score, tier = extract_domain_trust_score("federalreserve.gov", scoring_rules)

        assert score == 100.0
        assert tier == "high"

    def test_medium_trust_domain(self, scoring_rules):
        """brookings.edu → (50, 'medium')."""
        score, tier = extract_domain_trust_score("brookings.edu", scoring_rules)

        assert score == 50.0
        assert tier == "medium"

    def test_unknown_domain(self, scoring_rules):
        """unknown.com → (10, 'low')."""
        score, tier = extract_domain_trust_score("unknown.com", scoring_rules)

        assert score == 10.0
        assert tier == "low"

    def test_empty_domain(self, scoring_rules):
        """Empty domain returns (0, 'low')."""
        score, tier = extract_domain_trust_score("", scoring_rules)

        assert score == 0.0
        assert tier == "low"

    def test_www_variant_high_trust(self, scoring_rules):
        """www.federalreserve.gov is also high trust."""
        score, tier = extract_domain_trust_score(
            "www.federalreserve.gov", scoring_rules
        )

        assert score == 100.0
        assert tier == "high"


# =============================================================================
# TestExtractLaneReliabilityScore
# =============================================================================


class TestExtractLaneReliabilityScore:
    """Tests for extract_lane_reliability_score function."""

    def test_trusted_sources_highest(self, scoring_rules):
        """trusted_sources lane → 100."""
        score = extract_lane_reliability_score("trusted_sources", scoring_rules)

        assert score == 100.0

    def test_keyword_discovery_medium(self, scoring_rules):
        """keyword_discovery lane → 50."""
        score = extract_lane_reliability_score("keyword_discovery", scoring_rules)

        assert score == 50.0

    def test_seed_crawl_lower(self, scoring_rules):
        """seed_crawl lane → 30."""
        score = extract_lane_reliability_score("seed_crawl", scoring_rules)

        assert score == 30.0

    def test_unknown_lane_defaults(self, scoring_rules):
        """Unknown lane returns 0."""
        score = extract_lane_reliability_score("unknown_lane", scoring_rules)

        assert score == 0.0


# =============================================================================
# TestDeriveSourceType
# =============================================================================


class TestDeriveSourceType:
    """Tests for derive_source_type function."""

    def test_domain_map_override(self, scoring_rules):
        """federalreserve.gov → 'rulemaking'."""
        candidate = {
            "url": "https://federalreserve.gov/some/path",
            "source_domain": "federalreserve.gov",
        }

        source_type = derive_source_type(candidate, scoring_rules)

        assert source_type == "rulemaking"

    def test_auto_detect_press_release(self, scoring_rules):
        """URL with 'press-release' → 'press_release'."""
        candidate = {
            "url": "https://example.com/press-release-2024",
            "source_domain": "unknown.com",
        }

        source_type = derive_source_type(candidate, scoring_rules)

        assert source_type == "press_release"

    def test_auto_detect_speech(self, scoring_rules):
        """URL with 'speech' → 'speech'."""
        candidate = {
            "url": "https://example.com/speech-by-governor",
            "source_domain": "unknown.com",
        }

        source_type = derive_source_type(candidate, scoring_rules)

        assert source_type == "speech"

    def test_auto_detect_research(self, scoring_rules):
        """URL with 'research' → 'research'."""
        candidate = {
            "url": "https://example.com/research/analysis-2024",
            "source_domain": "unknown.com",
        }

        source_type = derive_source_type(candidate, scoring_rules)

        assert source_type == "research"

    def test_unknown_fallback(self, scoring_rules):
        """No match → 'unknown'."""
        candidate = {"url": "https://example.com/page", "source_domain": "unknown.com"}

        source_type = derive_source_type(candidate, scoring_rules)

        assert source_type == "unknown"

    def test_release_keyword_detected(self, scoring_rules):
        """URL with 'release' → 'press_release'."""
        candidate = {
            "url": "https://example.com/news/official-release-2024",
            "source_domain": "unknown.com",
        }

        source_type = derive_source_type(candidate, scoring_rules)

        assert source_type == "press_release"

    def test_testimony_keyword_detected(self, scoring_rules):
        """URL with 'testimony' → 'speech'."""
        candidate = {
            "url": "https://example.com/congress/testimony-2024",
            "source_domain": "unknown.com",
        }

        source_type = derive_source_type(candidate, scoring_rules)

        assert source_type == "speech"


# =============================================================================
# TestExtractCandidateFeatures (Integration)
# =============================================================================


class TestExtractCandidateFeatures:
    """Integration tests for extract_candidate_features function."""

    def test_extracts_all_features(self):
        """Full candidate gets all feature fields added."""
        candidate = {
            "candidate_id": "test_123",
            "url": "https://federalreserve.gov/press-release-2024",
            "title": "Fed Announces Inflation and Interest Rates Decision",
            "anchor_text": "Federal Reserve monetary policy",
            "source_domain": "federalreserve.gov",
            "lane": "trusted_sources",
            "discovered_at": datetime.now(timezone.utc).isoformat(),
        }

        result = extract_candidate_features(candidate)

        # Verify all feature fields are present
        expected_fields = [
            "source_domain",
            "freshness_hours",
            "url_quality_score",
            "title_quality_score",
            "keyword_match_score",
            "domain_trust_score",
            "domain_trust_tier",
            "lane_reliability_score",
            "duplication_risk_score",
            "source_type",
            "topic_hints",
        ]

        for field in expected_fields:
            assert field in result, f"Missing field: {field}"

    def test_feature_scores_normalized(self):
        """All scores are 0-100 range."""
        candidate = {
            "candidate_id": "test_123",
            "url": "https://federalreserve.gov/press-release-2024",
            "title": "Fed Announces Inflation and Interest Rates Decision",
            "anchor_text": "Federal Reserve monetary policy",
            "source_domain": "federalreserve.gov",
            "lane": "trusted_sources",
            "discovered_at": datetime.now(timezone.utc).isoformat(),
        }

        result = extract_candidate_features(candidate)

        # All score fields should be in 0-100 range
        score_fields = [
            "url_quality_score",
            "title_quality_score",
            "keyword_match_score",
            "domain_trust_score",
            "lane_reliability_score",
        ]

        for field in score_fields:
            score = result.get(field, -1)
            assert 0 <= score <= 100, f"{field} score {score} not in 0-100 range"

    def test_topic_hints_populated(self):
        """topic_hints list is populated with matched terms."""
        candidate = {
            "candidate_id": "test_123",
            "url": "https://federalreserve.gov/press-release-2024",
            "title": "Inflation and Interest Rates Monetary Policy Report",
            "anchor_text": "",
            "source_domain": "federalreserve.gov",
            "lane": "trusted_sources",
            "discovered_at": datetime.now(timezone.utc).isoformat(),
        }

        result = extract_candidate_features(candidate)

        # topic_hints should contain matched keywords
        assert isinstance(result.get("topic_hints"), list)
        # Should match at least some of: inflation, rates, monetary policy, interest rate
        matched = result.get("topic_hints", [])
        assert len(matched) > 0, "Expected some topic hints to be matched"

    def test_domain_trust_tier_is_high_for_federalreserve(self):
        """Federal Reserve domain gets high trust tier."""
        candidate = {
            "candidate_id": "test_123",
            "url": "https://federalreserve.gov/press-release-2024",
            "title": "Fed Press Release",
            "source_domain": "federalreserve.gov",
            "lane": "trusted_sources",
            "discovered_at": datetime.now(timezone.utc).isoformat(),
        }

        result = extract_candidate_features(candidate)

        assert result.get("domain_trust_tier") == "high"
        assert result.get("domain_trust_score") == 100.0
