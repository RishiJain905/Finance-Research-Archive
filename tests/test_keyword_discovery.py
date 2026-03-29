"""Tests for keyword discovery lane implementation.

Tests cover:
- normalize_search_results module (domain extraction, result validation, normalization)
- build_keyword_candidates module (domain blocking, term filtering, trust tiers, candidate building)
- Config loading functions
"""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from scripts.normalize_search_results import (
    extract_domain,
    validate_result,
    normalize_single_result,
    normalize_results,
)
from scripts.build_keyword_candidates import (
    is_blocked_domain,
    check_required_terms,
    should_fetch_page,
    determine_trust_tier,
    build_candidate_from_result,
    build_keyword_candidates,
    load_blocked_domains,
    load_domain_trust_tiers,
    extract_domain as build_extract_domain,
    domain_to_source_name,
)


class TestExtractDomain:
    """Tests for domain extraction from URLs."""

    def test_extract_domain_newyorkfed(self):
        """Test domain extraction from newyorkfed.org URL."""
        url = "https://www.newyorkfed.org/markets/reference-rates/rrp"
        domain = extract_domain(url)
        assert domain == "newyorkfed.org"

    def test_extract_domain_federalreserve(self):
        """Test domain extraction from federalreserve.gov URL."""
        url = "https://federalreserve.gov/pressreleases/2024-01.htm"
        domain = extract_domain(url)
        assert domain == "federalreserve.gov"

    def test_extract_domain_strips_www(self):
        """Test that www. prefix is stripped."""
        url = "https://www.brookings.edu/articles/fed-policy.html"
        domain = extract_domain(url)
        assert domain == "brookings.edu"

    def test_extract_domain_with_port(self):
        """Test domain extraction with port number."""
        url = "https://example.com:8080/page"
        domain = extract_domain(url)
        assert domain == "example.com"

    def test_extract_domain_empty_url(self):
        """Test empty URL returns empty string."""
        assert extract_domain("") == ""
        assert extract_domain(None) == ""

    def test_extract_domain_invalid_url(self):
        """Test invalid URL returns empty string."""
        assert extract_domain("not a url") == ""


class TestValidateResult:
    """Tests for result validation."""

    def test_validate_result_valid(self):
        """Valid result with http URL and title returns True."""
        result = {
            "title": "Federal Reserve Press Release",
            "url": "https://federalreserve.gov/press",
        }
        assert validate_result(result) is True

    def test_validate_result_https(self):
        """Valid result with https URL returns True."""
        result = {
            "title": "Market Update",
            "url": "https://example.com/article",
        }
        assert validate_result(result) is True

    def test_validate_result_missing_url(self):
        """Result missing URL returns False."""
        result = {"title": "Some Title"}
        assert validate_result(result) is False

    def test_validate_result_empty_url(self):
        """Result with empty URL returns False."""
        result = {"title": "Some Title", "url": ""}
        assert validate_result(result) is False

    def test_validate_result_non_http_url(self):
        """Result with non-http URL returns False."""
        result = {
            "title": "Some Title",
            "url": "ftp://example.com/file",
        }
        assert validate_result(result) is False

    def test_validate_result_missing_title(self):
        """Result missing title returns False."""
        result = {"url": "https://example.com"}
        assert validate_result(result) is False

    def test_validate_result_empty_title(self):
        """Result with empty title returns False."""
        result = {"title": "", "url": "https://example.com"}
        assert validate_result(result) is False

    def test_validate_result_whitespace_title(self):
        """Result with whitespace-only title returns False."""
        result = {"title": "   ", "url": "https://example.com"}
        assert validate_result(result) is False

    def test_validate_result_none_result(self):
        """None result returns False."""
        assert validate_result(None) is False

    def test_validate_result_empty_dict(self):
        """Empty dict returns False."""
        assert validate_result({}) is False


class TestNormalizeSingleResult:
    """Tests for single result normalization."""

    def test_normalize_tavily_result(self):
        """Test normalization of Tavily-style result."""
        raw_result = {
            "title": "Repo Market Stress and Liquidity",
            "url": "https://www.newyorkfed.org/markets/rrp",
            "snippet": "Current repo market conditions...",
            "published_date": "2024-01-15",
        }
        normalized = normalize_single_result(raw_result, "tavily")

        assert normalized["title"] == "Repo Market Stress and Liquidity"
        assert normalized["url"] == "https://www.newyorkfed.org/markets/rrp"
        assert normalized["snippet"] == "Current repo market conditions..."
        assert normalized["source_domain"] == "newyorkfed.org"
        assert normalized["provider"] == "tavily"
        assert normalized["published_at"] == "2024-01-15T00:00:00"

    def test_normalize_with_alternate_field_names(self):
        """Test normalization handles alternate field names (link, description, etc.)."""
        raw_result = {
            "title": "Test Article",
            "link": "https://example.com/article",
            "description": "Article description here",
        }
        normalized = normalize_single_result(raw_result, "serpapi")

        assert normalized["url"] == "https://example.com/article"
        assert normalized["snippet"] == "Article description here"

    def test_normalize_handles_missing_fields(self):
        """Test normalization handles missing fields gracefully."""
        raw_result = {}
        normalized = normalize_single_result(raw_result, "tavily")

        assert normalized["title"] == ""
        assert normalized["url"] == ""
        assert normalized["snippet"] == ""
        assert normalized["source_domain"] == ""
        assert normalized["published_at"] is None


class TestNormalizeResults:
    """Tests for batch result normalization."""

    def test_normalize_results_filters_invalid(self):
        """Invalid URLs should be filtered out (empty titles pass through in normalization)."""
        raw_results = [
            {
                "title": "Valid Result",
                "url": "https://example.com/valid",
                "snippet": "Valid snippet",
            },
            {
                "title": "Missing URL",
                # no url
                "snippet": "Has snippet but no URL",
            },
            {
                "title": "",
                "url": "https://example.com/no-title",
                "snippet": "Has URL but no title",
            },
            {
                "title": "Another Valid",
                "url": "https://example.com/another",
                "snippet": "Another valid snippet",
            },
        ]

        normalized = normalize_results(raw_results, "tavily")

        # normalize_results only filters by URL presence, not by title
        # So we get 3: valid, no-title (has URL), another valid
        assert len(normalized) == 3
        assert normalized[0]["title"] == "Valid Result"
        assert normalized[1]["title"] == ""
        assert normalized[2]["title"] == "Another Valid"

    def test_normalize_results_handles_non_dict_items(self):
        """Non-dict items in list should be skipped."""
        raw_results = [
            {"title": "Valid", "url": "https://example.com/1"},
            "not a dict",
            None,
            {"title": "Also Valid", "url": "https://example.com/2"},
        ]

        normalized = normalize_results(raw_results, "tavily")

        assert len(normalized) == 2


class TestIsBlockedDomain:
    """Tests for domain blocking."""

    def test_pinterest_blocked(self):
        """pinterest.com should be blocked."""
        blocked = {"pinterest.com", "facebook.com", "twitter.com"}
        assert is_blocked_domain("https://pinterest.com/pin/123", blocked) is True

    def test_pinterest_www_variant_blocked(self):
        """www.pinterest.com should also be blocked."""
        blocked = {"pinterest.com"}
        assert is_blocked_domain("https://www.pinterest.com/pin/123", blocked) is True

    def test_newyorkfed_not_blocked(self):
        """newyorkfed.org should not be blocked."""
        blocked = {"pinterest.com", "facebook.com"}
        assert is_blocked_domain("https://newyorkfed.org/markets", blocked) is False

    def test_empty_blocked_set(self):
        """With empty blocked set, nothing is blocked."""
        blocked = set()
        assert is_blocked_domain("https://anydomain.com/page", blocked) is False

    def test_invalid_url_treated_as_blocked(self):
        """Invalid URL should be treated as blocked."""
        blocked = set()
        assert is_blocked_domain("not-a-url", blocked) is True


class TestCheckRequiredTerms:
    """Tests for required term checking."""

    def test_all_terms_present(self):
        """All required terms present returns True."""
        text = "The repo market is experiencing liquidity stress"
        required = ["repo", "liquidity"]
        assert check_required_terms(text, required) is True

    def test_some_terms_missing(self):
        """Some required terms missing returns False."""
        text = "The repo market is experiencing stress"
        required = ["repo", "liquidity"]
        assert check_required_terms(text, required) is False

    def test_empty_required_terms(self):
        """Empty required terms list returns True."""
        text = "Any text whatsoever"
        assert check_required_terms(text, []) is True

    def test_case_insensitive(self):
        """Term matching is case insensitive."""
        text = "REPO and LIQUIDITY are important"
        required = ["repo", "liquidity"]
        assert check_required_terms(text, required) is True

    def test_partial_word_matched(self):
        """Substring matches do count (simple substring matching)."""
        text = "The repository is available"
        required = ["repo"]
        # Implementation uses substring matching, so "repo" is found in "repository"
        assert check_required_terms(text, required) is True


class TestShouldFetchPage:
    """Tests for page fetch decision logic."""

    def test_title_has_required_terms(self):
        """Title with required terms should trigger fetch."""
        title = "Repo Market Liquidity Analysis"
        snippet = "Market conditions..."
        required = ["repo", "liquidity"]
        assert should_fetch_page(title, snippet, required) is True

    def test_snippet_has_required_terms(self):
        """Snippet with required terms should trigger fetch (even if title doesn't)."""
        title = "Market Analysis Report"
        snippet = "Repo market and liquidity conditions"
        required = ["repo", "liquidity"]
        assert should_fetch_page(title, snippet, required) is True

    def test_neither_title_nor_snippet_has_terms(self):
        """If neither has required terms, don't fetch."""
        title = "General Market Update"
        snippet = "Market conditions remain stable"
        required = ["repo", "liquidity"]
        assert should_fetch_page(title, snippet, required) is False

    def test_empty_required_terms(self):
        """Empty required terms means no fetch needed."""
        title = "Any Title"
        snippet = "Any Snippet"
        assert should_fetch_page(title, snippet, []) is False

    def test_one_missing_term(self):
        """If even one term is missing, don't fetch."""
        title = "Repo Market Analysis"
        snippet = "Market analysis here"
        required = ["repo", "liquidity"]
        assert should_fetch_page(title, snippet, required) is False


class TestDetermineTrustTier:
    """Tests for trust tier determination."""

    def test_preferred_domain_high_trust(self):
        """Domain in preferred_domains returns high."""
        tier = determine_trust_tier(
            "newyorkfed.org",
            preferred_domains=["newyorkfed.org", "federalreserve.gov"],
        )
        assert tier == "high"

    def test_preferred_domain_www_variant_high_trust(self):
        """www variant of preferred domain returns high."""
        tier = determine_trust_tier(
            "newyorkfed.org",
            preferred_domains=["www.newyorkfed.org"],
        )
        assert tier == "high"

    def test_high_trust_tier_config(self):
        """Domain in high trust tier config returns high."""
        with patch(
            "scripts.build_keyword_candidates.load_domain_trust_tiers",
            return_value={
                "high": ["federalreserve.gov", "newyorkfed.org"],
                "medium": [],
                "low": [],
            },
        ):
            tier = determine_trust_tier("federalreserve.gov", preferred_domains=[])
            assert tier == "high"

    def test_medium_trust_tier_config(self):
        """Domain in medium trust tier config returns medium."""
        with patch(
            "scripts.build_keyword_candidates.load_domain_trust_tiers",
            return_value={
                "high": [],
                "medium": ["brookings.edu", "piie.com"],
                "low": [],
            },
        ):
            tier = determine_trust_tier("brookings.edu", preferred_domains=[])
            assert tier == "medium"

    def test_unknown_domain_low_trust(self):
        """Unknown domain returns low trust."""
        with patch(
            "scripts.build_keyword_candidates.load_domain_trust_tiers",
            return_value={"high": [], "medium": [], "low": []},
        ):
            tier = determine_trust_tier("unknown-site.com", preferred_domains=[])
            assert tier == "low"


class TestDomainToSourceName:
    """Tests for domain to source name conversion."""

    def test_removes_www(self):
        """www prefix is removed."""
        assert domain_to_source_name("www.example.com") == "Example"

    def test_removes_common_tlds(self):
        """Common TLDs are removed."""
        assert domain_to_source_name("federalreserve.gov") == "Federalreserve"
        assert domain_to_source_name("brookings.edu") == "Brookings"
        assert domain_to_source_name("example.org") == "Example"

    def test_title_case(self):
        """Result is title cased."""
        assert domain_to_source_name("NEWYORKFED") == "Newyorkfed"

    def test_replaces_hyphen_underscore(self):
        """Hyphens and underscores become spaces."""
        assert domain_to_source_name("federal_reserve") == "Federal Reserve"


class TestLoadBlockedDomains:
    """Tests for blocked domains loading."""

    def test_load_blocked_domains_success(self, tmp_path):
        """Successfully load blocked domains from config."""
        config_file = tmp_path / "blocked.json"
        config_file.write_text(
            json.dumps({"domains": ["pinterest.com", "facebook.com"]})
        )

        with patch(
            "scripts.build_keyword_candidates.BLOCKED_DOMAINS_PATH",
            config_file,
        ):
            domains = load_blocked_domains()

        assert "pinterest.com" in domains
        assert "facebook.com" in domains

    def test_load_blocked_domains_normalizes_www(self, tmp_path):
        """www. prefix is normalized."""
        config_file = tmp_path / "blocked.json"
        config_file.write_text(json.dumps({"domains": ["www.pinterest.com"]}))

        with patch(
            "scripts.build_keyword_candidates.BLOCKED_DOMAINS_PATH",
            config_file,
        ):
            domains = load_blocked_domains()

        # Should be normalized to pinterest.com without www
        assert "pinterest.com" in domains

    def test_load_blocked_domains_file_not_found(self, tmp_path):
        """Missing config file returns empty set."""
        non_existent = tmp_path / "nonexistent.json"

        with patch(
            "scripts.build_keyword_candidates.BLOCKED_DOMAINS_PATH",
            non_existent,
        ):
            domains = load_blocked_domains()

        assert domains == set()


class TestLoadDomainTrustTiers:
    """Tests for domain trust tiers loading."""

    def test_load_domain_trust_tiers_success(self, tmp_path):
        """Successfully load trust tiers from config."""
        config_file = tmp_path / "trust_tiers.json"
        config_file.write_text(
            json.dumps(
                {
                    "high": ["federalreserve.gov"],
                    "medium": ["brookings.edu"],
                    "low": [],
                }
            )
        )

        with patch(
            "scripts.build_keyword_candidates.DOMAIN_TRUST_TIERS_PATH",
            config_file,
        ):
            tiers = load_domain_trust_tiers()

        assert "federalreserve.gov" in tiers["high"]
        assert "brookings.edu" in tiers["medium"]
        assert tiers["low"] == []

    def test_load_domain_trust_tiers_file_not_found(self, tmp_path):
        """Missing config file returns empty tiers."""
        non_existent = tmp_path / "nonexistent.json"

        with patch(
            "scripts.build_keyword_candidates.DOMAIN_TRUST_TIERS_PATH",
            non_existent,
        ):
            tiers = load_domain_trust_tiers()

        assert tiers == {"high": [], "medium": [], "low": []}

    def test_load_domain_trust_tiers_normalizes_case(self, tmp_path):
        """Domain names are lowercased."""
        config_file = tmp_path / "trust_tiers.json"
        config_file.write_text(
            json.dumps({"high": ["FederalReserve.gov"], "medium": [], "low": []})
        )

        with patch(
            "scripts.build_keyword_candidates.DOMAIN_TRUST_TIERS_PATH",
            config_file,
        ):
            tiers = load_domain_trust_tiers()

        assert "federalreserve.gov" in tiers["high"]


class TestBuildCandidateFromResult:
    """Integration-style tests for candidate building."""

    def test_build_candidate_with_valid_result(self):
        """Build candidate from valid search result."""
        result = {
            "title": "Repo Market Stress and Liquidity Conditions",
            "url": "https://www.newyorkfed.org/markets/reference-rates/rrp",
            "snippet": "Current repo market conditions show increased stress...",
            "source_domain": "newyorkfed.org",
            "provider": "tavily",
            "published_at": "2024-01-15T10:00:00",
        }

        query_config = {
            "id": "repo_stress",
            "topic": "market structure",
            "query": "repo market stress liquidity",
            "required_terms": ["repo", "liquidity"],
            "preferred_domains": ["newyorkfed.org", "federalreserve.gov"],
            "blocked_domains": [],
            "max_results": 10,
        }

        with patch(
            "scripts.build_keyword_candidates.load_blocked_domains",
            return_value=set(),
        ):
            with patch(
                "scripts.build_keyword_candidates.load_domain_trust_tiers",
                return_value={"high": [], "medium": [], "low": []},
            ):
                with patch(
                    "scripts.build_keyword_candidates.fetch_and_extract_text",
                    return_value=None,
                ):
                    candidate = build_candidate_from_result(result, query_config)

        assert candidate is not None
        assert candidate["lane"] == "keyword_discovery"
        assert candidate["title"] == result["title"]
        assert candidate["source"]["domain"] == "newyorkfed.org"
        assert candidate["source"]["trust_tier"] == "high"
        assert "candidate_id" in candidate
        assert candidate["candidate_id"].startswith("keyword_discovery_newyorkfed_")

    def test_build_candidate_blocked_domain(self):
        """Candidate is None when domain is blocked."""
        result = {
            "title": "Pinterest Article",
            "url": "https://pinterest.com/article",
            "snippet": "Something",
            "source_domain": "pinterest.com",
        }

        query_config = {
            "id": "test",
            "topic": "test",
            "query": "test",
            "required_terms": [],
            "preferred_domains": [],
            "blocked_domains": [],
            "max_results": 10,
        }

        with patch(
            "scripts.build_keyword_candidates.load_blocked_domains",
            return_value={"pinterest.com"},
        ):
            candidate = build_candidate_from_result(result, query_config)

        assert candidate is None

    def test_build_candidate_missing_required_terms(self):
        """Candidate is None when required terms are missing from title/snippet."""
        result = {
            "title": "General Market Update",
            "url": "https://example.com/article",
            "snippet": "Market conditions are stable",
            "source_domain": "example.com",
        }

        query_config = {
            "id": "test",
            "topic": "test",
            "query": "test",
            "required_terms": ["repo", "liquidity"],  # Not in title or snippet
            "preferred_domains": [],
            "blocked_domains": [],
            "max_results": 10,
        }

        with patch(
            "scripts.build_keyword_candidates.load_blocked_domains",
            return_value=set(),
        ):
            with patch(
                "scripts.build_keyword_candidates.load_domain_trust_tiers",
                return_value={"high": [], "medium": [], "low": []},
            ):
                candidate = build_candidate_from_result(result, query_config)

        assert candidate is None

    def test_build_candidate_candidate_id_format(self):
        """Candidate ID follows expected format."""
        result = {
            "title": "Fed Announces New Policy",
            "url": "https://federalreserve.gov/press/2024",
            "snippet": "Federal Reserve announcement...",
            "source_domain": "federalreserve.gov",
        }

        query_config = {
            "id": "fed_announcement",
            "topic": "monetary policy",
            "query": "fed policy announcement",
            "required_terms": [],
            "preferred_domains": [],
            "blocked_domains": [],
            "max_results": 10,
        }

        with patch(
            "scripts.build_keyword_candidates.load_blocked_domains",
            return_value=set(),
        ):
            with patch(
                "scripts.build_keyword_candidates.load_domain_trust_tiers",
                return_value={"high": [], "medium": [], "low": []},
            ):
                with patch(
                    "scripts.build_keyword_candidates.fetch_and_extract_text",
                    return_value=None,
                ):
                    candidate = build_candidate_from_result(result, query_config)

        assert candidate is not None
        # Format: lane_domain_title_url_hash
        # lane is "keyword_discovery" which contains underscore, so we check the prefix
        assert candidate["candidate_id"].startswith("keyword_discovery_federalreserve")
        # The ID should contain at least lane, domain, and hash parts
        assert "_" in candidate["candidate_id"]

    def test_build_candidate_trust_tier_from_config(self):
        """Trust tier is correctly determined from config tiers."""
        result = {
            "title": "Brookings Analysis",
            "url": "https://brookings.edu/articles/analysis",
            "snippet": "Research analysis...",
            "source_domain": "brookings.edu",
        }

        query_config = {
            "id": "test",
            "topic": "test",
            "query": "test",
            "required_terms": [],
            "preferred_domains": [],
            "blocked_domains": [],
            "max_results": 10,
        }

        with patch(
            "scripts.build_keyword_candidates.load_blocked_domains",
            return_value=set(),
        ):
            with patch(
                "scripts.build_keyword_candidates.load_domain_trust_tiers",
                return_value={
                    "high": ["federalreserve.gov"],
                    "medium": ["brookings.edu"],
                    "low": [],
                },
            ):
                candidate = build_candidate_from_result(result, query_config)

        assert candidate is not None
        assert candidate["source"]["trust_tier"] == "medium"


class TestBuildKeywordCandidates:
    """Tests for batch candidate building."""

    def test_build_multiple_candidates(self):
        """Build candidates from multiple results."""
        results = [
            {
                "title": "Repo Market Update",
                "url": "https://newyorkfed.org/repo",
                "snippet": "Repo market...",
                "source_domain": "newyorkfed.org",
            },
            {
                "title": "Fed Policy Statement",
                "url": "https://federalreserve.gov/policy",
                "snippet": "Fed policy...",
                "source_domain": "federalreserve.gov",
            },
        ]

        query_config = {
            "id": "test",
            "topic": "test",
            "query": "test repo fed",
            "required_terms": [],
            "preferred_domains": [],
            "blocked_domains": [],
            "max_results": 10,
        }

        with patch(
            "scripts.build_keyword_candidates.load_blocked_domains",
            return_value=set(),
        ):
            with patch(
                "scripts.build_keyword_candidates.load_domain_trust_tiers",
                return_value={"high": [], "medium": [], "low": []},
            ):
                candidates = build_keyword_candidates(results, query_config)

        assert len(candidates) == 2
        assert candidates[0]["source"]["domain"] == "newyorkfed.org"
        assert candidates[1]["source"]["domain"] == "federalreserve.gov"

    def test_build_keyword_candidates_filters_blocked(self):
        """Blocked domains are filtered out."""
        results = [
            {
                "title": "Valid Result",
                "url": "https://newyorkfed.org/valid",
                "snippet": "Valid snippet",
                "source_domain": "newyorkfed.org",
            },
            {
                "title": "Blocked Result",
                "url": "https://pinterest.com/blocked",
                "snippet": "Blocked snippet",
                "source_domain": "pinterest.com",
            },
        ]

        query_config = {
            "id": "test",
            "topic": "test",
            "query": "test",
            "required_terms": [],
            "preferred_domains": [],
            "blocked_domains": [],
            "max_results": 10,
        }

        with patch(
            "scripts.build_keyword_candidates.load_blocked_domains",
            return_value={"pinterest.com"},
        ):
            with patch(
                "scripts.build_keyword_candidates.load_domain_trust_tiers",
                return_value={"high": [], "medium": [], "low": []},
            ):
                candidates = build_keyword_candidates(results, query_config)

        assert len(candidates) == 1
        assert candidates[0]["source"]["domain"] == "newyorkfed.org"
