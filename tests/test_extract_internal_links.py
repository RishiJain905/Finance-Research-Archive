"""
Tests for extract_internal_links module.
Verifies page fetching, link extraction, and scoring.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from bs4 import BeautifulSoup


class TestFetchPage:
    """Test page fetching with robots.txt respect."""

    def test_fetch_page_returns_soup(self):
        """fetch_page should return BeautifulSoup object on success."""
        from scripts.extract_internal_links import fetch_page
        import requests

        html_content = """
        <html>
            <head><title>Test Page</title></head>
            <body>
                <a href="https://example.com/page1">Link 1</a>
                <a href="https://example.com/page2">Link 2</a>
            </body>
        </html>
        """

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = html_content.encode("utf-8")
        mock_response.headers = {"Content-Type": "text/html"}

        with patch(
            "scripts.extract_internal_links.requests.get", return_value=mock_response
        ):
            with patch("scripts.extract_internal_links.robots_txt", return_value=True):
                result = fetch_page("https://example.com")
                assert result is not None
                assert isinstance(result, BeautifulSoup)

    def test_fetch_page_respects_robots_txt(self):
        """fetch_page should return None for robots-blocked pages."""
        from scripts.extract_internal_links import fetch_page

        with patch("scripts.extract_internal_links.robots_txt", return_value=False):
            result = fetch_page("https://example.com/blocked")
            assert result is None

    def test_fetch_page_handles_request_error(self):
        """fetch_page should return None on request error."""
        from scripts.extract_internal_links import fetch_page
        import requests

        with patch(
            "scripts.extract_internal_links.requests.get",
            side_effect=requests.RequestException("Network error"),
        ):
            result = fetch_page("https://example.com")
            assert result is None

    def test_fetch_page_handles_timeout(self):
        """fetch_page should return None on timeout."""
        from scripts.extract_internal_links import fetch_page
        import requests

        with patch(
            "scripts.extract_internal_links.requests.get",
            side_effect=requests.Timeout("Timeout"),
        ):
            result = fetch_page("https://example.com")
            assert result is None


class TestExtractLinks:
    """Test link extraction from BeautifulSoup."""

    def test_extract_links_returns_internal_only(self):
        """extract_links should only return internal links."""
        from scripts.extract_internal_links import extract_links

        html = """
        <html>
            <body>
                <a href="https://example.com/page1">Internal Link 1</a>
                <a href="https://example.com/page2">Internal Link 2</a>
                <a href="https://external.com/page">External Link</a>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")

        links = extract_links(soup, "https://example.com")

        # Should only have 2 internal links
        assert len(links) == 2
        urls = [link["url"] for link in links]
        assert "https://example.com/page1" in urls
        assert "https://example.com/page2" in urls
        assert "https://external.com/page" not in urls

    def test_extract_links_includes_anchor_text(self):
        """extract_links should include anchor text for each link."""
        from scripts.extract_internal_links import extract_links

        html = """
        <html>
            <body>
                <a href="https://example.com/page1">Press Release</a>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")

        links = extract_links(soup, "https://example.com")

        assert len(links) == 1
        assert links[0]["anchor_text"] == "Press Release"

    def test_extract_links_handles_no_links(self):
        """extract_links should return empty list when no links found."""
        from scripts.extract_internal_links import extract_links

        html = "<html><body><p>No links here</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")

        links = extract_links(soup, "https://example.com")

        assert links == []

    def test_extract_links_normalizes_relative_urls(self):
        """extract_links should normalize relative URLs to absolute."""
        from scripts.extract_internal_links import extract_links

        html = """
        <html>
            <body>
                <a href="/about">About</a>
                <a href="/research/2024">Research</a>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")

        links = extract_links(soup, "https://example.com")

        urls = [link["url"] for link in links]
        assert "https://example.com/about" in urls
        assert "https://example.com/research/2024" in urls


class TestScoreLink:
    """Test link scoring based on URL and anchor text."""

    def test_score_link_positive_keywords(self):
        """Links with positive keywords should get positive scores."""
        from scripts.extract_internal_links import score_link

        # Positive: press, release, statement, speech, report
        score = score_link(
            "https://federalreserve.gov/pressreleases/2024-statement.html",
            "Federal Reserve Issues Statement on Interest Rates",
        )

        assert score > 0

    def test_score_link_negative_keywords(self):
        """Links with negative keywords should get negative scores."""
        from scripts.extract_internal_links import score_link

        # Negative: about, careers, events, people
        score = score_link(
            "https://example.com/about/careers", "About Our Team and Careers"
        )

        assert score < 0

    def test_score_link_anchor_text_bonus(self):
        """Links with relevant anchor text should get bonus."""
        from scripts.extract_internal_links import score_link

        # Same URL with different anchor text
        score1 = score_link(
            "https://federalreserve.gov/pressrelease/2024.html",
            "Monetary Policy Statement",
        )
        score2 = score_link(
            "https://federalreserve.gov/pressrelease/2024.html", "Click Here"
        )

        # Score with relevant anchor should be higher
        assert score1 > score2

    def test_score_link_neutral(self):
        """Links with no special keywords should be near zero."""
        from scripts.extract_internal_links import score_link

        score = score_link("https://example.com/generic-page", "Click to view")

        # Should be close to 0 (not heavily positive or negative)
        assert -5 <= score <= 5


class TestPositiveNegativeHints:
    """Test that positive/negative hints are properly applied."""

    def test_positive_hints_listed(self):
        """Positive hints should include expected financial terms."""
        from scripts.extract_internal_links import POSITIVE_HINTS

        expected = [
            "press",
            "release",
            "statement",
            "speech",
            "testimony",
            "report",
            "bulletin",
            "commentary",
            "research",
            "staff-report",
            "market notice",
            "policy",
            "article",
        ]

        for hint in expected:
            assert hint in POSITIVE_HINTS

    def test_negative_hints_listed(self):
        """Negative hints should include expected non-content terms."""
        from scripts.extract_internal_links import NEGATIVE_HINTS

        expected = [
            "about",
            "careers",
            "events",
            "experts",
            "people",
            "education",
            "programs",
            "museum",
            "archive",
            "category",
            "tag",
            "subscribe",
        ]

        for hint in expected:
            assert hint in NEGATIVE_HINTS
