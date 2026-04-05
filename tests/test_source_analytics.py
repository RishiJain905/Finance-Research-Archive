"""
Tests for V2.7 Part 5: Source Performance Analytics (source_analytics.py)

Comprehensive test suite covering:
- JSON record loading from directories
- Filtered TXT record parsing
- Source extraction and normalization
- Stats computation (ratios, averages, timestamps)
- Stats persistence and loading
- Integration with real repo data
"""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from scripts import source_analytics


# =============================================================================
# Test Fixtures / Helper Data
# =============================================================================


def make_record(
    source_name="Test Source",
    domain="test.com",
    status="accepted",
    created_at="2026-04-01T00:00:00+00:00",
    verification_confidence=8,
    priority_score=None,
    source_type="article",
    url=None,
):
    """Factory to create mock JSON record simulating triage pipeline output."""
    if url is None:
        url = f"https://{domain}/article"

    record = {
        "id": f"test_{domain}_{status}",
        "created_at": created_at,
        "source": {
            "name": source_name,
            "domain": domain,
            "url": url,
            "source_type": source_type,
        },
        "status": status,
    }

    if verification_confidence is not None:
        record["llm_review"] = {
            "verification_confidence": verification_confidence,
            "initial_confidence": 5,
            "verdict": status,
        }

    if priority_score is not None:
        record["priority_score"] = priority_score

    return record


def make_filtered_txt(
    source_name="Test Source",
    domain="test.com",
    url=None,
    content="Article content here...",
):
    """Create a filtered TXT file content as string."""
    if url is None:
        url = f"https://{domain}/article"

    return f"""TARGET: {source_name}
URL: {url}
INDEX_URL: https://{domain}/

{content}"""


def make_filtered_record(
    source_name="Test Source",
    domain="test.com",
    url=None,
):
    """Create a mock filtered record dict (as returned by load_filtered_records)."""
    if url is None:
        url = f"https://{domain}/article"

    return {
        "source_name": source_name,
        "source_domain": domain,
        "source_url": url,
        "raw_path": f"data/filtered_out/{source_name.replace(' ', '_')}.txt",
    }


# =============================================================================
# TestSourceStatsSchema
# =============================================================================


class TestSourceStatsSchema(unittest.TestCase):
    """Test source_stats.json schema validity and structure."""

    def test_schema_file_exists(self):
        """Source stats files should exist in the analytics directory."""
        stats_dir = Path(__file__).resolve().parent.parent / "data" / "source_analytics"
        if not stats_dir.exists():
            self.skipTest("No source_analytics directory found")

        json_files = list(stats_dir.glob("*.json"))
        self.assertGreater(len(json_files), 0, "Expected at least one stats file")

    def test_schema_is_valid_json(self):
        """Source stats files should be valid JSON."""
        stats_dir = Path(__file__).resolve().parent.parent / "data" / "source_analytics"
        if not stats_dir.exists():
            self.skipTest("No source_analytics directory found")

        for json_file in list(stats_dir.glob("*.json"))[:3]:
            with json_file.open("r", encoding="utf-8") as f:
                try:
                    json.load(f)
                except json.JSONDecodeError as e:
                    self.fail(f"Invalid JSON in {json_file}: {e}")

    def test_schema_has_required_fields(self):
        """Source stats should have all required fields."""
        stats_dir = Path(__file__).resolve().parent.parent / "data" / "source_analytics"
        if not stats_dir.exists():
            self.skipTest("No source_analytics directory found")

        required_fields = {
            "source_name",
            "source_domain",
            "records_seen",
            "accepted_count",
            "review_count",
            "rejected_count",
            "filtered_out_count",
            "accepted_ratio",
            "review_ratio",
            "rejected_ratio",
            "filtered_ratio",
            "avg_priority_score",
            "avg_verification_confidence",
            "last_seen_at",
        }

        for json_file in list(stats_dir.glob("*.json"))[:3]:
            with json_file.open("r", encoding="utf-8") as f:
                stats = json.load(f)

            missing = required_fields - set(stats.keys())
            self.assertEqual(
                missing, set(), f"{json_file.name} missing fields: {missing}"
            )

    def test_schema_field_types(self):
        """Source stats fields should have correct types."""
        stats_dir = Path(__file__).resolve().parent.parent / "data" / "source_analytics"
        if not stats_dir.exists():
            self.skipTest("No source_analytics directory found")

        for json_file in list(stats_dir.glob("*.json"))[:3]:
            with json_file.open("r", encoding="utf-8") as f:
                stats = json.load(f)

            # Check counts are integers
            self.assertIsInstance(stats["records_seen"], int)
            self.assertIsInstance(stats["accepted_count"], int)
            self.assertIsInstance(stats["review_count"], int)
            self.assertIsInstance(stats["rejected_count"], int)
            self.assertIsInstance(stats["filtered_out_count"], int)

            # Check ratios are floats
            self.assertIsInstance(stats["accepted_ratio"], (int, float))
            self.assertIsInstance(stats["review_ratio"], (int, float))
            self.assertIsInstance(stats["rejected_ratio"], (int, float))
            self.assertIsInstance(stats["filtered_ratio"], (int, float))

            # Check averages are floats
            self.assertIsInstance(stats["avg_priority_score"], (int, float))
            self.assertIsInstance(stats["avg_verification_confidence"], (int, float))

            # Check string fields
            self.assertIsInstance(stats["source_name"], str)
            self.assertIsInstance(stats["source_domain"], str)
            self.assertIsInstance(stats["last_seen_at"], str)


# =============================================================================
# TestSourceRecommendationSchema
# =============================================================================


class TestSourceRecommendationSchema(unittest.TestCase):
    """Test source_recommendation.json schema validity and structure."""

    def test_schema_file_exists(self):
        """Recommendation files should exist in the recommendations directory."""
        rec_dir = (
            Path(__file__).resolve().parent.parent / "data" / "source_recommendations"
        )
        if not rec_dir.exists():
            self.skipTest("No source_recommendations directory found")

        json_files = list(rec_dir.glob("*_recommendation.json"))
        self.assertGreater(
            len(json_files), 0, "Expected at least one recommendation file"
        )

    def test_schema_is_valid_json(self):
        """Recommendation files should be valid JSON."""
        rec_dir = (
            Path(__file__).resolve().parent.parent / "data" / "source_recommendations"
        )
        if not rec_dir.exists():
            self.skipTest("No source_recommendations directory found")

        for json_file in list(rec_dir.glob("*_recommendation.json"))[:3]:
            with json_file.open("r", encoding="utf-8") as f:
                try:
                    json.load(f)
                except json.JSONDecodeError as e:
                    self.fail(f"Invalid JSON in {json_file}: {e}")

    def test_schema_has_required_fields(self):
        """Recommendation should have all required fields."""
        rec_dir = (
            Path(__file__).resolve().parent.parent / "data" / "source_recommendations"
        )
        if not rec_dir.exists():
            self.skipTest("No source_recommendations directory found")

        required_fields = {
            "source_name",
            "recommended_action",
            "reasons",
            "metrics_snapshot",
            "created_at",
        }

        for json_file in list(rec_dir.glob("*_recommendation.json"))[:3]:
            with json_file.open("r", encoding="utf-8") as f:
                rec = json.load(f)

            missing = required_fields - set(rec.keys())
            self.assertEqual(
                missing, set(), f"{json_file.name} missing fields: {missing}"
            )

    def test_schema_action_enum(self):
        """Recommended_action should be a valid action type."""
        rec_dir = (
            Path(__file__).resolve().parent.parent / "data" / "source_recommendations"
        )
        if not rec_dir.exists():
            self.skipTest("No source_recommendations directory found")

        valid_actions = {"keep", "tighten", "lower_max_links", "disable", "investigate"}

        for json_file in list(rec_dir.glob("*_recommendation.json"))[:10]:
            with json_file.open("r", encoding="utf-8") as f:
                rec = json.load(f)

            self.assertIn(
                rec["recommended_action"],
                valid_actions,
                f"{json_file.name} has invalid action: {rec['recommended_action']}",
            )


# =============================================================================
# TestLoadJsonRecords
# =============================================================================


class TestLoadJsonRecords(unittest.TestCase):
    """Test loading JSON records from directories."""

    def setUp(self):
        """Create temp directory for tests."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_dir = Path(self.temp_dir)

    def tearDown(self):
        """Clean up temp directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_loads_json_files(self):
        """Should load all JSON files from directory."""
        # Create test records
        record1 = make_record(source_name="Source A", domain="source-a.com")
        record2 = make_record(source_name="Source B", domain="source-b.com")

        (self.test_dir / "record1.json").write_text(
            json.dumps(record1), encoding="utf-8"
        )
        (self.test_dir / "record2.json").write_text(
            json.dumps(record2), encoding="utf-8"
        )

        result = source_analytics.load_json_records(self.test_dir)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["source"]["name"], "Source A")
        self.assertEqual(result[1]["source"]["name"], "Source B")

    def test_skips_gitkeep(self):
        """Should skip .gitkeep files."""
        record = make_record()
        (self.test_dir / "record.json").write_text(json.dumps(record), encoding="utf-8")
        (self.test_dir / ".gitkeep").write_text("", encoding="utf-8")

        result = source_analytics.load_json_records(self.test_dir)

        self.assertEqual(len(result), 1)
        self.assertNotEqual(result[0]["id"], ".gitkeep")

    def test_returns_empty_for_missing_dir(self):
        """Should return empty list for non-existent directory."""
        result = source_analytics.load_json_records(Path("/nonexistent/path"))
        self.assertEqual(result, [])

    def test_handles_invalid_json_gracefully(self):
        """Should skip files with invalid JSON."""
        record = make_record()
        (self.test_dir / "valid.json").write_text(json.dumps(record), encoding="utf-8")
        (self.test_dir / "invalid.json").write_text(
            "not valid json {", encoding="utf-8"
        )

        result = source_analytics.load_json_records(self.test_dir)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], record["id"])


# =============================================================================
# TestLoadFilteredRecords
# =============================================================================


class TestLoadFilteredRecords(unittest.TestCase):
    """Test parsing TXT filtered records with header metadata."""

    def setUp(self):
        """Create temp directory for tests."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_dir = Path(self.temp_dir)

    def tearDown(self):
        """Clean up temp directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_parses_txt_headers(self):
        """Should parse TARGET and URL from TXT headers."""
        content = make_filtered_txt(
            source_name="Test Source", domain="test.com", url="https://test.com/article"
        )
        (self.test_dir / "filtered1.txt").write_text(content, encoding="utf-8")

        result = source_analytics.load_filtered_records(self.test_dir)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source_name"], "Test Source")
        self.assertEqual(result[0]["source_url"], "https://test.com/article")

    def test_extracts_target_as_source_name(self):
        """TARGET header should become source_name."""
        content = "TARGET: My Source Name\nURL: https://example.com/\n\nBody"
        (self.test_dir / "test.txt").write_text(content, encoding="utf-8")

        result = source_analytics.load_filtered_records(self.test_dir)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source_name"], "My Source Name")

    def test_extracts_domain_from_url(self):
        """Should extract domain from URL."""
        content = "TARGET: Test\nURL: https://www.example.com/path\n\nBody"
        (self.test_dir / "test.txt").write_text(content, encoding="utf-8")

        result = source_analytics.load_filtered_records(self.test_dir)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["source_domain"], "example.com")

    def test_returns_empty_for_missing_dir(self):
        """Should return empty list for non-existent directory."""
        result = source_analytics.load_filtered_records(Path("/nonexistent/path"))
        self.assertEqual(result, [])

    def test_skips_malformed_files(self):
        """Should skip files without valid headers."""
        # File with no recognizable headers
        (self.test_dir / "malformed.txt").write_text(
            "Just some content without headers", encoding="utf-8"
        )

        result = source_analytics.load_filtered_records(self.test_dir)

        self.assertEqual(len(result), 0)


# =============================================================================
# TestExtractSourceFromRecord
# =============================================================================


class TestExtractSourceFromRecord(unittest.TestCase):
    """Test source extraction from JSON records."""

    def test_extracts_all_fields(self):
        """Should extract name, domain, url, source_type."""
        record = make_record(
            source_name="Federal Reserve",
            domain="federalreserve.gov",
            source_type="press_release",
            url="https://federalreserve.gov/press",
        )

        result = source_analytics.extract_source_from_record(record)

        self.assertEqual(result["name"], "Federal Reserve")
        self.assertEqual(result["domain"], "federalreserve.gov")
        self.assertEqual(result["url"], "https://federalreserve.gov/press")
        self.assertEqual(result["source_type"], "press_release")

    def test_handles_missing_source_dict(self):
        """Should handle records with no source dict."""
        record = {"id": "test", "status": "accepted"}

        result = source_analytics.extract_source_from_record(record)

        self.assertEqual(result["name"], "")
        self.assertEqual(result["domain"], "")
        self.assertEqual(result["url"], "")
        self.assertEqual(result["source_type"], "")

    def test_handles_missing_domain_uses_url(self):
        """Should extract domain from URL when domain field is missing."""
        record = {
            "source": {
                "name": "Test",
                "url": "https://example.com/page",
            }
        }

        result = source_analytics.extract_source_from_record(record)

        self.assertEqual(result["domain"], "example.com")

    def test_normalizes_domain_lowercase(self):
        """Should lowercase domain."""
        record = make_record(domain="EXAMPLE.COM")

        result = source_analytics.extract_source_from_record(record)

        self.assertEqual(result["domain"], "example.com")

    def test_strips_www_prefix(self):
        """Should strip www. prefix from domain."""
        record = make_record(domain="www.example.com")

        result = source_analytics.extract_source_from_record(record)

        self.assertEqual(result["domain"], "example.com")


# =============================================================================
# TestExtractSourceFromFiltered
# =============================================================================


class TestExtractSourceFromFiltered(unittest.TestCase):
    """Test source extraction from filtered records."""

    def test_extracts_fields(self):
        """Should extract name, domain, url from filtered record."""
        filtered = {
            "source_name": "Test Source",
            "source_domain": "test.com",
            "source_url": "https://test.com/article",
        }

        result = source_analytics.extract_source_from_filtered(filtered)

        self.assertEqual(result["name"], "Test Source")
        self.assertEqual(result["domain"], "test.com")
        self.assertEqual(result["url"], "https://test.com/article")

    def test_sets_source_type_to_filtered(self):
        """Should set source_type to 'filtered'."""
        filtered = {
            "source_name": "Test",
            "source_domain": "test.com",
            "source_url": "https://test.com/",
        }

        result = source_analytics.extract_source_from_filtered(filtered)

        self.assertEqual(result["source_type"], "filtered")

    def test_normalizes_domain(self):
        """Should normalize domain (lowercase, strip www)."""
        filtered = {
            "source_name": "Test",
            "source_domain": "WWW.EXAMPLE.COM",
            "source_url": "https://example.com/",
        }

        result = source_analytics.extract_source_from_filtered(filtered)

        self.assertEqual(result["domain"], "example.com")


# =============================================================================
# TestComputeRatios
# =============================================================================


class TestComputeRatios(unittest.TestCase):
    """Test ratio calculation."""

    def test_calculates_all_ratios(self):
        """Should calculate all four ratios correctly."""
        counts = {
            "accepted": 10,
            "review": 20,
            "rejected": 30,
            "filtered": 40,
        }

        result = source_analytics.compute_ratios(counts)

        self.assertEqual(result["accepted_ratio"], 0.10)
        self.assertEqual(result["review_ratio"], 0.20)
        self.assertEqual(result["rejected_ratio"], 0.30)
        self.assertEqual(result["filtered_ratio"], 0.40)

    def test_ratios_sum_to_one(self):
        """All ratios should sum to 1.0."""
        counts = {
            "accepted": 25,
            "review": 25,
            "rejected": 25,
            "filtered": 25,
        }

        result = source_analytics.compute_ratios(counts)

        total = (
            result["accepted_ratio"]
            + result["review_ratio"]
            + result["rejected_ratio"]
            + result["filtered_ratio"]
        )
        self.assertAlmostEqual(total, 1.0, places=10)

    def test_handles_zero_total(self):
        """Should return all zeros when total is zero."""
        counts = {
            "accepted": 0,
            "review": 0,
            "rejected": 0,
            "filtered": 0,
        }

        result = source_analytics.compute_ratios(counts)

        self.assertEqual(result["accepted_ratio"], 0.0)
        self.assertEqual(result["review_ratio"], 0.0)
        self.assertEqual(result["rejected_ratio"], 0.0)
        self.assertEqual(result["filtered_ratio"], 0.0)

    def test_handles_partial_counts(self):
        """Should handle missing keys in counts dict."""
        counts = {"accepted": 5}

        result = source_analytics.compute_ratios(counts)

        self.assertEqual(result["accepted_ratio"], 1.0)
        self.assertEqual(result["review_ratio"], 0.0)
        self.assertEqual(result["rejected_ratio"], 0.0)
        self.assertEqual(result["filtered_ratio"], 0.0)


# =============================================================================
# TestComputeAvgPriorityScore
# =============================================================================


class TestComputeAvgPriorityScore(unittest.TestCase):
    """Test average priority score calculation."""

    def test_computes_average(self):
        """Should compute average of priority scores."""
        records = [
            {"priority_score": 80},
            {"priority_score": 90},
            {"priority_score": 70},
        ]

        result = source_analytics.compute_avg_priority_score(records)

        self.assertEqual(result, 80.0)

    def test_returns_zero_for_no_scores(self):
        """Should return 0 when no records have priority scores."""
        records = [
            {"id": "1"},
            {"id": "2"},
        ]

        result = source_analytics.compute_avg_priority_score(records)

        self.assertEqual(result, 0.0)

    def test_handles_missing_priority_score(self):
        """Should skip records without priority_score."""
        records = [
            {"priority_score": 80},
            {"id": "no score"},
            {"priority_score": 100},
        ]

        result = source_analytics.compute_avg_priority_score(records)

        self.assertEqual(result, 90.0)


# =============================================================================
# TestComputeAvgVerificationConfidence
# =============================================================================


class TestComputeAvgVerificationConfidence(unittest.TestCase):
    """Test average verification confidence calculation."""

    def test_computes_average(self):
        """Should compute average of verification_confidence values."""
        records = [
            {"llm_review": {"verification_confidence": 8}},
            {"llm_review": {"verification_confidence": 6}},
            {"llm_review": {"verification_confidence": 10}},
        ]

        result = source_analytics.compute_avg_verification_confidence(records)

        self.assertEqual(result, 8.0)

    def test_returns_zero_for_no_confidence(self):
        """Should return 0 when no records have confidence values."""
        records = [
            {"id": "1"},
            {"llm_review": {}},
        ]

        result = source_analytics.compute_avg_verification_confidence(records)

        self.assertEqual(result, 0.0)

    def test_handles_missing_llm_review(self):
        """Should skip records without llm_review."""
        records = [
            {"llm_review": {"verification_confidence": 8}},
            {"id": "no review"},
            {"llm_review": {"verification_confidence": 10}},
        ]

        result = source_analytics.compute_avg_verification_confidence(records)

        self.assertEqual(result, 9.0)


# =============================================================================
# TestGetLastSeenAt
# =============================================================================


class TestGetLastSeenAt(unittest.TestCase):
    """Test timestamp extraction."""

    def test_returns_most_recent(self):
        """Should return the most recent (max) timestamp."""
        records = [
            {"created_at": "2026-04-01T00:00:00+00:00"},
            {"created_at": "2026-04-03T00:00:00+00:00"},
            {"created_at": "2026-04-02T00:00:00+00:00"},
        ]

        result = source_analytics.get_last_seen_at(records)

        self.assertEqual(result, "2026-04-03T00:00:00+00:00")

    def test_returns_empty_for_no_timestamps(self):
        """Should return empty string when no timestamps found."""
        records = [
            {"id": "1"},
            {"id": "2"},
        ]

        result = source_analytics.get_last_seen_at(records)

        self.assertEqual(result, "")


# =============================================================================
# TestComputeSourceStats
# =============================================================================


class TestComputeSourceStats(unittest.TestCase):
    """Test full stats computation."""

    def test_groups_by_domain(self):
        """Should group records by source domain."""
        accepted = [
            make_record(domain="source-a.com"),
            make_record(domain="source-a.com"),
            make_record(domain="source-b.com"),
        ]

        stats = source_analytics.compute_source_stats(accepted, [], [], [])

        self.assertEqual(len(stats), 2)
        self.assertIn("source-a.com", stats)
        self.assertIn("source-b.com", stats)
        self.assertEqual(stats["source-a.com"]["records_seen"], 2)
        self.assertEqual(stats["source-b.com"]["records_seen"], 1)

    def test_computes_counts_correctly(self):
        """Should compute correct counts for each bucket."""
        accepted = [make_record(domain="test.com")]
        review = [make_record(domain="test.com")]
        rejected = [make_record(domain="test.com")]
        filtered = [make_filtered_record(domain="test.com")]

        stats = source_analytics.compute_source_stats(
            accepted, review, rejected, filtered
        )

        self.assertEqual(stats["test.com"]["accepted_count"], 1)
        self.assertEqual(stats["test.com"]["review_count"], 1)
        self.assertEqual(stats["test.com"]["rejected_count"], 1)
        self.assertEqual(stats["test.com"]["filtered_out_count"], 1)

    def test_computes_ratios_correctly(self):
        """Should compute correct ratios."""
        accepted = [make_record(domain="test.com")]
        review = [make_record(domain="test.com")]
        rejected = [make_record(domain="test.com")]
        filtered = [make_filtered_record(domain="test.com")]

        stats = source_analytics.compute_source_stats(
            accepted, review, rejected, filtered
        )

        # All equal at 0.25 each
        self.assertEqual(stats["test.com"]["accepted_ratio"], 0.25)
        self.assertEqual(stats["test.com"]["review_ratio"], 0.25)
        self.assertEqual(stats["test.com"]["rejected_ratio"], 0.25)
        self.assertEqual(stats["test.com"]["filtered_ratio"], 0.25)

    def test_handles_empty_inputs(self):
        """Should handle all empty input lists."""
        stats = source_analytics.compute_source_stats([], [], [], [])

        self.assertEqual(stats, {})

    def test_normalizes_domain_for_grouping(self):
        """Should normalize domains for consistent grouping."""
        accepted = [
            make_record(domain="Example.COM"),
            make_record(domain="www.example.com"),
        ]

        stats = source_analytics.compute_source_stats(accepted, [], [], [])

        # Both should be grouped under example.com
        self.assertEqual(len(stats), 1)
        self.assertIn("example.com", stats)
        self.assertEqual(stats["example.com"]["records_seen"], 2)

    def test_uses_most_common_name(self):
        """Should use most common name when multiple names for same domain."""
        accepted = [
            make_record(source_name="Name A", domain="test.com"),
            make_record(source_name="Name A", domain="test.com"),
            make_record(source_name="Name B", domain="test.com"),
        ]

        stats = source_analytics.compute_source_stats(accepted, [], [], [])

        self.assertEqual(stats["test.com"]["source_name"], "Name A")


# =============================================================================
# TestSaveSourceStats
# =============================================================================


class TestSaveSourceStats(unittest.TestCase):
    """Test stats file persistence."""

    def setUp(self):
        """Create temp directory for tests."""
        self.temp_dir = tempfile.mkdtemp()
        self.output_dir = Path(self.temp_dir) / "stats_output"

    def tearDown(self):
        """Clean up temp directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_saves_one_file_per_source(self):
        """Should save one JSON file per source domain."""
        stats = {
            "source-a.com": {
                "source_name": "Source A",
                "source_domain": "source-a.com",
                "records_seen": 5,
            },
            "source-b.com": {
                "source_name": "Source B",
                "source_domain": "source-b.com",
                "records_seen": 3,
            },
        }

        source_analytics.save_source_stats(stats, self.output_dir)

        json_files = list(self.output_dir.glob("*.json"))
        self.assertEqual(len(json_files), 2)

    def test_creates_output_directory(self):
        """Should create output directory if it doesn't exist."""
        stats = {
            "test.com": {
                "source_name": "Test",
                "source_domain": "test.com",
                "records_seen": 1,
            }
        }

        source_analytics.save_source_stats(stats, self.output_dir)

        self.assertTrue(self.output_dir.exists())
        self.assertTrue(self.output_dir.is_dir())

    def test_files_conform_to_schema(self):
        """Saved files should have required schema fields."""
        stats = {
            "test.com": {
                "source_name": "Test",
                "source_domain": "test.com",
                "records_seen": 1,
                "accepted_count": 1,
                "review_count": 0,
                "rejected_count": 0,
                "filtered_out_count": 0,
                "accepted_ratio": 1.0,
                "review_ratio": 0.0,
                "rejected_ratio": 0.0,
                "filtered_ratio": 0.0,
                "avg_priority_score": 0.0,
                "avg_verification_confidence": 0.0,
                "last_seen_at": "",
            }
        }

        source_analytics.save_source_stats(stats, self.output_dir)

        saved_file = self.output_dir / "test.com.json"
        with saved_file.open("r", encoding="utf-8") as f:
            saved_stats = json.load(f)

        required_fields = {
            "source_name",
            "source_domain",
            "records_seen",
            "accepted_count",
            "review_count",
            "rejected_count",
            "filtered_out_count",
            "accepted_ratio",
            "review_ratio",
            "rejected_ratio",
            "filtered_ratio",
            "avg_priority_score",
            "avg_verification_confidence",
            "last_seen_at",
        }
        self.assertEqual(required_fields, set(saved_stats.keys()))


# =============================================================================
# TestLoadSourceStats
# =============================================================================


class TestLoadSourceStats(unittest.TestCase):
    """Test stats file loading."""

    def setUp(self):
        """Create temp directory for tests."""
        self.temp_dir = tempfile.mkdtemp()
        self.stats_dir = Path(self.temp_dir)

    def tearDown(self):
        """Clean up temp directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_loads_saved_stats(self):
        """Should load previously saved stats."""
        stats = {
            "source-a.com": {
                "source_name": "Source A",
                "source_domain": "source-a.com",
                "records_seen": 5,
            },
        }

        source_analytics.save_source_stats(stats, self.stats_dir)
        loaded = source_analytics.load_source_stats(self.stats_dir)

        self.assertIn("source-a.com", loaded)
        self.assertEqual(loaded["source-a.com"]["source_name"], "Source A")

    def test_returns_empty_for_missing_dir(self):
        """Should return empty dict for non-existent directory."""
        result = source_analytics.load_source_stats(Path("/nonexistent/path"))
        self.assertEqual(result, {})


# =============================================================================
# TestStatsFilenameGeneration
# =============================================================================


class TestStatsFilenameGeneration(unittest.TestCase):
    """Test deterministic filename generation."""

    def test_generates_valid_filename(self):
        """Should generate valid JSON filename from domain."""
        filename = source_analytics.generate_stats_filename("example.com")

        self.assertEqual(filename, "example.com.json")
        # Should not contain invalid characters
        self.assertNotIn("/", filename)
        self.assertNotIn(":", filename)

    def test_sanitizes_special_characters(self):
        """Should sanitize special characters in domain names."""
        filename = source_analytics.generate_stats_filename("test:example.com")

        # Colon should be replaced
        self.assertNotIn(":", filename)
        self.assertTrue(filename.endswith(".json"))

    def test_is_deterministic(self):
        """Same input should always produce same output."""
        filename1 = source_analytics.generate_stats_filename("test.com")
        filename2 = source_analytics.generate_stats_filename("test.com")

        self.assertEqual(filename1, filename2)


# =============================================================================
# TestRunWithRealData
# =============================================================================


class TestRunWithRealData(unittest.TestCase):
    """Integration test with actual repo data."""

    def test_run_analytics_produces_stats(self):
        """run_analytics should produce stats using real repo data."""
        base_dir = Path(__file__).resolve().parent.parent

        stats = source_analytics.run_analytics(
            base_dir=base_dir,
            dry_run=True,
        )

        self.assertIsInstance(stats, dict)
        self.assertGreater(len(stats), 0, "Expected at least one source stats")

    def test_stats_count_matches_expected_sources(self):
        """Stats should be generated for all sources found in data directories."""
        base_dir = Path(__file__).resolve().parent.parent

        stats = source_analytics.run_analytics(
            base_dir=base_dir,
            dry_run=True,
        )

        # Each stat entry should have required fields
        for domain, stat in stats.items():
            self.assertIn("records_seen", stat)
            self.assertIn("source_name", stat)
            self.assertGreater(stat["records_seen"], 0)


# =============================================================================
# TestMissingDataHandling
# =============================================================================


class TestMissingDataHandling(unittest.TestCase):
    """Test graceful handling of empty/missing directories."""

    def test_all_empty_directories(self):
        """Should handle case when all directories are empty or missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            (base_dir / "data" / "accepted").mkdir(parents=True)
            (base_dir / "data" / "review_queue").mkdir(parents=True)
            (base_dir / "data" / "rejected").mkdir(parents=True)
            (base_dir / "data" / "filtered_out").mkdir(parents=True)

            stats = source_analytics.run_analytics(
                base_dir=base_dir,
                dry_run=True,
            )

            self.assertEqual(stats, {})

    def test_mixed_empty_and_populated(self):
        """Should handle mix of empty and populated directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            accepted_dir = base_dir / "data" / "accepted"
            accepted_dir.mkdir(parents=True)
            (base_dir / "data" / "review_queue").mkdir(parents=True)
            (base_dir / "data" / "rejected").mkdir(parents=True)
            (base_dir / "data" / "filtered_out").mkdir(parents=True)

            # Add a record to accepted
            record = make_record(domain="test.com")
            (accepted_dir / "test.json").write_text(
                json.dumps(record), encoding="utf-8"
            )

            stats = source_analytics.run_analytics(
                base_dir=base_dir,
                dry_run=True,
            )

            self.assertIn("test.com", stats)


# =============================================================================
# TestStatsAreDeterministic
# =============================================================================


class TestStatsAreDeterministic(unittest.TestCase):
    """Test that same input produces same output."""

    def test_same_input_same_output(self):
        """Running analytics twice with same data should produce same stats."""
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            accepted_dir = base_dir / "data" / "accepted"
            accepted_dir.mkdir(parents=True)

            # Create same records
            record1 = make_record(
                source_name="Test Source",
                domain="test.com",
                created_at="2026-04-01T00:00:00+00:00",
            )
            record2 = make_record(
                source_name="Test Source",
                domain="test.com",
                created_at="2026-04-02T00:00:00+00:00",
            )

            (accepted_dir / "record1.json").write_text(
                json.dumps(record1), encoding="utf-8"
            )
            (accepted_dir / "record2.json").write_text(
                json.dumps(record2), encoding="utf-8"
            )

            # Run twice
            stats1 = source_analytics.run_analytics(base_dir=base_dir, dry_run=True)
            stats2 = source_analytics.run_analytics(base_dir=base_dir, dry_run=True)

            # Should produce same results
            self.assertEqual(stats1.keys(), stats2.keys())
            if "test.com" in stats1:
                self.assertEqual(
                    stats1["test.com"]["records_seen"],
                    stats2["test.com"]["records_seen"],
                )


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    unittest.main()
