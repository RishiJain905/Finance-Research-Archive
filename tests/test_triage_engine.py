"""
Tests for Stream 2: Triage Engine (triage_engine.py)

Following TDD (Red-Green-Refactor), these tests will initially FAIL
because triage_engine.py does not exist yet. Once the implementation
is created, these tests should pass.

Tests verify:
1. Candidate loading from directory of JSON files
2. Score calculation for all 8 dimensions
3. Priority band assignment
4. Action determination
5. Reasons generation
"""

import json
import os
import tempfile
import unittest
from pathlib import Path

# Import the module under test - this will fail until triage_engine.py is created
from scripts.triage_engine import (
    load_candidates,
    compute_triage_score,
    compute_source_trust,
    compute_freshness,
    compute_topic_relevance,
    compute_title_quality,
    compute_url_quality,
    compute_novelty,
    compute_quant_value,
    compute_duplicate_risk,
    calculate_weighted_score,
    assign_priority_band,
    generate_reasons,
    determine_action,
    run_triage,
)


# =============================================================================
# Test Fixtures / Helper Data
# =============================================================================


def create_mock_candidate(
    candidate_id="test_001",
    lane="trusted_sources",
    source_domain="federalreserve.gov",
    title="FOMC Statement March 2026",
    anchor_text="Federal Reserve issues statement",
    url="https://www.federalreserve.gov/press-release-2026",
    discovered_at="2026-03-29T12:00:00Z",
    domain_trust_score=100,
    freshness_hours=2.0,
    keyword_match_score=80,
    title_quality_score=70,
    url_quality_score=60,
    duplication_risk_score=10,
    topic_hints=None,
    source_type="press_release",
):
    """Factory to create mock candidate data simulating V2 scoring output."""
    if topic_hints is None:
        topic_hints = ["inflation", "rates"]
    return {
        "candidate_id": candidate_id,
        "lane": lane,
        "source_domain": source_domain,
        "title": title,
        "anchor_text": anchor_text,
        "url": url,
        "discovered_at": discovered_at,
        "domain_trust_score": domain_trust_score,
        "freshness_hours": freshness_hours,
        "keyword_match_score": keyword_match_score,
        "title_quality_score": title_quality_score,
        "url_quality_score": url_quality_score,
        "duplication_risk_score": duplication_risk_score,
        "topic_hints": topic_hints,
        "source_type": source_type,
    }


# =============================================================================
# TestLoadCandidates
# =============================================================================


class TestLoadCandidates(unittest.TestCase):
    """Tests for load_candidates function."""

    def setUp(self):
        """Create a temporary directory with mock candidate JSON files."""
        self.temp_dir = tempfile.mkdtemp()
        self.candidates_dir = Path(self.temp_dir)

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_load_candidates_returns_list(self):
        """load_candidates returns a list."""
        # Create a single test candidate file
        candidate = create_mock_candidate()
        candidate_path = self.candidates_dir / "candidate_001.json"
        with candidate_path.open("w", encoding="utf-8") as f:
            json.dump(candidate, f)

        result = load_candidates(self.candidates_dir)
        self.assertIsInstance(result, list)

    def test_load_candidates_parses_json_file(self):
        """load_candidates correctly parses a candidate JSON file."""
        candidate = create_mock_candidate(candidate_id="test_file_001")
        candidate_path = self.candidates_dir / "candidate_test.json"
        with candidate_path.open("w", encoding="utf-8") as f:
            json.dump(candidate, f)

        result = load_candidates(self.candidates_dir)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["candidate_id"], "test_file_001")

    def test_load_candidates_loads_multiple_files(self):
        """load_candidates loads all JSON files in directory."""
        for i in range(3):
            candidate = create_mock_candidate(candidate_id=f"test_{i}")
            candidate_path = self.candidates_dir / f"candidate_{i}.json"
            with candidate_path.open("w", encoding="utf-8") as f:
                json.dump(candidate, f)

        result = load_candidates(self.candidates_dir)
        self.assertEqual(len(result), 3)

    def test_load_candidates_ignores_non_json_files(self):
        """load_candidates ignores non-JSON files."""
        candidate = create_mock_candidate(candidate_id="json_candidate")
        candidate_path = self.candidates_dir / "candidate.json"
        with candidate_path.open("w", encoding="utf-8") as f:
            json.dump(candidate, f)

        # Create a non-JSON file
        text_path = self.candidates_dir / "readme.txt"
        with text_path.open("w", encoding="utf-8") as f:
            f.write("This is not a candidate")

        result = load_candidates(self.candidates_dir)
        self.assertEqual(len(result), 1)

    def test_load_candidates_returns_empty_for_empty_directory(self):
        """load_candidates returns empty list for empty directory."""
        result = load_candidates(self.candidates_dir)
        self.assertEqual(result, [])


# =============================================================================
# TestComputeSourceTrust
# =============================================================================


class TestComputeSourceTrust(unittest.TestCase):
    """Tests for compute_source_trust function."""

    def test_source_trust_maps_from_domain_trust_score(self):
        """source_trust should directly map from domain_trust_score (0-100)."""
        candidate = create_mock_candidate(domain_trust_score=80)
        result = compute_source_trust(candidate)
        self.assertEqual(result, 80.0)

    def test_source_trust_full_score_for_high_trust(self):
        """High domain trust (100) returns high source_trust."""
        candidate = create_mock_candidate(domain_trust_score=100)
        result = compute_source_trust(candidate)
        self.assertEqual(result, 100.0)

    def test_source_trust_low_score_for_low_trust(self):
        """Low domain trust (0) returns low source_trust."""
        candidate = create_mock_candidate(domain_trust_score=0)
        result = compute_source_trust(candidate)
        self.assertEqual(result, 0.0)

    def test_source_trust_clamps_negative_values(self):
        """Negative domain_trust_score is clamped to 0."""
        candidate = create_mock_candidate(domain_trust_score=-10)
        result = compute_source_trust(candidate)
        self.assertEqual(result, 0.0)

    def test_source_trust_clamps_values_above_100(self):
        """domain_trust_score above 100 is clamped to 100."""
        candidate = create_mock_candidate(domain_trust_score=150)
        result = compute_source_trust(candidate)
        self.assertEqual(result, 100.0)


# =============================================================================
# TestComputeFreshness
# =============================================================================


class TestComputeFreshness(unittest.TestCase):
    """Tests for compute_freshness function."""

    def test_freshness_high_for_recent_candidate(self):
        """Very recent candidate (freshness_hours=0) gets high freshness score."""
        candidate = create_mock_candidate(freshness_hours=0)
        result = compute_freshness(candidate)
        self.assertEqual(result, 100.0)

    def test_freshness_decreases_with_age(self):
        """Freshness score decreases as freshness_hours increases."""
        candidate_1h = create_mock_candidate(freshness_hours=1)
        candidate_24h = create_mock_candidate(freshness_hours=24)

        result_1h = compute_freshness(candidate_1h)
        result_24h = compute_freshness(candidate_24h)

        self.assertGreater(result_1h, result_24h)

    def test_freshness_zero_for_very_old_candidate(self):
        """Candidate older than max_hours gets 0 freshness."""
        # Assuming max freshness is around 168 hours (1 week)
        candidate = create_mock_candidate(freshness_hours=500)
        result = compute_freshness(candidate)
        self.assertEqual(result, 0.0)

    def test_freshness_half_score_at_midpoint(self):
        """Freshness at midpoint should give approximately half score."""
        # At 84 hours (half of 168 max), should be around 50
        candidate = create_mock_candidate(freshness_hours=84)
        result = compute_freshness(candidate)
        self.assertGreater(result, 40)
        self.assertLess(result, 60)


# =============================================================================
# TestComputeTopicRelevance
# =============================================================================


class TestComputeTopicRelevance(unittest.TestCase):
    """Tests for compute_topic_relevance function."""

    def test_topic_relevance_uses_keyword_match_score(self):
        """topic_relevance should incorporate keyword_match_score."""
        candidate = create_mock_candidate(keyword_match_score=80)
        result = compute_topic_relevance(candidate)
        # Should be influenced by keyword_match_score
        self.assertGreater(result, 0)

    def test_topic_relevance_increases_with_keyword_match(self):
        """Higher keyword_match_score increases topic_relevance."""
        candidate_low = create_mock_candidate(keyword_match_score=20)
        candidate_high = create_mock_candidate(keyword_match_score=100)

        result_low = compute_topic_relevance(candidate_low)
        result_high = compute_topic_relevance(candidate_high)

        self.assertGreater(result_high, result_low)

    def test_topic_relevance_considers_topic_hints(self):
        """topic_relevance should be higher when topic_hints are present."""
        candidate_no_hints = create_mock_candidate(
            keyword_match_score=50, topic_hints=[]
        )
        candidate_with_hints = create_mock_candidate(
            keyword_match_score=50, topic_hints=["inflation", "rates"]
        )

        result_no_hints = compute_topic_relevance(candidate_no_hints)
        result_with_hints = compute_topic_relevance(candidate_with_hints)

        self.assertGreater(result_with_hints, result_no_hints)

    def test_topic_relevance_zero_when_no_signals(self):
        """topic_relevance is low when no keyword match and no topic hints."""
        candidate = create_mock_candidate(keyword_match_score=0, topic_hints=[])
        result = compute_topic_relevance(candidate)
        self.assertLess(result, 20)


# =============================================================================
# TestComputeTitleQuality
# =============================================================================


class TestComputeTitleQuality(unittest.TestCase):
    """Tests for compute_title_quality function."""

    def test_title_quality_maps_from_title_quality_score(self):
        """title_quality should directly map from title_quality_score (0-100)."""
        candidate = create_mock_candidate(title_quality_score=75)
        result = compute_title_quality(candidate)
        self.assertEqual(result, 75.0)

    def test_title_quality_full_for_good_title(self):
        """High title_quality_score returns high title_quality."""
        candidate = create_mock_candidate(title_quality_score=100)
        result = compute_title_quality(candidate)
        self.assertEqual(result, 100.0)

    def test_title_quality_zero_for_poor_title(self):
        """Zero title_quality_score returns zero title_quality."""
        candidate = create_mock_candidate(title_quality_score=0)
        result = compute_title_quality(candidate)
        self.assertEqual(result, 0.0)


# =============================================================================
# TestComputeUrlQuality
# =============================================================================


class TestComputeUrlQuality(unittest.TestCase):
    """Tests for compute_url_quality function."""

    def test_url_quality_maps_from_url_quality_score(self):
        """url_quality should directly map from url_quality_score (0-100)."""
        candidate = create_mock_candidate(url_quality_score=65)
        result = compute_url_quality(candidate)
        self.assertEqual(result, 65.0)

    def test_url_quality_full_for_good_url(self):
        """High url_quality_score returns high url_quality."""
        candidate = create_mock_candidate(url_quality_score=100)
        result = compute_url_quality(candidate)
        self.assertEqual(result, 100.0)

    def test_url_quality_zero_for_poor_url(self):
        """Zero url_quality_score returns zero url_quality."""
        candidate = create_mock_candidate(url_quality_score=0)
        result = compute_url_quality(candidate)
        self.assertEqual(result, 0.0)


# =============================================================================
# TestComputeNovelty
# =============================================================================


class TestComputeNovelty(unittest.TestCase):
    """Tests for compute_novelty function."""

    def test_novelty_is_inverse_of_duplicate_risk(self):
        """novelty = 100 - duplication_risk_score."""
        candidate = create_mock_candidate(duplication_risk_score=30)
        result = compute_novelty(candidate)
        self.assertEqual(result, 70.0)

    def test_novelty_full_when_no_duplicate_risk(self):
        """novelty is 100 when duplication_risk_score is 0."""
        candidate = create_mock_candidate(duplication_risk_score=0)
        result = compute_novelty(candidate)
        self.assertEqual(result, 100.0)

    def test_novelty_zero_when_full_duplicate_risk(self):
        """novelty is 0 when duplication_risk_score is 100."""
        candidate = create_mock_candidate(duplication_risk_score=100)
        result = compute_novelty(candidate)
        self.assertEqual(result, 0.0)


# =============================================================================
# TestComputeQuantValue
# =============================================================================


class TestComputeQuantValue(unittest.TestCase):
    """Tests for compute_quant_value function."""

    def test_quant_value_zero_for_non_quant_lane(self):
        """quant_value should be 0 for non-quant lanes."""
        candidate = create_mock_candidate(lane="trusted_sources")
        result = compute_quant_value(candidate)
        self.assertEqual(result, 0.0)

    def test_quant_value_high_for_recent_quant_data(self):
        """quant_value is high for recent quant lane candidates."""
        candidate = create_mock_candidate(
            lane="quant",
            freshness_hours=1,
        )
        result = compute_quant_value(candidate)
        self.assertGreater(result, 50)

    def test_quant_value_decreases_with_age_for_quant(self):
        """quant_value decreases for older quant candidates."""
        candidate_recent = create_mock_candidate(lane="quant", freshness_hours=1)
        candidate_old = create_mock_candidate(lane="quant", freshness_hours=100)

        result_recent = compute_quant_value(candidate_recent)
        result_old = compute_quant_value(candidate_old)

        self.assertGreater(result_recent, result_old)


# =============================================================================
# TestComputeDuplicateRisk
# =============================================================================


class TestComputeDuplicateRisk(unittest.TestCase):
    """Tests for compute_duplicate_risk function."""

    def test_duplicate_risk_maps_from_duplication_risk_score(self):
        """duplicate_risk should directly map from duplication_risk_score (0-100)."""
        candidate = create_mock_candidate(duplication_risk_score=25)
        result = compute_duplicate_risk(candidate)
        self.assertEqual(result, 25.0)

    def test_duplicate_risk_full_for_high_risk(self):
        """High duplication_risk_score returns high duplicate_risk."""
        candidate = create_mock_candidate(duplication_risk_score=100)
        result = compute_duplicate_risk(candidate)
        self.assertEqual(result, 100.0)

    def test_duplicate_risk_zero_for_no_risk(self):
        """Zero duplication_risk_score returns zero duplicate_risk."""
        candidate = create_mock_candidate(duplication_risk_score=0)
        result = compute_duplicate_risk(candidate)
        self.assertEqual(result, 0.0)


# =============================================================================
# TestComputeTriageScore
# =============================================================================


class TestComputeTriageScore(unittest.TestCase):
    """Tests for compute_triage_score function."""

    def test_compute_triage_score_returns_dict(self):
        """compute_triage_score returns a dictionary."""
        candidate = create_mock_candidate()
        weights = {
            "source_trust": 0.23,
            "freshness": 0.17,
            "topic_relevance": 0.23,
            "title_quality": 0.12,
            "url_quality": 0.12,
            "novelty": 0.12,
            "quant_value": 0.16,
            "duplicate_risk": -0.15,
        }
        result = compute_triage_score(candidate, weights)
        self.assertIsInstance(result, dict)

    def test_compute_triage_score_has_all_eight_components(self):
        """compute_triage_score returns all 8 scoring components."""
        candidate = create_mock_candidate()
        weights = {
            "source_trust": 0.23,
            "freshness": 0.17,
            "topic_relevance": 0.23,
            "title_quality": 0.12,
            "url_quality": 0.12,
            "novelty": 0.12,
            "quant_value": 0.16,
            "duplicate_risk": -0.15,
        }
        result = compute_triage_score(candidate, weights)

        expected_components = [
            "source_trust",
            "freshness",
            "topic_relevance",
            "title_quality",
            "url_quality",
            "novelty",
            "quant_value",
            "duplicate_risk",
        ]
        # Components are nested under "scoring" key per spec
        scoring = result.get("scoring", {})
        for component in expected_components:
            self.assertIn(component, scoring, f"Missing component: {component}")

    def test_compute_triage_score_values_are_0_to_100(self):
        """All scoring components are in range 0-100."""
        candidate = create_mock_candidate()
        weights = {
            "source_trust": 0.23,
            "freshness": 0.17,
            "topic_relevance": 0.23,
            "title_quality": 0.12,
            "url_quality": 0.12,
            "novelty": 0.12,
            "quant_value": 0.16,
            "duplicate_risk": -0.15,
        }
        result = compute_triage_score(candidate, weights)

        # Components are nested under "scoring" key per spec
        scoring = result.get("scoring", {})
        for component, value in scoring.items():
            if not isinstance(value, (int, float)):
                continue
            self.assertGreaterEqual(
                value,
                0.0,
                f"Component {component} value {value} is less than 0",
            )
            self.assertLessEqual(
                value,
                100.0,
                f"Component {component} value {value} is greater than 100",
            )

    def test_compute_triage_score_includes_weighted_score(self):
        """Result includes the weighted_score calculation (as priority_score per spec)."""
        candidate = create_mock_candidate()
        weights = {
            "source_trust": 0.23,
            "freshness": 0.17,
            "topic_relevance": 0.23,
            "title_quality": 0.12,
            "url_quality": 0.12,
            "novelty": 0.12,
            "quant_value": 0.16,
            "duplicate_risk": -0.15,
        }
        result = compute_triage_score(candidate, weights)
        # Per spec, the score is called "priority_score"
        self.assertIn("priority_score", result)


# =============================================================================
# TestCalculateWeightedScore
# =============================================================================


class TestCalculateWeightedScore(unittest.TestCase):
    """Tests for calculate_weighted_score function."""

    def test_weighted_score_sums_correctly(self):
        """calculate_weighted_score correctly applies weights."""
        scoring = {
            "source_trust": 100.0,
            "freshness": 100.0,
            "topic_relevance": 100.0,
            "title_quality": 100.0,
            "url_quality": 100.0,
            "novelty": 100.0,
            "quant_value": 0.0,  # Not applicable for non-quant
            "duplicate_risk": 0.0,  # Negative weight
        }
        weights = {
            "source_trust": 0.23,
            "freshness": 0.17,
            "topic_relevance": 0.23,
            "title_quality": 0.12,
            "url_quality": 0.12,
            "novelty": 0.12,
            "quant_value": 0.16,
            "duplicate_risk": -0.15,
        }
        result = calculate_weighted_score(scoring, weights)
        # Should be a valid score between 0-100
        self.assertGreaterEqual(result, 0.0)
        self.assertLessEqual(result, 100.0)

    def test_negative_weight_reduces_score(self):
        """duplicate_risk with negative weight reduces final score."""
        scoring_high_risk = {
            "source_trust": 80.0,
            "freshness": 80.0,
            "topic_relevance": 80.0,
            "title_quality": 80.0,
            "url_quality": 80.0,
            "novelty": 80.0,
            "quant_value": 0.0,
            "duplicate_risk": 100.0,  # High risk
        }
        scoring_low_risk = {
            "source_trust": 80.0,
            "freshness": 80.0,
            "topic_relevance": 80.0,
            "title_quality": 80.0,
            "url_quality": 80.0,
            "novelty": 80.0,
            "quant_value": 0.0,
            "duplicate_risk": 0.0,  # No risk
        }
        weights = {
            "source_trust": 0.23,
            "freshness": 0.17,
            "topic_relevance": 0.23,
            "title_quality": 0.12,
            "url_quality": 0.12,
            "novelty": 0.12,
            "quant_value": 0.16,
            "duplicate_risk": -0.15,
        }

        result_high_risk = calculate_weighted_score(scoring_high_risk, weights)
        result_low_risk = calculate_weighted_score(scoring_low_risk, weights)

        self.assertLess(result_high_risk, result_low_risk)

    def test_weighted_score_handles_missing_components(self):
        """calculate_weighted_score handles missing scoring components."""
        scoring = {
            "source_trust": 100.0,
            # Other components missing
        }
        weights = {
            "source_trust": 0.23,
            "freshness": 0.17,
            "topic_relevance": 0.23,
            "title_quality": 0.12,
            "url_quality": 0.12,
            "novelty": 0.12,
            "quant_value": 0.16,
            "duplicate_risk": -0.15,
        }
        result = calculate_weighted_score(scoring, weights)
        # Should still compute with missing components treated as 0
        self.assertIsInstance(result, float)


# =============================================================================
# TestAssignPriorityBand
# =============================================================================


class TestAssignPriorityBand(unittest.TestCase):
    """Tests for assign_priority_band function."""

    def setUp(self):
        """Standard band configuration."""
        self.bands = {
            "critical": 85,
            "high": 70,
            "medium": 50,
            "low": 30,
        }

    def test_critical_band_for_high_score(self):
        """Score >= 85 gets critical band."""
        result = assign_priority_band(90, self.bands)
        self.assertEqual(result, "critical")

    def test_high_band_for_medium_high_score(self):
        """Score >= 70 and < 85 gets high band."""
        result = assign_priority_band(75, self.bands)
        self.assertEqual(result, "high")

    def test_medium_band_for_medium_score(self):
        """Score >= 50 and < 70 gets medium band."""
        result = assign_priority_band(60, self.bands)
        self.assertEqual(result, "medium")

    def test_low_band_for_low_score(self):
        """Score >= 30 and < 50 gets low band."""
        result = assign_priority_band(40, self.bands)
        self.assertEqual(result, "low")

    def test_below_low_band_for_very_low_score(self):
        """Score < 30 gets discard band (per spec)."""
        result = assign_priority_band(20, self.bands)
        self.assertEqual(result, "discard")

    def test_boundary_at_critical_threshold(self):
        """Score exactly at 85 gets critical."""
        result = assign_priority_band(85, self.bands)
        self.assertEqual(result, "critical")

    def test_boundary_at_high_threshold(self):
        """Score exactly at 70 gets high."""
        result = assign_priority_band(70, self.bands)
        self.assertEqual(result, "high")

    def test_boundary_at_medium_threshold(self):
        """Score exactly at 50 gets medium."""
        result = assign_priority_band(50, self.bands)
        self.assertEqual(result, "medium")

    def test_boundary_at_low_threshold(self):
        """Score exactly at 30 gets low."""
        result = assign_priority_band(30, self.bands)
        self.assertEqual(result, "low")


# =============================================================================
# TestGenerateReasons
# =============================================================================


class TestGenerateReasons(unittest.TestCase):
    """Tests for generate_reasons function."""

    def test_generate_reasons_returns_list(self):
        """generate_reasons returns a list of strings."""
        scoring = {
            "source_trust": 100.0,
            "freshness": 90.0,
            "topic_relevance": 80.0,
            "title_quality": 70.0,
            "url_quality": 60.0,
            "novelty": 95.0,
            "quant_value": 0.0,
            "duplicate_risk": 10.0,
        }
        result = generate_reasons(scoring)
        self.assertIsInstance(result, list)
        for reason in result:
            self.assertIsInstance(reason, str)

    def test_generate_reasons_includes_high_source_trust(self):
        """Reasons include high source_trust."""
        scoring = {
            "source_trust": 100.0,
            "freshness": 50.0,
            "topic_relevance": 50.0,
            "title_quality": 50.0,
            "url_quality": 50.0,
            "novelty": 50.0,
            "quant_value": 0.0,
            "duplicate_risk": 50.0,
        }
        reasons = generate_reasons(scoring)
        reasons_text = " ".join(reasons).lower()
        self.assertTrue(
            any("trust" in r.lower() or "source" in r.lower() for r in reasons),
            f"Expected trust/source in reasons, got: {reasons}",
        )

    def test_generate_reasons_includes_freshness(self):
        """Reasons include freshness."""
        scoring = {
            "source_trust": 50.0,
            "freshness": 95.0,
            "topic_relevance": 50.0,
            "title_quality": 50.0,
            "url_quality": 50.0,
            "novelty": 50.0,
            "quant_value": 0.0,
            "duplicate_risk": 50.0,
        }
        reasons = generate_reasons(scoring)
        reasons_text = " ".join(reasons).lower()
        self.assertTrue(
            any("fresh" in r.lower() or "recent" in r.lower() for r in reasons),
            f"Expected fresh/recent in reasons, got: {reasons}",
        )

    def test_generate_reasons_includes_duplicate_risk(self):
        """Reasons include duplicate risk when high."""
        scoring = {
            "source_trust": 50.0,
            "freshness": 50.0,
            "topic_relevance": 50.0,
            "title_quality": 50.0,
            "url_quality": 50.0,
            "novelty": 5.0,
            "quant_value": 0.0,
            "duplicate_risk": 95.0,
        }
        reasons = generate_reasons(scoring)
        reasons_text = " ".join(reasons).lower()
        self.assertTrue(
            any("duplicate" in r.lower() or "risk" in r.lower() for r in reasons),
            f"Expected duplicate/risk in reasons, got: {reasons}",
        )


# =============================================================================
# TestDetermineAction
# =============================================================================


class TestDetermineAction(unittest.TestCase):
    """Tests for determine_action function."""

    def setUp(self):
        """Standard budget configuration."""
        self.budget_config = {
            "article_process_limit": 25,
            "quant_process_limit": 10,
            "defer_medium": True,
        }

    def test_critical_band_gets_process_now(self):
        """critical band always gets process_now."""
        result = determine_action("critical", self.budget_config)
        self.assertEqual(result, "process_now")

    def test_high_band_gets_process_now(self):
        """high band always gets process_now."""
        result = determine_action("high", self.budget_config)
        self.assertEqual(result, "process_now")

    def test_medium_band_gets_defer_when_defer_medium_true(self):
        """medium band gets defer when defer_medium is True."""
        result = determine_action("medium", self.budget_config)
        self.assertEqual(result, "defer")

    def test_medium_band_gets_process_now_when_defer_medium_false(self):
        """medium band gets process_now when defer_medium is False."""
        config = self.budget_config.copy()
        config["defer_medium"] = False
        result = determine_action("medium", config)
        self.assertEqual(result, "process_now")

    def test_low_band_gets_defer(self):
        """low band gets defer."""
        result = determine_action("low", self.budget_config)
        self.assertEqual(result, "defer")

    def test_below_low_band_gets_discard(self):
        """below_low band gets discard."""
        result = determine_action("below_low", self.budget_config)
        self.assertEqual(result, "discard")


# =============================================================================
# TestRunTriage
# =============================================================================


class TestRunTriage(unittest.TestCase):
    """Tests for run_triage function."""

    def setUp(self):
        """Standard configuration."""
        self.weights = {
            "source_trust": 0.23,
            "freshness": 0.17,
            "topic_relevance": 0.23,
            "title_quality": 0.12,
            "url_quality": 0.12,
            "novelty": 0.12,
            "quant_value": 0.16,
            "duplicate_risk": -0.15,
        }
        self.bands = {
            "critical": 85,
            "high": 70,
            "medium": 50,
            "low": 30,
        }
        self.budget_config = {
            "article_process_limit": 25,
            "quant_process_limit": 10,
            "defer_medium": True,
        }

    def test_run_triage_returns_three_lists(self):
        """run_triage returns tuple of (process_now, defer, discard)."""
        candidates = [
            create_mock_candidate(candidate_id="c1", domain_trust_score=100),
        ]
        result = run_triage(candidates, self.weights, self.bands, self.budget_config)
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 3)

    def test_run_triage_process_now_for_critical_candidate(self):
        """Critical candidates go to process_now list."""
        candidates = [
            create_mock_candidate(
                candidate_id="critical_1",
                domain_trust_score=100,
                freshness_hours=0,
                keyword_match_score=100,
                title_quality_score=100,
                url_quality_score=100,
                duplication_risk_score=0,
            ),
        ]
        process_now, defer, discard = run_triage(
            candidates, self.weights, self.bands, self.budget_config
        )
        self.assertEqual(len(process_now), 1)
        self.assertEqual(process_now[0]["candidate_id"], "critical_1")

    def test_run_triage_defer_for_medium_candidate(self):
        """Medium candidates go to defer list when defer_medium=True."""
        candidates = [
            create_mock_candidate(
                candidate_id="medium_1",
                domain_trust_score=60,
                freshness_hours=50,
                keyword_match_score=60,
                title_quality_score=60,
                url_quality_score=60,
                duplication_risk_score=30,
            ),
        ]
        process_now, defer, discard = run_triage(
            candidates, self.weights, self.bands, self.budget_config
        )
        self.assertEqual(len(defer), 1)
        self.assertEqual(defer[0]["candidate_id"], "medium_1")

    def test_run_triage_discard_for_low_candidate(self):
        """Low priority candidates go to discard list."""
        candidates = [
            create_mock_candidate(
                candidate_id="low_1",
                domain_trust_score=10,
                freshness_hours=500,
                keyword_match_score=10,
                title_quality_score=10,
                url_quality_score=10,
                duplication_risk_score=90,
            ),
        ]
        process_now, defer, discard = run_triage(
            candidates, self.weights, self.bands, self.budget_config
        )
        self.assertEqual(len(discard), 1)
        self.assertEqual(discard[0]["candidate_id"], "low_1")

    def test_run_triage_adds_triage_result_to_candidates(self):
        """run_triage adds triage_result to each candidate."""
        candidates = [
            create_mock_candidate(candidate_id="test_1"),
        ]
        process_now, defer, discard = run_triage(
            candidates, self.weights, self.bands, self.budget_config
        )
        for candidate in process_now + defer + discard:
            self.assertIn("triage_result", candidate)
            triage = candidate["triage_result"]
            self.assertIn("priority_score", triage)
            self.assertIn("priority_band", triage)
            self.assertIn("scoring", triage)
            self.assertIn("action", triage)

    def test_run_triage_sorts_by_priority_score(self):
        """process_now list is sorted by priority_score descending."""
        candidates = [
            create_mock_candidate(
                candidate_id="lower_priority",
                domain_trust_score=70,
                freshness_hours=20,
                keyword_match_score=70,
                title_quality_score=70,
                url_quality_score=70,
                duplication_risk_score=20,
            ),
            create_mock_candidate(
                candidate_id="higher_priority",
                domain_trust_score=100,
                freshness_hours=0,
                keyword_match_score=100,
                title_quality_score=100,
                url_quality_score=100,
                duplication_risk_score=0,
            ),
        ]
        process_now, defer, discard = run_triage(
            candidates, self.weights, self.bands, self.budget_config
        )
        self.assertEqual(process_now[0]["candidate_id"], "higher_priority")
        self.assertEqual(process_now[1]["candidate_id"], "lower_priority")

    def test_run_triage_handles_empty_list(self):
        """run_triage handles empty candidate list."""
        result = run_triage([], self.weights, self.bands, self.budget_config)
        self.assertEqual(result, ([], [], []))


# =============================================================================
# Main
# =============================================================================


if __name__ == "__main__":
    unittest.main()
