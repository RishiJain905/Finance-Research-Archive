"""Tests for propose_keyword_expansions.py"""

import json
import tempfile
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

# Add scripts to path
scripts_path = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(scripts_path))

import pytest


class TestConfidenceCalculation:
    """Test suite for theme confidence score calculation."""

    def test_confidence_calculation_with_accepted_and_rejected(self):
        """Test confidence = accepted / (accepted + rejected + 1) * 100"""
        from propose_keyword_expansions import calculate_theme_confidence

        theme = {
            "theme_id": "test_theme",
            "accepted_count": 80,
            "rejected_count": 20,
        }
        confidence = calculate_theme_confidence(theme)
        # (80 / (80 + 20 + 1)) * 100 = 80/101 * 100 ≈ 79.2
        expected = (80 / 101) * 100
        assert abs(confidence - expected) < 0.1

    def test_confidence_calculation_with_no_rejections(self):
        """Test confidence when all accepted"""
        from propose_keyword_expansions import calculate_theme_confidence

        theme = {
            "theme_id": "test_theme",
            "accepted_count": 50,
            "rejected_count": 0,
        }
        confidence = calculate_theme_confidence(theme)
        # (50 / (50 + 0 + 1)) * 100 = 50/51 * 100 ≈ 98.04
        expected = (50 / 51) * 100
        assert abs(confidence - expected) < 0.1

    def test_confidence_calculation_with_zero_accepted(self):
        """Test confidence when nothing accepted"""
        from propose_keyword_expansions import calculate_theme_confidence

        theme = {
            "theme_id": "test_theme",
            "accepted_count": 0,
            "rejected_count": 10,
        }
        confidence = calculate_theme_confidence(theme)
        # (0 / (0 + 10 + 1)) * 100 = 0
        assert confidence == 0.0

    def test_confidence_calculation_with_no_data(self):
        """Test confidence with empty counts"""
        from propose_keyword_expansions import calculate_theme_confidence

        theme = {
            "theme_id": "test_theme",
        }
        confidence = calculate_theme_confidence(theme)
        # (0 / (0 + 0 + 1)) * 100 = 0
        assert confidence == 0.0


class TestExpansionOpportunities:
    """Test suite for identifying expansion opportunities."""

    def test_theme_not_in_any_bundle(self):
        """Test identifying theme that exists but not in any bundle"""
        from propose_keyword_expansions import identify_expansion_opportunities

        themes = {
            "inflation": {
                "theme_id": "inflation",
                "label": "Inflation",
                "terms": ["cpi", "ppi", "inflation"],
                "accepted_count": 50,
                "rejected_count": 5,
            }
        }
        bundles = {
            "bundle_1": {
                "bundle_id": "bundle_1",
                "label": "Market Structure",
                "keywords": ["liquidity", "repo", "treasury"],
                "priority": 1,
            }
        }

        opportunities = identify_expansion_opportunities(themes, bundles)
        assert "inflation" in opportunities
        assert opportunities["inflation"]["action"] == "create_bundle"

    def test_theme_terms_partially_in_bundle(self):
        """Test identifying theme with partial bundle match"""
        from propose_keyword_expansions import identify_expansion_opportunities

        themes = {
            "inflation": {
                "theme_id": "inflation",
                "label": "Inflation",
                "terms": ["cpi", "ppi", "inflation"],
                "accepted_count": 50,
                "rejected_count": 5,
            }
        }
        bundles = {
            "bundle_1": {
                "bundle_id": "bundle_1",
                "label": "Macro",
                "keywords": ["cpi", "gdp"],
                "priority": 1,
            }
        }

        opportunities = identify_expansion_opportunities(themes, bundles)
        assert "inflation" in opportunities
        # "inflation" term is not in bundle, so propose adding
        assert opportunities["inflation"]["action"] == "add_to_bundle"
        assert "inflation" in opportunities["inflation"]["missing_terms"]

    def test_theme_fully_covered_by_bundle_low_confidence(self):
        """Test identifying theme fully covered by bundle with low confidence (no action)"""
        from propose_keyword_expansions import identify_expansion_opportunities

        themes = {
            "inflation": {
                "theme_id": "inflation",
                "label": "Inflation",
                "terms": ["cpi", "ppi"],
                "accepted_count": 50,
                "rejected_count": 5,
            }
        }
        bundles = {
            "bundle_1": {
                "bundle_id": "bundle_1",
                "label": "Macro",
                "keywords": ["cpi", "ppi", "inflation"],
                "priority": 1,
            }
        }

        opportunities = identify_expansion_opportunities(themes, bundles)
        # Theme is fully covered but confidence = 89.3% >= 80 and priority = 1 < 3
        # So it SHOULD recommend priority increase
        assert "inflation" in opportunities
        assert opportunities["inflation"]["action"] == "increase_priority"

    def test_theme_fully_covered_by_bundle_high_priority(self):
        """Test identifying theme fully covered by high priority bundle (no action)"""
        from propose_keyword_expansions import identify_expansion_opportunities

        themes = {
            "inflation": {
                "theme_id": "inflation",
                "label": "Inflation",
                "terms": ["cpi", "ppi"],
                "accepted_count": 50,
                "rejected_count": 5,
            }
        }
        bundles = {
            "bundle_1": {
                "bundle_id": "bundle_1",
                "label": "Macro",
                "keywords": ["cpi", "ppi", "inflation"],
                "priority": 3,  # Higher priority
            }
        }

        opportunities = identify_expansion_opportunities(themes, bundles)
        # Theme is fully covered and priority is already high (3), no action needed
        assert "inflation" not in opportunities

    def test_high_confidence_theme_low_priority_bundle(self):
        """Test identifying high confidence theme in low priority bundle"""
        from propose_keyword_expansions import identify_expansion_opportunities

        themes = {
            "inflation": {
                "theme_id": "inflation",
                "label": "Inflation",
                "terms": ["cpi", "ppi"],
                "accepted_count": 100,
                "rejected_count": 5,
            }
        }
        bundles = {
            "bundle_1": {
                "bundle_id": "bundle_1",
                "label": "Macro",
                "keywords": ["cpi", "ppi"],
                "priority": 1,
            }
        }

        opportunities = identify_expansion_opportunities(themes, bundles)
        assert "inflation" in opportunities
        # High confidence (100/(100+5+1)*100 ≈ 65.6%) but priority is 1
        # Should recommend priority increase
        assert opportunities["inflation"]["action"] == "increase_priority"


class TestProposalGeneration:
    """Test suite for generating keyword expansion proposals."""

    def test_propose_bundle_addition_structure(self):
        """Test structure of bundle addition proposal"""
        from propose_keyword_expansions import (
            propose_bundle_addition,
            calculate_theme_confidence,
        )

        theme = {
            "theme_id": "inflation",
            "label": "Inflation",
            "terms": ["cpi", "ppi", "inflation"],
            "accepted_count": 80,
            "rejected_count": 10,
        }
        bundle = {
            "bundle_id": "macro_bundle",
            "label": "Macro Catalysts",
            "keywords": ["gdp", "unemployment"],
            "priority": 2,
        }

        proposal = propose_bundle_addition(theme, bundle)

        assert proposal["type"] == "add_to_bundle"
        assert proposal["target_bundle_id"] == "macro_bundle"
        assert "cpi" in proposal["terms_to_add"]
        assert "ppi" in proposal["terms_to_add"]
        assert "inflation" in proposal["terms_to_add"]
        assert proposal["status"] == "pending"
        assert proposal["confidence_score"] == calculate_theme_confidence(theme)

    def test_propose_new_bundle_structure(self):
        """Test structure of new bundle proposal"""
        from propose_keyword_expansions import (
            propose_new_bundle,
            calculate_theme_confidence,
        )

        theme = {
            "theme_id": "repo",
            "label": "Repo Market",
            "terms": ["repo", "repurchase", "funding"],
            "accepted_count": 60,
            "rejected_count": 4,
        }

        proposal = propose_new_bundle(theme)

        assert proposal["type"] == "create_bundle"
        assert proposal["target_bundle_id"] is None
        assert proposal["new_bundle_label"] == "Repo Market"
        assert set(proposal["terms_to_add"]) == {"repo", "repurchase", "funding"}
        assert proposal["status"] == "pending"
        expected_confidence = (60 / (60 + 4 + 1)) * 100
        assert abs(proposal["confidence_score"] - expected_confidence) < 0.1

    def test_propose_priority_increase_structure(self):
        """Test structure of priority increase proposal"""
        from propose_keyword_expansions import propose_priority_increase

        theme = {
            "theme_id": "treasury",
            "label": "Treasury",
            "terms": ["treasury", "bonds", "yields"],
            "accepted_count": 90,
            "rejected_count": 5,
        }
        bundle = {
            "bundle_id": "market_struct",
            "label": "Market Structure",
            "keywords": ["treasury", "bonds"],
            "priority": 1,
        }

        proposal = propose_priority_increase(theme, bundle)

        assert proposal["type"] == "increase_priority"
        assert proposal["target_bundle_id"] == "market_struct"
        assert proposal["new_priority"] == 3  # Current + 2
        assert proposal["status"] == "pending"

    def test_auto_activate_high_confidence(self):
        """Test auto-activation for confidence >= 80"""
        from propose_keyword_expansions import calculate_theme_confidence

        # 80+ confidence should auto-activate
        high_conf_theme = {
            "theme_id": "high_conf",
            "label": "High Confidence",
            "terms": ["term1", "term2"],
            "accepted_count": 100,
            "rejected_count": 10,
        }
        confidence = calculate_theme_confidence(high_conf_theme)
        assert confidence >= 80

        # 50-79 confidence should NOT auto-activate
        medium_conf_theme = {
            "theme_id": "medium_conf",
            "label": "Medium Confidence",
            "terms": ["term1", "term2"],
            "accepted_count": 50,
            "rejected_count": 25,
        }
        confidence = calculate_theme_confidence(medium_conf_theme)
        assert 50 <= confidence < 80


class TestGenerateProposals:
    """Test suite for the main generate_proposals function."""

    def test_min_confidence_threshold(self):
        """Test that proposals below min_confidence are excluded"""
        from propose_keyword_expansions import generate_proposals

        themes = {
            "low_conf": {
                "theme_id": "low_conf",
                "label": "Low Confidence",
                "terms": ["term1"],
                "accepted_count": 10,
                "rejected_count": 90,
            }
        }
        bundles = {}

        proposals = generate_proposals(themes, bundles, min_confidence=50)

        # Low confidence (10/(10+90+1)*100 ≈ 9.9%) should not generate proposals
        assert len(proposals) == 0

    def test_proposals_include_all_opportunity_types(self):
        """Test that proposals cover add, create, and priority increase"""
        from propose_keyword_expansions import generate_proposals

        themes = {
            "add_to_bundle": {
                "theme_id": "add_to_bundle",
                "label": "Add to Bundle",
                "terms": ["term1", "term2"],
                "accepted_count": 50,
                "rejected_count": 5,
            },
            "create_new": {
                "theme_id": "create_new",
                "label": "Create New",
                "terms": ["newterm1"],
                "accepted_count": 40,
                "rejected_count": 5,
            },
        }
        bundles = {
            "existing_bundle": {
                "bundle_id": "existing_bundle",
                "label": "Existing",
                "keywords": ["term1"],  # partial overlap
                "priority": 1,
            }
        }

        proposals = generate_proposals(themes, bundles, min_confidence=50)

        proposal_types = [p["type"] for p in proposals]
        assert "add_to_bundle" in proposal_types
        assert "create_bundle" in proposal_types

    def test_proposals_have_unique_ids(self):
        """Test that each proposal has a unique ID"""
        from propose_keyword_expansions import generate_proposals

        themes = {
            "theme1": {
                "theme_id": "theme1",
                "label": "Theme 1",
                "terms": ["t1"],
                "accepted_count": 30,
                "rejected_count": 5,
            },
            "theme2": {
                "theme_id": "theme2",
                "label": "Theme 2",
                "terms": ["t2"],
                "accepted_count": 25,
                "rejected_count": 5,
            },
        }
        bundles = {}

        proposals = generate_proposals(themes, bundles, min_confidence=50)

        proposal_ids = [p["proposal_id"] for p in proposals]
        assert len(proposal_ids) == len(set(proposal_ids))


class TestSaveProposals:
    """Test suite for saving proposals to file."""

    def test_save_proposals_creates_file(self):
        """Test that save_proposals creates the expansions.json file"""
        from propose_keyword_expansions import (
            save_proposals,
            load_themes,
            load_keyword_bundles,
        )
        import tempfile
        import json

        proposals = [
            {
                "proposal_id": "test-uuid-1",
                "type": "create_bundle",
                "target_bundle_id": None,
                "new_bundle_label": "Test Bundle",
                "terms_to_add": ["term1", "term2"],
                "confidence_score": 85.0,
                "auto_activate": True,
                "status": "pending",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "reviewed_at": None,
                "reviewed_by": None,
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            # Override paths for testing
            import propose_keyword_expansions as pke

            original_base = pke.BASE_DIR
            pke.BASE_DIR = Path(tmpdir)
            pke.EXPANSIONS_PATH = (
                Path(tmpdir) / "data" / "theme_memory" / "expansions.json"
            )

            # Ensure directory exists
            pke.EXPANSIONS_PATH.parent.mkdir(parents=True, exist_ok=True)

            save_proposals(proposals)

            assert pke.EXPANSIONS_PATH.exists()

            with open(pke.EXPANSIONS_PATH, "r") as f:
                loaded = json.load(f)

            assert len(loaded["proposals"]) == 1
            assert loaded["proposals"][0]["proposal_id"] == "test-uuid-1"

            # Restore
            pke.BASE_DIR = original_base


class TestLoadThemes:
    """Test suite for loading themes."""

    def test_load_themes_from_file(self):
        """Test loading themes from themes.json"""
        from propose_keyword_expansions import load_themes
        import tempfile
        import json

        test_themes = {
            "inflation": {
                "theme_id": "inflation",
                "label": "Inflation",
                "terms": ["cpi", "ppi"],
                "accepted_count": 50,
                "rejected_count": 10,
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            themes_path = Path(tmpdir) / "themes.json"
            with open(themes_path, "w") as f:
                json.dump({"themes": test_themes}, f)

            import propose_keyword_expansions as pke

            original_path = pke.THEMES_PATH
            pke.THEMES_PATH = themes_path

            themes = load_themes()

            assert "inflation" in themes
            assert themes["inflation"]["label"] == "Inflation"

            pke.THEMES_PATH = original_path

    def test_load_themes_empty_file(self):
        """Test loading themes from empty/non-existent file"""
        from propose_keyword_expansions import load_themes
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            themes_path = Path(tmpdir) / "themes.json"

            import propose_keyword_expansions as pke

            original_path = pke.THEMES_PATH
            pke.THEMES_PATH = themes_path

            themes = load_themes()

            assert themes == {}

            pke.THEMES_PATH = original_path


class TestLoadKeywordBundles:
    """Test suite for loading keyword bundles."""

    def test_load_keyword_bundles_from_file(self):
        """Test loading keyword bundles from keyword_bundles.json"""
        from propose_keyword_expansions import load_keyword_bundles
        import tempfile
        import json

        test_bundles = {
            "macro": {
                "bundle_id": "macro",
                "label": "Macro",
                "keywords": ["cpi", "gdp"],
                "priority": 2,
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            bundles_path = Path(tmpdir) / "keyword_bundles.json"
            with open(bundles_path, "w") as f:
                json.dump({"bundles": test_bundles}, f)

            import propose_keyword_expansions as pke

            original_path = pke.BUNDLES_PATH
            pke.BUNDLES_PATH = bundles_path

            bundles = load_keyword_bundles()

            assert "macro" in bundles
            assert bundles["macro"]["label"] == "Macro"

            pke.BUNDLES_PATH = original_path


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
