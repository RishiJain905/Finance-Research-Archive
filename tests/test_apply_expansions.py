"""Tests for apply_keyword_expansions.py"""

import json
import tempfile
import os
import sys
from pathlib import Path
from datetime import datetime, timezone
from copy import deepcopy

# Add scripts to path
scripts_path = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(scripts_path))

import pytest


class TestApplyBundleAddition:
    """Test suite for applying bundle addition expansions."""

    def test_apply_addition_to_existing_bundle(self):
        """Test adding new terms to an existing bundle"""
        from apply_keyword_expansions import apply_bundle_addition

        expansion = {
            "proposal_id": "test-1",
            "type": "add_to_bundle",
            "target_bundle_id": "macro",
            "terms_to_add": ["cpi", "ppi"],
            "confidence_score": 75.0,
            "auto_activate": False,
            "status": "approved",
        }

        bundles = {
            "macro": {
                "bundle_id": "macro",
                "label": "Macro Catalysts",
                "keywords": ["gdp", "unemployment"],
                "priority": 2,
            }
        }

        updated_bundles = apply_bundle_addition(expansion, bundles)

        assert "cpi" in updated_bundles["macro"]["keywords"]
        assert "ppi" in updated_bundles["macro"]["keywords"]
        assert "gdp" in updated_bundles["macro"]["keywords"]  # original preserved
        assert "unemployment" in updated_bundles["macro"]["keywords"]

    def test_apply_addition_no_duplicates(self):
        """Test that adding existing terms doesn't create duplicates"""
        from apply_keyword_expansions import apply_bundle_addition

        expansion = {
            "proposal_id": "test-1",
            "type": "add_to_bundle",
            "target_bundle_id": "macro",
            "terms_to_add": ["cpi", "gdp"],  # gdp already exists
            "confidence_score": 75.0,
            "status": "approved",
        }

        bundles = {
            "macro": {
                "bundle_id": "macro",
                "label": "Macro",
                "keywords": ["gdp"],
                "priority": 2,
            }
        }

        updated_bundles = apply_bundle_addition(expansion, bundles)

        # Should have only 2 keywords, not 3 (no duplicate gdp)
        assert len(updated_bundles["macro"]["keywords"]) == 2
        assert updated_bundles["macro"]["keywords"].count("gdp") == 1

    def test_apply_addition_bundle_not_found(self):
        """Test adding to non-existent bundle raises error"""
        from apply_keyword_expansions import apply_bundle_addition

        expansion = {
            "proposal_id": "test-1",
            "type": "add_to_bundle",
            "target_bundle_id": "nonexistent",
            "terms_to_add": ["cpi"],
            "confidence_score": 75.0,
            "status": "approved",
        }

        bundles = {}

        with pytest.raises(ValueError, match="Bundle.*not found"):
            apply_bundle_addition(expansion, bundles)


class TestApplyNewBundle:
    """Test suite for creating new bundles from expansions."""

    def test_apply_create_new_bundle(self):
        """Test creating a new bundle from expansion"""
        from apply_keyword_expansions import apply_new_bundle

        expansion = {
            "proposal_id": "test-1",
            "type": "create_bundle",
            "new_bundle_label": "Repo Market",
            "terms_to_add": ["repo", "repurchase", "funding"],
            "confidence_score": 85.0,
            "auto_activate": True,
            "status": "approved",
        }

        bundles = {
            "existing": {
                "bundle_id": "existing",
                "label": "Existing",
                "keywords": ["other"],
                "priority": 1,
            }
        }

        updated_bundles = apply_new_bundle(expansion, bundles)

        # Find the new bundle (should have generated ID)
        new_bundle = None
        for bid, bundle in updated_bundles.items():
            if bundle["label"] == "Repo Market":
                new_bundle = bundle
                break

        assert new_bundle is not None
        assert "repo" in new_bundle["keywords"]
        assert "repurchase" in new_bundle["keywords"]
        assert "funding" in new_bundle["keywords"]
        assert new_bundle["priority"] == 2  # Default priority

    def test_apply_create_bundle_with_id(self):
        """Test creating bundle with explicit bundle_id"""
        from apply_keyword_expansions import apply_new_bundle

        expansion = {
            "proposal_id": "test-1",
            "type": "create_bundle",
            "target_bundle_id": "new_repo_bundle",  # explicit ID
            "new_bundle_label": "Repo Market",
            "terms_to_add": ["repo"],
            "confidence_score": 85.0,
            "status": "approved",
        }

        bundles = {}

        updated_bundles = apply_new_bundle(expansion, bundles)

        assert "new_repo_bundle" in updated_bundles
        assert updated_bundles["new_repo_bundle"]["label"] == "Repo Market"


class TestApplyPriorityIncrease:
    """Test suite for applying priority increases."""

    def test_apply_priority_increase(self):
        """Test increasing bundle priority"""
        from apply_keyword_expansions import apply_priority_increase

        expansion = {
            "proposal_id": "test-1",
            "type": "increase_priority",
            "target_bundle_id": "macro",
            "new_priority": 5,
            "confidence_score": 70.0,
            "status": "approved",
        }

        bundles = {
            "macro": {
                "bundle_id": "macro",
                "label": "Macro",
                "keywords": ["cpi"],
                "priority": 2,
            }
        }

        updated_bundles = apply_priority_increase(expansion, bundles)

        assert updated_bundles["macro"]["priority"] == 5

    def test_apply_priority_bundle_not_found(self):
        """Test priority increase on non-existent bundle"""
        from apply_keyword_expansions import apply_priority_increase

        expansion = {
            "proposal_id": "test-1",
            "type": "increase_priority",
            "target_bundle_id": "nonexistent",
            "new_priority": 5,
            "status": "approved",
        }

        bundles = {}

        with pytest.raises(ValueError, match="Bundle.*not found"):
            apply_priority_increase(expansion, bundles)


class TestApplyNegativeBundle:
    """Test suite for applying negative bundle expansions."""

    def test_apply_negative_bundle_addition(self):
        """Test adding terms to negative bundles"""
        from apply_keyword_expansions import apply_negative_bundle

        expansion = {
            "proposal_id": "test-1",
            "type": "add_to_negative_bundle",
            "target_bundle_id": "noise",
            "terms_to_add": ["casino", "gambling"],
            "confidence_score": 90.0,
            "status": "approved",
        }

        negative_bundles = {
            "noise": {
                "bundle_id": "noise",
                "label": "Noise/Off-Topic",
                "negative_keywords": ["spam"],
                "priority": 1,
            }
        }

        updated = apply_negative_bundle(expansion, negative_bundles)

        assert "casino" in updated["noise"]["negative_keywords"]
        assert "gambling" in updated["noise"]["negative_keywords"]

    def test_apply_negative_bundle_creates_if_missing(self):
        """Test creating negative bundle if it doesn't exist"""
        from apply_keyword_expansions import apply_negative_bundle

        expansion = {
            "proposal_id": "test-1",
            "type": "add_to_negative_bundle",
            "target_bundle_id": "new_noise",
            "terms_to_add": ["casino"],
            "confidence_score": 90.0,
            "status": "approved",
        }

        negative_bundles = {}

        updated = apply_negative_bundle(expansion, negative_bundles)

        assert "new_noise" in updated
        assert "casino" in updated["new_noise"]["negative_keywords"]


class TestAutoActivation:
    """Test suite for auto-activation logic."""

    def test_auto_activate_high_confidence(self):
        """Test that confidence >= 80 triggers auto-activation"""
        from apply_keyword_expansions import auto_activate_high_confidence

        expansions = [
            {
                "proposal_id": "high-conf",
                "type": "create_bundle",
                "confidence_score": 85.0,
                "status": "pending",
                "auto_activated": False,
            },
            {
                "proposal_id": "exact-threshold",
                "type": "add_to_bundle",
                "confidence_score": 80.0,
                "status": "pending",
                "auto_activated": False,
            },
        ]

        updated = auto_activate_high_confidence(expansions)

        assert updated[0]["status"] == "approved"
        assert updated[0]["auto_activated"] is True
        assert updated[1]["status"] == "approved"
        assert updated[1]["auto_activated"] is True

    def test_no_auto_activate_medium_confidence(self):
        """Test that confidence 50-79 does NOT auto-activate"""
        from apply_keyword_expansions import auto_activate_high_confidence

        expansions = [
            {
                "proposal_id": "medium-conf",
                "type": "create_bundle",
                "confidence_score": 75.0,
                "status": "pending",
                "auto_activated": False,
            },
        ]

        updated = auto_activate_high_confidence(expansions)

        assert updated[0]["status"] == "pending"
        assert updated[0]["auto_activated"] is False

    def test_reject_low_confidence(self):
        """Test that confidence < 50 gets rejected"""
        from apply_keyword_expansions import auto_activate_high_confidence

        expansions = [
            {
                "proposal_id": "low-conf",
                "type": "create_bundle",
                "confidence_score": 40.0,
                "status": "pending",
                "auto_activated": False,
            },
        ]

        updated = auto_activate_high_confidence(expansions)

        assert updated[0]["status"] == "rejected"
        assert updated[0]["auto_activated"] is False


class TestMarkExpansionApplied:
    """Test suite for marking expansions as applied."""

    def test_mark_expansion_applied(self):
        """Test marking an expansion as applied"""
        from apply_keyword_expansions import mark_expansion_applied

        expansion = {
            "proposal_id": "test-1",
            "type": "create_bundle",
            "status": "approved",
        }

        updated = mark_expansion_applied(expansion)

        assert updated["status"] == "applied"
        assert updated["applied_at"] is not None
        # Should be valid ISO format
        datetime.fromisoformat(updated["applied_at"].replace("Z", "+00:00"))


class TestMain:
    """Test suite for the main() function."""

    def test_main_processes_pending_approved(self):
        """Test that main processes approved expansions"""
        from apply_keyword_expansions import main

        # Create temporary files with test data
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Setup expansions.json with approved expansion
            expansions_path = tmpdir / "expansions.json"
            expansions_data = {
                "proposals": [
                    {
                        "proposal_id": "approved-1",
                        "type": "add_to_bundle",
                        "target_bundle_id": "macro",
                        "terms_to_add": ["cpi"],
                        "confidence_score": 85.0,
                        "status": "approved",
                        "auto_activated": False,
                    }
                ]
            }
            with open(expansions_path, "w") as f:
                json.dump(expansions_data, f)

            # Setup keyword_bundles.json
            bundles_path = tmpdir / "keyword_bundles.json"
            bundles_data = {
                "bundles": {
                    "macro": {
                        "bundle_id": "macro",
                        "label": "Macro",
                        "keywords": ["gdp"],
                        "priority": 2,
                    }
                }
            }
            with open(bundles_path, "w") as f:
                json.dump(bundles_data, f)

            # Setup negative_bundles.json
            neg_path = tmpdir / "negative_keyword_bundles.json"
            neg_data = {"negative_bundles": {}}
            with open(neg_path, "w") as f:
                json.dump(neg_data, f)

            # Override paths
            import apply_keyword_expansions as ae

            original_paths = {
                "EXPANSIONS_PATH": ae.EXPANSIONS_PATH,
                "BUNDLES_PATH": ae.BUNDLES_PATH,
                "NEG_BUNDLES_PATH": ae.NEG_BUNDLES_PATH,
            }

            ae.EXPANSIONS_PATH = expansions_path
            ae.BUNDLES_PATH = bundles_path
            ae.NEG_BUNDLES_PATH = neg_path

            try:
                # Run main
                results = ae.main()

                assert results["applied"] == 1
                assert results["errors"] == 0

                # Verify bundles were updated
                with open(bundles_path, "r") as f:
                    updated_bundles = json.load(f)

                assert "cpi" in updated_bundles["bundles"]["macro"]["keywords"]

                # Verify expansion was marked applied
                with open(expansions_path, "r") as f:
                    updated_expansions = json.load(f)

                approved_proposal = next(
                    p
                    for p in updated_expansions["proposals"]
                    if p["proposal_id"] == "approved-1"
                )
                assert approved_proposal["status"] == "applied"
            finally:
                # Restore paths
                ae.EXPANSIONS_PATH = original_paths["EXPANSIONS_PATH"]
                ae.BUNDLES_PATH = original_paths["BUNDLES_PATH"]
                ae.NEG_BUNDLES_PATH = original_paths["NEG_BUNDLES_PATH"]

    def test_main_skips_pending_not_approved(self):
        """Test that main skips pending (not yet approved) expansions"""
        from apply_keyword_expansions import main

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Setup with pending (not approved) expansion
            expansions_path = tmpdir / "expansions.json"
            expansions_data = {
                "proposals": [
                    {
                        "proposal_id": "pending-1",
                        "type": "create_bundle",
                        "new_bundle_label": "New Bundle",
                        "terms_to_add": ["term1"],
                        "confidence_score": 60.0,
                        "status": "pending",
                        "auto_activated": False,
                    }
                ]
            }
            with open(expansions_path, "w") as f:
                json.dump(expansions_data, f)

            bundles_path = tmpdir / "keyword_bundles.json"
            bundles_data = {"bundles": {}}
            with open(bundles_path, "w") as f:
                json.dump(bundles_data, f)

            neg_path = tmpdir / "negative_keyword_bundles.json"
            with open(neg_path, "w") as f:
                json.dump({"negative_bundles": {}}, f)

            import apply_keyword_expansions as ae

            original_paths = {
                "EXPANSIONS_PATH": ae.EXPANSIONS_PATH,
                "BUNDLES_PATH": ae.BUNDLES_PATH,
                "NEG_BUNDLES_PATH": ae.NEG_BUNDLES_PATH,
            }

            ae.EXPANSIONS_PATH = expansions_path
            ae.BUNDLES_PATH = bundles_path
            ae.NEG_BUNDLES_PATH = neg_path

            try:
                results = ae.main()

                assert results["applied"] == 0
                assert results["skipped"] == 1

                # Verify no changes to bundles
                with open(bundles_path, "r") as f:
                    updated_bundles = json.load(f)

                assert len(updated_bundles["bundles"]) == 0
            finally:
                ae.EXPANSIONS_PATH = original_paths["EXPANSIONS_PATH"]
                ae.BUNDLES_PATH = original_paths["BUNDLES_PATH"]
                ae.NEG_BUNDLES_PATH = original_paths["NEG_BUNDLES_PATH"]


class TestLoadFunctions:
    """Test suite for load functions."""

    def test_load_expansions(self):
        """Test loading expansions from file"""
        from apply_keyword_expansions import load_expansions
        import tempfile
        import json

        test_data = {
            "proposals": [
                {"proposal_id": "test-1", "status": "pending"},
            ]
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "expansions.json"
            with open(path, "w") as f:
                json.dump(test_data, f)

            import apply_keyword_expansions as ae

            original_path = ae.EXPANSIONS_PATH
            ae.EXPANSIONS_PATH = path

            try:
                proposals = load_expansions()
                assert len(proposals) == 1
                assert proposals[0]["proposal_id"] == "test-1"
            finally:
                ae.EXPANSIONS_PATH = original_path

    def test_load_expansions_empty(self):
        """Test loading from empty file"""
        from apply_keyword_expansions import load_expansions
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "expansions.json"
            path.write_text("{}")

            import apply_keyword_expansions as ae

            original_path = ae.EXPANSIONS_PATH
            ae.EXPANSIONS_PATH = path

            try:
                proposals = load_expansions()
                assert proposals == []
            finally:
                ae.EXPANSIONS_PATH = original_path

    def test_load_keyword_bundles(self):
        """Test loading keyword bundles"""
        from apply_keyword_expansions import load_keyword_bundles
        import tempfile
        import json

        test_data = {"bundles": {"macro": {"bundle_id": "macro", "keywords": ["cpi"]}}}

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "keyword_bundles.json"
            with open(path, "w") as f:
                json.dump(test_data, f)

            import apply_keyword_expansions as ae

            original_path = ae.BUNDLES_PATH
            ae.BUNDLES_PATH = path

            try:
                bundles = load_keyword_bundles()
                assert "macro" in bundles
            finally:
                ae.BUNDLES_PATH = original_path

    def test_load_negative_bundles(self):
        """Test loading negative bundles"""
        from apply_keyword_expansions import load_negative_bundles
        import tempfile
        import json

        test_data = {
            "negative_bundles": {
                "noise": {"bundle_id": "noise", "negative_keywords": ["spam"]}
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "negative_bundles.json"
            with open(path, "w") as f:
                json.dump(test_data, f)

            import apply_keyword_expansions as ae

            original_path = ae.NEG_BUNDLES_PATH
            ae.NEG_BUNDLES_PATH = path

            try:
                bundles = load_negative_bundles()
                assert "noise" in bundles
            finally:
                ae.NEG_BUNDLES_PATH = original_path


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
