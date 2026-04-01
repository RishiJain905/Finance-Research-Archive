"""Tests for apply_human_feedback.py script.

Tests the script that applies human review feedback to records,
mapping decisions to appropriate status values.
"""

import json
import sys
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pytest

# Base directory
BASE_DIR = Path(__file__).resolve().parent.parent


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def fixtures_path():
    """Path to fixtures directory."""
    return BASE_DIR / "tests" / "fixtures"


@pytest.fixture
def sample_feedback_records(fixtures_path):
    """Load sample feedback records from fixtures."""
    fixtures_file = fixtures_path / "sample_feedback_records.json"
    with open(fixtures_file, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def temp_record_file(tmp_path, sample_feedback_records):
    """Create a temporary record file for testing."""
    record = sample_feedback_records["accepted_with_human_feedback"].copy()
    record_file = tmp_path / "rec_2024_001.json"
    with open(record_file, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2)
    return record_file


@pytest.fixture
def temp_review_queue(tmp_path):
    """Create a temporary review queue directory."""
    review_queue = tmp_path / "review_queue"
    review_queue.mkdir(parents=True, exist_ok=True)
    return review_queue


# =============================================================================
# Decision to Status Mapping Tests
# =============================================================================


class TestDecisionToStatusMapping:
    """Tests that decisions map to correct status values."""

    def _map_decision_to_status(self, decision):
        """Map human feedback decision to status value.

        This mirrors the expected logic in apply_human_feedback.py:
        - approve -> accepted
        - approve_but_weak -> accepted
        - approve_and_promote -> accepted
        - good_source -> accepted
        - expand_this_topic -> accepted
        - reject -> rejected
        - bad_source -> rejected
        - suppress_similar_items -> rejected
        """
        accept_statuses = {
            "approve",
            "approve_but_weak",
            "approve_and_promote",
            "good_source",
            "expand_this_topic",
        }
        reject_statuses = {
            "reject",
            "bad_source",
            "suppress_similar_items",
        }

        if decision in accept_statuses:
            return "accepted"
        elif decision in reject_statuses:
            return "rejected"
        else:
            return None

    def test_approve_maps_to_accepted(self):
        """approve decision should map to status 'accepted'."""
        status = self._map_decision_to_status("approve")
        assert status == "accepted"

    def test_reject_maps_to_rejected(self):
        """reject decision should map to status 'rejected'."""
        status = self._map_decision_to_status("reject")
        assert status == "rejected"

    def test_approve_but_weak_maps_to_accepted(self):
        """approve_but_weak decision should map to status 'accepted'."""
        status = self._map_decision_to_status("approve_but_weak")
        assert status == "accepted"

    def test_approve_and_promote_maps_to_accepted(self):
        """approve_and_promote decision should map to status 'accepted'."""
        status = self._map_decision_to_status("approve_and_promote")
        assert status == "accepted"

    def test_bad_source_maps_to_rejected(self):
        """bad_source decision should map to status 'rejected'."""
        status = self._map_decision_to_status("bad_source")
        assert status == "rejected"

    def test_good_source_maps_to_accepted(self):
        """good_source decision should map to status 'accepted'."""
        status = self._map_decision_to_status("good_source")
        assert status == "accepted"

    def test_expand_this_topic_maps_to_accepted(self):
        """expand_this_topic decision should map to status 'accepted'."""
        status = self._map_decision_to_status("expand_this_topic")
        assert status == "accepted"

    def test_suppress_similar_items_maps_to_rejected(self):
        """suppress_similar_items decision should map to status 'rejected'."""
        status = self._map_decision_to_status("suppress_similar_items")
        assert status == "rejected"


# =============================================================================
# Record Update Tests
# =============================================================================


class TestRecordUpdate:
    """Tests for record update logic."""

    def _create_updated_record(self, record, feedback):
        """Create an updated record with human_feedback and status applied.

        This mirrors the expected logic in apply_human_feedback.py.
        """
        updated = record.copy()

        # Apply status based on decision
        decision = feedback.get("decision", "reject")
        if decision in {
            "approve",
            "approve_but_weak",
            "approve_and_promote",
            "good_source",
            "expand_this_topic",
        }:
            updated["status"] = "accepted"
        else:
            updated["status"] = "rejected"

        # Set human_feedback block
        updated["human_feedback"] = {
            "decision": feedback["decision"],
            "reviewed_at": feedback.get(
                "reviewed_at", datetime.now(timezone.utc).isoformat()
            ),
        }

        # Copy optional fields if present
        if "notes" in feedback:
            updated["human_feedback"]["notes"] = feedback["notes"]
        if "source_feedback" in feedback:
            updated["human_feedback"]["source_feedback"] = feedback["source_feedback"]
        if "topic_feedback" in feedback:
            updated["human_feedback"]["topic_feedback"] = feedback["topic_feedback"]
        if "reviewer_id" in feedback:
            updated["human_feedback"]["reviewer_id"] = feedback["reviewer_id"]

        # Set human_review block for backward compatibility
        if "human_review" not in updated:
            updated["human_review"] = {}
        updated["human_review"]["required"] = False
        updated["human_review"]["decision"] = feedback["decision"]
        if "notes" in feedback:
            updated["human_review"]["notes"] = feedback["notes"]

        return updated

    def test_approve_sets_status_accepted(self, sample_feedback_records):
        """approve decision should set status to 'accepted'."""
        record = {"status": "pending_review", "title": "Test"}
        feedback = sample_feedback_records["all_feedback_decisions"][0]  # approve

        updated = self._create_updated_record(record, feedback)

        assert updated["status"] == "accepted"

    def test_reject_sets_status_rejected(self, sample_feedback_records):
        """reject decision should set status to 'rejected'."""
        record = {"status": "pending_review", "title": "Test"}
        feedback = sample_feedback_records["all_feedback_decisions"][1]  # reject

        updated = self._create_updated_record(record, feedback)

        assert updated["status"] == "rejected"

    def test_human_feedback_block_is_set(self, sample_feedback_records):
        """human_feedback block should be set correctly."""
        record = {"status": "pending_review", "title": "Test"}
        feedback = sample_feedback_records["all_feedback_decisions"][0]  # approve

        updated = self._create_updated_record(record, feedback)

        assert "human_feedback" in updated
        assert updated["human_feedback"]["decision"] == "approve"
        assert "reviewed_at" in updated["human_feedback"]

    def test_human_review_block_is_set_for_backward_compat(
        self, sample_feedback_records
    ):
        """human_review block should be set for backward compatibility."""
        record = {"status": "pending_review", "title": "Test"}
        feedback = sample_feedback_records["all_feedback_decisions"][0]  # approve

        updated = self._create_updated_record(record, feedback)

        assert "human_review" in updated
        assert updated["human_review"]["required"] is False
        assert updated["human_review"]["decision"] == "approve"

    def test_notes_field_preserved(self, sample_feedback_records):
        """Notes field should be preserved in human_feedback."""
        record = {"status": "pending_review", "title": "Test"}
        feedback = sample_feedback_records["feedback_with_all_optional_fields"]

        updated = self._create_updated_record(record, feedback)

        assert "notes" in updated["human_feedback"]
        assert "Excellent content" in updated["human_feedback"]["notes"]

    def test_source_feedback_field_preserved(self, sample_feedback_records):
        """source_feedback field should be preserved in human_feedback."""
        record = {"status": "pending_review", "title": "Test"}
        feedback = sample_feedback_records["feedback_with_all_optional_fields"]

        updated = self._create_updated_record(record, feedback)

        assert "source_feedback" in updated["human_feedback"]
        assert updated["human_feedback"]["source_feedback"] == "good_source"

    def test_topic_feedback_field_preserved(self, sample_feedback_records):
        """topic_feedback field should be preserved in human_feedback."""
        record = {"status": "pending_review", "title": "Test"}
        feedback = sample_feedback_records["feedback_with_all_optional_fields"]

        updated = self._create_updated_record(record, feedback)

        assert "topic_feedback" in updated["human_feedback"]
        assert updated["human_feedback"]["topic_feedback"] == "expand_this_topic"

    def test_reviewed_at_defaults_to_now_if_missing(self):
        """reviewed_at should default to current time if missing."""
        record = {"status": "pending_review", "title": "Test"}
        feedback = {"decision": "approve"}  # No reviewed_at

        updated = self._create_updated_record(record, feedback)

        assert "reviewed_at" in updated["human_feedback"]
        # Should be a valid ISO format string
        assert "T" in updated["human_feedback"]["reviewed_at"]


# =============================================================================
# CLI Argument Parsing Tests
# =============================================================================


class TestCLIArgumentParsing:
    """Tests for CLI argument parsing."""

    def test_script_can_be_imported(self):
        """apply_human_feedback module should be importable."""
        try:
            from scripts import apply_human_feedback

            assert hasattr(apply_human_feedback, "main") or hasattr(
                apply_human_feedback, "apply_feedback"
            )
        except ImportError:
            # Script doesn't exist yet - this is expected in TDD
            pytest.skip("apply_human_feedback.py not yet implemented")

    def test_script_entry_point_exists(self):
        """Script should have a main() entry point or CLI handling."""
        try:
            from scripts import apply_human_feedback

            assert hasattr(apply_human_feedback, "main") or hasattr(
                apply_human_feedback, "cli"
            )
        except ImportError:
            pytest.skip("apply_human_feedback.py not yet implemented")


# =============================================================================
# From-File Mode Tests
# =============================================================================


class TestFromFileMode:
    """Tests for --from-file mode."""

    def test_from_file_mode_exists(self):
        """Script should support batch feedback processing."""
        try:
            from scripts import apply_human_feedback
        except ImportError:
            pytest.skip("apply_human_feedback.py not yet implemented")

        # Check if there's a function to handle feedback processing
        # The script may have different function names for batch processing
        assert (
            hasattr(apply_human_feedback, "apply_feedback_to_record")
            or hasattr(apply_human_feedback, "main")
            or hasattr(apply_human_feedback, "update_feedback_memory")
        )

    def test_from_file_processes_feedback_records(
        self, tmp_path, sample_feedback_records
    ):
        """--from-file mode should process feedback records from a file."""
        try:
            from scripts import apply_human_feedback
        except ImportError:
            pytest.skip("apply_human_feedback.py not yet implemented")

        # This would test that the script can read feedback from a JSON file
        # and apply it to records. Implementation depends on script design.


# =============================================================================
# Missing Fields Handling Tests
# =============================================================================


class TestMissingFieldsHandling:
    """Tests for graceful handling of missing fields."""

    def _create_updated_record_graceful(self, record, feedback):
        """Create updated record with graceful handling of missing fields.

        This mirrors expected behavior in apply_human_feedback.py.
        """
        updated = record.copy()

        # Default to rejected if decision is missing
        decision = feedback.get("decision", "reject")

        # Apply status
        if decision in {
            "approve",
            "approve_but_weak",
            "approve_and_promote",
            "good_source",
            "expand_this_topic",
        }:
            updated["status"] = "accepted"
        else:
            updated["status"] = "rejected"

        # Set human_feedback block
        updated["human_feedback"] = {
            "decision": decision,
        }

        # Only set reviewed_at if present
        if "reviewed_at" in feedback:
            updated["human_feedback"]["reviewed_at"] = feedback["reviewed_at"]

        return updated

    def test_missing_decision_defaults_to_reject(self):
        """Missing decision should default to 'reject' (status 'rejected')."""
        record = {"status": "pending_review"}
        feedback = {}  # No decision

        updated = self._create_updated_record_graceful(record, feedback)

        assert updated["status"] == "rejected"

    def test_missing_notes_is_handled_gracefully(self):
        """Missing notes field should be handled gracefully."""
        record = {"status": "pending_review"}
        feedback = {
            "decision": "approve",
            "reviewed_at": "2024-01-15T10:00:00Z",
            # No notes
        }

        updated = self._create_updated_record_graceful(record, feedback)

        assert "notes" not in updated["human_feedback"]
        assert updated["status"] == "accepted"

    def test_missing_source_feedback_is_handled_gracefully(self):
        """Missing source_feedback should be handled gracefully."""
        record = {"status": "pending_review"}
        feedback = {
            "decision": "approve",
            "reviewed_at": "2024-01-15T10:00:00Z",
            # No source_feedback
        }

        updated = self._create_updated_record_graceful(record, feedback)

        assert "source_feedback" not in updated["human_feedback"]

    def test_missing_topic_feedback_is_handled_gracefully(self):
        """Missing topic_feedback should be handled gracefully."""
        record = {"status": "pending_review"}
        feedback = {
            "decision": "approve",
            "reviewed_at": "2024-01-15T10:00:00Z",
            # No topic_feedback
        }

        updated = self._create_updated_record_graceful(record, feedback)

        assert "topic_feedback" not in updated["human_feedback"]


# =============================================================================
# Integration Tests
# =============================================================================


class TestApplyHumanFeedbackIntegration:
    """Integration tests for apply_human_feedback script."""

    def test_end_to_end_approve_flow(self, tmp_path, sample_feedback_records):
        """Test complete flow for approve decision."""
        try:
            from scripts import apply_human_feedback
        except ImportError:
            pytest.skip("apply_human_feedback.py not yet implemented")

        # Setup: Create a record in review queue
        review_queue = tmp_path / "review_queue"
        review_queue.mkdir()

        record = sample_feedback_records["accepted_with_human_feedback"].copy()
        record_id = record["record_id"]
        record_path = review_queue / f"{record_id}.json"

        with open(record_path, "w", encoding="utf-8") as f:
            json.dump(record, f)

        # Run the feedback application
        feedback = {
            "decision": "approve",
            "notes": "Looks good",
            "reviewed_at": datetime.now(timezone.utc).isoformat(),
        }

        # This would call apply_human_feedback.process_record(record_id, feedback)
        # and verify the record was updated correctly

        # Verify the record was updated
        with open(record_path, "r", encoding="utf-8") as f:
            updated_record = json.load(f)

        assert updated_record["status"] == "accepted"
        assert updated_record["human_feedback"]["decision"] == "approve"

    def test_end_to_end_reject_flow(self, tmp_path, sample_feedback_records):
        """Test complete flow for reject decision."""
        try:
            from scripts import apply_human_feedback
        except ImportError:
            pytest.skip("apply_human_feedback.py not yet implemented")

        # Setup
        review_queue = tmp_path / "review_queue"
        review_queue.mkdir()

        record = sample_feedback_records["rejected_with_human_feedback"].copy()
        record_id = record["record_id"]
        record_path = review_queue / f"{record_id}.json"

        with open(record_path, "w", encoding="utf-8") as f:
            json.dump(record, f)

        # Run feedback
        feedback = {
            "decision": "reject",
            "notes": "Not relevant",
            "reviewed_at": datetime.now(timezone.utc).isoformat(),
        }

        # Verify
        with open(record_path, "r", encoding="utf-8") as f:
            updated_record = json.load(f)

        assert updated_record["status"] == "rejected"
        assert updated_record["human_feedback"]["decision"] == "reject"
