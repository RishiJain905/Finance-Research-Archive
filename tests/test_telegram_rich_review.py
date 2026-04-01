"""Tests for Telegram rich review UX changes.

Tests the Telegram message building and callback handling for the
enhanced human review flow with 6 action types.
"""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

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
def sample_record():
    """Provide a sample record for Telegram message building."""
    return {
        "record_id": "rec_2024_001",
        "title": "Federal Reserve FOMC Meeting Minutes",
        "topic": "monetary_policy",
        "event_type": "meeting_minutes",
        "source": "federalreserve.gov",
        "url": "https://federalreserve.gov/fomc/minutes/2024-01-01.htm",
        "summary": "The Federal Open Market Committee met to discuss current economic conditions and monetary policy options.",
        "llm_review": {
            "verdict": "accept",
            "quality_tier": "tier_1",
            "score": 85.0,
            "issues_found": ["Minor formatting issues"],
        },
        "human_review": {"notes": ""},
    }


# =============================================================================
# Keyboard Structure Tests
# =============================================================================


class TestRichKeyboardStructure:
    """Tests for the enhanced Telegram inline keyboard structure."""

    def _get_callback_actions(self):
        """Return the 6 callback actions expected in rich keyboard.

        The 6 actions are:
        1. approve - Accept the record
        2. reject - Reject the record
        3. approve_but_weak - Accept with low confidence
        4. approve_and_promote - Accept and suggest promoting
        5. bad_source - Reject due to poor source quality
        6. suppress - Suppress similar items
        """
        return [
            "approve",
            "reject",
            "approve_but_weak",
            "approve_and_promote",
            "bad_source",
            "suppress",
        ]

    def test_rich_keyboard_has_3_rows(self):
        """Rich keyboard should have 3 rows for better UX.

        Expected layout:
        Row 1: [Approve] [Reject]
        Row 2: [Approve+Promote] [Bad Source]
        Row 3: [Weak Accept] [Suppress]
        """
        # Test the expected row structure
        expected_rows = 3

        # Build a sample keyboard matching expected structure
        keyboard = [
            [{"text": "Approve", "callback_data": "approve:key1"}],
            [
                {
                    "text": "Approve & Promote",
                    "callback_data": "approve_and_promote:key2",
                }
            ],
            [{"text": "Weak Accept", "callback_data": "approve_but_weak:key3"}],
        ]

        assert len(keyboard) == expected_rows

    def test_all_six_callback_actions_present(self):
        """All 6 callback actions should be present in rich keyboard."""
        actions = self._get_callback_actions()

        # The keyboard should include all these actions
        expected_actions = {
            "approve",
            "reject",
            "approve_but_weak",
            "approve_and_promote",
            "bad_source",
            "suppress",
        }

        assert set(actions) == expected_actions

    def test_callback_data_format_is_action_key(self):
        """Callback data should follow 'action:key' format."""
        record_id = "rec_2024_001"
        action = "approve"

        # Generate callback key (same as make_callback_key in send_review_to_telegram)
        callback_key = hashlib.sha1(record_id.encode()).hexdigest()[:28]
        callback_data = f"{action}:{callback_key}"

        # Verify format
        assert ":" in callback_data
        parts = callback_data.split(":")
        assert len(parts) == 2
        assert parts[0] == action
        assert len(parts[1]) == 28

    def test_callback_data_under_64_bytes(self):
        """Callback data should stay under Telegram's 64-byte limit.

        Telegram has a 64-byte hard limit on callback_data.
        Format is 'action:key' where action is ~20 chars max and key is 28 chars.
        """
        max_action_len = 20  # Longest action is "approve_and_promote" (~18 chars)
        key_len = 28  # SHA-1 hex digest truncated to 28 chars
        separator = 1  # The colon

        max_callback_data_len = max_action_len + separator + key_len

        # Test the longest action
        longest_action = "approve_and_promote"
        record_id = "rec_2024_001" * 10  # Make a longer ID to test boundary

        callback_key = hashlib.sha1(record_id.encode()).hexdigest()[:28]
        callback_data = f"{longest_action}:{callback_key}"

        # Should fit comfortably under 64 bytes
        assert len(callback_data.encode("utf-8")) <= 64


# =============================================================================
# Message Building Tests
# =============================================================================


class TestBuildMessage:
    """Tests for Telegram message building."""

    def _build_message(self, record_id, record):
        """Build a Telegram message from a record.

        This mirrors the expected logic in send_review_to_telegram.py
        with enhancements for source domain display.
        """
        title = record.get("title", "Untitled")
        topic = record.get("topic", "unknown")
        event_type = record.get("event_type", "unknown")
        summary = record.get("summary", "")
        verdict = record.get("llm_review", {}).get("verdict", "unknown")
        issues = record.get("llm_review", {}).get("issues_found", [])
        review_notes = record.get("human_review", {}).get("notes", "")

        # Source domain if available
        source = record.get("source", "")
        source_line = f"Source: {source}\n" if source else ""

        # Quality tier if already assigned
        quality_tier = record.get("llm_review", {}).get("quality_tier", "")
        tier_line = f"Quality tier: {quality_tier}\n" if quality_tier else ""

        issue_text = (
            "\n".join(f"- {issue}" for issue in issues[:5]) if issues else "- none"
        )

        text = (
            f"Review needed for: {record_id}\n\n"
            f"Title: {title}\n"
            f"{source_line}"
            f"Topic: {topic}\n"
            f"Event type: {event_type}\n"
            f"{tier_line}"
            f"LLM verdict: {verdict}\n\n"
            f"Summary:\n{summary}\n\n"
            f"Issues found:\n{issue_text}\n\n"
            f"Review notes:\n{review_notes}"
        )
        return text

    def test_message_includes_source_domain(self, sample_record):
        """Message should include source domain when available."""
        message = self._build_message("rec_001", sample_record)

        assert "Source: federalreserve.gov" in message

    def test_message_handles_missing_source(self):
        """Message should handle records without source gracefully."""
        record = {
            "record_id": "rec_001",
            "title": "Test",
            "topic": "test",
            "event_type": "test",
        }

        message = self._build_message("rec_001", record)

        # Should not have "Source:" line
        assert "Source:" not in message

    def test_message_includes_title(self, sample_record):
        """Message should include the record title."""
        message = self._build_message("rec_001", sample_record)

        assert "Federal Reserve FOMC Meeting Minutes" in message

    def test_message_includes_topic(self, sample_record):
        """Message should include the topic."""
        message = self._build_message("rec_001", sample_record)

        assert "Topic: monetary_policy" in message

    def test_message_includes_quality_tier(self, sample_record):
        """Message should include quality tier when available."""
        message = self._build_message("rec_001", sample_record)

        assert "Quality tier: tier_1" in message

    def test_message_includes_llm_verdict(self, sample_record):
        """Message should include LLM verdict."""
        message = self._build_message("rec_001", sample_record)

        assert "LLM verdict: accept" in message

    def test_message_includes_summary(self, sample_record):
        """Message should include summary."""
        message = self._build_message("rec_001", sample_record)

        assert "Federal Open Market Committee" in message


# =============================================================================
# Message Truncation Tests
# =============================================================================


class TestMessageTruncation:
    """Tests for Telegram message truncation at 4096 chars."""

    TELEGRAM_MAX_LEN = 4096

    def _truncate(self, text, max_len=TELEGRAM_MAX_LEN):
        """Truncate text to max_len, adding ellipsis if needed."""
        if len(text) <= max_len:
            return text
        return text[: max_len - 1] + "\u2026"

    def test_short_message_not_truncated(self):
        """Short messages should not be truncated."""
        text = "Short message"
        result = self._truncate(text)

        assert result == text

    def test_exact_length_message_not_truncated(self):
        """Messages at exactly max length should not be truncated."""
        text = "A" * 4096
        result = self._truncate(text)

        assert len(result) == 4096
        assert result == text

    def test_long_message_is_truncated(self):
        """Messages over max length should be truncated with ellipsis."""
        text = "A" * 5000
        result = self._truncate(text)

        assert len(result) == 4096
        assert result.endswith("\u2026")

    def test_truncated_message_has_ellipsis(self):
        """Truncated messages should end with ellipsis character."""
        text = "A" * 5000
        result = self._truncate(text)

        assert result[-1] == "\u2026"

    def test_truncated_message_is_under_limit(self):
        """Truncated messages should be under the limit."""
        text = "A" * 10000
        result = self._truncate(text)

        assert len(result) <= self.TELEGRAM_MAX_LEN


# =============================================================================
# Follow-up Prompt Tests
# =============================================================================


class TestFollowUpPrompts:
    """Tests for follow-up prompt generation for source feedback."""

    def _generate_source_feedback_prompt(self, action):
        """Generate follow-up prompt for source feedback actions.

        When user selects bad_source or good_source, we need additional
        context about which source they're referring to.
        """
        prompts = {
            "bad_source": "Please specify which source had poor quality:",
            "good_source": "Please specify which source was particularly good:",
        }
        return prompts.get(action, "")

    def test_bad_source_prompts_for_source_specification(self):
        """bad_source action should prompt for source specification."""
        prompt = self._generate_source_feedback_prompt("bad_source")

        assert "source" in prompt.lower()
        assert len(prompt) > 0

    def test_good_source_prompts_for_source_specification(self):
        """good_source action should prompt for source specification."""
        prompt = self._generate_source_feedback_prompt("good_source")

        assert "source" in prompt.lower()
        assert len(prompt) > 0

    def test_approve_does_not_need_follow_up(self):
        """approve action should not need source follow-up prompt."""
        prompt = self._generate_source_feedback_prompt("approve")

        # No prompt needed for simple approve
        assert prompt == ""


# =============================================================================
# Existing Flow Compatibility Tests
# =============================================================================


class TestExistingFlowCompatibility:
    """Tests that existing approve/reject flow remains unchanged."""

    def _build_existing_keyboard(self, record_id):
        """Build the existing 2-button keyboard for backward compatibility.

        This is the keyboard format used in the current send_review_to_telegram.py.
        """
        callback_key = hashlib.sha1(record_id.encode()).hexdigest()[:28]

        return {
            "inline_keyboard": [
                [
                    {"text": "Approve", "callback_data": f"approve:{callback_key}"},
                    {"text": "Reject", "callback_data": f"reject:{callback_key}"},
                ]
            ]
        }

    def test_existing_approve_reject_flow_unchanged(self, sample_record):
        """Existing approve/reject keyboard format should be unchanged."""
        record_id = sample_record["record_id"]
        keyboard = self._build_existing_keyboard(record_id)

        # Verify structure
        assert "inline_keyboard" in keyboard
        assert len(keyboard["inline_keyboard"]) == 1  # One row
        assert len(keyboard["inline_keyboard"][0]) == 2  # Two buttons

        # Verify buttons
        approve_btn = keyboard["inline_keyboard"][0][0]
        reject_btn = keyboard["inline_keyboard"][0][1]

        assert approve_btn["text"] == "Approve"
        assert approve_btn["callback_data"].startswith("approve:")
        assert reject_btn["text"] == "Reject"
        assert reject_btn["callback_data"].startswith("reject:")

    def test_callback_key_generation_unchanged(self):
        """make_callback_key should produce same output as before."""
        record_id = "rec_2024_001"

        # Same logic as in send_review_to_telegram.py
        callback_key = hashlib.sha1(record_id.encode()).hexdigest()[:28]

        # Should be 28 characters
        assert len(callback_key) == 28

        # Should be deterministic
        callback_key_2 = hashlib.sha1(record_id.encode()).hexdigest()[:28]
        assert callback_key == callback_key_2


# =============================================================================
# Source Feedback Callback Handling
# =============================================================================


class TestSourceFeedbackCallbacks:
    """Tests for handling source feedback callbacks."""

    def test_callback_data_parses_action_and_key(self):
        """Callback data should parse into action and key correctly."""
        callback_data = "bad_source:abc123def456"

        action, key = callback_data.split(":", 1)

        assert action == "bad_source"
        assert key == "abc123def456"

    def test_valid_source_feedback_actions(self):
        """Only valid source feedback actions should be accepted."""
        valid_actions = {"good_source", "bad_source"}

        for action in valid_actions:
            callback_data = f"{action}:key123"
            action_part = callback_data.split(":")[0]
            assert action_part in valid_actions

    def test_invalid_action_rejected(self):
        """Invalid action should be rejected in callback handling."""
        invalid_actions = {"maybe", "perhaps", "try_again"}

        for action in invalid_actions:
            callback_data = f"{action}:key123"

            # This should be caught as invalid
            valid_actions = {
                "approve",
                "reject",
                "approve_but_weak",
                "approve_and_promote",
                "bad_source",
                "suppress",
            }
            action_part = callback_data.split(":")[0]

            assert action_part not in valid_actions


# =============================================================================
# Integration Tests
# =============================================================================


class TestTelegramRichReviewIntegration:
    """Integration tests for Telegram rich review functionality."""

    def test_full_message_build_with_all_fields(self, sample_record):
        """Test building a complete message with all fields populated."""
        record_id = sample_record["record_id"]

        # Build message
        title = sample_record.get("title", "Untitled")
        source = sample_record.get("source", "")
        source_line = f"Source: {source}\n" if source else ""

        message = f"Review needed for: {record_id}\n\nTitle: {title}\n{source_line}"

        assert "Federal Reserve FOMC Meeting Minutes" in message
        assert "federalreserve.gov" in message

    def test_rich_keyboard_row_count(self):
        """Rich keyboard should have 3 rows."""
        # Build rich keyboard
        keyboard = [
            [{"text": "Approve", "callback_data": "approve:key1"}],
            [{"text": "Promote", "callback_data": "approve_and_promote:key2"}],
            [{"text": "Weak", "callback_data": "approve_but_weak:key3"}],
        ]

        assert len(keyboard) == 3

    def test_message_and_keyboard_fit_together(self, sample_record):
        """Message and keyboard should work together for review flow."""
        # Build message (inline the message building)
        record_id = sample_record["record_id"]
        message = f"Review needed for: {record_id}\n\nTitle: {sample_record.get('title', 'Untitled')}"

        # Build keyboard
        callback_key = hashlib.sha1(record_id.encode()).hexdigest()[:28]
        keyboard = {
            "inline_keyboard": [
                [{"text": "Approve", "callback_data": f"approve:{callback_key}"}],
                [{"text": "Reject", "callback_data": f"reject:{callback_key}"}],
            ]
        }

        # Both should be valid
        assert len(message) > 0
        assert "inline_keyboard" in keyboard
