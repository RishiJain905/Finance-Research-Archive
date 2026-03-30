"""Tests for memory_persistence module.

Tests atomic read/write operations for domain, path, and source memory files.
Uses temporary directories to avoid polluting the actual data directory.
"""

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

# Import the module
from scripts import memory_persistence


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def temp_memory_dir(tmp_path):
    """Create a temporary memory directory for testing."""
    memory_dir = tmp_path / "source_memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    return memory_dir


@pytest.fixture
def temp_domain_memory_file(temp_memory_dir):
    """Path to temporary domain memory file."""
    return temp_memory_dir / "domain_memory.json"


@pytest.fixture
def temp_path_memory_file(temp_memory_dir):
    """Path to temporary path memory file."""
    return temp_memory_dir / "path_memory.json"


@pytest.fixture
def temp_source_memory_file(temp_memory_dir):
    """Path to temporary source memory file."""
    return temp_memory_dir / "source_memory.json"


@pytest.fixture
def mock_memory_paths(temp_memory_dir):
    """Mock the memory file paths to use temp directory."""
    domain_path = temp_memory_dir / "domain_memory.json"
    path_path = temp_memory_dir / "path_memory.json"
    source_path = temp_memory_dir / "source_memory.json"

    with (
        patch.object(memory_persistence, "DOMAIN_MEMORY_PATH", domain_path),
        patch.object(memory_persistence, "PATH_MEMORY_PATH", path_path),
        patch.object(memory_persistence, "SOURCE_MEMORY_PATH", source_path),
    ):
        yield {
            "domain": domain_path,
            "path": path_path,
            "source": source_path,
        }


@pytest.fixture
def sample_domain_memory():
    """Provide a sample domain memory record."""
    return {
        "domain": "federalreserve.gov",
        "trust_score": 85.0,
        "baseline_trust": 100.0,
        "total_candidates": 50,
        "accepted_count": 25,
        "accepted_human_count": 10,
        "accepted_auto_count": 15,
        "review_count": 10,
        "review_human_count": 5,
        "rejected_count": 8,
        "rejected_human_count": 3,
        "rejected_auto_count": 5,
        "filtered_out_count": 7,
        "yield_score": 0.5,
        "noise_score": 0.3,
        "first_seen": datetime.now(timezone.utc).isoformat(),
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }


@pytest.fixture
def sample_path_memory():
    """Provide a sample path memory record."""
    return {
        "domain": "brookings.edu",
        "path_pattern": "/events/",
        "trust_score": 72.0,
        "baseline_trust": 50.0,
        "total_candidates": 25,
        "accepted_count": 12,
        "accepted_human_count": 5,
        "review_count": 5,
        "review_human_count": 3,
        "rejected_count": 4,
        "rejected_human_count": 2,
        "filtered_out_count": 4,
        "yield_score": 0.48,
        "noise_score": 0.32,
        "first_seen": datetime.now(timezone.utc).isoformat(),
        "last_seen": datetime.now(timezone.utc).isoformat(),
    }


@pytest.fixture
def sample_source_memory():
    """Provide a sample source memory record."""
    return {
        "source_id": "federalreserve_fomc",
        "source_type": "trusted_source_monitor",
        "source_domain": "federalreserve.gov",
        "trust_score": 95.0,
        "yield_score": 0.6,
        "noise_score": 0.2,
        "total_candidates": 100,
        "accepted_count": 60,
        "accepted_human_count": 30,
        "review_count": 15,
        "review_human_count": 10,
        "rejected_count": 12,
        "rejected_human_count": 5,
        "filtered_out_count": 13,
        "first_seen": datetime.now(timezone.utc).isoformat(),
        "last_seen": datetime.now(timezone.utc).isoformat(),
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# TestDomainMemoryOperations
# =============================================================================


class TestDomainMemoryOperations:
    """Tests for domain memory CRUD operations."""

    def test_get_domain_memory_returns_none_for_non_existent(
        self, mock_memory_paths, temp_domain_memory_file
    ):
        """get_domain_memory returns None for non-existent domain."""
        # Create empty file
        temp_domain_memory_file.write_text("{}", encoding="utf-8")

        result = memory_persistence.get_domain_memory("nonexistent.com")

        assert result is None

    def test_get_domain_memory_returns_existing(
        self, mock_memory_paths, temp_domain_memory_file, sample_domain_memory
    ):
        """get_domain_memory returns existing memory record."""
        # Create file with data
        data = {"federalreserve.gov": sample_domain_memory}
        temp_domain_memory_file.write_text(json.dumps(data), encoding="utf-8")

        result = memory_persistence.get_domain_memory("federalreserve.gov")

        assert result is not None
        assert result["domain"] == "federalreserve.gov"
        assert result["trust_score"] == 85.0

    def test_get_domain_memory_case_insensitive(
        self, mock_memory_paths, temp_domain_memory_file, sample_domain_memory
    ):
        """get_domain_memory is case-insensitive."""
        data = {"federalreserve.gov": sample_domain_memory}
        temp_domain_memory_file.write_text(json.dumps(data), encoding="utf-8")

        # Different case variations
        result1 = memory_persistence.get_domain_memory("FederalReserve.gov")
        result2 = memory_persistence.get_domain_memory("FEDERALRESERVE.GOV")

        assert result1 is not None
        assert result2 is not None
        assert result1["domain"] == "federalreserve.gov"

    def test_save_domain_memory_persists_correctly(
        self, mock_memory_paths, temp_domain_memory_file, sample_domain_memory
    ):
        """save_domain_memory persists data correctly."""
        memory_persistence.save_domain_memory(
            "federalreserve.gov", sample_domain_memory.copy()
        )

        # Verify file contents
        content = json.loads(temp_domain_memory_file.read_text(encoding="utf-8"))
        assert "federalreserve.gov" in content
        assert content["federalreserve.gov"]["trust_score"] == 85.0

    def test_save_domain_memory_updates_last_seen(
        self, mock_memory_paths, temp_domain_memory_file, sample_domain_memory
    ):
        """save_domain_memory updates last_seen timestamp."""
        original_last_seen = sample_domain_memory["last_seen"]

        memory_persistence.save_domain_memory(
            "federalreserve.gov", sample_domain_memory.copy()
        )

        content = json.loads(temp_domain_memory_file.read_text(encoding="utf-8"))
        new_last_seen = content["federalreserve.gov"]["last_seen"]

        # last_seen should be updated (different from original)
        assert new_last_seen != original_last_seen

    def test_initialize_domain_memory_creates_proper_structure(self, mock_memory_paths):
        """initialize_domain_memory creates correct structure with baseline_trust."""
        result = memory_persistence.initialize_domain_memory(
            "newdomain.com", baseline_trust=50.0, trust_score=50.0
        )

        # Verify structure
        assert result["domain"] == "newdomain.com"
        assert result["baseline_trust"] == 50.0
        assert result["trust_score"] == 50.0
        assert result["total_candidates"] == 0
        assert result["accepted_count"] == 0
        assert result["review_count"] == 0
        assert result["rejected_count"] == 0
        assert result["filtered_out_count"] == 0
        assert result["yield_score"] == 0.0
        assert result["noise_score"] == 0.0
        assert "first_seen" in result
        assert "last_seen" in result

    def test_initialize_domain_memory_default_baseline(self, mock_memory_paths):
        """initialize_domain_memory has correct default baseline_trust."""
        result = memory_persistence.initialize_domain_memory("newdomain.com")

        assert result["baseline_trust"] == 10.0
        assert result["trust_score"] == 10.0

    def test_get_all_domain_memory_returns_all_records(
        self, mock_memory_paths, temp_domain_memory_file, sample_domain_memory
    ):
        """get_all_domain_memory returns all records."""
        data = {
            "federalreserve.gov": sample_domain_memory,
            "brookings.edu": {**sample_domain_memory, "domain": "brookings.edu"},
        }
        temp_domain_memory_file.write_text(json.dumps(data), encoding="utf-8")

        result = memory_persistence.get_all_domain_memory()

        assert len(result) == 2
        assert "federalreserve.gov" in result
        assert "brookings.edu" in result


# =============================================================================
# TestPathMemoryOperations
# =============================================================================


class TestPathMemoryOperations:
    """Tests for path memory CRUD operations."""

    def test_get_path_memory_returns_none_for_non_existent(
        self, mock_memory_paths, temp_path_memory_file
    ):
        """get_path_memory returns None for non-existent path."""
        temp_path_memory_file.write_text("{}", encoding="utf-8")

        result = memory_persistence.get_path_memory("brookings.edu", "/events/")

        assert result is None

    def test_get_path_memory_returns_existing(
        self, mock_memory_paths, temp_path_memory_file, sample_path_memory
    ):
        """get_path_memory returns existing path memory."""
        data = {"brookings.edu::/events/": sample_path_memory}
        temp_path_memory_file.write_text(json.dumps(data), encoding="utf-8")

        result = memory_persistence.get_path_memory("brookings.edu", "/events/")

        assert result is not None
        assert result["path_pattern"] == "/events/"
        assert result["trust_score"] == 72.0

    def test_save_path_memory_persists_correctly(
        self, mock_memory_paths, temp_path_memory_file, sample_path_memory
    ):
        """save_path_memory persists data correctly."""
        memory_persistence.save_path_memory(
            "brookings.edu", "/events/", sample_path_memory.copy()
        )

        content = json.loads(temp_path_memory_file.read_text(encoding="utf-8"))
        key = "brookings.edu::/events/"
        assert key in content
        assert content[key]["trust_score"] == 72.0

    def test_initialize_path_memory_creates_proper_structure(self, mock_memory_paths):
        """initialize_path_memory creates correct structure."""
        result = memory_persistence.initialize_path_memory(
            "brookings.edu", "/events/", baseline_trust=50.0
        )

        assert result["domain"] == "brookings.edu"
        assert result["path_pattern"] == "/events/"
        assert result["baseline_trust"] == 50.0
        assert result["trust_score"] == 50.0
        assert result["total_candidates"] == 0
        assert result["accepted_count"] == 0
        assert result["filtered_out_count"] == 0
        assert result["yield_score"] == 0.0
        assert result["noise_score"] == 0.0

    def test_get_path_memory_for_domain_returns_all_paths(
        self, mock_memory_paths, temp_path_memory_file, sample_path_memory
    ):
        """get_path_memory_for_domain returns all paths for a domain."""
        data = {
            "brookings.edu::/events/": sample_path_memory,
            "brookings.edu::/press/": {
                **sample_path_memory,
                "path_pattern": "/press/",
            },
            "other.com::/events/": {**sample_path_memory, "domain": "other.com"},
        }
        temp_path_memory_file.write_text(json.dumps(data), encoding="utf-8")

        result = memory_persistence.get_path_memory_for_domain("brookings.edu")

        assert len(result) == 2
        path_patterns = [r["path_pattern"] for r in result]
        assert "/events/" in path_patterns
        assert "/press/" in path_patterns

    def test_get_path_memory_for_domain_case_insensitive(
        self, mock_memory_paths, temp_path_memory_file, sample_path_memory
    ):
        """get_path_memory_for_domain is case-insensitive."""
        data = {"brookings.edu::/events/": sample_path_memory}
        temp_path_memory_file.write_text(json.dumps(data), encoding="utf-8")

        result = memory_persistence.get_path_memory_for_domain("BROOKINGS.EDU")

        assert len(result) == 1

    def test_path_memory_key_format(self):
        """_path_memory_key creates correct format."""
        key = memory_persistence._path_memory_key("Brookings.edu", "/events/")
        assert key == "brookings.edu::/events/"

    def test_save_path_memory_updates_last_seen(
        self, mock_memory_paths, temp_path_memory_file, sample_path_memory
    ):
        """save_path_memory updates last_seen timestamp."""
        original_last_seen = sample_path_memory["last_seen"]

        memory_persistence.save_path_memory(
            "brookings.edu", "/events/", sample_path_memory.copy()
        )

        content = json.loads(temp_path_memory_file.read_text(encoding="utf-8"))
        new_last_seen = content["brookings.edu::/events/"]["last_seen"]

        assert new_last_seen != original_last_seen


# =============================================================================
# TestSourceMemoryOperations
# =============================================================================


class TestSourceMemoryOperations:
    """Tests for source memory CRUD operations."""

    def test_get_source_memory_returns_none_for_non_existent(
        self, mock_memory_paths, temp_source_memory_file
    ):
        """get_source_memory returns None for non-existent source."""
        temp_source_memory_file.write_text("{}", encoding="utf-8")

        result = memory_persistence.get_source_memory("nonexistent_source")

        assert result is None

    def test_get_source_memory_returns_existing(
        self, mock_memory_paths, temp_source_memory_file, sample_source_memory
    ):
        """get_source_memory returns existing source memory."""
        data = {"federalreserve_fomc": sample_source_memory}
        temp_source_memory_file.write_text(json.dumps(data), encoding="utf-8")

        result = memory_persistence.get_source_memory("federalreserve_fomc")

        assert result is not None
        assert result["source_id"] == "federalreserve_fomc"
        assert result["source_type"] == "trusted_source_monitor"
        assert result["trust_score"] == 95.0

    def test_save_source_memory_persists_correctly(
        self, mock_memory_paths, temp_source_memory_file, sample_source_memory
    ):
        """save_source_memory persists data correctly."""
        memory_persistence.save_source_memory(
            "federalreserve_fomc", sample_source_memory.copy()
        )

        content = json.loads(temp_source_memory_file.read_text(encoding="utf-8"))
        assert "federalreserve_fomc" in content
        assert content["federalreserve_fomc"]["source_type"] == "trusted_source_monitor"

    def test_save_source_memory_updates_last_updated(
        self, mock_memory_paths, temp_source_memory_file, sample_source_memory
    ):
        """save_source_memory updates last_updated timestamp."""
        original_updated = sample_source_memory["last_updated"]

        memory_persistence.save_source_memory(
            "federalreserve_fomc", sample_source_memory.copy()
        )

        content = json.loads(temp_source_memory_file.read_text(encoding="utf-8"))
        new_updated = content["federalreserve_fomc"]["last_updated"]

        assert new_updated != original_updated

    def test_initialize_source_memory_creates_proper_structure(self, mock_memory_paths):
        """initialize_source_memory creates correct structure with source_type."""
        result = memory_persistence.initialize_source_memory(
            source_id="test_source",
            source_type="keyword_discovery",
            source_domain="example.com",
            baseline_trust=50.0,
        )

        assert result["source_id"] == "test_source"
        assert result["source_type"] == "keyword_discovery"
        assert result["source_domain"] == "example.com"
        assert result["trust_score"] == 50.0
        assert result["yield_score"] == 0.0
        assert result["noise_score"] == 0.0
        assert result["total_candidates"] == 0
        assert "first_seen" in result
        assert "last_seen" in result
        assert "last_updated" in result

    def test_initialize_source_memory_source_type_required(self, mock_memory_paths):
        """initialize_source_memory requires source_type."""
        result = memory_persistence.initialize_source_memory("test_source")

        assert result["source_type"] == "manual"  # default

    def test_initialize_source_memory_domain_lowercased(self, mock_memory_paths):
        """initialize_source_memory lowercases domain."""
        result = memory_persistence.initialize_source_memory(
            "test_source", source_domain="EXAMPLE.COM"
        )

        assert result["source_domain"] == "example.com"

    def test_get_all_source_memory_returns_all_records(
        self, mock_memory_paths, temp_source_memory_file, sample_source_memory
    ):
        """get_all_source_memory returns all records."""
        data = {
            "federalreserve_fomc": sample_source_memory,
            "brookings_economy": {
                **sample_source_memory,
                "source_id": "brookings_economy",
            },
        }
        temp_source_memory_file.write_text(json.dumps(data), encoding="utf-8")

        result = memory_persistence.get_all_source_memory()

        assert len(result) == 2
        assert "federalreserve_fomc" in result
        assert "brookings_economy" in result


# =============================================================================
# TestDefaultConfigLoading
# =============================================================================


class TestDefaultConfigLoading:
    """Tests for default configuration (from _default_memory_config function)."""

    def test_default_config_returns_valid_structure(self):
        """_default_memory_config returns valid config structure."""
        # Get the default config directly from the function
        default_config = memory_persistence._default_memory_config()

        assert isinstance(default_config, dict)
        assert "cold_start" in default_config
        assert "weights" in default_config
        assert "decay" in default_config
        assert "logging" in default_config

    def test_cold_start_has_required_fields(self):
        """cold_start section has required fields in default config."""
        default_config = memory_persistence._default_memory_config()
        cold_start = default_config.get("cold_start", {})

        assert "min_samples_for_learning" in cold_start
        assert "full_learning_threshold" in cold_start

    def test_min_samples_for_learning_default(self):
        """min_samples_for_learning defaults to 10."""
        default_config = memory_persistence._default_memory_config()
        min_samples = default_config.get("cold_start", {}).get(
            "min_samples_for_learning"
        )
        assert min_samples == 10, f"Expected 10, got {min_samples}"

    def test_full_learning_threshold_default(self):
        """full_learning_threshold defaults to 25."""
        default_config = memory_persistence._default_memory_config()
        threshold = default_config.get("cold_start", {}).get("full_learning_threshold")
        assert threshold == 25, f"Expected 25, got {threshold}"

    def test_weights_has_required_fields(self):
        """weights section has required fields in default config."""
        default_config = memory_persistence._default_memory_config()
        weights = default_config.get("weights", {})

        assert "accepted_weight" in weights
        assert "rejected_weight" in weights
        assert "filtered_weight" in weights
        assert "human_multiplier" in weights

    def test_human_multiplier_is_2_0(self):
        """human_multiplier defaults to 2.0."""
        default_config = memory_persistence._default_memory_config()
        weights = default_config.get("weights", {})

        assert weights.get("human_multiplier") == 2.0

    def test_decay_enabled_defaults_to_false(self):
        """decay.enabled defaults to false."""
        default_config = memory_persistence._default_memory_config()
        decay = default_config.get("decay", {})

        assert decay.get("enabled") is False

    def test_logging_log_updates_defaults_to_true(self):
        """logging.log_updates defaults to true."""
        default_config = memory_persistence._default_memory_config()
        logging_config = default_config.get("logging", {})

        assert logging_config.get("log_updates") is True

    def test_default_config_returned_if_file_missing(self, tmp_path):
        """Default config is returned if config file is missing."""
        # Mock config path to non-existent file
        fake_path = tmp_path / "nonexistent.json"

        with patch.object(memory_persistence, "MEMORY_CONFIG_PATH", fake_path):
            config = memory_persistence.load_memory_config()

        # Should return default config
        assert config["cold_start"]["min_samples_for_learning"] == 10
        assert config["cold_start"]["full_learning_threshold"] == 25


# =============================================================================
# TestFileOperations
# =============================================================================


class TestFileOperations:
    """Tests for file initialization operations."""

    def test_initialize_memory_files_creates_files(
        self, mock_memory_paths, temp_memory_dir
    ):
        """initialize_memory_files creates files if missing."""
        # Ensure no files exist
        for f in temp_memory_dir.glob("*.json"):
            f.unlink()

        memory_persistence.initialize_memory_files()

        # All files should now exist
        domain_file = temp_memory_dir / "domain_memory.json"
        path_file = temp_memory_dir / "path_memory.json"
        source_file = temp_memory_dir / "source_memory.json"

        assert domain_file.exists()
        assert path_file.exists()
        assert source_file.exists()

    def test_initialize_memory_files_creates_in_correct_directory(
        self, mock_memory_paths, tmp_path
    ):
        """initialize_memory_files creates in correct location."""
        # Create a new temp directory and mock paths to it
        new_dir = tmp_path / "new_memory_dir"
        new_dir.mkdir()

        domain_file = new_dir / "domain_memory.json"
        path_file = new_dir / "path_memory.json"
        source_file = new_dir / "source_memory.json"

        with (
            patch.object(memory_persistence, "MEMORY_DIR", new_dir),
            patch.object(memory_persistence, "DOMAIN_MEMORY_PATH", domain_file),
            patch.object(memory_persistence, "PATH_MEMORY_PATH", path_file),
            patch.object(memory_persistence, "SOURCE_MEMORY_PATH", source_file),
        ):
            memory_persistence.initialize_memory_files()

        assert new_dir.exists()
        assert domain_file.exists()
        assert path_file.exists()
        assert source_file.exists()

    def test_initialize_memory_files_preserves_existing_data(
        self, mock_memory_paths, temp_memory_dir
    ):
        """initialize_memory_files does not overwrite existing data."""
        # Create files with data
        domain_file = temp_memory_dir / "domain_memory.json"
        domain_file.write_text('{"existing": true}', encoding="utf-8")

        memory_persistence.initialize_memory_files()

        content = json.loads(domain_file.read_text(encoding="utf-8"))
        assert content == {"existing": True}


# =============================================================================
# TestAtomicOperations
# =============================================================================


class TestAtomicOperations:
    """Tests for atomic read/write behavior."""

    def test_load_json_atomic_returns_empty_dict_for_missing_file(
        self, mock_memory_paths, temp_memory_dir
    ):
        """_load_json_atomic returns empty dict for non-existent file."""
        non_existent = temp_memory_dir / "nonexistent.json"

        result = memory_persistence._load_json_atomic(non_existent)

        assert result == {}

    def test_load_json_atomic_handles_invalid_json(
        self, mock_memory_paths, temp_memory_dir
    ):
        """_load_json_atomic returns empty dict for invalid JSON."""
        invalid_file = temp_memory_dir / "invalid.json"
        invalid_file.write_text("{invalid json", encoding="utf-8")

        result = memory_persistence._load_json_atomic(invalid_file)

        assert result == {}

    def test_save_json_atomic_creates_temp_then_rename(
        self, mock_memory_paths, temp_memory_dir
    ):
        """_save_json_atomic writes to temp file then renames."""
        target_file = temp_memory_dir / "test.json"

        memory_persistence._save_json_atomic(target_file, {"key": "value"})

        # Target file should exist with correct content
        assert target_file.exists()
        content = json.loads(target_file.read_text(encoding="utf-8"))
        assert content == {"key": "value"}

        # No temp files should remain
        temp_files = list(temp_memory_dir.glob("*.tmp"))
        assert len(temp_files) == 0


# =============================================================================
# TestEdgeCases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_get_domain_memory_with_empty_string(self, mock_memory_paths):
        """get_domain_memory handles empty string domain."""
        result = memory_persistence.get_domain_memory("")
        assert result is None

    def test_get_path_memory_with_empty_strings(self, mock_memory_paths):
        """get_path_memory handles empty domain and path."""
        result = memory_persistence.get_path_memory("", "")
        assert result is None

    def test_get_source_memory_with_empty_string(self, mock_memory_paths):
        """get_source_memory handles empty source_id."""
        result = memory_persistence.get_source_memory("")
        assert result is None

    def test_save_domain_memory_normalizes_domain_case(
        self, mock_memory_paths, temp_domain_memory_file
    ):
        """save_domain_memory normalizes domain to lowercase."""
        memory_persistence.save_domain_memory("FederalReserve.gov", {"trust_score": 50})

        content = json.loads(temp_domain_memory_file.read_text(encoding="utf-8"))
        assert "federalreserve.gov" in content
        assert "FederalReserve.gov" not in content

    def test_save_path_memory_normalizes_domain_case(
        self, mock_memory_paths, temp_path_memory_file
    ):
        """save_path_memory normalizes domain to lowercase."""
        memory_persistence.save_path_memory(
            "Brookings.edu", "/events/", {"trust_score": 50}
        )

        content = json.loads(temp_path_memory_file.read_text(encoding="utf-8"))
        assert "brookings.edu::/events/" in content
        # path_pattern should be preserved as-is
        assert content["brookings.edu::/events/"]["path_pattern"] == "/events/"
