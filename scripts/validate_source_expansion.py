"""
V2.7 Part 6: Source Expansion Config Validation Script.

Validates all expansion config files for structural integrity,
required fields, valid enums, and cross-references.

Usage:
    python scripts/validate_source_expansion.py
"""

import argparse
import json
from pathlib import Path
from urllib.parse import urlparse
from typing import Any


# Base directory is the project root (two levels up from this file)
BASE_DIR = Path(__file__).resolve().parent.parent


# =============================================================================
# JSON Loading
# =============================================================================


def load_json(path: Path) -> dict | None:
    """Load JSON with error handling. Returns None on failure."""
    if not path.exists():
        print(f"  [ERROR] File not found: {path}")
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"  [ERROR] Invalid JSON: {e}")
        return None


# =============================================================================
# Families Config Validation
# =============================================================================


def validate_families_config(config: dict) -> list[str]:
    """Validate article_source_families.json structure.

    Returns list of error strings (empty if valid).
    Checks:
    - Has 'families', 'target_classes', 'priority_tiers' keys
    - Each family has: label, description, default_priority_tier, default_topic, batch, target_count
    - Each target_class has: description, typical_max_links
    - Each priority_tier has: description, enabled_by_default
    """
    errors = []

    # Check for required top-level keys
    required_toplevel_keys = ["families", "target_classes", "priority_tiers"]
    for key in required_toplevel_keys:
        if key not in config:
            errors.append(f"Missing required key '{key}'")
            return errors  # Can't validate further without these

    # Validate families
    families = config.get("families", {})
    if not isinstance(families, dict):
        errors.append("'families' must be a dictionary")
        return errors

    required_family_keys = [
        "label",
        "description",
        "default_priority_tier",
        "default_topic",
        "batch",
        "target_count",
    ]
    for family_id, family in families.items():
        if not isinstance(family, dict):
            errors.append(f"Family '{family_id}': must be a dictionary")
            continue
        for key in required_family_keys:
            if key not in family:
                errors.append(f"Family '{family_id}': missing required key '{key}'")

    # Validate target_classes
    target_classes = config.get("target_classes", {})
    if not isinstance(target_classes, dict):
        errors.append("'target_classes' must be a dictionary")
        return errors

    required_target_class_keys = ["description", "typical_max_links"]
    for class_id, target_class in target_classes.items():
        if not isinstance(target_class, dict):
            errors.append(f"Target class '{class_id}': must be a dictionary")
            continue
        for key in required_target_class_keys:
            if key not in target_class:
                errors.append(
                    f"Target class '{class_id}': missing required key '{key}'"
                )

    # Validate priority_tiers
    priority_tiers = config.get("priority_tiers", {})
    if not isinstance(priority_tiers, dict):
        errors.append("'priority_tiers' must be a dictionary")
        return errors

    required_priority_tier_keys = ["description", "enabled_by_default"]
    valid_tier_names = ["S", "A", "B", "C"]
    for tier_name in priority_tiers:
        if tier_name not in valid_tier_names:
            errors.append(
                f"Priority tier '{tier_name}': must be one of {valid_tier_names}"
            )
        tier = priority_tiers[tier_name]
        if not isinstance(tier, dict):
            errors.append(f"Priority tier '{tier_name}': must be a dictionary")
            continue
        for key in required_priority_tier_keys:
            if key not in tier:
                errors.append(
                    f"Priority tier '{tier_name}': missing required key '{key}'"
                )

    return errors


# =============================================================================
# Target Validation
# =============================================================================


def is_valid_url(url: str) -> bool:
    """Check if URL has valid format (non-empty and contains ://)."""
    if not url or not isinstance(url, str):
        return False
    return "://" in url


def validate_target(target: dict, families: dict, target_classes: dict) -> list[str]:
    """Validate a single target entry.

    Returns list of error strings (empty if valid).
    Checks:
    - Required fields: name, topic, url, allowed_prefixes, max_links, family, target_class, priority_tier, enabled
    - family references a valid family in families config
    - target_class references a valid class in target_classes config
    - priority_tier is one of S/A/B/C
    - enabled is boolean
    - url is non-empty string
    - allowed_prefixes is non-empty list
    - max_links is positive integer
    - min_word_count is positive integer (if present)
    - If enabled is False, disabled_reason should be present
    """
    errors = []

    # Required fields
    required_fields = [
        "name",
        "topic",
        "url",
        "allowed_prefixes",
        "max_links",
        "family",
        "target_class",
        "priority_tier",
        "enabled",
    ]
    for field in required_fields:
        if field not in target:
            errors.append(f"Missing required field '{field}'")

    # Get values with safe defaults for further validation
    name = target.get("name", "")
    url = target.get("url", "")
    allowed_prefixes = target.get("allowed_prefixes", [])
    max_links = target.get("max_links", 0)
    family = target.get("family", "")
    target_class = target.get("target_class", "")
    priority_tier = target.get("priority_tier", "")
    enabled = target.get("enabled")
    min_word_count = target.get("min_word_count")
    disabled_reason = target.get("disabled_reason")

    # Validate name
    if name and not isinstance(name, str):
        errors.append(f"name must be a string")

    # Validate URL
    if url and not is_valid_url(url):
        errors.append(f"url is invalid (must be non-empty string containing '://')")

    # Validate allowed_prefixes
    if not isinstance(allowed_prefixes, list):
        errors.append(f"allowed_prefixes must be a list")
    elif len(allowed_prefixes) == 0:
        errors.append(f"allowed_prefixes must be non-empty")

    # Validate max_links
    if not isinstance(max_links, int):
        errors.append(f"max_links must be an integer")
    elif max_links <= 0:
        errors.append(f"max_links must be a positive integer")

    # Validate min_word_count if present
    if min_word_count is not None:
        if not isinstance(min_word_count, int):
            errors.append(f"min_word_count must be an integer")
        elif min_word_count <= 0:
            errors.append(f"min_word_count must be a positive integer")

    # Validate family reference
    if family and family not in families:
        errors.append(
            f"family '{family}' not in registry (valid: {list(families.keys())})"
        )

    # Validate target_class reference
    if target_class and target_class not in target_classes:
        errors.append(
            f"target_class '{target_class}' not in registry (valid: {list(target_classes.keys())})"
        )

    # Validate priority_tier
    valid_priority_tiers = ["S", "A", "B", "C"]
    if priority_tier and priority_tier not in valid_priority_tiers:
        errors.append(
            f"priority_tier '{priority_tier}' must be one of {valid_priority_tiers}"
        )

    # Validate enabled is boolean
    if enabled is not None and not isinstance(enabled, bool):
        errors.append(f"enabled must be a boolean")

    # If enabled is False, disabled_reason should be present
    if enabled is False and not disabled_reason:
        errors.append(f"enabled is False but disabled_reason is missing")

    return errors


# =============================================================================
# Batch Config Validation
# =============================================================================


def validate_batch_config(
    config: dict, families: dict, target_classes: dict
) -> list[str]:
    """Validate a batch config file.

    Returns list of error strings (empty if valid).
    Checks:
    - Has 'batch', 'description', 'families', 'targets' keys
    - batch is integer 1-3
    - families list matches declared families
    - Each target passes validate_target
    - No duplicate URLs within the batch
    """
    errors = []

    # Check for required keys
    required_keys = ["batch", "description", "families", "targets"]
    for key in required_keys:
        if key not in config:
            errors.append(f"Missing required key '{key}'")
            return errors  # Can't validate further

    # Validate batch number
    batch = config.get("batch")
    if not isinstance(batch, int) or batch < 1 or batch > 3:
        errors.append(f"batch must be an integer 1-3, got: {batch}")

    # Validate families list
    families_list = config.get("families", [])
    if not isinstance(families_list, list):
        errors.append(f"'families' must be a list")
    elif len(families_list) == 0:
        errors.append(f"'families' list is empty")

    # Validate targets list
    targets = config.get("targets", [])
    if not isinstance(targets, list):
        errors.append(f"'targets' must be a list")
        return errors

    if len(targets) == 0:
        errors.append(f"'targets' list is empty")

    # Validate each target
    target_urls = []
    for i, target in enumerate(targets):
        if not isinstance(target, dict):
            errors.append(f"Target[{i}]: must be a dictionary")
            continue

        target_errors = validate_target(target, families, target_classes)
        for err in target_errors:
            errors.append(
                f"Target[{i}]{f' ({target.get("name", "unnamed")})' if 'name' in target else ''}: {err}"
            )

        # Collect URLs for duplicate check
        url = target.get("url", "")
        if url:
            target_urls.append((i, url))

    # Check for duplicate URLs within batch
    seen_urls = {}
    for i, url in target_urls:
        if url in seen_urls:
            errors.append(
                f"Target[{i}]: duplicate URL (also in target[{seen_urls[url]}])"
            )
        else:
            seen_urls[url] = i

    return errors


# =============================================================================
# Cross-Validation
# =============================================================================


def check_no_duplicate_urls_across_batches(batches: list[dict]) -> list[str]:
    """Check for duplicate URLs across all batches.

    Returns list of error strings (empty if no duplicates).
    """
    errors = []
    url_locations: dict[str, list[tuple[int, int]]] = {}

    for batch_config in batches:
        batch_num = batch_config.get("batch", "?")
        targets = batch_config.get("targets", [])

        if not isinstance(targets, list):
            continue

        for i, target in enumerate(targets):
            if not isinstance(target, dict):
                continue
            url = target.get("url", "")
            if url:
                if url not in url_locations:
                    url_locations[url] = []
                url_locations[url].append((batch_num, i))

    # Find duplicates
    for url, locations in url_locations.items():
        if len(locations) > 1:
            loc_strs = [f"batch {b} target {t}" for b, t in locations]
            errors.append(f"Duplicate URL: {url} ({', '.join(loc_strs)})")

    return errors


def check_family_target_counts(families_config: dict, batches: list[dict]) -> list[str]:
    """Verify family target counts match declared counts.

    Returns list of error strings (empty if counts match or exceed).
    """
    errors = []
    families = families_config.get("families", {})

    # Count targets per family across all batches
    family_counts: dict[str, int] = {f: 0 for f in families}

    for batch_config in batches:
        targets = batch_config.get("targets", [])
        if not isinstance(targets, list):
            continue

        for target in targets:
            if not isinstance(target, dict):
                continue
            family = target.get("family", "")
            if family in family_counts:
                family_counts[family] += 1

    # Compare with declared target_counts
    for family_id, family_data in families.items():
        declared_count = family_data.get("target_count", 0)
        actual_count = family_counts.get(family_id, 0)

        if actual_count < declared_count:
            errors.append(
                f"Family '{family_id}': expected at least {declared_count} targets, "
                f"found {actual_count} (batch targets: {family_counts[family_id]})"
            )

    return errors


# =============================================================================
# Main Validation
# =============================================================================


def validate_all_configs(base_dir: Path) -> dict[str, list[str]]:
    """Validate all expansion configs.

    Returns dict keyed by config file with list of errors.
    """
    results: dict[str, list[str]] = {}

    # Load families config
    families_path = base_dir / "config" / "article_source_families.json"
    families_config = load_json(families_path)

    if families_config is None:
        results[str(families_path)] = ["Failed to load families config"]
        return results

    # Validate families config
    families_errors = validate_families_config(families_config)
    if families_errors:
        results[str(families_path)] = families_errors
        return results

    results[str(families_path)] = []

    # Get families and target_classes for target validation
    families = families_config.get("families", {})
    target_classes = families_config.get("target_classes", {})

    # Load and validate batch configs
    batch_configs = []
    for batch_num in range(1, 4):
        batch_path = (
            base_dir / "config" / f"article_source_expansion_batch_{batch_num}.json"
        )
        batch_config = load_json(batch_path)

        if batch_config is None:
            results[str(batch_path)] = ["Failed to load batch config"]
            continue

        batch_errors = validate_batch_config(batch_config, families, target_classes)

        if batch_errors:
            results[str(batch_path)] = batch_errors
        else:
            results[str(batch_path)] = []

        batch_configs.append(batch_config)

    # Cross-validation
    cross_errors: list[str] = []

    # Check for duplicate URLs across batches
    dup_url_errors = check_no_duplicate_urls_across_batches(batch_configs)
    cross_errors.extend(dup_url_errors)

    # Check family target counts
    family_count_errors = check_family_target_counts(families_config, batch_configs)
    cross_errors.extend(family_count_errors)

    if cross_errors:
        results["_cross_validation"] = cross_errors
    else:
        results["_cross_validation"] = []

    return results


# =============================================================================
# CLI Entry Point
# =============================================================================


def main() -> None:
    """CLI entry point. Runs all validations and prints summary."""
    parser = argparse.ArgumentParser(
        description="Validate source expansion configs for structural integrity"
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=None,
        help="Base directory (default: script parent's parent)",
    )
    args = parser.parse_args()

    base_dir = args.base_dir if args.base_dir else BASE_DIR

    print("Validating source expansion configs...\n")

    # Run validation
    results = validate_all_configs(base_dir)

    # Track totals
    total_configs = 0
    total_errors = 0
    files_with_errors: list[str] = []

    # Load families config for stats
    families_path = base_dir / "config" / "article_source_families.json"
    families_config = load_json(families_path)
    families = families_config.get("families", {}) if families_config else {}
    target_classes = (
        families_config.get("target_classes", {}) if families_config else {}
    )
    priority_tiers = (
        families_config.get("priority_tiers", {}) if families_config else {}
    )

    # Print families config result
    families_result = results.get(str(families_path), [])
    if families_result:
        print(f"[ERROR] {families_path}")
        for err in families_result:
            print(f"  - {err}")
        files_with_errors.append(str(families_path))
        total_errors += len(families_result)
    else:
        print(f"[OK] {families_path}")
        print(
            f"  - {len(families)} families, {len(target_classes)} target classes, {len(priority_tiers)} priority tiers"
        )
        total_configs += 1

    # Print batch config results
    for batch_num in range(1, 4):
        batch_path = (
            base_dir / "config" / f"article_source_expansion_batch_{batch_num}.json"
        )
        batch_result = results.get(str(batch_path), [])

        if batch_result:
            print(f"\n[ERROR] {batch_path}")
            for err in batch_result:
                print(f"  - {err}")
            if str(batch_path) not in files_with_errors:
                files_with_errors.append(str(batch_path))
            total_errors += len(batch_result)
        else:
            batch_config = load_json(batch_path)
            if batch_config:
                num_targets = len(batch_config.get("targets", []))
                batch_families = batch_config.get("families", [])
                print(f"\n[OK] {batch_path}")
                print(f"  - {num_targets} targets, {len(batch_families)} families")
                total_configs += 1

    # Print cross-validation results
    print("\nCross-validation:")
    cross_errors = results.get("_cross_validation", [])

    if cross_errors:
        print("  [ERROR]")
        for err in cross_errors:
            print(f"    - {err}")
        total_errors += len(cross_errors)
    else:
        print("  [OK] No duplicate URLs across batches")
        print("  [OK] Family target counts match")

    # Count total sources
    total_sources = 0
    for batch_num in range(1, 4):
        batch_path = (
            base_dir / "config" / f"article_source_expansion_batch_{batch_num}.json"
        )
        batch_config = load_json(batch_path)
        if batch_config:
            total_sources += len(batch_config.get("targets", []))

    # Print summary
    print(f"\nTotal: {total_sources} sources across 3 batches")

    if files_with_errors or cross_errors:
        print(
            f"\nERRORS FOUND: {total_errors} issues across {len(files_with_errors) + (1 if cross_errors else 0)} files"
        )
        exit(1)
    else:
        print("All configs valid!")
        exit(0)


if __name__ == "__main__":
    main()
