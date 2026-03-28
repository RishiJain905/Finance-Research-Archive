import json
import re
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
FILTERED_DIR = BASE_DIR / "data" / "filtered_out"
MANIFEST_PATH = BASE_DIR / "data" / "filter_manifest.json"
INGESTION_MANIFEST_PATH = BASE_DIR / "data" / "ingestion_manifest.json"

MIN_TEXT_LENGTH = 500
DEFAULT_MIN_WORD_COUNT = 120

GLOBAL_RELEVANT_KEYWORDS = [
    "cpi",
    "ppi",
    "fomc",
    "federal reserve",
    "fed",
    "jobs",
    "employment",
    "inflation",
    "gdp",
    "retail sales",
    "treasury",
    "yield",
    "liquidity",
    "funding",
    "issuance",
    "market structure",
    "etf",
    "volatility",
    "options",
    "rates",
    "macro"
]

GLOBAL_NOISY_PATTERNS = [
    "cookie",
    "privacy policy",
    "terms of use",
    "sign up",
    "subscribe",
    "javascript is disabled",
    "all rights reserved"
]

METADATA_KEYS = {
    "TARGET",
    "TOPIC",
    "TITLE",
    "URL",
    "INDEX_URL",
    "ARTICLE_URL",
    "CANONICAL_URL",
    "PAGE_TITLE",
    "H1",
    "PUBLISHED_AT",
    "PAGE_TYPE",
    "EXPECTED_LANGUAGE",
    "DETECTED_LANGUAGE",
    "CONTENT_WORD_COUNT",
    "EXTRACTION_WARNINGS",
}

CONTAINER_PAGE_TYPES = {"homepage", "navigation_page", "listing_page", "search_page"}


def load_json(path: Path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def word_count(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))


def parse_raw_record(text: str) -> dict:
    metadata = {}
    body_lines = []
    in_body = False

    for line in text.splitlines():
        if not in_body:
            stripped = line.strip()
            if not stripped and metadata:
                in_body = True
                continue

            if ":" in stripped:
                key, value = stripped.split(":", 1)
                key = key.strip()
                if key in METADATA_KEYS:
                    metadata[key] = value.strip()
                    continue

            if stripped:
                in_body = True

        if in_body:
            body_lines.append(line)

    body = "\n".join(body_lines).strip()
    return {"metadata": metadata, "body": body}


def count_keyword_hits(text: str, keywords: list[str]) -> int:
    lower = text.lower()
    hits = 0
    for keyword in keywords:
        if keyword.lower() in lower:
            hits += 1
    return hits


def count_noisy_hits(text: str) -> int:
    lower = text.lower()
    hits = 0
    for pattern in GLOBAL_NOISY_PATTERNS:
        if pattern in lower:
            hits += 1
    return hits


def detect_quant_record(text: str) -> bool:
    upper = text.upper()
    return (
        "SOURCE: FRED" in upper
        or "SERIES_CODE:" in upper
        or "LATEST_OBSERVATION_VALUE:" in upper
        or "RECENT_OBSERVATIONS:" in upper
        or "SNAPSHOT_DATE:" in upper and ("SOURCE: TREASURY_FISCAL_DATA" in upper or "SOURCE: NYFED" in upper)
    )


def evaluate_quant_record(text: str) -> tuple[bool, list[str]]:
    reasons = []

    upper = text.upper()

    if "SNAPSHOT_DATE:" not in upper:
        reasons.append("missing_snapshot_date")

    if "LATEST_OBSERVATION_VALUE:" not in upper and "RECENT_OBSERVATIONS:" not in upper:
        reasons.append("missing_quant_values")

    recent_obs_lines = [
        line for line in text.splitlines()
        if line.strip().startswith("- ") and ":" in line
    ]
    if len(recent_obs_lines) < 1:
        reasons.append("not_enough_recent_observations")

    keep = len(reasons) == 0
    return keep, reasons


def evaluate_article_record(text: str, rules: dict, metadata: dict | None = None) -> tuple[bool, list[str]]:
    reasons = []
    metadata = metadata or {}

    page_type = metadata.get("PAGE_TYPE", "").strip().lower()
    if page_type in CONTAINER_PAGE_TYPES:
        reasons.append("non_article_page_type")

    expected_language = rules.get("expected_language", "").strip().lower()
    detected_language = metadata.get("DETECTED_LANGUAGE", "").strip().lower()
    if expected_language and detected_language and detected_language not in {expected_language, "unknown"}:
        reasons.append("language_mismatch")

    warnings = metadata.get("EXTRACTION_WARNINGS", "").lower()
    if "container_page" in warnings and "non_article_page_type" not in reasons:
        reasons.append("container_page_warning")

    if len(text) < MIN_TEXT_LENGTH:
        reasons.append("text_too_short")

    wc = word_count(text)
    min_word_count = rules.get("min_word_count", DEFAULT_MIN_WORD_COUNT)
    if wc < min_word_count:
        reasons.append("word_count_too_low")

    required_keywords = rules.get("required_keywords", [])
    blocked_keywords = rules.get("blocked_keywords", [])

    keyword_pool = required_keywords if required_keywords else GLOBAL_RELEVANT_KEYWORDS
    keyword_hits = count_keyword_hits(text, keyword_pool)

    if required_keywords:
        if keyword_hits < 1:
            reasons.append("missing_required_keywords")
    else:
        if keyword_hits < 2:
            reasons.append("not_enough_relevant_keywords")

    blocked_hits = count_keyword_hits(text, blocked_keywords)
    if blocked_hits > 0:
        reasons.append("contains_blocked_keywords")

    noisy_hits = count_noisy_hits(text)
    if noisy_hits >= 3:
        reasons.append("too_much_boilerplate_noise")

    keep = len(reasons) == 0
    return keep, reasons


def main() -> None:
    FILTERED_DIR.mkdir(parents=True, exist_ok=True)

    manifest = load_json(MANIFEST_PATH, {"filtered_out": {}, "kept": {}})
    ingestion_manifest = load_json(INGESTION_MANIFEST_PATH, {"record_rules": {}})

    kept = []
    filtered = []

    for raw_path in sorted(RAW_DIR.glob("*.txt")):
        record_id = raw_path.stem
        text = read_text(raw_path)
        rules = ingestion_manifest.get("record_rules", {}).get(record_id, {})
        parsed = parse_raw_record(text)
        metadata = parsed["metadata"]
        body_text = parsed["body"] or text

        is_quant = detect_quant_record(text)

        if is_quant:
            keep, reasons = evaluate_quant_record(text)
            rules_used = {"record_type": "quant"}
        else:
            keep, reasons = evaluate_article_record(body_text, rules, metadata)
            rules_used = {"record_type": "article", **rules}

        if keep:
            manifest["kept"][record_id] = {"reasons": [], "rules_used": rules_used}
            kept.append(record_id)
            print(f"KEEP: {record_id}")
            continue

        target_path = FILTERED_DIR / raw_path.name
        if raw_path.exists():
            raw_path.replace(target_path)

        manifest["filtered_out"][record_id] = {"reasons": reasons, "rules_used": rules_used}
        filtered.append(record_id)
        print(f"FILTER OUT: {record_id} -> {', '.join(reasons)}")

    save_json(MANIFEST_PATH, manifest)

    print("\nKept records:")
    if kept:
        for record_id in kept:
            print(f"- {record_id}")
    else:
        print("- none")

    print("\nFiltered records:")
    if filtered:
        for record_id in filtered:
            print(f"- {record_id}")
    else:
        print("- none")


if __name__ == "__main__":
    main()
