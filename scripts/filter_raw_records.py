import json
import re
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
FILTERED_DIR = BASE_DIR / "data" / "filtered_out"
MANIFEST_PATH = BASE_DIR / "data" / "filter_manifest.json"

MIN_TEXT_LENGTH = 500
MIN_WORD_COUNT = 120

RELEVANT_KEYWORDS = [
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

NOISY_PATTERNS = [
    "cookie",
    "privacy policy",
    "terms of use",
    "sign up",
    "subscribe",
    "javascript is disabled",
    "all rights reserved"
]


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


def count_keyword_hits(text: str) -> int:
    lower = text.lower()
    hits = 0
    for keyword in RELEVANT_KEYWORDS:
        if keyword in lower:
            hits += 1
    return hits


def count_noisy_hits(text: str) -> int:
    lower = text.lower()
    hits = 0
    for pattern in NOISY_PATTERNS:
        if pattern in lower:
            hits += 1
    return hits


def evaluate_record(text: str) -> tuple[bool, list[str]]:
    reasons = []

    if len(text) < MIN_TEXT_LENGTH:
        reasons.append("text_too_short")

    wc = word_count(text)
    if wc < MIN_WORD_COUNT:
        reasons.append("word_count_too_low")

    keyword_hits = count_keyword_hits(text)
    if keyword_hits < 2:
        reasons.append("not_enough_relevant_keywords")

    noisy_hits = count_noisy_hits(text)
    if noisy_hits >= 3:
        reasons.append("too_much_boilerplate_noise")

    keep = len(reasons) == 0
    return keep, reasons


def main() -> None:
    FILTERED_DIR.mkdir(parents=True, exist_ok=True)

    manifest = load_json(MANIFEST_PATH, {"filtered_out": {}, "kept": {}})
    kept = []
    filtered = []

    for raw_path in sorted(RAW_DIR.glob("*.txt")):
        record_id = raw_path.stem
        text = read_text(raw_path)

        keep, reasons = evaluate_record(text)

        if keep:
            manifest["kept"][record_id] = {"reasons": []}
            kept.append(record_id)
            print(f"KEEP: {record_id}")
            continue

        target_path = FILTERED_DIR / raw_path.name
        if raw_path.exists():
            raw_path.replace(target_path)

        manifest["filtered_out"][record_id] = {"reasons": reasons}
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