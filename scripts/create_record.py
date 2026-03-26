import re
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def main() -> None:
    if len(sys.argv) < 3:
        raise SystemExit('Usage: python scripts/create_record.py "<record title>" "<source text>"')

    record_title = sys.argv[1]
    source_text = sys.argv[2]

    record_id = slugify(record_title)
    output_path = RAW_DIR / f"{record_id}.txt"

    if output_path.exists():
        raise FileExistsError(f"Record already exists: {output_path}")

    output_path.write_text(source_text.strip(), encoding="utf-8")

    print("Created raw source file:")
    print(output_path.relative_to(BASE_DIR))
    print(f"Record ID: {record_id}")


if __name__ == "__main__":
    main()