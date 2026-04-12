"""
Local-only cleanup: delete *contents* of these three directories (nothing else):

  data/filtered_out/
  data/rejected/
  data/review_queue/

Parent folders are kept empty-able. All other paths under data/ are untouched.

Usage:
  python scripts/cleanup_discard_buckets.py
  python scripts/cleanup_discard_buckets.py --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent

DISCARD_DIRS = (
    BASE_DIR / "data" / "filtered_out",
    BASE_DIR / "data" / "rejected",
    BASE_DIR / "data" / "review_queue",
)


def _clear_directory(root: Path, dry_run: bool) -> tuple[int, int]:
    """Remove all files under root, then empty subdirs; keep root. Returns (file_count, bytes)."""
    if not root.is_dir():
        return 0, 0

    root_resolved = root.resolve()
    files = [p for p in root.rglob("*") if p.is_file()]
    files.sort(key=lambda p: len(p.parts), reverse=True)

    files_removed = 0
    bytes_removed = 0
    for path in files:
        try:
            resolved = path.resolve()
        except OSError as e:
            print(f"skip (cannot resolve): {path.relative_to(BASE_DIR)} ({e})", file=sys.stderr)
            continue
        if not resolved.is_relative_to(root_resolved):
            print(f"skip (outside bucket): {path.relative_to(BASE_DIR)}", file=sys.stderr)
            continue
        sz = path.stat().st_size
        if dry_run:
            print(f"would delete file {path.relative_to(BASE_DIR)} ({sz} bytes)")
        else:
            path.unlink()
        files_removed += 1
        bytes_removed += sz

    if not dry_run:
        dirs = [p for p in root.rglob("*") if p.is_dir()]
        dirs.sort(key=lambda p: len(p.parts), reverse=True)
        for path in dirs:
            try:
                path_resolved = path.resolve()
                if not path_resolved.is_relative_to(root_resolved):
                    continue
            except OSError:
                continue
            try:
                path.rmdir()
            except OSError:
                pass

    return files_removed, bytes_removed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print paths that would be deleted without removing anything",
    )
    args = parser.parse_args()

    total_files = 0
    total_bytes = 0
    for d in DISCARD_DIRS:
        rel = d.relative_to(BASE_DIR)
        if not d.is_dir():
            print(f"skip (missing): {rel}", file=sys.stderr)
            continue
        f, b = _clear_directory(d, args.dry_run)
        total_files += f
        total_bytes += b
        print(f"{'Would remove' if args.dry_run else 'Removed'} {f} files under {rel} ({b} bytes)")

    print(
        f"{'Would remove' if args.dry_run else 'Removed'} {total_files} files total "
        f"({total_bytes / (1024 * 1024):.2f} MiB)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
