"""Update Source Memory Script.

Called after a candidate outcome is determined to update persistent memory.

Usage:
    python scripts/update_source_memory.py <domain> <outcome> [source_id] [url] [candidate_id]

Examples:
    python scripts/update_source_memory.py federalreserve.gov accepted
    python scripts/update_source_memory.py brookings.edu rejected_human keyword_inflation https://brookings.edu/research/... candidate_123
"""

import sys
from pathlib import Path

# Add project root to path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from scripts.memory_manager import update_all_memory_on_outcome


def main():
    if len(sys.argv) < 3:
        print(
            "Usage: python update_source_memory.py <domain> <outcome> [source_id] [url] [candidate_id]"
        )
        sys.exit(1)

    domain = sys.argv[1]
    outcome = sys.argv[2]
    source_id = sys.argv[3] if len(sys.argv) > 3 else None
    url = sys.argv[4] if len(sys.argv) > 4 else None
    candidate_id = sys.argv[5] if len(sys.argv) > 5 else None

    result = update_all_memory_on_outcome(
        domain=domain,
        outcome=outcome,
        source_id=source_id,
        url=url,
        candidate_id=candidate_id,
    )

    print(f"Memory updated for {domain}")
    print(f"  Domain trust: {result['domain_memory']['trust_score']}")
    if result["path_memory"]:
        print(
            f"  Path trust ({result['path_pattern']}): {result['path_memory']['trust_score']}"
        )
    if result["source_memory"]:
        print(f"  Source trust: {result['source_memory']['trust_score']}")


if __name__ == "__main__":
    main()
