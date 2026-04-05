"""
V2.7 Part 4 Phase 2: Core Linking Script (Part 1).

Standalone article-quant link generation with:
- Time window scoring based on date proximity
- Topic compatibility using topic_to_series mapping
- Keyword overlap using keyword_to_series mapping
- Event alignment scoring
- Weighted combined scoring
- Relationship classification (supports/context/weak_context)

Links are saved as standalone JSON files in data/article_quant_links/.
"""

import hashlib
import json
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "quant_linking_rules.json"
ACCEPTED_DIR = BASE_DIR / "data" / "accepted"
OUTPUT_DIR = BASE_DIR / "data" / "article_quant_links"

# Quant series IDs from config/quant_sources.json
QUANT_SERIES_IDS = frozenset(
    {
        "sofr",
        "fed_funds",
        "iorb",
        "2y_yield",
        "10y_yield",
        "treasury_auctions",
        "upcoming_treasury_auctions",
        "repo_operations",
    }
)

# Quant source types
QUANT_SOURCE_TYPES = frozenset({"quant_snapshot", "dataset_snapshot"})


# =============================================================================
# Config Loading
# =============================================================================


def load_config(config_path: Path) -> dict:
    """Load quant linking rules configuration.

    Args:
        config_path: Path to quant_linking_rules.json

    Returns:
        Config dict with scoring_bands, topic_to_series, keyword_to_series, weights
    """
    if not config_path.exists():
        return _default_config()
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _default_config() -> dict:
    """Return default config if file doesn't exist."""
    return {
        "time_window_days": 7,
        "scoring_bands": {
            "supports": 80,
            "context": 60,
            "weak_context": 40,
        },
        "topic_to_series": {
            "monetary policy": ["fed_funds", "iorb"],
            "rates": ["fed_funds", "iorb", "2y_yield", "10y_yield"],
            "treasury": ["treasury_auctions", "upcoming_treasury_auctions"],
            "liquidity": ["sofr", "repo_operations"],
            "market structure": ["sofr", "repo_operations", "iorb"],
        },
        "keyword_to_series": {
            "sofr": ["sofr"],
            "fed_funds": ["fed_funds"],
            "iorb": ["iorb"],
            "treasury": ["treasury_auctions"],
            "repo": ["repo_operations"],
            "liquidity": ["sofr", "repo_operations"],
            "rates": ["fed_funds", "2y_yield", "10y_yield"],
            "fomc": ["fed_funds"],
            "policy": ["fed_funds", "iorb"],
        },
        "weights": {
            "time_window": 0.30,
            "topic_compatibility": 0.35,
            "keyword_overlap": 0.20,
            "event_alignment": 0.15,
        },
    }


def load_json(path: Path, default=None) -> Optional[dict]:
    """Load JSON file with fallback to default.

    Args:
        path: Path to JSON file
        default: Default value if file doesn't exist

    Returns:
        Parsed JSON data or default
    """
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data) -> None:
    """Save data to JSON file.

    Args:
        path: Path to output file
        data: Data to serialize
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# =============================================================================
# Record Loading
# =============================================================================


def is_quant_record(record: dict) -> bool:
    """Check if a record is a quant record.

    Args:
        record: Record dict with id, source.source_type, etc.

    Returns:
        True if record is a quant record
    """
    # Check source_type
    source_type = record.get("source", {}).get("source_type", "")
    if source_type in QUANT_SOURCE_TYPES:
        return True

    # Check record_id against known quant series patterns
    record_id = record.get("id", "")
    for series_id in QUANT_SERIES_IDS:
        if record_id.startswith(series_id + "_") or record_id == series_id:
            return True

    return False


def is_article_record(record: dict) -> bool:
    """Check if a record is an article record (not quant).

    Args:
        record: Record dict

    Returns:
        True if record is an article (not quant)
    """
    return not is_quant_record(record)


def load_accepted_records(base_dir: Path) -> tuple[list[dict], list[dict]]:
    """Load all accepted records and separate into (articles, quants).

    Args:
        base_dir: Base directory of the archive

    Returns:
        Tuple of (articles list, quants list)
    """
    accepted_dir = base_dir / "data" / "accepted"

    if not accepted_dir.exists():
        return [], []

    articles = []
    quants = []

    for json_file in accepted_dir.glob("*.json"):
        try:
            with json_file.open("r", encoding="utf-8") as f:
                record = json.load(f)

            if is_quant_record(record):
                quants.append(record)
            else:
                articles.append(record)
        except Exception as e:
            print(f"Warning: Failed to load {json_file}: {e}")
            continue

    return articles, quants


# =============================================================================
# Date Parsing
# =============================================================================


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse a date string in various formats.

    Supports: ISO 8601, "MM/DD/YYYY", "YYYY_MM_DD"

    Args:
        date_str: Date string to parse

    Returns:
        datetime object or None if parsing fails
    """
    if not date_str or not isinstance(date_str, str):
        return None

    date_str = date_str.strip()

    # Try ISO 8601 format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
    iso_formats = [
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in iso_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            # Normalize to naive datetime (strip timezone info)
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            return dt
        except ValueError:
            pass

    # Try MM/DD/YYYY
    try:
        return datetime.strptime(date_str, "%m/%d/%Y")
    except ValueError:
        pass

    # Try YYYY_MM_DD
    try:
        return datetime.strptime(date_str, "%Y_%m_%d")
    except ValueError:
        pass

    # Try with additional content after date (e.g., "3/18/2026 (FOMC statement)")
    import re

    match = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str)
    if match:
        month, day, year = match.groups()
        try:
            return datetime(int(year), int(month), int(day))
        except ValueError:
            pass

    return None


def is_business_day(date: datetime) -> bool:
    """Check if a date is a business day (Monday-Friday).

    Args:
        date: datetime object

    Returns:
        True if weekday (Mon=0, Sun=6), False if weekend
    """
    return date.weekday() < 5


def compute_time_window_score(date1_str: str, date2_str: str, config: dict) -> float:
    """Compute time window score based on days between dates.

    Uses time_window_days from config to determine scoring:
    - Same day: 100
    - Within time_window_days: scales from 100 to 40
    - Beyond time_window_days: 0

    Args:
        date1_str: First date string
        date2_str: Second date string
        config: Config dict with time_window_days

    Returns:
        Time window score (0-100)
    """
    date1 = parse_date(date1_str)
    date2 = parse_date(date2_str)

    if date1 is None or date2 is None:
        return 0.0

    # Normalize to date only (remove time component)
    date1 = date1.replace(hour=0, minute=0, second=0, microsecond=0)
    date2 = date2.replace(hour=0, minute=0, second=0, microsecond=0)

    delta_days = abs((date1 - date2).days)
    time_window_days = config.get("time_window_days", 7)

    if delta_days == 0:
        return 100.0
    elif delta_days <= time_window_days:
        # Linear decay from 100 to 40 over the time window
        score = 100.0 - (delta_days / time_window_days) * 60.0
        return round(score, 2)
    else:
        return 0.0


# =============================================================================
# Topic Compatibility
# =============================================================================


def get_topic_series(topic: str, config: dict) -> list[str]:
    """Look up series associated with a topic.

    Args:
        topic: Topic string to look up
        config: Config dict with topic_to_series mapping

    Returns:
        List of series IDs associated with the topic
    """
    topic_lower = topic.lower().strip()
    topic_to_series = config.get("topic_to_series", {})
    return topic_to_series.get(topic_lower, [])


def compute_topic_score(
    article: dict, quant: dict, config: dict
) -> tuple[float, list[str]]:
    """Compute topic compatibility score between article and quant.

    Checks if quant's series ID matches any of the article's topic series.

    Args:
        article: Article record dict with topic, tags
        quant: Quant record dict with id
        config: Config dict

    Returns:
        Tuple of (score, matched_series list)
    """
    article_topic = (article.get("topic") or "").lower().strip()
    quant_id = (quant.get("id") or "").lower()

    if not article_topic:
        return 0.0, []

    # Get series associated with article's topic
    topic_series = get_topic_series(article_topic, config)

    # Check if quant's ID starts with any of the topic series
    matched_series = []
    for series in topic_series:
        if quant_id.startswith(series + "_") or quant_id == series:
            matched_series.append(series)

    if not matched_series:
        return 0.0, []

    # Score based on number of matched series (up to 100)
    score = min(float(len(matched_series) * 50), 100.0)
    return round(score, 2), matched_series


# =============================================================================
# Keyword Overlap
# =============================================================================


def get_keyword_series(tags: list[str], config: dict) -> list[str]:
    """Look up series associated with keywords/tags.

    Args:
        tags: List of tag strings
        config: Config dict with keyword_to_series mapping

    Returns:
        List of series IDs associated with the tags
    """
    keyword_to_series = config.get("keyword_to_series", {})
    series_set = set()

    for tag in tags:
        tag_lower = tag.lower().strip()
        if tag_lower in keyword_to_series:
            series_set.update(keyword_to_series[tag_lower])

    return list(series_set)


def compute_keyword_overlap_score(
    article: dict, quant: dict, config: dict
) -> tuple[float, list[str]]:
    """Compute keyword/tag overlap score between article and quant.

    Checks if article's tags match quant's associated keywords.

    Args:
        article: Article record dict with tags
        quant: Quant record dict with tags
        config: Config dict

    Returns:
        Tuple of (score, matched_keywords list)
    """
    article_tags = set(tag.lower() for tag in (article.get("tags") or []))
    quant_tags = set(tag.lower() for tag in (quant.get("tags") or []))

    if not article_tags:
        return 0.0, []

    # Get series associated with article tags
    article_series = set(get_keyword_series(list(article_tags), config))
    # Get series associated with quant tags
    quant_series = set(get_keyword_series(list(quant_tags), config))

    # Find overlap
    matched = article_series & quant_series

    if not matched:
        return 0.0, []

    # Score based on Jaccard-like overlap (up to 100)
    union = article_series | quant_series
    if union:
        jaccard = len(matched) / len(union)
        score = jaccard * 100.0
    else:
        score = 0.0

    return round(score, 2), list(matched)


# =============================================================================
# Event Alignment
# =============================================================================


def load_events(event_dir: Path) -> list[dict]:
    """Load event clusters from data/events/ directory.

    Args:
        event_dir: Path to events directory

    Returns:
        List of event cluster dicts
    """
    if not event_dir.exists():
        return []

    events = []
    for event_file in event_dir.glob("*.json"):
        try:
            with event_file.open("r", encoding="utf-8") as f:
                event = json.load(f)
                events.append(event)
        except Exception as e:
            print(f"Warning: Failed to load event {event_file}: {e}")
            continue

    return events


def compute_event_alignment_score(
    article: dict, quant: dict, events: list[dict]
) -> tuple[float, str | None]:
    """Compute event alignment score between article and quant.

    Checks if both article and quant appear in the same event cluster.

    Args:
        article: Article record dict
        quant: Quant record dict
        events: List of event cluster dicts

    Returns:
        Tuple of (score, event_id or None)
    """
    article_id = article.get("id", "")
    quant_id = quant.get("id", "")

    if not article_id or not quant_id:
        return 0.0, None

    for event in events:
        record_ids = event.get("record_ids", [])
        if article_id in record_ids and quant_id in record_ids:
            # Both are in the same event - high alignment
            return 100.0, event.get("event_id")

    return 0.0, None


# =============================================================================
# Combined Scoring
# =============================================================================


def compute_link_score(
    article: dict, quant: dict, events: list[dict], config: dict
) -> tuple[float, list[str]]:
    """Compute combined link score between article and quant.

    Weighted sum of:
    - Time window score
    - Topic compatibility score
    - Keyword overlap score
    - Event alignment score

    Args:
        article: Article record dict
        quant: Quant record dict
        events: List of event cluster dicts
        config: Config dict with dimension_weights

    Returns:
        Tuple of (total_score, list of matched_dimension names)
    """
    weights = config.get("dimension_weights", {})
    weight_time = weights.get("time_window", 0.30)
    weight_topic = weights.get("topic", 0.30)
    weight_keyword = weights.get("keyword_overlap", 0.25)
    weight_event = weights.get("event_alignment", 0.20)

    # Get dates
    article_date = article.get("source", {}).get("published_at", "")
    quant_date = quant.get("source", {}).get("published_at", "")

    # Time window score (raw 0-100)
    time_score = compute_time_window_score(article_date, quant_date, config)

    # Topic compatibility score (raw 0-100)
    topic_score, topic_matched = compute_topic_score(article, quant, config)

    # Keyword overlap score (raw 0-100)
    keyword_score, keyword_matched = compute_keyword_overlap_score(
        article, quant, config
    )

    # Event alignment score (raw 0-100)
    event_score, event_id = compute_event_alignment_score(article, quant, events)

    # Weighted sum (each component is 0-100, weights sum to 1.0)
    total_score = (
        time_score * weight_time
        + topic_score * weight_topic
        + keyword_score * weight_keyword
        + event_score * weight_event
    )

    # Build matched dimensions list
    dimensions = []
    if time_score > 0:
        dimensions.append("time_window")
    if topic_score > 0:
        dimensions.append("topic")
    if keyword_score > 0:
        dimensions.append("keyword_overlap")
    if event_score > 0:
        dimensions.append("event_alignment")

    return round(total_score, 2), dimensions


# =============================================================================
# Relationship Classification
# =============================================================================


def classify_relationship(score: float, config: dict) -> str | None:
    """Map score to relationship type using scoring_bands.

    Relationship types:
    - supports: score >= 80 (or strong.min if configured)
    - context: score 60-79 (or contextual.min if configured)
    - weak_context: score 40-59 (or weak.min if configured)
    - None (no link): score < 40 (or ignore_below if configured)

    Args:
        score: Link score
        config: Config dict with scoring_bands

    Returns:
        Relationship type string or None
    """
    scoring_bands = config.get("scoring_bands", {})

    # Handle both flat and nested scoring_bands structures
    if "strong" in scoring_bands:
        # Nested structure: {"strong": {"min": 80, "relationship": "supports"}, ...}
        supports_threshold = scoring_bands.get("strong", {}).get("min", 80)
        context_threshold = scoring_bands.get("contextual", {}).get("min", 60)
        weak_context_threshold = scoring_bands.get("weak", {}).get("min", 40)
        ignore_below = scoring_bands.get("ignore_below", 40)
    else:
        # Flat structure: {"supports": 80, "context": 60, ...}
        supports_threshold = scoring_bands.get("supports", 80)
        context_threshold = scoring_bands.get("context", 60)
        weak_context_threshold = scoring_bands.get("weak_context", 40)
        ignore_below = scoring_bands.get("ignore_below", 40)

    if score >= supports_threshold:
        return "supports"
    elif score >= context_threshold:
        return "context"
    elif score >= weak_context_threshold:
        return "weak_context"
    elif ignore_below and score < ignore_below:
        return None
    elif score < weak_context_threshold:
        return None
    else:
        return None


# =============================================================================
# Link Creation
# =============================================================================


def generate_link_id(article_id: str, quant_id: str) -> str:
    """Generate deterministic link ID from article and quant IDs.

    Uses SHA256 hash of combined IDs for deterministic output.

    Args:
        article_id: Article record ID
        quant_id: Quant record ID

    Returns:
        Deterministic link ID string
    """
    combined = f"{article_id}:{quant_id}"
    hash_digest = hashlib.sha256(combined.encode("utf-8")).hexdigest()[:16]
    return f"link_{hash_digest}"


def create_link(
    article_id: str,
    quant_id: str,
    event_id: str | None,
    relationship: str,
    score: float,
    matched_dimensions: list[str],
) -> dict:
    """Create a link dict conforming to schema.

    Args:
        article_id: Article record ID
        quant_id: Quant record ID
        event_id: Associated event ID (or None)
        relationship: Relationship type (supports/context/weak_context)
        score: Link score
        matched_dimensions: List of dimension names that contributed

    Returns:
        Link dict with all required fields
    """
    link_id = generate_link_id(article_id, quant_id)
    now = datetime.now().isoformat()

    link = {
        "link_id": link_id,
        "article_id": article_id,
        "quant_id": quant_id,
        "relationship": relationship,
        "score": score,
        "matched_dimensions": matched_dimensions,
        "created_at": now,
    }

    if event_id is not None:
        link["event_id"] = event_id

    return link


# =============================================================================
# Link Persistence
# =============================================================================


def save_link(link: dict, output_dir: Path) -> None:
    """Write standalone link JSON to output directory.

    Args:
        link: Link dict to save
        output_dir: Directory to write link file
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    link_id = link.get(
        "link_id", generate_link_id(link["article_id"], link["quant_id"])
    )
    file_path = output_dir / f"{link_id}.json"
    save_json(file_path, link)


def load_links(output_dir: Path) -> list[dict]:
    """Load all link JSON files from output directory.

    Args:
        output_dir: Directory containing link JSON files

    Returns:
        List of link dicts
    """
    if not output_dir.exists():
        return []

    links = []
    for link_file in output_dir.glob("*.json"):
        try:
            link = load_json(link_file)
            if link:
                links.append(link)
        except Exception as e:
            print(f"Warning: Failed to load link {link_file}: {e}")
            continue

    return links


def load_event_links(output_dir: Path) -> dict:
    """Load existing links into dict keyed by (article_id, quant_id).

    Args:
        output_dir: Directory containing link JSON files

    Returns:
        Dict mapping (article_id, quant_id) tuple to link dict
    """
    links = load_links(output_dir)
    event_links = {}
    for link in links:
        key = (link.get("article_id", ""), link.get("quant_id", ""))
        event_links[key] = link
    return event_links


# =============================================================================
# Record Enrichment
# =============================================================================


def enrich_accepted_record(
    record: dict, related_links: list[dict], link_type: str
) -> dict:
    """
    Enrich an accepted record with quant_context or article_context block.

    Args:
        record: The accepted record dict
        related_links: List of link dicts (with record_id, relationship, link_score)
        link_type: "quant" for article records, "article" for quant records

    Returns:
        Enriched record dict with quant_context or article_context block
    """
    if link_type == "quant":
        context_key = "quant_context"
        linked_field = "linked_quant_records"
    else:
        context_key = "article_context"
        linked_field = "linked_article_records"

    # Build list of linked record IDs
    linked_record_ids = [link.get("record_id", "") for link in related_links]
    linked_record_ids = [rid for rid in linked_record_ids if rid]  # Filter empty

    # Build enrichment context
    context = {
        linked_field: linked_record_ids,
        "summary": build_enrichment_summary(related_links, BASE_DIR),
    }

    # Create enriched record (copy to avoid mutating original)
    enriched = dict(record)
    enriched[context_key] = context

    return enriched


def build_enrichment_summary(links: list[dict], base_dir: Path) -> str:
    """
    Build a human-readable summary of linked records for enrichment.

    Args:
        links: List of link dicts
        base_dir: Base directory for looking up linked record titles

    Returns:
        Summary string like "Front-end rates remained elevated around the policy communication."
    """
    if not links:
        return "No related records found."

    accepted_dir = base_dir / "data" / "accepted"

    # Collect titles of linked records
    titles = []
    for link in links:
        record_id = link.get("record_id", "")
        if not record_id:
            continue

        record_path = accepted_dir / f"{record_id}.json"
        if record_path.exists():
            try:
                with record_path.open("r", encoding="utf-8") as f:
                    record_data = json.load(f)
                title = record_data.get("title", "")
                if title:
                    titles.append(title)
            except Exception:
                pass

    if not titles:
        # Fallback: summarize by relationship types
        relationships = [link.get("relationship", "") for link in links]
        relationships = [r for r in relationships if r]
        if relationships:
            unique_rels = list(set(relationships))
            return f"Related via {', '.join(unique_rels)} relationship."

        return "Related records identified through multi-dimensional scoring."

    # Build a natural summary from titles
    if len(titles) == 1:
        return f"Related to: {titles[0]}."
    elif len(titles) == 2:
        return f"Related to: {titles[0]} and {titles[1]}."
    else:
        return f"Related to: {', '.join(titles[:-1])}, and {titles[-1]}."


def write_enriched_record(record: dict, base_dir: Path) -> None:
    """
    Write updated record back to data/accepted/.

    Args:
        record: Enriched record dict
        base_dir: Base directory
    """
    accepted_dir = base_dir / "data" / "accepted"
    record_id = record.get("id", "")
    if not record_id:
        return

    file_path = accepted_dir / f"{record_id}.json"
    save_json(file_path, record)


# =============================================================================
# Event Cluster Enrichment
# =============================================================================


def load_event(event_id: str, event_dir: Path) -> dict | None:
    """Load a single event cluster by event_id.

    Args:
        event_id: Event cluster ID
        event_dir: Path to events directory

    Returns:
        Event dict or None if not found
    """
    event_file = event_dir / f"{event_id}.json"
    if not event_file.exists():
        return None
    return load_json(event_file)


def enrich_event_cluster(
    event: dict, article_links: list[str], quant_links: list[str]
) -> dict:
    """
    Enrich an event cluster with both narrative (article_links) and numeric (quant_links) evidence.

    Args:
        event: Event cluster dict
        article_links: List of related article record IDs
        quant_links: List of related quant record IDs

    Returns:
        Enriched event cluster dict
    """
    # Create enriched event (copy to avoid mutating original)
    enriched = dict(event)

    # Ensure article_links and quant_links exist
    if "article_links" not in enriched:
        enriched["article_links"] = []
    if "quant_links" not in enriched:
        enriched["quant_links"] = []

    # Add new article links (avoiding duplicates)
    existing_article = set(enriched["article_links"])
    for article_id in article_links:
        if article_id and article_id not in existing_article:
            enriched["article_links"].append(article_id)
            existing_article.add(article_id)

    # Add new quant links (avoiding duplicates)
    existing_quant = set(enriched["quant_links"])
    for quant_id in quant_links:
        if quant_id and quant_id not in existing_quant:
            enriched["quant_links"].append(quant_id)
            existing_quant.add(quant_id)

    # Update timestamp
    enriched["updated_at"] = datetime.now().isoformat()

    return enriched


def save_event_cluster(event: dict, event_dir: Path) -> None:
    """Save updated event cluster to data/events/{event_id}.json.

    Args:
        event: Event cluster dict
        event_dir: Path to events directory
    """
    event_id = event.get("event_id", "")
    if not event_id:
        return

    file_path = event_dir / f"{event_id}.json"
    save_json(file_path, event)


def enrich_event_clusters_from_links(links: list[dict], event_dir: Path) -> int:
    """
    Process links and enrich associated event clusters.

    For each link with an event_id, add the linked records to that event cluster.

    Args:
        links: List of article_quant links
        event_dir: Path to events directory

    Returns:
        Number of event clusters enriched
    """
    # Group links by event_id
    event_to_links: dict[str, dict] = {}

    for link in links:
        event_id = link.get("event_id")
        if not event_id:
            continue

        if event_id not in event_to_links:
            event_to_links[event_id] = {
                "article_ids": [],
                "quant_ids": [],
            }

        article_id = link.get("article_id", "")
        quant_id = link.get("quant_id", "")

        if article_id:
            event_to_links[event_id]["article_ids"].append(article_id)
        if quant_id:
            event_to_links[event_id]["quant_ids"].append(quant_id)

    # Enrich each event
    enriched_count = 0
    for event_id, link_data in event_to_links.items():
        event = load_event(event_id, event_dir)
        if event is None:
            continue

        enriched = enrich_event_cluster(
            event,
            article_links=link_data["article_ids"],
            quant_links=link_data["quant_ids"],
        )
        save_event_cluster(enriched, event_dir)
        enriched_count += 1

    return enriched_count


# =============================================================================
# Main Orchestration
# =============================================================================


def find_related_quants_for_article(
    article: dict,
    quants: list[dict],
    events: list[dict],
    config: dict,
    top_n: int = 3,
) -> list[dict]:
    """Find quant records related to a given article.

    Args:
        article: Article record dict
        quants: List of quant record dicts to search
        events: List of event cluster dicts
        config: Config dict
        top_n: Maximum number of related quants to return

    Returns:
        List of link dicts for top related quants
    """
    if not article or not quants:
        return []

    links = []

    for quant in quants:
        score, dimensions = compute_link_score(article, quant, events, config)
        relationship = classify_relationship(score, config)

        # Only create link if relationship is established
        if relationship is not None:
            event_id = None
            # Get event_id if there was event alignment
            for event in events:
                article_id = article.get("id", "")
                quant_id = quant.get("id", "")
                if article_id in event.get("record_ids", []) and quant_id in event.get(
                    "record_ids", []
                ):
                    event_id = event.get("event_id")
                    break

            link = create_link(
                article_id=article.get("id", ""),
                quant_id=quant.get("id", ""),
                event_id=event_id,
                relationship=relationship,
                score=score,
                matched_dimensions=dimensions,
            )
            links.append(link)

    # Sort by score descending and take top_n
    links.sort(key=lambda x: x["score"], reverse=True)
    return links[:top_n]


def find_related_articles_for_quant(
    quant: dict,
    articles: list[dict],
    events: list[dict],
    config: dict,
    top_n: int = 3,
) -> list[dict]:
    """Find article records related to a given quant.

    Args:
        quant: Quant record dict
        articles: List of article record dicts to search
        events: List of event cluster dicts
        config: Config dict
        top_n: Maximum number of related articles to return

    Returns:
        List of link dicts for top related articles
    """
    if not quant or not articles:
        return []

    links = []

    for article in articles:
        score, dimensions = compute_link_score(article, quant, events, config)
        relationship = classify_relationship(score, config)

        # Only create link if relationship is established
        if relationship is not None:
            event_id = None
            # Get event_id if there was event alignment
            for event in events:
                article_id = article.get("id", "")
                quant_id = quant.get("id", "")
                if article_id in event.get("record_ids", []) and quant_id in event.get(
                    "record_ids", []
                ):
                    event_id = event.get("event_id")
                    break

            link = create_link(
                article_id=article.get("id", ""),
                quant_id=quant.get("id", ""),
                event_id=event_id,
                relationship=relationship,
                score=score,
                matched_dimensions=dimensions,
            )
            links.append(link)

    # Sort by score descending and take top_n
    links.sort(key=lambda x: x["score"], reverse=True)
    return links[:top_n]


def link_all_records(
    articles: list[dict],
    quants: list[dict],
    events: list[dict],
    config: dict,
    output_dir: Path,
) -> dict:
    """Run bidirectional linking across all records.

    Creates links in both directions (article->quant and quant->article).
    Each direction is independent and may produce different links.

    Args:
        articles: List of article record dicts
        quants: List of quant record dicts
        events: List of event cluster dicts
        config: Config dict
        output_dir: Directory to write link JSON files

    Returns:
        Stats dict with counts of links created
    """
    stats = {
        "total_articles": len(articles),
        "total_quants": len(quants),
        "links_created": 0,
    }

    # Load existing links for deduplication
    existing_links = load_event_links(output_dir)

    # Article -> Quant links
    article_to_quant_count = 0
    for article in articles:
        related_quants = find_related_quants_for_article(
            article, quants, events, config, top_n=3
        )

        for link in related_quants:
            key = (link["article_id"], link["quant_id"])
            if key not in existing_links:
                save_link(link, output_dir)
                existing_links[key] = link
                article_to_quant_count += 1

    # Quant -> Article links
    quant_to_article_count = 0
    for quant in quants:
        related_articles = find_related_articles_for_quant(
            quant, articles, events, config, top_n=3
        )

        for link in related_articles:
            key = (link["article_id"], link["quant_id"])
            if key not in existing_links:
                save_link(link, output_dir)
                existing_links[key] = link
                quant_to_article_count += 1

    stats["links_created"] = article_to_quant_count + quant_to_article_count

    return stats


def run_enrichment(links: list[dict], base_dir: Path) -> dict:
    """
    Run enrichment on accepted records and event clusters.

    Args:
        links: List of link dicts
        base_dir: Base directory

    Returns:
        Stats dict with enrichment counts
    """
    stats = {
        "records_enriched": 0,
        "event_clusters_enriched": 0,
    }

    accepted_dir = base_dir / "data" / "accepted"
    events_dir = base_dir / "data" / "events"

    # Group links by article_id and quant_id for enrichment
    article_to_quants: dict[str, list[dict]] = {}
    quant_to_articles: dict[str, list[dict]] = {}

    for link in links:
        article_id = link.get("article_id", "")
        quant_id = link.get("quant_id", "")

        if article_id:
            if article_id not in article_to_quants:
                article_to_quants[article_id] = []
            article_to_quants[article_id].append(
                {
                    "record_id": quant_id,
                    "relationship": link.get("relationship", ""),
                    "link_score": link.get("score", 0),
                }
            )

        if quant_id:
            if quant_id not in quant_to_articles:
                quant_to_articles[quant_id] = []
            quant_to_articles[quant_id].append(
                {
                    "record_id": article_id,
                    "relationship": link.get("relationship", ""),
                    "link_score": link.get("score", 0),
                }
            )

    # Enrich articles with quant_context
    for article_id, related_links in article_to_quants.items():
        record_path = accepted_dir / f"{article_id}.json"
        if not record_path.exists():
            continue

        record = load_json(record_path)
        if record is None:
            continue

        # Only add quant_context if it's an article record
        if not is_quant_record(record):
            enriched = enrich_accepted_record(record, related_links, link_type="quant")
            write_enriched_record(enriched, base_dir)
            stats["records_enriched"] += 1

    # Enrich quants with article_context
    for quant_id, related_links in quant_to_articles.items():
        record_path = accepted_dir / f"{quant_id}.json"
        if not record_path.exists():
            continue

        record = load_json(record_path)
        if record is None:
            continue

        # Only add article_context if it's a quant record
        if is_quant_record(record):
            enriched = enrich_accepted_record(
                record, related_links, link_type="article"
            )
            write_enriched_record(enriched, base_dir)
            stats["records_enriched"] += 1

    # Enrich event clusters
    if events_dir.exists():
        stats["event_clusters_enriched"] = enrich_event_clusters_from_links(
            links, events_dir
        )

    return stats


def main() -> None:
    """CLI entry point.

    Usage:
        python scripts/link_article_quant.py          # Full pipeline with enrichment
        python scripts/link_article_quant.py --dry-run  # Preview without writing
        python scripts/link_article_quant.py --no-enrich  # Skip enrichment (links only)
    """
    import argparse

    parser = argparse.ArgumentParser(description="Link articles to quant records")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview linking without writing files",
    )
    parser.add_argument(
        "--no-enrich",
        action="store_true",
        help="Skip enrichment step (links only)",
    )
    args = parser.parse_args()

    # Load config
    config = load_config(CONFIG_PATH)

    # Load records
    print("Loading accepted records...")
    articles, quants = load_accepted_records(BASE_DIR)

    print(f"Found {len(articles)} article records")
    print(f"Found {len(quants)} quant records")

    if not articles and not quants:
        print("No records found. Exiting.")
        return

    # Load events
    print("Loading event clusters...")
    events_dir = BASE_DIR / "data" / "events"
    events = load_events(events_dir)
    print(f"Found {len(events)} event clusters")

    # Create output directory
    output_dir = OUTPUT_DIR
    if args.dry_run:
        print("\n[DRY RUN] Would create links in:", output_dir)
        output_dir = Path(tempfile.gettempdir()) / "article_quant_links_dryrun"
        output_dir.mkdir(parents=True, exist_ok=True)

    # Run linking
    print("\nRunning article-quant linking...")
    stats = link_all_records(articles, quants, events, config, output_dir)

    print("\nLinking complete!")
    print(f"  Articles processed: {stats['total_articles']}")
    print(f"  Quants processed: {stats['total_quants']}")
    print(f"  Total links created: {stats['links_created']}")

    # Run enrichment unless --no-enrich is set
    if not args.no_enrich and not args.dry_run:
        print("\nRunning enrichment...")
        links = load_links(output_dir)
        enrichment_stats = run_enrichment(links, BASE_DIR)

        print("Enrichment complete!")
        print(f"  Records enriched: {enrichment_stats['records_enriched']}")
        print(
            f"  Event clusters enriched: {enrichment_stats['event_clusters_enriched']}"
        )
    elif args.no_enrich:
        print("\n[SKIP] Enrichment skipped (--no-enrich flag)")
    elif args.dry_run:
        print("\n[DRY RUN] Would run enrichment after saving links")
        print("  - Would enrich accepted records with quant_context/article_context")
        print("  - Would enrich event clusters with article_links and quant_links")

    if args.dry_run:
        print(f"\n[DRY RUN] Links would be saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
