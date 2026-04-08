# Phase 6 — BIS, IMF, and World Bank Publications Pipeline

**Depends on:** Phase 1 complete, Phase 5 recommended (establishes academic/institutional pattern)  
**Priority:** Medium — these institutions publish working papers, quarterly reviews, and data releases that are already in `sources.json` but not wired to actual ingest

---

## What This Phase Does

Wires up three major international financial institutions that are already acknowledged in the archive's source registry but are not actively ingested. All three have structured public feeds or REST endpoints requiring no API key.

---

## Source Details

### Bank for International Settlements (BIS)


| Publication type     | Feed URL                                                |
| -------------------- | ------------------------------------------------------- |
| Working Papers       | `https://www.bis.org/doclist/bis_fsi_publs.rss`         |
| Quarterly Review     | `https://www.bis.org/publ/qtrpdf/r_qt.rss`              |
| BIS Bulletins        | `https://www.bis.org/doclist/biswp.rss`                 |
| Statistical Releases | `https://www.bis.org/statistics/rss/bis_statistics.rss` |


All are standard RSS. No key required.

### International Monetary Fund (IMF)


| Publication type                  | Feed / URL                                     |
| --------------------------------- | ---------------------------------------------- |
| Working Papers                    | `https://www.imf.org/en/Publications/WP/rss`   |
| World Economic Outlook (WEO)      | `https://www.imf.org/en/Publications/WEO/rss`  |
| Global Financial Stability Report | `https://www.imf.org/en/Publications/GFSR/rss` |
| IMF Blog                          | `https://www.imf.org/en/Blogs/rss`             |


All standard RSS. No key required.

### World Bank


| Publication type        | Feed / URL                                                       |
| ----------------------- | ---------------------------------------------------------------- |
| Research & Publications | `https://www.worldbank.org/en/research/rss`                      |
| Data Blog               | `https://blogs.worldbank.org/en/rss.xml`                         |
| Open Data API           | `https://api.worldbank.org/v2/indicator/{INDICATOR}?format=json` |


RSS feeds are standard. The Open Data API is free but useful only for the quant pipeline (see note below).

---

## Implementation Plan

### Step 1 — Add to `config/rss_feeds.json`

These sources plug directly into the existing `ingest_rss.py` pipeline. Add entries:

```json
{ "name": "BIS Working Papers",        "url": "https://www.bis.org/doclist/biswp.rss",              "enabled": true, "topic": "central_bank_research" },
{ "name": "BIS Quarterly Review",      "url": "https://www.bis.org/publ/qtrpdf/r_qt.rss",           "enabled": true, "topic": "global_finance" },
{ "name": "IMF Working Papers",        "url": "https://www.imf.org/en/Publications/WP/rss",          "enabled": true, "topic": "macro_research" },
{ "name": "IMF World Economic Outlook","url": "https://www.imf.org/en/Publications/WEO/rss",         "enabled": true, "topic": "global_economy" },
{ "name": "IMF Global Fin Stability",  "url": "https://www.imf.org/en/Publications/GFSR/rss",        "enabled": true, "topic": "financial_stability" },
{ "name": "IMF Blog",                  "url": "https://www.imf.org/en/Blogs/rss",                    "enabled": true, "topic": "macro_commentary" },
{ "name": "World Bank Research",       "url": "https://www.worldbank.org/en/research/rss",            "enabled": true, "topic": "development_economics" }
```

No new script needed — `ingest_rss.py` handles these automatically.

### Step 2 — Add BIS/IMF as Ingestion Targets (Optional Depth Crawl)

For publications where the RSS only provides an abstract or title (common with IMF), add the publications index pages to `config/ingestion_targets.json` as trusted targets. This lets `ingest_sources.py` crawl the index pages for direct PDF/HTML links.

### Step 3 — World Bank Quant Data (Extend Quant Pipeline)

The World Bank Open Data API provides structured time-series for economic indicators (GDP growth, inflation, debt ratios, etc.) that complement FRED data. Add these as `dataset` entries in `config/quant_sources.json`:

```json
{
  "source": "worldbank",
  "indicator": "NY.GDP.MKTP.KD.ZG",
  "name": "GDP Growth Rate (World)",
  "enabled": true,
  "frequency": "annual"
}
```

Extend `ingest_quant_data.py` to handle a `worldbank` source type by fetching from `https://api.worldbank.org/v2/indicator/{indicator}?format=json&mrv=5` (most recent 5 values). This finally replaces some of the `build_dataset_snapshot` placeholder functions currently in the quant pipeline.

### Step 4 — Scoring for Institutional Sources

Add `"central_bank_research"` and `"multilateral_institution"` as trusted source tiers in `config/domain_trust_tiers.json` to ensure these sources score above the acceptance threshold without needing manual review.

---

## New GitHub Secrets Required

**None.** All BIS, IMF, and World Bank feeds and APIs are publicly accessible with no authentication.

---

## Files Changed in This Phase


| File                             | Change                                                      |
| -------------------------------- | ----------------------------------------------------------- |
| `config/rss_feeds.json`          | Add BIS, IMF, World Bank RSS entries                        |
| `config/ingestion_targets.json`  | Optionally add BIS/IMF publication index pages              |
| `config/quant_sources.json`      | Add World Bank indicator entries                            |
| `scripts/ingest_quant_data.py`   | Add `worldbank` source type handler                         |
| `config/domain_trust_tiers.json` | Add `bis.org`, `imf.org`, `worldbank.org` as tier-1 trusted |


---

## Acceptance Criteria

- At least 3 BIS or IMF records appear in `data/raw/` after the first pipeline run following config changes
- IMF and BIS entries score into `data/accepted/` without manual review (trust tier is set correctly)
- World Bank GDP and inflation indicators produce quant snapshots alongside FRED data
- No new secrets or credentials required at any point

