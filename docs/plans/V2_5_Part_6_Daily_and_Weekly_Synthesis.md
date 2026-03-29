# V2.5 Part 6 — Daily and Weekly Synthesis Layer

## Objective

Generate higher-level summaries from accepted and reviewed records so the archive produces not just individual entries, but reusable synthesized knowledge.

This part adds:

- daily macro digests
- daily market structure digests
- weekly synthesis summaries

---

## Why this part matters

Without synthesis:

- the archive stores many records
- but humans and future RAG have to reconstruct the bigger picture manually

With synthesis:

- the archive accumulates usable higher-level context
- event clusters become easier to retrieve
- daily/weekly market narratives become available as first-class records

---

## Scope

This part includes:

- daily digest generation
- weekly digest generation
- digest record storage
- digest metadata and links back to contributing records

---

## Digest types

### Daily macro digest

Summarizes:

- inflation-related developments
- labor / growth developments
- central bank communication
- rate / curve shifts when linked quant exists

### Daily market structure digest

Summarizes:

- liquidity
- repo / funding
- Treasury issuance
- exchange / clearing / market plumbing developments

### Weekly synthesis

Summarizes:

- major themes
- important accepted records
- key review outcomes
- most relevant quant shifts

---

## Files to add

Suggested files:

```text
scripts/
  build_daily_macro_digest.py
  build_daily_market_structure_digest.py
  build_weekly_digest.py
schemas/
  digest_record.json
data/
  digests/
```

---

## Digest record schema

Suggested fields:

```json
{
  "digest_id": "daily_macro_2026_04_01",
  "digest_type": "daily_macro",
  "date_range": {
    "start": "2026-04-01",
    "end": "2026-04-01"
  },
  "summary": "...",
  "key_themes": ["..."],
  "linked_record_ids": ["..."],
  "linked_quant_ids": ["..."],
  "confidence": 0,
  "notes": "..."
}
```

---

## Build flow

### Daily digests

```text
collect accepted records for day
-> group by theme/topic
-> optionally include linked quant context
-> generate digest
-> save digest record
```

### Weekly digests

```text
collect accepted + reviewed records for week
-> group by themes and significance
-> summarize major developments
-> save digest record
```

---

## AI role

MiniMax can be used here for high-level synthesis, but only after the archive has already filtered and verified the underlying records.

That makes digest generation much safer than open-ended summarization from raw sources.

---

## Acceptance criteria

This part is complete when:

- daily digests are generated from accepted records
- weekly digests summarize major developments
- digests link back to underlying archive records
- digests are stored as first-class archive artifacts

---

## Recommended implementation order

1. define digest schema
2. build daily macro digest
3. build daily market structure digest
4. build weekly digest
5. later add Telegram or email delivery if desired

---

## Output of this part

After this part, the archive starts producing knowledge products, not just storing research fragments.
