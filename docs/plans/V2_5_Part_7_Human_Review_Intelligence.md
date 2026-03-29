# V2.5 Part 7 — Human Review Intelligence

## Objective

Upgrade the human review layer from a binary approve/reject system into a richer feedback channel that teaches the archive how to improve.

This part turns human review into a stronger supervision signal.

---

## Why this part matters

Right now, approve/reject is useful, but it throws away a lot of information.

A reviewer may think:

- this is good, but weak
- this is good and important
- this source is bad
- this topic deserves expansion
- this item is acceptable but not archive-worthy

V2.5 should capture those distinctions.

---

## Scope

This part includes:

- richer review actions
- feedback capture
- storage of human feedback metadata
- integration into memory/scoring/theming

---

## Suggested review actions

Instead of only:

- approve
- reject

Add options such as:

- approve
- reject
- approve_but_weak
- approve_and_promote
- bad_source
- good_source
- expand_this_topic
- suppress_similar_items

These can be implemented gradually.

---

## Feedback schema

Suggested block:

```json
{
  "human_feedback": {
    "decision": "approve_and_promote",
    "notes": "High-value macro signal with useful rates context.",
    "source_feedback": "good_source",
    "topic_feedback": "expand_this_topic",
    "reviewed_at": "ISO-8601"
  }
}
```

---

## Files to add

Suggested files:

```text
scripts/
  apply_human_feedback.py
  update_feedback_memory.py
schemas/
  human_feedback.json
```

---

## Integration points

Human review feedback should feed into:

- source/domain memory
- theme memory
- scoring adjustments
- tier assignment

Examples:

- `bad_source` lowers source trust faster
- `good_source` raises source trust faster
- `expand_this_topic` promotes theme expansion
- `approve_and_promote` may push the record toward tier_1

---

## Telegram UX implications

This part may eventually require:

- more Telegram buttons
- follow-up prompts
- message editing
- optional lightweight review notes

You do not have to implement all of that at once.

A staged approach is better.

---

## Acceptance criteria

This part is complete when:

- review decisions can carry richer feedback than binary approve/reject
- feedback is stored in structured form
- at least some feedback types update memory or scoring behavior
- review becomes a true learning signal for the archive

---

## Recommended implementation order

1. define human feedback schema
2. add 1–2 richer feedback actions beyond approve/reject
3. store feedback metadata
4. route feedback into source/theme memory
5. later expand UI options and automation behavior

---

## Output of this part

After this part, human review becomes not just a safety gate but a training signal for the archive.
