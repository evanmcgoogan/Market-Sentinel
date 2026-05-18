# Update Schema

The Update is the load-bearing primitive of the Stream. It is the contract between
SCORE and SERVE, and between backend and frontend. Get this right.

This document is the source of truth for the schema. Any change to it must be approved
explicitly (escalate to operator).

---

## File Layout

Updates are stored as JSON files, one per update:

```
updates/YYYY-MM-DD/{update-id}.json
```

The directory layout mirrors `raw/` — append-only except for the `user_state` field, which
is mutable (read/dismissed/promoted transitions).

Update IDs are UUIDv4. The day directory comes from `created_at`.

---

## JSON Schema

```json
{
  "update_id": "uuid-v4",
  "type": "convergence | contradiction | thesis_pressure | entity_shift | prediction_resolved | anomaly | synthesis",
  "priority_tier": "inbox | feed | archive",
  "headline": "One sentence, present tense, leads with what changed.",
  "body": "One paragraph: why this matters to Evan specifically, written by Sonnet using wiki context. Names tickers, states direction, no hedging.",
  "affected_pages": ["[[Entity Name]]", "[[Thesis Title]]"],
  "affected_theses": ["ai-infrastructure-supercycle", "energy-abundance-supercycle"],
  "source_evidence": ["raw/transcripts/lex/2026-05-18--xxx.md", "raw/tweets/karpathy/2026-05-18.md"],
  "confidence_score": 78,
  "recommendation": null,
  "created_at": "2026-05-18T14:30:00Z",
  "expires_at": "2026-05-21T14:30:00Z",
  "actions": [
    {"label": "Dig in", "target": "wiki://Entity Name"},
    {"label": "Promote to thesis", "action": "create_thesis"},
    {"label": "Dismiss", "action": "dismiss"}
  ],
  "user_state": "unread"
}
```

### Field Definitions

| Field | Type | Required | Notes |
|---|---|---|---|
| `update_id` | UUIDv4 | yes | Immutable. |
| `type` | enum (7 values) | yes | See type definitions below. |
| `priority_tier` | enum | yes | See tier logic below. |
| `headline` | string ≤ 140 chars | yes | Present tense, what changed. No marketing language. |
| `body` | string ≤ 1000 chars | yes | Why this matters in Evan's context. Sonnet-written. |
| `affected_pages` | string[] | yes | Wikilink format. Empty array allowed. |
| `affected_theses` | string[] | yes | Slugs from `wiki/theses/`. Empty array allowed for non-thesis updates. |
| `source_evidence` | string[] | yes | Relative paths to `raw/` files. At least one required for evidence-based types. |
| `confidence_score` | int 0-100 | yes | Determines `priority_tier`. |
| `recommendation` | object \| null | optional | Full recommendation object (see `recommendation-schema.md`). Present when the update warrants an action; null otherwise. |
| `created_at` | ISO8601 | yes | Immutable. |
| `expires_at` | ISO8601 \| null | yes | See TTL map below. `null` for permanent updates. |
| `actions` | object[] | yes | UI actions. At minimum `dismiss`. |
| `user_state` | enum | yes | `unread \| read \| dismissed \| promoted`. Only mutable field. |

---

## The Seven Update Types

### 1. `convergence`
Three or more S/A-tier sources independently align on a claim within a rolling window.

**Trigger:** Score detects ≥3 distinct sources (different `source_type` × `handle/channel`) making compatible claims about the same entity or thesis within 7 days.

**Body shape:** "X, Y, and Z are all converging on [claim]. This pressures [thesis] in [direction]."

### 2. `contradiction`
Two or more sources disagree on something material to the wiki.

**Trigger:** Compile output flags conflicting claims about the same entity/thesis/event from sources of equal or higher tier.

**Body shape:** "[Source A] argues X. [Source B] argues opposite. The wiki cannot resolve. Implications for [thesis]: ..."

### 3. `thesis_pressure`
Material new evidence for or against an active thesis.

**Trigger:** New compile output adds ≥1 supporting or contradicting claim to an active thesis page, where the claim is high-confidence (score ≥ 60).

**Body shape:** "[Thesis] gained/lost evidence. Specifically: [claim]. Net effect on conviction: [delta]."

### 4. `entity_shift`
A watched person or company materially changes position, role, or stated view.

**Trigger:** Compile detects a meaningful position-change on a tracked entity. (E.g., "Karpathy now bullish on robotics", "Ackman exits MU", "Sacks joins government.")

**Body shape:** "[Entity] shifted from X to Y. Last position: [date]. Implications for [thesis]: ..."

### 5. `prediction_resolved`
A tracked prediction's resolution date has passed and we can grade it.

**Trigger:** Date-based check against predictions table in `wiki/recommendations/predictions.md`.

**Body shape:** "[Prediction] resolved [hit/miss/partial]. Predicted: X. Actual: Y. Brier contribution: ..."

### 6. `anomaly`
A pattern-break worth attention. Price move without news. Source silence on a major event. Volume shock. Cross-source sentiment divergence from price.

**Trigger:** Deterministic signal stack (ported from V1/V2) plus simple pattern-break detectors.

**Body shape:** "[Asset] moved X% with no S/A-tier source coverage. Either signal precedes news, or noise. Watch [specific indicator]."

### 7. `synthesis`
A scheduled synthesis brief just dropped. This is a wrapper, not a derived update.

**Trigger:** `synthesize.py` writes a brief; `score.py` wraps it as an update.

**Body shape:** "Intraday brief / daily wrap / weekly deep / monthly review available. Top three takeaways: ..."

---

## Priority Tier Logic (Deterministic)

```
inbox  ⇐ confidence_score ≥ 75 AND (touches active thesis OR convergence on watched entity)
feed   ⇐ confidence_score ≥ 40 (everything else above floor)
archive ⇐ confidence_score < 40 (kept for analysis, not surfaced)
```

Tier is computed at emit time and is immutable. The operator can promote/dismiss but
cannot mutate the tier.

Push notifications fire **only** for `inbox` tier. Everything else accumulates.

---

## Type → TTL Map (Expiration)

Different update types have different natural lifespans. The map is locked here so all
updates expire predictably.

| Type | TTL | Rationale |
|---|---|---|
| `convergence` | 14 days | Convergence claims age but stay relevant for context. |
| `contradiction` | 30 days | Contradictions are slow to resolve and worth keeping visible. |
| `thesis_pressure` | 14 days | Thesis state updates compound; recent ones matter most. |
| `entity_shift` | 30 days | Position changes are durable signal. |
| `prediction_resolved` | `null` (never expires) | Historical track record — permanent record. |
| `anomaly` | 72 hours | Anomalies are fast-moving by nature. |
| `synthesis` | 7 days (intraday) / 30 days (daily) / 90 days (weekly) / `null` (monthly) | Mirrors the supersession hierarchy. |

After `expires_at` passes, the update is **auto-archived** — moved to `priority_tier: archive`
but the file is not deleted. Archived updates can be queried but don't appear in the active
Stream. Permanent updates (`expires_at: null`) never archive.

---

## Recommendation Object (Embedded)

When an update warrants a tradeable action, it embeds a recommendation object. The full
schema lives in `agent_docs/recommendation-schema.md`. The embedded object is the same
schema, optionally trimmed.

```json
"recommendation": {
  "action": "buy",
  "instrument": "MU",
  "instrument_type": "equity",
  "direction": "long",
  "size_pct": 1.5,
  "conviction": 78,
  "horizon": "medium",
  "kill_conditions": ["HBM ASP drops below $X", "thesis falsifier triggered"],
  "thesis_anchors": ["ai-infrastructure-supercycle"],
  "expected_value": {"upside_pct": 35, "downside_pct": 12, "probability_up": 0.62}
}
```

The Action layer (future) consumes the embedded recommendation. For now, the operator
reads it and decides.

---

## Hard Architectural Rules

1. **Updates are append-only.** Once written, never modified except `user_state`.
2. **Tier is deterministic, body is LLM.** The "why this matters" body is Sonnet-written. Everything else (tier, evidence, expiration) is computed by code.
3. **Every update cites at least one raw file** (except `prediction_resolved`, which cites the prediction's original source).
4. **No update without an affected thesis or convergence target.** If the system can't articulate which thesis a signal touches, it's not worth surfacing.
5. **Recommend or watch, never hedge.** If conviction is below 40, emit nothing. If 40-74, emit feed-tier. If ≥75 with thesis match, emit inbox with recommendation.
6. **No `expires_at: null` except where the TTL map allows it.** Don't introduce permanent updates by accident.

---

## Future Extensions (Not Yet Decided)

These are flagged for future operator review, not committed to the schema today.

- **`tag` field for user-defined categorization** (e.g., "personal", "macro", "sector-specific"). Useful when the Stream gets dense.
- **`linked_updates` field for chains** (e.g., a `convergence` update that builds on three earlier `thesis_pressure` updates). Useful for tracking how a thesis develops.
- **`brier_score` field on `prediction_resolved` updates** for direct grading. Compute and surface.

---

## Versioning

This is v1.0. Any breaking schema change increments to v2.0 and triggers a migration of
existing updates. Additive changes (new optional fields) are backwards-compatible.
