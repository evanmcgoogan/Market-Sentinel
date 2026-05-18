# Recommendation Schema

A recommendation is the system's "what's next" output. It is the input the eventual
agentic trading layer will consume. It must contain enough structure for an agent to
execute, with the operator's explicit go-ahead in the loop.

The recommendation appears in two places:

1. **Embedded in an Update** (`updates/.../update-id.json` → `recommendation` field) when an Update warrants action.
2. **Standalone in the wiki** (`wiki/recommendations/{date}/{rec-id}.md`) as the durable, grade-able record.

Both forms use the same schema. The standalone wiki page includes additional grading
fields populated over time.

---

## JSON Schema (embedded form)

```json
{
  "rec_id": "uuid-v4",
  "action": "buy | sell | short | trim | hold | avoid | watch | close",
  "instrument": "MU",
  "instrument_type": "equity | etf | option | prediction_market | crypto | bond | other",
  "market_id": null,
  "direction": "long | short | yes | no | n/a",
  "size_pct": 1.5,
  "size_note": "1.5% of total portfolio; max position 4%",
  "conviction": 78,
  "horizon": "short | medium | long",
  "horizon_window": "3-18 months",
  "entry_zone": "$95-$105 or current",
  "expected_value": {
    "upside_pct": 35,
    "downside_pct": 12,
    "probability_up": 0.62,
    "ev_pct": 16.6
  },
  "kill_conditions": [
    "HBM ASP drops below $X per Gb",
    "Hynix capex guidance cut by >15% in next quarterly",
    "TSM 3nm yield issue causes >20% MU revenue revision"
  ],
  "thesis_anchors": ["ai-infrastructure-supercycle"],
  "convergence_anchors": ["sovereignty-over-material-layer"],
  "evidence": [
    "raw/transcripts/dwarkesh/2026-05-15--hynix-cto.md",
    "raw/papers/cs-lg/2026-05-12--memory-scaling.md",
    "wiki/entities/companies/sk-hynix.md"
  ],
  "rationale_summary": "One paragraph: why this trade, why now, what we're betting on.",
  "created_at": "2026-05-18T14:30:00Z",
  "resolution_date": "2027-08-18",
  "status": "open"
}
```

### Field Definitions

| Field | Type | Required | Notes |
|---|---|---|---|
| `rec_id` | UUIDv4 | yes | Immutable. |
| `action` | enum | yes | What to do. `watch` is valid — sometimes the right move is no move. |
| `instrument` | string | yes | Ticker for equities/ETFs/options; descriptive label otherwise. |
| `instrument_type` | enum | yes | Determines how the Action layer dispatches. |
| `market_id` | string \| null | conditional | Required for `prediction_market`. Polymarket/Kalshi market identifier. |
| `direction` | enum | yes | `long`/`short` for equities; `yes`/`no` for prediction markets; `n/a` for `watch`/`hold`. |
| `size_pct` | float | yes | % of total portfolio for new position; % of existing position for `trim`/`close`. |
| `size_note` | string | yes | Human-readable size context. |
| `conviction` | int 0-100 | yes | Must be ≥75 to recommend action; below that, `action: watch`. |
| `horizon` | enum | yes | `short` < 3mo, `medium` 3-18mo, `long` > 18mo. |
| `horizon_window` | string | yes | Specific expected window. |
| `entry_zone` | string | yes | Where to enter. "Current" is acceptable. |
| `expected_value` | object | yes | Probability-weighted return scenarios. |
| `kill_conditions` | string[] ≥ 2 | yes | Explicit invalidators. Falsifiable, time-bound, observable. |
| `thesis_anchors` | string[] ≥ 1 | yes | Slugs from `wiki/theses/`. At least one. |
| `convergence_anchors` | string[] | optional | Slugs from `wiki/convergences/`. |
| `evidence` | string[] ≥ 1 | yes | Citations to `raw/` and/or `wiki/`. |
| `rationale_summary` | string ≤ 600 chars | yes | Plain-English why. |
| `created_at` | ISO8601 | yes | Immutable. |
| `resolution_date` | ISO8601 | yes | When the rec gets graded. |
| `status` | enum | yes | `open \| executed \| skipped \| invalidated \| resolved`. |

---

## Recommendation States (Lifecycle)

```
open ──┬─→ executed   (operator gave go-ahead, position taken)
       ├─→ skipped    (operator declined)
       └─→ invalidated (kill condition triggered before execution)

executed ──┬─→ resolved_hit     (target reached or thesis confirmed)
           ├─→ resolved_miss    (kill condition triggered post-entry)
           ├─→ resolved_partial (mixed outcome; size adjusted)
           └─→ closed_neutral   (closed without clear resolution)
```

Status transitions are written to `wiki/recommendations/{date}/{rec-id}.md` as a changelog.
The standalone wiki page is the durable record; the embedded JSON in the Update is a
snapshot.

---

## Wiki Page (Standalone form)

Each recommendation also gets a wiki page at `wiki/recommendations/{YYYY-MM-DD}/{rec-id}.md`.
The page contains the full JSON in frontmatter plus a body for grading and post-mortem.

```yaml
---
rec_id: "uuid-v4"
title: "Buy MU 1.5% — HBM convergence + thesis 1 pressure"
type: recommendation
action: buy
instrument: MU
instrument_type: equity
direction: long
size_pct: 1.5
conviction: 78
horizon: medium
horizon_window: "3-18 months"
status: open                                # open | executed | skipped | resolved_hit | resolved_miss | resolved_partial | closed_neutral
thesis_anchors: [ai-infrastructure-supercycle]
convergence_anchors: [sovereignty-over-material-layer]
kill_conditions:
  - "HBM ASP drops below $X per Gb"
  - "Hynix capex guidance cut by >15% in next quarterly"
expected_value:
  upside_pct: 35
  downside_pct: 12
  probability_up: 0.62
  ev_pct: 16.6
created: 2026-05-18
resolution_date: 2027-08-18
brier_score: null                           # Populated on resolution.
actual_return_pct: null                     # Populated on resolution.
notes_count: 0                              # Operator notes during the trade.
---
```

### Body Sections

```markdown
## Rationale

[One paragraph: why this trade, why now, what we're betting on.]

## Evidence

[Citations to raw/ and wiki/. Same as embedded form, but expanded with context.]

- raw/transcripts/dwarkesh/2026-05-15--hynix-cto.md — Hynix CTO confirms HBM4 ramp
- raw/papers/cs-lg/2026-05-12--memory-scaling.md — Memory scaling laws paper
- [[SK Hynix]] — current entity view

## Expected Value

| Scenario | Probability | Outcome | Contribution |
|---|---|---|---|
| Thesis plays out | 0.62 | +35% | +21.7% |
| Drift / flat | 0.26 | +5%  | +1.3%  |
| Kill condition  | 0.12 | -25% | -3.0%  |
| **EV**          |      |      | **+20.0%** |

## Kill Conditions (Explicit)

1. [Condition with observable trigger]
2. [Condition with observable trigger]
3. [Condition with observable trigger]

## Operator Decision Log

- YYYY-MM-DD: Recommendation generated.
- YYYY-MM-DD: Operator executed. Entry price: $X. Position size: N shares.
- YYYY-MM-DD: [Update / adjustment / partial close]

## Resolution / Post-mortem

[Filled in at resolution_date or earlier if kill condition triggers.]

- Outcome: hit | miss | partial
- Actual return: X%
- What we got right:
- What we got wrong:
- Lessons for future recommendations:
```

---

## Hard Rules

1. **No recommendation below conviction 75.** Below that, emit an Update with `action: watch` and no recommendation object. The system should never recommend action it doesn't believe in.
2. **Kill conditions are mandatory.** Minimum two, falsifiable, observable, time-bound. "If the thesis breaks" is not a kill condition; "if HBM ASP drops below $X per Gb in the next 6 months" is.
3. **Thesis anchor required.** Every recommendation ties to at least one of the seven theses. If it doesn't, the recommendation is off-charter and shouldn't be made.
4. **Size discipline.** `size_pct` is a recommendation, not a command. Default ceiling: 3% for new positions, 5% for thesis-stacked positions. The Action layer enforces these caps.
5. **No advice framing externally; no hedging internally.** Internal: "Buy MU 1.5%, conviction 78." External (if/when external surfaces exist): wrapped with operator-context framing at the boundary.
6. **EV must be probability-weighted.** A recommendation that doesn't include downside scenarios is not a recommendation, it's a hope.
7. **Grading is automatic where possible.** When `resolution_date` passes, the system auto-generates a `prediction_resolved` Update prompting the operator to fill in actual return. Brier score is computed from the (probability_up, hit/miss) pair.

---

## Recommendation vs Watch — When to Generate Each

The system should be biased toward `watch` actions, not `buy/sell` actions. Most signals
are not tradeable. The discipline is to emit recommendations only when conviction passes
the 75 bar AND there's a clear thesis anchor AND kill conditions can be specified
falsifiably.

If any of those three fail, the Update gets emitted without a recommendation object, with
`action: watch` in the headline. The operator sees the signal but isn't pushed toward a
trade.

This is the structural defense against the failure mode where the system manufactures
conviction to fill a daily quota.

---

## Versioning

This is v1.0. Same versioning rules as `update-schema.md`.
