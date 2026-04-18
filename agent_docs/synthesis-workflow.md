# Synthesis Workflow

How the brain reframes its world model. Synthesis is the layer that turns accumulated intelligence into actionable perspective. It runs **multiple times per day** — this is not a batch report, it's a continuously updating lens.

## Design Principles

- **Recency dominates.** An intraday brief that's 6 hours old is already stale. The system must synthesize frequently enough that the human operator never has to mentally reconstruct what changed since the last synthesis.
- **Synthesis is cheap, ignorance is expensive.** Sonnet intraday synthesis costs ~$0.25/run. Missing a time-sensitive signal (Iran escalation, surprise Fed action, whale liquidation) costs infinitely more.
- **Hierarchy supersedes.** Daily wraps supersede intraday briefs. Weekly deeps supersede daily wraps. Monthly reviews supersede weekly deeps. Only the most recent non-superseded synthesis at each level is authoritative.
- **Event-driven beats scheduled.** When high_signal extractions cluster (3+ within 2 hours), trigger an immediate synthesis regardless of schedule.

## Synthesis Frequency

| Mode | Intraday Briefs | Daily Wrap | Trigger |
|------|----------------|------------|---------|
| Active (market hours) | Every 4 hours: 10:00, 14:00, 18:00 ET | 21:00 ET | 3+ high_signal in 2hr window |
| Watch (after-hours) | Once at 20:00 ET | 21:00 ET | 3+ high_signal in 2hr window |
| Sleep (overnight) | Skip | Skip | 3+ high_signal in 2hr window |

Weekly deep: Sunday 04:00 ET. Monthly review (Opus): 1st Sunday 05:00 ET.

## Intraday Brief Flow

### 1. Gather New Intelligence

- Read `extractions/.extracted` to find all extractions since the last synthesis timestamp
- Load each extraction JSON
- Group by theme and thesis affinity

### 2. Load Wiki Context

- Read all wiki pages referenced in the new extractions
- Read all active thesis pages (these are always relevant)
- Read the most recent prior synthesis (for continuity)

### 3. Generate Brief (Sonnet)

Prompt structure:
1. **System**: CLAUDE.md schema (cached)
2. **System**: Synthesis template — intraday brief section (cached)
3. **System**: Active theses list with current confidence values (cached per-session)
4. **User**: New extractions since last synthesis (JSON array)
5. **User**: Current state of affected wiki pages
6. **User**: Prior synthesis (for continuity — "what did we think 4 hours ago?")
7. **Assistant**: Structured intraday brief

### 4. Save and Track

- Write synthesis to `wiki/syntheses/YYYY-MM-DD--HHMM-intraday-brief.md`
- Add to `index.md`
- Log: `[timestamp] SYNTHESIZED intraday-brief | extractions: N | high_signal: M | themes: [list]`
- Record synthesis timestamp for next run's "since" boundary

## Daily Wrap Flow

Same as intraday, but:
- Covers the full day's extractions (not just since last brief)
- Reads all intraday briefs from the day for continuity
- Marks the day's intraday briefs as `status: superseded`
- More comprehensive thesis health assessment

## Event-Driven Synthesis

Triggered by the extraction pipeline when it detects a burst:
- 3+ `high_signal` extractions within a 2-hour rolling window
- Uses the same intraday brief flow but with `subtype: event-driven`
- Does NOT supersede scheduled intraday briefs (it's additive)
- Naming: `YYYY-MM-DD--HHMM-event-driven.md`

## Weekly Deep Flow

- Reads all daily wraps from the week
- Marks daily wraps as `superseded` (but preserves for audit trail)
- Performs theme trajectory analysis (confidence over time)
- Generates source performance summary
- Identifies weak signals: patterns appearing across 2+ sources that haven't become themes

## Monthly Review Flow (Opus)

- Reads all weekly deeps from the month
- Full thesis stress test: steelman every active counter-case
- Source accuracy audit: resolve pending predictions, update hit rates
- Cross-domain connection discovery (Opus's key value-add)
- Blind spot analysis: what's under-covered, what assumptions are untested?
- Structural recommendations: tier changes, threshold adjustments, new themes

## Cost Structure

| Component | Frequency | Per-Run Cost | Monthly Cost |
|-----------|-----------|-------------|-------------|
| Intraday brief (Sonnet) | ~60-90/month | ~$0.25 | ~$15-22 |
| Daily wrap (Sonnet) | ~30/month | ~$0.40 | ~$12 |
| Weekly deep (Sonnet) | ~4/month | ~$0.60 | ~$2.50 |
| Monthly review (Opus) | 1/month | ~$5.00 | ~$5 |
| Event-driven (Sonnet) | ~5-10/month | ~$0.25 | ~$1.25-2.50 |
| **Total synthesis** | | | **~$35-44/month** |

This is the cost of having a continuously current world model. It's a fraction of what a human analyst would cost and ensures the operator never has to reconstruct context manually.

## Skip Conditions

A synthesis run is skipped (no output, no cost) when:
- Zero new extractions since the last synthesis at that frequency level
- All new extractions are `noise` verdict (no signal to synthesize)
- The system is in Sleep mode (intraday briefs only; event-driven still fires)
