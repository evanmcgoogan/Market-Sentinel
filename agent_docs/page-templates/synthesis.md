# Synthesis Page Template

Multi-frequency synthesis keeps the brain's world model current. Intraday briefs use Sonnet for speed and cost efficiency. Monthly deep reviews use Opus for structural insight.

## Synthesis Hierarchy

| Subtype | Frequency | Model | Supersedes | Purpose |
|---------|-----------|-------|------------|---------|
| `intraday-brief` | 2-3x/day (Active/Watch) | Sonnet | nothing | Reframe all new intelligence since last brief |
| `daily-wrap` | 1x/day at 21:00 ET | Sonnet | day's intraday briefs | Coherent end-of-day narrative |
| `weekly-deep` | Sunday 04:00 ET | Sonnet | week's daily wraps | Theme trajectories, thesis health, source patterns |
| `monthly-review` | 1st Sunday 05:00 ET | Opus | month's weekly deeps | Cross-domain connections, structural blind spots |
| `event-driven` | on high_signal burst | Sonnet | nothing | Rapid reframe when 3+ high_signal in 2 hours |
| `thesis-stress-test` | on demand | Sonnet/Opus | nothing | Steelman attack on a specific thesis |

## Frontmatter

```yaml
---
title: "YYYY-MM-DD HH:MM Intraday Brief"  # or "YYYY-MM-DD Daily Wrap", etc.
type: synthesis
subtype: intraday-brief                     # see hierarchy above
period_start: 2026-04-06T10:00:00Z          # ISO 8601 — use datetime for intraday, date for daily+
period_end: 2026-04-06T14:00:00Z
model: sonnet-4.6                           # sonnet for intraday/daily/weekly, opus for monthly
extraction_count: 12                        # extractions synthesized in this run
high_signal_count: 3                        # high_signal extractions in this run
themes_covered:
  - "[[Theme Name]]"
theses_covered:
  - "[[Thesis Name]]"
sources_referenced: 8
wiki_pages_referenced: 15
key_findings: 3
created: 2026-04-06
updated: 2026-04-06
status: current                             # current | superseded | archived
supersedes: []                              # paths to syntheses this one replaces
superseded_by: null                         # path to newer synthesis
tags: [synthesis, intraday]
---
```

## Intraday Brief Body

```markdown
## What Changed

[3-5 bullet points: the most important new intelligence since the last brief. Lead with what matters for active theses and positions.]

## Active Thesis Impact

[For each thesis affected by new intelligence, one-line impact assessment.]

- **[[Thesis Name]]**: [direction] — [one sentence why, citing raw source]

## Developing Situations

[Fast-moving events that need monitoring. Time-sensitive context.]

1. [Situation] — Last update: [raw source], [time]. Next catalyst: [what to watch].

## New Signals

[High-signal extractions that don't fit existing theses. Potential new positions or themes.]

## Contradictions Surfaced

[Any new contradictions between sources or with existing wiki claims.]
```

## Daily Wrap Body

```markdown
## Executive Summary

[3-5 bullet points: the day's most important developments and what they mean for the portfolio.]

## Theme Updates

### [[Theme Name]]

- Direction change: [if any]
- New evidence: [summary with raw citations]
- Confidence shift: [old] → [new]

## Thesis Health

### [[Thesis Name]]

- Today's impact: [positive / negative / neutral]
- New supporting evidence: [if any]
- New counter-evidence: [if any]
- Confidence: [current value]
- Action required: [none / review / reduce / exit]

## New Signals Worth Watching

[Signals that emerged today but need more data before becoming actionable.]

## Source Highlights

[Which sources produced the most valuable intelligence today?]

## Changelog

- YYYY-MM-DD: Daily wrap generated covering [extraction_count] extractions
```

## Weekly Deep Body

```markdown
## Executive Summary

[5-7 bullet points: the week's macro narrative and key shifts.]

## Theme Trajectories

[For each active theme: week-over-week direction, evidence weight, and confidence movement.]

## Thesis Scorecard

| Thesis | Start Confidence | End Confidence | Key Events | Action |
|--------|-----------------|----------------|------------|--------|

## Source Performance

| Source | Extractions | High Signal | Notable Calls |
|--------|------------|-------------|---------------|

## Weak Signals

[Patterns emerging across sources that haven't become themes yet.]

## Changelog

- YYYY-MM-DD: Weekly deep generated for period [start] to [end]
```

## Monthly Review Body (Opus)

```markdown
## Strategic Overview

[Opus-level structural analysis. What patterns did the incremental pipeline miss? What connections exist across domains?]

## Cross-Domain Connections

[Links between themes/entities/signals that intraday synthesis can't see.]

1. [Connection description] — Links: [[Page A]], [[Page B]]

## Thesis Stress Tests

[For each active thesis: steelman the counter-case with full evidence.]

## Source Accuracy Audit

[30-day prediction resolution rates. Tier promotion/demotion candidates.]

## Blind Spot Analysis

[What topics are under-covered? What assumptions are untested?]

## Structural Recommendations

[Changes to thresholds, tiers, themes, or architecture suggested by the month's data.]

## Changelog

- YYYY-MM-DD: Monthly review generated for period [start] to [end]
```
