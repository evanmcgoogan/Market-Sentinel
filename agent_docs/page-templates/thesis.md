# Thesis Page Template

Use this template for active investment theses — structured beliefs about market direction with explicit entry/exit criteria and falsifiers.

## Frontmatter

```yaml
---
title: "Thesis Name"
type: thesis
status: active                           # active | invalidated | confirmed | dormant
confidence: 0.65                         # 0-1, current conviction level
confidence_history:
  - date: YYYY-MM-DD
    value: 0.52
    reason: "What changed confidence"
direction: long                          # long | short | neutral
primary_asset: TICKER
related_assets: [TICKER1, TICKER2]
entry_conditions:
  - "Condition that would trigger entry"
exit_conditions:
  - "Condition that would trigger exit"
position_sizing: "2-5% of portfolio"
max_exposure: "5%"
invalidation_level: "Specific price or condition"
falsifiers:
  - "Condition that would kill this thesis"
supporting_signals: []                   # Populated by signal layer
contradicting_signals: []                # Populated by signal layer
key_sources:
  - "[[Source Name]]"
affected_themes:
  - "[[Theme Name]]"
created: YYYY-MM-DD
updated: YYYY-MM-DD
update_count: 1
tags: [tag1, tag2]
---
```

## Body Sections

```markdown
## Thesis Statement

[One paragraph: what you believe, why, and over what timeframe.]

## Evidence Chain

[Numbered evidence supporting the thesis. Every item cites a raw source.]

1. YYYY-MM-DD: [Evidence] (raw/path/to/source.md)
2. YYYY-MM-DD: [Evidence] (raw/path/to/source.md)

## Counter-Evidence

[What argues against this thesis. Honest accounting.]

1. YYYY-MM-DD: [Counter-evidence] (raw/path/to/source.md)

## Active Signals

[Recent signals that affect this thesis — populated by the signal layer.]

| Date | Signal | Impact | Link |
|------|--------|--------|------|
| YYYY-MM-DD | Description | supporting/contradicting | [[Signal Page]] |

## Decision Log

[What you've done based on this thesis and why.]

- YYYY-MM-DD: [Action taken] — Reason: [why]

## Recommended Action

[LLM-generated recommendation based on current evidence and confidence. You decide whether to act.]

## Changelog

- YYYY-MM-DD: Thesis created. Initial confidence: X.XX
```
