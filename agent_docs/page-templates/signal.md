# Signal Page Template

Use this template for discrete, time-bound events worth tracking. Signals are the atomic unit of new information.

## Frontmatter

```yaml
---
title: "YYYY-MM-DD Signal Description"
type: signal
signal_type: market-move                 # market-move | whale-flow | source-claim | data-release | policy-action | technical-breakout
detected_at: YYYY-MM-DDTHH:MM:SSZ
source_file: raw/path/to/source.md
severity: high                           # high | medium | low
confidence: high                         # high | medium | low
affected_assets: [TICKER1, TICKER2]
affected_theses:
  - "[[Thesis Name]]"
affected_themes:
  - "[[Theme Name]]"
entities_involved:
  - "[[Entity Name]]"
status: active                           # active | resolved | superseded | noise
resolution: null                         # confirmed | invalidated | inconclusive (set when resolved)
resolved_at: null                        # YYYY-MM-DD (set when resolved)
created: YYYY-MM-DD
updated: YYYY-MM-DD
tags: [tag1, tag2]
---
```

## Body Sections

```markdown
## What Happened

[Factual description of the signal. What moved, by how much, when.]

## Why It Matters

[Connection to themes, theses, and entities. Why this signal is worth tracking.]

## Context

[What was happening around this signal? Other signals, news, market conditions.]

## Implications

[What this signal suggests about affected theses and themes.]

- For [[Thesis]]: [implication]
- For [[Theme]]: [implication]

## Follow-Up

[What to watch next. What would confirm or invalidate the signal's importance.]

- [ ] Watch for: [follow-up event]
- [ ] Check: [data point or source]

## Resolution

[Filled in when the signal's significance becomes clear.]

- Outcome: [what actually happened]
- Was the signal meaningful? yes | no | partially
- Lessons: [what this teaches about signal detection]

## Changelog

- YYYY-MM-DD: Signal detected from [source]
```
