# Synthesis Page Template

Use this template for deep analyses produced by Opus — monthly macro syntheses, thesis stress tests, weak signal reports, source performance reviews.

## Frontmatter

```yaml
---
title: "YYYY-MM-DD Synthesis Description"
type: synthesis
subtype: weekly-macro                    # weekly-macro | monthly-macro | thesis-stress-test | weak-signal-report | source-review | connection-discovery
period_start: YYYY-MM-DD
period_end: YYYY-MM-DD
model: opus-4.6                          # Which model generated this
themes_covered:
  - "[[Theme Name]]"
theses_covered:
  - "[[Thesis Name]]"
sources_referenced: 15                   # Count of distinct raw sources cited
wiki_pages_referenced: 23                # Count of wiki pages consulted
key_findings: 3                          # Count of notable findings
created: YYYY-MM-DD
updated: YYYY-MM-DD
status: current                          # current | superseded | archived
superseded_by: null                      # Path to newer synthesis if superseded
tags: [tag1, tag2]
---
```

## Body Sections

```markdown
## Executive Summary

[3-5 bullet points: the most important things that changed or emerged this period.]

## Theme Updates

[For each active theme, what changed during this period. Link to theme pages.]

### [[Theme Name]]

- Direction change: [if any]
- New evidence: [summary with raw citations]
- Confidence shift: [old] → [new]

## Thesis Health

[For each active thesis, current status and stress test.]

### [[Thesis Name]]

- Current confidence: X.XX
- Strongest counter-argument: [steelman the bear case]
- Approaching catalysts: [list with dates]
- Recommendation: [hold / increase conviction / decrease conviction / invalidate]

## Weak Signals

[Patterns emerging across sources that haven't become themes yet. These are the early-stage observations worth watching.]

1. [Pattern description] — Seen in: [list of sources/raw citations]
2. [Pattern description] — Seen in: [list of sources/raw citations]

## Source Performance

[Which sources predicted well this period? Which generated noise?]

| Source | Predictions Resolved | Hit Rate | Notable Calls |
|--------|---------------------|----------|---------------|

## Cross-Domain Connections

[Links between themes/entities/signals that the incremental pipeline might miss. This is where Opus adds value over Sonnet.]

1. [Connection description] — Links: [[Page A]], [[Page B]]

## Changelog

- YYYY-MM-DD: Synthesis generated for period [start] to [end]
```
