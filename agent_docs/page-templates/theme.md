# Theme Page Template

Use this template for macro trends, narratives, and market regimes.

## Frontmatter

```yaml
---
title: "Theme Name"
type: theme
subtype: macro-trend                     # macro-trend | sector-rotation | regulatory | geopolitical | technological
status: active                           # active | stale | superseded | archived
direction: accelerating                  # accelerating | stable | decelerating | reversing
confidence: high                         # high | medium | low | conflicting
created: YYYY-MM-DD
first_identified: YYYY-MM-DD
updated: YYYY-MM-DD
update_count: 1
affected_assets: [TICKER1, TICKER2]
affected_theses:
  - "[[Thesis Name]]"
key_sources:
  - "[[Source Name]]"
bull_case_confidence: 0.75               # 0-1
bear_case_confidence: 0.25               # 0-1 (should sum to ~1.0 with bull)
falsifiers:
  - "Condition that would invalidate this theme"
next_catalysts:
  - event: "Event description"
    date: YYYY-MM-DD
    expected_impact: high                # high | medium | low
related:
  - "[[Related Theme]]"
tags: [tag1, tag2]
---
```

## Body Sections

```markdown
## Narrative Summary

[What this theme is. Why it matters. How it connects to your investment worldview. 3-5 sentences.]

## Evidence For (Bull Case)

[Structured evidence supporting this theme. Every item cites a raw source.]

1. [Evidence point] (raw/path/to/source.md)
2. [Evidence point] (raw/path/to/source.md)

## Evidence Against (Bear Case)

[Structured counter-evidence. Every item cites a raw source.]

1. [Counter-evidence point] (raw/path/to/source.md)
2. [Counter-evidence point] (raw/path/to/source.md)

## Key Debates

[Where do smart sources disagree? Link to contradiction pages.]

- [[Contradiction Page]]: [Summary of disagreement]

## Timeline

[Chronological record of how this theme has evolved.]

- YYYY-MM-DD: [Event/development] (raw/path/to/source.md)

## Affected Assets

[Which instruments move with this theme and in which direction.]

| Asset | Direction | Sensitivity | Notes |
|-------|-----------|-------------|-------|
| TICKER | long/short | high/medium/low | Context |

## Related Themes

[Cross-references to themes that interact with this one.]

- [[Theme]]: [How they interact]

## Changelog

- YYYY-MM-DD: Page created from [source description]
```
