# Entity Page Template: Person

Use this template when creating a wiki page for a person.

## Frontmatter

```yaml
---
title: "Full Name"
type: entity
subtype: person
roles: [role1, role2]                    # e.g., ai-researcher, founder, fund-manager
affiliations: [org1, org2]               # Current and notable past
domains: [domain1, domain2]              # e.g., ai, macro, semiconductors, geopolitics
source_tier: S                           # S | A | B | C (if they are a tracked source)
signal_weight: 0.5                       # 0-1, learned from prediction outcomes
created: YYYY-MM-DD
updated: YYYY-MM-DD
update_count: 1
confidence: high                         # high | medium | low | conflicting
status: active                           # active | stale | superseded | archived
last_mentioned_in: raw/path/to/source.md
related:
  - "[[Related Entity]]"
  - "[[Related Theme]]"
tags: [tag1, tag2]
---
```

## Body Sections

```markdown
## Current Position & Context

[Who this person is. Why they matter to your worldview. 2-4 sentences max.]

## Recent Activity

[Last 7 days of notable output. Each entry cites a raw source.]

- YYYY-MM-DD: [Summary of activity] (raw/path/to/source.md)

## Key Claims & Predictions

[Falsifiable claims this person has made. Each entry has a date, the claim, and a citation.]

- YYYY-MM-DD: "Quoted or paraphrased claim" (raw/path/to/source.md)
  - Resolution: pending | confirmed | invalidated
  - Resolution date: YYYY-MM-DD (expected or actual)

## Track Record

[How accurate have their past predictions been? Updated as claims resolve.]

- Predictions tracked: N
- Predictions resolved: N
- Hit rate: N/A (populate after 10+ resolved)
- Notable hits: [list]
- Notable misses: [list]

## Connections

[How this entity relates to your theses, themes, and other entities.]

- Relevant theses: [[Thesis 1]], [[Thesis 2]]
- Key relationships: [[Person]], [[Company]]
- Domain overlap with: [[Source Profile]]

## Changelog

- YYYY-MM-DD: Page created from [source description]
```
