# Entity Page Template: Institution

Use this template for government bodies, central banks, regulators, international organizations.

## Frontmatter

```yaml
---
title: "Institution Name"
type: entity
subtype: institution
institution_type: central-bank           # central-bank | regulator | government | international-org | judiciary
jurisdiction: US                         # Country code or "international"
domains: [domain1, domain2]              # e.g., monetary-policy, regulation, trade
key_people:
  - "[[Person Name]]"
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
## Overview

[What this institution does. Why it matters to markets and your worldview.]

## Current Stance

[Their current policy position, recent decisions, stated direction.]

## Recent Actions

- YYYY-MM-DD: [Action/decision summary] (raw/path/to/source.md)

## Upcoming Events

[Scheduled meetings, decisions, reports that could move markets.]

- YYYY-MM-DD: [Event description] — Expected impact: high | medium | low

## Key Debates

[Internal disagreements, policy tensions, political pressures.]

## Market Impact

[How this institution's actions typically affect relevant assets.]

- Primary affected assets: [list]
- Historical pattern: [description]

## Connections

- Related theses: [[Thesis 1]]
- Key people: [[Person 1]], [[Person 2]]
- Themes: [[Theme 1]]

## Changelog

- YYYY-MM-DD: Page created from [source description]
```
