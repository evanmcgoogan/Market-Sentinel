# Entity Page Template: Company

Use this template when creating a wiki page for a company.

## Frontmatter

```yaml
---
title: "Company Name"
type: entity
subtype: company
sector: technology                       # technology | finance | energy | defense | healthcare | etc.
ticker: NVDA                             # Stock ticker if publicly traded, null otherwise
market_cap_tier: mega                    # mega | large | mid | small | private
domains: [domain1, domain2]              # e.g., ai, semiconductors, cloud
key_people:
  - "[[Person Name]]"
created: YYYY-MM-DD
updated: YYYY-MM-DD
update_count: 1
confidence: high                         # high | medium | low | conflicting
status: active                           # active | stale | superseded | archived
last_mentioned_in: raw/path/to/source.md
related:
  - "[[Related Company]]"
  - "[[Related Theme]]"
tags: [tag1, tag2]
---
```

## Body Sections

```markdown
## Overview

[What this company does. Why it matters to your investment worldview. 2-4 sentences.]

## Recent Developments

[Last 30 days of notable news, earnings, product launches, regulatory actions.]

- YYYY-MM-DD: [Development summary] (raw/path/to/source.md)

## Bull Case

[Why this company could outperform. Evidence with citations.]

## Bear Case

[Why this company could underperform. Evidence with citations.]

## Key Metrics to Watch

[Specific metrics that would change your view — earnings dates, guidance numbers, regulatory milestones.]

- Next earnings: YYYY-MM-DD
- Key metric: [description and threshold]

## Source Coverage

[Which tracked sources discuss this company most? Link to source profiles.]

- [[Source 1]]: [domain/angle they cover]
- [[Source 2]]: [domain/angle they cover]

## Connections

- Related theses: [[Thesis 1]], [[Thesis 2]]
- Competitor/peer: [[Company 1]], [[Company 2]]
- Key themes: [[Theme 1]], [[Theme 2]]

## Changelog

- YYYY-MM-DD: Page created from [source description]
```
