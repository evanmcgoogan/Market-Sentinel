# Source Profile Page Template

Use this template for tracked information sources — X accounts, YouTube channels, newsletters, etc.

## Frontmatter

```yaml
---
title: "Display Name (@handle or Channel Name)"
type: source
subtype: x-account                       # x-account | youtube-channel | newsletter | podcast | blog
platform: twitter                        # twitter | youtube | substack | podcast | web
handle: username                         # Platform-specific identifier
channel_id: null                         # YouTube channel ID (if applicable)
source_tier: S                           # S | A | B | C
domains: [domain1, domain2]              # e.g., macro, ai, semiconductors
signal_frequency: high                   # high | medium | low (how often they post signal)
signal_to_noise: 0.85                    # 0-1, estimated or measured
avg_output_per_day: 8                    # Tweets, videos, posts per day
created: YYYY-MM-DD
updated: YYYY-MM-DD
reliability_score: null                  # Populated after 30+ resolved predictions
hit_rate_30d: null
hit_rate_90d: null
total_predictions_tracked: 0
total_predictions_resolved: 0
lead_time_avg_hours: null                # How early are they vs consensus?
status: active                           # active | inactive | suspended | archived
tags: [tag1, tag2]
---
```

## Body Sections

```markdown
## Why This Source Matters

[Your curation rationale — why you follow this source. What unique perspective do they provide?]

## Domain Expertise

[What topics is this source most reliable on? Where should you trust them vs. discount them?]

- Strong on: [topics]
- Weak on: [topics]
- Unique angle: [what they see that others don't]

## Recent Notable Output

[Last 7 days of high-signal content with citations.]

- YYYY-MM-DD: [Summary] (raw/path/to/source.md)

## Tracked Predictions

[Falsifiable claims with resolution status. This is the source's scorecard.]

| Date | Prediction | Resolution | Outcome | Citation |
|------|-----------|------------|---------|----------|
| YYYY-MM-DD | Claim | pending/confirmed/invalidated | [outcome] | raw/path |

## Reliability History

[Performance over time — updated monthly by the lint process.]

| Month | Predictions Resolved | Hit Rate | Brier Score | Notes |
|-------|---------------------|----------|-------------|-------|

## Interaction Patterns

[Who do they engage with? What sources do they amplify? Network position.]

- Frequently agrees with: [[Source]]
- Frequently disagrees with: [[Source]]
- Amplifies: [[Source]]

## Tier History

[Record of tier changes with rationale.]

- YYYY-MM-DD: Assigned tier X — Reason: [initial assessment]

## Changelog

- YYYY-MM-DD: Profile created
```
