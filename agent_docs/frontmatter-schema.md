# Frontmatter Schema

Complete YAML frontmatter specification for all wiki page types. Every wiki page MUST have valid frontmatter conforming to this schema.

## Universal Fields (All Page Types)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `title` | string | yes | Display title |
| `type` | enum | yes | `entity` \| `theme` \| `signal` \| `thesis` \| `source` \| `contradiction` \| `synthesis` \| `lint-report` |
| `created` | date | yes | YYYY-MM-DD |
| `updated` | date | yes | YYYY-MM-DD |
| `tags` | string[] | yes | Lowercase, hyphenated |
| `status` | enum | yes | See per-type allowed values |

## Entity Pages

### Shared Entity Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `subtype` | enum | yes | `person` \| `company` \| `institution` |
| `domains` | string[] | yes | Topic domains: ai, macro, semiconductors, geopolitics, etc. |
| `update_count` | integer | yes | Incremented on each compilation update |
| `confidence` | enum | yes | `high` \| `medium` \| `low` \| `conflicting` |
| `status` | enum | yes | `active` \| `stale` \| `superseded` \| `archived` |
| `last_mentioned_in` | string | no | Relative path to most recent raw source |
| `related` | string[] | no | `[[wikilink]]` format |

### Person-Specific

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `roles` | string[] | yes | e.g., ai-researcher, fund-manager, ceo |
| `affiliations` | string[] | yes | Current and notable past organizations |
| `source_tier` | enum | no | `S` \| `A` \| `B` \| `C` (only if they are a tracked source) |
| `signal_weight` | float | no | 0-1, learned from prediction outcomes |

### Company-Specific

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sector` | string | yes | technology, finance, energy, defense, healthcare, etc. |
| `ticker` | string | no | Stock ticker if publicly traded |
| `market_cap_tier` | enum | no | `mega` \| `large` \| `mid` \| `small` \| `private` |
| `key_people` | string[] | no | `[[wikilink]]` format |

### Institution-Specific

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `institution_type` | enum | yes | `central-bank` \| `regulator` \| `government` \| `international-org` \| `judiciary` |
| `jurisdiction` | string | yes | Country code or "international" |
| `key_people` | string[] | no | `[[wikilink]]` format |

## Theme Pages

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `subtype` | enum | yes | `macro-trend` \| `sector-rotation` \| `regulatory` \| `geopolitical` \| `technological` |
| `status` | enum | yes | `active` \| `stale` \| `superseded` \| `archived` |
| `direction` | enum | yes | `accelerating` \| `stable` \| `decelerating` \| `reversing` |
| `confidence` | enum | yes | `high` \| `medium` \| `low` \| `conflicting` |
| `first_identified` | date | yes | YYYY-MM-DD |
| `update_count` | integer | yes | Incremented on each compilation update |
| `affected_assets` | string[] | yes | Tickers |
| `affected_theses` | string[] | no | `[[wikilink]]` format |
| `key_sources` | string[] | no | `[[wikilink]]` format |
| `bull_case_confidence` | float | yes | 0-1 |
| `bear_case_confidence` | float | yes | 0-1 |
| `falsifiers` | string[] | yes | Conditions that would invalidate the theme |
| `next_catalysts` | object[] | no | `{event: string, date: date, expected_impact: high\|medium\|low}` |

## Signal Pages

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `signal_type` | enum | yes | `market-move` \| `whale-flow` \| `source-claim` \| `data-release` \| `policy-action` \| `technical-breakout` |
| `detected_at` | datetime | yes | ISO 8601 |
| `source_file` | string | yes | Relative path to raw source |
| `severity` | enum | yes | `high` \| `medium` \| `low` |
| `confidence` | enum | yes | `high` \| `medium` \| `low` |
| `status` | enum | yes | `active` \| `resolved` \| `superseded` \| `noise` |
| `affected_assets` | string[] | no | Tickers |
| `affected_theses` | string[] | no | `[[wikilink]]` format |
| `affected_themes` | string[] | no | `[[wikilink]]` format |
| `entities_involved` | string[] | no | `[[wikilink]]` format |
| `resolution` | enum | no | `confirmed` \| `invalidated` \| `inconclusive` |
| `resolved_at` | date | no | YYYY-MM-DD |

## Thesis Pages

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `status` | enum | yes | `active` \| `invalidated` \| `confirmed` \| `dormant` |
| `confidence` | float | yes | 0-1, current conviction |
| `confidence_history` | object[] | yes | `{date: date, value: float, reason: string}` |
| `direction` | enum | yes | `long` \| `short` \| `neutral` |
| `primary_asset` | string | yes | Ticker |
| `related_assets` | string[] | no | Tickers |
| `entry_conditions` | string[] | yes | Conditions to enter position |
| `exit_conditions` | string[] | yes | Conditions to exit position |
| `position_sizing` | string | no | e.g., "2-5% of portfolio" |
| `max_exposure` | string | no | e.g., "5%" |
| `invalidation_level` | string | yes | Specific condition that kills the thesis |
| `falsifiers` | string[] | yes | Conditions that would invalidate |
| `supporting_signals` | string[] | no | Populated by signal layer |
| `contradicting_signals` | string[] | no | Populated by signal layer |
| `key_sources` | string[] | no | `[[wikilink]]` format |
| `affected_themes` | string[] | no | `[[wikilink]]` format |
| `update_count` | integer | yes | Incremented on each update |

## Source Profile Pages

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `subtype` | enum | yes | `x-account` \| `youtube-channel` \| `newsletter` \| `podcast` \| `blog` |
| `platform` | enum | yes | `twitter` \| `youtube` \| `substack` \| `podcast` \| `web` |
| `handle` | string | yes | Platform-specific identifier |
| `channel_id` | string | no | YouTube channel ID |
| `source_tier` | enum | yes | `S` \| `A` \| `B` \| `C` |
| `domains` | string[] | yes | Topic domains |
| `signal_frequency` | enum | yes | `high` \| `medium` \| `low` |
| `signal_to_noise` | float | yes | 0-1 |
| `avg_output_per_day` | float | no | Posts per day |
| `status` | enum | yes | `active` \| `inactive` \| `suspended` \| `archived` |
| `reliability_score` | float | no | Populated after 30+ resolved predictions |
| `hit_rate_30d` | float | no | 0-1 |
| `hit_rate_90d` | float | no | 0-1 |
| `total_predictions_tracked` | integer | yes | Running count |
| `total_predictions_resolved` | integer | yes | Running count |
| `lead_time_avg_hours` | float | no | How early vs consensus |

## Contradiction Pages

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `status` | enum | yes | `active` \| `resolved` \| `stale` |
| `detected_at` | date | yes | YYYY-MM-DD |
| `domains` | string[] | yes | Topic domains |
| `entities_involved` | string[] | yes | `[[wikilink]]` format |
| `affected_themes` | string[] | no | `[[wikilink]]` format |
| `affected_theses` | string[] | no | `[[wikilink]]` format |
| `resolution` | enum | no | `side-a-correct` \| `side-b-correct` \| `both-partially-correct` \| `still-unresolved` |
| `resolved_at` | date | no | YYYY-MM-DD |

## Synthesis Pages

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `subtype` | enum | yes | `weekly-macro` \| `monthly-macro` \| `thesis-stress-test` \| `weak-signal-report` \| `source-review` \| `connection-discovery` |
| `period_start` | date | yes | YYYY-MM-DD |
| `period_end` | date | yes | YYYY-MM-DD |
| `model` | string | yes | Which model generated the synthesis |
| `themes_covered` | string[] | no | `[[wikilink]]` format |
| `theses_covered` | string[] | no | `[[wikilink]]` format |
| `sources_referenced` | integer | yes | Count of distinct raw sources cited |
| `wiki_pages_referenced` | integer | yes | Count of wiki pages consulted |
| `key_findings` | integer | yes | Count of notable findings |
| `status` | enum | yes | `current` \| `superseded` \| `archived` |
| `superseded_by` | string | no | Path to newer synthesis |

## Validation Rules

1. All dates must be valid ISO 8601 (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ)
2. All `[[wikilinks]]` must use the exact page title
3. `related` arrays should link to pages that exist (lint catches missing targets)
4. `bull_case_confidence + bear_case_confidence` should sum to approximately 1.0
5. `confidence_history` entries must be in chronological order
6. `falsifiers` must contain at least one entry for themes and theses
7. `update_count` must be >= 1 and increment monotonically
8. `tags` must be lowercase and hyphenated (e.g., `ai-research`, not `AI Research`)
