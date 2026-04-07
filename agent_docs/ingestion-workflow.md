# Ingestion Workflow

How to process a new raw source into the Digital Brain.

## Trigger

A polling script (or manual import) has written a new file to `raw/`.

## Steps

### 1. Validate Raw File

- Confirm the file has valid YAML frontmatter with required fields:
  - `source`: twitter | youtube | polymarket | kalshi | price-feed | article
  - `collected_at`: ISO 8601 timestamp
  - Source-specific fields (see below)
- Confirm the file is in the correct directory per naming conventions in CLAUDE.md
- If invalid, log error to `log.md` and stop

### 2. Deduplicate

- Compute content hash (tweet ID, video ID, or price timestamp)
- Check against `raw/.hashes` (flat text file, one hash per line)
- If duplicate, log skip to `log.md` and stop
- If new, append hash to `raw/.hashes`

### 3. Extract (Haiku 4.5 via Batch API)

For each new raw file, produce a structured extraction:

```json
{
  "source_file": "raw/tweets/karpathy/2026-04-06.md",
  "extractions": [
    {
      "type": "claim | prediction | data_point | event | opinion",
      "content": "The actual claim or observation",
      "confidence": "stated_as_fact | high_conviction | medium_conviction | speculative | hedged",
      "entities": ["Entity1", "Entity2"],
      "themes": ["theme-slug-1", "theme-slug-2"],
      "sentiment": "bullish | bearish | neutral | mixed",
      "temporal": "current | forward_looking | historical",
      "falsifiable": true,
      "actionable": true,
      "signal_strength": 0.0-1.0
    }
  ],
  "triage_verdict": "high_signal | medium_signal | noise",
  "affected_wiki_pages": [
    "wiki/entities/people/andrej-karpathy.md",
    "wiki/themes/ai-capex-boom.md"
  ]
}
```

### 4. Triage

- **high_signal**: Proceed immediately to compilation (Stage 3)
- **medium_signal**: Queue for next batch compilation
- **noise**: Log to `log.md` with reason. Do NOT compile. Revisit if user feedback says the system is filtering too aggressively.

### 5. Log

Append to `log.md`:
```
[2026-04-06T14:35:00Z] INGESTED raw/tweets/karpathy/2026-04-06.md | 3 tweets | 2 extractions | triage: high_signal
```

## Raw File Frontmatter by Source Type

### Twitter
```yaml
---
source: twitter
handle: karpathy
collected_at: 2026-04-06T14:30:00Z
tweet_count: 3
contains_thread: true
---
```

### YouTube Transcript
```yaml
---
source: youtube
channel: 20vc
video_id: abc123def
title: "The Future of AI Infrastructure"
published_at: 2026-04-05T10:00:00Z
duration_minutes: 97
collected_at: 2026-04-06T14:30:00Z
transcript_method: youtube-transcript-api | whisper
---
```

### Market Snapshot
```yaml
---
source: polymarket | kalshi | price-feed
collected_at: 2026-04-06T14:30:00Z
market_count: 25
---
```

### Article
```yaml
---
source: article
title: "Article Title"
url: https://example.com/article
author: Author Name
published_at: 2026-04-05
collected_at: 2026-04-06T14:30:00Z
clipped_via: obsidian-web-clipper | manual
---
```
