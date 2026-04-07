# Source Tier Definitions

How sources are classified and what each tier means for ingestion behavior.

## Tier Definitions

### S-Tier: Outlier Signal

**Definition**: Sources with a demonstrated track record of producing non-consensus, actionable intelligence before the market prices it in. Following them is a measurable edge.

**Ingestion behavior**:
- Every piece of content is ingested and extracted
- Every falsifiable claim is tracked as a prediction
- Full transcripts preserved (never truncated)
- Real-time triage — high_signal extractions trigger immediate compilation

**Promotion criteria**: Demonstrated prediction accuracy (>60% hit rate over 30+ predictions) OR unique domain access that no other source provides.

**Demotion criteria**: Hit rate drops below 50% over 90 days. Signal-to-noise ratio drops below 0.5. Source goes silent for 30+ days without explanation.

### A-Tier: High Signal

**Definition**: Sources with strong domain expertise and good track records. Reliably useful but not at the outlier level of S-tier.

**Ingestion behavior**:
- All content ingested
- Key claims extracted (not every offhand comment)
- Predictions tracked when clearly stated
- Batch triage acceptable (doesn't need immediate compilation)

**Promotion criteria**: Consistent high signal-to-noise (>0.7) with evidence of leading consensus.

**Demotion criteria**: Signal-to-noise drops below 0.5. Predictions consistently lag consensus.

### B-Tier: Useful Signal

**Definition**: Sources with domain-specific value. Useful as confirming voices or for coverage of niche topics.

**Ingestion behavior**:
- Content ingested
- Triage first — only extract if triage verdict is high_signal or medium_signal
- Predictions tracked only for high-conviction, clearly stated claims
- Batch compilation only

**Promotion criteria**: Produces 3+ high_signal triage verdicts in 30 days. Shows evidence of unique insight.

**Demotion criteria**: Consistently triaged as noise. Signal-to-noise below 0.3.

### C-Tier: Ambient

**Definition**: Sources followed for ambient awareness. Context value, not primary signal.

**Ingestion behavior**:
- Content ingested
- Triage only — extract ONLY if triage verdict is high_signal
- No prediction tracking
- Compilation only in batch sweeps

**Promotion criteria**: Produces a genuinely surprising high_signal triage verdict. Shows a unique perspective not covered by higher-tier sources.

**Demotion criteria**: Not applicable (C is the floor). Sources producing zero signal over 90 days should be unfollowed entirely.

## Current Assignments

Source tier assignments are managed in `config/sources.yaml`. See that file for the complete list.

Initial assignments are based on your curation judgment. After 90 days of operation, the lint process will recommend tier changes based on measured signal quality and prediction accuracy.

## Tier Change Process

1. **Lint detects drift**: The weekly lint process flags sources whose output doesn't match their tier
2. **Recommendation logged**: Tier change recommendation added to lint report
3. **Human decides**: Tier changes are NEVER automated. You review and approve.
4. **Update recorded**: Change applied to `config/sources.yaml` with date and reason
5. **Source profile updated**: Tier change recorded in the source's Tier History section
