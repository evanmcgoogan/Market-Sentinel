# Lint Workflow

How to health-check the Digital Brain wiki. Run weekly (Sunday 03:00 ET).

## Trigger

Scheduled weekly run, or manual invocation when wiki quality feels off.

## Checks

### 1. Contradictions

Scan all wiki pages for claims that conflict with each other:
- Same entity, different claims about their position or prediction
- Same theme, conflicting direction assessments from different sources
- Same thesis, evidence that both supports and invalidates

**Action**: Create contradiction pages for any unrecorded disagreements. Link from both relevant pages.

### 2. Stale Claims

Find claims older than 30 days without:
- An update confirming they're still current
- A resolution (for predictions)
- An explicit "still valid as of YYYY-MM-DD" note

**Action**: Mark as `status: stale` in frontmatter. Add to lint report.

### 3. Orphan Pages

Wiki pages with zero inbound `[[wikilinks]]` from other pages.

**Action**: Either add appropriate links from related pages or flag for review. Pages that are truly orphaned may indicate a gap in cross-referencing.

### 4. Missing Pages

Entities, themes, or concepts frequently mentioned in `[[wikilinks]]` that don't have their own page.

**Action**: Create stub pages using the appropriate template. Mark as `confidence: low` until compilation adds substance.

### 5. Source Drift

Compare each source's recent output against their assigned tier:
- S-tier sources with low signal output → consider downgrade
- C-tier sources consistently producing high-signal content → consider upgrade
- Sources that have gone silent for 30+ days → flag for review

**Action**: Add tier change recommendations to lint report. Do NOT auto-change tiers (human decision).

### 6. Thesis Health

For each active thesis:
- Check if any catalyst dates have passed without an update
- Check if the market has moved past invalidation levels
- Check if confidence history shows a clear trend

**Action**: Flag theses needing attention in lint report. Add `status: needs_review` for urgent cases.

### 7. Prediction Resolution

Find predictions in wiki pages and `evals/predictions/active/` where:
- `resolution_date` has passed
- No resolution has been recorded

**Action**: Grade the prediction if outcome data is available. Move from `active/` to `resolved/`. Update the source's track record on their source profile page.

### 8. Frontmatter Compliance

Validate every wiki page's frontmatter against `agent_docs/frontmatter-schema.md`:
- All required fields present
- Values match allowed enums
- Dates are valid ISO format
- Related links point to pages that exist

**Action**: Fix minor issues automatically. Flag structural problems in lint report.

## Output

Write report to `wiki/lint-reports/YYYY-MM-DD-lint.md`:

```markdown
---
title: "Lint Report YYYY-MM-DD"
type: lint-report
run_at: 2026-04-06T03:00:00Z
pages_scanned: 142
issues_found: 23
issues_auto_fixed: 8
issues_flagged: 15
---

## Summary
[Overview of wiki health]

## Contradictions Found
[New contradictions detected]

## Stale Claims
[Claims needing refresh]

## Orphan Pages
[Pages with no inbound links]

## Missing Pages
[Frequently linked but nonexistent pages]

## Source Drift
[Tier change recommendations]

## Thesis Health
[Theses needing attention]

## Predictions Overdue
[Unresolved predictions past resolution date]

## Frontmatter Issues
[Schema violations]
```

## Log

Append to `log.md`:
```
[2026-04-06T03:15:00Z] LINT complete | 142 pages scanned | 23 issues found | 8 auto-fixed | report: wiki/lint-reports/2026-04-06-lint.md
```
