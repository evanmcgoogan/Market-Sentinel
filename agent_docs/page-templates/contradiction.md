# Contradiction Page Template

Use this template when sources disagree on a material claim. Contradictions are valuable — they reveal where the smart money disagrees and where alpha might live.

## Frontmatter

```yaml
---
title: "YYYY-MM-DD Contradiction Description"
type: contradiction
status: active                           # active | resolved | stale
detected_at: YYYY-MM-DD
domains: [domain1, domain2]
entities_involved:
  - "[[Entity/Source 1]]"
  - "[[Entity/Source 2]]"
affected_themes:
  - "[[Theme Name]]"
affected_theses:
  - "[[Thesis Name]]"
resolution: null                         # side-a-correct | side-b-correct | both-partially-correct | still-unresolved
resolved_at: null                        # YYYY-MM-DD
created: YYYY-MM-DD
updated: YYYY-MM-DD
tags: [tag1, tag2]
---
```

## Body Sections

```markdown
## The Disagreement

[One sentence: what do these sources disagree about?]

## Side A

**Claimed by**: [[Source/Entity]]
**Position**: [What they believe]
**Evidence**:
- [Evidence point] (raw/path/to/source.md)

**Confidence**: [How strongly do they hold this view?]

## Side B

**Claimed by**: [[Source/Entity]]
**Position**: [What they believe]
**Evidence**:
- [Evidence point] (raw/path/to/source.md)

**Confidence**: [How strongly do they hold this view?]

## Analysis

[Why this disagreement exists. What assumptions differ between the sides? What data would resolve it?]

## Resolution Criteria

[What would definitively prove one side right?]

- Side A is right if: [condition]
- Side B is right if: [condition]
- Expected resolution by: YYYY-MM-DD

## Impact

[What changes in your worldview depending on which side is right?]

- If Side A: [implications for theses/themes]
- If Side B: [implications for theses/themes]

## Resolution

[Filled in when the disagreement resolves.]

- Outcome: [what happened]
- Winner: Side A | Side B | Both partially correct
- Lessons: [what this teaches about these sources]

## Changelog

- YYYY-MM-DD: Contradiction detected between [[Source A]] and [[Source B]]
```
