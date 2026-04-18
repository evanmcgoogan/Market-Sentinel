# Compilation Workflow

How to update wiki pages from extracted raw sources. This is where knowledge compounds.

## Trigger

An extraction from the ingestion pipeline has passed triage (high_signal or medium_signal).

## Principles

- **One source → many page updates.** A single tweet thread might touch an entity page, 2 theme pages, and create a new signal page.
- **Incremental, never full recompile.** Read current page + new extraction → produce updated page. Costs stay linear with new content.
- **Citations are mandatory.** Every new claim added to the wiki must link back to its raw source file.
- **Contradictions are features.** If new information conflicts with existing wiki content, create a contradiction page. Do NOT silently overwrite.

## Steps

### 1. Identify Affected Pages

- Read `index.md` to get the full page catalog
- From the extraction's `entities`, `themes`, and `affected_wiki_pages`, determine which pages need updates
- Check if any referenced entities/themes lack wiki pages (these become candidates for new pages)

### 2. Read Current State

For each affected page:
- Read the full page content (use prompt caching — schema + index are cached across calls)
- Note the current `update_count` and `updated` date in frontmatter

### 3. Apply Updates

For each affected page, produce an updated version that:

- **Adds** new information to the relevant section (Recent Activity, Evidence For/Against, Key Claims, etc.)
- **Cites** the raw source: `(raw/tweets/karpathy/2026-04-06.md)`
- **Increments** `update_count` in frontmatter
- **Updates** `updated` date in frontmatter
- **Updates** `last_mentioned_in` to point to the raw source
- **Adds** new `[[wikilinks]]` for cross-references discovered in this extraction
- **Preserves** all existing content — compilation is additive, never destructive

### 4. Handle Contradictions

If the new extraction contradicts existing claims on a wiki page:

1. Do NOT modify the original claim
2. Add the new contradicting claim with its citation
3. Create a new contradiction page at `wiki/contradictions/YYYY-MM-DD--slug.md`
4. Link the contradiction page from both relevant wiki pages
5. The contradiction page documents: what disagrees, who says what, the evidence on each side

### 5. Create New Pages

If an entity, theme, or signal doesn't have a wiki page yet:

1. Use the appropriate template from `agent_docs/page-templates/`
2. Fill in all frontmatter fields
3. Write initial content sections from the extraction
4. Add to `index.md`

### 6. Update Index

After all page updates:
- Add any new pages to `index.md`
- Update modification dates for changed pages

### 7. Log

Append to `log.md`:
```
[2026-04-06T14:40:00Z] COMPILED from raw/tweets/karpathy/2026-04-06.md | updated: andrej-karpathy.md, ai-capex-boom.md | created: 2026-04-06--karpathy-autoresearch-claim.md
```

## Compilation Prompt Structure

When calling Sonnet for compilation, the prompt should be structured as:

1. **System**: CLAUDE.md schema (cached)
2. **System**: Relevant page template (cached)
3. **System**: Current index.md (cached)
4. **User**: The extraction JSON
5. **User**: Current content of each affected wiki page
6. **Assistant**: Updated wiki pages as structured output

Use prompt caching aggressively — the schema, templates, and index change rarely.

## Synthesis Triggering

Compilation feeds synthesis. After each compilation batch:

1. **Record the compilation timestamp and extraction count** — synthesis uses this to know what's new
2. **Check for high_signal burst** — if 3+ high_signal extractions compiled within a 2-hour window, trigger an event-driven synthesis immediately (see `agent_docs/synthesis-workflow.md`)
3. **Tag affected theses** — the synthesis layer uses thesis affinity to prioritize what to reframe

Compilation is the engine. Synthesis is the lens. They run in sequence: collect → extract → compile → synthesize.
