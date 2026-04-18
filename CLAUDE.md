# Signal Hunter Digital Brain

You are maintaining a personal market intelligence knowledge base.
The wiki is a compounding asset — every ingestion makes it richer.

## Architecture

Three layers. Never violate the boundaries.

1. **raw/** — Immutable, append-only. Tweets, transcripts, market snapshots. NEVER modify after write.
2. **agent_docs/** — Schema, workflows, templates. Co-evolved by human and LLM.
3. **wiki/** — LLM-generated, LLM-maintained. Entity pages, themes, signals, theses, contradictions, syntheses.

## Core Rules

- NEVER modify files in raw/. They are immutable evidence.
- ALWAYS cite raw sources with relative paths: `raw/tweets/karpathy/2026-04-06.md`
- ALWAYS update index.md after any wiki page creation or deletion.
- ALWAYS append to log.md after any operation (ingestion, compilation, lint, synthesis).
- Use `[[wikilinks]]` for cross-references. Obsidian-compatible.
- Every wiki page MUST have complete YAML frontmatter per `agent_docs/frontmatter-schema.md`.
- Contradictions are VALUABLE. Create contradiction pages. Never resolve by picking a side.

## Source Tiers

- **S-tier**: Outlier signal. Full attention. Every claim extracted and tracked.
- **A-tier**: High signal. Key claims extracted.
- **B-tier**: Useful signal. Triage first, extract if signal detected.
- **C-tier**: Ambient. Triage only. Promote to B on merit.
- Current assignments: `agent_docs/source-tiers.md`

## Page Types

| Type | Template | Location |
|------|----------|----------|
| Entity (person) | `agent_docs/page-templates/entity-person.md` | `wiki/entities/people/` |
| Entity (company) | `agent_docs/page-templates/entity-company.md` | `wiki/entities/companies/` |
| Entity (institution) | `agent_docs/page-templates/entity-institution.md` | `wiki/entities/institutions/` |
| Theme | `agent_docs/page-templates/theme.md` | `wiki/themes/` |
| Signal | `agent_docs/page-templates/signal.md` | `wiki/signals/` |
| Thesis | `agent_docs/page-templates/thesis.md` | `wiki/theses/` |
| Source Profile | `agent_docs/page-templates/source-profile.md` | `wiki/sources/{subtype}/` |
| Contradiction | `agent_docs/page-templates/contradiction.md` | `wiki/contradictions/` |
| Synthesis | `agent_docs/page-templates/synthesis.md` | `wiki/syntheses/` |

## Workflows

- Ingesting a source: `agent_docs/ingestion-workflow.md`
- Compiling wiki updates: `agent_docs/compilation-workflow.md`
- Synthesizing intelligence: `agent_docs/synthesis-workflow.md`
- Answering questions: `agent_docs/query-workflow.md`
- Health checking: `agent_docs/lint-workflow.md`

## Quality Rules

- Every claim needs a citation to `raw/`.
- Predictions MUST include resolution criteria and expected resolution date.
- Theme pages MUST include both bull and bear cases with evidence.
- Thesis pages MUST include falsifiers and invalidation levels.
- NEVER suppress contradictions. Surface them explicitly.
- Source profiles MUST track predictions with resolution status.

## Naming Conventions

- Wiki pages: `kebab-case.md` (e.g., `andrej-karpathy.md`, `ai-capex-boom.md`)
- Signal pages: `YYYY-MM-DD--slug.md` (e.g., `2026-04-06--polymarket-fed-cut-surge.md`)
- Raw tweets: `raw/tweets/{handle}/YYYY-MM-DD.md`
- Raw transcripts: `raw/transcripts/{channel}/{video-id}--{slug}.md`
- Raw market snapshots: `raw/markets/{platform}/YYYY-MM-DD-snapshot.md`
- Raw articles: `raw/articles/YYYY-MM-DD--{slug}.md`
- Synthesis pages: `YYYY-MM-DD--HHMM-{subtype}.md` (e.g., `2026-04-06--1400-intraday-brief.md`)
- Source profiles: `wiki/sources/x-accounts/{handle}.md` or `wiki/sources/youtube-channels/{slug}.md`

## Build & Deploy

- Python 3.12, FastAPI, APScheduler
- `pip install -e .` (uses pyproject.toml)
- `python scripts/serve.py` — starts scheduler + health endpoint
- Deploy: Render via render.yaml
