# Meridian — Personal Investment Intelligence OS

You are maintaining Meridian, personal investment infrastructure for one operator (Evan).
The wiki is a compounding asset — every ingestion makes it richer. The end-game is the
Action layer producing recommendations with EV math that feed an agentic trading layer.

**Compliance posture:** This is personal infrastructure for one operator. No external
surfaces today. Speak plainly internally — name tickers, state direction, size positions.
Hedging is not required and reduces utility. If we ever ship an external surface (shared
URL, second user, marketing site), we add a compliance wrapper at that boundary, not in
the system itself.

## Architecture

Six stages. Never violate the boundaries.

```
INGEST → EXTRACT → COMPILE → [SYNTHESIZE] → SCORE → SERVE
                                                        ↓
                                                      [ACT]   (future)
```

1. **raw/** — Immutable, append-only. Tweets, transcripts, papers, articles, market snapshots. NEVER modify after write.
2. **extractions/** — Structured JSON from Haiku extraction. Append-only.
3. **wiki/** — LLM-maintained knowledge structure. Entities, themes, signals, theses, contradictions, convergences, source profiles, syntheses, recommendations, portfolio.
4. **updates/** — Typed Stream Updates emitted by SCORE. Append-only except `user_state`.
5. **agent_docs/** — Schema, workflows, templates. Co-evolved by operator and LLM.

## The Seven Theses (wiki/theses/ spine)

The world model is anchored to seven explicit investment theses. Every wiki update should
ultimately be reasonable in terms of "which thesis does this touch."

1. **AI Infrastructure Supercycle** — memory repricing from cyclical to exponential, bottleneck stack from ASML to materials
2. **Robotics Era** — largest industry in history, manufacturing scale beats IP, endgame is Dark Factory Triad
3. **Energy Abundance Supercycle** — grid sized for 2010s must serve 2030s, nuclear + gas + grid infrastructure
4. **Space Economy Explosion** — cost-per-kg collapse drives asset accumulation, SpaceX dominant, public proxies weak
5. **Longevity Revolution** — GLP-1 demonstrates pace, AI + lab automation compounds, picks-and-shovels first
6. **Multipolarity & Spheres of Influence** — US/China/Russia/Israel asymmetric strengths, onshoring as decade-long CAPEX cycle
7. **Intuition as Edge** — meta-thesis; recommendation grading converts intuition-the-claim into intuition-the-evidence

## Core Rules

- NEVER modify files in raw/. They are immutable evidence.
- ALWAYS cite raw sources with relative paths: `raw/tweets/karpathy/2026-04-06.md`
- ALWAYS update index.md after any wiki page creation or deletion.
- ALWAYS append to log.md after any operation (ingestion, compilation, lint, synthesis, scoring).
- Use `[[wikilinks]]` for cross-references. Obsidian-compatible (Obsidian is the deep-work surface).
- Every wiki page MUST have complete YAML frontmatter per `agent_docs/frontmatter-schema.md`.
- Contradictions are VALUABLE. Create contradiction pages. Never resolve by picking a side.
- Synthesis and recommendations name tickers and state direction. No hedging.

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
| Convergence | `agent_docs/page-templates/convergence.md` | `wiki/convergences/` |
| Source Profile | `agent_docs/page-templates/source-profile.md` | `wiki/sources/{subtype}/` |
| Contradiction | `agent_docs/page-templates/contradiction.md` | `wiki/contradictions/` |
| Synthesis | `agent_docs/page-templates/synthesis.md` | `wiki/syntheses/` |
| Recommendation | `agent_docs/page-templates/recommendation.md` | `wiki/recommendations/` |

## Update Schema

The Stream is the daily interface. SCORE emits typed Update objects to `updates/` as JSON.
Schema lives in `agent_docs/update-schema.md`. Seven update types: Convergence,
Contradiction, Thesis Pressure, Entity Shift, Prediction Resolved, Anomaly, Synthesis.

## Workflows

- Ingesting a source: `agent_docs/ingestion-workflow.md`
- Compiling wiki updates: `agent_docs/compilation-workflow.md`
- Synthesizing intelligence: `agent_docs/synthesis-workflow.md`
- Scoring updates: `agent_docs/scoring-workflow.md`
- Answering questions: `agent_docs/query-workflow.md`
- Health checking: `agent_docs/lint-workflow.md`

## Quality Rules

- Every claim needs a citation to `raw/`.
- Predictions MUST include resolution criteria and expected resolution date.
- Theme pages MUST include both bull and bear cases with evidence.
- Thesis pages MUST include falsifiers and invalidation levels.
- Recommendations MUST include kill conditions and EV framing.
- NEVER suppress contradictions. Surface them explicitly.
- Source profiles MUST track predictions with resolution status.
- NEVER inflate certainty. Empty Stream days are fine; forced takes are not.

## Naming Conventions

- Wiki pages: `kebab-case.md` (e.g., `andrej-karpathy.md`, `ai-infrastructure-supercycle.md`)
- Signal pages: `YYYY-MM-DD--slug.md`
- Raw tweets: `raw/tweets/{handle}/YYYY-MM-DD.md`
- Raw transcripts: `raw/transcripts/{channel}/{video-id}--{slug}.md`
- Raw papers: `raw/papers/{category}/YYYY-MM-DD--{arxiv-id}--{slug}.md`
- Raw articles: `raw/articles/{publication-slug}/YYYY-MM-DD--{slug}.md`
- Raw market snapshots: `raw/markets/{platform}/YYYY-MM-DD-snapshot.md`
- Synthesis pages: `YYYY-MM-DD--HHMM-{subtype}.md`
- Updates: `updates/YYYY-MM-DD/{update-id}.json`
- Source profiles: `wiki/sources/x-accounts/{handle}.md` or `wiki/sources/youtube-channels/{slug}.md`

## Build & Deploy

- Python 3.12, FastAPI, APScheduler
- `pip install -e .` (uses pyproject.toml)
- `python scripts/serve.py` — starts scheduler + health endpoint + Stream API
- Deploy: Railway via railway.toml
- Persistence: git is the source of truth. Auto-commit every 60 minutes preserves wiki state across deploys.

## Required Environment Variables

- `ANTHROPIC_API_KEY` — Required. Extraction (Haiku), compilation (Sonnet), synthesis (Sonnet/Opus), scoring (Sonnet).
- `SOCIALDATA_API_KEY` — Required for Twitter ingestion (~$30-50/mo).
- `YOUTUBE_API_KEY` — Required for YouTube video discovery (Google Cloud, free tier).
- `PORT` — Server port (Railway sets this; default 10000).
