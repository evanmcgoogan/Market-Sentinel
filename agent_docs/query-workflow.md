# Query Workflow

How to answer questions using the Digital Brain wiki.

## Trigger

The user asks a question about the knowledge base — a person, theme, thesis, market situation, or cross-domain connection.

## Principles

- **Wiki-first.** Always check compiled wiki pages before looking at raw sources. The wiki is the synthesis layer.
- **Cite everything.** Every claim in a response must trace to either a wiki page or a raw source.
- **Surface contradictions.** If sources disagree on the answer, say so. Link to the contradiction page if one exists.
- **Admit gaps.** If the wiki doesn't cover the topic, say "The brain doesn't have a page for this yet" rather than hallucinating.

## Steps

### 1. Parse the Query

Identify what the user is asking about:
- **Entity query**: "What has Karpathy been saying?" → `wiki/entities/people/andrej-karpathy.md`
- **Theme query**: "What's the state of AI capex?" → `wiki/themes/ai-capex-boom.md`
- **Thesis query**: "How confident should I be in the Fed pivot?" → `wiki/theses/fed-pivot-h2-2026.md`
- **Cross-domain query**: "How does the semiconductor situation affect my AI thesis?" → multiple pages
- **Source query**: "How reliable is Unusual Whales?" → `wiki/sources/x-accounts/unusual-whales.md`
- **Signal query**: "What happened with Polymarket yesterday?" → `wiki/signals/` recent entries

### 2. Gather Context

- Read `index.md` to identify all relevant pages
- Read primary page(s) for the query topic
- Read linked pages for context (follow `related:` and `[[wikilinks]]`)
- Check `wiki/contradictions/` for active disagreements on the topic
- Check `wiki/signals/` for recent events related to the topic

### 3. Synthesize Response

- Lead with the answer, not the reasoning process
- Cite wiki pages and raw sources inline
- Flag confidence levels: is this well-established or based on thin evidence?
- Surface active contradictions and unresolved debates
- Note if relevant thesis pages have upcoming catalysts or approaching invalidation levels

### 4. Identify Gaps

If the query reveals missing coverage:
- Note which pages would be useful but don't exist
- Flag stale pages that need updating
- Log gap to `log.md` so the lint process can pick it up

## Response Format

```markdown
## [Answer to the question]

[Direct answer with citations to wiki pages]

**Key sources**: [[Entity Page]], [[Theme Page]]
**Confidence**: high | medium | low | conflicting
**Active contradictions**: [[Contradiction Page]] (if any)
**Gaps**: [What the brain doesn't cover yet on this topic]
```
