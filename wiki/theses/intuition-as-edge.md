---
title: "Intuition as a Measurable Edge"
slug: intuition-as-edge
type: thesis
status: active
confidence: 0.75
direction: meta
horizon: continuous
position_sizing_guidance: "Not directly tradeable. Governs how the other six theses get acted upon."
invalidation_level: "12+ months of operation with Brier score worse than coin-flip across recommendation cohort"
falsifiers:
  - "Recommendation track record shows operator alpha is indistinguishable from market (calibration curve flat or inverted)"
  - "Confidence ratings exhibit systematic overconfidence pattern not correctable through process"
  - "No demonstrable sector specialization emerges from 50+ graded recommendations"
supporting_signals: []
contradicting_signals: []
key_sources:
  - "[[Philip Tetlock]]"
  - "[[Daniel Kahneman]]"
  - "[[Stan Druckenmiller]]"
  - "[[Howard Marks]]"
  - "[[Annie Duke]]"
affected_themes:
  - "[[Calibration Discipline]]"
  - "[[Recommendation Grading]]"
related_theses:
  - "[[AI Infrastructure Supercycle]]"
  - "[[Robotics Era]]"
  - "[[Energy Abundance Supercycle]]"
  - "[[Space Economy Explosion]]"
  - "[[Longevity Revolution]]"
  - "[[Multipolarity & Spheres of Influence]]"
intersects_convergences: []
created: 2026-05-18
updated: 2026-05-18
update_count: 1
tags: [thesis, meta, calibration, grading, process, discipline]
---

## Thesis Statement

Intuition is pattern matching from deep, embodied domain experience. It is the thing that lets Druckenmiller take a position before macro signals confirm. It is what let Buffett identify Coca-Cola in 1988 before consensus DCF would justify the price. Tetlock's superforecaster research confirms it is real, present in a small minority of forecasters, and not reducible to formal methods. The catch: intuition is only a moat if you can prove it post-hoc. Otherwise it is indistinguishable from overconfidence, and the Dunning-Kruger curve is real. The way to convert intuition-the-claim into intuition-the-evidence is to track every directional call, grade them honestly, and let the track record speak.

## Why This Thesis is Meta

The other six theses describe the world. This thesis describes how the operator (Evan) and the system (Meridian) operate against the other six. It is the operational discipline that prevents the world model from becoming a confirmation bias machine.

Without this thesis, the system is a research tool. With this thesis, the system is investment infrastructure with a measurable feedback loop.

## Why Now / Structural Drivers

1. **Brier scoring is cheap to compute and powerful to surface.** Every recommendation has an associated probability and outcome; the math is mechanical.
2. **AI removes the bottleneck on grading.** Hand-grading recommendation track records is tedious; automating it makes the discipline tractable at scale.
3. **The compounding case for personalized infrastructure depends on this.** If we can't show that the operator's calibrated intuition compounds, the entire personal-infrastructure premise weakens.
4. **The agentic trading layer requires it.** Before any auto-execution, the system must demonstrate that its recommendations are actually well-calibrated. The grading discipline is the gate.

## Sub-Theses

### 7.1 — Recommendation Grading is the Only Test That Matters

Brier scores, calibration curves, hit/miss by horizon, alpha attribution vs benchmark. The track record either proves the system or exposes its failure modes. Honest grading is non-negotiable.

### 7.2 — Sector-Level Calibration Reveals Specialization

After 50+ graded recommendations, the data should show whether the operator's intuition is uniformly calibrated or specialized (e.g., strong in AI infrastructure, weak in longevity). The system then weights future recommendations accordingly.

### 7.3 — Overconfidence is the Default Failure Mode

The track record will likely reveal overconfidence in some areas. The discipline is to surface this and adjust, not rationalize. The system should automate the correction (e.g., scaling confidence scores down in sectors where calibration curve runs above 45 degrees).

### 7.4 — Process Discipline Compounds

A consistent grading process, applied for years, produces a knowledge structure (which intuitions worked, which didn't, in which contexts) that no generic AI product can replicate. This is the deepest layer of the moat described in the white paper.

## Operational Mechanics

### Brier Score Computation

For each resolved recommendation:
```
brier = (probability_up - actual_outcome) ^ 2
```
where `actual_outcome` is 1 if upside scenario realized, 0 if downside. Aggregate over recommendation cohort. Compare to 0.25 (coin-flip baseline). Lower is better.

### Calibration Curve

Bucket recommendations by stated confidence (e.g., 70-80%, 80-90%). For each bucket, compute hit rate. Plot stated vs actual. Diagonal = perfectly calibrated. Above = underconfident. Below = overconfident.

### Sector Attribution

For each thesis (1-6), track:
- Recommendation count
- Hit rate
- Mean return on hits
- Mean loss on misses
- Aggregate alpha vs sector benchmark

### Time Horizon Slicing

Calibration may differ by horizon. Track separately for short (<3mo), medium (3-18mo), long (>18mo) recommendations.

## Evidence Chain

Operator's accumulated track record. Populated continuously by the SCORE stage's
`prediction_resolved` Update type and the wiki's `wiki/recommendations/` durable records.

## Counter-Evidence

If the track record fails to show calibrated alpha, this thesis is invalidated and the
investment infrastructure premise weakens. That outcome is acceptable — the system would
have done its job by surfacing it honestly.

## Predictions

| Prediction | Resolves | Confidence | Status |
|---|---|---|---|
| First 25 recommendations show Brier score < 0.25 (better than coin-flip) | 2027-05-18 | 0.65 | open |
| Operator demonstrates measurable specialization in at least 2 of 6 thesis areas | 2027-12-31 | 0.70 | open |
| Calibration curve in AI Infrastructure thesis falls within ±10% of diagonal | 2028-05-18 | 0.65 | open |
| First 100 recommendations show aggregate alpha vs SPY benchmark | 2028-05-18 | 0.55 | open |

## Decision Log

- 2026-05-18: Thesis scaffolded.

## How This Could Be Wrong

The operator's intuition may not be measurably better than random in any specific sector. That is an empirical question the system will answer over time. The thesis isn't that Evan has alpha — the thesis is that the discipline of measurement is the only path to know. If the track record reveals the operator has no edge, the system has done its job by surfacing that honestly. Either outcome (proves edge / disproves edge) is more valuable than continued operation without knowing.

## Changelog

- 2026-05-18: Thesis page created. Initial confidence: 0.75.
