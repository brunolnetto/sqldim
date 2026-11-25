# ADR 0009 — Diamond is a DGM extension of the Medallion pattern

- **Status:** Accepted
- **Date:** 2026-11-25
- **Issue:** #94 (blocks #95)
- **Spec sections:** §1, §2.4, §10.3, §10.4 (D29, D39)

## Context

The specification presented Diamond as the top layer of the Medallion architecture. The pattern as commonly described has three layers: Bronze, Silver and Gold. The text did not distinguish a layer of the pattern from an extension proposed by DGM, so the comparison could be read as a description of the standard pattern.

## Options

1. **Diamond as DGM's own extension**, labelled as such.
2. Diamond as a layer of another, explicitly named framework.
3. Remove Diamond and keep the semantic layer outside Medallion.

## Decision

Option 1, chosen by the maintainer.

- The Medallion pattern is referred to as Bronze / Silver / Gold.
- **Diamond** is introduced as a fourth layer *proposed by DGM*, above Gold. It holds the governed semantic layer: the question space `Q_valid`, the recommender, the NL interface and the execution budget.
- Every place that names Diamond says it is a DGM extension. The `MedallionLayer` annotation keeps the value `DIAMOND`, documented as that extension.

## Rationale

- The semantic layer is a real part of DGM's architecture and benefits from a name in the same vocabulary as the pipeline layers.
- Labelling it removes the ambiguity without losing the mapping.
- Option 2 would need a framework that defines such a layer, and none is cited. Option 3 loses a useful name for something the specification already describes.

## Consequences

- §1, §2.4, §10.3 and D29 label Diamond as a DGM extension. D39 records this decision.
- Statements about the standard pattern mention only Bronze, Silver and Gold.
