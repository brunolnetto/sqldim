# ADR 0005 — PipelineArtifact is declarative

- **Status:** Accepted
- **Date:** 2026-11-25
- **Issue:** #53 (depends on ADR 0002)
- **Spec sections:** §2.4, §6.2 Rule 10, §6.3 Strategy 4, §10.4 (D20, D35)

## Context

D20 calls `PipelineArtifact` syntactic sugar of the analytical model. Yet Rule 10 spoke of appending transitions and emitting writes, and §6.3 scheduled asynchronous materialisation through it. The specification did not say whether the annotation only declares semantics or also orchestrates persistent side effects. An implementation could have performed writes merely by evaluating an annotation.

## Options

1. **Declarative.** The annotation informs the planner, which produces write plans and proposed transitions. Nothing is executed by evaluating the annotation or planning a query; an authorised runtime executes the plans and records the transitions.
2. **Active annotation.** The annotation triggers a scheduler or worker directly.
3. **Remove lifecycle and writes from DGM.**

## Decision

Option 1, chosen by the maintainer.

### Responsibilities

| Actor | Does | Never does |
|---|---|---|
| DGM model and planner | Read `Σ` and the artifact's current state; produce `WritePlan`s and `TransitionPlan`s as part of the `ExportPlan` | Write data, append transition facts, start jobs |
| Runtime (executor) | Execute an `ExportPlan`'s write plans under an identity authorised to write; append the transition facts; dispatch scheduled runs | Infer semantics that the planner did not plan |

```
TransitionPlan(
  artifact_key:    entity key of the artifact,
  from_state:      ArtifactState,
  kind:            TransitionKind,
  to_state:        ArtifactState,                  -- a row of δ (§2.4)
  condition:       ON_DISPATCH | ON_SUCCESS | ON_FAILURE,
  idempotency_key: H(artifact_key, from_state, kind, plan id))
```

### Rules

- Evaluating a query, checking an annotation or producing a plan has no side effect. In particular, a read query over `PipelineArtifact` facts never changes them.
- The runtime appends a transition fact only when its `condition` is met. `scheduled` is appended on dispatch, `succeeded` on success, and `failed` on failure. Each append uses the `idempotency_key`, so a retried runtime step records it once.
- The appended transition must be a row of δ, and PA1–PA4 (§2.4) must hold after the append; otherwise the runtime rejects it.
- Writes require an identity authorised to write to the sink and to the transition table. Under the supervisor this is a separate grant from READ (§12.10).

## Rationale

- Keeps the analytical model free of side effects, which is what makes query evaluation, caching and the budget gate safe to call repeatedly.
- Places effects where authorisation, retries and failure handling already live (§11.10 `enqueue_async`, §12.11).
- Keeps D20's claim true: the annotation is sugar over declared constructs plus planning.

## Consequences

- Rule 10 produces `TransitionPlan`s and `WritePlan`s instead of "appending" transitions.
- §6.3 Strategy 4 describes what the runtime does with an ASYNC decision; the gate itself schedules nothing.
- A deployment without an authorised runtime can still plan and inspect pipelines, but nothing is materialised.
