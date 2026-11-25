# ADR 0002 — PipelineArtifact transitions are facts

- **Status:** Accepted
- **Date:** 2026-11-25
- **Issue:** #25 (blocks #53); enables #32
- **Spec sections:** §2.4, §6.2 Rule 10, §10.1 Example 6, §10.4 (D32)

## Context

`PipelineArtifact` models a five-state machine — Missing, In-flight, Complete, Stale, Failed — for each materialised window of a pipeline. The specification described it in two incompatible ways at once: `P(f).state` held the state as a property of one fact, while a table of "transition bridge edges" ran between the state names as if `Missing` and `In-flight` were nodes. Nothing said whether a state was a property value, a node, a version of the fact, or an event. `PathPred`, causality, snapshots and audit could each read the same transition differently.

## Options

1. **Mutable property, history elsewhere.** `state` is updated in place; the history of changes lives outside the graph.
2. **Immutable artifact versions.** Each state change creates a new version of the artifact, with explicit transitions between versions.
3. **Transition facts.** Each transition is an event fact linked to its artifact; `state` on the artifact is the projection of the latest transition.

## Decision

Option 3, chosen by the maintainer.

### Representation

`PipelineArtifact(f, pipeline_id, ttl, backfill_horizon, write_mode, transitions)` names two fact types:

| Element | Kind | Content |
|---|---|---|
| `f` — the artifact | fact type, `Grain(f, ACCUMULATING)` | One instance per (pipeline, window). `state: Enum(ArtifactState)` is the **current state**. |
| `transitions` — the transition | fact type, `Grain(transitions, EVENT)` | One instance per state change: `from_state`, `to_state: Enum(ArtifactState)`, `kind: Enum(TransitionKind)`, `occurred_at: Timestamp`. |
| `(transitions, f, 'transition_of')` | bridge edge type, STRUCTURAL | Each transition belongs to exactly one artifact. |
| `(transitions, transitions, 'followed_by')` | bridge edge type, CAUSAL | Links each transition to the next transition of the same artifact. |

```
ArtifactState  ::= Missing | In-flight | Complete | Stale | Failed
TransitionKind ::= scheduled | succeeded | failed | expired | rescheduled | retried
```

`ArtifactState` and `TransitionKind` are enumerations of property values. No state is a node, and no edge runs between states.

### The state machine as a relation

The admissible transitions are a relation δ over property values:

| `from_state` | `kind` | `to_state` | Condition |
|---|---|---|---|
| Missing | scheduled | In-flight | |
| In-flight | succeeded | Complete | |
| In-flight | failed | Failed | no earlier transition of the artifact reached Complete (backfill) |
| In-flight | failed | Stale | an earlier transition reached Complete (refresh; the prior result is kept) |
| Complete | expired | Stale | `ttl` elapsed since the transition into Complete |
| Stale | rescheduled | In-flight | |
| Failed | retried | In-flight | |

### Invariants

```
PA1  Every transition instance has (from_state, kind, to_state) ∈ δ.
PA2  The transitions of one artifact form a single chain under 'followed_by', ordered by
     occurred_at; the first has from_state = Missing; consecutive transitions agree:
     next.from_state = previous.to_state.
PA3  state_at(a, t) = to_state of the last transition of a with occurred_at ≤ t,
                      or Missing when there is none.
PA4  P(a).state = state_at(a, now). If the artifact type is versioned, the version in force
     at t carries state = state_at(a, t).
```

PA3 and PA4 make the current state a projection of the history. A snapshot at `t` and the history read up to `t` cannot disagree.

### Example

The run Missing → In-flight → Failed for artifact `w`:

```
t₁ = (from_state = Missing,   kind = scheduled, to_state = In-flight, occurred_at = 09:00)
t₂ = (from_state = In-flight, kind = failed,    to_state = Failed,    occurred_at = 09:20)

t₁ —transition_of→ w        t₂ —transition_of→ w        t₁ —followed_by→ t₂

state_at(w, 08:59) = Missing      state_at(w, 09:10) = In-flight      state_at(w, 09:30) = Failed = P(w).state
```

Three nodes exist: the artifact and two transitions. `Missing`, `In-flight` and `Failed` appear only as property values.

## Rationale

- **Auditable history in the graph.** Each transition is a recorded event with its own timestamp, which is what causal analysis and CTL assertions traverse.
- **A real transition relation.** `followed_by` is one time-ordered edge type, so traces, `Repeat`, TemporalMode and `Monitor` apply without special cases.
- **No conflation with data versioning.** Option 2 reuses SCD machinery for pipeline events; an artifact's attributes can change for reasons other than a state change, and the two histories would be entangled.
- **Option 1 leaves nothing to traverse.** A mutable property cannot support a path predicate over the machine, and the audit trail would sit outside the model.

## Consequences

- `PipelineArtifact` gains the `transitions` argument. The six labelled state-to-state bridge edges are removed from §2.4.
- Semantics that used to be attached to those edges — CAUSAL, TEMPORAL, SUPERSESSION per transition — are replaced by `kind` and by the two `failed` rows of δ. Rule 10 selects the row instead of inferring a bridge semantics.
- Trace properties are anchored on the first transition of an artifact and follow `followed_by`. Example 6 is rewritten on that basis, including the SAFETY assertion of #32.
- Writing transitions — who appends a transition fact and when — is delimited in #53.

## Alternatives not taken

- **Option 1**, because the state machine would not be traversable and history would be external.
- **Option 2**, because it entangles pipeline events with attribute versions and multiplies artifact rows.
- **One edge type per transition kind.** Six edge types between transitions would restore per-edge BridgeSemantics, but a trace could then not be followed by a single `Repeat`; `kind` as a property carries the same information.
