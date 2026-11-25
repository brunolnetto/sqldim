# ADR 0001 — CTL over maximal paths, with an explicit empty-domain policy

- **Status:** Proposed
- **Date:** 2026-11-25
- **Issue:** #30 (blocks #32, #33, #66)
- **Spec sections:** §2.2, §4.1, §6.1, §8.1, §10.4 (D30)

## Context

The DGM specification reads `PathPred` with a `TemporalMode` as a CTL formula. Textbook CTL assumes a total transition relation: every state has a successor, so every path is infinite. DGM graphs are not total. Fact nodes cannot be the source of verb edges and are routinely sink nodes; event logs and `BridgeSemantics(CAUSAL)` chains are finite and acyclic.

Two questions were left without an answer:

1. What do EG, AG and AX mean at a node with no successor?
2. What does FORALL mean when there is no path at all — for example a customer with no sales under "all of this customer's sales exceed 500"?

Left open, a universal condition can hold by vacuity with no observed occurrence, and two implementations can disagree on the same query.

## Options

**Option 1 — maximal-path semantics (finite traces).** Quantify over maximal paths: a path ends where the graph ends. Add an explicit policy for an empty quantification domain.

**Option 2 — totalisation.** Add an implicit self-loop to every terminal node, making the relation total, and keep textbook CTL.

## Decision

Option 1.

### Definitions

For a `PathPred` evaluated at a start node `s`:

- **Evaluation position.** A node of a path instance at which every path-local alias referenced by `sub_filter` is bound. `sub_filter` is evaluated only at evaluation positions.
- **Trace.** The sequence of evaluation positions of one path instance from `s`, in path order.
- **Terminal.** A position after which the path instance cannot be extended: the node has no outgoing edge instance matching the next hop of the path. A fact node with no outgoing bridge edge is terminal for every forward path.
- **Maximal trace.** A trace that is not a proper prefix of another trace from `s`. A maximal trace is either finite, ending at a terminal, or infinite. Infinite traces exist only where the traversed subgraph has a cycle; under `BridgeSemantics(CAUSAL)` and in event logs every maximal trace is finite.
- **`T(s)`** — the set of maximal traces from `s`.
- **`Succ(s)`** — the set of nodes reached from `s` by exactly one hop of the path.
- **Empty domain.** `T(s) = ∅` when no path instance from `s` has an evaluation position; `Succ(s) = ∅` when `s` is terminal.

No transition is added to the graph. A finite trace is a complete object, not a truncated infinite one.

### Truth conditions

`π` ranges over `T(s)` and `i`, `j` over the positions of `π`.

| Form | Holds at `s` iff | Empty domain |
|---|---|---|
| EF φ | ∃π ∃i. φ(πᵢ) | false |
| EG φ | ∃π ∀i. φ(πᵢ) | false |
| E(φ U ψ) | ∃π ∃j. ψ(πⱼ) ∧ ∀i<j. φ(πᵢ) | false |
| EX φ | ∃m ∈ Succ(s). φ(m) | false |
| AF φ | ∀π ∃i. φ(πᵢ) | by `on_empty` |
| AG φ | ∀π ∀i. φ(πᵢ) | by `on_empty` |
| A(φ U ψ) | ∀π ∃j. ψ(πⱼ) ∧ ∀i<j. φ(πᵢ) | by `on_empty` |
| AX φ | ∀m ∈ Succ(s). φ(m) | by `on_empty` |

The domain is `T(s)` for the F, G and U forms and `Succ(s)` for the X forms. UNTIL is strong: on a finite trace ψ must occur before the trace ends. An infinite trace on a finite graph is decided on its lasso — the finite prefix and the cycle it closes.

EG φ and AG φ are satisfied by a finite trace on which φ holds at every position up to and including the terminal. Nothing is required "after" the terminal.

### Empty-domain policy

`PathPred` gains a field that applies to FORALL only:

```
on_empty?: REQUIRE_WITNESS | VACUOUS        -- default REQUIRE_WITNESS
```

- **REQUIRE_WITNESS** (default). FORALL is false when its domain is empty. "All sales of the customer exceed 500" is false for a customer with no sales; AX φ is false at a terminal.
- **VACUOUS.** FORALL is true when its domain is empty — the textbook reading, and the one produced by a bare `NOT EXISTS`. It must be requested explicitly.

EXISTS is false on an empty domain under either setting.

### Duality and construction

Under VACUOUS the CTL dualities hold unchanged on maximal traces: AG φ = ¬EF ¬φ, AF φ = ¬EG ¬φ, AX φ = ¬EX ¬φ, A(φ U ψ) = ¬E(¬ψ U (¬φ ∧ ¬ψ)) ∧ ¬EG ¬ψ.

Under REQUIRE_WITNESS each FORALL form is its VACUOUS form conjoined with a non-emptiness guard:

```
FORALL, mode ∈ {GLOBALLY, EVENTUALLY, UNTIL}:   AND(PathPred(EXISTS, EVENTUALLY, TRUE), <dual construction>)
FORALL, NEXT:                                   AND(PathPred(EXISTS, NEXT, TRUE),       <dual construction>)
```

`PathPred(EXISTS, EVENTUALLY, TRUE)` is exactly `T(s) ≠ ∅`, and `PathPred(EXISTS, NEXT, TRUE)` is exactly `Succ(s) ≠ ∅`.

### Worked values

**Graph A — terminal facts.** Customer `c` placed sales `s₁` (revenue 600) and `s₂` (revenue 300); customer `c₀` placed none. Path: `VerbHop(c, 'placed', s, node_alias=fs)`; φ = `fs.revenue > 500`. The facts are terminal. `T(c) = {(s₁), (s₂)}`, `Succ(c) = {s₁, s₂}`; `T(c₀) = Succ(c₀) = ∅`.

| At | EF φ | EG φ | EX φ | AF φ | AG φ | AX φ |
|---|---|---|---|---|---|---|
| `c` | true | true — trace (s₁) | true | false — trace (s₂) | false | false |
| `c₀`, REQUIRE_WITNESS | false | false | false | false | false | false |
| `c₀`, VACUOUS | false | false | false | true | true | true |

**Graph B — a chain ending in a terminal.** Artifact states `a₀ →scheduled→ a₁ →succeeded→ a₂`, with `a₂` terminal and `state = Complete` at `a₂` only. The path follows transition bridge edges; `sub_filter` reads the state of the current node, so the start node is an evaluation position. φ = `state = Complete`.

| At | EF φ | EG φ | EX φ | AF φ | AG φ | AX φ |
|---|---|---|---|---|---|---|
| `a₀` — `T = {(a₀ a₁ a₂)}` | true | false | false | true | false | false |
| `a₁` — `T = {(a₁ a₂)}` | true | false | true | true | false | true |
| `a₂` — `T = {(a₂)}`, `Succ = ∅`, REQUIRE_WITNESS | true | true | false | true | true | false |
| `a₂`, VACUOUS | true | true | false | true | true | true |

At the terminal `a₂`, EG φ and AG φ are true on the one-position trace without any self-loop. Only AX depends on the policy there, because only `Succ(a₂)` is empty.

## Rationale

- **No artificial transitions.** A self-loop on a terminal fact is an edge that no event produced. It would be visible to everything that counts or follows edges: hop counts, `SHORTEST`, path signatures, `PathAgg`, degree and cycle detection.
- **NEXT keeps its meaning.** With a self-loop, EX φ and AX φ at a terminal reduce to φ at that same node — "next" would mean "now".
- **Totalisation does not answer the second question.** A customer with no sales has no path under either option; an empty-domain policy is needed regardless.
- **Event logs are finite.** Maximal finite traces are the natural model for them, as in LTLf and CTLf; infinite traces are still accommodated where cycles exist.
- **Witness by default.** Analytical questions phrased with "all" are normally read with existential import. The vacuous reading remains available, but must be written down.

## Consequences

- The transition relation is not assumed total. §8.1 says so; whether DGM is presented as a Kripke structure or a labelled transition system is decided separately (#66).
- §4.1 defines traces, terminals and the empty-domain policy, and the FORALL elimination table gains the guard.
- §6.1 defines "end of a maximal path" for the EG template: a node with no matching outgoing edge, or the closing of a cycle.
- SAFETY over a zero-hop path (#32) and LIVENESS on finite logs (#33) are to be restated on these definitions. Neither is changed by this ADR.
- Existing FORALL predicates change meaning on nodes with no path instance: they become false unless `on_empty = VACUOUS` is set.
- Which positions of a multi-hop or closure path are evaluation positions follows from alias binding. A sharper statement of path order belongs to #31 and #35.

## Alternatives not taken

- **Option 2 (self-loops)**, for the reasons above.
- **VACUOUS as the default.** Keeps textbook duality without a guard, but silently admits the customer with no sales — the behaviour the issue asks to rule out.
- **Making `on_empty` mandatory on every FORALL.** Fully explicit, but every existing example would need the field; a default with a documented meaning achieves the same clarity.
