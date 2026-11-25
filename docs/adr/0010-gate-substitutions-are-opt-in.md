# ADR 0010 — Gate substitutions are opt-in

- **Status:** Accepted
- **Date:** 2026-11-25
- **Issue:** #141 (conformance audit, cross-cutting rule "fail closed, no silent fallback")
- **Spec sections:** §6.3, §11.9, §12.5, §10.4 (D25, D40)

## Context

The pre-execution gate (§6.3) could reduce an expensive plan on its own. It rewrote `PathPred(ALL)` to `SHORTEST`, limited unbounded `TrailExpr` depth to 3, narrowed a `Free→Free` scope to the forward cone, sampled anchors, and, in the PreComputation timeout guard, swapped an exact graph algorithm for an approximate one. The interface then executed the result. Each of these returns a different answer from the one asked. The PreComputation swap was not even reported as a decision: `deliver` returned `PROCEED`.

Audit #141 forbids changing strategy, scope or algorithm, or degrading to an alternative execution, unless the caller selected that behaviour explicitly as part of the contract.

## Options

1. **Keep automatic reductions** and declare them part of the gate's contract.
2. **Remove the reductions.** The gate only delivers exact plans, clarifies with refinements or rejects.
3. **Opt-in reductions.** Each kind of reduction is a named `Substitution` the caller allows in the budget. Allowed substitutions behave as before; viable ones that are not allowed are offered to the caller and run only when chosen.

## Decision

Option 3, chosen by the maintainer.

- `ExecutionBudget.substitutions : set[Substitution]`, default `∅`, with `Substitution ::= STRATEGY_SHORTEST | DEPTH_LIMIT | CONE_SCOPE | APPROX_ALGORITHM | SAMPLE`.
- Step 2 of `gate()` returns `REWRITE` only for a viable rewrite whose substitution is allowed. `REWRITE` names the substitution it applied.
- Step 3 returns `SAMPLE` only if `SAMPLE` is allowed.
- In the OVER zone, viable rewrites and the sampled plan that are not allowed become `CLARIFY` options of kind `SUBSTITUTION`, next to the lattice refinements. In the STREAMING and DEFERRED zones they are dropped and the exact plan is delivered.
- The PreComputation guard moves from `deliver` into the gate (Step 1b). A PreComputation over `max_precompute_time` is never started. Its approximation is returned as `REWRITE` if `APPROX_ALGORITHM` is allowed, offered as `CLARIFY` otherwise, and the plan is rejected if no executable approximation exists.
- The set travels with the budget. The NL interface passes it unchanged, a supervisor child budget inherits it, and no component widens it.
- Boundary cases T16–T20 fix the behaviour under the default.

Transformations that do not change the answer are not substitutions: BDD simplification, CSE and canonical DAG construction, pagination, and the choice among PROCEED, STREAM and ASYNC.

## Rationale

- The caller keeps the reductions when it wants them, so no capability is lost.
- With the default, the answer is exactly the one asked, or the caller is told why it cannot be computed within the budget and what can be.
- Option 1 contradicts the audit rule. Option 2 removes useful, well-defined behaviour, such as Hoeffding-bounded samples, that a caller may legitimately want.

## Consequences

- Implementations of `gate()` take the budget's `substitutions` into account. Tests cover T1–T20.
- `ClarifyOption` replaces the pair `options` / `cost_per_option`. It carries the option's kind and, for a substitution, its warning or error bound.
- The async preview of §11.9 is shown only when `SAMPLE` is allowed.
