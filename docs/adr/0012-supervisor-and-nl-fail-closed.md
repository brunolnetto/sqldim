# ADR 0012 — The supervisor and the NL interface fail closed

- **Status:** Accepted
- **Date:** 2026-11-25
- **Issue:** #141; specification side of #129, #130 and #143
- **Amends:** ADR 0008 (the recovery barrier now covers *replan* only)
- **Spec sections:** §11.10, §12.2–§12.5, §10.4 (D42)

## Context

The conformance audit found tools that returned `success=True` with placeholder data, `valid=True` without evaluating anything, synthetic finding ids, and NL graph nodes that fell back to passthrough stubs when no model was configured. The specification itself contained an automatic substitution: the *degrade* route of §12.4–§12.5 replaced a REJECTED step by the tool's `degrade_to` substitute. For example, `dgm_query` was replaced by `dgm_what_can_i_answer`, and `dgm_sensitivity_sweep` by `dgm_query`. The error taxonomy had no kind for a capability that is missing or a request a backend cannot execute, so an implementation had nothing honest to return.

## Options for the degrade route

1. **Remove degrade, keep replan.**
2. Keep degrade, but only if the caller enables it per request.
3. Remove degrade and replan, so that any REJECTED step aborts.

## Decision

Option 1, chosen by the maintainer, together with the following fail-closed rules.

- **Result semantics.** `SUCCEEDED` means the tool executed its whole contract, and its `data` is computed output. An empty collection is a success only after complete execution. Not executed, failed, not implemented, unsupported and unauthorized each have their own status and `ToolErrorKind`.
- **New error kinds.** `NOT_IMPLEMENTED` covers a capability that is not implemented or not configured in this deployment. `UNSUPPORTED` covers a valid request that the dataset, backend or version cannot execute. Neither is retryable.
- **Availability.** `ToolCapabilitySchema.degrade_to` is replaced by `available: bool`, computed at startup. The planner sees only available tools. P3 rejects plans that use an unavailable tool. Dispatching one returns `NOT_IMPLEMENTED` or `UNSUPPORTED` without running anything.
- **Recovery.** A REJECTED step is recovered only by a replan, which yields a new plan version validated against P1–P6 and recorded, or the run aborts. No router substitutes a tool, query, dataset, tenant, key, strategy or backend. Answer-changing reductions exist only as gate substitutions the caller allowed (ADR 0010).
- **NL graph.** The graph is built only with all fourteen components of §11.10. A missing model, agent, schema, checkpointer or backend is a startup error naming it. No stub or passthrough node is ever installed. If the NL interface is unavailable, `dgm_query` is registered with `available = false`. An `ASYNC` decision is always enqueued, and a failed submission is reported, never executed synchronously instead.

The non-interactive turn of `run_ask_from_source` still auto-confirms the top-ranked candidate. That is its declared contract, and the supervisor selects it explicitly; the other candidates are reported.

## Rationale

- A success that did not happen contaminates synthesis, coverage, retries and the knowledge base. A typed failure is recoverable by a replan and visible to the user.
- Replan keeps recovery bounded and auditable: every new call passes plan validation and appears in the plan history. Degrade swapped tools implicitly, inside the router.
- Option 3 would discard a recovery that never fabricates results.

## Consequences

- Implementation work: remove the stub paths in the supervisor tools and the NL graph (#129, #143); migrate the contracts and the router (#130).
- Tests assert, for every tool, that missing capability, missing configuration and missing grant are distinct from genuine empty results, and that no other tool is called in their place.
