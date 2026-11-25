# ADR 0008 — Bounded concurrency for independent supervisor steps

- **Status:** Accepted
- **Date:** 2026-11-25
- **Issue:** #86 (depends on #82)
- **Spec sections:** §12.4, §12.5, §12.11, §10.4 (D38)

## Context

A supervisor plan is an ordered list of tool calls with explicit dependencies (§12.2). Several workflows (§12.6) issue N independent calls, for example one query per dataset. The specification had left execution order open. That left undefined how budget fractions, cancellation and partial results behave when calls overlap, and two implementations could produce different costs and orderings.

## Options

1. Strictly sequential execution.
2. All independent steps in parallel, without a limit.
3. **Bounded concurrency** over steps that the dependency graph shows to be independent.

## Decision

Option 3, chosen by the maintainer.

### Rules

- **Independence.** A step is *runnable* when every step in its `depends_on` is SUCCEEDED. Runnable steps have no pending dependency on one another and may run concurrently.
- **Limit.** At most `MAX_CONCURRENT_CALLS` calls (default 4) are RUNNING at once. When a slot is free, the runnable steps are dispatched in plan order.
- **Writes.** A step whose tool has `writes_kb = true` is never RUNNING at the same time as another such step.
- **Budget reservation.** Before dispatch, the step's child budget (§12.5) is *reserved*. A step is dispatched only if `spent + reserved + its reservation ≤ B.max_estimated_cost`; otherwise it waits. When a call finishes, its reservation is released and its `cost_actual` is added to `spent`.
- **Recovery barrier.** When a result routes to *degrade* or *replan* (§12.4), no new step is dispatched until every RUNNING call has finished; the recovery then sees a stable set of results. *Retry* re-dispatches only the failed call.
- **Abort and cancellation.** Abort and cancellation set the cancel token of every RUNNING call and mark every PENDING step CANCELLED. The run ends when all RUNNING calls have acknowledged. Results that completed before the cancellation are kept in the diagnostic.
- **Deterministic synthesis.** `synthesize` consumes results in plan order (by `call_id`), never in completion order. With deterministic tools the outcome, coverage and response are the same whatever the timing.

## Rationale

- Independent calls such as one query per dataset gain latency without changing results.
- The dependency list already in the plan states independence explicitly; nothing is inferred.
- Reservation keeps the total-budget invariant of §12.5 under concurrency. The barrier and plan-order synthesis keep recovery and the final answer independent of scheduling.
- Unbounded parallelism (option 2) gives up resource control. Strict sequencing (option 1) wastes the independence the plan declares.

## Consequences

- §12.4 replaces "steps run one at a time" with these rules; `select_step` dispatches up to the limit.
- §12.11's cancellation applies to every running call.
- `MAX_CONCURRENT_CALLS = 1` recovers the sequential behaviour exactly.
