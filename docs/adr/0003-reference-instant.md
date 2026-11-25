# ADR 0003 — One reference instant per query evaluation

- **Status:** Accepted
- **Date:** 2026-11-25
- **Issue:** #46 (blocks #47)
- **Spec sections:** §4.1, §4.2, §5.1, §6.3, §8.14, §10.4 (D33)

## Context

`Now`, the calendar windows `YTD`, `QTD`, `MTD`, the windows `ROLLING` and `TRAILING`, TTL rules, monitor deadlines and the default `as_of` all depend on the current time. The specification also states that the same `(q, π)` yields the same query DAG and uses the DAG node id as a cache key. With no definition of *which* instant a query sees, two expressions of one query could read different instants, and a cached result could be returned for an instant other than the one it was computed for.

## Options

1. **Dynamic `now`.** Each expression reads the clock when it is evaluated.
2. **Mandatory `as_of`.** The caller always supplies the instant; a query without one is rejected.
3. **Captured reference instant.** One instant is fixed at the start of the evaluation, overridable by an explicit `as_of`, and is part of snapshot and cache identity.

## Decision

Option 3, chosen by the maintainer.

### The reference instant τ

Every query evaluation has exactly one reference instant τ, of type Timestamp.

```
τ = TemporalContext.default_as_of                  if the caller supplies it
τ = floor(clock(), REFERENCE_GRANULARITY)          otherwise — captured once, before planning
```

- **Source.** `clock()` is the clock of the service that plans the query — not the client's clock and not the storage backend's `CURRENT_TIMESTAMP`. The captured value is passed to the backend as a literal or bound parameter; no backend clock function appears in emitted SQL.
- **Granularity.** τ is truncated to `REFERENCE_GRANULARITY`, a policy constant with default one second.
- **Time zone.** τ is an instant and has no zone. Calendar arithmetic needs one: `TemporalContext.time_zone` is an IANA zone name, default `UTC`, and fixes where a day, month, quarter and year begin.

### Everything relative uses τ

| Construct | Resolves to |
|---|---|
| `Now` | τ |
| Version and validity resolution with no `TemporalJoin` override | `version(T, key, τ)`, `valid(e, τ)` |
| `YTD`, `QTD`, `MTD` | from the start of the year, quarter or month containing τ in `time_zone`, to τ inclusive |
| `ROLLING(d)` | the interval (τ − d, τ] |
| `TRAILING(n, unit)` | the n complete calendar units immediately before the unit containing τ, in `time_zone` |
| TTL expiry and backfill eligibility of `PipelineArtifact` | comparisons against τ |
| `Monitor` deadlines | `τ > clock + within` |
| `YEAR`, `QUARTER`, `MONTH`, `DAY`, `HOUR` of a Timestamp | calendar fields in `time_zone` |

Two occurrences of `Now` in one query are the same value. A composed query (`Q_algebra`) has one τ for all its CTEs. A query executed asynchronously or streamed keeps the τ captured at submission, however long execution takes.

### Explicit `as_of`

Supplying `default_as_of` replaces the captured instant and nothing else: the query text, its DAG and its parameters are otherwise unchanged, and every row of the table above is then resolved against the supplied instant. Re-running a query with the `default_as_of` of an earlier evaluation reproduces that evaluation over the same data. `TemporalJoin(join, as_of, …)` still overrides version and validity resolution for its own sub-join; it does not change τ.

### Identity

```
query DAG id        built from the query with Now as a symbol — never from a clock reading.
                    The same (q, π) yields the same DAG at any time.

evaluation identity (q_dag_id, G_snapshot_version, τ, time_zone)

cache key           (q_dag_id, τ, time_zone)       when the query is τ-dependent
                    (q_dag_id)                     when it is not
```

A query is τ-dependent iff it uses any construct of the table above, including default version or validity resolution over a type that is versioned or carries a validity window. A cached result is valid only for the `G_snapshot_version` it was computed on.

## Rationale

- **Dynamic `now` is incorrect, not merely inconvenient.** A filter `window_end ≤ now` and a window `ROLLING(7 days)` in the same query could straddle midnight and disagree; a cache keyed by plan identity would serve stale answers.
- **Mandatory `as_of` is correct but pushes a parameter into every call.** Interactive and NL use would need a default anyway, and that default is exactly the captured instant.
- **A captured instant keeps determinism where it was claimed** — the DAG does not depend on time — and moves time into an explicit, recorded component of evaluation identity.

## Consequences

- `TemporalContext.default_as_of` becomes optional and `time_zone` is added.
- τ-dependent queries issued at different instants do not share a cache entry. `REFERENCE_GRANULARITY` bounds the loss: evaluations within the same granule share τ. The parameter equivalence of §8.15 (`as_of = t₁ ≡ as_of = t₂` when nothing changed in between) remains available to merge entries.
- Exporters must emit τ as a value. `CURRENT_TIMESTAMP`, `now()` and equivalents are not valid output.
- Cache keys across sessions are finalised in #47, which builds on this identity.

## Alternatives not taken

- **Option 1**, for the reasons above.
- **Option 2**, because a default is needed in practice and an optional `as_of` gives the same reproducibility when it matters.
- **Using the backend's transaction timestamp.** It differs by backend and is unavailable to the planner and the budget gate, which need the instant before any SQL runs.
