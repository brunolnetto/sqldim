# ADR 0004 — Cache invalidation by source watermark

- **Status:** Accepted; amended by ADR 0011 (unversioned sources)
- **Date:** 2026-11-25
- **Issue:** #48
- **Spec sections:** §2.6, §8.14, §10.4 (D34)

## Context

Result caching (§8.14) keys a result by the question, its parameters, the reference instant and the snapshot. A cached result must not be served once the data it was computed from has changed. The specification had suggested invalidating an entry "when any node or edge referenced by the question has changed", which presupposes fine-grained provenance for every intermediate result. Nothing in the specification produces or stores such provenance.

## Options

1. **Fine-grained provenance.** Each result records the entities and edges it read; an entry is invalidated when one of them changes.
2. **Watermark per source.** Each source relation, or partition, has a version identifier that advances on every committed change. An entry records the watermarks of the sources it read and stays valid while none of them advances.
3. **Conservative TTL.** Entries expire after a fixed time, without tracking dependencies.

## Decision

Option 2, chosen by the maintainer, with option 3 as the fallback for sources that have no watermark.

### Definitions

- **Source.** A relation named by the mapping M (§2.6): the NodeTable of a node type, the RelationTable of an edge type, or a base relation of a Derived view. For a partitioned relation, each partition is a source.
- **Watermark** `w(src)`. A value that changes whenever a committed change to `src` becomes visible, and never otherwise. Examples are a Delta table version, an Iceberg snapshot id, or a commit counter maintained by the loader. A source for which the host provides no such value is **unversioned**.
- **Dependency set** `deps(plan)`. Computed statically from the physical plan. It contains every source the plan can read: relations of every node and edge type in its path patterns (including inside EXISTS and Repeat), the NodeTables used for version resolution, and the base relations of Derived views. When partition pruning applies, it contains only the partitions that survive pruning. Statistics used only to choose the plan are not dependencies.

### Rule

```
result_key  = H(question_key, W, τ?, time_zone?)          -- τ and time_zone only if τ-dependent (§5.1)
W           = sorted list of (src, w(src)) for src ∈ deps(plan)

hit  iff  an entry with the current result_key exists  and  deps(plan) contains no unversioned source
```

When a dependency advances, `W` changes, the key changes and the old entry is simply not found. Old entries are evicted by capacity or age; correctness does not depend on eviction.

**Unversioned sources.** If `deps(plan)` contains an unversioned source, the entry is stored with an expiry of `CACHE_TTL_UNVERSIONED`. It is served only before that expiry, and with the flag `possibly_stale = true` in the result metadata.

> **Amended by ADR 0011.** The TTL fallback above is replaced: an entry that depends on an unversioned source is served only when the caller sets `CachePolicy.accept_stale`, within that age, and flagged `possibly_stale = true`. By default the lookup misses and the plan runs.

**Non-deterministic plans.** A plan with sampling (SAMPLE) includes its seed in the parameters, so equal keys denote equal results.

### Correctness

If `W` is unchanged, no source in `deps(plan)` has a visible change. The plan reads only sources in `deps(plan)`. Deterministic evaluation over unchanged inputs gives the same result, so a hit returns exactly what re-execution would return. Unversioned sources are the only exception, and they are flagged.

### Example

`q = revenue by region`, reading `fact_sale` (partitioned by month) and `dim_customer`. With pruning to 2024, `deps = {fact_sale/2024-01 … fact_sale/2024-12, dim_customer}`. A load into `fact_sale/2025-01` advances no watermark in `deps`, so the cached 2024 result stays valid. An SCD2 update of `dim_customer` advances its watermark, so the next lookup misses and recomputes.

## Rationale

- Implementable now: table formats and loaders already expose versions; nothing new is recorded per row.
- Sound by construction under the stated assumption on watermarks, with an explicit, flagged fallback.
- Coarser than option 1: a change to an unrelated row of a source invalidates the entry. This is accepted for the MVP.

## Consequences

- `result_key` in §8.14 uses the dependency watermarks `W` in place of a global snapshot version. Two questions over disjoint sources do not invalidate each other.
- The planner must compute `deps(plan)` for every plan it caches.
- Fine-grained provenance (option 1) remains possible as a later refinement, provided the execution records what it read. It is adopted only with a demonstration that the recorded set covers everything that can affect the result.
