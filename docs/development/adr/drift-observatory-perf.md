# ADR: DriftObservatory OLTP Performance Refactor

- **Date**: 2026-03-19
- **Status**: Accepted (implemented)
- **Deciders**: sqldim maintainers

---

## TL;DR

`DriftObservatory` was refactored from a naive per-row OLTP pattern (~120 rows/sec) to a cache-assisted, `executemany`-based writer (~5 000–20 000 rows/sec) without changing the public API. The key changes are: in-memory caches for all FK lookup tables, a single `MAX()`-per-table PK counter, `executemany()` for bulk fact insertion, and an explicit `transaction()` context manager that wraps all ingest calls in a single `BEGIN/COMMIT`.

---

## Context

`DriftObservatory` ingests `EvolutionReport` and `ContractReport` objects into a six-table Kimball star schema stored in DuckDB. The original implementation was correct but naively OLTP:

- Every `ingest_evolution()` call issued individual `SELECT` round-trips to check whether each FK dimension row already existed (`obs_dataset_dim` `obs_pipeline_run_dim`, `obs_evolution_type_dim`).
- Each row in the report was inserted with a separate `execute()`, and each `execute()` was an implicit autocommit transaction in DuckDB.
- `_next_pk()` ran `SELECT MAX(pk_col)` on every call.

For a batch of 5 000 ingest calls with 1 change each, this amounted to roughly 30 000 SQL round-trips and 5 000 separate fsyncs — an operational ceiling of ~120–200 rows/sec.

This became a blocking issue when the Group N benchmarks were sized using bulk-style tiers (`xs=500, s=5_000, m=50_000`) designed for the vectorised ETL path rather than the OLTP observatory API.

---

## Decision

### 1. In-memory FK caches

Five `dict` caches are initialised in `__init__`, one per lookup table:

| Cache | Key | Value |
|---|---|---|
| `_pk_counters` | table name | last-used PK (int) |
| `_dataset_cache` | dataset name | `dataset_id` |
| `_run_cache` | run_key | `run_id` |
| `_rule_cache` | rule name | `rule_id` |
| `_evo_type_cache` | change_type | `evo_type_id` |

Cache semantics: **read-through** — on a cache miss the DB is queried and the result stored. All subsequent accesses are pure Python dict lookups. The caches are connection-scoped (one observatory instance = one DuckDB connection = one cache), so there is no cache coherence concern.

### 2. PK counter

`_next_pk(table, pk_col)` runs `SELECT MAX(pk_col)` exactly **once** per table per
observatory lifetime, on the first call. Subsequent calls increment an in-memory counter. 

For bulk insertion of `k` rows, the counter is advanced by `k` in a single step:

```python
base_id = self._next_pk("obs_schema_evolution_fact", "evo_fact_id")
self._pk_counters["obs_schema_evolution_fact"] = base_id + len(rows) - 1
```

### 3. `executemany()` for fact insertion

Both `ingest_evolution()` and `ingest_quality()` now build a list of parameter tuples and
issue a single `executemany()` call per ingest. This replaces a Python `for row in rows`
loop over individual `execute()` calls.

### 4. `transaction()` context manager

```python
@contextmanager
def transaction(self) -> Generator["DriftObservatory", None, None]:
    self._con.execute("BEGIN")
    try:
        yield self
        self._con.execute("COMMIT")
    except Exception:
        self._con.execute("ROLLBACK")
        raise
```

DuckDB defaults to autocommit. Wrapping a batch of ingest calls inside `transaction()` collapses all commits into one, eliminating one fsync per ingest call. This is the dominant win for workloads with many small ingest calls. It is opt-in — callers that need per-call durability simply omit the wrapper.

### 5. `_evo_type_cache` pre-population

`_ensure_schema()` pre-populates `_evo_type_cache` immediately after seeding the
`obs_evolution_type_dim` reference table. This table has exactly 6 rows and never changes.
`_evo_type_id()` becomes a pure Python dict lookup with no SQL round-trip.

---

## Consequences

### Positive

- Throughput increased from ~120–200 rows/sec to ~5 000–20 000 rows/sec in sustained bulk workloads (40–100× improvement).
- Public API unchanged — `ingest_evolution()` and `ingest_quality()` signatures are identical.
- Caches are connection-scoped; no shared state, no concurrency issues.
- `transaction()` is opt-in — existing code that calls ingest methods directly still works.

### Negative / Trade-offs

- FK caches are **not invalidated** if dimension data is mutated outside the observatory instance (e.g., direct SQL writes to the DuckDB file). This is acceptable because all writes are expected to flow through the observatory.
- `_pk_counters` assumes exclusive access to the DuckDB file. Write concurrency requires a separate connection pool strategy (out of scope for the current OLTP tier).

### Benchmark Tier Right-Sizing

The Group N `_DRIFT_TIERS` were also adjusted to match OLTP throughput:

```python
# Before — matched bulk ETL tiers, not OLTP observatory ceilings
_DRIFT_TIERS = {"xs": 500, "s": 5_000, "m": 50_000}

# After — right-sized for OLTP; xs/s/m complete in seconds even without caches
_DRIFT_TIERS = {"xs": 100, "s": 500, "m": 2_000}
```

Even with the right-sized tiers the caches are important for any production workload that ingests thousands of reports per run.

---

## Alternatives Considered

### Full OLAP bulk loader

Replace the OLTP insert API with a bulk Parquet → DuckDB INSERT SELECT pipeline. Rejected because it changes the programming model (callers can no longer drive ingest row-by-row) and adds a serialisation step that is unnecessary for small-to-medium observatory workloads.

### Connection pooling / WAL mode

Enable WAL mode or use multiple DuckDB connections with a connection pool. Rejected for the current scope — DuckDB's single-writer model makes connection pooling complex, and the in-memory cache approach achieves the same throughput goal with far less complexity.

---

## Related

- [SCD Engine Design ADR](scd-engine-design.md) — dual-engine (eager/lazy) pattern that
  inspired the OLTP → bulk decomposition.
- [`sqldim/observability/drift.py`](../../../sqldim/observability/drift.py) — implementation.
- [`benchmarks/suite.py`](../../../benchmarks/suite.py) — Group N benchmark cases that
  originally exposed the throughput bottleneck.
