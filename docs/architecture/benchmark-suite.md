# ADR: Benchmark Suite — Groups A–S

- **Date**: 2026-03-19
- **Status**: Accepted (all 2677 tests passing; A–S groups)
- **Deciders**: sqldim maintainers

---

## TL;DR

The sqldim benchmark suite spans 19 groups (A–S) organised in three domain modules. Each case is a self-contained function that generates a dataset, runs the pipeline under memory and scan probes, and returns a `BenchmarkResult`. This ADR documents the design rationale, the scale-tier model, lessons learned from the Group N hang investigation, and the Group O throughput measurement semantics for aggregating/windowing queries.

---

## Context

The benchmark suite exists to answer three questions for every major feature path:

1. **Does it regress?** Scan-count assertions catch VIEW vs TABLE regressions and unexpected plan changes.
2. **Does it scale?** Throughput (rows/sec) is measured across `xs / s / m` tiers to verify linear or better scaling.
3. **Is memory safe?** `MemoryProbe` samples RSS and available system memory every 200 ms and raises a safety abort if resident memory exceeds the configured ceiling or available system memory drops below the floor.

---

## Architecture

### Probes

| Probe | Class | Mechanism |
|---|---|---|
| Memory | `MemoryProbe` | Background thread; 200 ms sample interval; RSS + `psutil.virtual_memory()` |
| Scan | `ScanProbe` (`DuckDBObjectTracker`) | Counts DuckDB table/view scans before and after the workload |

Both probes are started before the workload and stopped after. Their output is merged into `BenchmarkResult`.

`MemoryProbe` exposes a module-level `_HARD_ABORT_EVENT` (`threading.Event`). It is set by either a Ctrl+C interrupt or an RSS safety breach. It **persists** until explicitly cleared — callers that run multiple sub-cases in sequence must call `MemoryProbe.reset_hard_abort()` at the start of each sub-case (see Group N Hang below).

### Scale tiers

The default `SCALE_TIERS` from `benchmarks/dataset_gen.py` apply to most groups:

| Tier | Rows |
|---|---|
| `xs` | 10 000 |
| `s` | 100 000 |
| `m` | 1 000 000 |
| `l` | 5 000 000 |
| `xl` | 20 000 000 |
| `xxl` | 60 000 000 |

Groups that deviate from the default have their own tier dict and an inline comment explaining why.

### `BenchmarkResult`

Every case returns one `BenchmarkResult` dataclass (defined in `suite.py`). Key fields:

| Field | Type | Description |
|---|---|---|
| `case_id` | `str` | Unique identifier, e.g. `"O-b1b2b3-full-m"` |
| `group` | `str` | Single letter, e.g. `"O"` |
| `tier` | `str` | `"xs"`, `"s"`, or `"m"` |
| `n_rows` | `int` | Input dataset size |
| `wall_s` | `float` | Wall-clock time in seconds |
| `rows_per_sec` | `float` | Throughput (see §Throughput Semantics) |
| `peak_rss_gb` | `float` | Peak resident set size |
| `scan_count` | `int` | Number of DuckDB table/view scans recorded |
| `inserted` | `int` | Fact rows written (or output rows for query benchmarks) |

---

## Group Inventory

Groups are split across three modules under `benchmarks/groups/`:  
`scd.py` (A–I) · `model.py` (J–N) · `dgm.py` (O–S)

| Group | Module | Cases | What is measured |
|---|---|---|---|
| A | scd | 3 | VIEW vs TABLE regression — scan count assertions |
| B | scd | 18 | Memory safety across all processors |
| C | scd | 3 | Throughput scaling: rows/sec by tier |
| D | scd | 3 | Streaming vs batch ingest comparison |
| E | scd | 3 | Change-rate sensitivity |
| F | scd | 9 | Processor comparison: SCD2 vs Metadata vs Type6 |
| G | scd | 3 | Beyond-memory / spill-to-disk simulation |
| H | scd | 6 | Source adapter: Parquet vs CSV |
| I | scd | 6 | SCD Type3 and Type4 processor throughput |
| J | model | 3 | Prebuilt dimension generation (DateDimension / TimeDimension) |
| K | model | 3 | Graph traversal and dimensional query builder |
| L | model | 3 | Narwhals SCD2 backfill throughput |
| M | model | 2 | ORM loader throughput and Medallion registry compute |
| N | model | 9 | Schema/quality drift observability (DriftObservatory star schema) |
| O | dgm | 12 | DGM three-band query builder (B1, B1∘B2, B1∘B3, B1∘B2∘B3) |
| P | dgm | — | BDD predicate evaluation throughput |
| Q | dgm | — | Recommender engine query throughput |
| R | dgm | — | Planner traversal throughput |
| S | dgm | — | Exporter serialisation throughput |

---

## Group N: DriftObservatory — Design and Hang Post-Mortem

### Sub-cases

| Sub-case | Tiers | What is measured |
|---|---|---|
| N-1 | xs / s / m | `ingest_evolution()` throughput across OLTP-right-sized tiers |
| N-2 | xs / s / m | `ingest_quality()` throughput across same tiers |
| N-3 | xs / s / m | Gold-layer analytical query latency (6 queries × 3 tiers) |

### OLTP Tier Sizing

`DriftObservatory` is an OLTP star-schema writer, not a bulk loader. Its throughput ceiling (before and after the performance refactor) is fundamentally different from the vectorised SCD2 path:

| Component | Throughput ceiling |
|---|---|
| Vectorised SCD2 (`LazySCDProcessor`) | 500K–5M rows/sec |
| `DriftObservatory` pre-refactor | ~120–200 rows/sec |
| `DriftObservatory` post-refactor (caches + executemany) | ~5 000–20 000 rows/sec |

The original `_DRIFT_TIERS = {"xs": 500, "s": 5_000, "m": 50_000}` were copied from bulk tiers. At 200 rows/sec, m=50 000 would take ~4 minutes with the original implementation.

**Fix**: right-sized to `{"xs": 100, "s": 500, "m": 2_000}`. See the [DriftObservatory OLTP Performance ADR](drift-observatory-perf.md) for the full
throughput improvement details.

### `_HARD_ABORT_EVENT` Poisoning

`MemoryProbe._HARD_ABORT_EVENT` is a module-level `threading.Event`. It is set by Ctrl+C or an RSS breach and **persists across sub-cases** until explicitly cleared. This caused a silent `SKIPPED: Query interrupted` result for all subsequent N sub-cases after any interruption of a prior one.

**Fix**: `MemoryProbe.reset_hard_abort()` is called at the start of each N sub-case loop. This pattern should be followed for any benchmark group that runs multiple independent sub-cases within a single function.

### N-3 Gold-Query Seeding

The original N-3 sub-case seeded 2 000 evolution and 2 000 quality records with individual `ingest_evolution(report, [1 change])` calls — 4 000 autocommit transactions. At 200 tx/sec this took ~20 seconds just for setup.

**Fix**: seeding is batched into 10 calls × 200 items wrapped in `obs.transaction()`.

---

## Group O: DGM Query Builder — Throughput Semantics

### Sub-cases

| Sub-case | Bands | SQL shape | `rows_per_sec` basis |
|---|---|---|---|
| O-1 (`O-b1-filter`) | B1 only | Filter + return | `len(output_rows) / wall_s` |
| O-2 (`O-b1b2-having`) | B1 ∘ B2 | GROUP BY + HAVING | `len(output_rows) / wall_s` |
| O-3 (`O-b1b3-qualify`) | B1 ∘ B3 | Window + QUALIFY | `len(output_rows) / wall_s` |
| O-4 (`O-b1b2b3-full`) | B1 ∘ B2 ∘ B3 | GROUP BY + Window + `QUALIFY rnk <= 10` | `n / wall_s` (input rows) |

### Why O-4 Uses Input Rows

O-4 uses `QUALIFY rnk <= 10`. This limits output to at most 10 rows per partition
regardless of input size — output is **constant** relative to `n`. Using `len(output_rows) / wall_s` would yield a misleading ~3 000 rows/sec even at `m=100 000` input rows.

The correct interpretation for aggregating/ranking queries where output does not scale with input is to measure **input rows processed per second**: `rows_per_sec = n / wall_s`.

For O-1, O-2, O-3 the output scales proportionally with `n`:

| Sub-case | Output at n=1 000 | Output at n=100 000 |
|---|---|---|
| O-1 | ~1 000 rows | ~100 000 rows |
| O-2 | ~90 rows (n_dim ≈ n/10 after GROUP BY) | ~9 000 rows |
| O-3 | ~100 rows (n_dim = n/10) | ~10 000 rows |
| O-4 | 10 rows (`QUALIFY rnk<=10`) | 10 rows |

This makes `len(output_rows) / wall_s` valid for O-1, O-2, O-3 and invalid for O-4.

**Rule of thumb**: if your query semantically collapses output to a constant or
sub-linear number of rows regardless of input size, measure `n / wall_s` (input
throughput) and record `result.inserted = len(output_rows)` for auditing.

### Verified Results (post-fix)

| Case | Tier | n | rows/sec |
|---|---|---|---|
| O-b1-filter | m | 100 000 | ~931 K/s |
| O-b1b2-having | m | 100 000 | ~516 K/s |
| O-b1b3-qualify | m | 100 000 | ~957 K/s |
| O-b1b2b3-full | m | 100 000 | ~11.2 M/s |

O-b1b2b3-full at 11.2 M/s reflects that DuckDB executes the full three-band pipeline (GROUP BY → window function → QUALIFY) in a single pass over a 100 000-row relation in under 10 ms — consistent with DuckDB's columnar vectorised execution engine.

---

## Group G: Spill-to-Disk — Design Note

Group G benchmarks the `SPILL_LIMIT_GB` configuration. The current threshold of `0.5 GB` is too high relative to the actual CNPJ working sets on a 12.5 GB machine: all three tiers (`xs`, `s`, `m`) complete comfortably within the limit and no spill is ever triggered.

This is a **hardware/sizing observation**, not a code bug. To exercise the spill path:
- Lower `SPILL_LIMIT_GB` to `~0.1 GB`, or
- Increase tier sizes so working sets genuinely exceed the threshold.

Neither change has been made; this note documents the gap for future work.

---

## Running the Suite

```bash
# All groups
python3 -m benchmarks.runner

# Single group
python3 -m benchmarks.runner --groups N
python3 -m benchmarks.runner --groups O

# Multiple groups
python3 -m benchmarks.runner --groups K L M N O
```

The runner prints a per-case table and a summary line:

```
Passed: 72  Failed: 0
Best: 11.2M/s [O-b1b2b3-full-m]   Worst: 20K/s [O-b1b2-having-xs]
```

---

## Related

- [DriftObservatory OLTP Performance ADR](drift-observatory-perf.md) — detailed write-up
  of the `DriftObservatory` refactor that unblocked Group N.
- [`benchmarks/suite.py`](../../../benchmarks/suite.py) — thin hub that re-exports from `benchmarks/infra.py` and `benchmarks/groups/`.
- [`benchmarks/infra.py`](../../../benchmarks/infra.py) — shared infrastructure (`BenchmarkResult`, probes, helpers).
- [`benchmarks/groups/scd.py`](../../../benchmarks/groups/scd.py) — Groups A–I.
- [`benchmarks/groups/model.py`](../../../benchmarks/groups/model.py) — Groups J–N.
- [`benchmarks/groups/dgm.py`](../../../benchmarks/groups/dgm.py) — Groups O–S.
- [`benchmarks/runner.py`](../../../benchmarks/runner.py) — CLI runner.
- [`benchmarks/memory_probe.py`](../../../benchmarks/memory_probe.py) — RSS probe and
  `_HARD_ABORT_EVENT`.
- [`sqldim/observability/drift.py`](../../../sqldim/observability/drift.py) — DriftObservatory
  implementation.
