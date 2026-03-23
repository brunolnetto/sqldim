"""Model drift benchmarks (group N): schema drift and type coercion."""
from __future__ import annotations

import os
import time
import traceback as _traceback

import duckdb
from sqlalchemy.pool import StaticPool

from sqldim.application.benchmarks._dataset import (
    BenchmarkDatasetGenerator,
    DatasetArtifact,
    SCALE_TIERS,
)
from sqldim.application.benchmarks.infra import (
    BenchmarkResult,
    SOURCE_NAMES,
    _make_source,
    _remove_db,
    _configure,
    _run_scd2_batch,
    _run_metadata_batch,
)
from sqldim.application.benchmarks.memory_probe import MemoryProbe
from sqldim.application.benchmarks.scan_probe import DuckDBObjectTracker

def group_n_drift_observatory(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "s",
    **_,
) -> list[BenchmarkResult]:
    """Group N — Schema/quality drift observability pipeline throughput."""
    import datetime as _dt
    from sqldim.contracts.engine import EvolutionChange, EvolutionReport
    from sqldim.contracts.report import ContractReport, ContractViolation
    from sqldim.observability.drift import DriftObservatory

    tier_order    = ["xs", "s", "m"]
    effective_max = max_tier if max_tier in tier_order else tier_order[-1]
    active_tiers  = [t for t in tier_order
                     if t in _DRIFT_TIERS
                     and tier_order.index(t) <= tier_order.index(effective_max)]
    results: list[BenchmarkResult] = []

    _datasets     = [f"dim_{i}" for i in range(20)]
    _change_types = ["added", "widened", "narrowed", "type_changed", "renamed", "removed"]
    _severities   = ["error", "warning", "info"]
    _rules        = ["not_null", "unique", "range_check", "freshness", "regex_match"]
    _base_ts      = _dt.datetime(2026, 1, 1, tzinfo=_dt.timezone.utc)

    # ── N-1: Evolution fact ingest throughput ────────────────────────────
    for tier in active_tiers:
        MemoryProbe.reset_hard_abort()  # fresh observatory per case; clear any prior signal
        n     = _DRIFT_TIERS[tier]
        cid   = f"N-drift-ingest-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="N", profile="drift-ingest", tier=tier,
            processor="DriftObservatory", sink="duckdb-memory",
            source="synthetic", phase="bulk-insert", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            obs   = DriftObservatory.in_memory()
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            # Batch 100 changes per EvolutionReport call — one call per run_id
            # block (matching the existing run-NNNNN grouping).  This avoids
            # 5K individual Python→DuckDB round-trips in favour of n//100 calls
            # each flushing 100 facts via executemany.
            _BATCH = 100
            with probe:
                t0 = time.perf_counter()
                with obs.transaction():
                    for run_seq in range(0, n, _BATCH):
                        rep = EvolutionReport()
                        for j in range(run_seq, min(run_seq + _BATCH, n)):
                            ct = _change_types[j % len(_change_types)]
                            ch = EvolutionChange(ct, f"col_{j % 50}", f"detail {j}")
                            if ct == "added":
                                rep.safe_changes.append(ch)
                            elif ct == "widened":
                                rep.additive_changes.append(ch)
                            else:
                                rep.breaking_changes.append(ch)
                        obs.ingest_evolution(
                            rep,
                            dataset=_datasets[run_seq % len(_datasets)],
                            run_id=f"run-{run_seq // _BATCH:05d}",
                            layer="silver",
                            detected_at=_base_ts + _dt.timedelta(hours=run_seq),
                        )
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.rows_per_sec     = n / max(result.wall_s, 0.001)
            result.inserted         = n
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

    # ── N-2: Quality drift fact ingest throughput ────────────────────────
    for tier in active_tiers:
        MemoryProbe.reset_hard_abort()  # fresh observatory per case; clear any prior signal
        n     = _DRIFT_TIERS[tier]
        cid   = f"N-quality-ingest-{tier}"
        result = BenchmarkResult(
            case_id=cid, group="N", profile="quality-ingest", tier=tier,
            processor="DriftObservatory", sink="duckdb-memory",
            source="synthetic", phase="bulk-insert", n_rows=n, n_changed=0,
        )
        try:
            MemoryProbe.check_safe_to_run(label=cid)
            obs   = DriftObservatory.in_memory()
            probe = MemoryProbe(temp_dir=temp_dir, label=cid)
            _BATCH = 100
            with probe:
                t0 = time.perf_counter()
                with obs.transaction():
                    for run_seq in range(0, n, _BATCH):
                        viols = [
                            ContractViolation(
                                rule=_rules[j % len(_rules)],
                                severity=_severities[j % len(_severities)],
                                count=j % 100,
                                detail=f"detail {j}",
                            )
                            for j in range(run_seq, min(run_seq + _BATCH, n))
                        ]
                        rpt = ContractReport(
                            violations=viols,
                            view=_datasets[run_seq % len(_datasets)],
                            elapsed_s=0.01,
                        )
                        obs.ingest_quality(
                            rpt,
                            dataset=_datasets[run_seq % len(_datasets)],
                            run_id=f"run-{run_seq // _BATCH:05d}",
                            layer="silver",
                            checked_at=_base_ts + _dt.timedelta(hours=run_seq),
                        )
                result.wall_s = time.perf_counter() - t0
            m = probe.report
            result.peak_rss_gb      = m.peak_rss_gb
            result.min_sys_avail_gb = m.min_sys_avail_gb
            result.rows_per_sec     = n / max(result.wall_s, 0.001)
            result.inserted         = n
        except RuntimeError as exc:
            result.ok = False; result.error = f"SKIPPED: {exc}"
        except Exception as exc:
            result.ok    = False
            result.error = (f"{type(exc).__name__}: {exc}\n"
                            + _traceback.format_exc()[-600:])
        results.append(result)

    # ── N-3: Gold-layer analytical query planning + execution ────────────
    cid   = "N-drift-gold-queries"
    result = BenchmarkResult(
        case_id=cid, group="N", profile="drift-gold", tier="n/a",
        processor="DriftObservatory", sink="duckdb-memory",
        source="synthetic", phase="query", n_rows=0, n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        obs = DriftObservatory.in_memory()
        _SEED_BATCH = 100
        with obs.transaction():
            for run_seq in range(0, 1_000, _SEED_BATCH):
                rep = EvolutionReport()
                for j in range(run_seq, run_seq + _SEED_BATCH):
                    ct  = _change_types[j % len(_change_types)]
                    ch  = EvolutionChange(ct, f"col_{j % 50}", "")
                    if ct == "added":
                        rep.safe_changes.append(ch)
                    elif ct == "widened":
                        rep.additive_changes.append(ch)
                    else:
                        rep.breaking_changes.append(ch)
                obs.ingest_evolution(rep, dataset=_datasets[run_seq % len(_datasets)],
                                     run_id=f"run-{run_seq // _SEED_BATCH:04d}", layer="silver",
                                     detected_at=_base_ts + _dt.timedelta(hours=run_seq))
        with obs.transaction():
            for run_seq in range(0, 1_000, _SEED_BATCH):
                viols = [
                    ContractViolation(rule=_rules[j % len(_rules)],
                                      severity=_severities[j % len(_severities)],
                                      count=j % 200, detail="")
                    for j in range(run_seq, run_seq + _SEED_BATCH)
                ]
                rpt = ContractReport(violations=viols, view=_datasets[run_seq % len(_datasets)])
                obs.ingest_quality(rpt, dataset=_datasets[run_seq % len(_datasets)],
                                   run_id=f"run-{run_seq // _SEED_BATCH:04d}", layer="silver",
                                   checked_at=_base_ts + _dt.timedelta(hours=run_seq))

        repeats = 100
        n_queries = repeats * 5
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            for _ in range(repeats):
                obs.breaking_change_rate().fetchall()
                obs.worst_quality_datasets(top_n=5).fetchall()
                obs.drift_velocity(bucket="week").fetchall()
                obs.migration_backlog().fetchall()
                obs.rule_failure_heatmap().fetchall()
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.n_rows           = n_queries
        result.peak_rss_gb      = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec     = n_queries / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False; result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok    = False
        result.error = (f"{type(exc).__name__}: {exc}\n"
                        + _traceback.format_exc()[-600:])
    results.append(result)

    return results


# ═══════════════════════════════════════════════════════════════════════════
# ── Group O  DGM three-band query builder ────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

_DGM_TIERS: dict[str, int] = {"xs": 1_000, "s": 10_000, "m": 100_000}


