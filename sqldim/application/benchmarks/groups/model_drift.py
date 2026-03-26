"""Model drift benchmarks (group N): schema drift and type coercion."""

from __future__ import annotations

import datetime as _dt
import time
import traceback as _traceback


from sqldim.application.benchmarks._dataset import (
    BenchmarkDatasetGenerator,
)
from sqldim.application.benchmarks.infra import (
    BenchmarkResult,
    _select_tiers,
)
from sqldim.application.benchmarks.memory_probe import MemoryProbe
from sqldim.contracts.engine import EvolutionChange, EvolutionReport

try:
    from sqldim.contracts.reporting.report import ContractReport, ContractViolation
except ImportError:  # module not yet released; benchmark will skip at runtime
    ContractReport = ContractViolation = None  # type: ignore[assignment,misc]
from sqldim.observability.drift import DriftObservatory

# Group N — Observability drift pipeline throughput
# ─────────────────────────────────────────────────────────────────────────────
#
# Measures the cost of treating schema/quality drift as first-class Kimball
# facts inside sqldim's own observability pipeline.  Three sub-cases:
#
#   N-drift-ingest   : Batch-insert k EvolutionReport events into the star schema
#   N-quality-ingest : Batch-insert k ContractReport violation events
#   N-drift-query    : Execute all five gold-layer analytical queries (×100 reps)

# DriftObservatory is an OLTP star-schema writer (~120–300 rows/sec ceiling).
# These tiers reflect realistic operational ingest volumes, not bulk-load scale.
_DRIFT_TIERS = {"xs": 100, "s": 500, "m": 2_000}

_N_DATASETS = [f"dim_{i}" for i in range(20)]
_N_CHANGE_TYPES = ["added", "widened", "narrowed", "type_changed", "renamed", "removed"]
_N_SEVERITIES = ["error", "warning", "info"]
_N_RULES = ["not_null", "unique", "range_check", "freshness", "regex_match"]
_N_BASE_TS = _dt.datetime(2026, 1, 1, tzinfo=_dt.timezone.utc)
_N_BATCH = 100


def _n_classify_change(ct: str, rep: EvolutionReport, ch: EvolutionChange) -> None:
    if ct == "added":
        rep.safe_changes.append(ch)
    elif ct == "widened":
        rep.additive_changes.append(ch)
    else:
        rep.breaking_changes.append(ch)


def _n1_drift_ingest_tier(tier: str, n: int, temp_dir: str) -> BenchmarkResult:
    cid = f"N-drift-ingest-{tier}"
    result = BenchmarkResult(
        case_id=cid,
        group="N",
        profile="drift-ingest",
        tier=tier,
        processor="DriftObservatory",
        sink="duckdb-memory",
        source="synthetic",
        phase="bulk-insert",
        n_rows=n,
        n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        obs = DriftObservatory.in_memory()
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            with obs.transaction():
                for run_seq in range(0, n, _N_BATCH):
                    rep = EvolutionReport()
                    for j in range(run_seq, min(run_seq + _N_BATCH, n)):
                        ct = _N_CHANGE_TYPES[j % len(_N_CHANGE_TYPES)]
                        ch = EvolutionChange(ct, f"col_{j % 50}", f"detail {j}")
                        _n_classify_change(ct, rep, ch)
                    obs.ingest_evolution(
                        rep,
                        dataset=_N_DATASETS[run_seq % len(_N_DATASETS)],
                        run_id=f"run-{run_seq // _N_BATCH:05d}",
                        layer="silver",
                        detected_at=_N_BASE_TS + _dt.timedelta(hours=run_seq),
                    )
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec = n / max(result.wall_s, 0.001)
        result.inserted = n
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    return result


def _n2_quality_ingest_tier(tier: str, n: int, temp_dir: str) -> BenchmarkResult:
    cid = f"N-quality-ingest-{tier}"
    result = BenchmarkResult(
        case_id=cid,
        group="N",
        profile="quality-ingest",
        tier=tier,
        processor="DriftObservatory",
        sink="duckdb-memory",
        source="synthetic",
        phase="bulk-insert",
        n_rows=n,
        n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        obs = DriftObservatory.in_memory()
        probe = MemoryProbe(temp_dir=temp_dir, label=cid)
        with probe:
            t0 = time.perf_counter()
            with obs.transaction():
                for run_seq in range(0, n, _N_BATCH):
                    viols = [
                        ContractViolation(
                            rule=_N_RULES[j % len(_N_RULES)],
                            severity=_N_SEVERITIES[j % len(_N_SEVERITIES)],
                            count=j % 100,
                            detail=f"detail {j}",
                        )
                        for j in range(run_seq, min(run_seq + _N_BATCH, n))
                    ]
                    rpt = ContractReport(
                        violations=viols,
                        view=_N_DATASETS[run_seq % len(_N_DATASETS)],
                        elapsed_s=0.01,
                    )
                    obs.ingest_quality(
                        rpt,
                        dataset=_N_DATASETS[run_seq % len(_N_DATASETS)],
                        run_id=f"run-{run_seq // _N_BATCH:05d}",
                        layer="silver",
                        checked_at=_N_BASE_TS + _dt.timedelta(hours=run_seq),
                    )
            result.wall_s = time.perf_counter() - t0
        m = probe.report
        result.peak_rss_gb = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec = n / max(result.wall_s, 0.001)
        result.inserted = n
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    return result


def _n3_setup_evolution_data(obs: DriftObservatory) -> None:
    with obs.transaction():
        for run_seq in range(0, 1_000, _N_BATCH):
            rep = EvolutionReport()
            for j in range(run_seq, run_seq + _N_BATCH):
                ct = _N_CHANGE_TYPES[j % len(_N_CHANGE_TYPES)]
                ch = EvolutionChange(ct, f"col_{j % 50}", "")
                _n_classify_change(ct, rep, ch)
            obs.ingest_evolution(
                rep,
                dataset=_N_DATASETS[run_seq % len(_N_DATASETS)],
                run_id=f"run-{run_seq // _N_BATCH:04d}",
                layer="silver",
                detected_at=_N_BASE_TS + _dt.timedelta(hours=run_seq),
            )


def _n3_setup_quality_data(obs: DriftObservatory) -> None:
    with obs.transaction():
        for run_seq in range(0, 1_000, _N_BATCH):
            viols = [
                ContractViolation(
                    rule=_N_RULES[j % len(_N_RULES)],
                    severity=_N_SEVERITIES[j % len(_N_SEVERITIES)],
                    count=j % 200,
                    detail="",
                )
                for j in range(run_seq, run_seq + _N_BATCH)
            ]
            rpt = ContractReport(
                violations=viols,
                view=_N_DATASETS[run_seq % len(_N_DATASETS)],
            )
            obs.ingest_quality(
                rpt,
                dataset=_N_DATASETS[run_seq % len(_N_DATASETS)],
                run_id=f"run-{run_seq // _N_BATCH:04d}",
                layer="silver",
                checked_at=_N_BASE_TS + _dt.timedelta(hours=run_seq),
            )


def _n3_gold_queries(temp_dir: str) -> BenchmarkResult:
    cid = "N-drift-gold-queries"
    result = BenchmarkResult(
        case_id=cid,
        group="N",
        profile="drift-gold",
        tier="n/a",
        processor="DriftObservatory",
        sink="duckdb-memory",
        source="synthetic",
        phase="query",
        n_rows=0,
        n_changed=0,
    )
    try:
        MemoryProbe.check_safe_to_run(label=cid)
        obs = DriftObservatory.in_memory()
        _n3_setup_evolution_data(obs)
        _n3_setup_quality_data(obs)
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
        result.n_rows = n_queries
        result.peak_rss_gb = m.peak_rss_gb
        result.min_sys_avail_gb = m.min_sys_avail_gb
        result.rows_per_sec = n_queries / max(result.wall_s, 0.001)
    except RuntimeError as exc:
        result.ok = False
        result.error = f"SKIPPED: {exc}"
    except Exception as exc:
        result.ok = False
        result.error = f"{type(exc).__name__}: {exc}\n" + _traceback.format_exc()[-600:]
    return result


def group_n_drift_observatory(
    gen: BenchmarkDatasetGenerator,
    temp_dir: str,
    max_tier: str = "s",
    **_,
) -> list[BenchmarkResult]:
    """Group N — Schema/quality drift observability pipeline throughput."""
    tier_order = ["xs", "s", "m"]
    active_tiers = _select_tiers(tier_order, _DRIFT_TIERS, max_tier)
    results: list[BenchmarkResult] = []

    for tier in active_tiers:
        MemoryProbe.reset_hard_abort()
        results.append(_n1_drift_ingest_tier(tier, _DRIFT_TIERS[tier], temp_dir))

    for tier in active_tiers:
        MemoryProbe.reset_hard_abort()
        results.append(_n2_quality_ingest_tier(tier, _DRIFT_TIERS[tier], temp_dir))

    results.append(_n3_gold_queries(temp_dir))
    return results
