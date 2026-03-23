"""SCD regression benchmarks (groups A–C): scan, memory safety, throughput."""
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

def group_a_scan_regression(gen: BenchmarkDatasetGenerator, temp_dir: str, **_) -> list[BenchmarkResult]:
    results = []
    for profile in ["products", "employees", "saas_users"]:
        ds = gen.generate(profile, tier="xs")
        results.append(_run_scd2_batch(ds, f"A_{profile}_scan_regression", "A_scan_regression", "xs", temp_dir))
        ds.cleanup()
    ds = gen.generate("cnpj_empresa", tier="xs")
    results.append(_run_metadata_batch(ds, "A_cnpj_metadata_scan_regression", "A_scan_regression", "xs", temp_dir))
    ds.cleanup()
    return results


# ── Group B — Memory safety ───────────────────────────────────────────────

def group_b_memory_safety(gen: BenchmarkDatasetGenerator, temp_dir: str, **_) -> list[BenchmarkResult]:
    results = []
    cases = [
        ("products",     "s",  "scd2"),
        ("products",     "m",  "scd2"),
        ("employees",    "s",  "scd2"),
        ("employees",    "m",  "scd2"),
        ("saas_users",   "s",  "scd2"),
        ("saas_users",   "m",  "scd2"),
        ("cnpj_empresa", "s",  "meta"),
        ("cnpj_empresa", "m",  "meta"),
    ]
    for profile, tier, proc_type in cases:
        ds = gen.generate(profile, tier=tier)
        cid = f"B_{profile}_{tier}_memory"
        r = (_run_metadata_batch(ds, cid, "B_memory_safety", tier, temp_dir)
             if proc_type == "meta"
             else _run_scd2_batch(ds, cid, "B_memory_safety", tier, temp_dir))
        ds.cleanup(); results.append(r)
    return results


# ── Group C — Throughput scaling ─────────────────────────────────────────

def group_c_throughput_scaling(gen: BenchmarkDatasetGenerator, temp_dir: str, max_tier: str = "m", **_) -> list[BenchmarkResult]:
    tier_order = list(SCALE_TIERS.keys())
    max_idx    = tier_order.index(max_tier)
    results    = []
    for tier in tier_order[: max_idx + 1]:
        try:
            MemoryProbe.check_safe_to_run(f"C_{tier}")
        except RuntimeError as e:
            results.append(BenchmarkResult(
                case_id=f"C_products_{tier}_throughput", group="C_throughput",
                profile="products", tier=tier, processor="LazySCDProcessor",
                sink="DuckDBSink", phase="batch",
                n_rows=SCALE_TIERS[tier], n_changed=0, ok=False, error=str(e),
            ))
            continue
        ds = gen.generate("products", tier=tier)
        results.append(_run_scd2_batch(ds, f"C_products_{tier}_throughput", "C_throughput", tier, temp_dir))
        ds.cleanup()
    return results


