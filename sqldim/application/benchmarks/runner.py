"""
sqldim/benchmarks/runner.py
============================
CLI entry point for the sqldim benchmark suite.

Three top-level **groups** map to the three benchmark domains::

    scd    — SCD processing (scan regression, streaming, type variety)
    model  — Dimensional model (dims, loaders, drift observatory)
    dgm    — DGM query algebra (query builder, model/planner/exporter)

Each group has named **subgroups**, each subgroup contains **profiles**
(data personas such as ``products``, ``employees``, …).  Every profile
follows the canonical ``result → MemoryProbe → execute`` pattern.

Usage::

    python -m sqldim.application.benchmarks.runner               # all
    python -m sqldim.application.benchmarks.runner scd           # SCD group
    python -m sqldim.application.benchmarks.runner scd.regression          # subgroup
    python -m sqldim.application.benchmarks.runner scd.regression.products # profile
    python -m sqldim.application.benchmarks.runner model.dims dgm          # combined
    python -m sqldim.application.benchmarks.runner --max-tier m            # cap tier
    python -m sqldim.application.benchmarks.runner --report json           # output fmt
    python -m sqldim.application.benchmarks.runner --fail-on-regression    # exit 1

Case selection
--------------
Each positional argument is a **cascade spec** of the form::

    group                      → all subgroups in that group
    group.subgroup             → all profiles in that subgroup
    group.subgroup.profile     → only cases whose case_id contains profile

Examples::

    scd                           → all SCD benchmarks
    scd.regression                → scan, memory safety, throughput
    scd.regression.products       → regression cases filtered to 'products'
    model.dims model.drift        → dims + drift subgroups
    dgm.model.planner             → planner profile only

Output
------
- Console table with colour-coded status
- JSON or CSV file in sqldim/benchmarks/results/
- Summary of regressions and safety breaches
"""
from __future__ import annotations

import argparse
import gc
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Callable

import psutil

from sqldim.application.benchmarks._dataset import BenchmarkDatasetGenerator, SCALE_TIERS
from sqldim.application.benchmarks.memory_probe import (
    SAFE_PCT, ABORT_FLOOR_GB, auto_max_tier, MemoryProbe,
)
from sqldim.application.benchmarks.suite import (
    BenchmarkResult,
    SOURCE_NAMES,
    group_a_scan_regression,
    group_b_memory_safety,
    group_c_throughput_scaling,
    group_d_stream_vs_batch,
    group_e_change_rate_sensitivity,
    group_f_processor_comparison,
    group_g_beyond_memory,
    group_h_source_sink_matrix,
    group_i_scd_type_variety,
    group_j_dim_generation,
    group_k_graph_query,
    group_l_narwhals_backfill,
    group_m_loaders_medallion,
    group_n_drift_observatory,
    group_o_dgm_query,
    group_p_bdd_predicate,
    group_q_recommender,
    group_r_planner,
    group_s_exporter,
    group_t_question_algebra,
)


# ── ANSI color helpers ─────────────────────────────────────────────────────

def _red(s: str) -> str:    return f"\033[91m{s}\033[0m"
def _green(s: str) -> str:  return f"\033[92m{s}\033[0m"
def _yellow(s: str) -> str: return f"\033[93m{s}\033[0m"
def _bold(s: str) -> str:   return f"\033[1m{s}\033[0m"
def _dim(s: str) -> str:    return f"\033[2m{s}\033[0m"


# ── Inter-group safety ────────────────────────────────────────────────────

def _check_memory_before_group(group_id: str) -> None:
    """Re-check available RAM before starting a group.  Aborts the whole run if
    below ABORT_FLOOR_GB; warns if below twice that threshold."""
    available_gb = psutil.virtual_memory().available / 1e9
    if available_gb < ABORT_FLOOR_GB:
        raise RuntimeError(
            f"Available RAM ({available_gb:.1f} GB) is below the safety "
            f"floor ({ABORT_FLOOR_GB:.1f} GB) before Group {group_id} — "
            f"aborting run to prevent OOM kill."
        )
    if available_gb < ABORT_FLOOR_GB * 2:
        print(_yellow(
            f"  ⚠️   Low RAM before Group {group_id}: "
            f"{available_gb:.1f} GB available — large tiers will be skipped."
        ))


# ── System info banner ─────────────────────────────────────────────────────

def print_system_info() -> None:
    vm = psutil.virtual_memory()
    total_gb   = vm.total / 1e9
    avail_gb   = vm.available / 1e9
    safe_gb    = total_gb * SAFE_PCT
    cpu_count  = os.cpu_count()

    print(_bold("\n╔══════════════════════════════════════════════════════╗"))
    print(_bold(  "║           sqldim Benchmark Suite                     ║"))
    print(_bold(  "╚══════════════════════════════════════════════════════╝"))
    print(f"  System RAM   : {total_gb:.1f} GB total  |  {avail_gb:.1f} GB available")
    print(f"  Safety floor : {ABORT_FLOOR_GB:.1f} GB (abort if below)")
    print(f"  Safe ceiling : {safe_gb:.1f} GB ({SAFE_PCT*100:.0f}% of total)")
    print(f"  CPU cores    : {cpu_count}")
    print()

    if avail_gb < ABORT_FLOOR_GB:
        print(_red(f"  ⛔  Only {avail_gb:.1f}GB available — below minimum {ABORT_FLOOR_GB}GB floor!"))
        print(_red("      Free memory before running benchmarks."))
        sys.exit(1)
    elif avail_gb < 3.0:
        print(_yellow(f"  ⚠️   Low available RAM ({avail_gb:.1f}GB). Large tiers will be skipped."))
    else:
        print(_green("  ✅  System OK to run benchmarks."))
    print()


# ── Result table printer ──────────────────────────────────────────────────

_COL_WIDTHS = {
    "case_id":          40,
    "tier":              4,
    "n_rows":           10,
    "wall_s":            7,
    "rows_per_sec":     10,
    "peak_rss_gb":       9,
    "spill_gb":          8,
    "scan_ok":           7,
    "mem_ok":            6,
    "status":            6,
}

def _fmt_rows(n: int) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.0f}K"
    return str(n)


def _ok_count(rs: list) -> int:
    return sum(1 for r in rs if r.ok)


def _with_regression(rs: list) -> list:
    return [r for r in rs if r.scan_regression]


def _with_breach(rs: list) -> list:
    return [r for r in rs if r.safety_breach]


def _with_spill(rs: list) -> list:
    return [r for r in rs if r.total_spill_gb > 0.001]


def _format_result_row(r: BenchmarkResult) -> tuple[str, bool]:
    scan_ok = "✅" if not r.scan_regression else _red("🔴 REG")
    mem_ok  = "✅" if not r.safety_breach   else _yellow("⚠️")
    status  = _green("OK") if r.ok else _red("FAIL")
    line = (
        f"  {r.case_id:<40} {r.tier:<4} {_fmt_rows(r.n_rows):<8} "
        f"{r.wall_s:>6.1f}s {_fmt_rps(r.rows_per_sec):>10} "
        f"{r.peak_rss_gb:>6.2f}G "
        f"{r.total_spill_gb:>5.2f}G "
        f"{scan_ok:>5} {mem_ok:>5} {status:>6}"
    )
    return line, r.ok

def _fmt_rps(rps: float) -> str:
    if rps >= 1_000_000:
        return f"{rps/1_000_000:.1f}M/s"
    if rps >= 1_000:
        return f"{rps/1_000:.0f}K/s"
    return f"{rps:.0f}/s"

def print_results_table(results: list[BenchmarkResult], group: str = "") -> None:
    header = (
        f"{'Case':<40} {'Tier':<4} {'Rows':<8} "
        f"{'Time':>7} {'Throughput':>10} {'RSS GB':>7} "
        f"{'Spill':>6} {'Scan':>5} {'Mem':>5} {'':>6}"
    )
    sep = "─" * len(header)
    if group:
        print(_bold(f"\n  Group {group}"))
    print(f"  {_dim(sep)}")
    print(f"  {_bold(header)}")
    print(f"  {_dim(sep)}")

    for r in results:
        line, ok = _format_result_row(r)
        if not ok:
            print(_red(line))
            print(_red(f"    ERROR: {r.error[:80]}"))
        else:
            print(line)

    print(f"  {_dim(sep)}\n")


# ── Summary ───────────────────────────────────────────────────────────────

def _print_regression_section(regressions: list[BenchmarkResult]) -> None:
    if regressions:
        print(_red(f"  🔴  SCAN REGRESSIONS DETECTED ({len(regressions)} cases):"))
        for r in regressions:
            print(_red(f"      - {r.case_id}: scan_count={r.scan_count}  (expected 1)"))
        print(_red("  → Fix: change VIEW → TABLE in _register_current_checksums()"))
    else:
        print(_green("  ✅  No scan regressions — all processors use TABLE materialization"))
    print()


def _print_breach_section(breaches: list[BenchmarkResult]) -> None:
    if breaches:
        print(_yellow(f"  ⚠️   MEMORY SAFETY BREACHES ({len(breaches)} cases):"))
        for r in breaches:
            print(_yellow(f"      - {r.case_id}: {r.breach_detail}"))
    else:
        print(_green(f"  ✅  All cases within memory safety ceiling ({SAFE_PCT*100:.0f}% of RAM)"))
    print()


def _print_spill_section(spilled: list[BenchmarkResult]) -> None:
    if spilled:
        print(f"  💾  Spill-to-disk detected in {len(spilled)} cases:")
        for r in spilled:
            print(f"      - {r.case_id}: {r.total_spill_gb:.3f} GB spilled")
        print("  → This is EXPECTED behavior for tight-memory or large-tier cases")
    else:
        print("  ℹ️   No spill-to-disk (all datasets fit comfortably in RAM)")
    print()


def _print_throughput_section(all_results: list[BenchmarkResult]) -> None:
    throughput_cases = [r for r in all_results if r.ok and r.rows_per_sec > 0]
    if not throughput_cases:
        return
    best  = max(throughput_cases, key=lambda r: r.rows_per_sec)
    worst = min(throughput_cases, key=lambda r: r.rows_per_sec)
    print("  ⚡  Throughput range:")
    print(f"      Best  : {_fmt_rps(best.rows_per_sec):<12} [{best.case_id}]")
    print(f"      Worst : {_fmt_rps(worst.rows_per_sec):<12} [{worst.case_id}]")
    print()


def print_summary(all_results: list[BenchmarkResult]) -> dict:
    total       = len(all_results)
    passed      = _ok_count(all_results)
    failed      = total - passed
    regressions = _with_regression(all_results)
    breaches    = _with_breach(all_results)
    spilled     = _with_spill(all_results)

    print(_bold("══════════════════════════════════════════════════════"))
    print(_bold("  SUMMARY"))
    print(_bold("══════════════════════════════════════════════════════"))
    print(f"  Cases run    : {total}")
    print(f"  Passed       : {_green(str(passed))}")
    print(f"  Failed       : {_red(str(failed)) if failed else str(failed)}")
    print()

    _print_regression_section(regressions)
    _print_breach_section(breaches)
    _print_spill_section(spilled)
    _print_throughput_section(all_results)

    return {
        "total":       total,
        "passed":      passed,
        "failed":      failed,
        "regressions": len(regressions),
        "breaches":    len(breaches),
        "spilled":     len(spilled),
    }


# ── Persistence ───────────────────────────────────────────────────────────

def save_results(results: list[BenchmarkResult], fmt: str, out_dir: str) -> str:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    rows = [r.row() for r in results]

    if fmt == "json":
        path = os.path.join(out_dir, f"bench_{ts}.json")
        with open(path, "w") as f:
            json.dump(rows, f, indent=2)
    elif fmt == "csv":
        import csv
        path = os.path.join(out_dir, f"bench_{ts}.csv")
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    else:
        path = ""

    return path


# ── Critical analysis ─────────────────────────────────────────────────────


def _c_throughput_sorted(ok_results: list[BenchmarkResult]) -> list[BenchmarkResult]:
    return sorted(
        [r for r in ok_results if r.group == "C_throughput"],
        key=lambda r: r.n_rows,
    )


def _tier_scale_flag(pct: float, row_factor: float) -> tuple[str, bool]:
    if pct < 10:
        return _yellow(f"⚠️  near-plateau (+{pct:.0f}%, {row_factor:.0f}x more rows)"), True
    if pct < 0:
        return _red(f"🔴 regression ({pct:+.0f}%)"), False
    return _green(f"✅ +{pct:.0f}%"), False


def _analyse_tier_scaling(ok_results: list[BenchmarkResult]) -> None:
    """Group C — tier scaling efficiency."""
    c_cases = _c_throughput_sorted(ok_results)
    if len(c_cases) < 2:
        return
    print("  📈  Group C — Tier scaling efficiency (rows/sec):")
    prev = c_cases[0]
    plateaus = []
    for cur in c_cases[1:]:
        pct = (cur.rows_per_sec - prev.rows_per_sec) / max(prev.rows_per_sec, 1) * 100
        row_factor = cur.n_rows / max(prev.n_rows, 1)
        flag, is_plateau = _tier_scale_flag(pct, row_factor)
        if is_plateau:
            plateaus.append(cur.tier)
        print(f"      {prev.tier}→{cur.tier}: "
              f"{_fmt_rps(prev.rows_per_sec)} → {_fmt_rps(cur.rows_per_sec)}  {flag}")
        prev = cur
    if plateaus:
        print(_yellow(
            f"  → Plateau at tier(s) {plateaus}: DuckDB parallelism is saturated. "
            "Not a regression — expected at xl+."
        ))
    print()


def _e_change_rate_cases(ok_results: list[BenchmarkResult]) -> list[BenchmarkResult]:
    return [r for r in ok_results if r.group == "E_change_rate"]


def _analyse_change_rate(ok_results: list[BenchmarkResult]) -> None:
    """Group E — change-rate variance."""
    e_cases = _e_change_rate_cases(ok_results)
    if len(e_cases) < 2:
        return
    rps_vals  = [r.rows_per_sec for r in e_cases]
    rps_range = (max(rps_vals) - min(rps_vals)) / max(max(rps_vals), 1) * 100
    print("  🔄  Group E — Change-rate sensitivity:")
    for r in e_cases:
        label = r.case_id.split("_")[-1]
        print(f"      {label:>5}: {_fmt_rps(r.rows_per_sec)}")
    if rps_range < 15:
        print(_yellow(
            f"  → Throughput variance {rps_range:.1f}% across change rates — "
            "near-flat. The SCD2 processor cost is dominated by the full-scan "
            "hash join, not the versioning I/O."
        ))
    else:
        print(_green(
            f"  → {rps_range:.1f}% throughput variance — processor is sensitive "
            "to change rate as expected."
        ))
    print()


def _spill_flag(ratio: float, spill_gb: float) -> str:
    if ratio > 1.5:
        return _yellow(f"⚠️  {ratio:.1f}x slower under pressure")
    if ratio < 0.95:
        if spill_gb == 0.0:
            return _yellow(
                f"⚠️  no-spill {1/ratio:.1f}x FASTER under tight limit "
                f"— smaller DuckDB buffer pool reduces overhead for this working set"
            )
        return _yellow(f"⚠️  spill {1/ratio:.1f}x FASTER — check measurement isolation")
    return _green("✅ marginal")


def _spill_grp_g(ok_results: list[BenchmarkResult]) -> dict:
    return {r.tier: r for r in ok_results if r.group == "G_beyond_memory"}


def _spill_grp_b(ok_results: list[BenchmarkResult]) -> dict:
    return {r.tier: r for r in ok_results
            if r.group == "B_memory_safety" and r.profile == "cnpj_empresa"}


def _analyse_spill_overhead(ok_results: list[BenchmarkResult]) -> None:
    """Group G vs B — spill overhead."""
    g_cases = _spill_grp_g(ok_results)
    b_cases = _spill_grp_b(ok_results)
    shared  = sorted(set(g_cases) & set(b_cases),
                     key=lambda t: ["xs", "s", "m", "l", "xl", "xxl"].index(t))
    if not shared:
        return
    print("  💾  Group G — Spill overhead vs Group B free-run:")
    for tier in shared:
        g, b  = g_cases[tier], b_cases[tier]
        ratio = b.rows_per_sec / max(g.rows_per_sec, 1)
        flag  = _spill_flag(ratio, g.total_spill_gb)
        print(f"      {tier}: free {_fmt_rps(b.rows_per_sec)} → "
              f"spill {_fmt_rps(g.rows_per_sec)}  {flag}")
    print()


def _h_source_cases(ok_results: list[BenchmarkResult]) -> list[BenchmarkResult]:
    return [r for r in ok_results if r.group == "H_source_sink_matrix"]


def _analyse_source_adapters(ok_results: list[BenchmarkResult]) -> None:
    """Group H — source adapter comparison."""
    h_cases = _h_source_cases(ok_results)
    if len(h_cases) < 2:
        return
    baseline = max(h_cases, key=lambda r: r.rows_per_sec)
    print("  🔌  Group H — Source adapter comparison:")
    for r in h_cases:
        ratio = r.rows_per_sec / max(baseline.rows_per_sec, 1)
        bar   = "▓" * int(ratio * 20)
        flag  = _green("✅ fastest") if r is baseline else _dim(f"{ratio*100:.0f}% of best")
        print(f"      {r.source:<8}: {_fmt_rps(r.rows_per_sec):>10}  {bar:<20}  {flag}")
    print()


def _analyse_ins_ver_unc(ok_results: list[BenchmarkResult]) -> None:
    """Overall insert/version/unchanged breakdown."""
    tracked = [r for r in ok_results if r.inserted + r.versioned + r.unchanged > 0]
    if not tracked:
        return
    print("  🔢  Insert / Version / Unchanged breakdown:")
    for r in tracked:
        total  = r.inserted + r.versioned + r.unchanged
        i_pct  = r.inserted  / total * 100
        v_pct  = r.versioned / total * 100
        u_pct  = r.unchanged / total * 100
        print(f"      {r.case_id:<42} "
              f"ins {i_pct:4.0f}%  ver {v_pct:4.0f}%  unc {u_pct:4.0f}%")
    print()


def print_critical_analysis(all_results: list[BenchmarkResult]) -> None:
    """Print a structured critique of the benchmark results.

    Checks:
    - Group C tier scaling efficiency (flags near-plateau)
    - Group E change-rate flat response
    - Group G spill overhead vs Group B free-run at matching tier
    - Group H source adapter comparison (if present)
    """
    ok_results = [r for r in all_results if r.ok and r.rows_per_sec > 0]
    if not ok_results:
        return

    print(_bold("══════════════════════════════════════════════════════"))
    print(_bold("  CRITICAL ANALYSIS"))
    print(_bold("══════════════════════════════════════════════════════"))

    _analyse_tier_scaling(ok_results)
    _analyse_change_rate(ok_results)
    _analyse_spill_overhead(ok_results)
    _analyse_source_adapters(ok_results)
    _analyse_ins_ver_unc(ok_results)


def _load_previous_rps(out_dir: str) -> tuple[dict, str] | None:
    """Return ``(prev_rps_dict, filename)`` for the last completed run, or ``None``."""
    jsons = sorted(Path(out_dir).glob("bench_*.json"), reverse=True)
    if len(jsons) < 2:
        print(_dim("  ℹ️   No previous run found for comparison."))
        return None
    prev_path = jsons[1]
    try:
        with open(prev_path) as f:
            prev_rows = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(_yellow(f"  ⚠️   Could not load previous results: {exc}"))
        return None
    return {row["case_id"]: row.get("rows_per_sec", 0) for row in prev_rows}, prev_path.name


def _print_rps_rows(rows: list, colour_fn) -> None:
    for cid, old, new in rows:
        pct = (new - old) / old * 100
        print(colour_fn(f"      {cid:<42} {_fmt_rps(old)} → {_fmt_rps(new)}  ({pct:+.0f}%)"))


def _is_regression(prev: float, cur: float) -> bool:
    return prev > 0 and (cur - prev) / prev < -0.15


def _is_improvement(prev: float, cur: float) -> bool:
    return prev > 0 and (cur - prev) / prev > 0.15


def _compute_deltas(
    prev_rps: dict, cur_rps: dict, shared: list
) -> tuple[list, list]:
    regressions = [
        (cid, prev_rps[cid], cur_rps[cid])
        for cid in shared
        if _is_regression(prev_rps[cid], cur_rps[cid])
    ]
    improvements = [
        (cid, prev_rps[cid], cur_rps[cid])
        for cid in shared
        if _is_improvement(prev_rps[cid], cur_rps[cid])
    ]
    return regressions, improvements


def _print_delta_table(
    prev_rps: dict, cur_rps: dict, prev_name: str
) -> None:
    """Print regression / improvement table against a previous run."""
    shared = sorted(set(prev_rps) & set(cur_rps))
    if not shared:
        print(_dim("  ℹ️   No overlapping cases found for comparison."))
        return

    regressions, improvements = _compute_deltas(prev_rps, cur_rps, shared)

    print(_bold("══════════════════════════════════════════════════════"))
    print(_bold(f"  COMPARISON vs {prev_name}"))
    print(_bold("══════════════════════════════════════════════════════"))
    if regressions:
        print(_red(f"  🔴  Throughput regressions (>15% slower, {len(regressions)} cases):"))
        _print_rps_rows(regressions, _red)
    else:
        print(_green("  ✅  No throughput regressions vs last run."))
    if improvements:
        print(_green(f"  🚀  Improvements (>15% faster, {len(improvements)} cases):"))
        _print_rps_rows(improvements, _green)
    print()


def _compare_with_last_run(current: list[BenchmarkResult], out_dir: str) -> None:
    """Load the most recent result JSON from *out_dir* and flag regressions."""
    loaded = _load_previous_rps(out_dir)
    if loaded is None:
        return
    prev_rps, prev_name = loaded
    cur_rps = {r.case_id: r.rows_per_sec for r in current if r.ok}
    _print_delta_table(prev_rps, cur_rps, prev_name)


# ── Main ──────────────────────────────────────────────────────────────────

# ── Three-level benchmark hierarchy ─────────────────────────────────────
#
#   group  →  subgroup  →  [internal letter IDs]
#
# The letter IDs below are kept for internal dispatch only; the public
# CLI surface uses the cascade notation  group.subgroup.profile.

BENCH_HIERARCHY: dict[str, dict[str, list[str]]] = {
    "scd": {
        "regression": ["A", "B", "C"],
        "stream":     ["D", "E", "F", "G", "H"],
        "types":      ["I"],
    },
    "model": {
        "dims":    ["J", "K"],
        "loaders": ["L", "M"],
        "drift":   ["N"],
    },
    "dgm": {
        "query":   ["O", "P"],
        "model":   ["Q", "R", "S"],
        "algebra": ["T"],
    },
}

# Reverse lookup: letter → "group.subgroup" display path
_LETTER_TO_PATH: dict[str, str] = {
    letter: f"{grp}.{sub}"
    for grp, subs in BENCH_HIERARCHY.items()
    for sub, letters in subs.items()
    for letter in letters
}

# Internal dispatch map (letter → callable) — not exposed in help
_LETTER_FN_MAP: dict[str, Callable] = {
    "A": group_a_scan_regression,
    "B": group_b_memory_safety,
    "C": group_c_throughput_scaling,
    "D": group_d_stream_vs_batch,
    "E": group_e_change_rate_sensitivity,
    "F": group_f_processor_comparison,
    "G": group_g_beyond_memory,
    "H": group_h_source_sink_matrix,
    "I": group_i_scd_type_variety,
    "J": group_j_dim_generation,
    "K": group_k_graph_query,
    "L": group_l_narwhals_backfill,
    "M": group_m_loaders_medallion,
    "N": group_n_drift_observatory,
    "O": group_o_dgm_query,
    "P": group_p_bdd_predicate,
    "Q": group_q_recommender,
    "R": group_r_planner,
    "S": group_s_exporter,
    "T": group_t_question_algebra,
}

# Subgroup descriptions keyed by "group.subgroup"
SUBGROUP_DESCRIPTIONS: dict[str, str] = {
    "scd.regression": "SCD regression: scan count, memory safety, throughput scaling",
    "scd.stream":     "SCD streaming: batch vs stream, change rate, processor, spill, source/sink",
    "scd.types":      "SCD type variety: Type3, Type4 processor throughput",
    "model.dims":     "Dimensional model: prebuilt dims (Date/Time) and graph query",
    "model.loaders":  "Model loaders: Narwhals SCD2 backfill and ORM/Medallion loaders",
    "model.drift":    "Model drift: schema/quality drift observability (DriftObservatory star schema)",
    "dgm.query":   "DGM query algebra: three-band query builder and BDD predicate compiler",
    "dgm.model":   "DGM model: recommender annotation, planner rule cycles, multi-target exporter",
    "dgm.algebra": "DGM algebra: QuestionAlgebra build/emit, Cross-CTE CSE, CORRELATE suggestions",
}

# Per-letter descriptions (used in results tables and detail display)
_LETTER_DESCRIPTIONS: dict[str, str] = {
    "A": "VIEW vs TABLE regression (scan count verification)",
    "B": "Memory safety across all processors",
    "C": "Throughput scaling (rows/sec by tier)",
    "D": "Streaming vs batch comparison",
    "E": "Change rate sensitivity",
    "F": "Processor comparison (SCD2 vs Metadata vs Type6)",
    "G": "Beyond-memory / spill-to-disk simulation",
    "H": "Source adapter comparison (parquet vs csv)",
    "I": "SCD Type3 and Type4 processor throughput",
    "J": "Prebuilt dimension generation (DateDimension / TimeDimension)",
    "K": "Graph traversal and dimensional query builder",
    "L": "Narwhals SCD2 backfill throughput",
    "M": "ORM loader throughput and Medallion registry compute",
    "N": "Schema/quality drift observability pipeline (DriftObservatory star schema)",
    "O": "DGM three-band query builder (B1 / B1\u2218B2 / B1\u2218B3 / B1\u2218B2\u2218B3) throughput",
    "P": "BDD predicate compilation throughput (compile / satisfiability / to_sql)",
    "Q": "DGMRecommender annotation + trail rule throughput",
    "R": "DGMPlanner rule cycle throughput (1a, 9, build_plan)",
    "S": "DGM multi-target exporter throughput (JSON/YAML \u00d7 simple/complex)",
    "T": "DGM algebra: QuestionAlgebra build+to_sql, CSE detection+apply, CORRELATE suggest throughput",
}


def _valid_specs_hint() -> str:
    """Return a compact hint of valid group.subgroup specs for error messages."""
    specs = [f"{g}.{s}" for g, subs in BENCH_HIERARCHY.items() for s in subs]
    return ", ".join([*BENCH_HIERARCHY.keys(), *specs])


def _letters_for(grp: str, sub: str | None) -> list[str]:
    """Return the letter IDs for a group (or subgroup when *sub* is given)."""
    if sub is None:
        return [ll for subs in BENCH_HIERARCHY[grp].values() for ll in subs]
    return list(BENCH_HIERARCHY[grp][sub])


def _validate_spec(grp: str, sub: str | None, original: str) -> None:
    """Raise ``ValueError`` if *grp* / *sub* are not in the hierarchy."""
    if grp not in BENCH_HIERARCHY:
        raise ValueError(f"Unknown group {grp!r}. Valid: {_valid_specs_hint()}")
    if sub is not None and sub not in BENCH_HIERARCHY[grp]:
        raise ValueError(f"Unknown subgroup {grp}.{sub!r}. Valid: {_valid_specs_hint()}")


def _resolve_spec(spec: str) -> list[tuple[str, str | None]]:
    """Resolve a cascade spec to ``[(letter_id, profile_filter), ...]``.

    Accepted formats::

        scd                     → all letters in scd group
        scd.regression          → all letters in scd.regression subgroup
        scd.regression.products → scd.regression letters with filter 'products'

    Raises ``ValueError`` for unknown specs.
    """
    parts = spec.split(".", 2)
    grp: str = parts[0]
    sub: str | None = parts[1] if len(parts) > 1 else None
    profile: str | None = parts[2] if len(parts) > 2 else None
    _validate_spec(grp, sub, spec)
    return [(ll, profile) for ll in _letters_for(grp, sub)]


def _parse_token(token: str) -> tuple[str, str | None]:
    # kept for internal _update_result_entry usage; _resolve_spec is the public entry
    if "." in token:
        group_id, element = token.split(".", 1)
        return group_id.upper(), element
    return token.upper(), None


def _update_result_entry(result: dict, group_id: str, element: str | None) -> None:
    if group_id not in result:
        result[group_id] = None if element is None else [element]
    elif result[group_id] is None:
        pass  # already "all" — group-level wins
    elif element is None:
        result[group_id] = None  # upgrade from filtered → all
    else:
        result[group_id].append(element)  # type: ignore[union-attr]


def _parse_cases(
    tokens: list[str],
) -> dict[str, list[str] | None]:
    """Parse cascade CASE tokens into ``{letter_id: profile_filters | None}``.

    ``None`` means *all profiles* in that letter's subgroup.  A list means
    only cases whose ``case_id`` contains at least one of the listed substrings.

    Rules
    -----
    * ``"scd"``                     → A–I, each with ``None``
    * ``"scd.regression"``          → A, B, C each with ``None``
    * ``"scd.regression.products"`` → A, B, C each with filter ``["products"]``
    * Two specs for the same letter merge their filters
    * A bare group/subgroup spec wins over any filtered spec (``None`` = all)
    """
    result: dict[str, list[str] | None] = {}
    for token in tokens:
        for letter, element in _resolve_spec(token):
            _update_result_entry(result, letter, element)
    return result


def _build_groups_epilog() -> str:
    """Return a formatted hierarchy tree for --help."""
    lines = [
        "benchmark groups  (group → subgroup → profile filter):",
        "",
    ]
    for grp, subs in BENCH_HIERARCHY.items():
        lines.append(f"  {grp}")
        for sub, letters in subs.items():
            desc = SUBGROUP_DESCRIPTIONS.get(f"{grp}.{sub}", "")
            lines.append(f"    .{sub:<12}  {desc}")
            for letter in letters:
                ldesc = _LETTER_DESCRIPTIONS.get(letter, "")
                lines.append(f"      [{letter}] {ldesc}")
        lines.append("")
    lines += [
        "case selection examples:",
        "  scd                       run all SCD benchmarks",
        "  scd.regression            scan regression, memory safety, throughput",
        "  scd.regression.products   regression cases filtered to 'products'",
        "  model.dims model.drift    dims + drift subgroups",
        "  dgm.model.planner         planner profile only",
        "",
    ]
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m sqldim.application.benchmarks.runner",
        description="sqldim comprehensive benchmark suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_build_groups_epilog(),
    )
    p.add_argument(
        "cases", nargs="*",
        metavar="CASE",
        help=(
            "Cascade specs to run: group, group.subgroup, or group.subgroup.profile. "
            "Omit to run everything.  "
            "Examples: 'scd' (all SCD), 'scd.regression' (regression subgroup), "
            "'scd.regression.products' (filtered to products profile), "
            "'model.dims dgm' (combined)."
        ),
    )
    p.add_argument(
        "--max-tier", choices=[*SCALE_TIERS.keys(), "auto"], default="auto",
        help=(
            "Maximum scale tier (default: auto — selected from available RAM). "
            "'auto' picks the largest tier whose minimum RAM requirement is met, "
            "preventing OOM kills on small VMs.  Pass an explicit tier to override."
        ),
    )
    p.add_argument(
        "--report", choices=["none", "json", "csv"], default="json",
        help="Output format for results file",
    )
    p.add_argument(
        "--out-dir", default="sqldim/benchmarks/results",
        help="Directory to write result files",
    )
    p.add_argument(
        "--fail-on-regression", action="store_true",
        help="Exit with code 1 if any scan regression is detected",
    )
    p.add_argument(
        "--fail-on-breach", action="store_true",
        help="Exit with code 1 if any memory safety breach is detected",
    )
    p.add_argument(
        "--source", choices=SOURCE_NAMES, default="parquet",
        help=(
            "Source adapter used for all benchmark groups (default: parquet). "
            "'csv' exports the generated Parquet snapshot to CSV first so the "
            "CSV parse overhead is included in wall-clock time."
        ),
    )
    p.add_argument(
        "--compare-last", action="store_true",
        help="Compare throughput with the most recent saved result file and flag regressions.",
    )
    return p


_TIER_ORDER = ["xs", "s", "m", "l", "xl", "xxl"]


def _resolve_effective_tier(args) -> str:
    """Return the initial effective max-tier string from parsed CLI args."""
    avail_gb = psutil.virtual_memory().available / 1e9
    return auto_max_tier(avail_gb) if args.max_tier == "auto" else args.max_tier


def _print_run_header(
    args,
    groups_to_run: list[tuple[str, list[str] | None]],
    effective_max_tier: str,
) -> None:
    print_system_info()
    if args.max_tier == "auto":
        print(f"  Effective max tier: {effective_max_tier}")
    if args.cases:
        lines = [
            f"{gid}.{{{','.join(ef)}}}" if ef else gid
            for gid, ef in groups_to_run
        ]
        print(f"  Running: {' '.join(lines)}")
    print()


def _filter_results(
    results: list[BenchmarkResult],
    element_filter: list[str] | None,
    group_id: str,
) -> list[BenchmarkResult]:
    if element_filter is None:
        return results
    return _apply_element_filter(results, element_filter, group_id)


def _run_one_group(
    gen,
    tmp_dir: str,
    group_id: str,
    element_filter: list[str] | None,
    fn,
    desc: str,
    effective_max_tier: str,
    args,
) -> list[BenchmarkResult] | None:
    """Run a single benchmark group; apply element filter; return results or None to skip."""
    MemoryProbe.reset_hard_abort()
    try:
        _check_memory_before_group(group_id)
    except RuntimeError as mem_exc:
        print(_red(f"\n  ⛔  {mem_exc}"))
        return None  # signals caller to break the run

    filter_label = f" [filter: {element_filter}]" if element_filter else ""
    path = _LETTER_TO_PATH.get(group_id, group_id)
    print(_bold(f"  ▶  {path} [{group_id}]: {desc}{filter_label}"))
    t0 = time.perf_counter()

    try:
        results = fn(gen, tmp_dir, max_tier=effective_max_tier, source_name=args.source)
    except Exception as exc:
        import traceback as _tb
        print(_red(f"\n  ❌  Group {group_id} crashed unexpectedly:"))
        print(_red(f"     {type(exc).__name__}: {exc}"))
        print(_red(_tb.format_exc()))
        results = [BenchmarkResult(
            case_id=f"{group_id}_group_crash",
            group=group_id, profile="n/a", tier="n/a",
            processor="n/a", sink="n/a", phase="n/a",
            n_rows=0, n_changed=0, ok=False,
            error=f"Group crash: {type(exc).__name__}: {exc}",
        )]

    results = _filter_results(results, element_filter, group_id)
    if not results:
        return []

    elapsed = time.perf_counter() - t0
    path = _LETTER_TO_PATH.get(group_id, group_id)
    print(f"     {path} [{group_id}] completed in {elapsed:.1f}s ({len(results)} cases)")
    print_results_table(results, group=f"{path} [{group_id}] — {desc}")
    return results


def _update_auto_tier(
    args, effective_max_tier: str, group_id: str
) -> str:
    """After a group completes, lower the tier cap if available RAM has dropped."""
    if args.max_tier != "auto":
        return effective_max_tier
    new_avail = psutil.virtual_memory().available / 1e9
    new_tier = auto_max_tier(new_avail)
    if _TIER_ORDER.index(new_tier) < _TIER_ORDER.index(effective_max_tier):
        path = _LETTER_TO_PATH.get(group_id, group_id)
        print(_yellow(
            f"  ⚠️  RAM dropped to {new_avail:.1f} GB after {path} [{group_id}]. "
            f"Auto-capping remaining groups at tier '{new_tier}'."
        ))
        return new_tier
    return effective_max_tier


def _check_regression_fail(args, summary: dict) -> int:
    if args.fail_on_regression and summary["regressions"] > 0:
        print(_red("  ⛔  Exiting with code 1 — scan regressions detected."))
        return 1
    return 0


def _check_breach_fail(args, summary: dict) -> int:
    if args.fail_on_breach and summary["breaches"] > 0:
        print(_red("  ⛔  Exiting with code 1 — memory safety breaches detected."))
        return 1
    return 0


def _compute_exit_code(args, summary: dict) -> int:
    code = max(_check_regression_fail(args, summary), _check_breach_fail(args, summary))
    if summary["failed"] > 0:
        code = max(code, 2)
    return code


def _record_low_ram_abort(
    all_results: list[BenchmarkResult], group_id: str
) -> None:
    all_results.append(BenchmarkResult(
        case_id=f"{group_id}_skipped_low_ram",
        group=group_id, profile="n/a", tier="n/a",
        processor="n/a", sink="n/a", phase="n/a",
        n_rows=0, n_changed=0, ok=False,
        error="Insufficient RAM before group",
    ))


def _run_all_groups(
    groups_to_run: list[tuple[str, list[str] | None]],
    args,
    initial_tier: str,
) -> list[BenchmarkResult]:
    """Run every group in *groups_to_run* and return accumulated results."""
    all_results: list[BenchmarkResult] = []
    effective_max_tier = initial_tier

    with tempfile.TemporaryDirectory(prefix="sqldim_bench_") as tmp_dir:
        with BenchmarkDatasetGenerator(tmp_root=tmp_dir) as gen:
            for group_id, element_filter in groups_to_run:
                fn   = _LETTER_FN_MAP[group_id]
                path = _LETTER_TO_PATH.get(group_id, group_id)
                desc = _LETTER_DESCRIPTIONS.get(group_id, path)
                results = _run_one_group(
                    gen, tmp_dir, group_id, element_filter,
                    fn, desc, effective_max_tier, args,
                )
                if results is None:
                    _record_low_ram_abort(all_results, group_id)
                    break
                if results:
                    all_results.extend(results)
                gc.collect()
                time.sleep(2)
                effective_max_tier = _update_auto_tier(args, effective_max_tier, group_id)

    return all_results


def _post_run(args, all_results: list[BenchmarkResult], summary: dict) -> None:
    if args.compare_last and all_results:
        _compare_with_last_run(all_results, args.out_dir)
    if args.report != "none" and all_results:
        path = save_results(all_results, args.report, args.out_dir)
        print(f"  📁  Results saved → {path}\n")


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    try:
        case_selection = _parse_cases(args.cases) if args.cases else None
    except ValueError as exc:
        print(_red(f"  ⛔  {exc}"))
        return 2

    groups_to_run: list[tuple[str, list[str] | None]] = (
        list(case_selection.items()) if case_selection is not None
        else [(g, None) for g in _LETTER_FN_MAP]
    )
    effective_max_tier = _resolve_effective_tier(args)
    _print_run_header(args, groups_to_run, effective_max_tier)

    all_results = _run_all_groups(groups_to_run, args, effective_max_tier)

    summary = print_summary(all_results)
    print_critical_analysis(all_results)
    _post_run(args, all_results, summary)

    return _compute_exit_code(args, summary)


if __name__ == "__main__":
    sys.exit(main())