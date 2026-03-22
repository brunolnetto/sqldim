"""
benchmarks/runner.py
=====================
CLI entry point for the sqldim benchmark suite.

Usage:
    python -m benchmarks.runner                        # all groups, safe tiers
    python -m benchmarks.runner --groups A B C         # specific groups only
    python -m benchmarks.runner --max-tier m           # cap at 1M rows
    python -m benchmarks.runner --report json          # output format
    python -m benchmarks.runner --fail-on-regression   # exit 1 on scan regressions

Output:
    - Console table with color-coded status
    - JSON or CSV file in benchmarks/results/
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

from benchmarks.dataset_gen import BenchmarkDatasetGenerator, SCALE_TIERS
from benchmarks.memory_probe import (
    SAFE_PCT, ABORT_FLOOR_GB, auto_max_tier, MemoryProbe,
)
from benchmarks.suite import (
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
        if not r.ok:
            print(_red(line))
            print(_red(f"    ERROR: {r.error[:80]}"))
        else:
            print(line)

    print(f"  {_dim(sep)}\n")


# ── Summary ───────────────────────────────────────────────────────────────

def print_summary(all_results: list[BenchmarkResult]) -> dict:
    total       = len(all_results)
    passed      = sum(1 for r in all_results if r.ok)
    failed      = total - passed
    regressions = [r for r in all_results if r.scan_regression]
    breaches    = [r for r in all_results if r.safety_breach]
    spilled     = [r for r in all_results if r.total_spill_gb > 0.001]

    print(_bold("══════════════════════════════════════════════════════"))
    print(_bold("  SUMMARY"))
    print(_bold("══════════════════════════════════════════════════════"))
    print(f"  Cases run    : {total}")
    print(f"  Passed       : {_green(str(passed))}")
    print(f"  Failed       : {_red(str(failed)) if failed else str(failed)}")
    print()

    # Scan regressions
    if regressions:
        print(_red(f"  🔴  SCAN REGRESSIONS DETECTED ({len(regressions)} cases):"))
        for r in regressions:
            print(_red(f"      - {r.case_id}: scan_count={r.scan_count}  (expected 1)"))
        print(_red("  → Fix: change VIEW → TABLE in _register_current_checksums()"))
    else:
        print(_green("  ✅  No scan regressions — all processors use TABLE materialization"))

    print()

    # Memory safety
    if breaches:
        print(_yellow(f"  ⚠️   MEMORY SAFETY BREACHES ({len(breaches)} cases):"))
        for r in breaches:
            print(_yellow(f"      - {r.case_id}: {r.breach_detail}"))
    else:
        print(_green(f"  ✅  All cases within memory safety ceiling ({SAFE_PCT*100:.0f}% of RAM)"))

    print()

    # Spill summary
    if spilled:
        print(f"  💾  Spill-to-disk detected in {len(spilled)} cases:")
        for r in spilled:
            print(f"      - {r.case_id}: {r.total_spill_gb:.3f} GB spilled")
        print("  → This is EXPECTED behavior for tight-memory or large-tier cases")
    else:
        print("  ℹ️   No spill-to-disk (all datasets fit comfortably in RAM)")

    print()

    # Throughput table
    throughput_cases = [r for r in all_results if r.ok and r.rows_per_sec > 0]
    if throughput_cases:
        best  = max(throughput_cases, key=lambda r: r.rows_per_sec)
        worst = min(throughput_cases, key=lambda r: r.rows_per_sec)
        print("  ⚡  Throughput range:")
        print(f"      Best  : {_fmt_rps(best.rows_per_sec):<12} [{best.case_id}]")
        print(f"      Worst : {_fmt_rps(worst.rows_per_sec):<12} [{worst.case_id}]")

    print()

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

    # ── Group C: tier scaling efficiency ─────────────────────────────────
    c_cases = sorted(
        [r for r in ok_results if r.group == "C_throughput"],
        key=lambda r: r.n_rows,
    )
    if len(c_cases) >= 2:
        print("  📈  Group C — Tier scaling efficiency (rows/sec):")
        prev = c_cases[0]
        plateaus = []
        for cur in c_cases[1:]:
            pct = (cur.rows_per_sec - prev.rows_per_sec) / max(prev.rows_per_sec, 1) * 100
            row_factor = cur.n_rows / max(prev.n_rows, 1)
            if pct < 10:
                flag = _yellow(f"⚠️  near-plateau (+{pct:.0f}%, {row_factor:.0f}x more rows)")
                plateaus.append(cur.tier)
            elif pct < 0:
                flag = _red(f"🔴 regression ({pct:+.0f}%)")
            else:
                flag = _green(f"✅ +{pct:.0f}%")
            print(f"      {prev.tier}→{cur.tier}: "
                  f"{_fmt_rps(prev.rows_per_sec)} → {_fmt_rps(cur.rows_per_sec)}  {flag}")
            prev = cur
        if plateaus:
            print(_yellow(
                f"  → Plateau at tier(s) {plateaus}: DuckDB parallelism is saturated. "
                "Not a regression — expected at xl+."
            ))
        print()

    # ── Group E: change-rate variance ────────────────────────────────────
    e_cases = [r for r in ok_results if r.group == "E_change_rate"]
    if len(e_cases) >= 2:
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

    # ── Group G vs B: spill overhead ─────────────────────────────────────
    g_cases = {r.tier: r for r in ok_results if r.group == "G_beyond_memory"}
    b_cases = {r.tier: r for r in ok_results if r.group == "B_memory_safety"
               and r.profile == "cnpj_empresa"}
    shared  = sorted(set(g_cases) & set(b_cases),
                     key=lambda t: ["xs", "s", "m", "l", "xl", "xxl"].index(t))
    if shared:
        print("  💾  Group G — Spill overhead vs Group B free-run:")
        for tier in shared:
            g, b  = g_cases[tier], b_cases[tier]
            # ratio > 1 → free is faster (expected); ratio < 1 → spill is faster (anomalous)
            ratio = b.rows_per_sec / max(g.rows_per_sec, 1)
            if ratio > 1.5:
                flag = _yellow(f"⚠️  {ratio:.1f}x slower under pressure")
            elif ratio < 0.95:
                spill_gb = g.total_spill_gb
                if spill_gb == 0.0:
                    flag = _yellow(
                        f"⚠️  no-spill {1/ratio:.1f}x FASTER under tight limit "
                        f"— smaller DuckDB buffer pool reduces overhead for this working set"
                    )
                else:
                    flag = _yellow(f"⚠️  spill {1/ratio:.1f}x FASTER — check measurement isolation")
            else:
                flag = _green("✅ marginal")
            print(f"      {tier}: free {_fmt_rps(b.rows_per_sec)} → "
                  f"spill {_fmt_rps(g.rows_per_sec)}  {flag}")
        print()

    # ── Group H: source adapter comparison ───────────────────────────────
    h_cases = [r for r in ok_results if r.group == "H_source_sink_matrix"]
    if len(h_cases) >= 2:
        baseline = max(h_cases, key=lambda r: r.rows_per_sec)
        print("  🔌  Group H — Source adapter comparison:")
        for r in h_cases:
            ratio = r.rows_per_sec / max(baseline.rows_per_sec, 1)
            bar   = "▓" * int(ratio * 20)
            flag  = _green("✅ fastest") if r is baseline else _dim(f"{ratio*100:.0f}% of best")
            print(f"      {r.source:<8}: {_fmt_rps(r.rows_per_sec):>10}  {bar:<20}  {flag}")
        print()

    # ── Overall: ins/ver/unc breakdown ───────────────────────────────────
    tracked = [r for r in ok_results if r.inserted + r.versioned + r.unchanged > 0]
    if tracked:
        print("  🔢  Insert / Version / Unchanged breakdown:")
        for r in tracked:
            total  = r.inserted + r.versioned + r.unchanged
            i_pct  = r.inserted  / total * 100
            v_pct  = r.versioned / total * 100
            u_pct  = r.unchanged / total * 100
            print(f"      {r.case_id:<42} "
                  f"ins {i_pct:4.0f}%  ver {v_pct:4.0f}%  unc {u_pct:4.0f}%")
        print()


def _compare_with_last_run(current: list[BenchmarkResult], out_dir: str) -> None:
    """Load the most recent result JSON from *out_dir* and flag regressions."""
    result_dir = Path(out_dir)
    jsons = sorted(result_dir.glob("bench_*.json"), reverse=True)
    # Skip the file we just wrote (it has the current timestamp)
    if len(jsons) < 2:
        print(_dim("  ℹ️   No previous run found for comparison."))
        return

    prev_path = jsons[1]   # second-most-recent = last completed run
    try:
        with open(prev_path) as f:
            prev_rows = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(_yellow(f"  ⚠️   Could not load previous results: {exc}"))
        return

    prev_rps = {row["case_id"]: row.get("rows_per_sec", 0) for row in prev_rows}
    cur_rps  = {r.case_id: r.rows_per_sec for r in current if r.ok}

    shared   = sorted(set(prev_rps) & set(cur_rps))
    if not shared:
        print(_dim("  ℹ️   No overlapping cases found for comparison."))
        return

    regressions = [
        (cid, prev_rps[cid], cur_rps[cid])
        for cid in shared
        if prev_rps[cid] > 0
        and (cur_rps[cid] - prev_rps[cid]) / prev_rps[cid] < -0.15
    ]
    improvements = [
        (cid, prev_rps[cid], cur_rps[cid])
        for cid in shared
        if prev_rps[cid] > 0
        and (cur_rps[cid] - prev_rps[cid]) / prev_rps[cid] > 0.15
    ]

    print(_bold("══════════════════════════════════════════════════════"))
    print(_bold(f"  COMPARISON vs {prev_path.name}"))
    print(_bold("══════════════════════════════════════════════════════"))
    if regressions:
        print(_red(f"  🔴  Throughput regressions (>15% slower, {len(regressions)} cases):"))
        for cid, old, new in regressions:
            pct = (new - old) / old * 100
            print(_red(f"      {cid:<42} {_fmt_rps(old)} → {_fmt_rps(new)}  ({pct:+.0f}%)"))
    else:
        print(_green("  ✅  No throughput regressions vs last run."))
    if improvements:
        print(_green(f"  🚀  Improvements (>15% faster, {len(improvements)} cases):"))
        for cid, old, new in improvements:
            pct = (new - old) / old * 100
            print(_green(f"      {cid:<42} {_fmt_rps(old)} → {_fmt_rps(new)}  ({pct:+.0f}%)"))
    print()


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


# ── Main ──────────────────────────────────────────────────────────────────

GROUP_MAP: dict[str, Callable] = {
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
}

GROUP_DESCRIPTIONS = {
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
}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m benchmarks.runner",
        description="sqldim comprehensive benchmark suite",
    )
    p.add_argument(
        "--groups", nargs="+", choices=list(GROUP_MAP.keys()),
        default=list(GROUP_MAP.keys()),
        help="Groups to run (default: all)",
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
        "--out-dir", default="benchmarks/results",
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


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    # ── Resolve effective max tier ──────────────────────────────────────────
    avail_gb = psutil.virtual_memory().available / 1e9
    if args.max_tier == "auto":
        effective_max_tier = auto_max_tier(avail_gb)
    else:
        effective_max_tier = args.max_tier

    print_system_info()
    if args.max_tier == "auto":
        print(f"  Effective max tier: {effective_max_tier}")

    all_results: list[BenchmarkResult] = []

    with tempfile.TemporaryDirectory(prefix="sqldim_bench_") as tmp_dir:
        with BenchmarkDatasetGenerator(tmp_root=tmp_dir) as gen:

            for group_id in args.groups:
                # Clear any hard-abort left over from the previous group so
                # a breach in group N does not permanently block group N+1.
                MemoryProbe.reset_hard_abort()

                fn   = GROUP_MAP[group_id]
                desc = GROUP_DESCRIPTIONS[group_id]

                # Re-check available RAM; abort entire run if below floor.
                try:
                    _check_memory_before_group(group_id)
                except RuntimeError as mem_exc:
                    print(_red(f"\n  ⛔  {mem_exc}"))
                    all_results.append(BenchmarkResult(
                        case_id=f"{group_id}_skipped_low_ram",
                        group=group_id, profile="n/a", tier="n/a",
                        processor="n/a", sink="n/a", phase="n/a",
                        n_rows=0, n_changed=0, ok=False,
                        error=str(mem_exc),
                    ))
                    break

                print(_bold(f"  ▶  Group {group_id}: {desc}"))
                t0 = time.perf_counter()

                try:
                    results = fn(gen, tmp_dir, max_tier=effective_max_tier,
                                 source_name=args.source)
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

                elapsed = time.perf_counter() - t0
                print(f"     Group {group_id} completed in {elapsed:.1f}s ({len(results)} cases)")
                print_results_table(results, group=f"{group_id} — {desc}")
                all_results.extend(results)

                # Release DuckDB and Python allocations before next group.
                gc.collect()
                time.sleep(2)

                # In auto mode, re-evaluate the tier cap after each group.
                # If available RAM dropped (e.g. after a spill-heavy group)
                # lower the cap for all remaining groups automatically.
                if args.max_tier == "auto":
                    new_avail = psutil.virtual_memory().available / 1e9
                    new_tier  = auto_max_tier(new_avail)
                    if _TIER_ORDER.index(new_tier) < _TIER_ORDER.index(effective_max_tier):
                        effective_max_tier = new_tier
                        print(_yellow(
                            f"  ⚠️  RAM dropped to {new_avail:.1f} GB after Group {group_id}. "
                            f"Auto-capping remaining groups at tier '{effective_max_tier}'."
                        ))

    summary = print_summary(all_results)
    print_critical_analysis(all_results)

    if args.compare_last and all_results:
        _compare_with_last_run(all_results, args.out_dir)

    if args.report != "none" and all_results:
        path = save_results(all_results, args.report, args.out_dir)
        print(f"  📁  Results saved → {path}\n")

    # Exit codes for CI
    exit_code = 0
    if args.fail_on_regression and summary["regressions"] > 0:
        print(_red("  ⛔  Exiting with code 1 — scan regressions detected."))
        exit_code = 1
    if args.fail_on_breach and summary["breaches"] > 0:
        print(_red("  ⛔  Exiting with code 1 — memory safety breaches detected."))
        exit_code = max(exit_code, 1)
    if summary["failed"] > 0:
        exit_code = max(exit_code, 2)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())