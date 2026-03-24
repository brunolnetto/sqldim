"""
sqldim/benchmarks/_output.py
==============================
Output helpers for the benchmark runner: ANSI colour codes, table printer,
summary builder, and result persistence.

Extracted from ``runner.py`` to keep that CLI module under 400 lines.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import psutil

from sqldim.application.benchmarks.memory_probe import SAFE_PCT, ABORT_FLOOR_GB


# ── ANSI colour helpers ────────────────────────────────────────────────────

def _red(s: str) -> str:    return f"\033[91m{s}\033[0m"
def _green(s: str) -> str:  return f"\033[92m{s}\033[0m"
def _yellow(s: str) -> str: return f"\033[93m{s}\033[0m"
def _bold(s: str) -> str:   return f"\033[1m{s}\033[0m"
def _dim(s: str) -> str:    return f"\033[2m{s}\033[0m"


# ── System-info banner ─────────────────────────────────────────────────────

def _check_memory_before_group(group_id: str) -> None:
    """Re-check available RAM before starting a group.

    Aborts the whole run if below ABORT_FLOOR_GB; warns if below twice
    that threshold.
    """
    import sys

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


def print_system_info() -> None:
    import sys

    vm = psutil.virtual_memory()
    total_gb  = vm.total / 1e9
    avail_gb  = vm.available / 1e9
    safe_gb   = total_gb * SAFE_PCT
    cpu_count = os.cpu_count()

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


# ── Result table printer ───────────────────────────────────────────────────

_COL_WIDTHS = {
    "case_id":      40,
    "tier":          4,
    "n_rows":       10,
    "wall_s":        7,
    "rows_per_sec": 10,
    "peak_rss_gb":   9,
    "spill_gb":      8,
    "scan_ok":       7,
    "mem_ok":        6,
    "status":        6,
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


def _ok_count(rs: list) -> int:
    return sum(1 for r in rs if r.ok)


def _with_regression(rs: list) -> list:
    return [r for r in rs if r.scan_regression]


def _with_breach(rs: list) -> list:
    return [r for r in rs if r.safety_breach]


def _with_spill(rs: list) -> list:
    return [r for r in rs if r.total_spill_gb > 0.001]


def _format_result_row(r) -> tuple[str, bool]:
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


def print_results_table(results: list, group: str = "") -> None:
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


# ── Summary ────────────────────────────────────────────────────────────────

def _print_regression_section(regressions: list) -> None:
    if regressions:
        print(_red(f"  🔴  SCAN REGRESSIONS DETECTED ({len(regressions)} cases):"))
        for r in regressions:
            print(_red(f"      - {r.case_id}: scan_count={r.scan_count}  (expected 1)"))
        print(_red("  → Fix: change VIEW → TABLE in _register_current_checksums()"))
    else:
        print(_green("  ✅  No scan regressions — all processors use TABLE materialization"))
    print()


def _print_breach_section(breaches: list) -> None:
    if breaches:
        print(_yellow(f"  ⚠️   MEMORY SAFETY BREACHES ({len(breaches)} cases):"))
        for r in breaches:
            print(_yellow(f"      - {r.case_id}: {r.breach_detail}"))
    else:
        print(_green(f"  ✅  All cases within memory safety ceiling ({SAFE_PCT*100:.0f}% of RAM)"))
    print()


def _print_spill_section(spilled: list) -> None:
    if spilled:
        print(f"  💾  Spill-to-disk detected in {len(spilled)} cases:")
        for r in spilled:
            print(f"      - {r.case_id}: {r.total_spill_gb:.3f} GB spilled")
        print("  → This is EXPECTED behavior for tight-memory or large-tier cases")
    else:
        print("  ℹ️   No spill-to-disk (all datasets fit comfortably in RAM)")
    print()


def _print_throughput_section(all_results: list) -> None:
    throughput_cases = [r for r in all_results if r.ok and r.rows_per_sec > 0]
    if not throughput_cases:
        return
    best  = max(throughput_cases, key=lambda r: r.rows_per_sec)
    worst = min(throughput_cases, key=lambda r: r.rows_per_sec)
    print("  ⚡  Throughput range:")
    print(f"      Best  : {_fmt_rps(best.rows_per_sec):<12} [{best.case_id}]")
    print(f"      Worst : {_fmt_rps(worst.rows_per_sec):<12} [{worst.case_id}]")
    print()


def print_summary(all_results: list) -> dict:
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


# ── Persistence ────────────────────────────────────────────────────────────

def save_results(results: list, fmt: str, out_dir: str) -> str:
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
