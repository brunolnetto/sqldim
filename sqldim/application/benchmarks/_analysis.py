"""
sqldim/benchmarks/_analysis.py
================================
Critical-analysis helpers for the benchmark runner.

Includes per-group diagnostic functions (tier scaling, change rate,
spill overhead, source adapters, ins/ver/unc breakdown) as well as the
delta-comparison helpers that flag regressions against a previous run.

Extracted from ``runner.py`` to keep that CLI module under 400 lines.
"""

from __future__ import annotations

import json
from pathlib import Path

from sqldim.application.benchmarks._output import (
    _red,
    _green,
    _yellow,
    _bold,
    _dim,
    _fmt_rps,
)


# ── Per-group diagnostic helpers ──────────────────────────────────────────


def _c_throughput_sorted(ok_results: list) -> list:
    return sorted(
        [r for r in ok_results if r.group == "C_throughput"],
        key=lambda r: r.n_rows,
    )


def _tier_scale_flag(pct: float, row_factor: float) -> tuple[str, bool]:
    if pct < 10:
        return _yellow(
            f"⚠️  near-plateau (+{pct:.0f}%, {row_factor:.0f}x more rows)"
        ), True
    if pct < 0:
        return _red(f"🔴 regression ({pct:+.0f}%)"), False
    return _green(f"✅ +{pct:.0f}%"), False


def _analyse_tier_scaling(ok_results: list) -> None:
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
        print(
            f"      {prev.tier}→{cur.tier}: "
            f"{_fmt_rps(prev.rows_per_sec)} → {_fmt_rps(cur.rows_per_sec)}  {flag}"
        )
        prev = cur
    if plateaus:
        print(
            _yellow(
                f"  → Plateau at tier(s) {plateaus}: DuckDB parallelism is saturated. "
                "Not a regression — expected at xl+."
            )
        )
    print()


def _e_change_rate_cases(ok_results: list) -> list:
    return [r for r in ok_results if r.group == "E_change_rate"]


def _analyse_change_rate(ok_results: list) -> None:
    """Group E — change-rate variance."""
    e_cases = _e_change_rate_cases(ok_results)
    if len(e_cases) < 2:
        return
    rps_vals = [r.rows_per_sec for r in e_cases]
    rps_range = (max(rps_vals) - min(rps_vals)) / max(max(rps_vals), 1) * 100
    print("  🔄  Group E — Change-rate sensitivity:")
    for r in e_cases:
        label = r.case_id.split("_")[-1]
        bar = "▓" * int(r.rows_per_sec / max(rps_vals) * 20)
        print(f"      {label:<10}: {_fmt_rps(r.rows_per_sec):>10}  {bar:<20}")
    if rps_range > 20:
        print(
            _yellow(
                f"  → {rps_range:.1f}% throughput variance — processor is sensitive "
                "to change rate as expected."
            )
        )
    print()


def _spill_flag(ratio: float, spill_gb: float) -> str:
    if ratio > 1.5:
        return _yellow(f"⚠️  {ratio:.1f}x slower under pressure")
    if ratio < 0.95:
        if spill_gb == 0.0:
            return _yellow(
                f"⚠️  no-spill {1 / ratio:.1f}x FASTER under tight limit "
                f"— smaller DuckDB buffer pool reduces overhead for this working set"
            )
        return _yellow(
            f"⚠️  spill {1 / ratio:.1f}x FASTER — check measurement isolation"
        )
    return _green("✅ marginal")


def _spill_grp_g(ok_results: list) -> dict:
    return {r.tier: r for r in ok_results if r.group == "G_beyond_memory"}


def _spill_grp_b(ok_results: list) -> dict:
    return {
        r.tier: r
        for r in ok_results
        if r.group == "B_memory_safety" and r.profile == "cnpj_empresa"
    }


def _analyse_spill_overhead(ok_results: list) -> None:
    """Group G vs B — spill overhead."""
    g_cases = _spill_grp_g(ok_results)
    b_cases = _spill_grp_b(ok_results)
    shared = sorted(
        set(g_cases) & set(b_cases),
        key=lambda t: ["xs", "s", "m", "l", "xl", "xxl"].index(t),
    )
    if not shared:
        return
    print("  💾  Group G — Spill overhead vs Group B free-run:")
    for tier in shared:
        g, b = g_cases[tier], b_cases[tier]
        ratio = b.rows_per_sec / max(g.rows_per_sec, 1)
        flag = _spill_flag(ratio, g.total_spill_gb)
        print(
            f"      {tier}: free {_fmt_rps(b.rows_per_sec)} → "
            f"spill {_fmt_rps(g.rows_per_sec)}  {flag}"
        )
    print()


def _h_source_cases(ok_results: list) -> list:
    return [r for r in ok_results if r.group == "H_source_sink_matrix"]


def _analyse_source_adapters(ok_results: list) -> None:
    """Group H — source adapter comparison."""
    h_cases = _h_source_cases(ok_results)
    if len(h_cases) < 2:
        return
    baseline = max(h_cases, key=lambda r: r.rows_per_sec)
    print("  🔌  Group H — Source adapter comparison:")
    for r in h_cases:
        ratio = r.rows_per_sec / max(baseline.rows_per_sec, 1)
        bar = "▓" * int(ratio * 20)
        flag = (
            _green("✅ fastest")
            if r is baseline
            else _dim(f"{ratio * 100:.0f}% of best")
        )
        print(f"      {r.source:<8}: {_fmt_rps(r.rows_per_sec):>10}  {bar:<20}  {flag}")
    print()


def _analyse_ins_ver_unc(ok_results: list) -> None:
    """Overall insert/version/unchanged breakdown."""
    tracked = [r for r in ok_results if r.inserted + r.versioned + r.unchanged > 0]
    if not tracked:
        return
    print("  🔢  Insert / Version / Unchanged breakdown:")
    for r in tracked:
        total = r.inserted + r.versioned + r.unchanged
        i_pct = r.inserted / total * 100
        v_pct = r.versioned / total * 100
        u_pct = r.unchanged / total * 100
        print(
            f"      {r.case_id:<42} "
            f"ins {i_pct:4.0f}%  ver {v_pct:4.0f}%  unc {u_pct:4.0f}%"
        )
    print()


def print_critical_analysis(all_results: list) -> None:
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


# ── Delta comparison (run vs run) ──────────────────────────────────────────


def _load_previous_rps(out_dir: str) -> "tuple[dict, str] | None":
    """Return ``(prev_rps_dict, filename)`` for the last completed run, or None."""
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
    return {
        row["case_id"]: row.get("rows_per_sec", 0) for row in prev_rows
    }, prev_path.name


def _print_rps_rows(rows: list, colour_fn) -> None:
    for cid, old, new in rows:
        pct = (new - old) / old * 100
        print(
            colour_fn(
                f"      {cid:<42} {_fmt_rps(old)} → {_fmt_rps(new)}  ({pct:+.0f}%)"
            )
        )


def _is_regression(prev: float, cur: float) -> bool:
    return prev > 0 and (cur - prev) / prev < -0.15


def _is_improvement(prev: float, cur: float) -> bool:
    return prev > 0 and (cur - prev) / prev > 0.15


def _compute_deltas(prev_rps: dict, cur_rps: dict, shared: list) -> tuple[list, list]:
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


def _print_delta_table(prev_rps: dict, cur_rps: dict, prev_name: str) -> None:
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
        print(
            _red(
                f"  🔴  Throughput regressions (>15% slower, {len(regressions)} cases):"
            )
        )
        _print_rps_rows(regressions, _red)
    else:
        print(_green("  ✅  No throughput regressions vs last run."))
    if improvements:
        print(_green(f"  🚀  Improvements (>15% faster, {len(improvements)} cases):"))
        _print_rps_rows(improvements, _green)
    print()


def _compare_with_last_run(current: list, out_dir: str) -> None:
    """Load the most recent result JSON from *out_dir* and flag regressions."""
    loaded = _load_previous_rps(out_dir)
    if loaded is None:
        return
    prev_rps, prev_name = loaded
    cur_rps = {r.case_id: r.rows_per_sec for r in current if r.ok}
    _print_delta_table(prev_rps, cur_rps, prev_name)
