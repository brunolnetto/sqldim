"""sqldim benchmark runner — CLI entry point for the full benchmark suite.

Groups: scd (A-I), model (J-N), dgm (O-T).  Output helpers → _output.py;
critical analysis → _analysis.py.  Usage: python -m sqldim...benchmarks.runner
"""

from __future__ import annotations

import gc
import tempfile
import time
from typing import Callable

import psutil  # type: ignore[import-untyped]

from sqldim.application.benchmarks._bench_parser import (
    _LETTER_DESCRIPTIONS,
    _LETTER_TO_PATH,
    _apply_element_filter,
    _parse_cases,
    build_parser,
)
from sqldim.application.benchmarks._dataset import (
    BenchmarkDatasetGenerator,
)
from sqldim.application.benchmarks._output import (
    _red,
    _yellow,
    _bold,
    _check_memory_before_group,
    print_system_info,
    print_results_table,
    print_summary,
    save_results,
)
from sqldim.application.benchmarks._analysis import (
    print_critical_analysis,
    _compare_with_last_run,
)
from sqldim.application.benchmarks.memory_probe import (
    auto_max_tier,
    MemoryProbe,
)
from sqldim.application.benchmarks.suite import (
    BenchmarkResult,
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


# ── Letter → function map ─────────────────────────────────────────────────

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

# ── Run helpers ────────────────────────────────────────────────────────────

_TIER_ORDER = ["xs", "s", "m", "l", "xl", "xxl"]


def _resolve_effective_tier(args) -> str:
    avail_gb = psutil.virtual_memory().available / 1e9
    return auto_max_tier(avail_gb) if args.max_tier == "auto" else args.max_tier


def _print_run_header(args, groups_to_run: list, effective_max_tier: str) -> None:
    print_system_info()
    if args.max_tier == "auto":
        print(f"  Effective max tier: {effective_max_tier}")
    if args.cases:
        lines = [
            f"{gid}.{{{','.join(ef)}}}" if ef else gid for gid, ef in groups_to_run
        ]
        print(f"  Running: {' '.join(lines)}")
    print()


def _filter_results(
    results: list[BenchmarkResult], element_filter: "list[str] | None", group_id: str
) -> list[BenchmarkResult]:
    if element_filter is None:
        return results
    return _apply_element_filter(results, element_filter, group_id)


def _run_one_group(
    gen,
    tmp_dir: str,
    group_id: str,
    element_filter: "list[str] | None",
    fn,
    desc: str,
    effective_max_tier: str,
    args,
) -> "list[BenchmarkResult] | None":
    """Run a single benchmark group; apply element filter; return results or None to skip."""
    MemoryProbe.reset_hard_abort()
    try:
        _check_memory_before_group(group_id)
    except RuntimeError as mem_exc:
        print(_red(f"\n  ⛔  {mem_exc}"))
        return None

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
        results = [
            BenchmarkResult(
                case_id=f"{group_id}_group_crash",
                group=group_id,
                profile="n/a",
                tier="n/a",
                processor="n/a",
                sink="n/a",
                phase="n/a",
                n_rows=0,
                n_changed=0,
                ok=False,
                error=f"Group crash: {type(exc).__name__}: {exc}",
            )
        ]

    results = _filter_results(results, element_filter, group_id)
    if not results:
        return []

    elapsed = time.perf_counter() - t0
    path = _LETTER_TO_PATH.get(group_id, group_id)
    print(
        f"     {path} [{group_id}] completed in {elapsed:.1f}s ({len(results)} cases)"
    )
    print_results_table(results, group=f"{path} [{group_id}] — {desc}")
    return results


def _update_auto_tier(args, effective_max_tier: str, group_id: str) -> str:
    if args.max_tier != "auto":
        return effective_max_tier
    new_avail = psutil.virtual_memory().available / 1e9
    new_tier = auto_max_tier(new_avail)
    if _TIER_ORDER.index(new_tier) < _TIER_ORDER.index(effective_max_tier):
        path = _LETTER_TO_PATH.get(group_id, group_id)
        print(
            _yellow(
                f"  ⚠️  RAM dropped to {new_avail:.1f} GB after {path} [{group_id}]. "
                f"Auto-capping remaining groups at tier '{new_tier}'."
            )
        )
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


def _record_low_ram_abort(all_results: list[BenchmarkResult], group_id: str) -> None:
    all_results.append(
        BenchmarkResult(
            case_id=f"{group_id}_skipped_low_ram",
            group=group_id,
            profile="n/a",
            tier="n/a",
            processor="n/a",
            sink="n/a",
            phase="n/a",
            n_rows=0,
            n_changed=0,
            ok=False,
            error="Insufficient RAM before group",
        )
    )


def _run_all_groups(
    groups_to_run: list, args, initial_tier: str
) -> list[BenchmarkResult]:
    all_results: list[BenchmarkResult] = []
    effective_max_tier = initial_tier

    with tempfile.TemporaryDirectory(prefix="sqldim_bench_") as tmp_dir:
        with BenchmarkDatasetGenerator(tmp_root=tmp_dir) as gen:
            for group_id, element_filter in groups_to_run:
                fn = _LETTER_FN_MAP[group_id]
                path = _LETTER_TO_PATH.get(group_id, group_id)
                desc = _LETTER_DESCRIPTIONS.get(group_id, path)
                results = _run_one_group(
                    gen,
                    tmp_dir,
                    group_id,
                    element_filter,
                    fn,
                    desc,
                    effective_max_tier,
                    args,
                )
                if results is None:
                    _record_low_ram_abort(all_results, group_id)
                    break
                if results:
                    all_results.extend(results)
                gc.collect()
                time.sleep(2)
                effective_max_tier = _update_auto_tier(
                    args, effective_max_tier, group_id
                )

    return all_results


def _post_run(args, all_results: list[BenchmarkResult], summary: dict) -> None:
    if args.compare_last and all_results:
        _compare_with_last_run(all_results, args.out_dir)
    if args.report != "none" and all_results:
        path = save_results(all_results, args.report, args.out_dir)
        print(f"  📁  Results saved → {path}\n")


# ── Entry point ────────────────────────────────────────────────────────────


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    try:
        case_selection = _parse_cases(args.cases) if args.cases else None
    except ValueError as exc:
        print(_red(f"  ⛔  {exc}"))
        return 2

    groups_to_run: list[tuple[str, "list[str] | None"]] = (
        list(case_selection.items())
        if case_selection is not None
        else [(g, None) for g in _LETTER_FN_MAP]
    )
    effective_max_tier = _resolve_effective_tier(args)
    _print_run_header(args, groups_to_run, effective_max_tier)

    all_results = _run_all_groups(groups_to_run, args, effective_max_tier)

    summary = print_summary(all_results)
    print_critical_analysis(all_results)
    _post_run(args, all_results, summary)

    return _compute_exit_code(args, summary)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
