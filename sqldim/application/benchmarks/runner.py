"""sqldim benchmark runner — CLI entry point for the full benchmark suite.

Groups: scd (A-I), model (J-N), dgm (O-T).  Output helpers → _output.py;
critical analysis → _analysis.py.  Usage: python -m sqldim...benchmarks.runner
"""
from __future__ import annotations

import argparse
import gc
import tempfile
import time
from typing import Callable

import psutil

from sqldim.application.benchmarks._dataset import BenchmarkDatasetGenerator, SCALE_TIERS
from sqldim.application.benchmarks._output import (
    _red, _green, _yellow, _bold, _dim,
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
    ABORT_FLOOR_GB, auto_max_tier, MemoryProbe,
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


# ── Three-level benchmark hierarchy ───────────────────────────────────────

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

_LETTER_TO_PATH: dict[str, str] = {
    letter: f"{grp}.{sub}"
    for grp, subs in BENCH_HIERARCHY.items()
    for sub, letters in subs.items()
    for letter in letters
}

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
    "O": "DGM three-band query builder (B1 / B1∘B2 / B1∘B3 / B1∘B2∘B3) throughput",
    "P": "BDD predicate compilation throughput (compile / satisfiability / to_sql)",
    "Q": "DGMRecommender annotation + trail rule throughput",
    "R": "DGMPlanner rule cycle throughput (1a, 9, build_plan)",
    "S": "DGM multi-target exporter throughput (JSON/YAML × simple/complex)",
    "T": "DGM algebra: QuestionAlgebra build+to_sql, CSE detection+apply, CORRELATE suggest throughput",
}


# ── Cascade-spec parsing ───────────────────────────────────────────────────

def _valid_specs_hint() -> str:
    specs = [f"{g}.{s}" for g, subs in BENCH_HIERARCHY.items() for s in subs]
    return ", ".join([*BENCH_HIERARCHY.keys(), *specs])


def _letters_for(grp: str, sub: "str | None") -> list[str]:
    if sub is None:
        return [ll for subs in BENCH_HIERARCHY[grp].values() for ll in subs]
    return list(BENCH_HIERARCHY[grp][sub])


def _validate_spec(grp: str, sub: "str | None", original: str) -> None:
    if grp not in BENCH_HIERARCHY:
        raise ValueError(f"Unknown group {grp!r}. Valid: {_valid_specs_hint()}")
    if sub is not None and sub not in BENCH_HIERARCHY[grp]:
        raise ValueError(f"Unknown subgroup {grp}.{sub!r}. Valid: {_valid_specs_hint()}")


def _resolve_spec(spec: str) -> "list[tuple[str, str | None]]":
    parts = spec.split(".", 2)
    grp: str = parts[0]
    sub: "str | None" = parts[1] if len(parts) > 1 else None
    profile: "str | None" = parts[2] if len(parts) > 2 else None
    _validate_spec(grp, sub, spec)
    return [(ll, profile) for ll in _letters_for(grp, sub)]


def _update_result_entry(result: dict, group_id: str, element: "str | None") -> None:
    if group_id not in result:
        result[group_id] = None if element is None else [element]
    elif result[group_id] is None:
        pass
    elif element is None:
        result[group_id] = None
    else:
        result[group_id].append(element)  # type: ignore[union-attr]


def _parse_cases(tokens: list[str]) -> "dict[str, list[str] | None]":
    """Parse cascade CASE tokens into ``{letter_id: profile_filters | None}``."""
    result: "dict[str, list[str] | None]" = {}
    for token in tokens:
        for letter, element in _resolve_spec(token):
            _update_result_entry(result, letter, element)
    return result


def _apply_element_filter(
    results: list[BenchmarkResult],
    element_filter: list[str],
    group_id: str,
) -> list[BenchmarkResult]:
    """Return only results whose case_id contains at least one filter substring."""
    return [r for r in results if any(f in r.case_id for f in element_filter)]


# ── CLI help ───────────────────────────────────────────────────────────────

def _build_groups_epilog() -> str:
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
            "Omit to run everything."
        ),
    )
    p.add_argument(
        "--max-tier", choices=[*SCALE_TIERS.keys(), "auto"], default="auto",
        help="Maximum scale tier (default: auto — selected from available RAM).",
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
        help="Source adapter used for all benchmark groups (default: parquet).",
    )
    p.add_argument(
        "--compare-last", action="store_true",
        help="Compare throughput with the most recent saved result file.",
    )
    return p


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
            f"{gid}.{{{','.join(ef)}}}" if ef else gid
            for gid, ef in groups_to_run
        ]
        print(f"  Running: {' '.join(lines)}")
    print()


def _filter_results(results: list[BenchmarkResult], element_filter: "list[str] | None", group_id: str) -> list[BenchmarkResult]:
    if element_filter is None:
        return results
    return _apply_element_filter(results, element_filter, group_id)


def _run_one_group(gen, tmp_dir: str, group_id: str, element_filter: "list[str] | None", fn, desc: str, effective_max_tier: str, args) -> "list[BenchmarkResult] | None":
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


def _update_auto_tier(args, effective_max_tier: str, group_id: str) -> str:
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


def _record_low_ram_abort(all_results: list[BenchmarkResult], group_id: str) -> None:
    all_results.append(BenchmarkResult(
        case_id=f"{group_id}_skipped_low_ram",
        group=group_id, profile="n/a", tier="n/a",
        processor="n/a", sink="n/a", phase="n/a",
        n_rows=0, n_changed=0, ok=False,
        error="Insufficient RAM before group",
    ))


def _run_all_groups(groups_to_run: list, args, initial_tier: str) -> list[BenchmarkResult]:
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


# ── Entry point ────────────────────────────────────────────────────────────

def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    try:
        case_selection = _parse_cases(args.cases) if args.cases else None
    except ValueError as exc:
        print(_red(f"  ⛔  {exc}"))
        return 2

    groups_to_run: list[tuple[str, "list[str] | None"]] = (
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


if __name__ == "__main__":  # pragma: no cover
    import sys
    sys.exit(main())
