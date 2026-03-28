"""Benchmark hierarchy declarations, cascade-spec parsing, and CLI parser.

Extracted from runner.py to keep that module focused on execution logic.
"""

from __future__ import annotations

import argparse

from sqldim.application.benchmarks._dataset import SCALE_TIERS
from sqldim.application.benchmarks.suite import BenchmarkResult, SOURCE_NAMES

__all__ = [
    "BENCH_HIERARCHY",
    "_LETTER_TO_PATH",
    "SUBGROUP_DESCRIPTIONS",
    "_LETTER_DESCRIPTIONS",
    "_parse_cases",
    "_apply_element_filter",
    "build_parser",
]


# ── Three-level benchmark hierarchy ───────────────────────────────────────

BENCH_HIERARCHY: dict[str, dict[str, list[str]]] = {
    "scd": {
        "regression": ["A", "B", "C"],
        "stream": ["D", "E", "F", "G", "H"],
        "types": ["I"],
    },
    "model": {
        "dims": ["J", "K"],
        "loaders": ["L", "M"],
        "drift": ["N"],
    },
    "dgm": {
        "query": ["O", "P"],
        "model": ["Q", "R", "S"],
        "algebra": ["T"],
    },
}

_LETTER_TO_PATH: dict[str, str] = {
    letter: f"{grp}.{sub}"
    for grp, subs in BENCH_HIERARCHY.items()
    for sub, letters in subs.items()
    for letter in letters
}

SUBGROUP_DESCRIPTIONS: dict[str, str] = {
    "scd.regression": "SCD regression: scan count, memory safety, throughput scaling",
    "scd.stream": "SCD streaming: batch vs stream, change rate, processor, spill, source/sink",
    "scd.types": "SCD type variety: Type3, Type4 processor throughput",
    "model.dims": "Dimensional model: prebuilt dims (Date/Time) and graph query",
    "model.loaders": "Model loaders: Narwhals SCD2 backfill and ORM/Medallion loaders",
    "model.drift": "Model drift: schema/quality drift observability (DriftObservatory star schema)",
    "dgm.query": "DGM query algebra: three-band query builder and BDD predicate compiler",
    "dgm.model": "DGM model: recommender annotation, planner rule cycles, multi-target exporter",
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


def _letters_for(grp: str, sub: str | None) -> list[str]:
    if sub is None:
        return [ll for subs in BENCH_HIERARCHY[grp].values() for ll in subs]
    return list(BENCH_HIERARCHY[grp][sub])


def _validate_spec(grp: str, sub: str | None, original: str) -> None:
    if grp not in BENCH_HIERARCHY:
        raise ValueError(f"Unknown group {grp!r}. Valid: {_valid_specs_hint()}")
    if sub is not None and sub not in BENCH_HIERARCHY[grp]:
        raise ValueError(
            f"Unknown subgroup {grp}.{sub!r}. Valid: {_valid_specs_hint()}"
        )


def _resolve_spec(spec: str) -> list[tuple[str, str | None]]:
    parts = spec.split(".", 2)
    grp: str = parts[0]
    sub: str | None = parts[1] if len(parts) > 1 else None
    profile: str | None = parts[2] if len(parts) > 2 else None
    _validate_spec(grp, sub, spec)
    return [(ll, profile) for ll in _letters_for(grp, sub)]


def _update_result_entry(result: dict, group_id: str, element: str | None) -> None:
    if group_id not in result:
        result[group_id] = None if element is None else [element]
    elif result[group_id] is None:
        pass
    elif element is None:
        result[group_id] = None
    else:
        result[group_id].append(element)  # type: ignore[union-attr]


def _parse_cases(tokens: list[str]) -> dict[str, list[str] | None]:
    """Parse cascade CASE tokens into ``{letter_id: profile_filters | None}``."""
    result: dict[str, list[str] | None] = {}
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
        "cases",
        nargs="*",
        metavar="CASE",
        help=(
            "Cascade specs to run: group, group.subgroup, or group.subgroup.profile. "
            "Omit to run everything."
        ),
    )
    p.add_argument(
        "--max-tier",
        choices=[*SCALE_TIERS.keys(), "auto"],
        default="auto",
        help="Maximum scale tier (default: auto — selected from available RAM).",
    )
    p.add_argument(
        "--report",
        choices=["none", "json", "csv"],
        default="json",
        help="Output format for results file",
    )
    p.add_argument(
        "--out-dir",
        default="sqldim/benchmarks/results",
        help="Directory to write result files",
    )
    p.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="Exit with code 1 if any scan regression is detected",
    )
    p.add_argument(
        "--fail-on-breach",
        action="store_true",
        help="Exit with code 1 if any memory safety breach is detected",
    )
    p.add_argument(
        "--source",
        choices=SOURCE_NAMES,
        default="parquet",
        help="Source adapter used for all benchmark groups (default: parquet).",
    )
    p.add_argument(
        "--compare-last",
        action="store_true",
        help="Compare throughput with the most recent saved result file.",
    )
    return p
