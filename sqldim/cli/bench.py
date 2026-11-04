"""CLI bench commands — ``sqldim exercise bench {list,run}``.

Thin wrapper around :mod:`sqldim.application.benchmarks.runner` — the
comprehensive benchmark suite (groups A–T: SCD, model, DGM).  The CLI
subcommand delegates directly to the application benchmark runner.
"""

from __future__ import annotations

import argparse


def bench_parser() -> argparse.ArgumentParser:
    """Return the argparse parser defined by the benchmarks module.

    Exposed so ``cli/__init__.py`` can merge its arguments into the
    top-level ``sqldim exercise bench run`` sub-parser.
    """
    from sqldim.application.benchmarks._bench_parser import (
        build_parser as _build_bench_parser,
    )

    return _build_bench_parser()


def cmd_bench_list(args: argparse.Namespace) -> int:
    """List all available benchmark groups (A–T) with descriptions."""
    from sqldim.application.benchmarks._bench_parser import (
        BENCH_HIERARCHY,
        SUBGROUP_DESCRIPTIONS,
        _LETTER_DESCRIPTIONS,
    )

    total_letters = sum(
        len(letters) for subs in BENCH_HIERARCHY.values() for letters in subs.values()
    )
    print(
        f"[sqldim bench] {total_letters} benchmark group(s) across "
        f"{len(BENCH_HIERARCHY)} domain(s)\n"
    )

    for domain, subgroups in sorted(BENCH_HIERARCHY.items()):
        for subgroup, letters in sorted(subgroups.items()):
            desc = SUBGROUP_DESCRIPTIONS.get(f"{domain}.{subgroup}", subgroup)
            print(f"  {domain}.{subgroup} — {desc}")
            for letter in sorted(letters):
                letter_desc = _LETTER_DESCRIPTIONS.get(letter, "")
                print(f"    [{letter}] {letter_desc}")
            print()
    return 0


def cmd_bench_run(args: argparse.Namespace) -> int:
    """Run the application benchmark suite (groups A–T).

    Delegates to :func:`sqldim.application.benchmarks.runner.main` with
    the argv list reconstructed from the parsed *args* namespace.
    """
    from sqldim.application.benchmarks.runner import main as bench_main

    argv: list[str] = []

    # positional: cascade case specs (e.g. "scd", "A", "dgm.query")
    cases: list[str] | None = getattr(args, "cases", None)
    if cases:
        argv.extend(cases)

    # --max-tier
    max_tier: str = getattr(args, "max_tier", "auto")
    argv.extend(["--max-tier", max_tier])

    # --report
    report: str = getattr(args, "report", "json")
    argv.extend(["--report", report])

    # --out-dir
    out_dir: str | None = getattr(args, "out_dir", None)
    if out_dir:
        argv.extend(["--out-dir", out_dir])

    # --source
    source: str = getattr(args, "source", "parquet")
    argv.extend(["--source", source])

    # boolean flags
    if getattr(args, "fail_on_regression", False):
        argv.append("--fail-on-regression")
    if getattr(args, "fail_on_breach", False):
        argv.append("--fail-on-breach")
    if getattr(args, "compare_last", False):
        argv.append("--compare-last")

    return bench_main(argv)
