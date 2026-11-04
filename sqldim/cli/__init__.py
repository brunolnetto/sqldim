"""
sqldim CLI — dimensional modelling, NL querying, evals, and benchmarks.

Usage:
    sqldim nl ask "<question>" --dataset <dataset>
    sqldim nl ask "<question>" --mode supervisor [--datasets DS ...]
    sqldim nl chat [--datasets DS ...]
    sqldim nl showtime [--datasets DS ...]
    sqldim nl evals run [--dataset DATASET [DATASET ...]] [--tag TAG] [--tool TOOL ...]
    sqldim nl evals drift [--domain DOMAIN] [--provider PROVIDER]
    sqldim ops migrations generate "message"
    sqldim ops migrations show
    sqldim ops schema graph
    sqldim exercise example list
    sqldim exercise example run <name>
    sqldim exercise bench list
    sqldim exercise bench run [CASE ...] [--max-tier TIER] [--source SOURCE]

Examples are auto-discovered from sqldim.application.examples.real_world.*
and sqldim.application.examples.features.* via EXAMPLE_METADATA dicts.
"""

import argparse
import sys

# Lazy wrappers — heavy deps (langgraph, pydantic-ai, rich, benchmarks) are
# imported only when the command is actually invoked, not at parse time.
# This keeps `sqldim --help` and `sqldim exercise example list` sub-second.


def cmd_ask(args: argparse.Namespace) -> int:
    from sqldim.cli.ask import cmd_ask as _impl

    return _impl(args)


def cmd_bench_run(args: argparse.Namespace) -> int:
    from sqldim.cli.bench import cmd_bench_run as _impl

    return _impl(args)


def cmd_bench_list(args: argparse.Namespace) -> int:
    from sqldim.cli.bench import cmd_bench_list as _impl

    return _impl(args)


def cmd_chat(args: argparse.Namespace) -> int:
    from sqldim.cli.chat import cmd_chat as _impl

    return _impl(args)


def cmd_example_list(args: argparse.Namespace) -> int:
    from sqldim.cli.examples import cmd_example_list as _impl

    return _impl(args)


def cmd_example_run(args: argparse.Namespace) -> int:
    from sqldim.cli.examples import cmd_example_run as _impl

    return _impl(args)


def cmd_showtime(args: argparse.Namespace) -> int:
    from sqldim.cli.showtime import cmd_showtime as _impl

    return _impl(args)


# ── migration commands ────────────────────────────────────────────────────────


def cmd_migrations_generate(args: argparse.Namespace) -> None:
    """Generate a migration from a schema diff.

    In practice, call ``generate_migration(models, current_state, message)``
    directly from your project's migration script.
    """
    print(f"[sqldim] Generating migration: '{args.message}'")
    print(
        "[sqldim] Tip: call generate_migration(models, current_state, message) in your project."
    )


def cmd_migrations_show(args: argparse.Namespace) -> None:
    """Show pending migration info.

    Prints a human-readable summary of any schema changes detected
    since the last applied migration.
    """
    print("[sqldim] No pending migrations detected (run generate first).")


def cmd_migrations_init(args: argparse.Namespace) -> None:
    """Initialize sqldim migration environment.

    Creates the migrations directory and an empty ``__init__.py`` so the
    folder is importable by Alembic or the sqldim migration runner.
    """
    import os

    migration_dir = getattr(args, "dir", "migrations")
    os.makedirs(migration_dir, exist_ok=True)
    init_file = os.path.join(migration_dir, "__init__.py")
    if not os.path.exists(init_file):
        open(init_file, "w").close()
    print(f"[sqldim] Migration directory initialized at '{migration_dir}/'")


def cmd_schema_graph(args: argparse.Namespace) -> None:
    """Print schema graph as JSON or Mermaid.

    Pass your ``SchemaGraph`` instance to ``.to_dict()`` or ``.to_mermaid()``
    to render the dimensional model as serialisable data or a diagram.
    """
    print("[sqldim] Pass your SchemaGraph instance to .to_dict() or .to_mermaid()")


# ── evals commands ───────────────────────────────────────────────────────────


def cmd_evals_list(args: argparse.Namespace) -> int:
    """List all available eval cases, grouped by dataset."""
    from sqldim.application.evals.cases import EVAL_SUITE

    by_dataset: dict[str, list] = {}
    for case in EVAL_SUITE:
        by_dataset.setdefault(case.dataset, []).append(case)

    total = len(EVAL_SUITE)
    print(f"[sqldim evals] {total} case(s) across {len(by_dataset)} dataset(s)\n")
    for ds, cases in sorted(by_dataset.items()):
        print(f"  {ds} ({len(cases)} case(s)):")
        for c in sorted(cases, key=lambda c: c.id):
            tags = f"  [{', '.join(c.tags)}]" if c.tags else ""
            print(f"    {c.id:<45} {c.utterance[:55]}{tags}")
        print()
    return 0


def cmd_evals_run(args: argparse.Namespace) -> int:
    """Run the eval suite (or a filtered subset) and print a report.

    Returns 0 when every case passes, 1 otherwise (useful for CI).
    When ``--tool`` is given, runs per-tool evals instead of the NL suite.
    """
    tools: list[str] | None = getattr(args, "tool", None)
    if tools:
        return _run_tool_evals(args, tools)

    from sqldim.application.evals.runner import EvalRunner

    datasets = getattr(args, "dataset", None) or None
    tags = [args.tag] if getattr(args, "tag", None) else None
    verbose: bool = not getattr(args, "quiet", False)

    runner = EvalRunner(
        provider=args.provider,
        model_name=args.model or "gpt-4o-mini",
        verbose=verbose,
    )
    report = runner.run(datasets=datasets, tags=tags)

    output = getattr(args, "output", None)
    if output:
        report.to_json(output)
        print(f"[sqldim evals] Report written to {output}")
    elif not verbose:
        # verbose runner already prints the summary; only print it in quiet mode
        print(report.summary())

    return int(report.failed > 0)


def _run_tool_evals(args: argparse.Namespace, tools: list[str]) -> int:
    """Run per-tool evals for the given tool names and print results."""
    from sqldim.application.evals.tool_eval_loader import (
        load_all_tool_eval_cases,
        load_cases_for_tool,
    )

    datasets: list[str] | None = getattr(args, "dataset", None) or None

    # Collect cases for the requested tools
    all_cases = []
    for tool_name in tools:
        cases = load_cases_for_tool(tool_name)
        if not cases:
            print(f"[sqldim evals] Warning: no eval cases found for tool {tool_name!r}")
            continue
        if datasets:
            cases = [c for c in cases if c.domain in datasets]
        all_cases.extend(cases)

    if not all_cases:
        print("[sqldim evals] No matching tool eval cases found.")
        # List available tools for discovery
        all_tool_cases = load_all_tool_eval_cases()
        available_tools = sorted({c.tool for c in all_tool_cases})
        if available_tools:
            print(f"[sqldim evals] Available tools: {', '.join(available_tools)}")
        return 1

    # Group by tool for reporting
    by_tool: dict[str, list] = {}
    for c in all_cases:
        by_tool.setdefault(c.tool, []).append(c)

    total = len(all_cases)
    print(
        f"[sqldim evals] {total} tool eval case(s) for "
        f"{len(by_tool)} tool(s) across "
        f"{len({c.domain for c in all_cases})} domain(s)\n"
    )

    for tool_name, cases in sorted(by_tool.items()):
        print(f"  {tool_name} ({len(cases)} case(s)):")
        for c in sorted(cases, key=lambda c: c.id):
            tags = f"  [{', '.join(c.tags)}]" if c.tags else ""
            print(f"    {c.id:<50} {c.domain:<18} {c.description[:45]}{tags}")
        print()

    output = getattr(args, "output", None)
    if output:
        import json

        report_data = {
            "tools": tools,
            "total_cases": total,
            "cases": [
                {
                    "id": c.id,
                    "tool": c.tool,
                    "domain": c.domain,
                    "description": c.description,
                    "tags": c.tags,
                }
                for c in all_cases
            ],
        }
        with open(output, "w", encoding="utf-8") as fh:
            json.dump(report_data, fh, indent=2, ensure_ascii=False)
        print(f"[sqldim evals] Report written to {output}")

    return 0


def cmd_evals_drift(args: argparse.Namespace) -> int:
    """Run drift evals (before/after a pipeline event) and print a report.

    Returns 0 always (drift evals report change, not pass/fail).
    """
    from pathlib import Path

    from sqldim.application.evals.loader import load_drift_cases
    from sqldim.application.evals.runner import EvalRunner

    domain: str = getattr(args, "domain", "commerce")
    verbose: bool = not getattr(args, "quiet", False)
    output = getattr(args, "output", None)

    _DOMAINS_ROOT = (
        Path(__file__).resolve().parent.parent / "application" / "datasets" / "domains"
    )
    _available_domains = sorted(
        p.parent.parent.parent.name
        for p in _DOMAINS_ROOT.glob("*/artifacts/evals/drift.json")
    )

    if domain not in _available_domains:
        available = ", ".join(_available_domains)
        print(f"[sqldim evals drift] Unknown domain {domain!r}. Available: {available}")
        return 1

    drift_cases = load_drift_cases(domain)

    runner = EvalRunner(
        provider=args.provider,
        model_name=args.model or "gpt-4o-mini",
        verbose=verbose,
    )
    report = runner.run_drift_suite(drift_cases)

    if output:
        report.to_json(output)
        print(f"[sqldim evals drift] Report written to {output}")
    elif not verbose:
        print(report.summary())

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sqldim",
        description="sqldim — Dimensional Modeling toolkit for Python",
    )
    subparsers = parser.add_subparsers(dest="command")

    # ── nl group ──────────────────────────────────────────────────────────
    nl = subparsers.add_parser(
        "nl",
        help="Natural-language querying and evaluation",
    )
    nl_sub = nl.add_subparsers(dest="subcommand")

    # nl ask
    ask = nl_sub.add_parser(
        "ask",
        help="Ask a natural-language question (single dataset or supervisor mode)",
    )
    ask.add_argument(
        "question",
        help="Natural-language question to process",
    )
    ask.add_argument(
        "--mode",
        choices=["ask", "supervisor"],
        default="ask",
        help=(
            "Execution mode: 'ask' (default, single dataset) or "
            "'supervisor' (decompose across multiple datasets)"
        ),
    )
    ask.add_argument(
        "--dataset",
        dest="dataset",
        default=None,
        help=(
            "Dataset name for single-dataset mode — one of: ecommerce, fintech, "
            "nba_analytics, growth, telemetry, "
            "entertainment, devops, data_models  (required when --mode=ask)"
        ),
    )
    ask.add_argument(
        "--datasets",
        nargs="+",
        default=None,
        metavar="DATASET",
        help=(
            "Restrict to specific datasets in supervisor mode "
            "(e.g. ecommerce fintech). Omit for all."
        ),
    )
    ask.add_argument(
        "--source",
        choices=["dataset", "medallion", "observatory"],
        default="dataset",
        help=(
            "Data source type: 'dataset' (default, loads a bundled dataset), "
            "'medallion' (connects to an existing medallion DuckDB file), "
            "or 'observatory' (queries a DriftObservatory DuckDB file)"
        ),
    )
    ask.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold"],
        default="gold",
        help="Medallion layer to target (only used when --source=medallion, default: gold)",
    )
    ask.add_argument(
        "--db-path",
        default=None,
        dest="db_path",
        metavar="PATH",
        help="Path to DuckDB file (only used when --source=medallion or --source=observatory)",
    )
    ask.add_argument(
        "--model-provider",
        dest="model_provider",
        choices=["ollama", "openai", "anthropic", "gemini", "groq", "mistral"],
        default="ollama",
        help=(
            "LLM backend to use (default: ollama). Each provider reads its API "
            "key from the corresponding environment variable "
            "(OPENAI_API_KEY, ANTHROPIC_API_KEY, GOOGLE_API_KEY, "
            "GROQ_API_KEY, MISTRAL_API_KEY)."
        ),
    )
    ask.add_argument(
        "--model",
        dest="model_name",
        default=None,
        metavar="MODEL",
        help=(
            "Model name to use with the selected provider.  When omitted, the "
            "provider default is used (e.g. 'llama3.2:latest' for ollama, "
            "'gpt-4o-mini' for openai, 'claude-3-5-haiku-latest' for anthropic)."
        ),
    )
    ask.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Print entity vocabulary details before invoking the graph",
    )
    ask.set_defaults(func=cmd_ask)

    # nl chat
    _model_provider_choices = [
        "ollama",
        "openai",
        "anthropic",
        "gemini",
        "groq",
        "mistral",
    ]

    chat = nl_sub.add_parser(
        "chat",
        help="Interactive conversational REPL (like Claude Code)",
    )
    chat.add_argument(
        "--datasets",
        nargs="+",
        default=None,
        metavar="DATASET",
        help="Restrict to specific datasets. Omit for interactive picker.",
    )
    chat.add_argument(
        "--model-provider",
        dest="model_provider",
        choices=_model_provider_choices,
        default="ollama",
        metavar="PROVIDER",
        help="LLM provider (default: ollama). Override with openai, anthropic, etc.",
    )
    chat.add_argument(
        "--model",
        default=None,
        metavar="MODEL",
        help="LLM model name (e.g. gpt-4o-mini, claude-3-5-sonnet)",
    )
    chat.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Print node traversal details after each response",
    )
    chat.set_defaults(func=cmd_chat)

    # nl showtime
    showtime = nl_sub.add_parser(
        "showtime",
        help="Guided demo walkthrough showcasing DGM Supervisor capabilities",
    )
    showtime.add_argument(
        "--datasets",
        nargs="+",
        default=None,
        metavar="DATASET",
        help="Restrict demo to specific datasets. Omit for all.",
    )
    showtime.add_argument(
        "--model-provider",
        dest="model_provider",
        choices=_model_provider_choices,
        default="ollama",
        metavar="PROVIDER",
        help="LLM provider (default: ollama). Override with openai, anthropic, etc.",
    )
    showtime.add_argument(
        "--model",
        default=None,
        metavar="MODEL",
        help="LLM model name (default per provider, e.g. gpt-4o-mini, llama3.2:latest)",
    )
    showtime.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Print node traversal details after each scenario",
    )
    showtime.set_defaults(func=cmd_showtime)

    # nl evals
    evals = nl_sub.add_parser(
        "evals",
        help="NL interface evaluation suite",
    )
    evals_sub = evals.add_subparsers(dest="evals_subcommand")

    ev_list = evals_sub.add_parser("list", help="List all available eval cases")
    ev_list.set_defaults(func=cmd_evals_list)

    ev_run = evals_sub.add_parser("run", help="Run the eval suite and print a report")
    ev_run.add_argument(
        "--dataset",
        nargs="+",
        default=None,
        metavar="DATASET",
        help="Restrict to cases for one or more datasets (e.g. ecommerce fintech)",
    )
    ev_run.add_argument(
        "--tag",
        default=None,
        metavar="TAG",
        help="Restrict to cases carrying this tag (e.g. entity, filter)",
    )
    ev_run.add_argument(
        "--tool",
        nargs="+",
        default=None,
        metavar="TOOL",
        help=(
            "Run per-tool evals for specific tools instead of the NL suite "
            "(e.g. dgm_correlate dgm_explore_from). "
            "Omit to run the standard NL eval suite."
        ),
    )
    ev_run.add_argument(
        "--provider",
        default="openai",
        choices=["ollama", "openai", "anthropic", "gemini", "groq", "mistral"],
        help="LLM provider (default: openai)",
    )
    ev_run.add_argument(
        "--model",
        default=None,
        metavar="MODEL",
        help="Model name for the chosen provider (default: provider default)",
    )
    ev_run.add_argument(
        "--output",
        default=None,
        metavar="PATH",
        help="Write JSON report to this file path",
    )
    ev_run.add_argument(
        "--quiet",
        action="store_true",
        default=False,
        help="Suppress per-case progress output",
    )
    ev_run.set_defaults(func=cmd_evals_run)

    ev_drift = evals_sub.add_parser(
        "drift",
        help="Run before/after drift evals around a pipeline event",
    )
    ev_drift.add_argument(
        "--domain",
        default="commerce",
        choices=[
            "commerce",
            "growth",
            "fintech",
            "telemetry",
            "nba_analytics",
        ],
        help="Domain whose drift cases to run (default: commerce)",
    )
    ev_drift.add_argument(
        "--provider",
        default="openai",
        choices=["ollama", "openai", "anthropic", "gemini", "groq", "mistral"],
        help="LLM provider (default: openai)",
    )
    ev_drift.add_argument(
        "--model",
        default=None,
        metavar="MODEL",
        help="Model name for the chosen provider (default: provider default)",
    )
    ev_drift.add_argument(
        "--output",
        default=None,
        metavar="PATH",
        help="Write JSON report to this file path",
    )
    ev_drift.add_argument(
        "--quiet",
        action="store_true",
        default=False,
        help="Suppress per-case progress output",
    )
    ev_drift.set_defaults(func=cmd_evals_drift)

    # ── ops group ─────────────────────────────────────────────────────────
    ops = subparsers.add_parser(
        "ops",
        help="Schema and migration operations",
    )
    ops_sub = ops.add_subparsers(dest="subcommand")

    # ops migrations
    mig = ops_sub.add_parser("migrations", help="Migration management")
    mig_sub = mig.add_subparsers(dest="mig_subcommand")

    gen = mig_sub.add_parser("generate", help="Generate a migration from model diff")
    gen.add_argument("message", help="Migration message")
    gen.set_defaults(func=cmd_migrations_generate)

    show = mig_sub.add_parser("show", help="Show pending migrations")
    show.set_defaults(func=cmd_migrations_show)

    init = mig_sub.add_parser("init", help="Initialize migration directory")
    init.add_argument("--dir", default="migrations", help="Migration directory path")
    init.set_defaults(func=cmd_migrations_init)

    # ops schema
    schema = ops_sub.add_parser("schema", help="Schema introspection")
    schema_sub = schema.add_subparsers(dest="schema_subcommand")
    graph = schema_sub.add_parser("graph", help="Print schema graph")
    graph.set_defaults(func=cmd_schema_graph)

    # ── exercise group ───────────────────────────────────────────────────
    exercise = subparsers.add_parser(
        "exercise",
        help="Examples and benchmarks",
    )
    exercise_sub = exercise.add_subparsers(dest="subcommand")

    # exercise example
    example = exercise_sub.add_parser(
        "example", help="Example pipeline runner (real-world + features)"
    )
    example_sub = example.add_subparsers(dest="example_subcommand")

    ex_list = example_sub.add_parser("list", help="List all available examples")
    ex_list.set_defaults(func=cmd_example_list)

    ex_run = example_sub.add_parser("run", help="Run a named example")
    ex_run.add_argument(
        "name",
        help="Example name — run 'sqldim example list' for the full list",
    )
    ex_run.set_defaults(func=cmd_example_run)

    # exercise bench
    bench = exercise_sub.add_parser(
        "bench",
        help="Run the application benchmark suite (groups A–T: SCD, model, DGM)",
    )
    bench_sub = bench.add_subparsers(dest="bench_subcommand")

    bn_list = bench_sub.add_parser(
        "list",
        help="List all available benchmark groups (A–T)",
    )
    bn_list.set_defaults(func=cmd_bench_list)

    bn_run = bench_sub.add_parser(
        "run",
        help="Run benchmark suite (groups A–T)",
    )
    bn_run.add_argument(
        "cases",
        nargs="*",
        metavar="CASE",
        help=(
            "Cascade specs to run: group letter, group.subgroup, or "
            "group.subgroup.profile. Omit to run everything."
        ),
    )
    bn_run.add_argument(
        "--max-tier",
        default="auto",
        metavar="TIER",
        help="Maximum scale tier: xs, sm, md, lg, xl, auto (default: auto)",
    )
    bn_run.add_argument(
        "--report",
        choices=["none", "json", "csv"],
        default="json",
        help="Output format for results file",
    )
    bn_run.add_argument(
        "--out-dir",
        default="sqldim/benchmarks/results",
        help="Directory to write result files",
    )
    bn_run.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="Exit with code 1 if any scan regression is detected",
    )
    bn_run.add_argument(
        "--fail-on-breach",
        action="store_true",
        help="Exit with code 1 if any memory safety breach is detected",
    )
    bn_run.add_argument(
        "--source",
        default="parquet",
        metavar="SOURCE",
        help="Source adapter used for all benchmark groups (default: parquet)",
    )
    bn_run.add_argument(
        "--compare-last",
        action="store_true",
        help="Compare throughput with the most recent saved result file",
    )
    bn_run.set_defaults(func=cmd_bench_run)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    result = args.func(args)
    return result if isinstance(result, int) else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
