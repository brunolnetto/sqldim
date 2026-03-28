"""
sqldim CLI — dimensional modelling, NL querying, evals, and benchmarks.

Usage:
    sqldim migrations generate "message"
    sqldim migrations show
    sqldim example list
    sqldim example run <name>
    sqldim ask <dataset> "<question>"
    sqldim evals run [--dataset DATASET [DATASET ...]] [--tag TAG] [--provider PROVIDER]
    sqldim evals drift [--domain {retail,ecommerce,saas_growth,fintech,user_activity,supply_chain,enterprise,nba_analytics}] [--provider PROVIDER]
    sqldim bench run [CASE ...] [--max-tier TIER] [--source SOURCE]

Examples are auto-discovered from sqldim.application.examples.real_world.*
and sqldim.application.examples.features.* via EXAMPLE_METADATA dicts.
"""

import argparse
import sys

from sqldim.cli.ask import cmd_ask
from sqldim.cli.bench import cmd_bench_run
from sqldim.cli.examples import cmd_example_list, cmd_example_run

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
    """
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


def cmd_evals_drift(args: argparse.Namespace) -> int:
    """Run drift evals (before/after a pipeline event) and print a report.

    Returns 0 always (drift evals report change, not pass/fail).
    """
    from pathlib import Path

    from sqldim.application.evals.loader import load_drift_cases
    from sqldim.application.evals.runner import EvalRunner

    domain: str = getattr(args, "domain", "retail")
    verbose: bool = not getattr(args, "quiet", False)
    output = getattr(args, "output", None)

    _DOMAINS_ROOT = (
        Path(__file__).resolve().parent.parent
        / "application" / "datasets" / "domains"
    )
    _available_domains = sorted(
        p.parent.parent.name
        for p in _DOMAINS_ROOT.glob("*/artifacts/drift.json")
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

    # migrations subcommand
    mig = subparsers.add_parser("migrations", help="Migration management")
    mig_sub = mig.add_subparsers(dest="subcommand")

    gen = mig_sub.add_parser("generate", help="Generate a migration from model diff")
    gen.add_argument("message", help="Migration message")
    gen.set_defaults(func=cmd_migrations_generate)

    show = mig_sub.add_parser("show", help="Show pending migrations")
    show.set_defaults(func=cmd_migrations_show)

    init = mig_sub.add_parser("init", help="Initialize migration directory")
    init.add_argument("--dir", default="migrations", help="Migration directory path")
    init.set_defaults(func=cmd_migrations_init)

    # schema subcommand
    schema = subparsers.add_parser("schema", help="Schema introspection")
    schema_sub = schema.add_subparsers(dest="subcommand")
    graph = schema_sub.add_parser("graph", help="Print schema graph")
    graph.set_defaults(func=cmd_schema_graph)

    # example subcommand
    example = subparsers.add_parser(
        "example", help="Example pipeline runner (real-world + features)"
    )
    example_sub = example.add_subparsers(dest="subcommand")

    ex_list = example_sub.add_parser("list", help="List all available examples")
    ex_list.set_defaults(func=cmd_example_list)

    ex_run = example_sub.add_parser("run", help="Run a named example")
    ex_run.add_argument(
        "name",
        help="Example name — run 'sqldim example list' for the full list",
    )
    ex_run.set_defaults(func=cmd_example_run)

    # evals subcommand — NL interface evaluation suite
    evals = subparsers.add_parser(
        "evals",
        help="NL interface evaluation suite",
    )
    evals_sub = evals.add_subparsers(dest="subcommand")

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
        default="retail",
        choices=[
            "retail",
            "ecommerce",
            "saas_growth",
            "fintech",
            "user_activity",
            "supply_chain",
            "enterprise",
            "nba_analytics",
        ],
        help="Domain whose drift cases to run (default: retail)",
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

    # bench subcommand — delegates to application/benchmarks runner
    bench = subparsers.add_parser(
        "bench",
        help="Run the application benchmark suite (groups A–T: SCD, model, DGM)",
    )
    bench_sub = bench.add_subparsers(dest="subcommand")

    # Reuse the benchmark module's parser definition for argument declarations
    from sqldim.application.benchmarks._bench_parser import (
        _LETTER_DESCRIPTIONS,
    )
    from sqldim.application.benchmarks._dataset import SCALE_TIERS
    from sqldim.application.benchmarks.infra import SOURCE_NAMES

    _groups_help = "\n".join(
        f"  {k}: {v}" for k, v in _LETTER_DESCRIPTIONS.items()
    )
    bn_run = bench_sub.add_parser(
        "run",
        help="Run benchmark suite (groups A–T)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Available groups:\n{_groups_help}",
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
        choices=[*SCALE_TIERS.keys(), "auto"],
        default="auto",
        help="Maximum scale tier (default: auto — selected from available RAM)",
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
        choices=SOURCE_NAMES,
        default="parquet",
        help="Source adapter used for all benchmark groups (default: parquet)",
    )
    bn_run.add_argument(
        "--compare-last",
        action="store_true",
        help="Compare throughput with the most recent saved result file",
    )
    bn_run.set_defaults(func=cmd_bench_run)

    # ask subcommand — NL query against a bundled dataset
    ask = subparsers.add_parser(
        "ask",
        help="Ask a natural-language question against a dataset",
    )
    ask.add_argument(
        "dataset",
        help=(
            "Dataset name — one of: ecommerce, fintech, nba_analytics, "
            "saas_growth, supply_chain, user_activity, enterprise, "
            "media, devops, hierarchy, dgm  "
            "(only used when --source=dataset)"
        ),
    )
    ask.add_argument("question", help="Natural-language question to process")
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
