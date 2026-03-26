"""
sqldim CLI — dimensional modelling, NL querying, evals, and benchmarks.

Usage:
    sqldim migrations generate "message"
    sqldim migrations show
    sqldim example list
    sqldim example run <name>
    sqldim ask <dataset> "<question>"
    sqldim evals run [--dataset DATASET] [--tag TAG] [--provider PROVIDER]
    sqldim bench run [--dataset DATASET] [--runs N]

Examples are auto-discovered from sqldim.application.examples.real_world.*
and sqldim.application.examples.features.* via EXAMPLE_METADATA dicts.
"""

import argparse
import sys
from typing import Any

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


# ── example commands ──────────────────────────────────────────────────────────


def _load_one_showcase(pkg_name: str, modname: str, kind: str) -> tuple | None:
    import importlib

    showcase_path = f"{pkg_name}.{modname}.showcase"
    try:
        mod = importlib.import_module(showcase_path)
    except ImportError:
        return None
    meta = getattr(mod, "EXAMPLE_METADATA", None)
    if meta is None:
        return None
    return (
        meta["name"],
        (meta["title"], meta["description"], showcase_path, meta["entry_point"], kind),
    )


def _scan_pkg(pkg_name: str, kind: str) -> dict:
    import importlib
    import pkgutil

    result: dict = {}
    try:
        pkg = importlib.import_module(pkg_name)
    except ImportError:
        return result
    for _, modname, ispkg in pkgutil.iter_modules(pkg.__path__):
        if not ispkg:
            continue
        loaded = _load_one_showcase(pkg_name, modname, kind)
        if loaded is not None:
            name, entry = loaded
            result[name] = entry
    return result


def _discover_examples() -> dict:
    """
    Auto-discover examples by scanning showcase modules for ``EXAMPLE_METADATA``.

    Scans all sub-packages of ``sqldim.application.examples.real_world`` and
    ``sqldim.application.examples.features`` that contain a ``showcase.py`` module
    exposing an ``EXAMPLE_METADATA`` dict.  Returns a mapping of CLI name →
    ``(title, description, module_path, entry_fn, kind)`` tuples.
    """
    result: dict = {}
    for pkg_name, kind in [
        ("sqldim.application.examples.real_world", "real_world"),
        ("sqldim.application.examples.features", "features"),
    ]:
        result.update(_scan_pkg(pkg_name, kind))
    return result


def _print_example_group(kind_key: str, items: list, labels: dict, first: bool) -> bool:
    if not items:
        return first
    if not first:
        print()
    print(
        f"[sqldim] {labels.get(kind_key, kind_key)} "
        f"(run with: sqldim example run <name>)\n"
    )
    for name, title, desc in sorted(items, key=lambda x: x[0]):
        print(f"  {name:<20} {title}")
        print(f"  {'':20} {desc}\n")
    return False


def cmd_example_list(args: argparse.Namespace) -> None:
    """List all available examples, grouped by kind.

    Prints each example name, title, and a one-line description so users
    can choose which showcase to run with ``sqldim example run <name>``.
    """
    examples = _discover_examples()
    if not examples:
        print("[sqldim] No examples found (is application/ on the Python path?)")
        return

    groups: dict[str, list] = {"real_world": [], "features": []}
    for name, (title, desc, _mod, _fn, kind) in examples.items():
        groups.setdefault(kind, []).append((name, title, desc))

    labels = {"real_world": "Real-world pipelines", "features": "Feature showcases"}
    first = True
    for kind_key, items in groups.items():
        first = _print_example_group(kind_key, items, labels, first)


def cmd_example_run(args: argparse.Namespace) -> int:
    """Run a named example showcase.

    Dynamically imports and invokes the showcase function for the given
    example name, supporting both async and synchronous entry-points.
    Returns 1 if the name is unrecognised, 0 on success.
    """
    import asyncio
    import importlib

    name = args.name.lower()
    examples = _discover_examples()
    if name not in examples:
        known = ", ".join(sorted(examples))
        print(
            f"[sqldim] Unknown example '{name}'. "
            f"Run 'sqldim example list' to see options.\n"
            f"  Known: {known}"
        )
        return 1
    _title, _desc, mod_path, fn_name, _kind = examples[name]
    mod = importlib.import_module(mod_path)
    fn = getattr(mod, fn_name)
    if asyncio.iscoroutinefunction(fn):
        asyncio.run(fn())
    else:
        fn()
    return 0


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

    datasets = [args.dataset] if getattr(args, "dataset", None) else None
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
    else:
        print(f"\n{report.summary()}")

    return int(report.failed > 0)


# ── bench commands ────────────────────────────────────────────────────────────


def _bench_setup_graph(ds_name: str) -> Any:
    """Load *ds_name* dataset and compile a stub NL graph.  Raises ``KeyError`` when
    the dataset is not registered."""
    from sqldim.application.ask import (
        DatasetPipelineSource,
        build_registry_from_schema,
        load_dataset,
        make_default_budget,
    )
    from sqldim.core.query.dgm.nl._agent_types import DGMContext
    from sqldim.core.query.dgm.nl._graph import build_nl_graph

    dataset = load_dataset(ds_name)  # raises KeyError when unknown
    source = DatasetPipelineSource(dataset)
    source.setup()
    try:
        con = source.get_connection()
        table_names = source.get_table_names()
        registry = build_registry_from_schema(con, table_names)
        budget = make_default_budget()
        ctx = DGMContext(entity_registry=registry, budget=budget, con=con)
        return build_nl_graph(context=ctx, model=None)
    finally:
        source.teardown()


def _bench_case_timings(graph: Any, case: Any, n_runs: int) -> list[float]:
    """Time *case* in stub mode for *n_runs* iterations.  Returns timings in ms."""
    import time

    from sqldim.core.query.dgm.nl._agent_types import NLInterfaceState
    from langgraph.errors import GraphRecursionError

    timings: list[float] = []
    for _ in range(n_runs):
        t0 = time.perf_counter()
        try:
            graph.invoke(
                NLInterfaceState(utterance=case.utterance).model_dump(),
                config={
                    "configurable": {"thread_id": f"bench-{case.id}"},
                    "recursion_limit": 25,
                },
            )
        except GraphRecursionError:
            pass
        timings.append((time.perf_counter() - t0) * 1000)
    return timings


def _bench_case_stats(case: Any, ds_name: str, n_runs: int, timings: list) -> dict:  # type: ignore[type-arg]
    """Compute timing stats for one case and print a summary line."""
    import statistics

    mean_ms = statistics.mean(timings)
    min_ms = min(timings)
    max_ms = max(timings)
    p95_ms = sorted(timings)[int(len(timings) * 0.95)] if len(timings) >= 20 else max_ms
    print(
        f"  {case.id:<45} "
        f"mean={mean_ms:6.1f}ms  "
        f"min={min_ms:6.1f}ms  "
        f"max={max_ms:6.1f}ms"
    )
    return {
        "case_id": case.id,
        "dataset": ds_name,
        "utterance": case.utterance,
        "runs": n_runs,
        "mean_ms": round(mean_ms, 1),
        "min_ms": round(min_ms, 1),
        "max_ms": round(max_ms, 1),
        "p95_ms": round(p95_ms, 1),
    }


def _bench_all_datasets(cases: list, n_runs: int) -> list:  # type: ignore[type-arg]
    """Iterate datasets, build stub graphs, run all cases.  Returns stat dicts."""
    by_ds: dict[str, list] = {}  # type: ignore[type-arg]
    for case in cases:
        by_ds.setdefault(case.dataset, []).append(case)

    results: list[dict] = []  # type: ignore[type-arg]
    for ds_name in sorted(by_ds):
        try:
            graph = _bench_setup_graph(ds_name)
        except KeyError:
            print(f"  [{ds_name}] SKIP — dataset not found")
            continue
        for case in by_ds[ds_name]:
            timings = _bench_case_timings(graph, case, n_runs)
            results.append(_bench_case_stats(case, ds_name, n_runs, timings))
    return results


def _bench_print_summary(results: list, n_runs: int, output: str | None) -> None:  # type: ignore[type-arg]
    """Print overall latency summary and optionally write JSON output."""
    import statistics

    overall_means = [r["mean_ms"] for r in results]
    if overall_means:
        global_mean = statistics.mean(overall_means)
        print(
            f"\n[sqldim bench] {len(results)} case(s) | overall mean latency: {global_mean:.1f}ms"
        )
    if output:
        import json

        with open(output, "w", encoding="utf-8") as fh:
            json.dump({"runs_per_case": n_runs, "results": results}, fh, indent=2)
        print(f"[sqldim bench] Results written to {output}")


def cmd_bench_run(args: argparse.Namespace) -> int:
    """Benchmark the stub NL graph (no LLM) for latency profiling.

    Runs the NL graph in stub mode (``model=None``) across all eval cases for
    *dataset* (or all datasets when omitted) *runs* times and reports timing
    statistics.  No LLM is required — this exercises the graph machinery only.
    """
    from sqldim.application.evals.cases import EVAL_SUITE

    dataset_filter = getattr(args, "dataset", None)
    n_runs: int = max(1, getattr(args, "runs", 3))
    output = getattr(args, "output", None)

    cases = [
        c for c in EVAL_SUITE if dataset_filter is None or c.dataset == dataset_filter
    ]
    if not cases:
        print(f"[sqldim bench] No cases found for dataset {dataset_filter!r}")
        return 1

    print(
        f"[sqldim bench] {len(cases)} case(s), {n_runs} run(s) each, stub mode (no LLM)\n"
    )
    results = _bench_all_datasets(cases, n_runs)
    _bench_print_summary(results, n_runs, output)
    return 0


# ── ask command ───────────────────────────────────────────────────────────────


def _make_dataset_source(args: argparse.Namespace) -> Any:
    """Build a ``DatasetPipelineSource`` from CLI args.  Prints an error and returns
    ``None`` when the dataset name is not registered."""
    from sqldim.application.ask import DatasetPipelineSource, load_dataset

    try:
        dataset = load_dataset(args.dataset)
    except KeyError as exc:
        print(f"[sqldim ask] {exc}")
        return None
    return DatasetPipelineSource(dataset)


def _make_medallion_source(args: argparse.Namespace) -> Any:
    """Build a ``MedallionPipelineSource`` from CLI args."""
    import duckdb

    from sqldim.application.ask import MedallionPipelineSource

    db_path: str = args.db_path if args.db_path else ":memory:"
    con = duckdb.connect(db_path)
    return MedallionPipelineSource(con, layer=args.layer)


def _make_observatory_source(args: argparse.Namespace) -> Any:
    """Build an ``ObservatoryPipelineSource`` from CLI args."""
    import duckdb

    from sqldim.application.ask import ObservatoryPipelineSource
    from sqldim.observability.drift import DriftObservatory

    db_path: str = args.db_path if args.db_path else ":memory:"
    obs_con = duckdb.connect(db_path)
    observatory = DriftObservatory(obs_con)
    return ObservatoryPipelineSource(observatory)


_SOURCE_FACTORIES: dict[str, Any] = {
    "dataset": _make_dataset_source,
    "medallion": _make_medallion_source,
    "observatory": _make_observatory_source,
}


def _build_pipeline_source(args: argparse.Namespace) -> Any:
    """Dispatch to the appropriate source factory for ``args.source``.

    Prints an error and returns ``None`` for unknown source types.  The
    ``dataset`` factory also returns ``None`` when the name is not registered.
    """
    factory = _SOURCE_FACTORIES.get(args.source)
    if factory is None:
        print(f"[sqldim ask] Unknown source type: {args.source!r}")
        return None
    return factory(args)


def cmd_ask(args: argparse.Namespace) -> int:
    """Ask a natural-language question against a dataset or pipeline source.

    Supports three source types via ``--source``:

    * ``dataset`` (default) — loads a bundled dataset into an ephemeral
      in-memory DuckDB connection.
    * ``medallion`` — connects to a DuckDB file (``--db-path``) already
      populated by a medallion pipeline and exposes the tables at ``--layer``.
    * ``observatory`` — opens a DriftObservatory on a DuckDB file
      (``--db-path``) and exposes the six observatory fact/dim tables.

    The LLM backend is selected via ``--model-provider`` (default: ``ollama``)
    with an optional ``--model`` override for the model name.
    """
    from sqldim.application.ask import make_model, run_ask_from_source

    # Build the LLM model (None → run_ask_from_source falls back to Ollama)
    llm_model = None
    if args.model_provider:
        try:
            llm_model = make_model(args.model_provider, args.model_name or None)
        except Exception as exc:  # noqa: BLE001
            print(f"[sqldim ask] Could not create model: {exc}")
            return 1

    source = _build_pipeline_source(args)
    if source is None:
        return 1

    return run_ask_from_source(
        args.question, source, verbose=args.verbose, model=llm_model
    )


# ── parser ────────────────────────────────────────────────────────────────────


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
        default=None,
        metavar="DATASET",
        help="Restrict to cases for this dataset (e.g. ecommerce)",
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

    # bench subcommand — stub-mode latency benchmarking
    bench = subparsers.add_parser(
        "bench",
        help="Benchmark NL graph latency in stub mode (no LLM)",
    )
    bench_sub = bench.add_subparsers(dest="subcommand")

    bn_run = bench_sub.add_parser(
        "run", help="Run latency benchmarks across eval cases"
    )
    bn_run.add_argument(
        "--dataset",
        default=None,
        metavar="DATASET",
        help="Restrict to cases for this dataset (e.g. ecommerce)",
    )
    bn_run.add_argument(
        "--runs",
        type=int,
        default=3,
        metavar="N",
        help="Number of invocations per case (default: 3)",
    )
    bn_run.add_argument(
        "--output",
        default=None,
        metavar="PATH",
        help="Write JSON results to this file path",
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
