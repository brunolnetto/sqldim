"""CLI ask command — ``sqldim ask``."""

from __future__ import annotations

import argparse
from typing import Any


def _make_dataset_source(args: argparse.Namespace) -> Any:
    """Open the bundled dataset named by ``--dataset`` through the local host.

    Prints the refusal and returns ``None`` when the dataset is not granted —
    the local host grants exactly the bundled datasets (§12.10, D44).
    """
    from sqldim.application.supervisor_host import open_local_dataset
    from sqldim.core.query.dgm.nl.supervisor._auth import AccessDenied

    try:
        return open_local_dataset(args.dataset)
    except AccessDenied as exc:
        print(f"[sqldim ask] UNAUTHORIZED: {exc} ({args.dataset!r})")
        return None


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

    When ``--mode supervisor`` is given the question is decomposed across
    multiple datasets using the supervisor agent instead.

    The LLM backend is selected via ``--model-provider`` (default: ``ollama``)
    with an optional ``--model`` override for the model name.
    """
    mode: str = getattr(args, "mode", "ask")

    if mode == "supervisor":
        return _run_supervisor(args)

    # Single-dataset mode: --dataset is required
    if not getattr(args, "dataset", None):
        print(
            "[sqldim ask] --dataset is required in ask mode (or use --mode supervisor)"
        )
        return 1

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


def _format_hop(hop: Any) -> str:
    """Render one tool result as ``[call_id] tool STATUS details``."""
    status = hop.status.value
    mark = "\u2713" if status == "SUCCEEDED" else "\u2717"
    head = f"[{hop.call_id}] {hop.tool_name} {mark} {status}"
    if hop.demo:
        head += " (demo)"
    if hop.error is not None:
        return f"{head} {hop.error.kind.value}: {hop.error.message}"
    return head


def _resolve_datasets(
    selected: list[str] | None, all_datasets: list[str], prefix: str
) -> list[str] | None:
    """Validate an explicit dataset selection; ``None`` means unknown names."""
    if not selected:
        return all_datasets
    unknown = [d for d in selected if d not in all_datasets]
    if unknown:
        print(f"[{prefix}] Unknown dataset(s): {', '.join(unknown)}")
        print(f"[{prefix}] Available: {', '.join(all_datasets)}")
        return None
    return sorted(selected)


def _build_llm_model(
    model_provider: str | None, model_name: str | None, prefix: str
) -> Any:
    """Build the supervisor LLM model, warning (not failing) on errors."""
    from sqldim.core.query.dgm.nl._agents import make_model

    if not model_provider:
        return None
    try:
        return make_model(provider=model_provider, model_name=model_name)
    except Exception as exc:
        print(f"[{prefix}] Warning: could not build LLM model: {exc}")
        return None


def _invoke_supervisor(
    available: list[str], llm_model: Any, question: str, *, demo: bool = False
) -> Any:
    """Run the supervisor once on *question* for the local principal."""
    from sqldim.application.supervisor_host import local_supervisor_runtime
    from sqldim.core.query.dgm.nl.supervisor._graph import run_supervisor

    runtime = local_supervisor_runtime(available, model=llm_model, demo=demo)
    return run_supervisor(question, runtime, thread_id="cli-supervisor")


def _print_verbose(result: Any, prefix: str) -> None:
    """Print visited nodes, timings and per-tool hops of a supervisor run."""
    print(f"\n[{prefix}] Nodes visited: {result.visited_nodes}")
    for node, ms in result.node_timings.items():
        print(f"  {node}: {ms}ms")
    for hop in result.results.values():
        print(f"  {_format_hop(hop)}")


def _run_supervisor(args: argparse.Namespace) -> int:
    """Run the supervisor agent across multiple datasets."""
    from sqldim.application.ask import KNOWN_DATASETS

    prefix = "sqldim ask"
    all_datasets = sorted(KNOWN_DATASETS.keys())
    available = _resolve_datasets(getattr(args, "datasets", None), all_datasets, prefix)
    if available is None:
        return 1

    model_provider: str | None = getattr(args, "model_provider", None)
    model_name: str | None = getattr(args, "model_name", None) or getattr(
        args, "model", None
    )
    llm_model = _build_llm_model(model_provider, model_name, prefix)

    result = _invoke_supervisor(
        available, llm_model, args.question, demo=bool(getattr(args, "demo", False))
    )
    print(result.synthesis or f"Outcome: {result.outcome}")

    if getattr(args, "verbose", False):
        _print_verbose(result, prefix)

    return _outcome_exit_code(result)


def _outcome_exit_code(result: Any) -> int:
    """0 only for COMPLETE; a partial or failed answer is not a success."""
    outcome = result.outcome.value if result.outcome is not None else "FAILED"
    return 0 if outcome == "COMPLETE" else 1
