"""CLI ask command — ``sqldim ask``."""

from __future__ import annotations

import argparse
from typing import Any


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


def _run_supervisor(args: argparse.Namespace) -> int:
    """Run the supervisor agent across multiple datasets."""
    from sqldim.application.ask import KNOWN_DATASETS
    from sqldim.core.query.dgm.nl._agents import make_model
    from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph
    from sqldim.core.query.dgm.nl.supervisor._types import (
        SupervisorContext,
        SupervisorState,
    )

    all_datasets = sorted(KNOWN_DATASETS.keys())
    selected: list[str] | None = getattr(args, "datasets", None)
    if selected:
        unknown = [d for d in selected if d not in all_datasets]
        if unknown:
            print(f"[sqldim ask] Unknown dataset(s): {', '.join(unknown)}")
            print(f"[sqldim ask] Available: {', '.join(all_datasets)}")
            return 1
        available = sorted(selected)
    else:
        available = all_datasets

    model_provider: str | None = getattr(args, "model_provider", None)
    model_name: str | None = getattr(args, "model_name", None) or getattr(
        args, "model", None
    )

    llm_model = None
    if model_provider:
        try:
            llm_model = make_model(provider=model_provider, model_name=model_name)
        except Exception as exc:
            print(f"[sqldim ask] Warning: could not build LLM model: {exc}")

    context = SupervisorContext(
        available_datasets=available,
        model=llm_model,
    )

    graph = build_supervisor_graph(context=context)
    initial = SupervisorState(utterance=args.question)

    result = graph.invoke(
        initial.model_dump(),
        config={
            "configurable": {"thread_id": "cli-supervisor"},
            "recursion_limit": 25,
        },
    )

    synthesis = result.get("synthesis")
    if synthesis:
        print(synthesis)
    else:
        print("[sqldim ask] No synthesis produced.")

    verbose: bool = getattr(args, "verbose", False)
    if verbose:
        visited = result.get("visited_nodes", [])
        timings = result.get("node_timings", {})
        print(f"\n[sqldim ask] Nodes visited: {visited}")
        for node, ms in timings.items():
            print(f"  {node}: {ms}ms")
        for hop in result.get("results", []):
            _name = (
                hop.get("tool_name")
                if isinstance(hop, dict)
                else getattr(hop, "tool_name", "?")
            )
            ok = (
                hop.get("success")
                if isinstance(hop, dict)
                else getattr(hop, "success", False)
            )
            data = (
                (hop.get("data") or {})
                if isinstance(hop, dict)
                else (getattr(hop, "data") or {})
            )
            err = (
                hop.get("error")
                if isinstance(hop, dict)
                else getattr(hop, "error", None)
            )
            status = "\u2713" if ok else "\u2717"
            if err:
                print(f"  [{_name}] {status} ERROR: {err}")
            else:
                display = {
                    k: v
                    for k, v in (data if isinstance(data, dict) else {}).items()
                    if k != "stub" and v is not None
                }
                print(f"  [{_name}] {status} {display}")

    return 0
