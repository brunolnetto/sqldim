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
