"""DGM NL interface CLI integration — ``sqldim ask`` backend (§11.10).

Integration points
------------------
Three integration strategies are supported, selected via :class:`PipelineSource`:

``DatasetPipelineSource``
    The original direct-to-dataset path.  Loads a :class:`Dataset` into an
    ephemeral in-memory DuckDB connection and introspects the OLTP schema.

``MedallionPipelineSource``
    Connects to a pre-existing medallion pipeline DuckDB connection (file-backed
    or in-memory) and targets a specific layer (``bronze``/``silver``/``gold``).
    All tables present at that layer are exposed as the NL vocabulary.  If a
    :class:`~sqldim.medallion.MedallionRegistry` is provided, table discovery
    is restricted to datasets registered at the requested layer.

``ObservatoryPipelineSource``
    Connects to a :class:`~sqldim.observability.DriftObservatory` instance and
    exposes its six-table Kimball star schema (schema-evolution + quality-drift
    facts) as the NL vocabulary.  Allows natural-language queries like
    "which datasets have the highest breaking-change rate?" to run directly
    against pipeline observability data.

All sources share the same :func:`run_ask_from_source` flow:
    1. ``source.setup()`` — connect / load data
    2. ``source.get_table_names()`` → table list
    3. ``build_registry_from_schema(source.get_connection(), tables)``
    4. ``DGMContext`` + ``build_nl_graph`` + ``invoke`` as before
    5. ``source.teardown()`` — cleanup
"""

from __future__ import annotations

import importlib
from datetime import timedelta
from typing import Any

import duckdb
from langgraph.errors import GraphRecursionError

from sqldim.application._pipeline_sources import (
    DatasetPipelineSource,
    MedallionPipelineSource,
    ObservatoryPipelineSource,
    PipelineSource,
)
from sqldim.application.datasets.dataset import Dataset
from sqldim.core.query.dgm.nl._agent_types import DGMContext, NLInterfaceState
from sqldim.core.query.dgm.nl._agents import make_model
from sqldim.core.query.dgm.nl._graph import NLPipelineUnavailable, build_nl_graph
from sqldim.core.query.dgm.nl._types import EntityRegistry
from sqldim.core.query.dgm.nl.supervisor._auth import AccessDenied, DataAccess
from sqldim.core.query.dgm.planner._gate import ExecutionBudget
from sqldim.core.query.dgm.planner._targets import CostEstimate

__all__ = [
    "PipelineSource",
    "DatasetPipelineSource",
    "MedallionPipelineSource",
    "ObservatoryPipelineSource",
    "KNOWN_DATASETS",
    "load_dataset",
    "build_registry_from_schema",
    "make_default_budget",
    "make_model",
    "run_ask_from_source",
    "run_ask",
]


# ---------------------------------------------------------------------------
# Dataset discovery — scans domains subpackages for DATASET_METADATA
# ---------------------------------------------------------------------------

_DOMAINS_PKG = "sqldim.application.datasets.domains"


def _load_one_dataset(
    pkg_name: str, modname: str
) -> tuple[str, tuple[str, str]] | None:
    """Try to load DATASET_METADATA from ``{pkg_name}.{modname}.dataset``.

    Returns ``(name, (module_path, attr))`` on success, ``None`` otherwise.
    """
    dataset_path = f"{pkg_name}.{modname}.dataset"
    try:
        mod = importlib.import_module(dataset_path)
    except ImportError:
        return None
    meta = getattr(mod, "DATASET_METADATA", None)
    if not isinstance(meta, dict):
        return None
    name: str = meta.get("name", modname)
    attr: str = meta.get("dataset_attr", "")
    if not attr:
        return None
    return (name, (dataset_path, attr))


def _discover_datasets() -> dict[str, tuple[str, str]]:
    """Scan ``sqldim.application.datasets.domains.*`` for DATASET_METADATA.

    Returns a dict mapping CLI name → ``(module_path, attribute_name)``,
    the same structure as the legacy static :data:`KNOWN_DATASETS` dict.
    Subpackages without a ``dataset.py`` or ``DATASET_METADATA`` are silently
    skipped — no error is raised.
    """
    import pkgutil

    result: dict[str, tuple[str, str]] = {}
    try:
        pkg = importlib.import_module(_DOMAINS_PKG)
    except ImportError:
        return result

    for _, modname, ispkg in pkgutil.iter_modules(pkg.__path__):
        if not ispkg:
            continue
        loaded = _load_one_dataset(_DOMAINS_PKG, modname)
        if loaded is not None:
            name, entry = loaded
            result[name] = entry

    return result


KNOWN_DATASETS: dict[str, tuple[str, str]] = _discover_datasets()


# ---------------------------------------------------------------------------
# load_dataset
# ---------------------------------------------------------------------------


def load_dataset(name: str) -> Dataset:
    """Return the :class:`Dataset` singleton for *name*.

    Parameters
    ----------
    name:
        Key from :data:`KNOWN_DATASETS` (e.g. ``"ecommerce"``).

    Raises
    ------
    KeyError
        When *name* is not registered in :data:`KNOWN_DATASETS`.
    """
    if name not in KNOWN_DATASETS:
        raise KeyError(
            f"Unknown dataset '{name}'. Known datasets: {sorted(KNOWN_DATASETS)}"
        )
    mod_path, attr = KNOWN_DATASETS[name]
    mod = importlib.import_module(mod_path)
    return getattr(mod, attr)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# build_registry_from_schema
# ---------------------------------------------------------------------------


def build_registry_from_schema(
    con: duckdb.DuckDBPyConnection,
    table_names: list[str],
) -> EntityRegistry:
    """Populate an :class:`EntityRegistry` from *table_names* in *con*.

    Each table name is registered as a node term (alias = table name).
    Each column in each table is registered as a prop term with a qualified
    propref ``"<table>.<column>"``, preventing cross-table name collisions.

    The table names originate from the Dataset's internal configuration —
    they are not user-supplied input, so DESCRIBE formatting is safe.

    Parameters
    ----------
    con:
        Open DuckDB connection containing the tables to introspect.
    table_names:
        Ordered list of table names (as returned by ``Dataset.table_names()``).
    """
    registry = EntityRegistry()
    for table in table_names:
        registry.register_node(table, table)
        rows: list[Any] = con.execute(f"DESCRIBE {table}").fetchall()
        for row in rows:
            col_name: str = row[0]
            registry.register_prop(col_name, f"{table}.{col_name}")
    return registry


# ---------------------------------------------------------------------------
# make_default_budget
# ---------------------------------------------------------------------------


def make_default_budget() -> ExecutionBudget:
    """Return a generous :class:`ExecutionBudget` for interactive CLI use."""
    ceiling = CostEstimate(cpu_ops=10_000_000, io_ops=1_000_000)
    stream_threshold = CostEstimate(cpu_ops=1_000_000, io_ops=100_000)
    async_threshold = CostEstimate(cpu_ops=5_000_000, io_ops=500_000)
    return ExecutionBudget(
        max_estimated_cost=ceiling,
        max_result_rows=100_000,
        max_wall_time=timedelta(seconds=30),
        max_precompute_time=timedelta(seconds=10),
        streaming_threshold=stream_threshold,
        async_threshold=async_threshold,
    )


# ---------------------------------------------------------------------------
# run_ask_from_source — generic NL-graph driver for any PipelineSource
# ---------------------------------------------------------------------------


def _print_ask_verbose(
    source: "PipelineSource",
    table_names: list,  # type: ignore[type-arg]
    registry: Any,
    model: Any,
) -> None:
    """Print verbose pre-run information to stdout."""
    print(f"[sqldim ask] Source   : {source.label}")
    print(f"[sqldim ask] Tables   : {table_names}")
    print(f"[sqldim ask] Nodes    : {sorted(registry.node_terms)}")
    print(f"[sqldim ask] PropTerms: {len(registry.prop_terms)} registered")
    print(f"[sqldim ask] Model    : {model._model_name if model else 'stub'}")


def _print_rows(rows: list, count: int) -> None:  # type: ignore[type-arg]
    """Print up to five result rows with a trailing count hint."""
    for row in rows[:5]:
        print(f"  {row}")
    if count > 5:
        print(f"  ... ({count - 5} more rows)")


def _print_query_result_block(query_result: Any) -> None:
    """Print query result rows when *query_result* is a non-empty dict."""
    if not (query_result and isinstance(query_result, dict)):
        return
    cols = query_result.get("columns", [])
    count = query_result.get("count", 0)
    print(f"[sqldim ask] Query returned {count} row(s).")
    if cols:
        print(f"[sqldim ask] Columns: {', '.join(cols)}")
    _print_rows(query_result.get("rows", []), count)


def _print_ask_result(
    utterance: str,
    registry: Any,
    table_names: list,  # type: ignore[type-arg]
    model: Any,
    explanation: str | None,
    query_result: Any,
) -> None:
    """Print the graph result to stdout."""
    if explanation:
        print(explanation)
    else:
        print(f"[sqldim ask] Utterance '{utterance}' processed.")
        print(
            f"[sqldim ask] Vocabulary: {len(registry.prop_terms)} terms "
            f"across {len(table_names)} tables."
        )
        if model is None:
            print("[sqldim ask] (no model given — LLM nodes are demo stubs.)")
    _print_query_result_block(query_result)


def run_ask_from_source(
    utterance: str,
    source: PipelineSource,
    *,
    verbose: bool = False,
    model: Any = None,
    demo: bool = False,
) -> int:
    """Process *utterance* against an already-open :class:`PipelineSource`.

    It opens nothing by identifier: the source is the authority, and
    authorization belongs to whoever opened it (D44, §12.10).  Hosts open
    sources through :class:`DataAccess` — see :func:`run_ask`.

    Fail closed (ADR 0012, D47).  The NL graph available today is the
    heuristic demo path, so without ``demo=True`` nothing runs and
    ``NOT_IMPLEMENTED`` is reported with what is missing.  No model is
    chosen on the caller's behalf: ``model=None`` means the LLM nodes are
    demo stubs, never "try Ollama".

    Returns
    -------
    int
        ``0`` when the turn produced a result, ``1`` when it failed, ``2``
        when the pipeline is not available.
    """
    if not demo:
        print(f"[sqldim ask] NOT_IMPLEMENTED: {NLPipelineUnavailable()}")
        print("[sqldim ask] Pass --demo to run the heuristic demonstration.")
        return 2
    print("[sqldim ask] [demo mode — heuristic output, not a contract result (D47)]")
    source.setup()
    try:
        con = source.get_connection()
        table_names = source.get_table_names()
        registry = build_registry_from_schema(con, table_names)
        ctx = DGMContext(
            entity_registry=registry, budget=make_default_budget(), con=con
        )
        if verbose:
            _print_ask_verbose(source, table_names, registry, model)
        result = _invoke_demo_graph(utterance, ctx, model)
        if result is None:
            print("[sqldim ask] TOOL_FAILURE: the NL graph did not converge")
            return 1
        error = result.get("error")
        if error:
            print(f"[sqldim ask] {error['kind']}: {error['message']}")
            return 1
        _print_ask_result(
            utterance,
            registry,
            table_names,
            model,
            result.get("explanation"),
            result.get("result"),
        )
        return 0
    finally:
        source.teardown()


def _invoke_demo_graph(utterance: str, ctx: Any, model: Any) -> dict[str, Any] | None:
    """One turn of the demo graph; ``None`` when it does not converge."""
    graph = build_nl_graph(context=ctx, model=model, demo=True)
    try:
        out: dict[str, Any] = graph.invoke(
            NLInterfaceState(utterance=utterance).model_dump(),
            config={"configurable": {"thread_id": "cli-ask"}, "recursion_limit": 25},
        )
    except GraphRecursionError:
        return None
    return out


# ---------------------------------------------------------------------------
# run_ask — top-level CLI pipeline (delegates to run_ask_from_source)
# ---------------------------------------------------------------------------


def run_ask(
    utterance: str,
    dataset_name: str,
    *,
    data: DataAccess | None,
    verbose: bool = False,
    model: Any = None,
    demo: bool = False,
) -> int:
    """Open *dataset_name* through *data*, build the NL graph, and answer.

    This is the NL interface of §11 used directly, so it is an entry point
    under D44 (§12.10): the host supplies a :class:`DataAccess` bound to the
    caller's :class:`AuthContext`.  Without one, or without a ``READ`` grant
    on the dataset, nothing is opened and the call is refused with the same
    message whether or not the dataset exists.

    Parameters
    ----------
    utterance:
        Natural-language question from the user.
    dataset_name:
        Identifier of the dataset to open through *data*.
    data:
        Authorized data access from the host (required; there is no default
        context).
    verbose:
        When ``True`` print entity vocabulary details before invoking the graph.
    model:
        A pre-built pydantic-ai model instance (e.g. from :func:`make_model`).

    demo:
        Explicit selection of the heuristic path (D47); see
        :func:`run_ask_from_source`.

    Returns
    -------
    int
        ``1`` when the call is not authorized; otherwise the code of
        :func:`run_ask_from_source`.
    """
    if data is None:
        print("[sqldim ask] UNAUTHORIZED: no authorization context")
        return 1
    try:
        source = data.scoped([dataset_name]).open(dataset_name)
    except AccessDenied as exc:
        print(f"[sqldim ask] UNAUTHORIZED: {exc}")
        return 1
    return run_ask_from_source(
        utterance, source, verbose=verbose, model=model, demo=demo
    )
