"""Loader factory — builds the right Lazy loader from a FactModel class.

Used by :meth:`FactModel.as_loader` and available for direct use.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from sqldim.core.loaders._utils import _infer_loader_params

if TYPE_CHECKING:
    pass


def _cv(
    model_cls: type,
    attr: str,
    inferred: dict[str, Any],
    infer_key: str | None = None,
) -> Any:
    """Return ClassVar value from *model_cls*, falling back to *inferred* dict."""
    val = getattr(model_cls, attr, None)
    if not val and infer_key:
        val = inferred.get(infer_key)
    return val


def _require_runtime(
    model_cls: type, strategy: str, runtime_kwargs: dict[str, Any], key: str
) -> Any:
    """Return *key* from *runtime_kwargs* or raise :exc:`TypeError`."""
    if key not in runtime_kwargs:
        raise TypeError(
            f"{model_cls.__name__}.as_loader(): strategy '{strategy}' requires "
            f"'{key}' to be passed as a keyword argument."
        )
    return runtime_kwargs[key]


# ---------------------------------------------------------------------------
# Per-strategy builder functions
# ---------------------------------------------------------------------------


def _build_bulk_loader(
    model_cls: type, sink: Any, inferred: dict[str, Any], runtime_kwargs: dict[str, Any]
) -> Any:
    from sqldim.core.loaders.snapshot import LazyTransactionLoader

    loader = LazyTransactionLoader(sink)
    loader._model_cls = model_cls
    return loader


def _build_snapshot_loader(
    model_cls: type, sink: Any, inferred: dict[str, Any], runtime_kwargs: dict[str, Any]
) -> Any:
    from sqldim.core.loaders.snapshot import LazySnapshotLoader

    loader = LazySnapshotLoader(
        sink,
        snapshot_date=runtime_kwargs.get("snapshot_date"),
        date_field=getattr(model_cls, "__snapshot_date_field__", "snapshot_date"),
    )
    loader._model_cls = model_cls
    return loader


def _build_accumulating_loader(
    model_cls: type, sink: Any, inferred: dict[str, Any], runtime_kwargs: dict[str, Any]
) -> Any:
    from sqldim.core.loaders.accumulating import LazyAccumulatingLoader

    match_col = _cv(model_cls, "__match_column__", inferred, "match_column")
    milestones = _cv(model_cls, "__milestones__", inferred, "milestones") or []
    if not match_col:
        raise TypeError(
            f"{model_cls.__name__}.as_loader(): strategy 'accumulating' requires "
            "'__match_column__' or '__natural_key__' to derive the match column."
        )
    loader = LazyAccumulatingLoader(model_cls, match_col, milestones, sink)
    loader._model_cls = model_cls
    return loader


def _build_cumulative_loader(
    model_cls: type, sink: Any, inferred: dict[str, Any], runtime_kwargs: dict[str, Any]
) -> Any:
    from sqldim.core.loaders.cumulative import LazyCumulativeLoader

    partition_key = _cv(model_cls, "__partition_key__", inferred, "partition_key")
    cumulative_col = _cv(
        model_cls, "__cumulative_column__", inferred, "cumulative_column"
    )
    metric_cols = _cv(model_cls, "__metric_columns__", inferred, "metric_columns") or []
    current_period_col = getattr(
        model_cls, "__current_period_column__", "current_season"
    )
    if not partition_key or not cumulative_col:
        raise TypeError(
            f"{model_cls.__name__}.as_loader(): strategy 'cumulative' requires "
            "'__partition_key__' and '__cumulative_column__' (or inferrable from fields)."
        )
    loader = LazyCumulativeLoader(
        model_cls,
        partition_key,
        cumulative_col,
        metric_cols,
        sink,
        current_period_column=current_period_col,
    )
    loader._model_cls = model_cls
    return loader


def _build_bitmask_loader(
    model_cls: type, sink: Any, inferred: dict[str, Any], runtime_kwargs: dict[str, Any]
) -> Any:
    from sqldim.core.loaders.bitmask import LazyBitmaskLoader

    partition_key = _cv(model_cls, "__partition_key__", inferred, "partition_key")
    dates_col = _cv(model_cls, "__dates_column__", inferred, "dates_column")
    window_days = getattr(model_cls, "__window_days__", 32)
    if not partition_key or not dates_col:
        raise TypeError(
            f"{model_cls.__name__}.as_loader(): strategy 'bitmask' requires "
            "'__partition_key__' and '__dates_column__' (or inferrable from fields)."
        )
    loader = LazyBitmaskLoader(
        model_cls,
        partition_key,
        dates_col,
        reference_date=_require_runtime(
            model_cls, "bitmask", runtime_kwargs, "reference_date"
        ),
        sink=sink,
        window_days=window_days,
    )
    loader._model_cls = model_cls
    return loader


def _build_array_metric_loader(
    model_cls: type, sink: Any, inferred: dict[str, Any], runtime_kwargs: dict[str, Any]
) -> Any:
    from sqldim.core.loaders.array_metric import LazyArrayMetricLoader

    partition_key = _cv(model_cls, "__partition_key__", inferred, "partition_key")
    value_col = _cv(model_cls, "__value_column__", inferred, "value_column")
    metric_name = (
        _cv(model_cls, "__metric_name__", inferred, "metric_name") or value_col
    )
    if not partition_key or not value_col:
        raise TypeError(
            f"{model_cls.__name__}.as_loader(): strategy 'array_metric' requires "
            "'__partition_key__' and '__value_column__' (or inferrable from fields)."
        )
    loader = LazyArrayMetricLoader(
        model_cls,
        partition_key,
        value_col,
        metric_name,
        month_start=_require_runtime(
            model_cls, "array_metric", runtime_kwargs, "month_start"
        ),
        sink=sink,
    )
    loader._model_cls = model_cls
    return loader


def _build_edge_projection_loader(
    model_cls: type, sink: Any, inferred: dict[str, Any], runtime_kwargs: dict[str, Any]
) -> Any:
    from sqldim.core.loaders.edge_projection import LazyEdgeProjectionLoader

    subject_key = getattr(model_cls, "__subject_key__", None)
    object_key = getattr(model_cls, "__object_key__", None)
    if not subject_key or not object_key:
        raise TypeError(
            f"{model_cls.__name__}.as_loader(): strategy 'edge_projection' requires "
            "explicit '__subject_key__' and '__object_key__' ClassVars (not inferrable)."
        )
    loader = LazyEdgeProjectionLoader(
        model_cls,
        subject_key,
        object_key,
        sink,
        property_map=getattr(model_cls, "__property_map__", None),
        self_join=getattr(model_cls, "__self_join__", False),
        self_join_key=getattr(model_cls, "__self_join_key__", None),
    )
    loader._model_cls = model_cls
    return loader


_STRATEGY_BUILDERS: dict[str | None, Any] = {
    None: _build_bulk_loader,
    "bulk": _build_bulk_loader,
    "snapshot": _build_snapshot_loader,
    "accumulating": _build_accumulating_loader,
    "cumulative": _build_cumulative_loader,
    "bitmask": _build_bitmask_loader,
    "array_metric": _build_array_metric_loader,
    "edge_projection": _build_edge_projection_loader,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_lazy_loader(model_cls: type, sink, **runtime_kwargs: Any):
    """Instantiate and return the correct lazy loader for *model_cls*.

    The loader type is selected from ``model_cls.__strategy__``.  Required
    constructor parameters are read from strategy-specific ClassVars on the
    model, with field-type inference used as a fallback for any that are
    ``None`` or empty.

    Runtime-bound values that cannot live on the model class
    (``snapshot_date``, ``reference_date``, ``month_start``) are passed as
    keyword arguments to this function.

    Parameters
    ----------
    model_cls:
        A :class:`~sqldim.core.kimball.models.FactModel` subclass.
    sink:
        A DuckDB-compatible sink adapter.
    **runtime_kwargs:
        ``snapshot_date``, ``reference_date``, or ``month_start`` depending
        on the strategy.

    Returns
    -------
    An instantiated Lazy loader ready to call ``.load()`` or ``.process()``.

    Raises
    ------
    TypeError
        If the strategy is unknown, if a required runtime kwarg is missing,
        or if the strategy cannot be used with a lazy loader (``"upsert"`` /
        ``"merge"``).
    """
    strategy: str | None = getattr(model_cls, "__strategy__", None)
    if strategy in ("upsert", "merge"):
        raise TypeError(
            f"{model_cls.__name__}.as_loader(): strategy '{strategy}' is session-based "
            "and has no DuckDB lazy equivalent. Use DimensionalLoader with a SQLModel "
            "Session for this strategy."
        )

    builder = _STRATEGY_BUILDERS.get(strategy)
    if builder is None:
        raise TypeError(
            f"{model_cls.__name__}.as_loader(): unknown strategy '{strategy}'. "
            "Expected one of: bulk, snapshot, accumulating, cumulative, bitmask, "
            "array_metric, edge_projection."
        )

    return builder(model_cls, sink, _infer_loader_params(model_cls), runtime_kwargs)
