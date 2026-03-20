"""DWSchema — dimension-first multi-model load orchestrator."""

from __future__ import annotations

from typing import Any, Dict


class DWSchema:
    """Orchestrate loading of dimension and fact models in the correct order.

    Dimension models are always processed before fact models, regardless of
    the order in which they are listed.  Only models that appear as keys in
    the *source_map* passed to :meth:`load` (or :meth:`aload`) are actually
    loaded — unregistered models are silently skipped.

    When to use ``DWSchema`` vs :class:`~sqldim.core.loaders.dimensional.DimensionalLoader`
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Use ``DWSchema`` when:

    * Your sources are DuckDB-native (file paths, SQL views, DuckDB relations).
    * You don't need FK → SK resolution (your DuckDB tables already carry the
      correct surrogate keys, or the loader derives them internally).
    * You want a minimal ``schema.load(source_map)`` API with no model
      registration step.

    Use :class:`~sqldim.core.loaders.dimensional.DimensionalLoader` when:

    * Your fact source is a ``list[Dict]`` of Python records (ORM-style).
    * You need FK → SK resolution across dimension/fact boundaries.
    * You want OpenLineage / custom lineage events emitted per-load.

    Parameters
    ----------
    models :
        List of :class:`~sqldim.core.kimball.models.DimensionModel` and / or
        :class:`~sqldim.core.kimball.models.FactModel` subclasses.
    sink :
        A DuckDB-compatible sink adapter, used for every fact loader.
    session :
        A SQLModel / SQLAlchemy ``Session`` — required when any dimension
        model appears in the source map that is passed to :meth:`load` /
        :meth:`aload`.

    Source map values
    -----------------
    Each value in the *source_map* dict is a **source spec**:

    * ``source`` — single positional arg to the loader's primary method.
    * ``(source, extra_arg)`` — two positional args (e.g. for
      ``LazyCumulativeLoader`` which wants ``(today_source, target_period)``).
    * ``(source, kwargs_dict)`` — ``kwargs_dict`` is forwarded as keyword
      arguments (e.g. ``snapshot_date`` for ``LazySnapshotLoader``).
    * ``(source, extra_arg, kwargs_dict)`` — both extra positional and keyword.

    For :class:`~sqldim.core.kimball.models.DimensionModel` entries the
    *source* must be a ``list[Dict]`` of records.

    Examples
    --------
    Load only facts (dimensions already loaded / not in map)::

        schema = DWSchema([CustomerDim, OrderFact], sink)
        results = schema.load({OrderFact: "orders.parquet"})

    Load both (session required for dimensions)::

        schema = DWSchema([CustomerDim, OrderFact], sink, session=session)
        results = schema.load({
            CustomerDim: dim_records,
            OrderFact:   "orders.parquet",
        })

    Async (preferred inside ``async def`` contexts)::

        results = await schema.aload({
            CustomerDim: dim_records,
            OrderFact:   "orders.parquet",
        })

    Snapshot with call-time date::

        results = schema.load({
            SnapshotFact: ("balances.parquet", {"snapshot_date": "2024-03-31"}),
        })
    """

    def __init__(self, models: list[type], sink, *, session=None) -> None:
        from sqldim.core.kimball.models import DimensionModel, FactModel

        self._dims: list[type] = [m for m in models if issubclass(m, DimensionModel)]
        self._facts: list[type] = [m for m in models if issubclass(m, FactModel)]
        self.sink = sink
        self.session = session

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, source_map: dict[type, Any]) -> dict[type, Any]:
        """Load each model present in *source_map*: dimensions first, facts second.

        Dimension entries require ``session`` to be set at construction.

        .. note::
            This method is **not safe** to call from within a running event loop
            (e.g. inside an ``async def`` function, a Jupyter notebook cell, or a
            FastAPI route handler).  Use ``await schema.aload(source_map)`` instead,
            and you will get a clear :class:`RuntimeError` here if you forget.

        Returns a dict mapping each loaded model class to its loader result.
        """
        import asyncio

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass  # no running loop — asyncio.run() is safe
        else:
            raise RuntimeError(
                "DWSchema.load() cannot be called from within a running event loop. "
                "Use 'await schema.aload(source_map)' instead."
            )

        results: dict[type, Any] = {}
        for model in self._dims + self._facts:
            if model not in source_map:
                continue
            source, args, kwargs = self._unpack(source_map[model])
            results[model] = self._run_sync(model, source, args, kwargs, asyncio)
        return results

    async def aload(self, source_map: dict[type, Any]) -> dict[type, Any]:
        """Async variant of :meth:`load`.

        Dimension loaders are awaited directly; fact loaders run via their
        ``aload()`` method (which uses ``asyncio.to_thread`` internally).

        Returns a dict mapping each loaded model class to its loader result.
        """
        results: dict[type, Any] = {}
        for model in self._dims + self._facts:
            if model not in source_map:
                continue
            source, args, kwargs = self._unpack(source_map[model])
            results[model] = await self._run_async(model, source, args, kwargs)
        return results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _unpack(spec: Any):
        """Decompose a source spec into ``(source, extra_args, extra_kwargs)``."""
        if not isinstance(spec, tuple):
            return spec, (), {}
        items = list(spec)
        kwargs: dict[str, Any] = (
            items.pop() if items and isinstance(items[-1], dict) else {}
        )
        source = items[0] if items else None
        extra = tuple(items[1:])
        return source, extra, kwargs

    def _require_session(self, model_name: str) -> None:
        if self.session is None:
            raise TypeError(
                f"DWSchema: loading dimension '{model_name}' requires "
                "'session' to be provided at DWSchema construction."
            )

    def _run_sync(
        self, model: type, source: Any, args: tuple, kwargs: Dict, asyncio_mod
    ) -> Any:
        from sqldim.core.kimball.models import DimensionModel

        if issubclass(model, DimensionModel):
            self._require_session(model.__name__)
            handler = model.as_loader(self.session)
            records = source if isinstance(source, list) else [source]
            return asyncio_mod.run(handler.process(records))

        loader = model.as_loader(self.sink)
        # Route all fact loaders through aload() — unified entry point.
        # Transaction/snapshot loaders pick up _model_cls as the table;
        # model-bound loaders wrap process() via asyncio.to_thread.
        return asyncio_mod.run(loader.aload(source, *args, **kwargs))

    async def _run_async(
        self, model: type, source: Any, args: tuple, kwargs: Dict
    ) -> Any:
        from sqldim.core.kimball.models import DimensionModel

        if issubclass(model, DimensionModel):
            self._require_session(model.__name__)
            handler = model.as_loader(self.session)
            records = source if isinstance(source, list) else [source]
            return await handler.aload(records)

        loader = model.as_loader(self.sink)
        return await loader.aload(source, *args, **kwargs)
