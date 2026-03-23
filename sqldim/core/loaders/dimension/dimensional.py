"""Dimensional loader: ordered dimension-first loading with SK resolution.

Provides :class:`SKResolver` (cached surrogate-key lookup) and
:class:`DimensionalLoader` (register models, resolve FKs, then load).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from sqlmodel import Session, select
from sqldim.core.kimball.schema_graph import SchemaGraph
from sqldim.core.kimball.models import DimensionModel, FactModel
from sqldim.core.kimball.dimensions.scd.handler import SCDHandler

if TYPE_CHECKING:
    from sqldim.lineage.emitter import LineageEmitter


class SKResolver:
    """Lightweight SK resolver with an in-process LRU-style cache.

    Queries ``SELECT id FROM <dimension> WHERE <nk> = ? AND is_current = TRUE``
    and caches the result to avoid redundant DB round-trips within a single
    loader run.
    """

    def __init__(self, session: Session):
        self.session = session
        self._cache: dict[tuple[type[DimensionModel], str, object], object] = {}

    def resolve(
        self, model: type[DimensionModel], natural_key_name: str, value: object
    ) -> object:
        """Resolves a natural key value to a surrogate key (id)."""
        cache_key = (model, natural_key_name, value)
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Query the current version of the dimension row
        stmt = select(model.id).where(
            getattr(model, natural_key_name) == value, getattr(model, "is_current")
        )
        sk = self.session.exec(stmt).first()

        if sk is not None:
            self._cache[cache_key] = sk
        return sk

    def bulk_resolve(
        self,
        model: type[DimensionModel],
        natural_key_name: str,
        values: list[object],
    ) -> None:
        """Pre-warm the SK cache for *values* using a single IN query.

        Replaces N individual :meth:`resolve` round-trips with one
        ``SELECT id, <nk> FROM <dim> WHERE <nk> IN (…) AND is_current``
        call.  Call this once before iterating over a fact batch to avoid
        per-record DB overhead.
        """
        uncached = [
            v for v in set(filter(lambda x: x is not None, values))
            if (model, natural_key_name, v) not in self._cache
        ]
        if not uncached:
            return
        nk_col = getattr(model, natural_key_name)
        stmt = (
            select(model.id, nk_col)
            .where(nk_col.in_(uncached), getattr(model, "is_current"))
        )
        for sk, nk_val in self.session.exec(stmt).all():
            self._cache[(model, natural_key_name, nk_val)] = sk


class DimensionalLoader:
    """Orchestrates a dimension-first load across multiple models.

    Register each model with :meth:`register`, then call :meth:`run` to
    load dimensions first (via :class:`~sqldim.scd.handler.SCDHandler`) and
    facts second (after resolving all FK natural keys to surrogate keys).

    When to use ``DimensionalLoader`` vs :class:`~sqldim.core.kimball.schema.DWSchema`
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Use ``DimensionalLoader`` when:

    * Your fact source is a ``List[Dict]`` of Python records (ORM-style).
    * You need surrogate-key (FK → SK) resolution across dimension/fact boundaries.
    * You want OpenLineage / custom lineage events emitted per load.

    Use :class:`~sqldim.core.kimball.schema.DWSchema` when:

    * Your sources are DuckDB-native (file paths, views, DuckDB tables).
    * You don't need FK → SK resolution (DuckDB handles JOINs directly).
    * You want a simpler, single-call ``schema.load(source_map)`` API.

    Parameters
    ----------
    session:
        Active SQLModel session.
    models:
        All dimension and fact model classes that form the star schema.
    lineage_emitter:
        Optional :class:`~sqldim.lineage.LineageEmitter`.  When provided,
        ``START`` / ``COMPLETE`` / ``FAIL`` lineage events are emitted for
        the overall load and for each individual model.  Pass ``None``
        (default) to skip lineage tracking.
    """

    def __init__(
        self,
        session: Session,
        models: list[type],
        *,
        lineage_emitter: LineageEmitter | None = None,
    ) -> None:
        self.session = session
        self.graph = SchemaGraph.from_models(models)
        self._registry: dict[
            type,
            tuple[list[dict[str, object]], dict[str, tuple[type[DimensionModel], str]]],
        ] = {}
        self.resolver = SKResolver(session)
        self._lineage_emitter = lineage_emitter

    def register(
        self,
        model: type,
        source: list[dict[str, object]],
        key_map: dict[str, tuple[type[DimensionModel], str]] | None = None,
    ):
        """
        Register a model and its source data for loading.
        key_map: For facts, maps {fact_fk_column: (DimensionModel, natural_key_name)}
        """
        self._registry[model] = (source, key_map or {})

    def _get_load_order(self) -> list[type]:
        """Return registered models: all dimensions first, then all facts."""
        dims = [m for m in self._registry.keys() if issubclass(m, DimensionModel)]
        facts = [m for m in self._registry.keys() if issubclass(m, FactModel)]
        return dims + facts

    async def _load_dimension(self, model: Type, data: list) -> None:
        """Run SCD handler for a dimension model over *data* rows."""
        track_cols = [
            name
            for name in model.model_fields.keys()
            if name not in ["id", "valid_from", "valid_to", "is_current", "checksum"]
        ]
        handler = SCDHandler(model, self.session, track_columns=track_cols)
        await handler.process(data)

    def _resolve_fks(self, record: dict, key_map: dict) -> dict:
        """Return *record* with natural-key FK values replaced by surrogate keys."""
        processed = record.copy()
        for fk_col, (dim_model, nk_name) in key_map.items():
            sk_value = self.resolver.resolve(dim_model, nk_name, record.get(fk_col))
            if sk_value is not None:
                processed[fk_col] = sk_value
        return processed

    def _insert_all(self, model: Type, records: list) -> None:
        """Bulk-insert *records* into *model* and commit."""
        from sqlalchemy import insert as _sa_insert

        self.session.execute(_sa_insert(model), records)
        self.session.commit()

    def _emit(self, event: Any) -> None:
        """Forward *event* to the lineage emitter when one is configured."""
        if self._lineage_emitter is not None:
            self._lineage_emitter.emit(event)

    def _pipeline_outputs(self, loaded_models: list[str]) -> list:
        """Return DatasetRef list for *loaded_models* (deferred import to avoid circulars)."""
        from sqldim.lineage.events import DatasetRef

        return [DatasetRef(namespace="sqldim.silver", name=m) for m in loaded_models]

    async def _load_single_model(
        self,
        model: Type,
        data: list,
        key_map: dict,
        run_id: "str | None",
    ) -> str:
        """Load one model and emit per-model lineage START / COMPLETE / FAIL events."""
        from sqldim.lineage.events import DatasetRef, LineageEvent, RunState

        model_label = model.__name__
        model_type = "dimension" if issubclass(model, DimensionModel) else "fact"
        self._emit(
            LineageEvent(
                run_id=run_id,
                job_name=f"load.{model_label}",
                state=RunState.START,
                facets={"model_type": model_type, "rows": len(data)},
            )
        )
        try:
            if issubclass(model, DimensionModel):
                await self._load_dimension(model, data)
            else:
                # Bulk pre-warm SK cache for every FK column before the
                # per-record resolution loop — one IN query per FK column
                # instead of one query per fact row.
                if key_map:
                    for fk_col, (dim_model, nk_name) in key_map.items():
                        self.resolver.bulk_resolve(
                            dim_model, nk_name, [r.get(fk_col) for r in data]
                        )
                processed_data = [self._resolve_fks(r, key_map) for r in data]
                await self._execute_fact_strategy(
                    model, getattr(model, "__strategy__", None), processed_data, key_map
                )
            self._emit(
                LineageEvent(
                    run_id=run_id,
                    job_name=f"load.{model_label}",
                    state=RunState.COMPLETE,
                    outputs=[DatasetRef(namespace="sqldim.silver", name=model_label)],
                )
            )
        except Exception:  # noqa: BLE001
            self._emit(
                LineageEvent(
                    run_id=run_id,
                    job_name=f"load.{model_label}",
                    state=RunState.FAIL,
                )
            )
            raise
        return model_label

    async def _execute_fact_strategy(
        self,
        model: type,
        strategy_name: str | None,
        processed_data: list,
        key_map: dict,
    ) -> None:
        nk = getattr(model, "__natural_key__", ["id"])[0]
        if strategy_name == "bulk":
            from sqldim.core.loaders.strategies import BulkInsertStrategy

            BulkInsertStrategy().execute(self.session, model, processed_data)
        elif strategy_name == "upsert":
            from sqldim.core.loaders.strategies import UpsertStrategy

            UpsertStrategy(conflict_column=nk).execute(
                self.session, model, processed_data
            )
        elif strategy_name == "merge":
            from sqldim.core.loaders.strategies import MergeStrategy

            MergeStrategy(match_column=nk).execute(self.session, model, processed_data)
        elif strategy_name == "accumulating":
            from sqldim.core.loaders.fact.accumulating import AccumulatingLoader

            loader = AccumulatingLoader(
                model,
                getattr(model, "__match_column__", "id"),
                getattr(model, "__milestones__", []),
                self.session,
            )
            loader.process(processed_data)
        else:
            self._insert_all(model, processed_data)

    def _start_pipeline_lineage(self, model_names: list) -> "str | None":
        """Emit pipeline START event (if emitter configured) and return run_id."""
        from sqldim.lineage.events import LineageEvent, RunState

        if self._lineage_emitter is None:
            return None
        run_id = LineageEvent(job_name="load.pipeline").run_id
        self._emit(
            LineageEvent(
                run_id=run_id,
                job_name="load.pipeline",
                state=RunState.START,
                facets={"models": model_names},
            )
        )
        return run_id

    def _finish_pipeline_lineage(
        self, run_id: "str | None", failed: bool, loaded_models: list
    ) -> None:
        """Emit pipeline COMPLETE/FAIL event when run_id is set."""
        from sqldim.lineage.events import LineageEvent, RunState

        if run_id is None:
            return
        state = RunState.FAIL if failed else RunState.COMPLETE
        self._emit(
            LineageEvent(
                run_id=run_id,
                job_name="load.pipeline",
                state=state,
                facets={"loaded_models": loaded_models},
                outputs=self._pipeline_outputs(loaded_models),
            )
        )

    async def run(self):
        """Executes the load in the correct order with SK resolution.

        If a *lineage_emitter* was provided at construction, lineage events
        are emitted for the overall load and for each individual model load.
        """
        load_order = self._get_load_order()
        model_names = list(map(lambda m: m.__name__, load_order))
        run_id = self._start_pipeline_lineage(model_names)

        failed = False
        loaded_models: list[str] = []
        try:
            for model in load_order:
                data, key_map = self._registry[model]
                loaded_models.append(
                    await self._load_single_model(model, data, key_map, run_id)
                )
        except Exception:  # noqa: BLE001
            failed = True
            raise
        finally:
            self._finish_pipeline_lineage(run_id, failed, loaded_models)
