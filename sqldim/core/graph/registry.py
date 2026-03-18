"""
GraphModel — registry and traversal coordinator.

Ties vertex and edge models together into a queryable graph.
Not a storage layer — it's a query coordinator over existing SQL tables.
"""
from __future__ import annotations

from typing import Any, Literal, Optional, TYPE_CHECKING

from sqldim.exceptions import SchemaError, SemanticError, GrainCompatibilityError
from sqldim.core.graph.traversal import TraversalEngine, _temporal_filter_clause
from sqldim.core.graph.models import EdgeModel, VertexModel

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class GraphModel:
    """
    Registry and traversal API for a graph built from VertexModel /
    EdgeModel subclasses.

    Parameters
    ----------
    *models:
        Any combination of VertexModel and EdgeModel subclasses that
        form the graph.
    session:
        An SQLAlchemy AsyncSession (or sync Session for testing).

    Usage
    -----
    .. code-block:: python

        graph = GraphModel(Player, Game, PlaysInEdge, session=session)
        jordan = await graph.get_vertex(Player, id=1)
        opponents = await graph.neighbors(jordan, edge_type=PlaysAgainstEdge)
    """

    def _register_model(self, m: type) -> None:
        if isinstance(m, type) and issubclass(m, VertexModel):
            self._vertex_models[m] = m.vertex_type()
        elif isinstance(m, type) and issubclass(m, EdgeModel):
            self._edge_models[m] = m.edge_type()
        else:
            raise SchemaError(f"{m} is neither a VertexModel nor an EdgeModel subclass.")

    def __init__(
        self,
        *models: type[VertexModel | EdgeModel],
        session: Any,
    ) -> None:
        self._session = session
        self._engine = TraversalEngine()
        self._vertex_models: dict[type[VertexModel], str] = {}
        self._edge_models: dict[type[EdgeModel], str] = {}
        for m in models:
            self._register_model(m)

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    async def get_vertex(
        self,
        vertex_type: type[VertexModel],
        id: int,
    ) -> VertexModel | None:
        """Fetch a single vertex by its primary-key id."""
        self._assert_vertex_registered(vertex_type)
        from sqlalchemy import text

        table = vertex_type.__tablename__  # type: ignore[attr-defined]
        sql = text(f"SELECT * FROM {table} WHERE id = :id")
        
        result = self._session.execute(sql, {"id": id})
        if hasattr(result, "__await__"):
            result = await result

        row = result.mappings().first()
        if row is None:
            return None
        return vertex_type(**dict(row))

    async def get_vertex_by_key(
        self,
        vertex_type: type[VertexModel],
        **key_values: Any,
    ) -> VertexModel | None:
        """
        Fetch a single vertex by its natural (business) key.

        Resolves business identity without knowing the surrogate ``id``.
        Essential for cross-system graph queries.

        Parameters
        ----------
        vertex_type:
            A registered :class:`VertexModel` subclass that declares
            ``__natural_key__``.
        **key_values:
            Column-name / value pairs covering all natural-key columns.

        Raises
        ------
        SchemaError
            If ``vertex_type`` is not registered, has no natural key, or
            the provided keys don't match ``__natural_key__``.
        """
        self._assert_vertex_registered(vertex_type)
        natural_key: list[str] = getattr(vertex_type, "__natural_key__", [])
        if not natural_key:
            raise SchemaError(
                f"{vertex_type.__name__} has no __natural_key__ defined."
            )
        from sqlalchemy import text

        table = vertex_type.__tablename__  # type: ignore[attr-defined]
        where_parts = " AND ".join(f"{col} = :{col}" for col in natural_key)
        sql = text(f"SELECT * FROM {table} WHERE {where_parts}")

        result = self._session.execute(sql, key_values)
        if hasattr(result, "__await__"):
            result = await result

        row = result.mappings().first()
        if row is None:
            return None
        return vertex_type(**dict(row))

    # ------------------------------------------------------------------
    # Traversal
    # ------------------------------------------------------------------

    async def _execute_sql(self, sql_text: Any) -> Any:
        result = self._session.execute(sql_text)
        if hasattr(result, "__await__"):
            result = await result
        return result

    async def neighbors(
        self,
        vertex: VertexModel,
        edge_type: type[EdgeModel] | None = None,
        direction: Literal["out", "in", "both"] = "both",
        filters: dict[str, Any] | None = None,
        as_of: Any = None,
    ) -> list[VertexModel]:
        """
        Return neighboring vertices connected to *vertex*.

        Parameters
        ----------
        as_of:
            Optional point-in-time filter for SCD Type 2 neighbor vertices.
            Pass a ``datetime.date`` / ``datetime.datetime`` to restrict
            hydrated neighbors to the version active at that instant.
            ``None`` (default) preserves current behaviour — no temporal filter.
        """
        from sqlalchemy import text

        edge_model = self._resolve_edge(vertex.__class__, edge_type)
        start_id = getattr(vertex, "id")
        sql_str = self._engine.neighbors_sql(edge_model, start_id, direction, filters)

        result = await self._execute_sql(text(sql_str))
        neighbor_ids = [row[0] for row in result.fetchall()]

        subject_cls = edge_model.__subject__
        object_cls = edge_model.__object__
        vertex_cls = self._pick_neighbor_class(vertex.__class__, subject_cls, object_cls, direction)

        if not neighbor_ids:
            return []

        id_list = ", ".join(str(i) for i in neighbor_ids)
        temporal_clause = _temporal_filter_clause(vertex_cls, as_of)
        rows_result = await self._execute_sql(
            text(f"SELECT * FROM {vertex_cls.__tablename__} WHERE id IN ({id_list}){temporal_clause}")
        )
        return [vertex_cls(**dict(row)) for row in rows_result.mappings().fetchall()]

    async def paths(
        self,
        source: VertexModel,
        target: VertexModel,
        via: type[EdgeModel] | None = None,
        max_hops: int = 3,
    ) -> list[list[int]]:
        """
        Find all paths (as lists of vertex IDs) from *source* to *target*.

        Returns a list of paths, where each path is a list of vertex IDs.
        """
        from sqlalchemy import text

        edge_model = self._resolve_edge(source.__class__, via)
        sql_str = self._engine.paths_sql(
            edge_model,
            start_id=getattr(source, "id"),
            target_id=getattr(target, "id"),
            max_hops=max_hops,
        )
        result = await self._session.execute(text(sql_str))
        return [list(row[0]) for row in result.fetchall()]

    # ------------------------------------------------------------------
    # Analytical
    # ------------------------------------------------------------------

    async def neighbor_aggregation(
        self,
        vertex: VertexModel,
        edge_type: type[EdgeModel],
        measure: str,
        agg: Literal["sum", "avg", "count", "max", "min"] = "sum",
        direction: Literal["out", "in", "both"] = "both",
        validate_additive: bool = False,
        weighted: bool = False,
    ) -> float:
        """
        Aggregate *measure* across edges incident to *vertex*.

        Parameters
        ----------
        validate_additive:
            When ``True``, raises :class:`SemanticError` if ``agg`` is
            ``"sum"`` or ``"avg"`` and the column's ``additive`` metadata
            is ``False``.  Defaults to ``False`` to preserve backward
            compatibility.
        weighted:
            When ``True``, multiplies *measure* by the edge table's
            ``weight`` column before aggregating — intended for
            bridge-table allocation semantics.
        """
        from sqlalchemy import text

        self._assert_edge_registered(edge_type)

        if validate_additive and agg in ("sum", "avg"):
            self._check_additive(edge_type, measure, agg)

        sql_str = self._engine.aggregate_sql(
            edge_type,
            start_id=getattr(vertex, "id"),
            measure=measure,
            agg=agg,
            direction=direction,
            weighted=weighted,
        )
        result = await self._session.execute(text(sql_str))
        row = result.fetchone()
        if row is None or row[0] is None:
            return 0.0
        return float(row[0])

    # ------------------------------------------------------------------
    # Metadata validation methods (Tier 1)
    # ------------------------------------------------------------------

    def validate_grain_join(self, *edge_types: type[EdgeModel]) -> None:
        """
        Validate that all edge models with declared grains are compatible.

        Edge models without ``__grain__`` are silently ignored (they make
        no grain claim and cannot conflict).

        Raises
        ------
        GrainCompatibilityError
            If two or more edge models declare *different* grains.
        """
        grains: dict[str, str] = {}
        for et in edge_types:
            grain = getattr(et, "__grain__", None)
            if grain is not None:
                grains[et.__name__] = grain
        unique_grains = set(grains.values())
        if len(unique_grains) > 1:
            detail = ", ".join(f"{n}={g!r}" for n, g in grains.items())
            raise GrainCompatibilityError(
                f"Incompatible grains in multi-fact join: {detail}"
            )

    # ------------------------------------------------------------------
    # Role-aware edge discovery (Tier 3)
    # ------------------------------------------------------------------

    def discover_role_edges(
        self,
        schema_graph: Any,
    ) -> dict[str, list]:
        """
        Discover role-playing edge types from a :class:`SchemaGraph`.

        For each fact in *schema_graph* that has role-playing dimension
        FK columns (declared with ``role=``), returns.

        Returns
        -------
        dict[str, list[RolePlayingRef]]
            Maps fact class name → list of
            :class:`~sqldim.core.kimball.schema_graph.RolePlayingRef`
            instances, one per logical role.
        """
        all_facts = list(getattr(schema_graph, "facts", []))
        # Also include EdgeModel subclasses registered in this GraphModel
        for e in self._edge_models:
            if e not in all_facts:
                all_facts.append(e)

        result: dict[str, list] = {}
        for fact_cls in all_facts:
            refs = schema_graph.get_role_playing_dimensions(fact_cls)
            if refs:
                result[fact_cls.__name__] = refs
        return result

    # ------------------------------------------------------------------
    # Strategy-guided freshness (Tier 3)
    # ------------------------------------------------------------------

    async def freshness(
        self,
        edge_type: type[EdgeModel],
        timestamp_column: str = "updated_at",
    ) -> Any:
        """
        Return the high-water mark timestamp for an edge table.

        Behaviour depends on ``edge_type.__strategy__``:

        * ``"upsert"`` / ``"merge"`` — data is always considered current;
          returns ``None`` without issuing a DB query.
        * ``"bulk"`` / ``"accumulating"`` / ``None`` — queries
          ``MAX(timestamp_column)`` from the edge table and returns the
          result (or ``None`` if the table is empty).

        Raises
        ------
        SchemaError
            If ``edge_type`` is not registered in this GraphModel.
        """
        from sqlalchemy import text

        self._assert_edge_registered(edge_type)
        strategy = getattr(edge_type, "__strategy__", None)
        if strategy in ("upsert", "merge"):
            return None

        table = edge_type.__tablename__  # type: ignore[attr-defined]
        sql = text(f"SELECT MAX({timestamp_column}) FROM {table}")
        result = await self._execute_sql(sql)
        row = result.fetchone()
        return row[0] if row else None

    async def degree(
        self,
        vertex: VertexModel,
        edge_type: type[EdgeModel] | None = None,
        direction: Literal["out", "in", "both"] = "both",
    ) -> int:
        """Return the number of edges incident to *vertex*."""
        from sqlalchemy import text

        edge_model = self._resolve_edge(vertex.__class__, edge_type)
        sql_str = self._engine.degree_sql(
            edge_model,
            start_id=getattr(vertex, "id"),
            direction=direction,
        )
        result = await self._session.execute(text(sql_str))
        row = result.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    # ------------------------------------------------------------------
    # SQL inspection
    # ------------------------------------------------------------------

    def explain(self, operation: str, **kwargs: Any) -> str:
        """
        Return the SQL string for a named operation without executing it.

        Parameters
        ----------
        operation:
            One of: "neighbors", "paths", "aggregate", "degree".
        **kwargs:
            Arguments forwarded to the corresponding TraversalEngine method.
        """
        dispatch = {
            "neighbors": self._engine.neighbors_sql,
            "paths": self._engine.paths_sql,
            "aggregate": self._engine.aggregate_sql,
            "degree": self._engine.degree_sql,
        }
        if operation not in dispatch:
            raise SchemaError(
                f"Unknown operation {operation!r}. "
                f"Valid operations: {list(dispatch.keys())}"
            )
        return dispatch[operation](**kwargs)

    # ------------------------------------------------------------------
    # Introspection helpers
    # ------------------------------------------------------------------

    @property
    def vertex_models(self) -> list[type[VertexModel]]:
        return list(self._vertex_models.keys())

    @property
    def edge_models(self) -> list[type[EdgeModel]]:
        return list(self._edge_models.keys())

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _assert_vertex_registered(self, cls: type[VertexModel]) -> None:
        if cls not in self._vertex_models:
            raise SchemaError(
                f"{cls.__name__} is not registered in this GraphModel."
            )

    def _assert_edge_registered(self, cls: type[EdgeModel]) -> None:
        if cls not in self._edge_models:
            raise SchemaError(
                f"{cls.__name__} is not registered in this GraphModel."
            )

    @staticmethod
    def _check_additive(edge_type: type[EdgeModel], measure: str, agg: str) -> None:
        """
        Raise :class:`SemanticError` if *measure* is flagged non-additive
        and *agg* is ``sum`` or ``avg``.

        Reads ``column.info["additive"]`` set by the sqldim ``Field()``
        factory.  When the column carries no ``additive`` metadata (e.g.
        plain SQLModel fields), the check is skipped (permissive default).
        """
        if not hasattr(edge_type, "__table__"):
            return
        col = edge_type.__table__.columns.get(measure)
        if col is None or not col.info:
            return
        additive = col.info.get("additive", True)
        if not additive:
            raise SemanticError(
                f"Column {measure!r} on {edge_type.__name__} is non-additive; "
                f"{agg!r} is not semantically valid. "
                f"Use count, max, or min instead."
            )

    def _find_candidate_edges(self, vertex_cls: type[VertexModel]) -> list[type[EdgeModel]]:
        return [
            e for e in self._edge_models
            if getattr(e, "__subject__", None) is vertex_cls
            or getattr(e, "__object__", None) is vertex_cls
        ]

    def _resolve_edge(
        self,
        vertex_cls: type[VertexModel],
        edge_type: type[EdgeModel] | None,
    ) -> type[EdgeModel]:
        if edge_type is not None:
            self._assert_edge_registered(edge_type)
            return edge_type
        candidates = self._find_candidate_edges(vertex_cls)
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) == 0:
            raise SchemaError(f"No edge type registered that connects to {vertex_cls.__name__}.")
        raise SchemaError(
            f"Multiple edge types connect to {vertex_cls.__name__}; "
            f"specify edge_type explicitly."
        )

    @staticmethod
    def _pick_neighbor_class(
        vertex_cls: type[VertexModel],
        subject_cls: type[VertexModel],
        object_cls: type[VertexModel],
        direction: str,
    ) -> type[VertexModel]:
        """Determine which vertex class the neighbor IDs belong to."""
        if direction == "out":
            return object_cls
        if direction == "in":
            return subject_cls
        # "both" — if subject == object (self-referential), either works
        if subject_cls is object_cls:
            return subject_cls
        # For "both" on heterogeneous edges, return the non-source class
        if vertex_cls is subject_cls:
            return object_cls
        return subject_cls
