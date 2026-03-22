"""
UnifiedGraph — Tier 4 facade bridging SchemaGraph and GraphModel.

Extracted from registry.py to keep file sizes manageable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqldim.core.graph.registry import GraphModel

if TYPE_CHECKING:
    from sqldim.core.graph.schema_graph import SchemaGraph
    from sqldim.core.graph._impl._schema_diff import SchemaDiff
    from sqldim.core.graph.models import EdgeModel, VertexModel


class UnifiedGraph:
    """
    Tier 4 facade that bridges the ERD-level :class:`~sqldim.core.graph.schema_graph.SchemaGraph`
    (FK topology) and the runtime :class:`GraphModel` (traversal engine) into
    a single analytical surface.

    This is the recommended entry point for graph analytics over a
    complete sqldim data mart.

    Capabilities
    ------------
    * **Auto-edge discovery** — reads FK metadata from ``schema_graph`` and
      synthesises implicit edge registration in ``graph_model`` for any fact
      that has ``dimension`` column annotations.  No manual ``__subject__`` /
      ``__object__`` declarations required.
    * **Schema diffing** — compare two ``SchemaGraph`` snapshots and obtain a
      structured :class:`~sqldim.core.graph.schema_graph.SchemaDiff`.
    * **Traversal delegation** — all ``GraphModel`` methods (``neighbors``,
      ``paths``, ``neighbor_aggregation``, …) are accessible via ``graph``.

    Parameters
    ----------
    schema_graph:
        A :class:`~sqldim.core.graph.schema_graph.SchemaGraph` instance
        describing the star-schema topology.
    session:
        An SQLAlchemy session forwarded to the underlying :class:`GraphModel`.
    auto_register:
        When ``True`` (default), auto-discover edges from FK metadata and
        register them in the :class:`GraphModel`.

    Example
    -------
    .. code-block:: python

        from sqldim.core.graph import UnifiedGraph, SchemaGraph

        sg = SchemaGraph([CustomerDim, DateDim, SalesFact])
        ug = UnifiedGraph(sg, session=session)

        # All facts are now traversable — no EdgeModel subclassing needed
        neighbors = await ug.graph.neighbors(customer, direction="out")

        # Schema diffing
        sg_v2 = SchemaGraph([CustomerDim, DateDim, ProductDim, SalesFact])
        diff = ug.diff(sg_v2)
        print(diff.summary())
    """

    def __init__(
        self,
        schema_graph: "SchemaGraph",
        *,
        session: object,
        auto_register: bool = True,
    ) -> None:
        self._schema_graph = schema_graph
        self.graph = GraphModel(session=session)
        if auto_register:
            self._auto_register_from_schema()

    # ------------------------------------------------------------------
    # Auto-edge discovery (Tier 4)
    # ------------------------------------------------------------------

    def _register_vertex(self, dim_cls: "type[VertexModel]") -> None:
        """Register *dim_cls* as a vertex if not already present."""
        if dim_cls not in self.graph._vertex_models:
            self.graph._vertex_models[dim_cls] = getattr(
                dim_cls, "__vertex_type__", dim_cls.__name__.lower()
            )

    def _register_edge(self, fact_cls: "type[EdgeModel]") -> None:
        """Register *fact_cls* as an edge, patching endpoints if needed."""
        fk_dims = self._schema_graph._fk_dimensions(fact_cls)
        dim_list = list(fk_dims.values())
        if len(dim_list) < 2:
            return
        if not getattr(fact_cls, "__subject__", None):
            fact_cls.__subject__ = dim_list[0]  # type: ignore[attr-defined]
        if not getattr(fact_cls, "__object__", None):
            fact_cls.__object__ = dim_list[1]  # type: ignore[attr-defined]
        self.graph._edge_models[fact_cls] = getattr(
            fact_cls, "__edge_type__", fact_cls.__name__.lower()
        )

    def _auto_register_from_schema(self) -> None:
        """
        Synthesise implicit :class:`VertexModel` / :class:`EdgeModel`
        registrations from FK metadata in the :class:`SchemaGraph`.

        Algorithm
        ---------
        1. Every ``DimensionModel`` in ``schema_graph.vertices`` is
           registered as a vertex, reusing its own class object (sqldim's
           ``DimensionModel`` already extends ``VertexModel``'s structural
           interface for purposes of traversal).
        2. Every ``FactModel`` in ``schema_graph.edges`` is introspected for
           FK columns carrying ``dimension`` metadata.  The first two
           dimension FK columns are used as ``__subject__`` and ``__object__``
           endpoints.  The fact class is then registered as an edge.
        3. Facts with fewer than two FK columns are skipped (cannot form an
           edge without two endpoints).
        """
        for dim_cls in self._schema_graph.vertices:
            self._register_vertex(dim_cls)

        for fact_cls in self._schema_graph.edges:
            if fact_cls not in self.graph._edge_models:
                self._register_edge(fact_cls)

    # ------------------------------------------------------------------
    # Schema diffing (Tier 4)
    # ------------------------------------------------------------------

    def diff(self, other_schema_graph: "SchemaGraph") -> "SchemaDiff":
        """
        Compute the graph edit distance between the current schema and
        *other_schema_graph*.

        Delegates to :meth:`SchemaGraph.diff <sqldim.core.graph.schema_graph.SchemaGraph.diff>`.

        Parameters
        ----------
        other_schema_graph:
            A newer :class:`~sqldim.core.graph.schema_graph.SchemaGraph`
            snapshot to compare against.

        Returns
        -------
        SchemaDiff
            Structured diff with added/removed vertices, edges, and
            per-model column changes.

        Example
        -------
        .. code-block:: python

            diff = unified_graph.diff(sg_v3)
            if not diff.is_empty:
                print(diff.summary())
        """
        return self._schema_graph.diff(other_schema_graph)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def schema_graph(self) -> "SchemaGraph":
        """The underlying :class:`SchemaGraph`."""
        return self._schema_graph

    def registered_vertices(self) -> list:
        return self.graph.vertex_models

    def registered_edges(self) -> list:
        return self.graph.edge_models
