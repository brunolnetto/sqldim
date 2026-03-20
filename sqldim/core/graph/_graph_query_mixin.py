"""GraphModel query-helper and inspection methods.

Extracted from registry.py to keep file sizes manageable.
These methods are mixed into :class:`GraphModel` via inheritance.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from sqldim.core.graph.models import EdgeModel, VertexModel

from sqldim.exceptions import SchemaError, SemanticError


class _GraphQueryMixin:
    """SQL inspection and internal helper methods for GraphModel."""

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
            raise SchemaError(f"{cls.__name__} is not registered in this GraphModel.")

    def _assert_edge_registered(self, cls: type[EdgeModel]) -> None:
        if cls not in self._edge_models:
            raise SchemaError(f"{cls.__name__} is not registered in this GraphModel.")

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

    def _find_candidate_edges(
        self, vertex_cls: type[VertexModel]
    ) -> list[type[EdgeModel]]:
        return [
            e
            for e in self._edge_models
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
            raise SchemaError(
                f"No edge type registered that connects to {vertex_cls.__name__}."
            )
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

    # ---------------------------------------------------------------------------
    # UnifiedGraph — Tier 4: fuses SchemaGraph (ERD) + GraphModel (runtime)
    # ---------------------------------------------------------------------------
