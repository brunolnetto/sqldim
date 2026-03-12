"""
Extended SchemaGraph — TD-001 revision.

Any DimensionModel is implicitly a vertex.
Any FactModel is implicitly an edge.
VertexModel/EdgeModel subclasses with explicit __vertex_type__/__edge_type__
are enriched but not required.
"""
from __future__ import annotations

from typing import Any

from sqldim.core.graph import SchemaGraph as _BaseSchemaGraph
from sqldim.core.models import DimensionModel, FactModel
from sqldim.exceptions import SchemaError


def _safe_subclass(cls: Any, base: type) -> bool:
    try:
        return isinstance(cls, type) and issubclass(cls, base)
    except TypeError:
        return False


# ---------------------------------------------------------------------------
# Data objects
# ---------------------------------------------------------------------------

class GraphSchema:
    """Serialisable representation of the graph portion of a schema."""

    def __init__(
        self,
        vertices: list[dict[str, Any]],
        edges: list[dict[str, Any]],
    ) -> None:
        self.vertices = vertices
        self.edges = edges

    def to_dict(self) -> dict[str, Any]:
        return {"vertices": self.vertices, "edges": self.edges}


# ---------------------------------------------------------------------------
# SchemaGraph
# ---------------------------------------------------------------------------

class SchemaGraph(_BaseSchemaGraph):
    """
    Drop-in replacement for the core SchemaGraph that additionally
    exposes graph-oriented views of the star schema.

    TD-001: VertexModel/EdgeModel base classes are not required.
    - ``vertices`` = all DimensionModel subclasses in the model list
    - ``edges``    = all FactModel subclasses in the model list
    Models that explicitly inherit VertexModel/EdgeModel or declare
    __vertex_type__/__edge_type__ are enriched with that metadata.
    """

    # ``dimensions`` and ``facts`` are already populated by _BaseSchemaGraph.
    # ``vertices``/``edges`` are now just aliases with the same sets.

    @property
    def vertices(self) -> list[type[DimensionModel]]:
        """All DimensionModel subclasses — every dimension is implicitly a vertex."""
        return list(self.dimensions)

    @property
    def edges(self) -> list[type[FactModel]]:
        """All FactModel subclasses — every fact is implicitly an edge."""
        return list(self.facts)

    # ------------------------------------------------------------------
    # graph_schema() — auto-derives from FK metadata
    # ------------------------------------------------------------------

    def graph_schema(self) -> GraphSchema:
        """
        Return a GraphSchema describing all vertex and edge types.

        For FactModels the subject/object are derived from __subject__/__object__
        when explicitly set, otherwise inferred from FK column metadata
        (``column.info["dimension"]``).
        """
        vertex_info: list[dict[str, Any]] = []
        for v in self.vertices:
            vertex_info.append({
                "name": v.__name__,
                "vertex_type": getattr(v, "__vertex_type__", v.__name__.lower()),
                "natural_key": getattr(v, "__natural_key__", []),
                "columns": list(v.model_fields.keys()),
            })

        dim_by_name = {d.__name__: d for d in self.dimensions}

        edge_info: list[dict[str, Any]] = []
        for e in self.edges:
            subject_cls = getattr(e, "__subject__", None)
            object_cls = getattr(e, "__object__", None)

            # Auto-derive subject/object from FK metadata when not explicit
            if subject_cls is None or object_cls is None:
                fk_dims = self._fk_dimensions(e)
                dim_list = list(fk_dims.values())
                if subject_cls is None and len(dim_list) >= 1:
                    subject_cls = dim_list[0]
                if object_cls is None and len(dim_list) >= 2:
                    object_cls = dim_list[1]

            directed: bool = getattr(e, "__directed__", True)
            self_ref = (subject_cls is not None and subject_cls is object_cls)

            edge_info.append({
                "name": e.__name__,
                "edge_type": getattr(e, "__edge_type__", e.__name__.lower()),
                "subject": subject_cls.__name__ if subject_cls else None,
                "object": object_cls.__name__ if object_cls else None,
                "directed": directed,
                "self_referential": self_ref,
                "columns": list(e.model_fields.keys()),
            })

        return GraphSchema(vertices=vertex_info, edges=edge_info)

    # ------------------------------------------------------------------
    # to_graph() — projection approach (TD-001)
    # ------------------------------------------------------------------

    def to_graph(self) -> dict[str, Any]:
        """
        Derive a graph representation from existing DimensionModel/FactModel
        FK metadata automatically — no VertexModel/EdgeModel required.

        Returns a dict with:
          "vertices": list of {name, vertex_type, columns}
          "edges":    list of {name, edge_type, subject, object, columns}
        """
        return self.graph_schema().to_dict()

    # ------------------------------------------------------------------
    # validate()
    # ------------------------------------------------------------------

    def validate(self) -> list[SchemaError]:
        """
        Validate graph consistency.

        Checks only models that explicitly declare __subject__/__object__
        (i.e. EdgeModel subclasses with explicit FK references).
        For implicit edges (plain FactModels), no graph validation is done —
        FK integrity is already enforced at the DB level.
        """
        errors: list[SchemaError] = []
        vertex_set = set(self.vertices)

        for fact_cls in self.edges:
            subject = getattr(fact_cls, "__subject__", None)
            obj = getattr(fact_cls, "__object__", None)

            # Only validate models that explicitly declare __subject__/__object__
            if subject is None and obj is None:
                continue

            if subject is not None and subject not in vertex_set:
                errors.append(SchemaError(
                    f"{fact_cls.__name__}: __subject__ {subject.__name__!r} "
                    f"is not a registered vertex."
                ))
            if obj is not None and obj not in vertex_set:
                errors.append(SchemaError(
                    f"{fact_cls.__name__}: __object__ {obj.__name__!r} "
                    f"is not a registered vertex."
                ))

        return errors

    # ------------------------------------------------------------------
    # to_mermaid() — extended to render graph models
    # ------------------------------------------------------------------

    def to_mermaid(self) -> str:
        """
        Render a Mermaid ER diagram that includes both the star schema
        (dimensions + facts) and graph relationships.

        VertexModel subclasses with __vertex_type__ are labelled; plain
        DimensionModels are rendered as standard rectangles.
        """
        from sqldim.models.graph import VertexModel, EdgeModel

        lines = ["erDiagram"]
        rendered: set[str] = set()

        def render_model(m: type) -> None:
            if m.__name__ in rendered:
                return
            rendered.add(m.__name__)
            lines.append(f"    {m.__name__} {{")
            for col_name in m.model_fields:
                annotation = m.__annotations__.get(col_name)
                if annotation is int:
                    col_type = "int"
                elif annotation is float:
                    col_type = "float"
                elif annotation is bool:
                    col_type = "bool"
                else:
                    col_type = "string"
                lines.append(f"        {col_type} {col_name}")
            lines.append("    }")

        # Dimensions (includes VertexModel subclasses)
        for d in self.dimensions:
            render_model(d)

        # Facts (includes EdgeModel subclasses)
        for f in self.facts:
            render_model(f)

        # Star schema relationships
        for fact in self.facts:
            # Prefer explicit __subject__/__object__ for graph edges
            if _safe_subclass(fact, EdgeModel) and (
                getattr(fact, "__subject__", None) or getattr(fact, "__object__", None)
            ):
                subject = getattr(fact, "__subject__", None)
                obj = getattr(fact, "__object__", None)
                edge_label = getattr(fact, "__edge_type__", fact.__name__.lower())
                if subject:
                    lines.append(
                        f"    {subject.__name__} ||--o{{ {fact.__name__} : \"{edge_label}\""
                    )
                if obj:
                    lines.append(
                        f"    {fact.__name__} }}o--|| {obj.__name__} : \"\""
                    )
            else:
                # Implicit: render FK → dimension relationships
                star = self.get_star_schema(fact)
                for fk_col, dim in star["dimensions"].items():
                    lines.append(f"    {fact.__name__} }}o--||  {dim.__name__} : \"{fk_col}\"")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fk_dimensions(self, fact_cls: type[FactModel]) -> dict[str, type[DimensionModel]]:
        """Return {fk_col: DimensionClass} from FK metadata on the fact table."""
        star = self.get_star_schema(fact_cls)
        return star.get("dimensions", {})
