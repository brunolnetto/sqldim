"""
Extended SchemaGraph — TD-001 revision.

Any DimensionModel is implicitly a vertex.
Any FactModel is implicitly an edge.
VertexModel/EdgeModel subclasses with explicit __vertex_type__/__edge_type__
are enriched but not required.
"""
from __future__ import annotations

from typing import Any

from sqldim.core.kimball.schema_graph import SchemaGraph as _BaseSchemaGraph
from sqldim.core.kimball.models import DimensionModel, FactModel
from sqldim.exceptions import SchemaError


def _safe_subclass(cls: Any, base: type) -> bool:
    try:
        return isinstance(cls, type) and issubclass(cls, base)
    except TypeError:
        return False


def _vertex_info(v: type) -> dict[str, Any]:
    return {
        "name": v.__name__,
        "vertex_type": getattr(v, "__vertex_type__", v.__name__.lower()),
        "natural_key": getattr(v, "__natural_key__", []),
        "columns": list(v.model_fields.keys()),
    }


def _infer_endpoints_from_star(
    e: type, dimensions: list
) -> tuple[type | None, type | None]:
    from sqldim.core.kimball.schema_graph import SchemaGraph as _Core
    star = _Core(list(dimensions)).get_star_schema(e)
    dim_list = list(star.get("dimensions", {}).values())
    subject_cls = dim_list[0] if len(dim_list) >= 1 else None
    object_cls = dim_list[1] if len(dim_list) >= 2 else None
    return subject_cls, object_cls


def _resolve_edge_endpoints(
    e: type, dimensions: list
) -> tuple[type | None, type | None]:
    subject_cls = getattr(e, "__subject__", None)
    object_cls = getattr(e, "__object__", None)
    if subject_cls is None or object_cls is None:
        inferred_sub, inferred_obj = _infer_endpoints_from_star(e, dimensions)
        if subject_cls is None:
            subject_cls = inferred_sub
        if object_cls is None:
            object_cls = inferred_obj
    return subject_cls, object_cls


def _edge_info(e: type, dimensions: list) -> dict[str, Any]:
    subject_cls, object_cls = _resolve_edge_endpoints(e, dimensions)
    directed: bool = getattr(e, "__directed__", True)
    return {
        "name": e.__name__,
        "edge_type": getattr(e, "__edge_type__", e.__name__.lower()),
        "subject": subject_cls.__name__ if subject_cls else None,
        "object": object_cls.__name__ if object_cls else None,
        "directed": directed,
        "self_referential": subject_cls is not None and subject_cls is object_cls,
        "columns": list(e.model_fields.keys()),
    }


def _annotation_type(annotation: Any) -> str:
    if annotation is int:
        return "int"
    if annotation is float:
        return "float"
    if annotation is bool:
        return "bool"
    return "string"


def _render_graph_model(m: type, lines: list) -> None:
    if hasattr(m, "_rendered_in_mermaid"):
        return
    lines.append(f"    {m.__name__} {{")
    for col_name in m.model_fields:
        annotation = m.__annotations__.get(col_name)
        lines.append(f"        {_annotation_type(annotation)} {col_name}")
    lines.append("    }")


def _validate_endpoint(
    fact_cls: type, attr_name: str, vertex_set: set
) -> "SchemaError | None":
    endpoint = getattr(fact_cls, attr_name, None)
    if endpoint is not None and endpoint not in vertex_set:
        return SchemaError(
            f"{fact_cls.__name__}: {attr_name} {endpoint.__name__!r} "
            f"is not a registered vertex."
        )
    return None


def _validate_edge(
    fact_cls: type, vertex_set: set
) -> list["SchemaError"]:
    subject = getattr(fact_cls, "__subject__", None)
    obj = getattr(fact_cls, "__object__", None)
    if subject is None and obj is None:
        return []
    errors = []
    for attr in ("__subject__", "__object__"):
        err = _validate_endpoint(fact_cls, attr, vertex_set)
        if err:
            errors.append(err)
    return errors


def _render_edge_lines(fact: type, lines: list, subject, obj, edge_label: str) -> None:
    if subject:
        lines.append(f"    {subject.__name__} ||--o{{ {fact.__name__} : \"{edge_label}\"")
    if obj:
        lines.append(f"    {fact.__name__} }}o--|| {obj.__name__} : \"\"")


def _render_edge_relations(fact: type, lines: list, star: dict) -> None:
    from sqldim.core.graph.models import EdgeModel
    if _safe_subclass(fact, EdgeModel) and (
        getattr(fact, "__subject__", None) or getattr(fact, "__object__", None)
    ):
        subject = getattr(fact, "__subject__", None)
        obj = getattr(fact, "__object__", None)
        edge_label = getattr(fact, "__edge_type__", fact.__name__.lower())
        _render_edge_lines(fact, lines, subject, obj, edge_label)
    else:
        for fk_col, dim in star["dimensions"].items():
            lines.append(f"    {fact.__name__} }}o--||  {dim.__name__} : \"{fk_col}\"")


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
        """
        vertex_info = [_vertex_info(v) for v in self.vertices]
        edge_info = [_edge_info(e, self.dimensions) for e in self.edges]
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
        """
        vertex_set = set(self.vertices)
        errors: list[SchemaError] = []
        for fact_cls in self.edges:
            errors.extend(_validate_edge(fact_cls, vertex_set))
        return errors

    # ------------------------------------------------------------------
    # to_mermaid() — extended to render graph models
    # ------------------------------------------------------------------

    def _render_models(self, models, lines: list, rendered: set) -> None:
        for m in models:
            if m.__name__ not in rendered:
                rendered.add(m.__name__)
                _render_graph_model(m, lines)

    def to_mermaid(self) -> str:
        """
        Render a Mermaid ER diagram that includes both the star schema
        (dimensions + facts) and graph relationships.
        """
        lines = ["erDiagram"]
        rendered: set[str] = set()
        self._render_models(self.dimensions, lines, rendered)
        self._render_models(self.facts, lines, rendered)
        for fact in self.facts:
            star = self.get_star_schema(fact)
            _render_edge_relations(fact, lines, star)
        return "\n".join(lines)

    # ------------------------------------------------------------------

    def _fk_dimensions(self, fact_cls: type[FactModel]) -> dict[str, type[DimensionModel]]:
        """Return {fk_col: DimensionClass} from FK metadata on the fact table."""
        star = self.get_star_schema(fact_cls)
        return star.get("dimensions", {})
