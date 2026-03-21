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
        "edge_kind": "verb",
        "subject": subject_cls.__name__ if subject_cls else None,
        "object": object_cls.__name__ if object_cls else None,
        "directed": directed,
        "self_referential": subject_cls is not None and subject_cls is object_cls,
        "columns": list(e.model_fields.keys()),
        "valid_from": "valid_from" in e.model_fields,
        "valid_to": "valid_to" in e.model_fields,
    }


def _has_column(model_cls: type, col: str) -> bool:
    """Return True if *col* is present in model_fields (O(1) dict lookup)."""
    return col in model_cls.model_fields


def _verb_adj_entries(
    fact_cls: type, fk_dims: dict, fadj: dict[str, list[str]]
) -> None:
    """Add forward verb edges (dim→fact) for *fact_cls* into *fadj* in-place."""
    fact_name = fact_cls.__name__
    for dim_cls in fk_dims.values():
        dim_name = dim_cls.__name__
        if dim_name in fadj:
            fadj[dim_name].append(fact_name)


def _cls_name(obj: object) -> str:
    """Return `obj.__name__` when *obj* is a type, else `str(obj)` (CC=1)."""
    return obj.__name__ if isinstance(obj, type) else str(obj)  # type: ignore[union-attr]


def _bridge_adj_entries(bridge_cls: type, fadj: dict[str, list[str]]) -> None:
    """Add forward bridge edge (subject→object) for *bridge_cls* into *fadj*."""
    subj = getattr(bridge_cls, "__subject__", None)
    obj = getattr(bridge_cls, "__object__", None)
    if subj is None or obj is None:
        return
    subj_name = _cls_name(subj)
    obj_name = _cls_name(obj)
    if subj_name in fadj:
        fadj[subj_name].append(obj_name)


def _build_transposed_adj(
    node_names: list[str], forward: dict[str, list[str]]
) -> dict[str, list[str]]:
    """Reverse all forward edges to build G^T adjacency list (DGM §2.1, D18)."""
    tadj: dict[str, list[str]] = {n: [] for n in node_names}
    for src, targets in forward.items():
        for tgt in targets:
            tadj[tgt].append(src)
    return tadj


def _bridge_edge_info(b: type) -> dict[str, Any]:
    """Build edge info dict for a BridgeModel (τ_E → bridge)."""
    return {
        "name": b.__name__,
        "edge_type": getattr(b, "__edge_type__", b.__name__.lower()),
        "edge_kind": "bridge",
        "subject": getattr(b, "__subject__", None),
        "object": getattr(b, "__object__", None),
        "directed": getattr(b, "__directed__", False),
        "columns": list(b.model_fields.keys()),
        "has_weight": "weight" in b.model_fields,
        "valid_from": _has_column(b, "valid_from"),
        "valid_to": _has_column(b, "valid_to"),
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


def _validate_edge(fact_cls: type, vertex_set: set) -> list["SchemaError"]:
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
        lines.append(f'    {subject.__name__} ||--o{{ {fact.__name__} : "{edge_label}"')
    if obj:
        lines.append(f'    {fact.__name__} }}o--|| {obj.__name__} : ""')


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
            lines.append(f'    {fact.__name__} }}o--||  {dim.__name__} : "{fk_col}"')


# ---------------------------------------------------------------------------
# Data objects
# ---------------------------------------------------------------------------


class GraphSchema:
    """Serialisable representation of the graph portion of a schema.

    The optional *annotations* parameter holds the Σ set of SchemaAnnotation
    objects defined in §2.4 of the DGM v0.16 spec.  It defaults to an empty
    list so that all existing call-sites remain unchanged.
    """

    def __init__(
        self,
        vertices: list[dict[str, Any]],
        edges: list[dict[str, Any]],
        annotations: list | None = None,
    ) -> None:
        self.vertices = vertices
        self.edges = edges
        self.annotations: list = annotations if annotations is not None else []

    def to_dict(self) -> dict[str, Any]:
        from sqldim.core.query._dgm_annotations import annotation_kind

        def _ann_to_dict(ann: object) -> dict[str, Any]:
            kind = annotation_kind(ann)  # type: ignore[arg-type]
            d: dict[str, Any] = {"kind": kind}
            for k, v in vars(ann).items():
                if isinstance(v, (set, frozenset)):
                    d[k] = sorted(v)
                else:
                    d[k] = v
            return d

        result: dict[str, Any] = {"vertices": self.vertices, "edges": self.edges}
        if self.annotations:
            result["annotations"] = [_ann_to_dict(a) for a in self.annotations]
        else:
            result["annotations"] = []
        return result


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

    DGM extension: pass ``bridge_models`` to include BridgeModel edges
    classified as ``edge_kind='bridge'`` in graph_schema().
    """

    def __init__(self, models: list, bridge_models: list | None = None) -> None:
        super().__init__(models)
        self._bridge_models: list = bridge_models or []

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

        Includes both verb edges (FactModel) classified as edge_kind='verb'
        and bridge edges (BridgeModel) classified as edge_kind='bridge'.
        """
        vertex_info = [_vertex_info(v) for v in self.vertices]
        verb_edges = [_edge_info(e, self.dimensions) for e in self.edges]
        bridge_edges = [_bridge_edge_info(b) for b in self._bridge_models]
        return GraphSchema(vertices=vertex_info, edges=verb_edges + bridge_edges)

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

    # ------------------------------------------------------------------
    # DGM §2.1: N = D ∪ F, forward adjacency, G^T transposed adjacency
    # ------------------------------------------------------------------

    @property
    def all_nodes(self) -> list:
        """N = D ∪ F: both dimension and fact model classes per DGM §2.1."""
        return list(self.vertices) + list(self.edges)

    @property
    def forward_adj(self) -> dict[str, list[str]]:
        """Forward adjacency: class_name → [reachable class_names].

        Verb edges contribute dim → fact entries.
        Bridge edges with explicit ``__subject__``/``__object__`` contribute
        subject → object entries.
        """
        node_names = [n.__name__ for n in self.all_nodes]
        fadj: dict[str, list[str]] = {n: [] for n in node_names}
        for fact in self.edges:
            _verb_adj_entries(fact, self._fk_dimensions(fact), fadj)
        for bridge in self._bridge_models:
            _bridge_adj_entries(bridge, fadj)
        return fadj

    @property
    def transposed_adj(self) -> dict[str, list[str]]:
        """G^T adjacency: reverse all forward edges (DGM §2.1, D18).

        Used for backward-cone computation (REACHABLE_TO), SINCE/ONCE/
        PREVIOUSLY TemporalMode SQL, and INCOMING_SIGNATURES algorithms.
        Built in O(|E|) — all edges traversed exactly once.
        """
        node_names = [n.__name__ for n in self.all_nodes]
        return _build_transposed_adj(node_names, self.forward_adj)

    # ------------------------------------------------------------------

    def _fk_dimensions(
        self, fact_cls: type[FactModel]
    ) -> dict[str, type[DimensionModel]]:
        """Return {fk_col: DimensionClass} from FK metadata on the fact table."""
        star = self.get_star_schema(fact_cls)
        return star.get("dimensions", {})

    # ------------------------------------------------------------------
    # diff() — schema evolution diffing (Tier 4)
    # ------------------------------------------------------------------

    def diff(self, other: "SchemaGraph") -> "SchemaDiff":
        """
        Compute the graph edit distance between *self* (old schema) and
        *other* (new schema).

        Returns a :class:`SchemaDiff` describing what changed:

        * Added / removed vertices (dimensions)
        * Added / removed edges (facts)
        * Added / removed columns per model

        This fulfils the *"schema evolution diffing"* capability from
        Graph Analytics Tier 4 — enabling *"what changed between schema
        v2 and v3?"* queries.

        Parameters
        ----------
        other:
            The newer :class:`SchemaGraph` snapshot to compare against.

        Example
        -------
        .. code-block:: python

            sg_v2 = SchemaGraph([CustomerDim, SalesFact])
            sg_v3 = SchemaGraph([CustomerDim, ProductDim, SalesFact])
            diff = sg_v2.diff(sg_v3)
            print(diff.added_vertices)   # [ProductDim]
        """
        old_verts = {v.__name__: v for v in self.vertices}
        new_verts = {v.__name__: v for v in other.vertices}
        old_edges = {e.__name__: e for e in self.edges}
        new_edges = {e.__name__: e for e in other.edges}

        column_diffs: list[ColumnDiff] = []
        _collect_diffs_for(old_verts, new_verts, "vertex", column_diffs)
        _collect_diffs_for(old_edges, new_edges, "edge", column_diffs)

        return SchemaDiff(
            added_vertices=_added(old_verts, new_verts),
            removed_vertices=_added(new_verts, old_verts),
            added_edges=_added(old_edges, new_edges),
            removed_edges=_added(new_edges, old_edges),
            column_diffs=column_diffs,
        )


from sqldim.core.graph._schema_diff import ColumnDiff, SchemaDiff, _collect_column_diff  # noqa: E402, F401


def _added(absent_from: dict, present_in: dict) -> list:
    """Return values present in *present_in* but absent from *absent_from*."""
    return [present_in[n] for n in present_in if n not in absent_from]


def _collect_diffs_for(old: dict, new: dict, kind: str, out: list) -> None:
    for name in set(old) | set(new):
        _collect_column_diff(name, kind, old.get(name), new.get(name), out)
