"""The declared model, anchor sets and annotations (§2.1, §2.4, §2.6, §3.4.6).

* ``dgm-model`` — ``DGMModel`` construction, ``partition_problems``,
  ``validate_anchor_set`` and the question-space scope ``build_space``;
* ``dgm-annotations`` — what an annotation object decides by itself
  (``Grain.rejects_sum`` for SUM, AD1–AD2).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from sqldim.core.query.dgm.analytical_space import validate_anchor_set
from sqldim.core.query.dgm.annotations import (
    Conformed,
    Grain,
    GrainKind,
    MedallionLayer,
    MedallionLayerKind,
    RolePlaying,
    SCDKind,
    SCDType,
)
from sqldim.core.query.dgm.lattice._space import build_space
from sqldim.core.query.dgm.model import (
    DGMModel,
    EdgeKind,
    EdgeType,
    ModelDeclarationError,
    NodeKind,
    NodeType,
)
from tests.conformance.cases import Case
from tests.conformance.cases.anchors import SCHEMAS
from tests.conformance.runners import REJECTED, Backend, NotExpressible

KINDS = frozenset(
    {"Conformed", "FactlessFact", "Grain", "MedallionLayer", "RolePlaying", "SCDType"}
)

_BUILD: dict[str, Callable[[tuple], Any]] = {
    "Conformed": lambda a: Conformed(a[0], set(a[1:])),
    "Grain": lambda a: Grain(a[0], GrainKind.EVENT),
    "MedallionLayer": lambda a: MedallionLayer(a[0], MedallionLayerKind.GOLD),
    "SCDType": lambda a: SCDType(a[0], SCDKind.SCD2),
}


def _nodes(kinds: Mapping[str, str], names: Any = None) -> list[NodeType]:
    return [NodeType(n, NodeKind(kinds[n])) for n in (names or kinds)]


def _edges(edges: Any) -> list[EdgeType]:
    return [EdgeType(s, t, label, EdgeKind(kind)) for s, t, label, kind in edges]


def model_of(schema: Mapping[str, Any]) -> tuple[DGMModel, dict[Any, str]]:
    """The DGMModel of a schema fixture and the display name of each annotation."""
    names: dict[Any, str] = {}
    for display, named_nodes, _labels in schema.get("sigma", ()):
        names[_BUILD[display.split("(")[0]](named_nodes)] = display
    roles = [RolePlaying(d, list(rs)) for d, rs in schema.get("roles", {}).items()]
    model = DGMModel(
        node_types=tuple(_nodes(schema["nodes"])),
        edge_types=tuple(_edges(schema["edges"])),
        sigma=tuple(names) + tuple(roles),
        sigma_kinds=KINDS,
    )
    return model, names


_TAGS = (("AS1:", "AS1"), ("AS2:", "AS2"), ("AS3:", "AS3"), ("at least two", "K"),
         ("listed twice", "DUP"), ("not a node type", "UNKNOWN"))  # fmt: skip


def _valid(case: Case) -> Any:
    model, _ = model_of(SCHEMAS[case.fixture])
    i = case.input
    msgs = validate_anchor_set(
        model, i["anchors"], max_hops=i["h"], max_anchors=i["max_anchors"]
    )
    return sorted({tag for m in msgs for key, tag in _TAGS if key in m})


def _space(part: str) -> Callable[[Case], Any]:
    def run(case: Case) -> Any:
        model, names = model_of(SCHEMAS[case.fixture])
        i = case.input
        space = build_space(model, i["anchors"], h=i["h"], max_anchors=i["max_anchors"])
        if part == "nodes":
            return sorted(space.nodes)
        if part == "edges":
            return sorted((e.source, e.label, e.target) for e in space.edges)
        return sorted(names[a] for a in space.sigma if a in names)

    return run


_DECL: dict[str, Callable[[tuple], Any]] = {
    "Conformed": lambda a: Conformed(a[0], set(a[1])),
    "Grain": lambda a: Grain(a[0], GrainKind.EVENT),
    "SCDType": lambda a: SCDType(a[0], SCDKind.SCD2),
}


def _declaration(case: Case) -> Any:
    i = case.input
    try:
        DGMModel(
            node_types=tuple(_nodes(i["nodes"], i.get("node_list"))),
            edge_types=tuple(_edges(i["edges"])),
            sigma=tuple(_DECL[k](args) for k, args in i["sigma"]),
            sigma_kinds=KINDS,
        )
    except ModelDeclarationError:
        return REJECTED
    return "ACCEPTED"


def _partition(case: Case) -> Any:
    i = case.input
    model = DGMModel(node_types=tuple(_nodes(i["nodes"])), sigma_kinds=KINDS)
    s, t, label, kind = i["edge"]
    return bool(model.partition_problems(EdgeType(s, t, label, EdgeKind(kind))))


_MODEL: dict[str, Callable[[Case], Any]] = {
    "anchor_valid": _valid,
    "anchor_nodes": _space("nodes"),
    "anchor_edges": _space("edges"),
    "anchor_sigma": _space("sigma"),
    "declaration": _declaration,
    "declaration_partition": _declaration,
    "partition": _partition,
}

MODEL = Backend(
    "dgm-model",
    "DGMModel, validate_anchor_set and build_space (G_𝒜)",
    frozenset(_MODEL),
    lambda case: _MODEL[case.area](case),
)


def _additivity(case: Case) -> Any:
    i = case.input
    if i.get("kind") is not None or i["fn"] != "SUM":
        raise NotExpressible(
            "sqldim has no Additivity annotation; Grain decides SUM only"
        )
    invalid = Grain("AccountBalance", GrainKind[i["grain"]]).rejects_sum(
        group_by_axes=i.get("group_by_axes", ()),
        where_pins=i.get("where_pins", ()),
        window=i.get("window"),
    )
    return REJECTED if invalid else "VALID"


ANNOTATIONS = Backend(
    "dgm-annotations",
    "Grain.rejects_sum (AD1–AD2: is SUM rejected at construction)",
    frozenset({"additivity"}),
    _additivity,
)
