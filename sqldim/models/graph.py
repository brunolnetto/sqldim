"""
Graph extension for sqldim — Phase 6 / TD-001.

VertexModel and EdgeModel are *optional* convenience wrappers.
Any DimensionModel is implicitly a vertex; any FactModel is implicitly an edge.
__vertex_type__, __edge_type__, __subject__, __object__ are optional metadata
annotations — not required base classes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar, Optional, TYPE_CHECKING

from sqldim.core.models import DimensionModel, FactModel
from sqldim.exceptions import SchemaError

if TYPE_CHECKING:
    pass


# ---------------------------------------------------------------------------
# Lightweight transfer object — decoupled from SQLModel
# ---------------------------------------------------------------------------

@dataclass
class Vertex:
    """Lightweight representation of a vertex for traversal results."""
    id: int
    type: str
    properties: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"Vertex(id={self.id}, type={self.type!r})"


# ---------------------------------------------------------------------------
# VertexModel — optional convenience wrapper over DimensionModel
# ---------------------------------------------------------------------------

class VertexModel(DimensionModel):
    """
    A DimensionModel with optional graph metadata.

    ``__vertex_type__`` is an optional annotation that labels the vertex.
    When absent, ``vertex_type()`` falls back to the lower-cased class name.

    Note: Any plain ``DimensionModel`` is also implicitly a graph vertex —
    inheriting ``VertexModel`` is not required.

    Class variables (all optional)
    --------------------------------
    __vertex_type__      : str — logical label (e.g. "player"); defaults to class name
    __vertex_properties__ : list[str] — columns to expose; empty = all columns
    """
    __vertex_type__: ClassVar[str]
    __vertex_properties__: ClassVar[list[str]] = []

    # No __init_subclass__ enforcement — __vertex_type__ is optional.

    @classmethod
    def vertex_type(cls) -> str:
        vt = getattr(cls, "__vertex_type__", None)
        if vt is None:
            raise SchemaError(f"{cls.__name__} has no __vertex_type__ defined.")
        return vt

    @classmethod
    def as_vertex(cls, instance: "VertexModel") -> Vertex:
        """Convert an ORM instance into a lightweight Vertex dataclass."""
        vt = getattr(cls, "__vertex_type__", cls.__name__.lower())
        props_keys = cls.__vertex_properties__ or list(cls.model_fields.keys())
        props = {k: getattr(instance, k, None) for k in props_keys}
        return Vertex(id=getattr(instance, "id"), type=vt, properties=props)


# ---------------------------------------------------------------------------
# EdgeModel — optional convenience wrapper over FactModel
# ---------------------------------------------------------------------------

class EdgeModel(FactModel):
    """
    A FactModel with optional graph metadata.

    ``__edge_type__``, ``__subject__``, and ``__object__`` are optional
    annotations.  When absent:
    - ``edge_type()`` raises ``SchemaError`` (explicit declaration still needed
      when you call the classmethod directly).
    - ``is_self_referential()`` returns ``False``.

    Note: Any plain ``FactModel`` is also implicitly a graph edge —
    inheriting ``EdgeModel`` is not required.

    Class variables (all optional)
    --------------------------------
    __edge_type__  : str — logical label (e.g. "plays_in")
    __subject__    : type[DimensionModel] — subject vertex class
    __object__     : type[DimensionModel] — object vertex class
    __directed__   : bool — True (default) = directed; False = undirected
    """
    __edge_type__: ClassVar[str]
    __subject__: ClassVar[type[DimensionModel]]
    __object__: ClassVar[type[DimensionModel]]
    __directed__: ClassVar[bool] = True

    # No __init_subclass__ enforcement — all attributes are optional.

    @classmethod
    def edge_type(cls) -> str:
        et = getattr(cls, "__edge_type__", None)
        if et is None:
            raise SchemaError(f"{cls.__name__} has no __edge_type__ defined.")
        return et

    @classmethod
    def is_self_referential(cls) -> bool:
        """True when subject and object vertex types are the same class."""
        s = getattr(cls, "__subject__", None)
        o = getattr(cls, "__object__", None)
        return s is not None and s is o
