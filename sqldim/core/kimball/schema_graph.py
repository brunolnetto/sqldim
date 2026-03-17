"""Schema graph utilities for star/snowflake dimension resolution.

Provides :class:`SchemaGraph` — a lightweight model registry that maps
fact tables to their dimension relationships (including role-playing
dimensions) and can render Mermaid ER diagrams.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from sqldim.core.kimball.models import DimensionModel, FactModel
else:
    from sqldim.core.kimball.models import DimensionModel, FactModel


def _col_dimension(fact_model: Type, name: str) -> Any:
    """Return the dimension class wired into *name*'s SA column ``info`` dict.

    Returns ``None`` if the column does not exist or carries no ``dimension``
    key in its ``info`` mapping.
    """
    if not hasattr(fact_model, "__table__"):
        return None
    column = fact_model.__table__.columns.get(name)
    if column is not None and column.info:
        return column.info.get("dimension")
    return None


def _resolve_field_dim(fact_model: Type, name: str, field: Any) -> Any:
    """Return the dimension class for *name* via json_schema_extra or SA column info.

    Checks ``json_schema_extra`` first (SQLModel field metadata) and falls back
    to the SA ``column.info`` dict so both declaration styles are supported.
    """
    if hasattr(field, "json_schema_extra") and field.json_schema_extra:
        dim = field.json_schema_extra.get("dimension")
        if dim:
            return dim
    return _col_dimension(fact_model, name)


def _annotation_type(annotation: Any) -> str:
    """Map a Python type annotation to a Mermaid-compatible type string."""
    if annotation is int:
        return "int"
    if annotation is float:
        return "float"
    if annotation is bool:
        return "bool"
    return "string"


def _render_mermaid_model(model: Type, lines: list) -> None:
    """Append Mermaid entity lines for *model* to *lines* in place."""
    lines.append(f"    {model.__name__} {{")
    for col_name in model.model_fields:
        col_type = _annotation_type(model.__annotations__.get(col_name))
        lines.append(f"        {col_type} {col_name}")
    lines.append("    }")


def _role_ref_from_col(col: Any) -> "RolePlayingRef | None":
    """Extract a RolePlayingRef from a SA column's ``info`` dict, or return None."""
    if not col.info:
        return None
    role = col.info.get("role")
    dim = col.info.get("dimension")
    if role and dim:
        return RolePlayingRef(dimension=dim, role=role, fk_column=col.name)
    return None


class RolePlayingRef:
    """Represents a dimension playing a named role in a fact table.

    For example, a ``DateDimension`` joined twice as ``departure_date`` and
    ``arrival_date`` would produce two :class:`RolePlayingRef` instances.
    """
    def __init__(self, dimension: Type[DimensionModel], role: str, fk_column: str):
        self.dimension = dimension
        self.role = role
        self.fk_column = fk_column

    def __repr__(self) -> str:
        return f"RolePlayingRef(dim={self.dimension.__name__}, role={self.role!r}, fk={self.fk_column!r})"


class SchemaGraph:
    """Registry mapping fact tables to their dimension relationships in a star or snowflake schema."""

    def __init__(self, models: List[Type[Any]]):
        """Partition *models* into dimension and fact lists for downstream graph queries."""
        self.models = models
        self.dimensions = [m for m in models if issubclass(m, DimensionModel)]
        self.facts = [m for m in models if issubclass(m, FactModel)]

    @classmethod
    def from_models(cls, models: List[Type[Any]]) -> "SchemaGraph":
        """Construct a SchemaGraph from an iterable of model classes."""
        return cls(models)

    def get_star_schema(self, fact_model: Type[FactModel]) -> Dict[str, Any]:
        """Return the star-schema for *fact_model* as a dict mapping FK column names to dimension classes."""
        relationships = {
            name: _resolve_field_dim(fact_model, name, field)
            for name, field in fact_model.model_fields.items()
            if _resolve_field_dim(fact_model, name, field) is not None
        }
        return {"fact": fact_model, "dimensions": relationships}

    def get_role_playing_dimensions(self, fact_model: Type[FactModel]) -> List[RolePlayingRef]:
        """
        Returns all role-playing dimension references in a fact table.
        A role-playing dimension is when the same physical dimension is joined
        multiple times under different logical roles (e.g. departure_date, arrival_date).
        """
        refs = []
        if not hasattr(fact_model, "__table__"):
            return refs
        for col in fact_model.__table__.columns:
            ref = _role_ref_from_col(col)
            if ref is not None:
                refs.append(ref)
        return refs

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation of the full schema graph."""
        result: Dict[str, Any] = {"facts": [], "dimensions": []}

        for dim in self.dimensions:
            result["dimensions"].append({
                "name": dim.__name__,
                "natural_key": getattr(dim, "__natural_key__", []),
                "scd_type": getattr(dim, "__scd_type__", 1),
                "columns": list(dim.model_fields.keys()),
            })

        for fact in self.facts:
            star = self.get_star_schema(fact)
            roles = self.get_role_playing_dimensions(fact)
            result["facts"].append({
                "name": fact.__name__,
                "grain": getattr(fact, "__grain__", None),
                "dimensions": {
                    fk: dim.__name__ for fk, dim in star["dimensions"].items()
                },
                "role_playing": [
                    {"fk": r.fk_column, "dimension": r.dimension.__name__, "role": r.role}
                    for r in roles
                ],
                "columns": list(fact.model_fields.keys()),
            })

        return result

    def to_mermaid(self) -> str:
        """Render the schema as a Mermaid ER diagram string."""
        lines = ["erDiagram"]
        for dim in self.dimensions:
            _render_mermaid_model(dim, lines)
        for fact in self.facts:
            _render_mermaid_model(fact, lines)
        for fact in self.facts:
            star = self.get_star_schema(fact)
            for fk_col, dim in star["dimensions"].items():
                lines.append(f"    {fact.__name__} }}o--||  {dim.__name__} : \"{fk_col}\"")
        return "\n".join(lines)
