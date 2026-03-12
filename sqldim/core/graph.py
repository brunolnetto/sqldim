from typing import List, Type, Dict, Any, Optional
from sqldim.core.models import DimensionModel, FactModel


class RolePlayingRef:
    """Represents a dimension playing a named role in a fact table."""
    def __init__(self, dimension: Type[DimensionModel], role: str, fk_column: str):
        self.dimension = dimension
        self.role = role
        self.fk_column = fk_column

    def __repr__(self) -> str:
        return f"RolePlayingRef(dim={self.dimension.__name__}, role={self.role!r}, fk={self.fk_column!r})"


class SchemaGraph:
    def __init__(self, models: List[Type[Any]]):
        self.models = models
        self.dimensions = [m for m in models if issubclass(m, DimensionModel)]
        self.facts = [m for m in models if issubclass(m, FactModel)]

    @classmethod
    def from_models(cls, models: List[Type[Any]]) -> "SchemaGraph":
        return cls(models)

    def get_star_schema(self, fact_model: Type[FactModel]) -> Dict[str, Any]:
        relationships = {}
        for name, field in fact_model.model_fields.items():
            dim = None

            # Check for dimension in field attributes (pydantic side)
            if hasattr(field, "json_schema_extra") and field.json_schema_extra:
                dim = field.json_schema_extra.get("dimension")

            # Check for dimension in sa_column (SQLAlchemy side)
            if not dim and hasattr(fact_model, "__table__"):
                column = fact_model.__table__.columns.get(name)
                if column is not None and column.info:
                    dim = column.info.get("dimension")

            if dim:
                relationships[name] = dim
        return {
            "fact": fact_model,
            "dimensions": relationships
        }

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
            if col.info:
                role = col.info.get("role")
                dim = col.info.get("dimension")
                if role and dim:
                    refs.append(RolePlayingRef(
                        dimension=dim,
                        role=role,
                        fk_column=col.name,
                    ))
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
            lines.append(f"    {dim.__name__} {{")
            for col_name, field in dim.model_fields.items():
                col_type = "string"
                annotation = dim.__annotations__.get(col_name)
                if annotation in (int,):
                    col_type = "int"
                elif annotation in (float,):
                    col_type = "float"
                elif annotation in (bool,):
                    col_type = "bool"
                lines.append(f"        {col_type} {col_name}")
            lines.append("    }")

        for fact in self.facts:
            lines.append(f"    {fact.__name__} {{")
            for col_name, field in fact.model_fields.items():
                col_type = "string"
                annotation = fact.__annotations__.get(col_name)
                if annotation in (int,):
                    col_type = "int"
                elif annotation in (float,):
                    col_type = "float"
                lines.append(f"        {col_type} {col_name}")
            lines.append("    }")

        # Relationships
        for fact in self.facts:
            star = self.get_star_schema(fact)
            for fk_col, dim in star["dimensions"].items():
                lines.append(f"    {fact.__name__} }}o--||  {dim.__name__} : \"{fk_col}\"")

        return "\n".join(lines)
