from typing import Any, Dict, List, Optional, Type
from sqldim.core.models import DimensionModel, FactModel
from sqldim.core.graph import SchemaGraph

# Change types detected by dimensional diff
CHANGE_ADD_COLUMN = "add_column"
CHANGE_DROP_COLUMN = "drop_column"
CHANGE_SCD_UPGRADE = "scd_upgrade"     # Type 1 → 2
CHANGE_SCD_DOWNGRADE = "scd_downgrade" # Type 2 → 1 (destructive)
CHANGE_NK_CHANGE = "natural_key_change"

class SchemaChange:
    def __init__(self, change_type: str, model: Type, details: Dict[str, Any]):
        self.change_type = change_type
        self.model = model
        self.details = details
        self.is_destructive = change_type in (CHANGE_DROP_COLUMN, CHANGE_SCD_DOWNGRADE)

    def __repr__(self) -> str:
        return f"SchemaChange({self.change_type!r}, model={self.model.__name__}, details={self.details})"


class DimensionalMigrationContext:
    """
    Dimensional-aware schema diff engine.
    Compares a set of registered models against the current DB state
    and produces a list of SchemaChange objects with dimensional context.
    """

    def __init__(self, models: List[Type[Any]]):
        self.models = models
        self.graph = SchemaGraph.from_models(models)

    def diff(self, current_state: Dict[str, Any]) -> List[SchemaChange]:
        """
        Compute schema changes between the current model definitions
        and the provided current_state snapshot.

        current_state format:
        {
            "TableName": {
                "columns": ["col1", "col2", ...],
                "scd_type": 1 | 2,
                "natural_key": ["col_name"],
            }
        }

        Returns a list of SchemaChange objects sorted by severity
        (non-destructive first, destructive last).
        """
        changes: List[SchemaChange] = []

        for model in self.models:
            table_name = model.__tablename__ if hasattr(model, "__tablename__") else model.__name__.lower()
            if table_name not in current_state:
                continue  # New table — CREATE TABLE, handled by Alembic core

            state = current_state[table_name]
            current_cols = set(state.get("columns", []))
            model_cols = set(model.model_fields.keys())

            # Detect added columns
            for col in model_cols - current_cols:
                changes.append(SchemaChange(
                    CHANGE_ADD_COLUMN, model,
                    {"column": col, "table": table_name}
                ))

            # Detect dropped columns
            for col in current_cols - model_cols:
                changes.append(SchemaChange(
                    CHANGE_DROP_COLUMN, model,
                    {"column": col, "table": table_name}
                ))

            # Detect SCD type transitions (only for DimensionModel)
            if issubclass(model, DimensionModel):
                current_scd = state.get("scd_type", 1)
                new_scd = getattr(model, "__scd_type__", 1)

                if current_scd == 1 and new_scd == 2:
                    changes.append(SchemaChange(
                        CHANGE_SCD_UPGRADE, model,
                        {"from": 1, "to": 2, "table": table_name}
                    ))
                elif current_scd == 2 and new_scd == 1:
                    changes.append(SchemaChange(
                        CHANGE_SCD_DOWNGRADE, model,
                        {"from": 2, "to": 1, "table": table_name}
                    ))

            # Detect natural key changes
            if issubclass(model, DimensionModel):
                current_nk = set(state.get("natural_key", []))
                new_nk = set(getattr(model, "__natural_key__", []))
                if current_nk and current_nk != new_nk:
                    changes.append(SchemaChange(
                        CHANGE_NK_CHANGE, model,
                        {"from": list(current_nk), "to": list(new_nk), "table": table_name}
                    ))

        # Sort: non-destructive first, destructive last
        return sorted(changes, key=lambda c: c.is_destructive)
