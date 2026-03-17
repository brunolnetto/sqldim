"""Dimensional-aware schema diff context.

Detects column additions and removals, SCD-type upgrades / downgrades,
and natural-key renames between a set of :class:`DimensionModel` /
:class:`FactModel` definitions and the current database state.
"""
from typing import Any, Dict, List, Optional, Type
from sqldim.core.kimball.models import DimensionModel, FactModel
from sqldim.core.kimball.schema_graph import SchemaGraph

# Change types detected by dimensional diff
CHANGE_ADD_COLUMN = "add_column"
CHANGE_DROP_COLUMN = "drop_column"
CHANGE_SCD_UPGRADE = "scd_upgrade"     # Type 1 → 2
CHANGE_SCD_DOWNGRADE = "scd_downgrade" # Type 2 → 1 (destructive)
CHANGE_NK_CHANGE = "natural_key_change"

class SchemaChange:
    """Represents a single detected schema change with its type, owning model, and detail dict.

    The ``is_destructive`` flag is set automatically for DROP_COLUMN and
    SCD_DOWNGRADE change types so callers can gate on safety before applying.
    """

    def __init__(self, change_type: str, model: Type, details: Dict[str, Any]):
        """Initialise the change record, flagging destructive operations automatically.

        Sets ``is_destructive = True`` for change types that delete data or
        downgrade SCD complexity (DROP_COLUMN, SCD_DOWNGRADE).
        """
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

    def _detect_column_changes(
        self, model: Type, table_name: str, model_cols: set, current_cols: set
    ) -> List[SchemaChange]:
        """Return add-column and drop-column changes between the model and current column sets.

        Compares *model_cols* against *current_cols* and emits one
        ADD_COLUMN ``SchemaChange`` per new column and one DROP_COLUMN per
        column that was removed.
        """
        changes = []
        for col in model_cols - current_cols:
            changes.append(SchemaChange(CHANGE_ADD_COLUMN, model, {"column": col, "table": table_name}))
        for col in current_cols - model_cols:
            changes.append(SchemaChange(CHANGE_DROP_COLUMN, model, {"column": col, "table": table_name}))
        return changes

    def _detect_scd_upgrade(self, current_scd: int, new_scd: int, model, table_name: str):
        """Return an SCD-type change when *current_scd* and *new_scd* differ, else None."""
        if current_scd == 1 and new_scd == 2:
            return SchemaChange(CHANGE_SCD_UPGRADE, model, {"from": 1, "to": 2, "table": table_name})
        if current_scd == 2 and new_scd == 1:
            return SchemaChange(CHANGE_SCD_DOWNGRADE, model, {"from": 2, "to": 1, "table": table_name})
        return None

    def _detect_scd_change(
        self, model: Type, table_name: str, state: Dict[str, Any]
    ) -> Optional[SchemaChange]:
        """Return an SCD change for dimension models; always None for fact models."""
        if not issubclass(model, DimensionModel):
            return None
        return self._detect_scd_upgrade(
            state.get("scd_type", 1),
            getattr(model, "__scd_type__", 1),
            model,
            table_name,
        )

    def _detect_nk_change(
        self, model: Type, table_name: str, state: Dict[str, Any]
    ) -> Optional[SchemaChange]:
        """Return a natural-key-change record when the NK column set has shifted, else None."""
        if not issubclass(model, DimensionModel):
            return None
        current_nk = set(state.get("natural_key", []))
        new_nk = set(getattr(model, "__natural_key__", []))
        if current_nk and current_nk != new_nk:
            return SchemaChange(CHANGE_NK_CHANGE, model, {"from": list(current_nk), "to": list(new_nk), "table": table_name})
        return None

    def _diff_model(self, model: Type, current_state: Dict[str, Any]) -> List[SchemaChange]:
        """Return all SchemaChange objects for a single model against its current-state entry."""
        table_name = model.__tablename__ if hasattr(model, "__tablename__") else model.__name__.lower()
        if table_name not in current_state:
            return []
        state = current_state[table_name]
        model_cols = set(model.model_fields.keys())
        changes = list(self._detect_column_changes(model, table_name, model_cols, set(state.get("columns", []))))
        scd = self._detect_scd_change(model, table_name, state)
        if scd:
            changes.append(scd)
        nk = self._detect_nk_change(model, table_name, state)
        if nk:
            changes.append(nk)
        return changes

    def diff(self, current_state: Dict[str, Any]) -> List[SchemaChange]:
        """Diff all registered models against *current_state* and return changes sorted by severity."""
        changes: List[SchemaChange] = []
        for model in self.models:
            changes.extend(self._diff_model(model, current_state))
        return sorted(changes, key=lambda c: c.is_destructive)
