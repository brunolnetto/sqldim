"""Dimensional-aware schema diff context.

Detects column additions and removals, SCD-type upgrades / downgrades,
and natural-key renames between a set of :class:`DimensionModel` /
:class:`FactModel` definitions and the current database state.
"""

import re
from typing import Any, Type
from sqldim.core.kimball.models import DimensionModel
from sqldim.core.kimball.schema_graph import SchemaGraph

# Change types detected by dimensional diff
CHANGE_ADD_COLUMN = "add_column"
CHANGE_DROP_COLUMN = "drop_column"
CHANGE_SCD_UPGRADE = "scd_upgrade"  # Type 1 → 2
CHANGE_SCD_DOWNGRADE = "scd_downgrade"  # Type 2 → 1 (destructive)
CHANGE_NK_CHANGE = "natural_key_change"


class SchemaChange:
    """Represents a single detected schema change with its type, owning model, and detail dict.

    The ``is_destructive`` flag is set automatically for DROP_COLUMN and
    SCD_DOWNGRADE change types so callers can gate on safety before applying.
    """

    def __init__(self, change_type: str, model: Type, details: dict[str, Any]):
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

    def __init__(self, models: list[type[Any]]):
        self.models = models
        self.graph = SchemaGraph.from_models(models)

    def _detect_column_changes(
        self, model: Type, table_name: str, model_cols: set, current_cols: set
    ) -> list[SchemaChange]:
        """Return add-column and drop-column changes between the model and current column sets.

        Compares *model_cols* against *current_cols* and emits one
        ADD_COLUMN ``SchemaChange`` per new column and one DROP_COLUMN per
        column that was removed.
        """
        changes = []
        for col in model_cols - current_cols:
            changes.append(
                SchemaChange(
                    CHANGE_ADD_COLUMN, model, {"column": col, "table": table_name}
                )
            )
        for col in current_cols - model_cols:
            changes.append(
                SchemaChange(
                    CHANGE_DROP_COLUMN, model, {"column": col, "table": table_name}
                )
            )
        return changes

    def _detect_scd_upgrade(
        self, current_scd: int, new_scd: int, model, table_name: str
    ):
        """Return an SCD-type change when *current_scd* and *new_scd* differ, else None."""
        if current_scd == 1 and new_scd == 2:
            return SchemaChange(
                CHANGE_SCD_UPGRADE, model, {"from": 1, "to": 2, "table": table_name}
            )
        if current_scd == 2 and new_scd == 1:
            return SchemaChange(
                CHANGE_SCD_DOWNGRADE, model, {"from": 2, "to": 1, "table": table_name}
            )
        return None

    def _detect_scd_change(
        self, model: Type, table_name: str, state: dict[str, Any]
    ) -> SchemaChange | None:
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
        self, model: Type, table_name: str, state: dict[str, Any]
    ) -> SchemaChange | None:
        """Return a natural-key-change record when the NK column set has shifted, else None."""
        if not issubclass(model, DimensionModel):
            return None
        current_nk = set(state.get("natural_key", []))
        new_nk = set(getattr(model, "__natural_key__", []))
        if current_nk and current_nk != new_nk:
            return SchemaChange(
                CHANGE_NK_CHANGE,
                model,
                {"from": list(current_nk), "to": list(new_nk), "table": table_name},
            )
        return None

    def _diff_model(
        self, model: Type, current_state: dict[str, Any]
    ) -> list[SchemaChange]:
        """Return all SchemaChange objects for a single model against its current-state entry."""
        table_name = (
            model.__tablename__
            if hasattr(model, "__tablename__")
            else model.__name__.lower()
        )
        if table_name not in current_state:
            return []
        state = current_state[table_name]
        model_cols = set(model.model_fields.keys())
        changes = list(
            self._detect_column_changes(
                model, table_name, model_cols, set(state.get("columns", []))
            )
        )
        scd = self._detect_scd_change(model, table_name, state)
        if scd:
            changes.append(scd)
        nk = self._detect_nk_change(model, table_name, state)
        if nk:
            changes.append(nk)
        return changes

    def diff(self, current_state: dict[str, Any]) -> list[SchemaChange]:
        """Diff all registered models against *current_state* and return changes sorted by severity."""
        changes: list[SchemaChange] = []
        for model in self.models:
            changes.extend(self._diff_model(model, current_state))
        return sorted(changes, key=lambda c: c.is_destructive)

    # ------------------------------------------------------------------
    # Live-DB introspection
    # ------------------------------------------------------------------

    #: SCD-2 is inferred when all three sentinel columns are present in the table.
    _SCD2_INDICATOR_COLS: frozenset = frozenset(
        {"valid_from", "valid_to", "is_current"}
    )

    @staticmethod
    def _parse_nk_from_index_sql(sql: str) -> list[str]:
        """Extract column names from a CREATE INDEX SQL statement."""
        m = re.search(r"\(([^)]+)\)", sql)
        if m:
            return [c.strip() for c in m.group(1).split(",")]
        return []

    def _introspect_natural_key(self, con: Any, table_name: str, model) -> list[str]:
        """Read NK columns from the DuckDB index; fall back to the model's ``__natural_key__``."""
        nk: list[str] = list(getattr(model, "__natural_key__", []))
        try:
            idx_rows = con.execute(
                "SELECT sql FROM duckdb_indexes() "
                "WHERE table_name = ? AND index_name = ?",
                [table_name, f"ix_{table_name}_nk"],
            ).fetchall()
            if idx_rows and idx_rows[0][0]:
                parsed = self._parse_nk_from_index_sql(idx_rows[0][0])
                if parsed:
                    nk = parsed
        except Exception:
            pass
        return nk

    def _introspect_table(self, con: Any, model) -> tuple | None:
        """Introspect one model's table; return ``(table_name, state_dict)`` or ``None``."""
        table_name = getattr(model, "__tablename__", model.__name__.lower())
        try:
            rows = con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = ?",
                [table_name],
            ).fetchall()
        except Exception:
            return None
        if not rows:
            return None  # table does not exist yet
        cols = {r[0] for r in rows}
        scd_type = 2 if self._SCD2_INDICATOR_COLS.issubset(cols) else 1
        nk = self._introspect_natural_key(con, table_name, model)
        return table_name, {
            "columns": sorted(cols),
            "scd_type": scd_type,
            "natural_key": nk,
        }

    def introspect(self, con: Any) -> dict[str, Any]:
        """Build a ``current_state`` dict by querying the live database.

        Uses ``information_schema.columns`` (supported by DuckDB and
        PostgreSQL).  For each registered model whose table already exists,
        the method records:

        * ``columns`` — the set of column names currently in the table.
        * ``scd_type`` — ``2`` when the three SCD-2 sentinel columns
          (``valid_from``, ``valid_to``, ``is_current``) are all present,
          otherwise ``1``.
        * ``natural_key`` — best-effort: tries to read the NK index
          (``ix_{table}_nk``) from DuckDB's ``duckdb_indexes()`` catalog;
          falls back to the model's declared ``__natural_key__`` for other
          backends.

        Tables that do not yet exist in the database are silently skipped
        (they require a CREATE, not an ALTER, which is handled outside
        this context).

        Parameters
        ----------
        con:
            An open database connection whose ``.execute()`` method accepts
            ``(sql, params)`` with ``?`` positional placeholders (DuckDB
            native API or any DBAPI-2 compatible driver using ``?``).

        Returns
        -------
        dict[str, Any]
            A ``current_state`` dict suitable for passing directly to
            :meth:`diff`.
        """
        current_state: dict[str, Any] = {}
        for model in self.models:
            result = self._introspect_table(con, model)
            if result is not None:
                table_name, state = result
                current_state[table_name] = state
        return current_state

    def diff_from_connection(self, con: Any) -> list[SchemaChange]:
        """Introspect *con* and diff all registered models against the live schema.

        Convenience wrapper around :meth:`introspect` + :meth:`diff`.
        Suitable for use at application start-up to detect outstanding
        migrations without any manually maintained ``current_state`` dict.

        Parameters
        ----------
        con:
            An open database connection (see :meth:`introspect`).

        Returns
        -------
        list[SchemaChange]
            Schema changes sorted by severity (non-destructive first).
        """
        return self.diff(self.introspect(con))
