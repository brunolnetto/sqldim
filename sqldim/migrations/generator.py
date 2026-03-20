"""Dimensional-aware migration script generator.

Translates a :class:`~sqldim.migrations.context.DimensionalMigrationContext`
diff into a :class:`MigrationScript` containing Alembic-style ``upgrade``
and ``downgrade`` operations, backfill hints, and safety warnings.
"""

from datetime import datetime
from typing import Any, Type
from sqldim.migrations.context import (
    DimensionalMigrationContext,
    SchemaChange,
    CHANGE_SCD_UPGRADE,
    CHANGE_ADD_COLUMN,
    CHANGE_DROP_COLUMN,
    CHANGE_NK_CHANGE,
)
from sqldim.migrations.ops import add_backfill_hint, initialize_scd2_rows


def _apply_add_column(change: SchemaChange, script: "MigrationScript") -> None:
    """Append add-column upgrade/downgrade ops and a NULL-backfill hint.

    The downgrade simply drops the column; the backfill hint reminds the
    developer to populate ``NULL`` values before marking the column ``NOT NULL``.
    """
    table = change.details.get("table", "unknown")
    col = change.details["column"]
    script.upgrade_ops.append(
        f"op.add_column('{table}', sa.Column('{col}', sa.String()))"
    )
    script.downgrade_ops.append(f"op.drop_column('{table}', '{col}')")
    script.backfill_hints.append(
        add_backfill_hint(table, col, f"Backfill '{col}' before marking NOT NULL")
    )


def _apply_drop_column(change: SchemaChange, script: "MigrationScript") -> None:
    """Append drop-column ops and a data-loss warning to *script*.

    The warning is recorded before the ops so it surfaces first when
    the caller prints the generated migration.
    """
    table = change.details.get("table", "unknown")
    col = change.details["column"]
    script.warnings.append(
        f"DROP COLUMN '{col}' from '{table}' — historical data will be lost."
    )
    script.upgrade_ops.append(f"op.drop_column('{table}', '{col}')")
    script.downgrade_ops.append(
        f"op.add_column('{table}', sa.Column('{col}', sa.String()))"
    )


def _apply_scd_upgrade(change: SchemaChange, script: "MigrationScript") -> None:
    """Append the four SCD-2 columns (valid_from/to, is_current, checksum).

    Also emits the ``initialize_scd2_rows`` backfill op so existing rows
    receive a ``valid_from`` timestamp and ``is_current = TRUE``.
    """
    table = change.details.get("table", "unknown")
    for col_def in [
        "'valid_from', sa.DateTime()",
        "'valid_to', sa.DateTime(), nullable=True",
        "'is_current', sa.Boolean()",
        "'checksum', sa.String(32), nullable=True",
    ]:
        script.upgrade_ops.append(f"op.add_column('{table}', sa.Column({col_def}))")
    script.upgrade_ops.append(initialize_scd2_rows(table))
    for col_name in ("valid_from", "valid_to", "is_current", "checksum"):
        script.downgrade_ops.append(f"op.drop_column('{table}', '{col_name}')")


def _apply_nk_change(change: SchemaChange, script: "MigrationScript") -> None:
    """Emit a natural-key rename warning and commented index stubs.

    Appends a human-readable warning to *script.warnings* noting the old and
    new NK column names, and adds commented-out ``CREATE INDEX`` / ``DROP INDEX``
    stub lines to the upgrade and downgrade blocks.
    """
    table = change.details.get("table", "unknown")
    old_nk = change.details["from"]
    new_nk = change.details["to"]
    script.warnings.append(
        f"Natural key changed from {old_nk} → {new_nk} on '{table}'. "
        "Verify indexes and FK references before applying."
    )
    script.upgrade_ops.append(f"# op.drop_index('ix_{table}_{'_'.join(old_nk)}')")
    script.upgrade_ops.append(
        f"# op.create_index('ix_{table}_{'_'.join(new_nk)}', '{table}', {new_nk})"
    )


_CHANGE_HANDLERS = {
    CHANGE_ADD_COLUMN: _apply_add_column,
    CHANGE_DROP_COLUMN: _apply_drop_column,
    CHANGE_SCD_UPGRADE: _apply_scd_upgrade,
    CHANGE_NK_CHANGE: _apply_nk_change,
}


def _apply_change_to_script(change: SchemaChange, script: "MigrationScript") -> None:
    handler = _CHANGE_HANDLERS.get(change.change_type)
    if handler:
        handler(change, script)


class MigrationScript:
    """Represents a generated dimensional migration."""

    def __init__(self, message: str, changes: list[SchemaChange]):
        self.message = message
        self.created_at = datetime.now()
        self.changes = changes
        self.upgrade_ops: list[str] = []
        self.downgrade_ops: list[str] = []
        self.backfill_hints: list[Any] = []
        self.warnings: list[str] = []

    def render(self) -> str:
        """Render the script as a Python string with ``upgrade`` / ``downgrade`` functions."""
        lines = [
            f'"""Migration: {self.message}"""',
            f"# Generated: {self.created_at.isoformat()}",
            "",
            "def upgrade():",
        ]
        lines += self._render_ops_block(self.upgrade_ops)
        lines += ["", "def downgrade():"]
        lines += self._render_ops_block(self.downgrade_ops)
        if self.warnings:
            lines += ["", "# WARNINGS:"]
            for w in self.warnings:
                lines.append(f"# ⚠️  {w}")
        return "\n".join(lines)

    def _render_ops_block(self, ops: list[str]) -> list[str]:
        """Return indented op lines, or a single ``pass`` if there are none."""
        if ops:
            return [f"    # {op}" for op in ops]
        return ["    pass"]


def generate_migration(
    models: list[Type],
    current_state: dict[str, Any],
    message: str = "auto migration",
) -> MigrationScript:
    """
    Generate a dimensional-aware migration script from a schema diff.
    """
    ctx = DimensionalMigrationContext(models)
    changes = ctx.diff(current_state)
    script = MigrationScript(message=message, changes=changes)
    for change in changes:
        _apply_change_to_script(change, script)
    return script
