from datetime import datetime
from typing import Any, Dict, List, Optional, Type
from sqldim.migrations.context import DimensionalMigrationContext, SchemaChange, CHANGE_SCD_UPGRADE, CHANGE_ADD_COLUMN, CHANGE_DROP_COLUMN, CHANGE_NK_CHANGE
from sqldim.migrations.ops import add_backfill_hint, initialize_scd2_rows

class MigrationScript:
    """Represents a generated dimensional migration."""

    def __init__(self, message: str, changes: List[SchemaChange]):
        self.message = message
        self.created_at = datetime.now()
        self.changes = changes
        self.upgrade_ops: List[str] = []
        self.downgrade_ops: List[str] = []
        self.backfill_hints: List[Any] = []
        self.warnings: List[str] = []

    def render(self) -> str:
        lines = [
            f'"""Migration: {self.message}"""',
            f"# Generated: {self.created_at.isoformat()}",
            "",
            "def upgrade():",
        ]
        if self.upgrade_ops:
            for op in self.upgrade_ops:
                lines.append(f"    # {op}")
        else:
            lines.append("    pass")

        lines += ["", "def downgrade():"]
        if self.downgrade_ops:
            for op in self.downgrade_ops:
                lines.append(f"    # {op}")
        else:
            lines.append("    pass")

        if self.warnings:
            lines += ["", "# WARNINGS:"]
            for w in self.warnings:
                lines.append(f"# ⚠️  {w}")

        return "\n".join(lines)


def generate_migration(
    models: List[Type],
    current_state: Dict[str, Any],
    message: str = "auto migration",
) -> MigrationScript:
    """
    Generate a dimensional-aware migration script from a schema diff.
    """
    ctx = DimensionalMigrationContext(models)
    changes = ctx.diff(current_state)
    script = MigrationScript(message=message, changes=changes)

    for change in changes:
        table = change.details.get("table", "unknown")

        if change.change_type == CHANGE_ADD_COLUMN:
            col = change.details["column"]
            script.upgrade_ops.append(f"op.add_column('{table}', sa.Column('{col}', sa.String()))")
            script.downgrade_ops.append(f"op.drop_column('{table}', '{col}')")
            script.backfill_hints.append(
                add_backfill_hint(table, col, f"Backfill '{col}' before marking NOT NULL")
            )

        elif change.change_type == CHANGE_DROP_COLUMN:
            col = change.details["column"]
            script.warnings.append(
                f"DROP COLUMN '{col}' from '{table}' — historical data will be lost."
            )
            script.upgrade_ops.append(f"op.drop_column('{table}', '{col}')")
            script.downgrade_ops.append(f"op.add_column('{table}', sa.Column('{col}', sa.String()))")

        elif change.change_type == CHANGE_SCD_UPGRADE:
            script.upgrade_ops.append(f"op.add_column('{table}', sa.Column('valid_from', sa.DateTime()))")
            script.upgrade_ops.append(f"op.add_column('{table}', sa.Column('valid_to', sa.DateTime(), nullable=True))")
            script.upgrade_ops.append(f"op.add_column('{table}', sa.Column('is_current', sa.Boolean()))")
            script.upgrade_ops.append(f"op.add_column('{table}', sa.Column('checksum', sa.String(32), nullable=True))")
            script.upgrade_ops.append(initialize_scd2_rows(table))
            script.downgrade_ops.append(f"op.drop_column('{table}', 'valid_from')")
            script.downgrade_ops.append(f"op.drop_column('{table}', 'valid_to')")
            script.downgrade_ops.append(f"op.drop_column('{table}', 'is_current')")
            script.downgrade_ops.append(f"op.drop_column('{table}', 'checksum')")

        elif change.change_type == CHANGE_NK_CHANGE:
            old_nk = change.details["from"]
            new_nk = change.details["to"]
            script.warnings.append(
                f"Natural key changed from {old_nk} → {new_nk} on '{table}'. "
                "Verify indexes and FK references before applying."
            )
            script.upgrade_ops.append(f"# op.drop_index('ix_{table}_{'_'.join(old_nk)}')")
            script.upgrade_ops.append(f"# op.create_index('ix_{table}_{'_'.join(new_nk)}', '{table}', {new_nk})")

    return script
