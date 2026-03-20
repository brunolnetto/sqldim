

class BackfillHint:
    """Metadata record instructing operators to backfill a column before making it NOT NULL.

    Created by :func:`add_backfill_hint` and collected in the migration
    script’s ``backfill_hints`` list so operators can act on them before
    applying the generated ``ALTER TABLE`` statement.
    """

    def __init__(self, table: str, column: str, note: str):
        self.table = table
        self.column = column
        self.note = note

    def __repr__(self) -> str:
        return f"BackfillHint(table={self.table!r}, column={self.column!r}, note={self.note!r})"


def add_backfill_hint(table: str, column: str, note: str) -> BackfillHint:
    """Register a backfill hint for a newly added nullable column.

    Returns a :class:`BackfillHint` containing *table*, *column*, and a
    human-readable *note* describing the required pre-upgrade backfill.
    """
    hint = BackfillHint(table=table, column=column, note=note)
    return hint


def initialize_scd2_rows(
    table: str,
    valid_from_default: str = "1970-01-01",
    valid_to_default: str | None = None,
) -> str:
    """
    Generate the SQL UPDATE statement to initialize existing rows as current
    SCD2 versions when upgrading a Type 1 → Type 2 dimension.
    """
    valid_to_expr = f"'{valid_to_default}'" if valid_to_default else "NULL"
    return (
        f"UPDATE {table} SET "
        f"valid_from = '{valid_from_default}', "
        f"valid_to = {valid_to_expr}, "
        f"is_current = TRUE "
        f"WHERE valid_from IS NULL"
    )
