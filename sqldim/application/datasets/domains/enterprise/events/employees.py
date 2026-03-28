"""Employee-source drift events for the enterprise domain."""

from __future__ import annotations

import duckdb


def apply_reorg(con: duckdb.DuckDBPyConnection) -> None:
    """Rename the HR department to Operations (organisational restructuring).

    **OLTP mutation** — updates ``employees.department = 'Operations'`` for
    every employee currently in ``'HR'``.  Department-distribution queries
    will reflect the change immediately.
    """
    con.execute(
        "UPDATE employees SET department = 'Operations' WHERE department = 'HR'"
    )


__all__ = ["apply_reorg"]
