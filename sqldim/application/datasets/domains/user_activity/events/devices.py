"""Device-source drift events for the user_activity domain."""

from __future__ import annotations

import duckdb


def apply_mobile_migration(con: duckdb.DuckDBPyConnection) -> None:
    """Re-classify all Tablet devices as Mobile.

    **OLTP mutation** — updates ``devices.device_type = 'Mobile'`` for every
    row currently classified as ``'Tablet'``.  Any NL query that counts or
    filters by device type will reflect the new distribution immediately.
    """
    con.execute(
        "UPDATE devices SET device_type = 'Mobile' WHERE device_type = 'Tablet'"
    )


def apply_os_migration(con: duckdb.DuckDBPyConnection) -> None:
    """Re-classify all Windows devices as macOS (fleet OS migration).

    **OLTP mutation** — updates ``devices.os_type = 'macOS'`` for every row
    currently on ``'Windows'``.  OS-distribution queries will show the shift.
    """
    con.execute(
        "UPDATE devices SET os_type = 'macOS' WHERE os_type = 'Windows'"
    )


__all__ = ["apply_mobile_migration", "apply_os_migration"]
