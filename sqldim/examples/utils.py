"""
sqldim/examples/utils.py
==========================
Utility helpers for sqldim showcase scripts.

These utilities eliminate repetitive boilerplate that appears in every
showcase (temp-file creation, section headers, dimension lifecycle
management) so showcase code can focus on demonstrating the library.

Quick reference
---------------
``tmp_db()``
    Context manager that yields a temp DuckDB file path and auto-deletes
    it on exit (WAL file too).  Preferred over ``make_tmp_db()``.

``make_tmp_db()``
    Returns a temp DuckDB path.  Caller owns the file and must unlink it.

``setup_dim(con, src, table)``
    Call ``src.setup(con, table)``; tolerates sources with no ``setup()``.

``teardown_dim(con, src, table)``
    Call ``src.teardown(con, table)``; falls back to ``DROP TABLE IF EXISTS``.

``with section(title):``
    Context manager that prints a formatted section header on entry.

``banner(title, subtitle)``
    Print a framed banner (used at the top of each showcase).

``print_rows(rows, headers, *, indent)``
    Pretty-print a result set.

``show_provider(src)``
    Print the data-source description for a ``BaseSource`` instance.
"""
from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from typing import Any

import duckdb

__all__ = [
    "tmp_db",
    "make_tmp_db",
    "setup_dim",
    "teardown_dim",
    "section",
    "banner",
    "print_rows",
    "show_provider",
]


# ── Temp database ─────────────────────────────────────────────────────────────

@contextmanager
def tmp_db():
    """
    Context manager yielding a temporary DuckDB file path.

    The file is deleted on context exit (both the ``.duckdb`` file and any
    WAL companion).

    Usage::

        with tmp_db() as path:
            con = duckdb.connect(path)
            ...
    """
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        path = f.name
    os.unlink(path)  # DuckDB requires the file to not exist yet
    try:
        yield path
    finally:
        for p in (path, path + ".wal"):
            if os.path.exists(p):
                try:
                    os.unlink(p)
                except OSError:
                    pass


def make_tmp_db() -> str:
    """
    Return a fresh temporary DuckDB file path (no context manager).

    The caller is responsible for deleting the file when done.
    Prefer ``tmp_db()`` when possible to guarantee cleanup.
    """
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        path = f.name
    os.unlink(path)
    return path


# ── Dimension lifecycle ───────────────────────────────────────────────────────

def setup_dim(
    con: duckdb.DuckDBPyConnection,
    src: Any,
    table: str,
) -> None:
    """
    Call ``src.setup(con, table)`` if the method exists.

    Tolerates source objects that do not implement ``setup()``, in which
    case this is a no-op.
    """
    if hasattr(src, "setup"):
        src.setup(con, table)


def teardown_dim(
    con: duckdb.DuckDBPyConnection,
    src: Any,
    table: str,
) -> None:
    """
    Call ``src.teardown(con, table)`` if the method exists, otherwise
    execute ``DROP TABLE IF EXISTS <table>``.
    """
    if hasattr(src, "teardown"):
        src.teardown(con, table)
    else:
        con.execute(f"DROP TABLE IF EXISTS {table}")


# ── Console output helpers ────────────────────────────────────────────────────

class section:
    """
    Context manager that prints a labelled section header on entry.

    Usage::

        with section("Example 1: Products (SCD2)"):
            ...
    """

    _WIDTH = 64

    def __init__(self, title: str) -> None:
        self._title = title

    def __enter__(self) -> "section":
        pad = max(0, self._WIDTH - len(self._title) - 4)
        print(f"\n── {self._title} {'─' * pad}")
        return self

    def __exit__(self, *_) -> None:
        pass


def banner(title: str, subtitle: str = "") -> None:
    """
    Print a framed banner suitable for the top of a showcase module.

    Usage::

        banner("SCD Types Showcase", "Examples 1 – 4")
    """
    width = max(len(title), len(subtitle)) + 4
    print("=" * width)
    print(f"  {title}")
    if subtitle:
        print(f"  {subtitle}")
    print("=" * width)


def print_rows(
    rows: list[tuple],
    headers: list[str] | None = None,
    *,
    indent: int = 2,
    col_width: int = 20,
) -> None:
    """
    Pretty-print a result set.

    Parameters
    ----------
    rows     : List of tuples returned by DuckDB ``fetchall()``.
    headers  : Optional column names for the header row.
    indent   : Number of leading spaces.
    col_width: Minimum column width.
    """
    pad = " " * indent
    if headers:
        header_line = "  ".join(str(h).ljust(col_width) for h in headers)
        print(f"{pad}{header_line}")
        print(f"{pad}{'-' * len(header_line)}")
    for row in rows:
        line = "  ".join(str(v).ljust(col_width) for v in row)
        print(f"{pad}{line}")


def show_provider(src: Any) -> None:
    """
    Print the data-source description for a ``BaseSource`` instance.

    No-op if *src* does not have a ``describe_provider()`` method.
    """
    if hasattr(src, "describe_provider"):
        print(src.describe_provider())
    elif hasattr(src, "provider") and src.provider is not None:
        print(src.provider.describe())
