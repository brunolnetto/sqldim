"""Shared DuckDB connection factory with resource configuration.

All sqldim sinks call :func:`make_connection` in their ``__enter__`` methods
instead of creating a bare ``duckdb.connect()`` so that memory limits,
spill-to-disk, and ordering settings are applied consistently across all
backends.
"""

from __future__ import annotations

import os

import duckdb


def _default_memory_limit() -> str:
    """Return 70 % of system RAM as a DuckDB-style string, or ``'4GB'`` as fallback."""
    try:
        import psutil  # type: ignore[import-untyped]

        total_gb = psutil.virtual_memory().total / (1024**3)
        return f"{total_gb * 0.70:.0f}GB"
    except (ImportError, Exception):
        return "4GB"


def make_connection(
    memory_limit: str | None = None,
    temp_directory: str | None = None,
    threads: int | None = None,
    preserve_insertion_order: bool = False,
) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB in-memory connection with sensible production defaults.

    Parameters
    ----------
    memory_limit:
        DuckDB memory ceiling, e.g. ``'7GB'``.  Defaults to 60 % of system
        RAM (via psutil) or ``'4GB'`` if psutil is not installed.
        Counter-intuitively, keeping this below physical RAM lets DuckDB
        spill intermediate results rather than using unconstrained mmap
        which can circumvent the buffer manager.
    temp_directory:
        Spill-to-disk path.  Defaults to the ``SQLDIM_TEMP_DIR`` env var, or
        ``/tmp/sqldim_spill``.  Required for beyond-memory workloads —
        without it DuckDB raises instead of spilling.
    threads:
        Worker thread count.  Defaults to CPU core count.  For remote sources
        (S3, httpfs) raise to 2-5× CPU cores since DuckDB uses synchronous IO
        (one request per thread).
    preserve_insertion_order:
        ``False`` (default) allows DuckDB to reorder output, reducing memory
        pressure significantly at xl/xxl tier.  Safe for all sqldim operations
        which never rely on insertion order.
    """
    resolved_limit = memory_limit or os.environ.get(
        "SQLDIM_MEMORY_LIMIT", _default_memory_limit()
    )
    resolved_tmp = temp_directory or os.environ.get(
        "SQLDIM_TEMP_DIR", "/tmp/sqldim_spill"
    )

    os.makedirs(resolved_tmp, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{resolved_limit}'")
    con.execute(f"SET temp_directory = '{resolved_tmp}'")
    con.execute(
        f"SET preserve_insertion_order = {'true' if preserve_insertion_order else 'false'}"
    )
    if threads is not None:
        con.execute(f"SET threads = {threads}")
    return con


class OwnsConnection:
    """The public ``connection`` of a sink that opens its own (#192).

    The sink attaches its storage to a DuckDB connection on ``__enter__``.
    Lazy loaders and processors given the sink and no ``con`` write through
    this connection; pass it explicitly as ``con=sink.connection`` to share
    it with your own statements.
    """

    _con: duckdb.DuckDBPyConnection | None

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """The sink's open connection; :class:`SinkNotOpen` outside ``with``."""
        from sqldim.sinks._errors import SinkNotOpen

        if self._con is None:
            raise SinkNotOpen(
                f"{type(self).__name__} has no open connection: use it as "
                f"`with {type(self).__name__}(...) as sink:` and read "
                "sink.connection inside the block"
            )
        return self._con


def connection_for(
    sink: object, con: duckdb.DuckDBPyConnection | None
) -> duckdb.DuckDBPyConnection:
    """The connection a lazy loader or processor writes through (#192).

    *con* when given.  Otherwise the sink's own :attr:`OwnsConnection.connection`
    (raising :class:`SinkNotOpen` if the sink is not open), since a fresh
    connection would not see the sink's attached storage.  A sink that owns
    no connection (``ParquetSink``, a custom sink) gets a new in-memory one,
    as it reads and writes its storage by path.
    """
    if con is not None:
        return con
    if isinstance(sink, OwnsConnection):
        return sink.connection
    return duckdb.connect()
