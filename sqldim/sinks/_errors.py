"""Typed refusals of the sinks (#178).

A sink executes the contract of :class:`~sqldim.sinks.base.SinkAdapter`
exactly, or raises :class:`SinkUnsupported` before it writes anything.  It
never degrades in silence: it does not overwrite instead of appending,
return 0 instead of closing, or change a column's type.
"""

from __future__ import annotations

from sqldim.exceptions import SqldimError

__all__ = ["SinkNotOpen", "SinkUnsupported"]


class SinkUnsupported(SqldimError, NotImplementedError):
    """The sink cannot execute this operation exactly; nothing was written."""

    code = "UNSUPPORTED"


class SinkNotOpen(SqldimError, RuntimeError):
    """The sink's connection was read outside its ``with`` block (#192).

    A sink that attaches its storage to its own DuckDB connection opens it
    on ``__enter__`` and closes it on ``__exit__``; there is no connection
    to hand out before or after, and none is opened in its place.
    """

    code = "NOT_OPEN"
