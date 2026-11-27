"""Typed refusals of the sinks (#178).

A sink executes the contract of :class:`~sqldim.sinks.base.SinkAdapter`
exactly, or raises :class:`SinkUnsupported` before it writes anything.  It
never degrades in silence: it does not overwrite instead of appending,
return 0 instead of closing, or change a column's type.
"""

from __future__ import annotations

from sqldim.exceptions import SqldimError

__all__ = ["SinkUnsupported"]


class SinkUnsupported(SqldimError, NotImplementedError):
    """The sink cannot execute this operation exactly; nothing was written."""

    code = "UNSUPPORTED"
