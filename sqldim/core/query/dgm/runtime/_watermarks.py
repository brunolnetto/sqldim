"""Watermark readers for the result cache of §8.14 (D45).

A :class:`~sqldim.core.query.dgm.runtime._dag.WatermarkReader` answers
``w(src)`` for one dependency of ``deps(plan)``: a value that changes whenever
a committed change to the source becomes visible and never otherwise, or
``None`` when the source has none (*unversioned*, D41).

Per-backend premise (the table of §8.14 *Reading watermarks*):

=====================  ===============================================  ==========
Backend                Watermark                                        Pinned read
=====================  ===============================================  ==========
Delta table            table version                                    yes
Iceberg (REST)         current snapshot id                              yes
DuckDB native table    loader commit counter (:class:`CommitCounters`)  no — rule 2
PostgreSQL table       as DuckDB                                        no — rule 2
Parquet / CSV files    none; file times are not trusted                 unversioned
=====================  ===============================================  ==========

The cache of this package never pins reads; it always applies rule 2 of D45
(read ``W`` again after execution and store only if it is unchanged), which
is valid for every row above.  Monotonicity is the host's contract: a reader
must return ``None`` for a source whose watermark can move without a commit or
fail to move on one.  This module cannot check that from a single read.

Fail closed: a reader that cannot read the watermark table raises
:class:`WatermarkReadError`; it never answers ``None`` for "could not read",
which would turn an outage into an unversioned source.
"""

from __future__ import annotations

from typing import Any

from sqldim.core.query.dgm.planner._ir import check_identifier
from sqldim.core.query.dgm.planner._ir_identity import Dependency
from sqldim.core.query.dgm.runtime._errors import NotConfigured, RuntimeAPIError

__all__ = ["WatermarkReadError", "CommitCounters"]


class WatermarkReadError(RuntimeAPIError):
    """A watermark could not be read; nothing is served or stored (D45)."""

    code = "WATERMARK"


class CommitCounters:
    """Watermarks from a loader-maintained commit counter table (DuckDB, PostgreSQL).

    The table has the columns ``relation VARCHAR``, ``partition VARCHAR``
    (``NULL`` for the relation as a whole) and ``version BIGINT``.  The
    loader increments the row of the relation, and the row of every partition
    it changed, in the same transaction as the change (§8.14).

    * A dependency without partitions reads the relation row.
    * A dependency with partitions reads one row per listed partition, so a
      load into a partition outside ``deps(plan)`` leaves the key unchanged.
      If any listed partition has no row, or none is listed, the relation row
      is used instead: it advances on every commit to the relation, so it is
      coarser but never misses a change.
    * No applicable row: the source is unversioned (``None``).
    """

    def __init__(self, connection: Any, table: str = "sqldim_watermark") -> None:
        if connection is None:
            raise NotConfigured("CommitCounters needs a connection")
        self._con = connection
        self._table = check_identifier(table, "watermark table")

    def _rows(self, relation: str) -> dict[str | None, int]:
        sql = f"SELECT partition, version FROM {self._table} WHERE relation = ?"
        try:
            rows = self._con.execute(sql, [relation]).fetchall()
        except Exception as exc:  # noqa: BLE001 — reported as a typed failure
            raise WatermarkReadError(
                f"cannot read the watermark of {relation!r}: {exc}"
            ) from exc
        out: dict[str | None, int] = {}
        for part, version in rows:
            if isinstance(version, bool) or not isinstance(version, int):
                raise WatermarkReadError(
                    f"watermark of {relation!r} is not an integer: {version!r}"
                )
            if part in out:
                raise WatermarkReadError(
                    f"watermark table has two rows for {relation!r}/{part!r}"
                )
            out[part] = version
        return out

    def watermark(self, dep: Dependency) -> str | int | None:
        rows = self._rows(dep.relation)
        parts = dep.partitions
        if parts and all(p in rows for p in parts):
            return ";".join(f"{p}={rows[p]}" for p in parts)
        return rows.get(None)
