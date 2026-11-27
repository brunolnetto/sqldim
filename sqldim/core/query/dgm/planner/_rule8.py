"""Planner Rule 8 — sink-aware write planning, as a typed plan (§6.2, #178).

Rule 8 returns a :class:`SinkWritePlan`: the statement form of the write,
its target, whether it is partitioned by timestamp unit and whether it is
APPEND only.  The target is always a writable relation; a read-only table
function (``delta_scan``, ``iceberg_scan``) is never a write target and a
plan naming one cannot be built.

* DELTA — ``ATTACH … (TYPE delta)`` then ``INSERT INTO`` the attached table,
  APPEND only (§6.2 *Delta writes*).
* ICEBERG — ``INSERT INTO <catalog>.<schema>.<table>`` on an attached REST
  catalog.  The catalog is declared with :class:`IcebergCatalog`; a plan
  with no catalog is rejected here, at planning time (§6.2 *Iceberg
  writes*).  Rule 10 also reads the catalog's ``engine_version`` and
  ``merge_on_read`` before it plans a MERGE.

A sink Rule 8 does not know raises :class:`UnsupportedSink`; nothing is
defaulted.
"""

from __future__ import annotations

from sqldim.core.query.dgm.planner._targets import (
    _STATEMENT,
    ICEBERG_DOCUMENTED_VERSIONS,
    IcebergCatalog,
    SinkTarget,
    SinkWritePlan,
    UnsupportedSink,
    WriteStatement,
)

__all__ = [
    "ICEBERG_DOCUMENTED_VERSIONS",
    "IcebergCatalog",
    "SinkWritePlan",
    "WriteStatement",
    "plan_rule_8",
    "require_iceberg_catalog",
]

_FILE_SINKS = (SinkTarget.PARQUET, SinkTarget.DELTA, SinkTarget.ICEBERG)


_TARGET: dict[SinkTarget, str] = {
    SinkTarget.DUCKDB: "<table>",
    SinkTarget.MOTHERDUCK: "<database>.<table>",
    SinkTarget.POSTGRESQL: "<attached postgres>.<schema>.<table>",
    SinkTarget.PARQUET: "<directory>",
    SinkTarget.DELTA: "<attached Delta table>",
}


def require_iceberg_catalog(catalog: IcebergCatalog | None) -> IcebergCatalog:
    """The catalog, or :class:`UnsupportedSink` when none is attached (§6.2)."""
    if not isinstance(catalog, IcebergCatalog):
        raise UnsupportedSink(
            "§6.2 Iceberg writes: an ICEBERG write plan needs an attached Iceberg "
            "REST catalog (IcebergCatalog); there is none, so it is rejected at "
            "planning time"
        )
    return catalog


def plan_rule_8(
    sink: SinkTarget,
    *,
    temporal_agg: bool,
    iceberg_catalog: IcebergCatalog | None = None,
) -> SinkWritePlan:
    """Rule 8: the :class:`SinkWritePlan` for *sink*."""
    if not isinstance(sink, SinkTarget):
        raise UnsupportedSink(
            f"Rule 8 has no write plan for sink {sink!r}; it is never defaulted "
            "to INSERT INTO"
        )
    catalog = None
    if sink is SinkTarget.ICEBERG:
        catalog = require_iceberg_catalog(iceberg_catalog)
        target = f"{catalog.name}.<schema>.<table>"
    else:
        target = _TARGET[sink]
    return SinkWritePlan(
        sink,
        _STATEMENT[sink],
        target,
        partition_by_time_unit=bool(temporal_agg) and sink in _FILE_SINKS,
        append_only=sink is SinkTarget.DELTA,
        catalog=catalog,
    )
