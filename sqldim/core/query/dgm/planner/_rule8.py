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

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any

from sqldim.core.query.dgm.planner._targets import SinkTarget, UnsupportedSink

__all__ = [
    "ICEBERG_DOCUMENTED_VERSIONS",
    "IcebergCatalog",
    "SinkWritePlan",
    "WriteStatement",
    "plan_rule_8",
    "require_iceberg_catalog",
]

#: The DuckDB versions the §6.2 Iceberg capability table was read for.
ICEBERG_DOCUMENTED_VERSIONS: frozenset[str] = frozenset({"2.0", "2.0.1"})

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SCAN = re.compile(r"\b\w+_scan\b", re.I)
_FILE_SINKS = (SinkTarget.PARQUET, SinkTarget.DELTA, SinkTarget.ICEBERG)


class WriteStatement(str, Enum):
    """The statement form of a Rule 8 write (§6.2 Rule 8)."""

    CREATE_TABLE_AS_OR_INSERT = "CREATE TABLE AS / INSERT INTO"
    POSTGRES_INSERT = "ATTACH … (TYPE postgres); CREATE TABLE / INSERT INTO via COPY"
    COPY_TO_PARQUET = "COPY TO (FORMAT PARQUET)"
    DELTA_ATTACHED_INSERT = "ATTACH … (TYPE delta); INSERT INTO <attached table>"
    ICEBERG_CATALOG_INSERT = "INSERT INTO <catalog>.<schema>.<table>"


_STATEMENT: dict[SinkTarget, WriteStatement] = {
    SinkTarget.DUCKDB: WriteStatement.CREATE_TABLE_AS_OR_INSERT,
    SinkTarget.MOTHERDUCK: WriteStatement.CREATE_TABLE_AS_OR_INSERT,
    SinkTarget.POSTGRESQL: WriteStatement.POSTGRES_INSERT,
    SinkTarget.PARQUET: WriteStatement.COPY_TO_PARQUET,
    SinkTarget.DELTA: WriteStatement.DELTA_ATTACHED_INSERT,
    SinkTarget.ICEBERG: WriteStatement.ICEBERG_CATALOG_INSERT,
}


@dataclass(frozen=True)
class IcebergCatalog:
    """An Iceberg REST catalog attached in the session (§6.2 *Iceberg writes*).

    ``name`` is the alias it is attached under (``ATTACH … AS name (TYPE
    iceberg, …)``); ``engine_version`` the DuckDB version the plan targets;
    ``merge_on_read`` whether the target table's ``write.update.mode``,
    ``write.delete.mode`` and ``write.merge.mode`` are merge-on-read.
    """

    name: str
    engine_version: str
    merge_on_read: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not _IDENT.match(self.name):
            raise UnsupportedSink(f"invalid Iceberg catalog name {self.name!r}")
        if not isinstance(self.engine_version, str) or not self.engine_version:
            raise UnsupportedSink("an Iceberg catalog needs the engine version")
        if not isinstance(self.merge_on_read, bool):
            raise UnsupportedSink("merge_on_read must be a bool")

    @property
    def documented(self) -> bool:
        """Whether §6.2's Iceberg capability table covers ``engine_version``."""
        return self.engine_version in ICEBERG_DOCUMENTED_VERSIONS


@dataclass(frozen=True)
class SinkWritePlan:
    """The typed Rule 8 write plan for one sink (§6.2).

    ``target`` is the form of the relation written (``<attached Delta
    table>``, ``iceberg_cat.<schema>.<table>``); it never names a read-only
    ``*_scan`` function.  ``append_only`` is set when the sink admits no
    other write (Delta).
    """

    sink: SinkTarget
    statement: WriteStatement
    target: str
    partition_by_time_unit: bool = False
    append_only: bool = False
    catalog: IcebergCatalog | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.sink, SinkTarget):
            raise UnsupportedSink(f"unknown sink {self.sink!r}")
        if _STATEMENT[self.sink] is not self.statement:
            raise UnsupportedSink(
                f"{self.sink.value} is not written with {self.statement.value!r}"
            )
        if _SCAN.search(self.target) or _SCAN.search(self.statement.value):
            raise UnsupportedSink(
                f"{self.target!r}: a read-only *_scan function is never a write "
                "target (§6.2)"
            )
        if (self.sink is SinkTarget.ICEBERG) != (self.catalog is not None):
            raise UnsupportedSink("an ICEBERG write plan, and only one, has a catalog")

    def describe(self) -> str:
        """A one-line description, for logs and exports."""
        text = self.statement.value
        if self.partition_by_time_unit:
            text += "; PARTITION_BY timestamp_unit"
        if self.append_only:
            text += "; APPEND only"
        return text

    def to_dict(self) -> dict[str, Any]:
        return {
            "sink": self.sink.value,
            "statement": self.statement.value,
            "target": self.target,
            "partition_by_time_unit": self.partition_by_time_unit,
            "append_only": self.append_only,
            "catalog": None
            if self.catalog is None
            else {
                "name": self.catalog.name,
                "engine_version": self.catalog.engine_version,
                "merge_on_read": self.catalog.merge_on_read,
            },
        }


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
