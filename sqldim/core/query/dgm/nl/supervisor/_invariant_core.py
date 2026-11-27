"""DGM Supervisor — shared machinery of the invariant evaluators (§12.3, D43).

:class:`Evaluator` holds the connection, the declared model, the witness
tally and the cancel token of one ``dgm_validate_invariant`` call.  The
evaluators themselves live in ``_invariant_eval`` (§2.1, §2.6) and
``_invariant_pa`` (§2.4).

Fail closed: an evaluator either examines every element of its scope and
reports what it found, or raises :class:`NotEvaluable` with the matching
``ToolErrorKind``.  Nothing is sampled, and an element that cannot be read
is never counted as satisfying the invariant.

Identifiers come from the declared model, never from request arguments; they
are still quoted, and Enum values are emitted as escaped SQL literals.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import duckdb

from sqldim.core.query.dgm.model import (
    DGMModel,
    Derived,
    EdgeType,
    ForeignKey,
    Holder,
    NodeTable,
    NodeType,
    PropType,
    Reference,
    RelationTable,
)
from sqldim.core.query.dgm.nl.supervisor._types import CancelToken, ToolErrorKind

__all__ = [
    "EdgeSql",
    "Evaluation",
    "Evaluator",
    "NotEvaluable",
    "col",
    "conforms",
    "ident",
    "lit",
    "plain",
]


class NotEvaluable(Exception):
    """The invariant could not be evaluated completely (FAILED, never holds)."""

    def __init__(self, kind: ToolErrorKind, message: str, **context: Any) -> None:
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.context = context


@dataclass
class Evaluation:
    """The complete evaluation of one invariant (``ValidateInvariantOutput``)."""

    checked: int = 0
    violation_count: int = 0
    violations: list[dict[str, Any]] = field(default_factory=list)

    @property
    def holds(self) -> bool:
        return self.violation_count == 0

    def as_output(self) -> dict[str, Any]:
        return {
            "holds": self.holds,
            "checked": self.checked,
            "violation_count": self.violation_count,
            "violations": list(self.violations),
        }


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------


def ident(name: str) -> str:
    """Quote a (possibly schema-qualified) relation or column name."""
    return ".".join('"' + part.replace('"', '""') + '"' for part in name.split("."))


def col(alias: str, name: str) -> str:
    return f"{alias}." + '"' + name.replace('"', '""') + '"'


def lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def plain(value: Any) -> Any:
    """A witness value as JSON-compatible data."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    return str(value)


_INTEGER = frozenset(
    {
        "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
        "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
    }
)  # fmt: skip
_TIMESTAMP = frozenset(
    {
        "TIMESTAMP", "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS",
    }
)  # fmt: skip


def conforms(column_type: str, declared: PropType) -> bool:
    """§2.6: the column type conforms to ``K_S(T)(k)``."""
    t = column_type.upper()
    if declared.name == "Integer":
        return t in _INTEGER
    if declared.name == "Real":
        return t in _INTEGER or t in {"FLOAT", "DOUBLE"} or t.startswith("DECIMAL")
    if declared.name == "Enum":
        return t == "VARCHAR" or t.startswith("ENUM(")
    if declared.name == "Timestamp":
        return t in _TIMESTAMP
    return (
        t == {"String": "VARCHAR", "Boolean": "BOOLEAN", "Date": "DATE"}[declared.name]
    )


def describe(declared: PropType) -> str:
    if declared.name == "Enum":
        return f"Enum({', '.join(declared.values)})"
    return declared.name


# ---------------------------------------------------------------------------
# Edge instances (§2.6, *The instance graph as a view*)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EdgeSql:
    """``E_I`` of one edge type as SQL, subject to M5.

    One output row per mapping row that yields an edge instance, with columns
    ``s_0…`` (source entity key), ``t_0…`` (target entity key) and ``p_0…``
    (the mapped edge properties, in the order of ``props``).
    """

    sql: str
    n_source: int
    n_target: int
    props: tuple[str, ...]


@dataclass(frozen=True)
class _End:
    select: list[str]
    join: str = ""
    where: str = "TRUE"


def _unsupported(message: str, **context: Any) -> NotEvaluable:
    return NotEvaluable(ToolErrorKind.UNSUPPORTED, message, **context)


def _referenced_end(
    ref: str,
    columns: Sequence[str],
    table: NodeTable,
    reference: Reference,
    tag: str,
    edge: EdgeType,
) -> _End:
    """The endpoint a stored reference resolves to (M5, M6)."""
    rel = ident(table.relation)
    if reference is Reference.VERSION_KEY:
        if table.version_key is None or len(columns) != 1:
            raise _unsupported(
                f"edge type {edge.key}: a VERSION_KEY reference needs one column "
                "and a referenced NodeTable with a version_key (M6)",
                edge=edge.key,
            )
        join = (
            f"JOIN {rel} {tag} ON {col(tag, table.version_key)} = "
            f"{col(ref, columns[0])}"
        )
        return _End([col(tag, k) for k in table.entity_key], join=join)
    if len(columns) != len(table.entity_key):
        raise _unsupported(
            f"edge type {edge.key}: {len(columns)} reference column(s) for an "
            f"entity_key of {len(table.entity_key)}",
            edge=edge.key,
        )
    match = " AND ".join(
        f"{col(tag, k)} = {col(ref, c)}" for k, c in zip(table.entity_key, columns)
    )
    return _End(
        [col(ref, c) for c in columns],
        where=f"EXISTS (SELECT 1 FROM {rel} {tag} WHERE {match})",
    )


def _own_end(alias: str, table: NodeTable) -> _End:
    return _End([col(alias, k) for k in table.entity_key])


def _assemble(base: str, src: _End, tgt: _End, props: dict[str, str]) -> EdgeSql:
    select = [f"{e} AS s_{i}" for i, e in enumerate(src.select)]
    select += [f"{e} AS t_{i}" for i, e in enumerate(tgt.select)]
    select += [f"{col('h', c)} AS p_{i}" for i, c in enumerate(props.values())]
    sql = (
        f"SELECT {', '.join(select)} FROM {base} h {src.join} {tgt.join} "
        f"WHERE {src.where} AND {tgt.where}"
    )
    return EdgeSql(sql, len(src.select), len(tgt.select), tuple(props))


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------


class Evaluator:
    """State of one evaluation: data, model, tally and cancellation."""

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection | None,
        model: DGMModel,
        max_witnesses: int,
        cancel: CancelToken | None = None,
        now: datetime | None = None,
    ) -> None:
        self._con = con
        self.model = model
        self.max_witnesses = max_witnesses
        self.cancel = cancel or CancelToken()
        self.now = now
        self.result = Evaluation()
        self._columns: dict[str, dict[str, str]] = {}

    # -- tally ---------------------------------------------------------------

    def examine(self, n: int = 1) -> None:
        self.check_cancel()
        self.result.checked += n

    def violate(self, witness: dict[str, Any]) -> None:
        self.violate_many(1, [witness])

    def violate_many(self, count: int, witnesses: Sequence[dict[str, Any]]) -> None:
        """Count *count* violations; keep witnesses up to ``max_witnesses``."""
        self.result.violation_count += count
        room = self.room
        self.result.violations.extend(witnesses[:room])

    @property
    def room(self) -> int:
        return max(self.max_witnesses - len(self.result.violations), 0)

    def check_cancel(self) -> None:
        if self.cancel.cancelled:
            raise NotEvaluable(
                ToolErrorKind.CANCELLED,
                "the run was cancelled during the evaluation",
                checked=self.result.checked,
            )

    # -- data ----------------------------------------------------------------

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:  # pragma: no cover — schema invariants read no data
            raise RuntimeError("this invariant reads data but no connection is open")
        return self._con

    def rows(self, sql: str) -> list[tuple[Any, ...]]:
        self.check_cancel()
        return self.con.execute(sql).fetchall()

    def scalar(self, sql: str) -> Any:
        row = self.rows(sql)[0]
        return row[0]

    def count(self, from_where: str) -> int:
        return int(self.scalar(f"SELECT COUNT(*) {from_where}"))

    def sample(self, select: str, from_where: str) -> list[tuple[Any, ...]]:
        """Up to ``room`` rows for witnesses (never a substitute for a count)."""
        if self.room == 0:
            return []
        return self.rows(f"SELECT {select} {from_where} LIMIT {self.room}")

    def columns(self, relation: str) -> dict[str, str]:
        """Column name → DuckDB type of *relation* (from the catalog)."""
        if relation not in self._columns:
            described = self.rows(f"DESCRIBE SELECT * FROM {ident(relation)}")
            self._columns[relation] = {str(r[0]): str(r[1]) for r in described}
        return self._columns[relation]

    def column_type(self, relation: str, column: str, owner: str) -> str:
        found = self.columns(relation).get(column)
        if found is None:
            raise _unsupported(
                f"{owner}: column {column!r} is not in relation {relation!r}",
                relation=relation,
                column=column,
            )
        return found

    # -- model ---------------------------------------------------------------

    def node_table(self, name: str, owner: str) -> NodeTable:
        node = self.model.node(name)
        if node is None or node.mapping is None:
            raise _unsupported(
                f"{owner}: node type {name!r} has no NodeTable, so its instances "
                "cannot be read (M1)",
                node_type=name,
            )
        return node.mapping

    def mapped_nodes(self) -> list[tuple[NodeType, NodeTable]]:
        """Every node type with its NodeTable; an unmapped one is fatal (M1)."""
        return [
            (n, self.node_table(n.name, f"node type {n.name!r}"))
            for n in self.model.node_types
        ]

    def edge_sql(self, edge: EdgeType) -> EdgeSql:
        """``E_I`` of *edge* (§2.6), or UNSUPPORTED if it cannot be derived."""
        mapping = edge.mapping
        if mapping is None:
            raise _unsupported(
                f"edge type {edge.key} has no mapping, so its instances cannot be "
                "read (M1)",
                edge=edge.key,
            )
        src = self.node_table(edge.source, f"edge type {edge.key}")
        tgt = self.node_table(edge.target, f"edge type {edge.key}")
        if isinstance(mapping, ForeignKey):
            return self._fk_sql(edge, mapping, src, tgt)
        if isinstance(mapping, RelationTable):
            return _assemble(
                ident(mapping.relation),
                _referenced_end(
                    "h", mapping.source, src, mapping.references, "e_s", edge
                ),
                _referenced_end(
                    "h", mapping.target, tgt, mapping.references, "e_t", edge
                ),
                dict(mapping.props),
            )
        return self._derived_sql(edge, mapping, src, tgt)

    @staticmethod
    def _fk_sql(
        edge: EdgeType, fk: ForeignKey, src: NodeTable, tgt: NodeTable
    ) -> EdgeSql:
        if fk.holder is Holder.TARGET:
            return _assemble(
                ident(tgt.relation),
                _referenced_end("h", fk.columns, src, fk.references, "e_s", edge),
                _own_end("h", tgt),
                {},
            )
        return _assemble(
            ident(src.relation),
            _own_end("h", src),
            _referenced_end("h", fk.columns, tgt, fk.references, "e_t", edge),
            {},
        )

    @staticmethod
    def _derived_sql(
        edge: EdgeType, view: Derived, src: NodeTable, tgt: NodeTable
    ) -> EdgeSql:
        return _assemble(
            ident(view.view),
            _referenced_end("h", view.source, src, Reference.ENTITY_KEY, "e_s", edge),
            _referenced_end("h", view.target, tgt, Reference.ENTITY_KEY, "e_t", edge),
            {},
        )
