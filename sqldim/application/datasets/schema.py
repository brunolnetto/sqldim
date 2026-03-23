"""
sqldim/examples/datasets/schema.py
=====================================
Schema-driven synthetic data generation.

Design
------
``FieldSpec``    — declares a column: SQL type *and* how to synthesise values
                  from that same definition.  Vocabulary (choices, ranges) lives
                  here, not as scattered module-level constants.

``EntitySchema`` — a collection of ``FieldSpec`` objects.  Generates both DDL
                  (OLTP and SCD dimension variants) and synthetic row dicts
                  without any hardcoded SQL string literals.

``ChangeRule``   — one field-mutation rule for an event batch, expressed as two
                  callables (condition + mutate) so the intent stays readable.

``EventSpec``    — collects ``ChangeRule`` objects and produces event batches.

Supported ``FieldSpec`` kinds
-----------------------------
seq       : sequential integer       (start, step)
faker     : Faker method call        (method; optional pattern for numerify/
                                      lexify; optional transform str)
choices   : random.choice            (choices list)
uniform   : random.uniform float     (low, high; optional precision)
randint   : random.randint integer   (low, high)
const     : fixed value              (value)
computed  : arbitrary callable       (fn(fake, i) -> Any)

All ``FieldSpec`` instances may supply an optional ``post`` callable
``(value, fake, i) -> Any`` applied after the core generation step.

Fields with ``sql_export=False`` appear in generate() output and DDL but are
*not* included in ``to_sql()`` — useful for internal tracking columns like
``updated_at`` that sqldim does not consume.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any

# ── constants ─────────────────────────────────────────────────────────────────

_STRING_SQL: frozenset[str] = frozenset(
    {"VARCHAR", "TEXT", "TIMESTAMP", "DATE", "CHAR"}
)

_SCD_AUDIT: list[tuple[str, str]] = [
    ("valid_from", "VARCHAR"),
    ("valid_to", "VARCHAR"),
    ("is_current", "BOOLEAN"),
    ("checksum", "VARCHAR"),
]

# ── per-kind generator functions ──────────────────────────────────────────────


def _gen_seq(spec: "FieldSpec", fake: Any, i: int) -> Any:
    return spec.start + i * spec.step


def _gen_faker(spec: "FieldSpec", fake: Any, i: int) -> Any:
    method_fn = getattr(fake, spec.method)
    val = method_fn(spec.pattern) if spec.pattern is not None else method_fn()
    return getattr(str(val), spec.transform)() if spec.transform else val


def _gen_choices(spec: "FieldSpec", fake: Any, i: int) -> Any:
    return random.choice(spec.choices)


def _gen_uniform(spec: "FieldSpec", fake: Any, i: int) -> Any:
    v = random.uniform(float(spec.low), float(spec.high))
    return round(v, spec.precision) if spec.precision is not None else v


def _gen_randint(spec: "FieldSpec", fake: Any, i: int) -> Any:
    return random.randint(int(spec.low), int(spec.high))


def _gen_const(spec: "FieldSpec", fake: Any, i: int) -> Any:
    return spec.value


def _gen_computed(spec: "FieldSpec", fake: Any, i: int) -> Any:
    return spec.fn(fake, i)


_KIND_GENERATORS: dict[str, Any] = {
    "seq": _gen_seq,
    "faker": _gen_faker,
    "choices": _gen_choices,
    "uniform": _gen_uniform,
    "randint": _gen_randint,
    "const": _gen_const,
    "computed": _gen_computed,
}

# ── FieldSpec ─────────────────────────────────────────────────────────────────


@dataclass
class FieldSpec:
    """
    Declares a single column: its SQL type and how to synthesise values.

    Parameters
    ----------
    name       : Column name.
    sql_type   : SQL data type, e.g. ``"INTEGER"``, ``"VARCHAR"``.
    kind       : Generation strategy — see module docstring.
    start      : (seq) Starting integer.                      Default 1.
    step       : (seq) Row increment.                         Default 1.
    method     : (faker) Faker method, e.g. ``"name"``, ``"email"``.
    pattern    : (faker) Pattern string, e.g. ``"555-####"`` for numerify.
    transform  : (faker) ``str`` method applied to the result, e.g. ``"title"``.
    choices    : (choices) List of candidate values.
    low        : (uniform / randint) Lower bound.
    high       : (uniform / randint) Upper bound.
    precision  : (uniform) Decimal places, e.g. ``2`` for currency.
    value      : (const) The constant to return.
    fn         : (computed) ``callable(fake, i) -> Any``.
    post       : Optional post-process ``callable(value, fake, i) -> Any``.
    sql_export : If ``False``, field is excluded from ``EntitySchema.to_sql()``
                 but still appears in DDL and ``generate()`` output.
    """

    name: str
    sql_type: str
    kind: str = "const"
    start: int = 1
    step: int = 1
    method: str | None = None
    pattern: str | None = None
    transform: str | None = None
    choices: list | None = None
    low: float | None = None
    high: float | None = None
    precision: int | None = None
    value: Any = None
    fn: Any = None
    post: Any = None
    sql_export: bool = True

    def generate(self, fake: Any, i: int) -> Any:
        """Return a synthetic value for row *i*."""
        gen = _KIND_GENERATORS.get(self.kind)
        if gen is None:
            raise ValueError(f"Unknown FieldSpec kind: {self.kind!r}")
        v = gen(self, fake, i)
        return self.post(v, fake, i) if self.post is not None else v

    def as_literal(self, v: Any) -> str:
        """Format *v* as a SQL literal for inclusion in a SELECT statement."""
        if v is None:
            return "NULL"
        if self.sql_type in _STRING_SQL:
            return f"'{str(v).replace(chr(39), chr(39) * 2)}'"
        return str(v)

    def ddl_col(self) -> str:
        """Return e.g. ``"product_id       INTEGER"``."""
        return f"{self.name:<16} {self.sql_type}"


# ── EntitySchema ──────────────────────────────────────────────────────────────


@dataclass
class EntitySchema:
    """
    A collection of ``FieldSpec`` objects that fully describes an entity.

    Generates DDL and synthetic rows without any hardcoded SQL strings.
    The same field definitions drive both schema introspection and data
    generation, keeping vocabulary (choices, ranges) in one place.

    Parameters
    ----------
    name       : Entity name (informational).
    fields     : Ordered field declarations.
    dim_extra  : Extra ``(name, sql_type)`` pairs inserted between the OLTP
                 columns and the SCD audit columns in ``dim_ddl()``.
                 Used for SCD Type-3 previous-value columns.
    """

    name: str
    fields: list[FieldSpec]
    dim_extra: list[tuple[str, str]] = field(default_factory=list)

    def _col_block(
        self,
        extras: list[tuple[str, str]] | None = None,
        fields: list["FieldSpec"] | None = None,
    ) -> str:
        src = fields if fields is not None else self.fields
        lines = [f"    {f.ddl_col()}" for f in src]
        for col_name, col_type in extras or []:
            lines.append(f"    {col_name:<16} {col_type}")
        return ",\n".join(lines)

    def oltp_ddl(self, table: str = "{table}") -> str:
        """DDL for the transactional source table (includes all fields)."""
        return f"CREATE TABLE IF NOT EXISTS {table} (\n{self._col_block()}\n)"

    def dim_ddl(self, table: str = "{table}") -> str:
        """DDL for the sqldim-managed dimension target.

        Only includes ``sql_export=True`` fields so the column list matches
        the ``SELECT`` produced by ``to_sql()``.  Appends ``dim_extra``
        pairs (e.g. SCD Type-3 previous-value columns) then the four SCD
        audit columns.
        """
        dim_fields = [f for f in self.fields if f.sql_export]
        extras = list(self.dim_extra) + _SCD_AUDIT
        return f"CREATE TABLE IF NOT EXISTS {table} (\n{self._col_block(extras, dim_fields)}\n)"

    def generate_row(self, fake: Any, i: int) -> dict:
        """Return one synthetic row dict for row index *i*."""
        return {f.name: f.generate(fake, i) for f in self.fields}

    def generate(self, n: int, fake: Any) -> list[dict]:
        """Return *n* synthetic row dicts."""
        return [self.generate_row(fake, i) for i in range(n)]

    def _row_sql(self, row: dict) -> str:
        parts = [
            f"{f.as_literal(row[f.name])} AS {f.name}"
            for f in self.fields
            if f.sql_export
        ]
        return "SELECT " + ", ".join(parts)

    def to_sql(self, rows: list[dict]) -> str:
        """Render *rows* as a ``SELECT … UNION ALL …`` SQL expression."""
        return " UNION ALL ".join(self._row_sql(r) for r in rows)


# ── DatasetSpec ───────────────────────────────────────────────────────────────


@dataclass
class DatasetSpec:
    """
    Groups the ``EntitySchema`` objects that belong to one logical dataset.

    Each schema is accessed by its *role name* as an attribute, so callers
    can write ``spec.source``, ``spec.fact``, ``spec.edge``, etc., instead
    of managing a collection of scattered ``_DATASET_ROLE_SCHEMA`` module
    variables.

    Parameters
    ----------
    name    : Dataset name (informational, e.g. ``"orders"``).
    schemas : Mapping of role-name → ``EntitySchema``.

    Example
    -------
    .. code-block:: python

        _ORDERS_SPEC = DatasetSpec("orders", {
            "source": EntitySchema("order", fields=[...]),
            "fact":   EntitySchema("order_fact", fields=[...]),
        })

        # DDL via role attribute:
        _ORDERS_SPEC.source.oltp_ddl()   # CREATE TABLE IF NOT EXISTS {table} …
        _ORDERS_SPEC.fact.oltp_ddl()     # CREATE TABLE IF NOT EXISTS {table} …
    """

    name: str
    schemas: dict[str, EntitySchema]
    events: "EventSpec | None" = None
    """Optional event spec shared by all ``SchematicSource`` subclasses."""

    def __post_init__(self) -> None:
        if "events" in self.schemas:
            raise ValueError(
                f"DatasetSpec {self.name!r}: 'events' is a reserved attribute "
                "name and cannot be used as a schema role."
            )

    def __getattr__(self, key: str) -> EntitySchema:
        schemas = object.__getattribute__(self, "schemas")
        try:
            return schemas[key]
        except KeyError:
            roles = list(schemas)
            raise AttributeError(
                f"DatasetSpec {self.name!r} has no schema role {key!r}. "
                f"Available roles: {roles}"
            ) from None

    def __repr__(self) -> str:
        has_events = object.__getattribute__(self, "events") is not None
        suffix = ", events=<EventSpec>" if has_events else ""
        return f"DatasetSpec({self.name!r}, roles={list(self.schemas)}{suffix})"


# ── ChangeRule / EventSpec ────────────────────────────────────────────────────


@dataclass
class ChangeRule:
    """
    One field-mutation rule for an event batch.

    Parameters
    ----------
    field     : Name of the field to mutate.
    condition : ``callable(i, row) -> bool`` — whether the row at index *i*
                should have this field changed.  Receives the current (possibly
                already-mutated) row dict.
    mutate    : ``callable(old_value, row, fake) -> new_value`` — the
                transformation applied when *condition* returns ``True``.
    """

    field: str
    condition: Any  # (i: int, row: dict) -> bool
    mutate: Any  # (old_value: Any, row: dict, fake) -> Any


@dataclass
class EventSpec:
    """
    Produces an event batch by applying ``ChangeRule`` objects to a row list.

    Parameters
    ----------
    changes         : Ordered rules applied per row (later rules see values set
                      by earlier ones).
    timestamp_field : If set, stamp this field with ``event_ts`` whenever any
                      change occurs on a row.
    event_ts        : Timestamp string written to ``timestamp_field``.
    new_rows_fn     : Optional ``callable(initial_rows, fake) -> list[dict]``
                      appended to the event batch — useful for events that
                      include entirely new records (e.g. a newly opened issue).
    """

    changes: list[ChangeRule]
    timestamp_field: str | None = None
    event_ts: str = ""
    new_rows_fn: Any = None  # callable(rows, fake) -> list[dict]

    def _stamp_ts(self, row: dict) -> None:
        if self.timestamp_field:
            row[self.timestamp_field] = self.event_ts

    def _mutate_row(self, i: int, row: dict, fake: Any) -> tuple[dict, bool]:
        new_row = dict(row)
        changed = False
        for rule in self.changes:
            if rule.condition(i, new_row):
                new_val = rule.mutate(new_row[rule.field], new_row, fake)
                if new_val != new_row[rule.field]:
                    new_row[rule.field] = new_val
                    changed = True
        if changed:
            self._stamp_ts(new_row)
        return new_row, changed

    def apply(self, rows: list[dict], fake: Any) -> list[dict]:
        """Return event rows — only rows with at least one changed field."""
        result = []
        for i, row in enumerate(rows):
            new_row, changed = self._mutate_row(i, row, fake)
            if changed:
                result.append(new_row)
        if self.new_rows_fn is not None:
            result.extend(self.new_rows_fn(rows, fake))
        return result
