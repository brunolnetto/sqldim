"""Dimension joins of :class:`DGMQuery` and their SCD resolution (§2.5, §2.6).

A dimension join is a hop from the anchor fact to a versioned dimension.
Which version it reads is fixed by its SCD resolution mode and by the kind of
reference the fact stores (§2.5):

=================  ==========================================  ================================
Reference          AS_WAS (the default)                        AS_IS
=================  ==========================================  ================================
``ENTITY_KEY``     ``version(T, key, event_time(f))``          ``version(T, key, τ)``
``VERSION_KEY``    the stored version                          ``version(T, entity, τ)``
=================  ==========================================  ================================

τ is the instant of :meth:`DGMQuery.as_of`, or the current version when the
query states none.  Windows are half-open and a NULL bound is open (§2.6), so
an instant is compared as it is: never cast to a date.

Both ``DGMQuery.to_sql()`` and the plan IR (``planner/_ir_build.py``) render
a dimension join from the same :func:`dim_join_steps`.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqldim.exceptions import SemanticError

__all__ = [
    "AS_IS",
    "AS_WAS",
    "DimJoin",
    "DimJoinStep",
    "dim_join_steps",
    "quote",
    "valid_at",
]

AS_WAS, AS_IS = "AS_WAS", "AS_IS"
CURRENT, AT_TAU = "CURRENT", "AT_TAU"


def quote(value: str) -> str:
    """Render *value* as a SQL string literal (quotes doubled)."""
    return "'" + value.replace("'", "''") + "'"


def valid_at(alias: str, at: str) -> str:
    """``valid_from ≤ at < valid_to`` with NULL bounds open (§2.6)."""
    return (
        f"({alias}.valid_from IS NULL OR {alias}.valid_from <= {at})"
        f" AND ({alias}.valid_to IS NULL OR {alias}.valid_to > {at})"
    )


@dataclass(frozen=True)
class DimJoin:
    """A fact → dimension hop declared with :meth:`DGMQuery.join_dim`.

    ``version_key`` is set when the fact stores a version surrogate
    (``VERSION_KEY``): ``fact_fk`` then matches the dimension's
    ``version_key`` column and ``dim_pk`` is the durable entity key.
    Otherwise the fact stores the entity key (``ENTITY_KEY``) and ``fact_fk``
    matches ``dim_pk``.  AS_WAS over an entity key needs ``event_time``, the
    fact's event timestamp column.
    """

    table: str
    alias: str
    fact_fk: str
    dim_pk: str = "id"
    mode: str = AS_WAS
    event_time: str | None = None
    version_key: str | None = None

    def __post_init__(self) -> None:
        if self.mode not in (AS_WAS, AS_IS):
            raise SemanticError(
                f"join_dim mode {self.mode!r}: the SCD resolution is AS_WAS or AS_IS"
            )
        if self.mode == AS_WAS and self.version_key is None and not self.event_time:
            raise SemanticError(
                f"join_dim({self.table!r}): AS_WAS, the default (§2.5), reads the "
                "version in force when the fact happened; pass event_time= (the "
                "fact's event timestamp column), version_key= when the fact "
                "stores a version surrogate, or mode='AS_IS'"
            )


@dataclass(frozen=True)
class DimJoinStep:
    """One ``LEFT JOIN`` of a dimension join; ``version`` is the condition
    added to ``on``: ``CURRENT``, ``AT_TAU`` or none."""

    table: str
    alias: str
    on: str
    version: str | None


def _column(anchor_alias: str, column: str) -> str:
    return column if "." in column else f"{anchor_alias}.{column}"


def dim_join_steps(
    j: DimJoin, anchor_alias: str, as_of: str | None
) -> list[DimJoinStep]:
    """The joins that read the version *j* resolves to (§2.5 table)."""
    fk = f"{anchor_alias}.{j.fact_fk}"
    at_tau = AT_TAU if as_of else CURRENT
    if j.version_key is not None:
        if j.mode == AS_WAS:  # the stored version, no interval lookup
            on = f"{fk} = {j.alias}.{j.version_key}"
            return [DimJoinStep(j.table, j.alias, on, None)]
        stored = f"{j.alias}__stored"  # the stored version identifies the entity
        return [
            DimJoinStep(j.table, stored, f"{fk} = {stored}.{j.version_key}", None),
            DimJoinStep(
                j.table, j.alias, f"{j.alias}.{j.dim_pk} = {stored}.{j.dim_pk}", at_tau
            ),
        ]
    on = f"{fk} = {j.alias}.{j.dim_pk}"
    if j.mode == AS_WAS:
        assert j.event_time is not None
        at = _column(anchor_alias, j.event_time)
        return [
            DimJoinStep(j.table, j.alias, f"{on} AND {valid_at(j.alias, at)}", None)
        ]
    return [DimJoinStep(j.table, j.alias, on, at_tau)]


def version_sql(step: DimJoinStep, as_of: str | None) -> str:
    """The version condition of *step* as ``DGMQuery.to_sql()`` renders it."""
    if step.version == CURRENT:
        return f" AND {step.alias}.is_current = TRUE"
    if step.version == AT_TAU:
        assert as_of is not None
        return " AND " + valid_at(step.alias, quote(as_of))
    return ""
