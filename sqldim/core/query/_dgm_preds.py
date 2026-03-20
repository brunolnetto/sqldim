"""DGM predicate and path types — ScalarPred, AND/OR/NOT, Strategy, PathPred, PathAgg."""

from __future__ import annotations

import enum

from sqldim.core.query._dgm_refs import (
    PropRef,
    AggRef,
    WinRef,
    _format_value,
    _paren_if_compound,
)

# Re-export so callers can still do ``from sqldim.core.query.dgm import PropRef``
# via dgm.py (which imports from here).
__all__ = [
    "PropRef",
    "AggRef",
    "WinRef",
    "_format_value",
    "_paren_if_compound",
    "ScalarPred",
    "AND",
    "OR",
    "NOT",
    "RawPred",
    "Strategy",
    "ALL",
    "SHORTEST",
    "K_SHORTEST",
    "MIN_WEIGHT",
    "Quantifier",
    "_HopBase",
    "VerbHop",
    "BridgeHop",
    "Compose",
    "_flatten_path",
    "_path_pred_sql",
    "PathPred",
    "PathAgg",
]

# ---------------------------------------------------------------------------
# Predicate types
# ---------------------------------------------------------------------------


class ScalarPred:
    """Scalar comparison: ref op value."""

    def __init__(self, ref: object, op: str, value: object) -> None:
        self.ref = ref
        self.op = op
        self.value = value

    def to_sql(self) -> str:
        if self.op in ("IS", "IS NOT") and self.value is None:
            return f"{self.ref.to_sql()} {self.op} NULL"
        return f"{self.ref.to_sql()} {self.op} {_format_value(self.value)}"


class AND:
    """Conjunction of predicates; single predicate short-circuits to itself."""

    def __init__(self, *preds: object) -> None:
        self.preds = preds

    def to_sql(self) -> str:
        return _paren_if_compound([p.to_sql() for p in self.preds], "AND")


class OR:
    """Disjunction of predicates; single predicate short-circuits to itself."""

    def __init__(self, *preds: object) -> None:
        self.preds = preds

    def to_sql(self) -> str:
        return _paren_if_compound([p.to_sql() for p in self.preds], "OR")


class NOT:
    """Negation; NOT(NOT(T)) ≡ T via __new__ double-negation folding."""

    def __new__(cls, pred: object) -> object:
        if isinstance(pred, NOT):
            return pred.pred  # double-negation fold
        obj = super().__new__(cls)
        return obj

    def __init__(self, pred: object) -> None:
        if not isinstance(pred, NOT):
            self.pred = pred

    def to_sql(self) -> str:
        return f"NOT ({self.pred.to_sql()})"


class RawPred:
    """Wraps a raw SQL string as a predicate for backward compatibility."""

    def __init__(self, sql: str) -> None:
        self._sql = sql

    def to_sql(self) -> str:
        return self._sql


# ---------------------------------------------------------------------------
# Path Strategy types  (DGM §15, §18.4)
# ---------------------------------------------------------------------------


class Strategy:
    """Base class for explicit path-traversal strategies."""


class ALL(Strategy):
    """Select every matching path (explicit fan-out)."""


class SHORTEST(Strategy):
    """Select the single path minimising hop count."""


class K_SHORTEST(Strategy):
    """Select the *k* paths minimising hop count."""

    def __init__(self, k: int) -> None:
        self.k = k


class MIN_WEIGHT(Strategy):
    """Select the single path minimising the sum of edge weights."""


# ---------------------------------------------------------------------------
# Quantifier  (DGM §18.3)
# ---------------------------------------------------------------------------


class Quantifier(enum.Enum):
    """Explicit quantifier for :class:`PathPred` — EXISTS (∃) or FORALL (∀)."""

    EXISTS = "EXISTS"
    FORALL = "FORALL"


# ---------------------------------------------------------------------------
# Path types  (τ_E: E → {verb, bridge})
# ---------------------------------------------------------------------------


class _HopBase:
    """Base for a single traversal hop in a path predicate.

    *table* and *on* can be omitted when *model* is supplied together with a
    ``DGMQuery`` that has the relevant aliases registered — the query's
    ``path_join`` method will fill them in automatically via FK inference.
    """

    def __init__(
        self,
        from_alias: str,
        label: str,
        to_alias: str,
        *,
        model: type | None = None,
        table: str | None = None,
        on: str | None = None,
    ) -> None:
        self.from_alias = from_alias
        self.label = label
        self.to_alias = to_alias
        self.model = model
        self.table = table
        self.on = on


class VerbHop(_HopBase):
    """Verb-edge hop: traverses a fact table (FactModel edge)."""

    kind = "verb"


class BridgeHop(_HopBase):
    """Bridge-edge hop: traverses a bridge table (BridgeModel edge)."""

    kind = "bridge"


class Compose:
    """Sequential path composition: left hop followed by right hop."""

    def __init__(self, left: object, right: object) -> None:
        self.left = left
        self.right = right


# ---------------------------------------------------------------------------
# Path utilities
# ---------------------------------------------------------------------------


def _flatten_path(path: object) -> list:
    """Flatten a Compose tree into an ordered list of hops."""
    if isinstance(path, Compose):
        return _flatten_path(path.left) + _flatten_path(path.right)
    return [path]


def _path_pred_sql(pp: "PathPred") -> str:
    """Build EXISTS subquery SQL from a PathPred."""
    hops = _flatten_path(pp.path)
    if not hops:  # pragma: no cover
        return "TRUE"
    first = hops[0]
    from_clause = f"{first.table} {first.to_alias}"
    join_sql = " ".join(f"JOIN {h.table} {h.to_alias} ON {h.on}" for h in hops[1:])
    where_terms = [pp.sub_filter.to_sql(), first.on]
    sql = f"EXISTS (SELECT 1 FROM {from_clause}"
    if join_sql:
        sql += f" {join_sql}"
    return sql + " WHERE " + " AND ".join(where_terms) + ")"


# ---------------------------------------------------------------------------
# PathPred
# ---------------------------------------------------------------------------


class PathPred:
    """
    Existence (or universal) predicate over a traversal path.

    By default renders as EXISTS (SELECT 1 FROM <path joins> WHERE <sub_filter>
    AND <correlation>).  Supply ``quantifier`` and ``strategy`` to express
    quantified / strategy-controlled path predicates per DGM §18.3–18.4.
    """

    def __init__(
        self,
        anchor: str,
        path: object,
        sub_filter: object,
        *,
        quantifier: "Quantifier | None" = None,
        strategy: "Strategy | None" = None,
    ) -> None:
        self.anchor = anchor
        self.path = path
        self.sub_filter = sub_filter
        self.quantifier = quantifier
        self.strategy = strategy

    def to_sql(self) -> str:
        return _path_pred_sql(self)


class PathAgg:
    """
    Path-traversal aggregation with an explicit strategy (DGM §16.2).

    Computes ``fn(ref)`` over the rows reachable via *path* from *anchor*,
    restricted by the given *strategy*.
    """

    def __init__(
        self,
        anchor: str,
        path: object,
        strategy: "Strategy",
        fn: str,
        ref: str,
    ) -> None:
        self.anchor = anchor
        self.path = path
        self.strategy = strategy
        self.fn = fn
        self.ref = ref

    def to_sql(self) -> str:
        """Render a placeholder aggregation expression."""
        return f"{self.fn}({self.ref})"
