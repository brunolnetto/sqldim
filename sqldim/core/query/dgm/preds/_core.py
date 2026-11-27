"""DGM predicate and path types — ScalarPred, AND/OR/NOT, Strategy, PathPred, PathAgg."""

from __future__ import annotations

import enum
from sqldim.exceptions import SemanticError

from sqldim.core.query.dgm.refs import (
    PropRef,
    AggRef,
    WinRef,
    _SQLExpr,
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
    "VerbHopInverse",
    "BridgeHop",
    "Compose",
    "_flatten_path",
    "_path_pred_sql",
    "PathPred",
    "PathAgg",
    # TemporalProperty sugar (DGM §4.1)
    "SAFETY",
    "LIVENESS",
    "RESPONSE",
    "PERSISTENCE",
    "RECURRENCE",
    # Signature predicate (DGM §4.1)
    "SequenceMatch",
    "SignaturePred",
]

# ---------------------------------------------------------------------------
# Predicate types
# ---------------------------------------------------------------------------


class ScalarPred:
    """Scalar comparison: ref op value.

    NULL is not a literal (§4.1): a comparison with ``None`` is rejected at
    construction; ``IS`` / ``IS NOT`` with ``None`` is the NULL test.
    """

    def __init__(self, ref: _SQLExpr, op: str, value: object) -> None:
        if value is None and str(op).upper() not in ("IS", "IS NOT"):
            raise SemanticError(
                f"ScalarPred(…, {op!r}, None): NULL is not a literal (§4.1); "
                "test it with op 'IS' or 'IS NOT'"
            )
        self.ref = ref
        self.op = op
        self.value = value

    def to_sql(self) -> str:
        if self.op in ("IS", "IS NOT") and self.value is None:
            return f"{self.ref.to_sql()} {self.op} NULL"
        return f"{self.ref.to_sql()} {self.op} {_format_value(self.value)}"


class AND:
    """Conjunction of predicates; single predicate short-circuits to itself."""

    def __init__(self, *preds: _SQLExpr) -> None:
        self.preds = preds

    def to_sql(self) -> str:
        return _paren_if_compound([p.to_sql() for p in self.preds], "AND")


class OR:
    """Disjunction of predicates; single predicate short-circuits to itself."""

    def __init__(self, *preds: _SQLExpr) -> None:
        self.preds = preds

    def to_sql(self) -> str:
        return _paren_if_compound([p.to_sql() for p in self.preds], "OR")


class NOT:
    """Negation; NOT(NOT(T)) ≡ T via __new__ double-negation folding."""

    def __new__(cls, pred: _SQLExpr) -> object:  # type: ignore[misc]
        if isinstance(pred, NOT):
            return pred.pred  # double-negation fold
        obj = super().__new__(cls)
        return obj

    def __init__(self, pred: _SQLExpr) -> None:
        if not isinstance(pred, NOT):
            self.pred = pred

    def to_sql(self) -> str:
        return f"NOT ({self.pred.to_sql()})"


class NotTrue:
    """``NotTrue(φ)``: φ is FALSE or UNKNOWN — SQL ``(φ) IS NOT TRUE`` (§6.1).

    Two-valued; produced by FORALL elimination (§4.1) so that a position
    where φ is UNKNOWN counts against a universal condition.
    """

    def __init__(self, pred: _SQLExpr) -> None:
        self.pred = pred

    def to_sql(self) -> str:
        return f"({self.pred.to_sql()}) IS NOT TRUE"

    def __repr__(self) -> str:
        return f"NotTrue({self.pred!r})"


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


class VerbHopInverse(_HopBase):
    """Reverse verb-edge hop: traverses from a fact table back to a dimension.

    Corresponds to VerbHop⁻¹(f, label, d) in the DGM spec.  Enables
    constellation paths (e.g. f₁ ← shared_dim → f₂) and INCOMING_SIGNATURES
    traversal via G^T.
    """

    kind = "verb_inverse"


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


# ---------------------------------------------------------------------------
# Construction rules and the fail-closed SQL rendering of a PathPred
# ---------------------------------------------------------------------------


def _is_past_mode(mode: object) -> bool:
    from sqldim.core.query.dgm.temporal import ONCE, PREVIOUSLY, SinceMode

    return mode is ONCE or mode is PREVIOUSLY or isinstance(mode, SinceMode)


def _path_hops(path: object) -> list:
    """Every hop of *path*, through Compose and the body of a Repeat."""
    from sqldim.core.query.dgm.preds._repeat import Repeat

    out: list = []
    for item in _flatten_path(path):
        out += _path_hops(item.path) if isinstance(item, Repeat) else [item]
    return out


def _check_path_pred(pp: "PathPred") -> None:
    """Construction-time rules of §3.5 and §4.1 that need no schema graph."""
    from sqldim.core.query.dgm.temporal import ONCE, SinceMode

    if pp.promote and not isinstance(pp.strategy, (SHORTEST, MIN_WEIGHT)):
        raise SemanticError(
            "PathPred(promote=True) is valid only for SHORTEST and MIN_WEIGHT "
            "(§3.5); it is rejected for ALL and K_SHORTEST"
        )
    mode = pp.temporal_mode
    if pp.quantifier is Quantifier.FORALL and (
        mode is ONCE or isinstance(mode, SinceMode)
    ):
        raise SemanticError("FORALL with ONCE or SINCE is rejected (§4.1)")
    if _is_past_mode(mode) and any(
        isinstance(h, (VerbHop, VerbHopInverse)) for h in _path_hops(pp.path)
    ):
        raise SemanticError(
            "SINCE, ONCE and PREVIOUSLY need a time-ordered path (§2.3, §4.1); "
            "a verb hop is never time-ordered"
        )


def _check_renderable(pp: "PathPred") -> None:
    """``to_sql()`` renders only what it renders faithfully (fail-closed).

    The same constructs are refused by the plan IR (``planner/_ir_build.py``);
    a temporal mode is checked by ``_positions.without_mode``.
    """
    from sqldim.core.query.dgm.planner._ir import UnsupportedConstruct

    if pp.strategy is not None and not isinstance(pp.strategy, ALL):
        raise UnsupportedConstruct(STRATEGY_REFUSAL.format(type(pp.strategy).__name__))
    # promote=True implies SHORTEST or MIN_WEIGHT (§3.5), refused here


STRATEGY_REFUSAL = (
    "PathPred strategy {} restricts the path set by the order ≺ of §3.3, which "
    "breaks ties by node keys a hop does not declare; only ALL is rendered"
)


def _path_pred_sql(pp: "PathPred") -> str:
    """EXISTS / FORALL subquery SQL of a PathPred with strategy ALL; a
    temporal mode is rendered by its one-position form (``_positions``)."""
    if pp.temporal_mode is not None:
        from sqldim.core.query.dgm.preds._positions import without_mode

        _check_renderable(pp)
        pp = without_mode(pp)
    hops = _flatten_path(pp.path)
    if not hops:  # pragma: no cover
        return "TRUE"
    if any(not isinstance(h, _HopBase) for h in hops):
        raise SemanticError(
            "PathPred over a Repeat (§3.2) has no to_sql() rendering; plan the "
            "query with DGMPlanner.plan_query()"
        )
    _check_renderable(pp)
    first = hops[0]
    from_clause = f"{first.table} {first.to_alias}"
    join_sql = " ".join(f"JOIN {h.table} {h.to_alias} ON {h.on}" for h in hops[1:])
    sql = f"EXISTS (SELECT 1 FROM {from_clause}"
    if join_sql:
        sql += f" {join_sql}"
    if pp.quantifier is Quantifier.FORALL:
        # §4.1 FORALL elimination with NotTrue, under the REQUIRE_WITNESS
        # default of ADR 0001: no violating position, and at least one.
        sub = pp.sub_filter.to_sql()
        return (
            f"(NOT {sql} WHERE ({sub}) IS NOT TRUE AND {first.on})"
            f" AND {sql} WHERE {first.on}))"
        )
    return sql + " WHERE " + " AND ".join([pp.sub_filter.to_sql(), first.on]) + ")"


# ---------------------------------------------------------------------------
# PathPred
# ---------------------------------------------------------------------------


class PathPred:
    """
    Existence (or universal) predicate over a traversal path.

    By default renders as EXISTS (SELECT 1 FROM <path joins> WHERE <sub_filter>
    AND <correlation>).  Supply ``quantifier`` and ``strategy`` to express
    quantified / strategy-controlled path predicates per DGM §18.3–18.4.

    Parameters
    ----------
    anchor:
        Alias of the context node the path is rooted at.
    path:
        A BoundPath (VerbHop, BridgeHop, VerbHopInverse, Compose, …).
    sub_filter:
        Predicate evaluated inside the path traversal context.
    quantifier:
        EXISTS (∃, default) or FORALL (∀, De Morgan'd to NOT…EXISTS…NOT).
    strategy:
        Path selection strategy (ALL, SHORTEST, K_SHORTEST, MIN_WEIGHT).
    temporal_mode:
        CTL temporal mode applied to the traversal (DGM §4.1).
    promote:
        When True + single-instance strategy, lift path-local context L into C.
    """

    def __init__(
        self,
        anchor: str,
        path: object,
        sub_filter: _SQLExpr,
        *,
        quantifier: "Quantifier | None" = None,
        strategy: "Strategy | None" = None,
        temporal_mode: "object | None" = None,
        promote: "bool | None" = None,
    ) -> None:
        self.anchor = anchor
        self.path = path
        self.sub_filter = sub_filter
        self.quantifier = quantifier
        self.strategy = strategy
        self.temporal_mode = temporal_mode
        self.promote = promote
        _check_path_pred(self)

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


# ---------------------------------------------------------------------------
# TemporalProperty sugar + SignaturePred (DGM §4.1)
# ---------------------------------------------------------------------------

from sqldim.core.query.dgm.preds._signature import (  # noqa: E402, F401
    SAFETY,
    LIVENESS,
    RESPONSE,
    PERSISTENCE,
    RECURRENCE,
    SequenceMatch,
    SignaturePred,
)
