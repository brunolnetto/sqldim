"""DGM Question Algebra — DGM §8.13.

Formal extension of the question-completeness theorem (§8.11).

``Q_valid(A, B)`` is a *generating set*; the full question space is its closure
under CTE composition:

    Q_algebra(A, B) = closure of Q_valid(A, B) under {JOIN, UNION, INTERSECT,
                                                        EXCEPT, WITH}

This module provides:

* :class:`ComposeOp`     — enumeration of the five composition operators.
* :class:`ComposedQuery` — a named CTE wrapper around a :class:`~sqldim.core.query.dgm.core.DGMQuery`
  *or* a composed SQL fragment, with a ``to_cte_sql()`` method that renders
  the CTE definition body (``<name> AS (<sql>)``).
* :class:`QuestionAlgebra` — ordered registry of CTEs that implements
  composition operations and emits a single ``WITH … SELECT`` statement.

Semiring structure (§8.13)
--------------------------
``(Q_algebra(A,B), UNION, INTERSECT, ∅, Q_top)`` forms a semiring where:

* ``EMPTY_Q`` (∅) is the additive identity — a query that returns no rows.
* ``TOP_Q``   (Q_top) is the multiplicative identity — a query that returns
  all rows with no filter applied.

Complexity
----------
CTE chain construction and SQL emission are O(|CTEs|) — each of the *n* CTEs
is visited exactly once during topological ordering and once during SQL
rendering.  No exponential blowup arises from composition because each CTE
stores a reference to its SQL fragment, not a deep copy.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqldim.core.query.dgm.core import DGMQuery

__all__ = [
    "ComposeOp",
    "ComposedQuery",
    "QuestionAlgebra",
]


# ---------------------------------------------------------------------------
# ComposeOp
# ---------------------------------------------------------------------------


class ComposeOp(Enum):
    """The five CTE composition operators (§8.13).

    * ``JOIN``      — Cross-question correlation (most expressive).
    * ``UNION``     — Disjunctive answering; ``UNION ALL`` SQL semantics.
    * ``INTERSECT`` — Conjunctive answering.
    * ``EXCEPT``    — Set difference.
    * ``WITH``      — Dependent chain: right CTE uses left CTE as its scope.
    """

    JOIN = "JOIN"
    UNION = "UNION"
    INTERSECT = "INTERSECT"
    EXCEPT = "EXCEPT"
    WITH = "WITH"


# ---------------------------------------------------------------------------
# ComposedQuery
# ---------------------------------------------------------------------------


class ComposedQuery:
    """A named CTE wrapper — either a :class:`DGMQuery` leaf or a composed fragment.

    Parameters
    ----------
    name:
        The CTE name used in ``WITH <name> AS (…)`` syntax.
    query:
        The underlying :class:`~sqldim.core.query.dgm.core.DGMQuery` (for leaf
        CTEs) or ``None`` when *sql* is supplied directly.
    sql:
        Pre-rendered SQL fragment used by composed CTEs.  Exactly one of
        *query* and *sql* must be provided.
    """

    def __init__(
        self,
        name: str,
        query: "DGMQuery | None" = None,
        sql: str | None = None,
    ) -> None:
        if query is None and sql is None:
            raise ValueError("ComposedQuery requires either 'query' or 'sql'.")
        self.name = name
        self.query = query
        self._sql = sql

    # -- SQL rendering -------------------------------------------------------

    def to_cte_sql(self) -> str:
        """Render the full CTE definition: ``<name> AS (<body>)``."""
        body = self._sql if self._sql is not None else self.query.to_sql()  # type: ignore[union-attr]
        return f"{self.name} AS (\n{body}\n)"

    def __repr__(self) -> str:
        return f"ComposedQuery(name={self.name!r})"


# ---------------------------------------------------------------------------
# QuestionAlgebra
# ---------------------------------------------------------------------------


class QuestionAlgebra:
    """Ordered registry of CTEs that implements the question algebra.

    Maintains insertion order so that dependent CTEs are always emitted before
    the CTEs that reference them.  All composition operations auto-register
    the resulting CTE into the algebra, enabling multi-step composition chains.

    Class attributes
    ----------------
    EMPTY_Q:
        SQL string for the additive-identity query (returns no rows).
    TOP_Q:
        SQL string for the multiplicative-identity query (returns all rows with
        a trivially-true filter — used as a placeholder when no graph table is
        known yet).
    """

    #: Additive identity ∅ — a query that returns no rows.
    EMPTY_Q: str = "SELECT 1 WHERE FALSE"

    #: Multiplicative identity Q_top — a trivially-true query.
    TOP_Q: str = "SELECT 1 WHERE TRUE"

    def __init__(self) -> None:
        # Ordered dict preserves insertion order (Python 3.7+).
        self._ctes: dict[str, ComposedQuery] = {}
        # Maps composed CTE name → (left_name, op, right_name).
        # Leaf CTEs added via add() are NOT recorded here.
        self._composition: dict[str, tuple[str, "ComposeOp", str]] = {}

    # -- Container interface -------------------------------------------------

    def __len__(self) -> int:
        return len(self._ctes)

    def __getitem__(self, name: str) -> ComposedQuery:
        return self._ctes[name]  # KeyError propagates naturally.

    @property
    def names(self) -> list[str]:
        """Names of all registered CTEs in insertion order."""
        return list(self._ctes.keys())

    # -- Registration --------------------------------------------------------

    def add(self, name: str, query: "DGMQuery") -> "QuestionAlgebra":
        """Wrap *query* as a named CTE and register it.

        Returns *self* for method chaining.

        Raises :class:`ValueError` if *name* is already registered.
        """
        if name in self._ctes:
            raise ValueError(
                f"CTE name {name!r} is already registered in this QuestionAlgebra. "
                "Each name must be unique."
            )
        self._ctes[name] = ComposedQuery(name=name, query=query)
        return self

    # -- Composition operations ----------------------------------------------

    def compose(
        self,
        left: str,
        op: ComposeOp,
        right: str,
        *,
        name: str,
        on: str | None = None,
    ) -> ComposedQuery:
        """Compose two registered CTEs via *op* and register the result as *name*.

        Parameters
        ----------
        left, right:
            Names of already-registered CTEs.
        op:
            One of the :class:`ComposeOp` operators.
        name:
            Name for the new composed CTE.
        on:
            JOIN condition (required for ``ComposeOp.JOIN``; ignored otherwise).

        Returns the newly-created :class:`ComposedQuery`.

        Raises
        ------
        KeyError
            If *left* or *right* are not registered.
        ValueError / TypeError
            If ``ComposeOp.JOIN`` is used without an *on* clause.
        """
        # Resolve — KeyError propagates naturally if name is unknown.
        _left = self._ctes[left]
        _right = self._ctes[right]

        sql = self._compose_sql(_left, _right, op, on=on)
        composed = ComposedQuery(name=name, sql=sql)
        self._ctes[name] = composed
        self._composition[name] = (left, op, right)
        return composed

    # -- Minimisation --------------------------------------------------------

    def minimize(self, bdd: object) -> "QuestionAlgebra":
        """Return a minimized copy using Rule 11 Extended semiring elimination.

        Delegates to :func:`~sqldim.core.query.dgm._dag.apply_semiring_minimisation`.
        The original algebra is never mutated.
        """
        from sqldim.core.query.dgm._dag import apply_semiring_minimisation

        return apply_semiring_minimisation(self, bdd)  # type: ignore[arg-type]

    # -- SQL emission --------------------------------------------------------

    def to_sql(self, final: str) -> str:
        """Emit a single ``WITH … SELECT`` statement ending with *final*.

        All CTEs that are transitively referenced by *final* are emitted in
        insertion order (which is guaranteed to be a valid topological order
        since ``compose`` can only reference already-registered names).

        Complexity: O(|CTEs|) — one pass over the ordered dict.

        Raises
        ------
        KeyError
            If *final* is not registered.
        """
        # Validate *final* exists (KeyError propagates).
        _ = self._ctes[final]

        # Emit all CTEs defined up to and including *final* in insertion order.
        # Because compose() requires existing names this is always topologically
        # valid without an explicit DAG sort.
        cte_defs: list[str] = []
        for cte in self._ctes.values():
            cte_defs.append(cte.to_cte_sql())
            if cte.name == final:
                break

        with_block = ",\n".join(cte_defs)
        return f"WITH\n{with_block}\nSELECT * FROM {final}"

    # -- Internal SQL builders -----------------------------------------------

    @staticmethod
    def _compose_sql(
        left: ComposedQuery,
        right: ComposedQuery,
        op: ComposeOp,
        on: str | None,
    ) -> str:
        """Return the inner SQL body for a composed CTE.

        The body uses the CTE names as references — the outer ``to_sql()``
        ensures those names are defined earlier in the WITH block.
        """
        _set_ops = {
            ComposeOp.UNION: f"SELECT * FROM {left.name}\nUNION ALL\nSELECT * FROM {right.name}",
            ComposeOp.INTERSECT: f"SELECT * FROM {left.name}\nINTERSECT\nSELECT * FROM {right.name}",
            ComposeOp.EXCEPT: f"SELECT * FROM {left.name}\nEXCEPT\nSELECT * FROM {right.name}",
            ComposeOp.WITH: f"SELECT * FROM {right.name}",
        }
        if op in _set_ops:
            return _set_ops[op]
        if op is ComposeOp.JOIN:
            if on is None:
                raise ValueError(
                    "ComposeOp.JOIN requires an 'on' join condition. "
                    "Pass on='<left_col> = <right_col>'."
                )
            return f"SELECT l.*, r.*\nFROM {left.name} l\nJOIN {right.name} r ON {on}"
        raise ValueError(f"Unknown ComposeOp: {op!r}")  # pragma: no cover
