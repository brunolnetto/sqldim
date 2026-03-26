"""DGM Cross-CTE Common Sub-expression Elimination — §6.2 Rule 11.

When multiple CTEs inside a :class:`~sqldim.core.query.dgm.algebra.QuestionAlgebra`
share an identical WHERE-predicate BDD node (same canonical integer ID), that
predicate sub-expression can be extracted once into a shared ``__cse_<id>``
CTE.  All original CTEs then implicitly benefit from the pre-filtered scan.

Public API
----------
find_shared_predicates(algebra, bdd)
    O(|CTEs|) detection pass.  Returns ``{bdd_id: [cte_names]}`` for every BDD
    node ID that appears as the WHERE predicate of 2+ leaf CTEs.

apply_cse(algebra, bdd)
    Returns a *new* :class:`~sqldim.core.query.dgm.algebra.QuestionAlgebra`
    (never mutates its input) with one ``__cse_<bdd_id>`` CTE injected before
    each group of sharing CTEs.  The ``__cse_*`` CTE contains:

        SELECT * FROM <anchor_table> WHERE <pred_sql>

    so that subsequent CTEs (or user-written compositions) can reference the
    pre-filtered result directly.

Complexity
----------
* ``find_shared_predicates``: O(|CTEs|) — one dict lookup per CTE entry.
* ``apply_cse``: O(|CTEs|) — one sequential pass building the new registry.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqldim.core.query.dgm.algebra import ComposedQuery, QuestionAlgebra

if TYPE_CHECKING:
    from sqldim.core.query.dgm.bdd import DGMPredicateBDD

__all__ = [
    "find_shared_predicates",
    "apply_cse",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _pred_bdd_id(cq: ComposedQuery, bdd: "DGMPredicateBDD") -> int | None:
    """Return the BDD node ID for the WHERE predicate of a leaf CTE, or None.

    Returns ``None`` for:
    * Composed CTEs (raw SQL, no underlying ``DGMQuery``).
    * Leaf CTEs whose ``DGMQuery`` has no WHERE predicate.
    """
    if cq.query is None:
        return None
    pred = getattr(cq.query, "_where_pred", None)
    if pred is None:
        return None
    return bdd.compile(pred)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def find_shared_predicates(
    algebra: QuestionAlgebra,
    bdd: "DGMPredicateBDD",
) -> dict[int, list[str]]:
    """Detect CTEs that share a WHERE-predicate BDD node.

    This is a **pure detection** pass — the algebra is not modified.

    Parameters
    ----------
    algebra:
        The :class:`QuestionAlgebra` to inspect.
    bdd:
        A :class:`~sqldim.core.query.dgm.bdd.DGMPredicateBDD` used for
        compiling predicate objects to canonical BDD node IDs.  The caller
        may reuse the same ``bdd`` instance across multiple calls; the
        computed cache inside ``BDDManager`` makes repeated compilations O(1).

    Returns
    -------
    dict[int, list[str]]
        Mapping ``bdd_node_id → [cte_name, …]`` for every node ID that appears
        as the WHERE predicate of **two or more** leaf CTEs.  Groups with only
        one CTE are omitted (nothing to share).  An empty dict means no sharing
        opportunities exist.

    Complexity
    ----------
    O(|CTEs|): one ``bdd.compile()`` call per leaf CTE, each O(|pred_tree|).
    Dict grouping is O(n) amortised.
    """
    groups: dict[int, list[str]] = {}
    for name, cq in algebra._ctes.items():
        bdd_id = _pred_bdd_id(cq, bdd)
        if bdd_id is None:
            continue
        groups.setdefault(bdd_id, []).append(name)
    # Only report genuine sharing (2+ CTEs per group).
    return {k: v for k, v in groups.items() if len(v) >= 2}


# ---------------------------------------------------------------------------
# Private helpers for apply_cse
# ---------------------------------------------------------------------------


def _build_cse_lookup(
    algebra: QuestionAlgebra,
    shared: "dict[int, list[str]]",
) -> "tuple[dict[str, str], dict[str, str]]":
    """Build (name_to_cse, cse_sql) mappings from the shared-predicate groups."""
    name_to_cse: dict[str, str] = {}
    cse_sql: dict[str, str] = {}
    for bdd_id, names in shared.items():
        cse_name = f"__cse_{bdd_id}"
        first_cq = algebra._ctes[names[0]]
        dq = first_cq.query  # DGMQuery — guaranteed non-None by find_shared
        assert dq is not None
        anchor = dq._anchor_table or "unknown"
        pred_sql = dq._where_pred.to_sql()  # type: ignore[union-attr]
        cse_sql[cse_name] = f"SELECT * FROM {anchor} WHERE {pred_sql}"
        for n in names:
            name_to_cse[n] = cse_name
    return name_to_cse, cse_sql


def _rebuild_algebra_with_cse(
    algebra: QuestionAlgebra,
    name_to_cse: "dict[str, str]",
    cse_sql: "dict[str, str]",
) -> QuestionAlgebra:
    """Rebuild algebra inserting each __cse_<k> CTE before its first sharing CTE."""
    new_alg = QuestionAlgebra()
    inserted_cse: set[str] = set()
    for name, cq in algebra._ctes.items():
        if name in name_to_cse:
            cse_name = name_to_cse[name]
            if cse_name not in inserted_cse:
                new_alg._ctes[cse_name] = ComposedQuery(
                    name=cse_name, sql=cse_sql[cse_name]
                )
                inserted_cse.add(cse_name)
        new_alg._ctes[name] = cq
    return new_alg


def apply_cse(
    algebra: QuestionAlgebra,
    bdd: "DGMPredicateBDD",
) -> QuestionAlgebra:
    """Inject shared-filter CTEs for every group of CTEs that share a WHERE pred.

    Returns a **new** :class:`QuestionAlgebra`; the original is never mutated.

    For each group *G* = ``[name_0, name_1, …]`` sharing BDD node *k*:

    1. A new CTE ``__cse_<k>`` is inserted *before* the first member of *G*.
       Its body is:
       ``SELECT * FROM <anchor_table> WHERE <pred_sql>``
       where ``<anchor_table>`` is taken from the first member's ``DGMQuery``
       and ``<pred_sql>`` is produced by calling ``to_sql()`` on the shared
       predicate object.

    2. All original CTEs (including the sharing ones) are preserved unchanged
       in the new algebra.  Subsequent composition can then reference
       ``__cse_<k>`` to avoid re-evaluating the predicate.

    Parameters
    ----------
    algebra:
        Source algebra (read-only).
    bdd:
        BDD instance for predicate compilation.

    Returns
    -------
    QuestionAlgebra
        New algebra with shared-filter CTEs injected at the earliest valid
        position (before the first sharing CTE in insertion order), preserving
        full topological validity.

    Complexity
    ----------
    O(|CTEs|): detect pass O(n) + single sequential rebuild O(n).
    """
    shared = find_shared_predicates(algebra, bdd)
    if not shared:
        # Fast path: nothing to do — copy the registry references directly.
        new_alg = QuestionAlgebra()
        for name, cq in algebra._ctes.items():
            new_alg._ctes[name] = cq
        return new_alg

    name_to_cse, cse_sql = _build_cse_lookup(algebra, shared)
    return _rebuild_algebra_with_cse(algebra, name_to_cse, cse_sql)
