"""Phase 2 — Cross-CTE Common Sub-expression Elimination (DGM §6.2 Rule 11).

RED → GREEN test suite.

``find_shared_predicates(algebra, bdd)`` detects WHERE predicates whose BDD
canonical ID appears in 2+ CTEs — O(|CTEs|) single pass.

``apply_cse(algebra, bdd)`` returns a new QuestionAlgebra with shared-filter
CTEs injected before the CTEs that share them (original CTEs are preserved
unchanged but the shared filter CTE becomes available for composition).

BDD node IDs are canonical (same logical predicate ↔ same integer ID), so
identity detection requires no deep comparison — just an int equality.
"""

from __future__ import annotations


from sqldim.core.query.dgm.algebra import ComposedQuery, QuestionAlgebra
from sqldim.core.query.dgm.bdd import BDDManager, DGMPredicateBDD
from sqldim.core.query.dgm.core import DGMQuery
from sqldim.core.query.dgm.preds import ScalarPred, AND
from sqldim.core.query.dgm.refs import PropRef
from sqldim.core.query.dgm._cse import find_shared_predicates, apply_cse


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bdd() -> DGMPredicateBDD:
    return DGMPredicateBDD(BDDManager())


def _q(anchor: str = "fact", alias: str = "f") -> DGMQuery:
    return DGMQuery().anchor(anchor, alias)


def _q_where(col: str, val: object, anchor: str = "fact", alias: str = "f") -> DGMQuery:
    return (
        DGMQuery()
        .anchor(anchor, alias)
        .where(ScalarPred(PropRef(alias, col), "=", val))
    )


def _q_and(
    col1: str,
    val1: object,
    col2: str,
    val2: object,
    anchor: str = "fact",
    alias: str = "f",
) -> DGMQuery:
    p1 = ScalarPred(PropRef(alias, col1), "=", val1)
    p2 = ScalarPred(PropRef(alias, col2), "=", val2)
    return DGMQuery().anchor(anchor, alias).where(AND(p1, p2))


# ---------------------------------------------------------------------------
# find_shared_predicates — detection
# ---------------------------------------------------------------------------


class TestFindSharedPredicates:
    def test_empty_algebra_returns_empty(self):
        """No CTEs → no shared predicates."""
        qa = QuestionAlgebra()
        result = find_shared_predicates(qa, _bdd())
        assert result == {}

    def test_one_cte_no_sharing(self):
        """Single CTE can't share with itself."""
        qa = QuestionAlgebra()
        qa.add("q1", _q_where("region", "US"))
        result = find_shared_predicates(qa, _bdd())
        assert result == {}

    def test_two_ctes_different_pred_no_sharing(self):
        """Two CTEs with different predicates → no sharing."""
        qa = QuestionAlgebra()
        qa.add("q1", _q_where("region", "US"))
        qa.add("q2", _q_where("region", "EU"))
        result = find_shared_predicates(qa, _bdd())
        assert result == {}

    def test_two_ctes_same_pred_detected(self):
        """Two CTEs using the *same predicate object* → identical BDD IDs → detected."""
        pred = ScalarPred(PropRef("f", "region"), "=", "US")
        q1 = DGMQuery().anchor("fact", "f").where(pred)
        q2 = DGMQuery().anchor("fact", "f").where(pred)
        qa = QuestionAlgebra()
        qa.add("q1", q1)
        qa.add("q2", q2)
        bdd = _bdd()
        result = find_shared_predicates(qa, bdd)
        # At least one shared group containing both q1 and q2
        assert len(result) == 1
        shared_names = next(iter(result.values()))
        assert set(shared_names) == {"q1", "q2"}

    def test_three_ctes_two_share_one_unique(self):
        """Three CTEs — two share pred, one is unique → only the pair returned."""
        pred = ScalarPred(PropRef("f", "region"), "=", "US")
        q1 = DGMQuery().anchor("fact", "f").where(pred)
        q2 = DGMQuery().anchor("fact", "f").where(pred)
        q3 = _q_where("segment", "retail")
        qa = QuestionAlgebra()
        qa.add("q1", q1)
        qa.add("q2", q2)
        qa.add("q3", q3)
        bdd = _bdd()
        result = find_shared_predicates(qa, bdd)
        assert len(result) == 1
        shared_names = next(iter(result.values()))
        assert set(shared_names) == {"q1", "q2"}
        assert "q3" not in shared_names

    def test_no_where_pred_skipped(self):
        """CTEs without a WHERE predicate are not candidates."""
        q_no_pred = _q()
        pred = ScalarPred(PropRef("f", "region"), "=", "US")
        q_with = DGMQuery().anchor("fact", "f").where(pred)
        qa = QuestionAlgebra()
        qa.add("base", q_no_pred)
        qa.add("filtered", q_with)
        bdd = _bdd()
        result = find_shared_predicates(qa, bdd)
        assert result == {}

    def test_composed_cte_skipped(self):
        """Composed CTEs (raw SQL, no DGMQuery) are not WHERE-pred candidates."""
        pred = ScalarPred(PropRef("f", "region"), "=", "US")
        q1 = DGMQuery().anchor("fact", "f").where(pred)
        qa = QuestionAlgebra()
        qa.add("q1", q1)
        # Add a compose CTE
        qa.compose(
            "q1",
            __import__(
                "sqldim.core.query.dgm.algebra", fromlist=["ComposeOp"]
            ).ComposeOp.UNION,
            "q1",
            name="q_self_union",
        )
        bdd = _bdd()
        result = find_shared_predicates(qa, bdd)
        # Composed CTE is skipped; q1 appears only once as a leaf → no sharing
        assert result == {}

    def test_returns_dict_of_int_to_list(self):
        """Return type is dict mapping int → list[str]."""
        pred = ScalarPred(PropRef("f", "region"), "=", "US")
        q1 = DGMQuery().anchor("fact", "f").where(pred)
        q2 = DGMQuery().anchor("fact", "f").where(pred)
        qa = QuestionAlgebra()
        qa.add("q1", q1)
        qa.add("q2", q2)
        result = find_shared_predicates(qa, _bdd())
        assert isinstance(result, dict)
        for k, v in result.items():
            assert isinstance(k, int)
            assert isinstance(v, list)

    def test_bdd_canonical_id_comparison(self):
        """BDD equivalence relies on canonical ID — same predicate object → same ID."""
        bdd = _bdd()
        pred = ScalarPred(PropRef("f", "x"), "=", 1)
        id_a = bdd.compile(pred)
        id_b = bdd.compile(pred)
        assert id_a == id_b

    def test_complexity_linear(self):
        """n CTEs with unique predicates → O(n) detection (no blowup)."""
        qa = QuestionAlgebra()
        n = 500
        for i in range(n):
            qa.add(f"q{i}", _q_where("col", i))
        bdd = _bdd()
        result = find_shared_predicates(qa, bdd)
        # All unique — no sharing
        assert result == {}
        # n CTEs added
        assert len(qa) == n


# ---------------------------------------------------------------------------
# apply_cse — structural insertion
# ---------------------------------------------------------------------------


class TestApplyCSE:
    def test_no_sharing_returns_equivalent_algebra(self):
        """When no predicates are shared, apply_cse returns an algebra with the
        same CTE names."""
        qa = QuestionAlgebra()
        qa.add("q1", _q_where("region", "US"))
        qa.add("q2", _q_where("region", "EU"))
        bdd = _bdd()
        result = apply_cse(qa, bdd)
        assert isinstance(result, QuestionAlgebra)
        # Original CTE names preserved
        assert "q1" in result.names
        assert "q2" in result.names

    def test_shared_pred_inserts_cse_cte(self):
        """When two CTEs share a pred, apply_cse inserts a __cse_<id> CTE."""
        pred = ScalarPred(PropRef("f", "region"), "=", "US")
        q1 = DGMQuery().anchor("fact", "f").where(pred)
        q2 = DGMQuery().anchor("fact", "f").where(pred)
        qa = QuestionAlgebra()
        qa.add("q1", q1)
        qa.add("q2", q2)
        bdd = _bdd()
        result = apply_cse(qa, bdd)
        # A __cse_<n> CTE must be present
        cse_names = [n for n in result.names if n.startswith("__cse_")]
        assert len(cse_names) >= 1

    def test_cse_cte_is_composed_query(self):
        """The injected __cse_* CTE is a ComposedQuery with SQL containing WHERE."""
        pred = ScalarPred(PropRef("f", "tier"), "=", "gold")
        q1 = DGMQuery().anchor("dim_customer", "f").where(pred)
        q2 = DGMQuery().anchor("dim_customer", "f").where(pred)
        qa = QuestionAlgebra()
        qa.add("q1", q1)
        qa.add("q2", q2)
        bdd = _bdd()
        result = apply_cse(qa, bdd)
        cse_name = next(n for n in result.names if n.startswith("__cse_"))
        cse_cq = result[cse_name]
        assert isinstance(cse_cq, ComposedQuery)
        cte_sql = cse_cq.to_cte_sql()
        assert "WHERE" in cte_sql.upper()

    def test_original_ctes_preserved(self):
        """Original CTEs are still present in the result algebra."""
        pred = ScalarPred(PropRef("f", "level"), "=", "A")
        q1 = DGMQuery().anchor("dim_t", "f").where(pred)
        q2 = DGMQuery().anchor("dim_t", "f").where(pred)
        qa = QuestionAlgebra()
        qa.add("q1", q1)
        qa.add("q2", q2)
        bdd = _bdd()
        result = apply_cse(qa, bdd)
        assert "q1" in result.names
        assert "q2" in result.names

    def test_cse_cte_precedes_sharing_ctes(self):
        """The injected __cse_* CTE must appear before the CTEs that share it."""
        pred = ScalarPred(PropRef("f", "status"), "=", "active")
        q1 = DGMQuery().anchor("orders", "f").where(pred)
        q2 = DGMQuery().anchor("orders", "f").where(pred)
        qa = QuestionAlgebra()
        qa.add("q1", q1)
        qa.add("q2", q2)
        bdd = _bdd()
        result = apply_cse(qa, bdd)
        names = result.names
        cse_idx = next(i for i, n in enumerate(names) if n.startswith("__cse_"))
        q1_idx = names.index("q1")
        q2_idx = names.index("q2")
        assert cse_idx < q1_idx
        assert cse_idx < q2_idx

    def test_original_algebra_not_mutated(self):
        """apply_cse must not mutate the original algebra."""
        pred = ScalarPred(PropRef("f", "x"), "=", 99)
        q1 = DGMQuery().anchor("t", "f").where(pred)
        q2 = DGMQuery().anchor("t", "f").where(pred)
        qa = QuestionAlgebra()
        qa.add("q1", q1)
        qa.add("q2", q2)
        original_names = list(qa.names)
        bdd = _bdd()
        apply_cse(qa, bdd)
        assert qa.names == original_names

    def test_returns_question_algebra(self):
        """apply_cse always returns a QuestionAlgebra instance."""
        qa = QuestionAlgebra()
        result = apply_cse(qa, _bdd())
        assert isinstance(result, QuestionAlgebra)

    def test_multiple_shared_groups(self):
        """Two independent shared pred groups → two __cse_ CTEs injected."""
        pred_a = ScalarPred(PropRef("f", "region"), "=", "US")
        pred_b = ScalarPred(PropRef("g", "tier"), "=", "gold")
        qa = QuestionAlgebra()
        qa.add("q1", DGMQuery().anchor("t", "f").where(pred_a))
        qa.add("q2", DGMQuery().anchor("t", "f").where(pred_a))
        qa.add("q3", DGMQuery().anchor("t", "g").where(pred_b))
        qa.add("q4", DGMQuery().anchor("t", "g").where(pred_b))
        bdd = _bdd()
        result = apply_cse(qa, bdd)
        cse_names = [n for n in result.names if n.startswith("__cse_")]
        assert len(cse_names) == 2
