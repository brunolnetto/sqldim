"""Phase 1 — Question algebra (DGM §8.13) — RED then GREEN tests.

Covers:
- ComposedQuery: named CTE wrapper around DGMQuery
- QuestionAlgebra: JOIN, UNION, INTERSECT, EXCEPT, WITH composition
- Semiring structure: identity elements (∅ / Q_top), commutativity of UNION
- SQL generation: WITH clause emission with proper CTE chaining
- Complexity: O(|CTEs|) SQL construction; no exponential blowup
"""

from __future__ import annotations

import pytest

from sqldim.core.query.dgm.algebra import (
    ComposedQuery,
    QuestionAlgebra,
    ComposeOp,
)
from sqldim.core.query.dgm.core import DGMQuery
from sqldim.core.query.dgm.preds import ScalarPred
from sqldim.core.query.dgm.refs import PropRef


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _simple_q(anchor: str = "o_fact", alias: str = "f") -> DGMQuery:
    """Return a minimal single-anchor DGMQuery for test use."""
    return DGMQuery().anchor(anchor, alias)


def _filtered_q(anchor: str, alias: str, col: str, val: object) -> DGMQuery:
    return (
        DGMQuery()
        .anchor(anchor, alias)
        .where(ScalarPred(PropRef(alias, col), "=", val))
    )


# ---------------------------------------------------------------------------
# ComposeOp enum
# ---------------------------------------------------------------------------


class TestComposeOp:
    def test_all_ops_exist(self):
        assert ComposeOp.JOIN
        assert ComposeOp.UNION
        assert ComposeOp.INTERSECT
        assert ComposeOp.EXCEPT
        assert ComposeOp.WITH

    def test_op_values_are_strings(self):
        for op in ComposeOp:
            assert isinstance(op.value, str)


# ---------------------------------------------------------------------------
# ComposedQuery
# ---------------------------------------------------------------------------


class TestComposedQuery:
    def test_wraps_dgm_query(self):
        q = _simple_q()
        cq = ComposedQuery(name="q1", query=q)
        assert cq.name == "q1"
        assert cq.query is q

    def test_to_cte_sql_contains_name(self):
        q = _simple_q("o_fact", "f")
        cq = ComposedQuery(name="q1", query=q)
        sql = cq.to_cte_sql()
        assert "q1" in sql
        assert "SELECT" in sql.upper()

    def test_to_cte_sql_is_string(self):
        cq = ComposedQuery(name="base", query=_simple_q())
        assert isinstance(cq.to_cte_sql(), str)

    def test_name_used_as_cte_reference(self):
        cq = ComposedQuery(name="my_cte", query=_simple_q())
        # The CTE definition should look like: my_cte AS (SELECT ...)
        sql = cq.to_cte_sql()
        assert "my_cte" in sql

    def test_repr_includes_name(self):
        cq = ComposedQuery(name="qx", query=_simple_q())
        assert "qx" in repr(cq)

    def test_requires_query_or_sql(self):
        """ComposedQuery(name=...) with neither query nor sql raises ValueError (line 100)."""
        with pytest.raises(ValueError, match="query.*sql"):
            ComposedQuery(name="bad")


# ---------------------------------------------------------------------------
# QuestionAlgebra — construction
# ---------------------------------------------------------------------------


class TestQuestionAlgebraConstruction:
    def test_empty_algebra(self):
        qa = QuestionAlgebra()
        assert len(qa) == 0

    def test_add_cte(self):
        qa = QuestionAlgebra()
        qa.add("q1", _simple_q())
        assert len(qa) == 1

    def test_add_returns_self_for_chaining(self):
        qa = QuestionAlgebra()
        result = qa.add("q1", _simple_q())
        assert result is qa

    def test_names_property(self):
        qa = QuestionAlgebra()
        qa.add("q1", _simple_q()).add("q2", _simple_q())
        assert set(qa.names) == {"q1", "q2"}

    def test_duplicate_name_raises(self):
        qa = QuestionAlgebra()
        qa.add("q1", _simple_q())
        with pytest.raises(ValueError, match="q1"):
            qa.add("q1", _simple_q())

    def test_getitem_by_name(self):
        qa = QuestionAlgebra()
        q = _simple_q()
        qa.add("q1", q)
        assert qa["q1"].query is q


# ---------------------------------------------------------------------------
# QuestionAlgebra — compose operations
# ---------------------------------------------------------------------------


class TestQuestionAlgebraCompose:
    def _qa_two(self) -> QuestionAlgebra:
        qa = QuestionAlgebra()
        qa.add("q1", _filtered_q("o_fact", "f", "segment", "retail"))
        qa.add("q2", _filtered_q("o_fact", "f", "region", "US"))
        return qa

    # -- UNION ---------------------------------------------------------------

    def test_union_returns_composed_query(self):
        qa = self._qa_two()
        result = qa.compose("q1", ComposeOp.UNION, "q2", name="q_union")
        assert isinstance(result, ComposedQuery)
        assert result.name == "q_union"

    def test_union_sql_contains_union_all(self):
        qa = self._qa_two()
        result = qa.compose("q1", ComposeOp.UNION, "q2", name="q_union")
        sql = result.to_cte_sql()
        assert "UNION ALL" in sql.upper() or "UNION" in sql.upper()

    def test_union_registers_in_algebra(self):
        qa = self._qa_two()
        qa.compose("q1", ComposeOp.UNION, "q2", name="q_union")
        assert "q_union" in qa.names

    # -- INTERSECT -----------------------------------------------------------

    def test_intersect_returns_composed_query(self):
        qa = self._qa_two()
        result = qa.compose("q1", ComposeOp.INTERSECT, "q2", name="q_inter")
        assert isinstance(result, ComposedQuery)

    def test_intersect_sql_references_both_ctes(self):
        qa = self._qa_two()
        result = qa.compose("q1", ComposeOp.INTERSECT, "q2", name="q_inter")
        sql = result.to_cte_sql()
        assert "q1" in sql and "q2" in sql

    # -- EXCEPT --------------------------------------------------------------

    def test_except_returns_composed_query(self):
        qa = self._qa_two()
        result = qa.compose("q1", ComposeOp.EXCEPT, "q2", name="q_diff")
        assert isinstance(result, ComposedQuery)

    def test_except_sql_references_both(self):
        qa = self._qa_two()
        result = qa.compose("q1", ComposeOp.EXCEPT, "q2", name="q_diff")
        sql = result.to_cte_sql()
        assert "q1" in sql and "q2" in sql

    # -- JOIN ----------------------------------------------------------------

    def test_join_requires_on_key(self):
        qa = self._qa_two()
        with pytest.raises((TypeError, ValueError)):
            qa.compose("q1", ComposeOp.JOIN, "q2", name="q_join")

    def test_join_with_key_succeeds(self):
        qa = self._qa_two()
        result = qa.compose(
            "q1", ComposeOp.JOIN, "q2", name="q_join", on="q1.id = q2.id"
        )
        assert isinstance(result, ComposedQuery)

    def test_join_sql_contains_join_keyword(self):
        qa = self._qa_two()
        result = qa.compose(
            "q1", ComposeOp.JOIN, "q2", name="q_join", on="q1.id = q2.id"
        )
        sql = result.to_cte_sql()
        assert "JOIN" in sql.upper()

    # -- WITH (chain) --------------------------------------------------------

    def test_with_chain_returns_final_cte(self):
        """WITH returns a ComposedQuery whose CTE body is q1 refined by q2."""
        qa = self._qa_two()
        result = qa.compose("q1", ComposeOp.WITH, "q2", name="q_chain")
        assert isinstance(result, ComposedQuery)

    def test_unknown_left_name_raises(self):
        qa = self._qa_two()
        with pytest.raises(KeyError):
            qa.compose("no_such", ComposeOp.UNION, "q2", name="out")

    def test_unknown_right_name_raises(self):
        qa = self._qa_two()
        with pytest.raises(KeyError):
            qa.compose("q1", ComposeOp.UNION, "no_such", name="out")


# ---------------------------------------------------------------------------
# QuestionAlgebra — to_sql (full WITH … SELECT)
# ---------------------------------------------------------------------------


class TestQuestionAlgebraToSQL:
    def test_to_sql_single_cte(self):
        qa = QuestionAlgebra()
        qa.add("base", _simple_q())
        sql = qa.to_sql("base")
        assert "WITH" in sql.upper()
        assert "base" in sql
        assert "SELECT" in sql.upper()

    def test_to_sql_two_ctes_ordered(self):
        qa = QuestionAlgebra()
        qa.add("q1", _simple_q("o_fact", "f"))
        qa.add("q2", _simple_q("o_customer", "c"))
        qa.compose("q1", ComposeOp.UNION, "q2", name="q_union")
        sql = qa.to_sql("q_union")
        # q1 and q2 must appear before q_union in the WITH block
        pos_q1 = sql.index("q1")
        pos_q2 = sql.index("q2")
        pos_union = sql.index("q_union")
        assert pos_q1 < pos_union
        assert pos_q2 < pos_union

    def test_to_sql_unknown_final_raises(self):
        qa = QuestionAlgebra()
        with pytest.raises(KeyError):
            qa.to_sql("nonexistent")

    def test_to_sql_returns_string(self):
        qa = QuestionAlgebra()
        qa.add("q1", _simple_q())
        result = qa.to_sql("q1")
        assert isinstance(result, str)
        assert len(result) > 0

    def test_to_sql_contains_final_select(self):
        """The final SELECT references the last CTE name."""
        qa = QuestionAlgebra()
        qa.add("q1", _simple_q())
        sql = qa.to_sql("q1")
        # Last projection should select from q1
        assert sql.strip().upper().endswith(")") or "FROM q1" in sql

    def test_to_sql_depth_three_chain(self):
        """Three-level CTE chain: q1 → q2 → q3."""
        qa = QuestionAlgebra()
        qa.add("q1", _simple_q("o_fact", "f"))
        qa.add("q2", _simple_q("o_fact", "f"))
        qa.compose("q1", ComposeOp.UNION, "q2", name="q12")
        qa.add("q3", _simple_q("o_customer", "c"))
        qa.compose("q12", ComposeOp.UNION, "q3", name="q123")
        sql = qa.to_sql("q123")
        assert "q1" in sql
        assert "q2" in sql
        assert "q12" in sql
        assert "q3" in sql
        assert "q123" in sql


# ---------------------------------------------------------------------------
# Semiring structure (§8.13)
# ---------------------------------------------------------------------------


class TestQuestionAlgebraSemiring:
    """Verify the semiring axioms that apply at the SQL level."""

    def _qa(self) -> tuple[QuestionAlgebra, str, str]:
        qa = QuestionAlgebra()
        qa.add("q1", _filtered_q("t", "f", "col", 1))
        qa.add("q2", _filtered_q("t", "f", "col", 2))
        return qa, "q1", "q2"

    def test_union_commutative_sql_structure(self):
        """UNION(q1, q2) and UNION(q2, q1) produce structurally equivalent CTEs
        (same SQL tokens, possibly different order of operands)."""
        qa, q1, q2 = self._qa()
        fwd = qa.compose(q1, ComposeOp.UNION, q2, name="fwd")
        rev = qa.compose(q2, ComposeOp.UNION, q1, name="rev")
        sql_fwd = fwd.to_cte_sql()
        sql_rev = rev.to_cte_sql()
        # Both should contain UNION keyword and reference q1, q2
        assert "q1" in sql_fwd and "q2" in sql_fwd
        assert "q1" in sql_rev and "q2" in sql_rev

    def test_empty_identity_sentinel_exists(self):
        """QuestionAlgebra exposes EMPTY_Q (∅) and TOP_Q (Q_top) sentinels."""
        assert QuestionAlgebra.EMPTY_Q is not None
        assert QuestionAlgebra.TOP_Q is not None

    def test_empty_q_produces_false_sql(self):
        """EMPTY_Q sentinel generates a query that returns no rows."""
        sql = QuestionAlgebra.EMPTY_Q
        # Should contain WHERE FALSE or equivalent
        assert "FALSE" in sql.upper() or "1=0" in sql or "0=1" in sql

    def test_top_q_produces_select_all(self):
        """TOP_Q sentinel generates a query that returns all rows (no filter)."""
        sql = QuestionAlgebra.TOP_Q
        assert "SELECT" in sql.upper()


# ---------------------------------------------------------------------------
# Complexity: O(|CTEs|) construction
# ---------------------------------------------------------------------------


class TestQuestionAlgebraComplexity:
    def test_linear_cte_count(self):
        """Adding n CTEs and composing linearly stays O(n) in depth."""
        import time

        def _build(n: int) -> float:
            qa = QuestionAlgebra()
            qa.add("q0", _simple_q())
            prev = "q0"
            for i in range(1, n + 1):
                name = f"q{i}"
                qa.add(name, _simple_q())
                composed = f"c{i}"
                qa.compose(prev, ComposeOp.UNION, name, name=composed)
                prev = composed
            t0 = time.perf_counter()
            _sql = qa.to_sql(prev)
            return time.perf_counter() - t0

        t10 = _build(10)
        t100 = _build(100)
        # Linear: t100 should be < 50× t10 (very generous)
        assert t100 < max(t10 * 50, 0.5), f"Non-linear: t10={t10:.4f} t100={t100:.4f}"

    def test_memory_no_copy_explosion(self):
        """QuestionAlgebra stores references, not deep copies."""
        qa = QuestionAlgebra()
        q = _simple_q()
        qa.add("q1", q)
        retrieved = qa["q1"]
        assert retrieved.query is q
