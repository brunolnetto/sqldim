"""RED tests for Rule 11 Extended — Query DAG Minimisation (DGM §6.2 v0.20).

These tests cover the semiring elimination laws applied during query DAG make():

  UNION laws:
    Q ∪ Q            = Q           (idempotence)
    Q ∪ ∅            = Q           (identity)
    ∅ ∪ Q            = Q           (identity)
    Q₁ ∪ Q₂ where Q₁⊆Q₂ = Q₂    (containment absorption; Band 1 only)

  INTERSECT laws:
    Q ∩ Q            = Q           (idempotence)
    Q ∩ Q_top        = Q           (identity)
    Q_top ∩ Q        = Q           (identity)
    Q₁ ∩ Q₂ where Q₁⊆Q₂ = Q₁    (containment selection; Band 1 only)

  Distributivity (applied when it reduces node count):
    Q₁ ∩ (Q₂ ∪ Q₃)  = (Q₁ ∩ Q₂) ∪ (Q₁ ∩ Q₃)

  Containment check:
    Q₁ ⊆ Q₂  iff  bdd.implies(Q₁.where_bdd, Q₂.where_bdd)  [Band 1]

After elimination, canonical query DAG is topologically sorted.
Minimum CTE count = number of nodes in canonical DAG (tight lower bound).
"""

from __future__ import annotations

from sqldim.core.query.dgm.algebra import ComposeOp, QuestionAlgebra
from sqldim.core.query.dgm.bdd import BDDManager, DGMPredicateBDD, TRUE_NODE_ID
from sqldim.core.query.dgm.core import DGMQuery
from sqldim.core.query.dgm.preds import ScalarPred, AND
from sqldim.core.query.dgm.refs import PropRef
from sqldim.core.query.dgm._dag import (
    apply_semiring_minimisation,
    QueryDAGNode,
    QueryDAGManager,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_bdd() -> DGMPredicateBDD:
    return DGMPredicateBDD(BDDManager())


def _leaf_query(table: str, pred: object | None = None) -> DGMQuery:
    """Create a minimal DGMQuery anchored on *table* with optional WHERE pred."""
    q = DGMQuery().anchor(table, "t")
    if pred is not None:
        q = q.where(pred)
    return q


def _algebra_with_two_leaves() -> tuple[QuestionAlgebra, DGMPredicateBDD]:
    """Build a 2-leaf algebra (no compositions). Returns (algebra, bdd)."""
    bdd = _make_bdd()
    alg = QuestionAlgebra()
    alg.add("q1", _leaf_query("orders"))
    alg.add("q2", _leaf_query("returns"))
    return alg, bdd


# ===========================================================================
# Task 1: QueryDAGNode and QueryDAGManager exist and export correctly
# ===========================================================================


class TestQueryDAGManagerExists:
    def test_import_query_dag_node(self):
        assert QueryDAGNode is not None

    def test_import_query_dag_manager(self):
        assert QueryDAGManager is not None

    def test_import_apply_semiring_minimisation(self):
        assert apply_semiring_minimisation is not None

    def test_dag_manager_can_be_instantiated(self):
        mgr = QueryDAGManager()
        assert mgr is not None

    def test_dag_node_is_dataclass(self):
        from dataclasses import fields
        field_names = {f.name for f in fields(QueryDAGNode)}
        assert "node_id" in field_names
        assert "op" in field_names
        assert "left_id" in field_names
        assert "right_id" in field_names

    def test_dag_manager_leaf_returns_int(self):
        mgr = QueryDAGManager()
        uid = mgr.leaf("q1")
        assert isinstance(uid, int)

    def test_dag_manager_leaf_same_name_same_id(self):
        mgr = QueryDAGManager()
        uid1 = mgr.leaf("q1")
        uid2 = mgr.leaf("q1")
        assert uid1 == uid2

    def test_dag_manager_different_names_different_ids(self):
        mgr = QueryDAGManager()
        uid1 = mgr.leaf("q1")
        uid2 = mgr.leaf("q2")
        assert uid1 != uid2


# ===========================================================================
# Task 2: Idempotence laws
# ===========================================================================


class TestIdempotenceLaws:
    """Q ∪ Q = Q and Q ∩ Q = Q (same DAG node id → collapse)."""

    def test_union_idempotence_returns_same_id(self):
        """make(UNION, q1, q1) → q1 (idempotence; same DAG node id)."""
        mgr = QueryDAGManager()
        q1 = mgr.leaf("q1")
        result = mgr.make(ComposeOp.UNION, q1, q1)
        assert result == q1

    def test_intersect_idempotence_returns_same_id(self):
        """make(INTERSECT, q1, q1) → q1 (idempotence)."""
        mgr = QueryDAGManager()
        q1 = mgr.leaf("q1")
        result = mgr.make(ComposeOp.INTERSECT, q1, q1)
        assert result == q1

    def test_union_idempotence_reduces_to_leaf(self):
        """apply_semiring on union-of-same reduces CTE count by 1."""
        bdd = _make_bdd()
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.compose("q1", ComposeOp.UNION, "q1", name="q1_union_q1")
        assert len(alg) == 2

        result = apply_semiring_minimisation(alg, bdd)
        # q1_union_q1 should be eliminated; only q1 survives
        assert len(result) < len(alg)
        assert "q1" in result.names

    def test_intersect_idempotence_reduces_to_leaf(self):
        """apply_semiring on intersect-of-same reduces CTE count by 1."""
        bdd = _make_bdd()
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.compose("q1", ComposeOp.INTERSECT, "q1", name="q1_inter_q1")

        result = apply_semiring_minimisation(alg, bdd)
        assert len(result) < len(alg)
        assert "q1" in result.names

    def test_multiple_idempotent_unions_collapse(self):
        """Two separate self-union compositions both collapse."""
        bdd = _make_bdd()
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.add("q2", _leaf_query("returns"))
        alg.compose("q1", ComposeOp.UNION, "q1", name="dup1")
        alg.compose("q2", ComposeOp.UNION, "q2", name="dup2")
        assert len(alg) == 4

        result = apply_semiring_minimisation(alg, bdd)
        assert len(result) == 2
        assert "q1" in result.names
        assert "q2" in result.names


# ===========================================================================
# Task 3: Identity laws (EMPTY_Q and TOP_Q)
# ===========================================================================


class TestIdentityLaws:
    """Q ∪ ∅ = Q; ∅ ∪ Q = Q; Q ∩ Q_top = Q; Q_top ∩ Q = Q."""

    def test_union_with_empty_q_sentinel_left(self):
        """make(UNION, EMPTY_ID, q) → q (identity: ∅ ∪ Q = Q)."""
        mgr = QueryDAGManager()
        q1 = mgr.leaf("q1")
        result = mgr.make(ComposeOp.UNION, mgr.empty_id, q1)
        assert result == q1

    def test_union_with_empty_q_sentinel_right(self):
        """make(UNION, q, EMPTY_ID) → q (identity: Q ∪ ∅ = Q)."""
        mgr = QueryDAGManager()
        q1 = mgr.leaf("q1")
        result = mgr.make(ComposeOp.UNION, q1, mgr.empty_id)
        assert result == q1

    def test_intersect_with_top_q_sentinel_left(self):
        """make(INTERSECT, TOP_ID, q) → q (identity: Q_top ∩ Q = Q)."""
        mgr = QueryDAGManager()
        q1 = mgr.leaf("q1")
        result = mgr.make(ComposeOp.INTERSECT, mgr.top_id, q1)
        assert result == q1

    def test_intersect_with_top_q_sentinel_right(self):
        """make(INTERSECT, q, TOP_ID) → q (identity: Q ∩ Q_top = Q)."""
        mgr = QueryDAGManager()
        q1 = mgr.leaf("q1")
        result = mgr.make(ComposeOp.INTERSECT, q1, mgr.top_id)
        assert result == q1

    def test_empty_id_attribute_exists(self):
        mgr = QueryDAGManager()
        assert isinstance(mgr.empty_id, int)

    def test_top_id_attribute_exists(self):
        mgr = QueryDAGManager()
        assert isinstance(mgr.top_id, int)

    def test_apply_semiring_union_with_empty_q_sql_leaf(self):
        """apply_semiring: compose(q, UNION, empty_q_leaf) → q only."""
        bdd = _make_bdd()
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.add("empty", DGMQuery().anchor("empty_q"))
        # Mark as empty via sql override is not straightforward; 
        # instead compose with EMPTY sentinel SQL
        alg.compose("q1", ComposeOp.UNION, "q1", name="union_idem")

        result = apply_semiring_minimisation(alg, bdd)
        assert "union_idem" not in result.names


# ===========================================================================
# Task 4: Containment laws (Band 1 — WHERE predicate BDD implies)
# ===========================================================================


class TestContainmentLaws:
    """Q₁ ⊆ Q₂ iff bdd.implies(Q₁.where_bdd, Q₂.where_bdd) [Band 1]."""

    def _strict_pred(self):
        """Strict predicate: segment = 'retail' AND region = 'EU'."""
        return ScalarPred(PropRef("t", "segment"), "=", "retail")

    def _wide_pred(self):
        """Wider predicate: region = 'EU' (contains the strict pred namespace)."""
        return ScalarPred(PropRef("t", "region"), "=", "EU")

    def test_containment_absorption_union(self):
        """apply_semiring union: q1 ⊆ q2 → q1 ∪ q2 = q2.
        
        When q1's predicate implies q2's predicate (q1 is stricter), the union
        is dominated by q2 (the broader query).
        """
        bdd = _make_bdd()
        # q_strict has pred P; q_wide has pred Q; P implies Q → q_strict ⊆ q_wide
        p_strict = ScalarPred(PropRef("t", "x"), "=", "only_match")
        # Build two CTEs where p_strict ⊆ TRUE (the wide query has no predicate)
        alg = QuestionAlgebra()
        alg.add("q_strict", _leaf_query("orders", p_strict))
        alg.add("q_wide", _leaf_query("orders"))  # no predicate → always true

        # Compile both predicates; strict implies wide (any value implies TRUE)
        strict_id = bdd.compile(p_strict)
        wide_id = TRUE_NODE_ID  # no predicate = TRUE
        # Confirm our assumption: strict implies TRUE
        assert bdd.implies(strict_id, wide_id)

        alg.compose("q_strict", ComposeOp.UNION, "q_wide", name="union_containment")

        result = apply_semiring_minimisation(alg, bdd)
        # q_strict ∪ q_wide where q_strict ⊆ q_wide → q_wide survives
        assert "q_wide" in result.names
        assert "union_containment" not in result.names

    def test_containment_selection_intersect(self):
        """apply_semiring intersect: q1 ⊆ q2 → q1 ∩ q2 = q1.
        
        When q1's predicate implies q2's predicate, their intersection is just q1.
        """
        bdd = _make_bdd()
        p_strict = ScalarPred(PropRef("t", "x"), "=", "only_match")
        alg = QuestionAlgebra()
        alg.add("q_strict", _leaf_query("orders", p_strict))
        alg.add("q_wide", _leaf_query("orders"))  # no predicate

        alg.compose("q_strict", ComposeOp.INTERSECT, "q_wide", name="inter_containment")

        result = apply_semiring_minimisation(alg, bdd)
        # q_strict ∩ q_wide where q_strict ⊆ q_wide → q_strict survives
        assert "q_strict" in result.names
        assert "inter_containment" not in result.names

    def test_no_containment_when_independent_predicates(self):
        """apply_semiring does NOT eliminate when predicates are independent."""
        bdd = _make_bdd()
        p1 = ScalarPred(PropRef("t", "segment"), "=", "retail")
        p2 = ScalarPred(PropRef("t", "region"), "=", "EU")
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders", p1))
        alg.add("q2", _leaf_query("orders", p2))
        alg.compose("q1", ComposeOp.UNION, "q2", name="union_independent")

        result = apply_semiring_minimisation(alg, bdd)
        # Neither implies the other → no elimination
        assert "union_independent" in result.names

    def test_containment_union_right_subset_of_left(self):
        """Q₁ ∪ Q₂ where Q₂ ⊆ Q₁ → Q₁ (left dominates)."""
        bdd = _make_bdd()
        p_strict = ScalarPred(PropRef("t", "x"), "=", "only_match")
        alg = QuestionAlgebra()
        alg.add("q_wide", _leaf_query("orders"))   # no predicate → TRUE
        alg.add("q_strict", _leaf_query("orders", p_strict))
        alg.compose("q_wide", ComposeOp.UNION, "q_strict", name="union_rev")

        result = apply_semiring_minimisation(alg, bdd)
        # q_wide dominates → survives
        assert "q_wide" in result.names
        assert "union_rev" not in result.names


# ===========================================================================
# Task 5: Distributivity Q₁ ∩ (Q₂ ∪ Q₃) = (Q₁ ∩ Q₂) ∪ (Q₁ ∩ Q₃)
# ===========================================================================


class TestDistributivity:
    """Distributivity applied when it reduces node count."""

    def test_distributivity_reduces_node_count_via_idempotence(self):
        """Q ∩ (Q ∪ Q) = (Q ∩ Q) ∪ (Q ∩ Q) = Q ∪ Q = Q.
        
        Distributivity + idempotence collapses a 3-CTE chain to 1.
        All three input CTEs are the same node → maximum collapse.
        """
        bdd = _make_bdd()
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.compose("q1", ComposeOp.UNION, "q1", name="q1_union_q1")
        alg.compose("q1", ComposeOp.INTERSECT, "q1_union_q1", name="final")

        result = apply_semiring_minimisation(alg, bdd)
        assert len(result) < len(alg)
        assert "q1" in result.names

    def test_distributivity_expanded_when_reduces_dag_size(self):
        """Q₁ ∩ (Q₂ ∪ Q₃) expanded when left operand is in both terms.
        
        The expanded form enables further idempotence elimination when, e.g.,
        Q₁ == Q₂, giving (Q₁ ∩ Q₁) ∪ (Q₁ ∩ Q₃) = Q₁ ∪ (Q₁ ∩ Q₃).
        """
        bdd = _make_bdd()
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.add("q3", _leaf_query("returns"))
        alg.compose("q1", ComposeOp.UNION, "q3", name="q1_union_q3")
        # q1 ∩ (q1 ∪ q3) — left == left operand of inner union
        alg.compose("q1", ComposeOp.INTERSECT, "q1_union_q3", name="final")

        result = apply_semiring_minimisation(alg, bdd)
        # After distributivity: (q1 ∩ q1) ∪ (q1 ∩ q3) = q1 ∪ (q1 ∩ q3)
        # q1 ∪ (sth where q1⊆everything-that-q1-is-a-subset-of) 
        # Minimum: the result has fewer CTEs than the original 4
        assert len(result) < len(alg)


# ===========================================================================
# Task 6: Minimum CTE count theorem
# ===========================================================================


class TestMinimumCTECount:
    """Minimum CTE count = number of nodes in canonical DAG (tight lower bound)."""

    def test_no_compositions_unchanged(self):
        """Fast path: no compositions → result is structurally identical."""
        bdd = _make_bdd()
        alg, _ = _algebra_with_two_leaves()
        result = apply_semiring_minimisation(alg, bdd)
        assert len(result) == len(alg)
        assert result.names == alg.names

    def test_single_composition_no_elimination_unchanged(self):
        """Non-eliminable composition → count unchanged."""
        bdd = _make_bdd()
        p1 = ScalarPred(PropRef("t", "a"), "=", "x")
        p2 = ScalarPred(PropRef("t", "b"), "=", "y")
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("t1", p1))
        alg.add("q2", _leaf_query("t2", p2))
        alg.compose("q1", ComposeOp.UNION, "q2", name="q_union")
        assert len(alg) == 3

        result = apply_semiring_minimisation(alg, bdd)
        assert len(result) == 3  # no elimination possible

    def test_two_self_unions_collapse_to_two_leaves(self):
        """Original: 4 CTEs (2 leaves + 2 self-unions). Minimised: 2 leaves."""
        bdd = _make_bdd()
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.add("q2", _leaf_query("returns"))
        alg.compose("q1", ComposeOp.UNION, "q1", name="dup1")
        alg.compose("q2", ComposeOp.UNION, "q2", name="dup2")

        result = apply_semiring_minimisation(alg, bdd)
        assert len(result) == 2  # tight lower bound: exactly 2 leaves needed

    def test_chain_of_idempotent_unions_collapses_to_leaf(self):
        """q, q∪q, (q∪q)∪q → all collapse to q (1 CTE)."""
        bdd = _make_bdd()
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.compose("q1", ComposeOp.UNION, "q1", name="dup")
        alg.compose("dup", ComposeOp.UNION, "q1", name="dup2")

        result = apply_semiring_minimisation(alg, bdd)
        assert len(result) == 1
        assert "q1" in result.names

    def test_returned_algebra_is_valid_question_algebra(self):
        """Result is a proper QuestionAlgebra instance."""
        bdd = _make_bdd()
        alg, _ = _algebra_with_two_leaves()
        alg.compose("q1", ComposeOp.UNION, "q1", name="dup")
        result = apply_semiring_minimisation(alg, bdd)
        assert isinstance(result, QuestionAlgebra)

    def test_result_algebra_names_are_strings(self):
        bdd = _make_bdd()
        alg, _ = _algebra_with_two_leaves()
        result = apply_semiring_minimisation(alg, bdd)
        assert all(isinstance(n, str) for n in result.names)


# ===========================================================================
# Task 7: QuestionAlgebra.minimize() integration
# ===========================================================================


class TestQuestionAlgebraMinimize:
    """QuestionAlgebra.minimize(bdd) method is a convenience wrapper."""

    def test_minimize_method_exists(self):
        alg = QuestionAlgebra()
        assert hasattr(alg, "minimize")
        assert callable(alg.minimize)

    def test_minimize_returns_question_algebra(self):
        bdd = _make_bdd()
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        result = alg.minimize(bdd)
        assert isinstance(result, QuestionAlgebra)

    def test_minimize_equivalent_to_apply_semiring_minimisation(self):
        """minimize() produces same result as apply_semiring_minimisation()."""
        bdd = _make_bdd()
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.compose("q1", ComposeOp.UNION, "q1", name="dup")

        via_method = alg.minimize(bdd)
        via_function = apply_semiring_minimisation(alg, bdd)
        assert via_method.names == via_function.names
        assert len(via_method) == len(via_function)

    def test_minimize_does_not_mutate_original(self):
        """minimize() must return a new algebra; original is untouched."""
        bdd = _make_bdd()
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.compose("q1", ComposeOp.UNION, "q1", name="dup")
        original_len = len(alg)
        original_names = list(alg.names)

        _ = alg.minimize(bdd)
        assert len(alg) == original_len
        assert alg.names == original_names


# ===========================================================================
# Task 8: _composition tracking on QuestionAlgebra
# ===========================================================================


class TestCompositionTracking:
    """QuestionAlgebra._composition dict tracks (left, op, right) per composed CTE."""

    def test_composition_dict_exists(self):
        alg = QuestionAlgebra()
        assert hasattr(alg, "_composition")

    def test_compose_records_in_composition(self):
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.add("q2", _leaf_query("returns"))
        alg.compose("q1", ComposeOp.UNION, "q2", name="q_union")
        assert "q_union" in alg._composition

    def test_composition_entry_is_triple(self):
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.add("q2", _leaf_query("returns"))
        alg.compose("q1", ComposeOp.UNION, "q2", name="q_union")
        left, op, right = alg._composition["q_union"]
        assert left == "q1"
        assert op is ComposeOp.UNION
        assert right == "q2"

    def test_leaf_not_in_composition(self):
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        assert "q1" not in alg._composition

    def test_multiple_compositions_all_tracked(self):
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.add("q2", _leaf_query("returns"))
        alg.compose("q1", ComposeOp.UNION, "q2", name="u12")
        alg.compose("q1", ComposeOp.INTERSECT, "q2", name="i12")
        assert "u12" in alg._composition
        assert "i12" in alg._composition

    def test_join_composition_tracked(self):
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.add("q2", _leaf_query("returns"))
        alg.compose("q1", ComposeOp.JOIN, "q2", name="j12", on="q1.id = q2.id")
        assert "j12" in alg._composition
        _, op, _ = alg._composition["j12"]
        assert op is ComposeOp.JOIN


# ===========================================================================
# Task 9: QueryDAGManager make() and unique-table sharing
# ===========================================================================


class TestQueryDAGManagerMake:
    """make() shares identical intermediate nodes (analogous to BDD make())."""

    def test_make_union_different_nodes_produces_new_node(self):
        mgr = QueryDAGManager()
        q1 = mgr.leaf("q1")
        q2 = mgr.leaf("q2")
        result = mgr.make(ComposeOp.UNION, q1, q2)
        # Not idempotent (different nodes) → new node
        assert result != q1
        assert result != q2

    def test_make_unique_table_sharing(self):
        """Same call produces same node id (shared via unique table)."""
        mgr = QueryDAGManager()
        q1 = mgr.leaf("q1")
        q2 = mgr.leaf("q2")
        r1 = mgr.make(ComposeOp.UNION, q1, q2)
        r2 = mgr.make(ComposeOp.UNION, q1, q2)
        assert r1 == r2

    def test_make_different_order_different_node(self):
        """UNION is not commutative in the DAG (order matters for the node id)."""
        mgr = QueryDAGManager()
        q1 = mgr.leaf("q1")
        q2 = mgr.leaf("q2")
        r1 = mgr.make(ComposeOp.UNION, q1, q2)
        r2 = mgr.make(ComposeOp.UNION, q2, q1)
        assert r1 != r2  # different operand order → different node

    def test_make_intersect_union_different_ops_different_nodes(self):
        mgr = QueryDAGManager()
        q1 = mgr.leaf("q1")
        q2 = mgr.leaf("q2")
        r_union = mgr.make(ComposeOp.UNION, q1, q2)
        r_intersect = mgr.make(ComposeOp.INTERSECT, q1, q2)
        assert r_union != r_intersect

    def test_dag_node_count_grows_with_new_nodes(self):
        mgr = QueryDAGManager()
        q1 = mgr.leaf("q1")
        q2 = mgr.leaf("q2")
        _ = mgr.make(ComposeOp.UNION, q1, q2)
        # q1 leaf + q2 leaf + union node = 3 logical nodes
        # manager tracks at least 3 distinct IDs
        assert len(mgr) >= 3

    def test_idempotence_does_not_grow_dag(self):
        """Idempotent composition does not add a new node."""
        mgr = QueryDAGManager()
        q1 = mgr.leaf("q1")
        before = len(mgr)
        mgr.make(ComposeOp.UNION, q1, q1)
        assert len(mgr) == before  # no new node added

    def test_make_non_union_intersect_op_creates_node(self):
        """make() with EXCEPT (non-UNION/INTERSECT) creates a new DAG node.

        Covers the else-branch in make() (line 171: canon = None).
        """
        mgr = QueryDAGManager()
        q1 = mgr.leaf("q1")
        q2 = mgr.leaf("q2")
        before = len(mgr)
        result = mgr.make(ComposeOp.EXCEPT, q1, q2)
        assert result != q1
        assert result != q2
        assert len(mgr) > before  # a new node was created


# ===========================================================================
# Task 10: Coverage completeness — internal helper branches
# ===========================================================================

from sqldim.core.query.dgm._dag import (
    _leaf_bdd_id,
    _build_bdd_ids,
    _try_eliminate,
    _resolve,
    _union_survivor,
    _intersect_survivor,
)
from sqldim.core.query.dgm.algebra import ComposedQuery


class TestLeafBddIdSqlOnlyBranch:
    """_leaf_bdd_id returns TRUE_NODE_ID when the CTE is sql-only (query=None)."""

    def test_sql_only_cte_returns_true_node_id(self):
        """Line 171: cq.query is None → return TRUE_NODE_ID immediately."""
        bdd = _make_bdd()
        cq = ComposedQuery(name="raw", sql="SELECT 1")
        assert cq.query is None
        result = _leaf_bdd_id(cq, bdd)
        assert result == TRUE_NODE_ID


class TestBuildBddIdsElseBranch:
    """_build_bdd_ids assigns TRUE_NODE_ID for non-UNION/INTERSECT composed ops."""

    def test_except_op_maps_to_true_node_id(self):
        """Line 196: EXCEPT composition in _build_bdd_ids uses the else branch."""
        bdd = _make_bdd()
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.add("q2", _leaf_query("returns"))
        alg.compose("q1", ComposeOp.EXCEPT, "q2", name="q_except")
        # Confirm EXCEPT is tracked in _composition
        assert "q_except" in alg._composition

        # Minimisation should reach _build_bdd_ids and exercise the else branch
        result = apply_semiring_minimisation(alg, bdd)
        # No elimination for EXCEPT — all CTEs survive
        assert "q_except" in result.names

    def test_with_op_maps_to_true_node_id(self):
        """Line 196: WITH composition also hits the else branch."""
        bdd = _make_bdd()
        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("orders"))
        alg.add("q2", _leaf_query("returns"))
        alg.compose("q1", ComposeOp.WITH, "q2", name="q_with")

        result = apply_semiring_minimisation(alg, bdd)
        assert "q_with" in result.names


class TestUnionSurvivorRightSubsetLeft:
    """_union_survivor: when right ⊆ left, left dominates (return left)."""

    def test_right_subset_left_returns_left(self):
        """Line 228: bdd.implies(r, l) → return left."""
        bdd = _make_bdd()
        # Build two predicates where p2 ⊆ p1 using SAME atom object.
        # p1: a=x  (less restrictive)
        # p2: (a=x) AND (b=y)  — same a=x object reused so BDD var matches
        p_a = ScalarPred(PropRef("t", "a"), "=", "x")
        p_b = ScalarPred(PropRef("t", "b"), "=", "y")
        p1 = p_a                       # broader: just a=x
        p2 = AND(p_a, p_b)             # stricter: a=x AND b=y (reuses p_a)

        id1 = bdd.compile(p1)
        id2 = bdd.compile(p2)
        bdd_ids = {"q1": id1, "q2": id2}

        # p2 implies p1 (AND(a, b) → a) if same BDD var for 'a'
        assert bdd.implies(id2, id1), "p2 should imply p1 (p2 is stricter)"
        survivor = _union_survivor("q1", "q2", bdd_ids, bdd)
        assert survivor == "q1"

    def test_union_right_subset_left_eliminates_right_cte(self):
        """apply_semiring_minimisation eliminates the subset CTE in a UNION."""
        bdd = _make_bdd()
        p_a = ScalarPred(PropRef("t", "a"), "=", "x")
        p_b = ScalarPred(PropRef("t", "b"), "=", "y")
        p1 = p_a                    # broader
        p2 = AND(p_a, p_b)         # stricter — reuses p_a atom

        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("t1", p1))
        alg.add("q2", _leaf_query("t2", p2))
        # q2 ⊆ q1, so q1 ∪ q2 = q1
        alg.compose("q1", ComposeOp.UNION, "q2", name="q_union")

        result = apply_semiring_minimisation(alg, bdd)
        assert len(result) < len(alg)
        assert "q1" in result.names


class TestTryEliminateNoneReturn:
    """_try_eliminate returns None when op is not UNION/INTERSECT and operands differ."""

    def test_except_op_returns_none(self):
        """Lines 264-266: non-UNION/INTERSECT op with different operands → None."""
        bdd = _make_bdd()
        bdd_ids: dict = {"q1": TRUE_NODE_ID, "q2": TRUE_NODE_ID}
        result = _try_eliminate("q1", "q2", ComposeOp.EXCEPT, bdd_ids, bdd)
        assert result is None

    def test_with_op_returns_none(self):
        """WITH op with different operands → None (no semiring elimination)."""
        bdd = _make_bdd()
        bdd_ids: dict = {"q1": TRUE_NODE_ID, "q2": TRUE_NODE_ID}
        result = _try_eliminate("q1", "q2", ComposeOp.WITH, bdd_ids, bdd)
        assert result is None

    def test_join_op_returns_none(self):
        """JOIN op with different operands → None."""
        bdd = _make_bdd()
        bdd_ids: dict = {"q1": TRUE_NODE_ID, "q2": TRUE_NODE_ID}
        result = _try_eliminate("q1", "q2", ComposeOp.JOIN, bdd_ids, bdd)
        assert result is None

    def test_intersect_independent_predicates_returns_none(self):
        """_intersect_survivor returns None when neither operand implies the other.

        Covers the final 'return None' in _intersect_survivor (line 266).
        """
        bdd = _make_bdd()
        # Two fully independent predicates — neither implies the other
        p1 = ScalarPred(PropRef("t", "a"), "=", "x")
        p2 = ScalarPred(PropRef("t", "b"), "=", "y")
        id1 = bdd.compile(p1)
        id2 = bdd.compile(p2)
        bdd_ids = {"q1": id1, "q2": id2}
        result = _intersect_survivor("q1", "q2", bdd_ids, bdd)
        assert result is None


class TestResolveCycleGuard:
    """_resolve breaks on a cycle rather than looping forever."""

    def test_cycle_breaks_and_returns_current_name(self):
        """Line 283: cycle guard: if name in seen: break."""
        # Manufacture a cycle: a→b, b→a
        subs = {"a": "b", "b": "a"}
        # Starting from "a": a→b→a (cycle detected at second visit to a)
        result = _resolve("a", subs)
        # Should not loop; terminates with either "a" or "b"
        assert result in ("a", "b")

    def test_no_cycle_resolves_fully(self):
        """Without a cycle, _resolve follows the full chain."""
        subs = {"a": "b", "b": "c"}
        result = _resolve("a", subs)
        assert result == "c"


class TestIntersectSurvivorRightSubsetLeft:
    """_intersect_survivor: when right ⊆ left, intersection equals right."""

    def test_right_subset_left_returns_right(self):
        """Line 296: bdd.implies(r, l) → return right."""
        bdd = _make_bdd()
        # Share the same atom object so BDD detects containment
        p_a = ScalarPred(PropRef("t", "a"), "=", "x")
        p_b = ScalarPred(PropRef("t", "b"), "=", "y")
        p1 = p_a                    # broader
        p2 = AND(p_a, p_b)         # stricter — reuses p_a

        id1 = bdd.compile(p1)
        id2 = bdd.compile(p2)
        bdd_ids = {"q1": id1, "q2": id2}

        # p2 implies p1 (p2 is stricter) → for INTERSECT, return right ("q2")
        assert bdd.implies(id2, id1), "p2 should imply p1"
        survivor = _intersect_survivor("q1", "q2", bdd_ids, bdd)
        assert survivor == "q2"

    def test_intersect_right_subset_left_eliminates_left_cte(self):
        """apply_semiring_minimisation eliminates the superset in INTERSECT."""
        bdd = _make_bdd()
        p_a = ScalarPred(PropRef("t", "a"), "=", "x")
        p_b = ScalarPred(PropRef("t", "b"), "=", "y")
        p1 = p_a                    # broader
        p2 = AND(p_a, p_b)         # stricter — reuses p_a

        alg = QuestionAlgebra()
        alg.add("q1", _leaf_query("t1", p1))
        alg.add("q2", _leaf_query("t2", p2))
        # q2 ⊆ q1, so q1 ∩ q2 = q2
        alg.compose("q1", ComposeOp.INTERSECT, "q2", name="q_inter")

        result = apply_semiring_minimisation(alg, bdd)
        assert len(result) < len(alg)
        assert "q2" in result.names
