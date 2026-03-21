"""RED tests for Phase 3b — BDD canonical predicate layer.

Covers:
- BDDNode structure and terminal nodes
- BDDManager: make, apply (AND/OR), negate, Shannon expansion, unique table
- DGMPredicateBDD: compile, is_satisfiable, is_tautology, implies, equivalent,
  minterms enumeration
- 8-step logical optimisation pipeline
"""

from __future__ import annotations

import pytest

from sqldim.core.query._dgm_bdd import (
    BDDNode,
    FALSE_NODE_ID,
    TRUE_NODE_ID,
    BDDManager,
    DGMPredicateBDD,
)
from sqldim.core.query._dgm_preds import (
    ScalarPred,
    AND,
    OR,
    NOT,
    PathPred,
    SignaturePred,
    SequenceMatch,
    VerbHop,
)
from sqldim.core.query._dgm_refs import PropRef, AggRef


# ---------------------------------------------------------------------------
# BDDNode terminals
# ---------------------------------------------------------------------------


class TestBDDNodeTerminals:
    def test_false_node_id(self):
        assert FALSE_NODE_ID == 0

    def test_true_node_id(self):
        assert TRUE_NODE_ID == 1

    def test_bdd_node_fields(self):
        n = BDDNode(id=2, var=0, low=0, high=1)
        assert n.id == 2
        assert n.var == 0
        assert n.low == 0
        assert n.high == 1

    def test_bdd_node_is_hashable(self):
        n = BDDNode(id=2, var=0, low=0, high=1)
        _ = {n}  # must not raise

    def test_bdd_node_equality(self):
        n1 = BDDNode(id=2, var=0, low=0, high=1)
        n2 = BDDNode(id=2, var=0, low=0, high=1)
        assert n1 == n2


# ---------------------------------------------------------------------------
# BDDManager
# ---------------------------------------------------------------------------


class TestBDDManager:
    def setup_method(self):
        self.mgr = BDDManager()

    def test_terminals_preloaded(self):
        assert self.mgr.get_node(FALSE_NODE_ID).id == FALSE_NODE_ID
        assert self.mgr.get_node(TRUE_NODE_ID).id == TRUE_NODE_ID

    def test_make_variable(self):
        # A single variable: low=FALSE, high=TRUE
        vid = self.mgr.make(var=0, low=FALSE_NODE_ID, high=TRUE_NODE_ID)
        assert vid >= 2  # 0 and 1 are reserved
        node = self.mgr.get_node(vid)
        assert node.var == 0
        assert node.low == FALSE_NODE_ID
        assert node.high == TRUE_NODE_ID

    def test_make_elimination_rule(self):
        # If low == high, return low (elimination)
        vid = self.mgr.make(var=0, low=TRUE_NODE_ID, high=TRUE_NODE_ID)
        assert vid == TRUE_NODE_ID

    def test_make_sharing_rule(self):
        # Same (var, low, high) returns same ID
        v1 = self.mgr.make(var=0, low=FALSE_NODE_ID, high=TRUE_NODE_ID)
        v2 = self.mgr.make(var=0, low=FALSE_NODE_ID, high=TRUE_NODE_ID)
        assert v1 == v2

    def test_apply_and_true_true(self):
        result = self.mgr.apply("AND", TRUE_NODE_ID, TRUE_NODE_ID)
        assert result == TRUE_NODE_ID

    def test_apply_and_true_false(self):
        result = self.mgr.apply("AND", TRUE_NODE_ID, FALSE_NODE_ID)
        assert result == FALSE_NODE_ID

    def test_apply_and_false_true(self):
        result = self.mgr.apply("AND", FALSE_NODE_ID, TRUE_NODE_ID)
        assert result == FALSE_NODE_ID

    def test_apply_and_false_false(self):
        result = self.mgr.apply("AND", FALSE_NODE_ID, FALSE_NODE_ID)
        assert result == FALSE_NODE_ID

    def test_apply_or_false_false(self):
        result = self.mgr.apply("OR", FALSE_NODE_ID, FALSE_NODE_ID)
        assert result == FALSE_NODE_ID

    def test_apply_or_true_false(self):
        result = self.mgr.apply("OR", TRUE_NODE_ID, FALSE_NODE_ID)
        assert result == TRUE_NODE_ID

    def test_apply_or_false_true(self):
        result = self.mgr.apply("OR", FALSE_NODE_ID, TRUE_NODE_ID)
        assert result == TRUE_NODE_ID

    def test_negate_false(self):
        assert self.mgr.negate(FALSE_NODE_ID) == TRUE_NODE_ID

    def test_negate_true(self):
        assert self.mgr.negate(TRUE_NODE_ID) == FALSE_NODE_ID

    def test_negate_variable(self):
        vid = self.mgr.make(var=0, low=FALSE_NODE_ID, high=TRUE_NODE_ID)
        neg = self.mgr.negate(vid)
        # NOT(x) should have low=TRUE (was FALSE), high=FALSE (was TRUE)
        node = self.mgr.get_node(neg)
        assert node.low == TRUE_NODE_ID
        assert node.high == FALSE_NODE_ID

    def test_apply_is_memoised(self):
        r1 = self.mgr.apply("AND", TRUE_NODE_ID, TRUE_NODE_ID)
        r2 = self.mgr.apply("AND", TRUE_NODE_ID, TRUE_NODE_ID)
        assert r1 == r2
        # Check that the computed cache is populated
        assert len(self.mgr._computed) > 0

    def test_shannon_expansion(self):
        # Build x AND y; confirm via Shannon: substitute x=T → y, x=F → FALSE
        x = self.mgr.make(0, FALSE_NODE_ID, TRUE_NODE_ID)
        y = self.mgr.make(1, FALSE_NODE_ID, TRUE_NODE_ID)
        xy = self.mgr.apply("AND", x, y)
        node = self.mgr.get_node(xy)
        # When x=False → FALSE; when x=True → y
        assert node.low == FALSE_NODE_ID
        assert node.high == y


# ---------------------------------------------------------------------------
# DGMPredicateBDD
# ---------------------------------------------------------------------------


class TestDGMPredicateBDD:
    def setup_method(self):
        self.mgr = BDDManager()
        self.bdd = DGMPredicateBDD(self.mgr)

    def _scalar(self, alias, prop, val):
        return ScalarPred(PropRef(alias, prop), "=", val)

    def test_compile_tautology_pred_returns_true(self):
        # RawPred("TRUE") or a trivially true BDD
        from sqldim.core.query._dgm_preds import RawPred
        uid = self.bdd.compile_true()
        assert uid == TRUE_NODE_ID

    def test_compile_false_returns_false(self):
        uid = self.bdd.compile_false()
        assert uid == FALSE_NODE_ID

    def test_compile_scalar_pred_atom(self):
        pred = self._scalar("c", "region", "EMEA")
        uid = self.bdd.compile(pred)
        assert isinstance(uid, int)
        assert uid >= 2  # not a terminal

    def test_compile_same_pred_gives_same_id(self):
        pred = self._scalar("c", "region", "EMEA")
        u1 = self.bdd.compile(pred)
        u2 = self.bdd.compile(pred)
        assert u1 == u2

    def test_compile_and_pred(self):
        p1 = self._scalar("c", "region", "EMEA")
        p2 = self._scalar("c", "tier", "gold")
        pred = AND(p1, p2)
        uid = self.bdd.compile(pred)
        assert uid >= 0

    def test_compile_or_pred(self):
        p1 = self._scalar("c", "region", "EMEA")
        p2 = self._scalar("c", "tier", "gold")
        pred = OR(p1, p2)
        uid = self.bdd.compile(pred)
        assert uid >= 0

    def test_compile_not_pred(self):
        p = self._scalar("c", "active", True)
        pred = NOT(p)
        uid = self.bdd.compile(pred)
        assert uid >= 0

    def test_is_satisfiable_atom(self):
        pred = self._scalar("c", "region", "EMEA")
        uid = self.bdd.compile(pred)
        assert self.bdd.is_satisfiable(uid)

    def test_is_satisfiable_false(self):
        assert not self.bdd.is_satisfiable(FALSE_NODE_ID)

    def test_is_tautology_true(self):
        assert self.bdd.is_tautology(TRUE_NODE_ID)

    def test_is_tautology_atom_false(self):
        pred = self._scalar("c", "region", "EMEA")
        uid = self.bdd.compile(pred)
        assert not self.bdd.is_tautology(uid)

    def test_equivalent_same_pred(self):
        pred = self._scalar("c", "region", "EMEA")
        u1 = self.bdd.compile(pred)
        u2 = self.bdd.compile(pred)
        assert self.bdd.equivalent(u1, u2)

    def test_equivalent_different_preds(self):
        p1 = self._scalar("c", "region", "EMEA")
        p2 = self._scalar("c", "tier", "gold")
        u1 = self.bdd.compile(p1)
        u2 = self.bdd.compile(p2)
        assert not self.bdd.equivalent(u1, u2)

    def test_implies_true(self):
        # p → TRUE always
        pred = self._scalar("c", "region", "EMEA")
        uid = self.bdd.compile(pred)
        assert self.bdd.implies(uid, TRUE_NODE_ID)

    def test_implies_false_antecedent(self):
        # FALSE → anything
        pred = self._scalar("c", "region", "EMEA")
        uid = self.bdd.compile(pred)
        assert self.bdd.implies(FALSE_NODE_ID, uid)

    def test_implies_self(self):
        pred = self._scalar("c", "region", "EMEA")
        uid = self.bdd.compile(pred)
        assert self.bdd.implies(uid, uid)

    def test_minterms_single_atom(self):
        pred = self._scalar("c", "region", "EMEA")
        uid = self.bdd.compile(pred)
        terms = self.bdd.minterms(uid)
        assert isinstance(terms, list)
        assert len(terms) == 1

    def test_minterms_false_is_empty(self):
        terms = self.bdd.minterms(FALSE_NODE_ID)
        assert terms == []

    def test_minterms_true_all_assignments(self):
        terms = self.bdd.minterms(TRUE_NODE_ID)
        # For zero variables, TRUE has exactly one minterm: empty dict
        assert len(terms) >= 1

    def test_compile_signature_pred_atom(self):
        sig = SignaturePred("p1", ["placed", "shipped"], SequenceMatch.EXACT)
        uid = self.bdd.compile(sig)
        assert uid >= 2

    def test_compile_path_pred_atom(self):
        hop = VerbHop("c", "placed", "s", table="fact_order", on="c.id = s.cid")
        sub = ScalarPred(PropRef("s", "status"), "=", "ok")
        pp = PathPred("c", hop, sub)
        uid = self.bdd.compile(pp)
        assert uid >= 2


# ---------------------------------------------------------------------------
# Logical optimisation pipeline (8 steps)
# ---------------------------------------------------------------------------


class TestLogicalOptimizationPipeline:
    """DGMPredicateBDD.optimize() applies the 8-step pipeline."""

    def setup_method(self):
        self.mgr = BDDManager()
        self.bdd = DGMPredicateBDD(self.mgr)

    def _scalar(self, alias, prop, val):
        return ScalarPred(PropRef(alias, prop), "=", val)

    # Step 1 — Tautology / empty-result short-circuit
    def test_optimize_false_short_circuits(self):
        result = self.bdd.optimize(FALSE_NODE_ID)
        assert result == FALSE_NODE_ID

    def test_optimize_true_short_circuits(self):
        result = self.bdd.optimize(TRUE_NODE_ID)
        assert result == TRUE_NODE_ID

    # Step 2 — Redundant clause elimination (AND)
    def test_optimize_and_with_true(self):
        # TRUE AND x = x
        pred = self._scalar("c", "region", "EMEA")
        uid = self.bdd.compile(pred)
        combined = self.mgr.apply("AND", TRUE_NODE_ID, uid)
        optimised = self.bdd.optimize(combined)
        assert self.bdd.equivalent(optimised, uid)

    # Step 3 — Predicate subsumption (OR)
    def test_optimize_or_with_false(self):
        # FALSE OR x = x
        pred = self._scalar("c", "tier", "gold")
        uid = self.bdd.compile(pred)
        combined = self.mgr.apply("OR", FALSE_NODE_ID, uid)
        optimised = self.bdd.optimize(combined)
        assert self.bdd.equivalent(optimised, uid)

    # Step 5 — FORALL De Morgan rewriting
    def test_forall_pathpred_demorgan(self):
        """FORALL PathPred compiles to NOT(PathPred(EXISTS, NOT(sub_filter)))."""
        from sqldim.core.query._dgm_preds import Quantifier
        hop = VerbHop("c", "placed", "s", table="fact_order", on="c.id = s.cid")
        sub = self._scalar("s", "ok", True)
        pp_forall = PathPred("c", hop, sub, quantifier=Quantifier.FORALL)
        pp_exists = PathPred("c", hop, sub, quantifier=Quantifier.EXISTS)
        u_forall = self.bdd.compile(pp_forall)
        u_exists = self.bdd.compile(pp_exists)
        # FORALL and EXISTS should map to different BDD nodes
        assert u_forall != u_exists

    # Step 7 — SQL translation strategy
    def test_sql_translation_few_minterms(self):
        """With ≤ THRESHOLD minterms, to_sql_union() uses UNION ALL."""
        pred = self._scalar("c", "region", "EMEA")
        uid = self.bdd.compile(pred)
        sql = self.bdd.to_sql(uid)
        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_optimize_returns_int(self):
        """optimize() always returns a valid BDD node ID."""
        pred = self._scalar("c", "active", True)
        uid = self.bdd.compile(pred)
        result = self.bdd.optimize(uid)
        assert isinstance(result, int)
        _ = self.mgr.get_node(result)  # must not raise
