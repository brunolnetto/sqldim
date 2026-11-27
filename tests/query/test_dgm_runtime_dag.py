"""§11.10 runtime APIs: question specs, the query DAG and the registry (#149)."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass

import pytest

from sqldim.core.query.dgm.analytical_space import (
    DGMSchema,
    EdgeTypeDecl,
    NodeTypeDecl,
)
from sqldim.core.query.dgm.nl.supervisor._auth import NOT_AUTHORIZED_OR_NOT_FOUND
from sqldim.core.query.dgm.planner import (
    ExportPlan,
    PlanIdentityUnavailable,
    Substitution,
    UnknownRelation,
    UnsafeFragment,
    UnsupportedConstruct,
    dag_hash,
    emit_sql,
)
from sqldim.core.query.dgm.planner._gate import RewriteCandidate
from sqldim.core.query.dgm.runtime import (
    GroundedEntityRegistry,
    InMemoryQuestionStore,
    InvalidRuntimeArgument,
    InvalidSpec,
    NotConfigured,
    PlanRef,
    QueryDAG,
    SchemaVocabulary,
    Unauthorized,
    UnknownIdentifier,
    UnsupportedOperation,
    decode_spec,
    encode_spec,
    spec_hash,
)
from tests.query._runtime_fixtures import (
    CATALOG,
    SCHEMA_VERSION,
    FilterCostModel,
    Watermarks,
    alg_except,
    alg_union,
    make_dag,
    plan_of,
    q_customers,
    q_eu,
    q_sales,
    spec_of,
)
from tests.query.test_dgm_plan_ir import SINGLE_FIXTURES
from tests.query.test_dgm_plan_ir import alg_join, alg_nested
from tests.query.test_dgm_plan_ir import alg_union as ir_union

ALL_SHAPES = {
    **SINGLE_FIXTURES,
    "alg_union": ir_union,
    "alg_join": alg_join,
    "alg_nested": alg_nested,
}


# ---------------------------------------------------------------------------
# Specs
# ---------------------------------------------------------------------------


class TestSpec:
    @pytest.mark.parametrize("name", sorted(ALL_SHAPES))
    def test_round_trip_preserves_the_ir(self, name: str) -> None:
        plan = plan_of(ALL_SHAPES[name]())
        assert plan.ir is not None
        spec = json.loads(json.dumps(encode_spec(plan.ir, {"k": 3})))
        back = decode_spec(spec)
        assert back.ir == plan.ir
        assert back.params == {"k": 3}
        assert dag_hash(back.ir) == dag_hash(plan.ir)
        assert emit_sql(back.ir) == plan.query_text

    def test_hash_ignores_key_order_but_not_content(self) -> None:
        spec = spec_of(q_eu())
        shuffled = json.loads(json.dumps(spec, sort_keys=True))
        assert spec_hash(spec) == spec_hash(shuffled)
        assert spec_hash(spec) != spec_hash(spec_of(q_customers()))

    @staticmethod
    def _mutate(path: list, value: object) -> dict:
        spec = copy.deepcopy(spec_of(q_eu()))
        node = spec
        for p in path[:-1]:
            node = node[p]
        node[path[-1]] = value
        return spec

    @pytest.mark.parametrize(
        ("path", "value"),
        [
            (["format"], "dgm-question/0"),
            (["root"], 5),
            (["nodes", 0, "type"], "table"),
            (["nodes", 0, "anchor", "role"], "PATH_JOIN"),
            (["nodes", 0, "anchor", "relation"], "read_csv('x')"),
            (["nodes", 0, "where"], {"xor": []}),
            (["nodes", 0, "where"], {"cmp": ["c.region", "~", "EU"]}),
            (["nodes", 0, "where"], {"cmp": ["c.region", "=", float("inf")]}),
            (["nodes", 0, "where"], {"and": []}),
            (["params"], {"1bad": 1}),
        ],
    )
    def test_malformed_spec_is_rejected(self, path: list, value: object) -> None:
        with pytest.raises(UnsupportedConstruct):  # InvalidSpec is one
            decode_spec(self._mutate(path, value))

    def test_unknown_key_is_rejected(self) -> None:
        spec = spec_of(q_eu())
        spec["nodes"][0]["limit"] = 10
        with pytest.raises(InvalidSpec, match="keys"):
            decode_spec(spec)

    @pytest.mark.parametrize(
        "raw", ["c.id IN (SELECT id FROM other)", "x.id > 0", "1 = 1; DROP TABLE t"]
    )
    def test_fragment_cannot_open_a_relation(self, raw: str) -> None:
        spec = self._mutate(["nodes", 0, "where"], {"raw": raw})
        with pytest.raises(UnsafeFragment):
            decode_spec(spec)

    def test_forward_child_reference_is_rejected(self) -> None:
        spec = spec_of(alg_union())
        root = spec["nodes"][spec["root"]]
        root["left"] = spec["root"]
        with pytest.raises(InvalidSpec, match="earlier node"):
            decode_spec(spec)


# ---------------------------------------------------------------------------
# QueryDAG
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_equal_questions_share_a_node_and_key(self) -> None:
        dag, _ = make_dag()
        a = dag.make_from_spec(spec_of(q_eu()))
        b = dag.make_from_spec(json.loads(json.dumps(spec_of(q_eu()))))
        assert a == b
        assert dag.question_key(a) == plan_of(q_eu()).question_key(
            schema_version=SCHEMA_VERSION
        )

    def test_key_does_not_depend_on_cte_names_or_manager(self) -> None:
        dag1, _ = make_dag()
        dag2, _ = make_dag()
        k1 = dag1.question_key(dag1.make_from_spec(spec_of(alg_union("a", "b"))))
        dag2.make_from_spec(spec_of(q_sales()))  # different local ids
        k2 = dag2.question_key(dag2.make_from_spec(spec_of(alg_union("x", "y"))))
        assert k1 == k2

    def test_params_and_schema_version_enter_the_key(self) -> None:
        dag, _ = make_dag()
        base = dag.key_for_spec(spec_of(q_eu()))
        assert dag.key_for_spec(spec_of(q_eu(), k=1)) != base
        other, _ = make_dag(schema_version="schema-v2")
        assert other.key_for_spec(spec_of(q_eu())) != base

    def test_unknown_node_id(self) -> None:
        dag, _ = make_dag()
        with pytest.raises(UnknownIdentifier):
            dag.question_key(0)  # type: ignore[arg-type]
        with pytest.raises(InvalidRuntimeArgument):
            dag.question_key("0")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "missing", ["store", "catalog", "watermarks", "cost_model", "schema_version"]
    )
    def test_every_dependency_is_required(self, missing: str) -> None:
        _, store = make_dag()
        args = {
            "schema_version": SCHEMA_VERSION,
            "store": store,
            "catalog": CATALOG,
            "watermarks": Watermarks(),
            "cost_model": FilterCostModel(),
        }
        args[missing] = None
        with pytest.raises(NotConfigured):
            QueryDAG(**args)  # type: ignore[arg-type]


class TestPlanFor:
    def test_rebuilds_from_the_store_in_a_new_manager(self) -> None:
        dag, store = make_dag()
        key = dag.question_key(dag.make_from_spec(spec_of(q_eu())))
        fresh, _ = make_dag(store=store)
        plan = fresh.plan_for(PlanRef(key))
        assert plan.query_text == plan_of(q_eu()).query_text
        assert plan.cost_estimate is not None and plan.estimated_rows == 10

    def test_unknown_question(self) -> None:
        dag, _ = make_dag()
        with pytest.raises(UnknownIdentifier):
            dag.plan_for(PlanRef("0" * 64))

    def test_stored_under_another_schema_is_not_rekeyed(self) -> None:
        dag, store = make_dag()
        key = dag.question_key(dag.make_from_spec(spec_of(q_eu())))
        moved, _ = make_dag(store=store, schema_version="schema-v2")
        with pytest.raises(InvalidRuntimeArgument, match="another schema"):
            moved.plan_for(PlanRef(key))

    def test_plan_version_must_match_rewrites(self) -> None:
        dag, _ = make_dag()
        key = dag.question_key(dag.make_from_spec(spec_of(q_eu())))
        with pytest.raises(InvalidRuntimeArgument):
            dag.plan_for(PlanRef(key, plan_version=1))
        with pytest.raises(InvalidRuntimeArgument, match="twice"):
            dag.plan_for(PlanRef(key, 2, ("DEPTH_LIMIT", "DEPTH_LIMIT")))

    def test_recorded_rewrite_needs_a_provider(self) -> None:
        dag, _ = make_dag()
        key = dag.question_key(dag.make_from_spec(spec_of(q_eu())))
        with pytest.raises(NotConfigured):
            dag.plan_for(PlanRef(key, 1, ("DEPTH_LIMIT",)))

    def test_recorded_rewrite_is_reproduced(self) -> None:
        target = plan_of(q_customers())

        @dataclass
        class Rewrites:
            def candidates(self, plan: ExportPlan) -> list[RewriteCandidate]:
                return [RewriteCandidate(target, Substitution.DEPTH_LIMIT, "depth 3")]

        dag, _ = make_dag(rewrites=Rewrites())
        key = dag.question_key(dag.make_from_spec(spec_of(q_eu())))
        plan = dag.plan_for(PlanRef(key, 1, ("DEPTH_LIMIT",)))
        assert plan.query_text == target.query_text
        with pytest.raises(UnsupportedOperation, match="not reproduced"):
            dag.plan_for(PlanRef(key, 1, ("CONE_SCOPE",)))


class TestResultKey:
    def test_key_follows_watermarks(self) -> None:
        marks = Watermarks()
        dag, _ = make_dag(watermarks=marks)
        ref = PlanRef(dag.question_key(dag.make_from_spec(spec_of(q_sales()))))
        k1 = dag.result_key(ref)
        assert k1.versioned and k1.question_key == ref.question_key
        assert dag.result_key(ref) == k1
        marks.marks["fact_sale"] = 4
        assert dag.result_key(ref).value != k1.value

    def test_unrelated_source_does_not_change_the_key(self) -> None:
        marks = Watermarks()
        dag, _ = make_dag(watermarks=marks)
        ref = PlanRef(dag.question_key(dag.make_from_spec(spec_of(q_eu()))))
        k1 = dag.result_key(ref)
        marks.marks["fact_sale"] = 99
        assert dag.result_key(ref) == k1

    def test_missing_watermark_is_reported_unversioned(self) -> None:
        dag, _ = make_dag(watermarks=Watermarks({"dim_customer": None}))
        ref = PlanRef(dag.question_key(dag.make_from_spec(spec_of(q_eu()))))
        key = dag.result_key(ref)
        assert not key.versioned and key.unversioned == ("dim_customer",)

    def test_bad_watermark_type(self) -> None:
        dag, _ = make_dag(watermarks=Watermarks({"dim_customer": 1.5}))  # type: ignore[dict-item]
        ref = PlanRef(dag.question_key(dag.make_from_spec(spec_of(q_eu()))))
        with pytest.raises(InvalidRuntimeArgument):
            dag.result_key(ref)

    def test_undeclared_relation(self) -> None:
        from sqldim.core.query.dgm import DGMQuery

        dag, _ = make_dag()
        ref = PlanRef(
            dag.question_key(dag.make_from_spec(spec_of(DGMQuery().anchor("t_x", "x"))))
        )
        with pytest.raises(UnknownRelation):
            dag.result_key(ref)


class TestDecompose:
    def test_union_decomposition(self) -> None:
        dag, _ = make_dag()
        key = dag.question_key(dag.make_from_spec(spec_of(alg_union())))
        d = dag.decompose_by_key(key)
        kinds = [s.kind for s in d.steps]
        assert kinds == ["QUESTION", "QUESTION", "UNION"]
        assert d.steps[-1].text == "either question 1 or question 2"
        assert d.steps[0].text.startswith("question 1: {")
        assert json.loads(json.dumps(d.to_dict()))["question_key"] == key

    def test_op_without_an_explanation(self) -> None:
        dag, _ = make_dag()
        key = dag.question_key(dag.make_from_spec(spec_of(alg_except())))
        with pytest.raises(UnsupportedOperation, match="EXCEPT"):
            dag.decompose_by_key(key)

    def test_unknown_key(self) -> None:
        dag, _ = make_dag()
        with pytest.raises(UnknownIdentifier):
            dag.decompose_by_key("missing")

    def test_spec_for_plan_needs_an_ir(self) -> None:
        dag, _ = make_dag()
        text_plan = ExportPlan(plan_of(q_eu()).query_target, "SELECT 1")
        with pytest.raises(PlanIdentityUnavailable):
            dag.spec_for_plan(text_plan, {})


def test_store_returns_copies() -> None:
    store = InMemoryQuestionStore()
    store.put("k", {"a": [1]})
    got = store.get("k")
    assert got == {"a": [1]}
    got["a"].append(2)  # type: ignore[index, union-attr]
    assert store.get("k") == {"a": [1]}
    assert store.get("missing") is None


def test_unauthorized_message_matches_the_auth_layer() -> None:
    assert str(Unauthorized()) == NOT_AUTHORIZED_OR_NOT_FOUND


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def schema() -> DGMSchema:
    return DGMSchema(
        nodes=(
            NodeTypeDecl("Customer", "dim", (("id", "int"), ("region", "str"))),
            NodeTypeDecl("Product", "dim", (("sku", "str"),)),
            NodeTypeDecl("Sale", "fact", (("revenue", "float"),)),
        ),
        edges=(
            EdgeTypeDecl("Customer", "placed", "Sale", "verb"),
            EdgeTypeDecl("Customer", "returned", "Sale", "verb"),
        ),
    )


class TestRegistry:
    def test_property_keys(self) -> None:
        vocab = SchemaVocabulary(schema())
        assert vocab.property_keys("Customer") == ["id", "region"]
        with pytest.raises(UnknownIdentifier):
            vocab.property_keys("Supplier")

    def test_verb_labels_per_pair(self) -> None:
        reg = GroundedEntityRegistry(SchemaVocabulary(schema()), {"c": "Customer"})
        assert reg.verb_labels[("Customer", "Sale")] == ("placed", "returned")
        assert reg.verb_labels.get(("Customer", "Product")) == ()
        assert reg.verb_labels[("Sale", "Customer")] == ()  # directed as declared
        assert list(reg.verb_labels) == [("Customer", "Sale")]
        assert ("Customer", "Sale") in reg.verb_labels

    @pytest.mark.parametrize("pair", [("Customer", "Supplier"), ("X", "Sale")])
    def test_unknown_type_is_not_an_empty_answer(self, pair: tuple[str, str]) -> None:
        reg = GroundedEntityRegistry(SchemaVocabulary(schema()), {})
        with pytest.raises(UnknownIdentifier):
            reg.verb_labels[pair]
        with pytest.raises(UnknownIdentifier):
            reg.verb_labels.get(pair, ["fallback"])  # type: ignore[arg-type]

    def test_alias_types_and_prop_refs(self) -> None:
        reg = GroundedEntityRegistry(
            SchemaVocabulary(schema()), {"c": "Customer", "s": "Sale"}
        )
        assert dict(reg.alias_types) == {"c": "Customer", "s": "Sale"}
        assert reg.prop_refs("Customer") == ["c.id", "c.region"]
        assert reg.prop_refs("Product") == []  # declared, no alias bound
        with pytest.raises(UnknownIdentifier):
            reg.prop_refs("Supplier")
        with pytest.raises(TypeError):
            reg.alias_types["p"] = "Product"  # type: ignore[index]

    def test_bad_bindings(self) -> None:
        vocab = SchemaVocabulary(schema())
        with pytest.raises(UnknownIdentifier):
            GroundedEntityRegistry(vocab, {"x": "Supplier"})
        reg = GroundedEntityRegistry(vocab, {"c": "Customer"})
        reg.register_node("customer", "c")
        assert reg.resolve_node("customer") == "c"
        with pytest.raises(UnknownIdentifier):
            reg.register_node("product", "p")
        with pytest.raises(InvalidRuntimeArgument):
            SchemaVocabulary(object())  # type: ignore[arg-type]


class TestSpecLimits:
    """Constructs added by #148 that spec format 1 does not carry (fail closed)."""

    def test_repeat_is_refused(self) -> None:
        from sqldim.core.query.dgm import DGMQuery
        from sqldim.core.query.dgm.preds import Repeat, RepeatMode
        from tests.query.test_dgm_plan_ir_complete import reports_to

        r = Repeat(reports_to(), 1, 2, mode=RepeatMode.WALK)
        q = DGMQuery().anchor("employee", "e").path_join(r)
        plan = plan_of(q)
        assert plan.ir is not None
        with pytest.raises(InvalidSpec, match="Repeat"):
            encode_spec(plan.ir)

    def test_pre_compute_is_refused(self) -> None:
        from tests.query.test_dgm_plan_ir_complete import _step, events, planner

        plan = planner().plan_query(events(), pre_compute=[_step()])
        assert plan.ir is not None and plan.ir.pre_compute
        with pytest.raises(InvalidSpec, match="PreComputation"):
            encode_spec(plan.ir)
