"""Plan IR completion (#148): PreComputations, partition pruning and Repeat.

DGM §6.2 Rule 3/Rule 5, §6.3 Step 1b, §3.2 and §8.14 ``deps(plan)``.
"""

from __future__ import annotations

import dataclasses
from datetime import date, timedelta
from typing import Any

import duckdb
import pytest

from sqldim.core.query.dgm import (
    NOT,
    OR,
    UNBOUNDED,
    BridgeHop,
    Compose as PathCompose,
    DGMQuery,
    PathPred,
    PropRef,
    AggRef,
    Repeat,
    RepeatMode,
    ScalarPred,
    VerbHop,
)
from sqldim.core.query.dgm.algebra import ComposeOp, QuestionAlgebra
from sqldim.core.query.dgm.graph import (
    GLOBAL_SIGNATURE_COUNT,
    OUTGOING_SIGNATURES,
    SIGNATURE_ENTROPY,
    GraphStatistics,
    NodeExpr,
    SubgraphExpr,
)
from sqldim.core.query.dgm.graph import RelationshipSubgraph, Bound, Free
from sqldim.core.query.dgm.planner import (
    MAX_UNROLL,
    SMALL_GRAPH_THRESHOLD,
    ApproximateVariant,
    ClarifyDecision,
    CostEstimate,
    DeclaredPrecomputeProvider,
    Dependency,
    DGMPlanner,
    ExecutionBudget,
    ExecutionGate,
    ExportPlan,
    GraphExprSchedule,
    Partition,
    PartitionKind,
    Partitioning,
    Placement,
    PlanIdentityUnavailable,
    PlanIR,
    PreComputation,
    PrecomputeEstimate,
    QueryTarget,
    RejectDecision,
    RewriteDecision,
    StaticCatalog,
    Substitution,
    UndeclaredReads,
    UnknownRelation,
    UnsupportedConstruct,
    dag_hash,
    deps,
    relations_read,
)
from sqldim.core.query.dgm.planner._ir import ReadRole, rename_aliases, Fragment
from sqldim.core.query.dgm.planner._ir_build import lower_query
from sqldim.exceptions import SemanticError


def planner(statistics: Any = None, cost_model: Any = None) -> DGMPlanner:
    return DGMPlanner(cost_model, statistics, None, None, QueryTarget.SQL_DUCKDB)  # type: ignore[arg-type]


def key(plan: ExportPlan, **params: object) -> str:
    return plan.question_key(schema_version="schema-1", params=params)


# ---------------------------------------------------------------------------
# Relations and a catalog with declared partitioning
# ---------------------------------------------------------------------------

TABLES = {
    "fact_event": "id INTEGER, customer_id INTEGER, day DATE, amount DOUBLE",
    "dim_customer": "id INTEGER, region VARCHAR, score INTEGER",
    "employee": "id INTEGER, manager_id INTEGER, title VARCHAR",
    "graph_edge": "src INTEGER, dst INTEGER",
}

MONTHS = Partitioning(
    column="day",
    kind=PartitionKind.DATE,
    partitions=(
        Partition("2024-01", lower=date(2024, 1, 1), upper=date(2024, 2, 1)),
        Partition("2024-02", lower="2024-02-01", upper="2024-03-01"),
        Partition("2024-03", lower="2024-03-01", upper="2024-04-01"),
    ),
)
ALL_MONTHS = ("2024-01", "2024-02", "2024-03")
REGIONS = Partitioning(
    column="region",
    kind=PartitionKind.STRING,
    partitions=(
        Partition("eu", values=frozenset({"EU"})),
        Partition("am", values=frozenset({"US", "BR"})),
    ),
)
SCORES = Partitioning(
    column="score",
    kind=PartitionKind.NUMBER,
    partitions=(
        Partition("low", upper=10),
        Partition("mid", lower=10, upper=100),
        Partition("high", lower=100),
    ),
)

CATALOG = StaticCatalog(
    base=frozenset(TABLES),
    derived={"v_events": frozenset({"fact_event"})},
    partitions={"fact_event": MONTHS, "dim_customer": REGIONS},
)


def events() -> DGMQuery:
    return DGMQuery().anchor("fact_event", "e")


def deps_of(q: DGMQuery, catalog: StaticCatalog = CATALOG) -> tuple[Dependency, ...]:
    return planner().plan_query(q).deps(catalog)


def day(op: str, v: object) -> ScalarPred:
    return ScalarPred(PropRef("e", "day"), op, v)


# ---------------------------------------------------------------------------
# Partition pruning (§8.14: "or partitions after pruning")
# ---------------------------------------------------------------------------


class TestPartitionPruning:
    @pytest.mark.parametrize(
        "preds,expected",
        [
            ([day(">=", "2024-02-01")], ("2024-02", "2024-03")),
            ([day(">", "2024-02-29")], ("2024-02", "2024-03")),
            ([day("<", "2024-02-01")], ("2024-01",)),
            ([day("<=", "2024-02-01")], ("2024-01", "2024-02")),
            ([day("=", "2024-01-15")], ("2024-01",)),
            ([day(">=", "2024-02-01"), day("<", "2024-03-01")], ("2024-02",)),
            ([day(">=", "2025-01-01")], ()),
        ],
    )
    def test_anchor_where_conjuncts_prune(
        self, preds: list[ScalarPred], expected: tuple[str, ...]
    ) -> None:
        q = events()
        for p in preds:
            q = q.where(p)
        assert deps_of(q) == (Dependency("fact_event", expected),)

    @pytest.mark.parametrize(
        "pred",
        [
            OR(day("=", "2024-01-15"), day("=", "2024-03-15")),
            NOT(day("<", "2024-02-01")),
            day("!=", "2024-01-15"),
            day(">=", 20240201),  # not a DATE literal
            day(">=", "2024-2-1"),  # not ISO: DuckDB and Python may disagree
            day(">=", "2024-02-30"),  # not a date
            ScalarPred(PropRef("e", "amount"), ">", 3),  # not the partition column
            "e.day >= '2024-02-01'",  # raw SQL is never interpreted
        ],
        ids=lambda p: type(p).__name__ if not isinstance(p, str) else "raw",
    )
    def test_unproven_pruning_reads_all_partitions_explicitly(self, pred: Any) -> None:
        assert deps_of(events().where(pred)) == (Dependency("fact_event", ALL_MONTHS),)

    def test_no_where_reads_all_partitions(self) -> None:
        assert deps_of(events()) == (Dependency("fact_event", ALL_MONTHS),)

    def test_having_does_not_prune(self) -> None:
        q = (
            events()
            .group_by("e.day")
            .agg(n="COUNT(*)")
            .having(ScalarPred(AggRef("n"), ">", 1))
        )
        assert deps_of(q) == (Dependency("fact_event", ALL_MONTHS),)

    def test_joined_and_exists_reads_are_not_pruned(self) -> None:
        joined = (
            DGMQuery()
            .anchor("dim_customer", "c")
            .path_join(
                VerbHop("c", "did", "e", table="fact_event", on="e.customer_id = c.id")
            )
            .where(day(">=", "2024-03-01"))
            .where(ScalarPred(PropRef("c", "region"), "=", "EU"))
        )
        assert deps_of(joined) == (
            Dependency("dim_customer", ("eu",)),
            Dependency("fact_event", ALL_MONTHS),
        )
        exists = (
            DGMQuery()
            .anchor("dim_customer", "c")
            .where(
                PathPred(
                    "c",
                    VerbHop(
                        "c", "did", "e", table="fact_event", on="e.customer_id = c.id"
                    ),
                    day(">=", "2024-03-01"),
                )
            )
        )
        assert deps_of(exists) == (
            Dependency("dim_customer", ("am", "eu")),
            Dependency("fact_event", ALL_MONTHS),
        )

    def test_string_list_and_number_range_partitions(self) -> None:
        q = DGMQuery().anchor("dim_customer", "c")
        region = ScalarPred(PropRef("c", "region"), "=", "BR")
        assert deps_of(q.where(region)) == (Dependency("dim_customer", ("am",)),)
        q2 = DGMQuery().anchor("dim_customer", "c")
        lt = ScalarPred(PropRef("c", "region"), "<", "F")  # "EU" < "F" < "US"
        assert deps_of(q2.where(lt)) == (Dependency("dim_customer", ("am", "eu")),)
        scored = StaticCatalog(
            base=frozenset(TABLES), partitions={"dim_customer": SCORES}
        )
        for op, v, expected in [
            ("=", 10, ("mid",)),
            ("<", 10, ("low",)),
            (">=", 100, ("high",)),
            (">", 9.5, ("high", "low", "mid")),
            ("=", True, ("high", "low", "mid")),  # a bool is not a NUMBER
        ]:
            q3 = DGMQuery().anchor("dim_customer", "c")
            q3 = q3.where(ScalarPred(PropRef("c", "score"), op, v))
            assert deps_of(q3, scored) == (Dependency("dim_customer", expected),)

    def test_derived_view_bases_are_not_pruned(self) -> None:
        q = DGMQuery().anchor("v_events", "e").where(day(">=", "2024-03-01"))
        assert deps_of(q) == (Dependency("fact_event", ALL_MONTHS),)

    def test_undeclared_partitioning_is_all_partitions(self) -> None:
        q = DGMQuery().anchor("employee", "x")
        assert deps_of(q) == (Dependency("employee", None),)

    def test_bands_of_an_algebra_union_their_partitions(self) -> None:
        alg = QuestionAlgebra()
        alg.add("jan", events().where(day("<", "2024-02-01")))
        alg.add("mar", events().where(day(">=", "2024-03-01")))
        alg.compose("jan", ComposeOp.UNION, "mar", name="jan_or_mar")
        plan = planner().plan_query(alg, final="jan_or_mar")
        assert plan.deps(CATALOG) == (Dependency("fact_event", ("2024-01", "2024-03")),)

    def test_untouched_partitions_cannot_change_the_result(self) -> None:
        """The pruned deps are sound: a load into another partition keeps the result."""
        q = events().where(day(">=", "2024-02-01")).group_by("e.customer_id")
        plan = planner().plan_query(q.agg(total="SUM(e.amount)"))
        assert plan.deps(CATALOG) == (Dependency("fact_event", ("2024-02", "2024-03")),)
        con = duckdb.connect()
        con.execute(f"CREATE TABLE fact_event ({TABLES['fact_event']})")
        con.execute(
            "INSERT INTO fact_event VALUES (1, 1, '2024-01-10', 5), "
            "(2, 1, '2024-02-10', 7), (3, 2, '2024-03-10', 11)"
        )
        before = sorted(con.execute(plan.query_text).fetchall())
        con.execute("INSERT INTO fact_event VALUES (4, 1, '2024-01-20', 100)")
        assert sorted(con.execute(plan.query_text).fetchall()) == before
        con.execute("INSERT INTO fact_event VALUES (5, 1, '2024-02-20', 100)")
        assert sorted(con.execute(plan.query_text).fetchall()) != before


class TestPartitionDeclarations:
    @pytest.mark.parametrize(
        "kwargs,msg",
        [
            ({"partitions": ()}, "at least one"),
            ({"partitions": (Partition("a"), Partition("a"))}, "distinct"),
            ({"column": "select"}, "reserved"),
            (
                {"partitions": (Partition("a", values=frozenset({1}), lower=0),)},
                "either",
            ),
            ({"partitions": (Partition("a", values=frozenset()),)}, "either"),
            ({"partitions": (Partition("a", values=frozenset({None})),)}, "NULL"),
            ({"partitions": (Partition("a", values=frozenset({"x"})),)}, "NUMBER"),
            ({"partitions": (Partition("a", lower=5, upper=5),)}, "below"),
            ({"partitions": (Partition("a", lower=float("nan")),)}, "NUMBER"),
            ({"kind": "NUMBER"}, "PartitionKind"),
        ],
    )
    def test_invalid_partitionings(self, kwargs: dict[str, Any], msg: str) -> None:
        base: dict[str, Any] = {
            "column": "score",
            "kind": PartitionKind.NUMBER,
            "partitions": (Partition("all"),),
        }
        with pytest.raises((ValueError, UnsupportedConstruct), match=msg):
            Partitioning(**{**base, **kwargs})

    def test_catalog_partitioning_must_name_a_base_relation(self) -> None:
        with pytest.raises(ValueError, match="not a base relation"):
            StaticCatalog(
                base=frozenset({"a"}),
                derived={"v": frozenset({"a"})},
                partitions={"v": SCORES},
            )
        with pytest.raises(ValueError, match="not a Partitioning"):
            StaticCatalog(base=frozenset({"a"}), partitions={"a": "score"})  # type: ignore[dict-item]

    def test_date_partitions_accept_dates_only(self) -> None:
        with pytest.raises(ValueError, match="DATE"):
            Partitioning("day", PartitionKind.DATE, (Partition("a", lower="2024-1-1"),))
        with pytest.raises(ValueError, match="DATE"):
            Partitioning("day", PartitionKind.DATE, (Partition("a", lower=20240101),))


# ---------------------------------------------------------------------------
# PreComputation steps inside the IR (Rule 3, Step 1b, §8.14)
# ---------------------------------------------------------------------------


def _cost(n: int) -> CostEstimate:
    return CostEstimate(cpu_ops=n, io_ops=0)


def _variant(**kw: Any) -> ApproximateVariant:
    base: dict[str, Any] = dict(
        name="pc_entropy_approx",
        query="SIGNATURE_ENTROPY(sampled)",
        kind="graph",
        estimated_time=timedelta(seconds=1),
        cost_estimate=_cost(5),
        accuracy="reservoir sample of path instances (k=1000)",
        reads=frozenset({"graph_edge"}),
    )
    return ApproximateVariant(**{**base, **kw})


def _step(**kw: Any) -> PreComputation:
    base: dict[str, Any] = dict(
        name="pc_entropy",
        query="SIGNATURE_ENTROPY()",
        kind="graph",
        estimated_time=timedelta(seconds=60),
        cost_estimate=_cost(40),
        approximation=_variant(),
        reads=frozenset({"graph_edge"}),
    )
    return PreComputation(**{**base, **kw})


def _plan(*steps: PreComputation, q: DGMQuery | None = None) -> ExportPlan:
    plan = planner().plan_query(q or events(), pre_compute=steps)
    return dataclasses.replace(plan, cost_estimate=_cost(50), estimated_rows=10)


def _budget(*subs: Substitution) -> ExecutionBudget:
    return ExecutionBudget(
        max_estimated_cost=_cost(10_000),
        max_result_rows=1_000,
        max_wall_time=timedelta(seconds=30),
        max_precompute_time=timedelta(seconds=10),
        streaming_threshold=_cost(100),
        async_threshold=_cost(1_000),
        substitutions=frozenset(subs),
    )


def _gate() -> ExecutionGate:
    return ExecutionGate(precompute=DeclaredPrecomputeProvider())


class TestPreComputationsInTheIR:
    def test_steps_are_part_of_the_ir_and_have_an_identity(self) -> None:
        plan = _plan(_step())
        assert plan.ir is not None and plan.ir.pre_compute == (_step(),)
        assert plan.pre_compute == [_step()]
        assert key(plan) != key(_plan())
        assert plan.deps(CATALOG) == (
            Dependency("fact_event", ALL_MONTHS),
            Dependency("graph_edge"),
        )

    def test_key_covers_the_step_but_not_its_estimates(self) -> None:
        base = key(_plan(_step()))
        assert key(_plan(_step(query="SIGNATURE_ENTROPY(max_depth=3)"))) != base
        assert key(_plan(_step(reads=frozenset({"employee"})))) != base
        assert key(_plan(_step(kind="sql"))) != base
        same = _step(estimated_time=timedelta(seconds=1), approximation=None)
        assert key(_plan(same)) == base
        a, b = _step(name="pc_a", reads=None), _step(name="pc_b", reads=None)
        assert key(_plan(a, b)) != key(_plan(b, a))  # steps are ordered

    def test_exact_plan_and_its_approximation_never_share_a_key(self) -> None:
        plan = _plan(_step())
        d = _gate().gate(plan, _budget(Substitution.APPROX_ALGORITHM))
        assert isinstance(d, RewriteDecision)
        approx = d.plan
        assert approx.ir is not None
        assert approx.ir.pre_compute == tuple(approx.pre_compute)
        assert approx.pre_compute[0].accuracy == _variant().accuracy
        assert approx.query_text == plan.query_text
        assert key(approx) != key(plan)
        assert approx.deps(CATALOG) == plan.deps(CATALOG)
        c = _gate().gate(plan, _budget())
        assert isinstance(c, ClarifyDecision)
        assert key(c.options[0].plan) == key(approx)

    def test_identical_variant_text_still_has_another_key(self) -> None:
        v = _variant(name="pc_entropy", query="SIGNATURE_ENTROPY()")
        plan = _plan(_step(approximation=v))
        d = _gate().gate(plan, _budget(Substitution.APPROX_ALGORITHM))
        assert isinstance(d, RewriteDecision)
        assert key(d.plan) != key(plan)  # the accuracy statement differs

    def test_variant_reads_are_never_copied_from_the_exact_step(self) -> None:
        plan = _plan(_step(approximation=_variant(reads=None)))
        d = _gate().gate(plan, _budget(Substitution.APPROX_ALGORITHM))
        assert isinstance(d, RewriteDecision)
        with pytest.raises(UndeclaredReads, match="declares no reads"):
            d.plan.deps(CATALOG)

    def test_step_without_reads_has_a_key_but_no_deps(self) -> None:
        plan = _plan(_step(reads=None))
        assert key(plan)
        with pytest.raises(UndeclaredReads):
            plan.deps(CATALOG)
        with pytest.raises(UnknownRelation):  # UndeclaredReads is an UnknownRelation
            plan.deps(CATALOG)

    def test_step_reading_an_undeclared_relation_is_an_error(self) -> None:
        with pytest.raises(UnknownRelation, match="mystery"):
            _plan(_step(reads=frozenset({"mystery"}))).deps(CATALOG)

    def test_query_may_read_a_step_output(self) -> None:
        q = DGMQuery().anchor("pc_entropy", "p")
        plan = _plan(_step(), q=q)
        assert plan.ir is not None and relations_read(plan.ir) == {"pc_entropy"}
        assert plan.deps(CATALOG) == (Dependency("graph_edge"),)
        with pytest.raises(UnknownRelation, match="pc_entropy"):
            planner().plan_query(q).deps(CATALOG)

    def test_a_step_may_read_an_earlier_step(self) -> None:
        first = _step(name="pc_first")
        second = _step(name="pc_second", reads=frozenset({"pc_first", "employee"}))
        assert _plan(first, second).deps(CATALOG) == (
            Dependency("employee"),
            Dependency("fact_event", ALL_MONTHS),
            Dependency("graph_edge"),
        )

    @pytest.mark.parametrize(
        "steps,msg",
        [
            ((_step(), _step()), "bound twice"),
            ((_step(name="select"),), "reserved"),
            ((_step(name="pc x"),), "plain identifier"),
            ((_step(reads=frozenset({"pc_entropy"})),), "no earlier step"),
            (
                (_step(name="a", reads=frozenset({"b"})), _step(name="b")),
                "no earlier step",
            ),
        ],
    )
    def test_invalid_steps(self, steps: tuple[PreComputation, ...], msg: str) -> None:
        with pytest.raises(UnsupportedConstruct, match=msg):
            _plan(*steps)

    def test_step_name_must_not_be_a_cte_name(self) -> None:
        alg = QuestionAlgebra()
        alg.add("pc_entropy", events())
        with pytest.raises(UnsupportedConstruct, match="bound twice"):
            planner().plan_query(alg, final="pc_entropy", pre_compute=[_step()])

    def test_non_step_is_rejected(self) -> None:
        with pytest.raises(UnsupportedConstruct, match="not a PreComputation"):
            PlanIR(root=lower_query(events()), pre_compute=("SELECT 1",))  # type: ignore[arg-type]

    def test_reads_are_validated(self) -> None:
        with pytest.raises(ValueError, match="frozenset"):
            PreComputation("x", "q", reads={"a"})  # type: ignore[arg-type]
        with pytest.raises(UnsupportedConstruct, match="table name"):
            PreComputation("x", "q", reads=frozenset({"read_csv('f')"}))
        with pytest.raises(ValueError, match="frozenset"):
            _variant(reads=["graph_edge"])

    def test_steps_and_ir_must_agree(self) -> None:
        plan = _plan(_step())
        plan.pre_compute.append(_step(name="pc_other"))
        with pytest.raises(PlanIdentityUnavailable, match="no longer matches"):
            key(plan)
        with pytest.raises(PlanIdentityUnavailable):
            plan.deps(CATALOG)
        assert plan.ir is not None
        with pytest.raises(ValueError, match="pre_compute does not match"):
            ExportPlan(QueryTarget.SQL_DUCKDB, plan.query_text, ir=plan.ir)
        with pytest.raises(TypeError, match="comes from the IR"):
            ExportPlan.from_ir(plan.ir, QueryTarget.SQL_DUCKDB, pre_compute=[])

    def test_steps_are_immutable(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            _step().reads = None  # type: ignore[misc]


class _CostModel:
    def __init__(self, estimate: Any) -> None:
        self.estimate = estimate
        self.seen: list[Any] = []

    def estimate_precompute(self, expr: Any) -> Any:
        self.seen.append(expr)
        return self.estimate


BIG = GraphStatistics(node_count=SMALL_GRAPH_THRESHOLD, edge_count=10)
SMALL_G = GraphStatistics(node_count=SMALL_GRAPH_THRESHOLD - 1, edge_count=10)
EST = PrecomputeEstimate(
    estimated_time=timedelta(seconds=60),
    cost_estimate=_cost(40),
    approximation=_variant(),
)


class TestRule3TypedSteps:
    def test_large_graph_schedules_a_declared_step(self) -> None:
        cm = _CostModel(EST)
        expr = SubgraphExpr(SIGNATURE_ENTROPY())
        s = planner(BIG, cm).apply_rule_3(expr, reads=frozenset({"graph_edge"}))
        assert s.placement is Placement.PRECOMPUTE
        pc = s.pre_computation
        assert pc is not None and cm.seen == [expr]
        assert pc.kind == "graph" and pc.query == "SIGNATURE_ENTROPY()"
        assert pc.estimated_time == EST.estimated_time
        assert pc.cost_estimate == EST.cost_estimate
        assert pc.approximation == EST.approximation
        assert pc.reads == frozenset({"graph_edge"})
        assert pc.name.startswith("pc_") and pc.name == (
            planner(BIG, cm).apply_rule_3(expr).pre_computation.name  # type: ignore[union-attr]
        )

    def test_step_goes_through_the_ir_and_the_gate(self) -> None:
        s = planner(BIG, _CostModel(EST)).apply_rule_3(
            SubgraphExpr(SIGNATURE_ENTROPY()), reads=frozenset({"graph_edge"})
        )
        assert s.pre_computation is not None
        plan = _plan(s.pre_computation)
        d = _gate().gate(plan, _budget(Substitution.APPROX_ALGORITHM))
        assert isinstance(d, RewriteDecision)
        assert key(d.plan) != key(plan)
        assert d.plan.deps(CATALOG) == plan.deps(CATALOG)

    def test_nothing_is_invented_without_a_cost_model(self) -> None:
        s = planner(BIG, object()).apply_rule_3(SubgraphExpr(SIGNATURE_ENTROPY()))
        pc = s.pre_computation
        assert pc is not None
        assert (pc.estimated_time, pc.cost_estimate, pc.approximation) == (None,) * 3
        assert pc.reads is None
        d = _gate().gate(_plan(pc), _budget(Substitution.APPROX_ALGORITHM))
        assert isinstance(d, RejectDecision) and "time estimate" in d.reason
        with pytest.raises(UndeclaredReads):
            _plan(pc).deps(CATALOG)
        none = planner(BIG, _CostModel(None)).apply_rule_3(
            SubgraphExpr(SIGNATURE_ENTROPY())
        )
        assert none.pre_computation is not None
        assert none.pre_computation.estimated_time is None

    def test_unknown_node_count_is_not_small(self) -> None:
        s = planner(None).apply_rule_3(SubgraphExpr(GLOBAL_SIGNATURE_COUNT()))
        assert s.placement is Placement.PRECOMPUTE

    def test_small_graph_and_node_exprs_are_inline(self) -> None:
        for expr in (
            SubgraphExpr(SIGNATURE_ENTROPY()),
            SubgraphExpr(GLOBAL_SIGNATURE_COUNT()),
        ):
            s = planner(SMALL_G).apply_rule_3(expr)
            assert s.placement is Placement.INLINE and s.pre_computation is None
        s = planner(BIG).apply_rule_3(NodeExpr(OUTGOING_SIGNATURES(), alias="a"))
        assert s.placement is Placement.INLINE

    def test_distinct_algorithms_get_distinct_steps(self) -> None:
        p = planner(BIG)
        a = p.apply_rule_3(SubgraphExpr(SIGNATURE_ENTROPY())).pre_computation
        b = p.apply_rule_3(SubgraphExpr(SIGNATURE_ENTROPY(max_depth=3))).pre_computation
        assert a is not None and b is not None and a.name != b.name

    def test_scoped_subgraph_is_not_scheduled_without_a_descriptor(self) -> None:
        scope = RelationshipSubgraph(Bound("a"), Free())
        with pytest.raises(UnsupportedConstruct, match="scoped"):
            planner(BIG).apply_rule_3(SubgraphExpr(SIGNATURE_ENTROPY(), scope=scope))

    def test_cost_model_must_return_an_estimate(self) -> None:
        with pytest.raises(TypeError, match="estimate_precompute"):
            planner(BIG, _CostModel("60s")).apply_rule_3(
                SubgraphExpr(SIGNATURE_ENTROPY())
            )

    def test_schedule_invariant(self) -> None:
        with pytest.raises(ValueError, match="exactly when"):
            GraphExprSchedule(Placement.PRECOMPUTE, "x")
        with pytest.raises(ValueError, match="exactly when"):
            GraphExprSchedule(Placement.INLINE, "x", _step())


# ---------------------------------------------------------------------------
# Repeat (§3.2) in DGMQuery, the IR and deps
# ---------------------------------------------------------------------------


def reports_to(frm: str = "e", to: str = "m") -> BridgeHop:
    return BridgeHop(
        frm, "reports_to", to, table="employee", on=f"{to}.id = {frm}.manager_id"
    )


def managers(r: Repeat) -> DGMQuery:
    return (
        DGMQuery()
        .anchor("employee", "e")
        .path_join(r)
        .group_by("e.id")
        .agg(n="COUNT(m.id)")
    )


EMPLOYEES = [  # a chain 1 ← 2 ← 3 ← 4 ← 5 and a cycle 6 ⇄ 7
    (1, None, "CEO"),
    (2, 1, "VP"),
    (3, 2, "Dir"),
    (4, 3, "Mgr"),
    (5, 4, "Eng"),
    (6, 7, "A"),
    (7, 6, "B"),
]


def employee_db(extra: dict[str, str] | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"CREATE TABLE employee ({TABLES['employee']})")
    con.executemany("INSERT INTO employee VALUES (?, ?, ?)", EMPLOYEES)
    for name, ddl in (extra or {}).items():
        con.execute(f"CREATE TABLE {name} ({ddl})")
    return con


class TestRepeatConstruction:
    @pytest.mark.parametrize(
        "args,kwargs,msg",
        [
            ((-1, 3), {}, "min"),
            ((True, 3), {}, "min"),
            ((UNBOUNDED, 3), {}, "min"),
            ((0, 0), {}, "R2"),
            ((3, 2), {}, "R2"),
            ((1, 2.5), {}, "R2"),
            ((1, UNBOUNDED), {"mode": RepeatMode.WALK}, "R4"),
            ((1, 2), {"mode": "WALK"}, "RepeatMode"),
            ((1, 2), {"depth_alias": 3}, "depth_alias"),
        ],
    )
    def test_well_formedness(
        self, args: tuple, kwargs: dict[str, Any], msg: str
    ) -> None:
        with pytest.raises(SemanticError, match=msg):
            Repeat(reports_to(), *args, **kwargs)

    def test_body_is_hops_without_repeat(self) -> None:
        inner = Repeat(reports_to(), 1, 2)
        with pytest.raises(SemanticError, match="R3"):
            Repeat(PathCompose(reports_to(), inner), 1, 2)
        with pytest.raises(SemanticError, match="hop"):
            Repeat("e.manager_id", 1, 2)

    def test_accepted_forms(self) -> None:
        r = Repeat(reports_to(), 0, UNBOUNDED, depth_alias="lvl")
        assert (r.min, r.max, r.mode, r.bounded) == (
            0,
            UNBOUNDED,
            RepeatMode.SIMPLE,
            False,
        )
        assert (r.from_alias, r.to_alias) == ("e", "m")
        assert repr(UNBOUNDED) == "UNBOUNDED"
        assert Repeat(reports_to(), 2, 2, mode=RepeatMode.WALK).bounded

    def test_legacy_to_sql_refuses_repeat(self) -> None:
        with pytest.raises(SemanticError, match="plan_query"):
            managers(Repeat(reports_to(), 1, 2)).to_sql()
        pp = PathPred(
            "e",
            Repeat(reports_to(), 1, 2),
            ScalarPred(PropRef("m", "title"), "=", "VP"),
        )
        with pytest.raises(SemanticError, match="plan_query"):
            pp.to_sql()


class TestRepeatReads:
    def test_path_join_repeat_reads_are_deps(self) -> None:
        q = (
            DGMQuery()
            .anchor("dim_customer", "c")
            .path_join(VerbHop("c", "is", "e", table="employee", on="e.id = c.id"))
            .path_join(Repeat(reports_to(), 1, UNBOUNDED))
        )
        ir = PlanIR(root=lower_query(q))
        roles = {s.role for s in ir.root.scans()}  # type: ignore[union-attr]
        assert ReadRole.REPEAT in roles
        assert deps(ir, CATALOG) == (
            Dependency("dim_customer", ("am", "eu")),
            Dependency("employee"),
        )
        with pytest.raises(UnsupportedConstruct, match="SIMPLE"):
            planner().plan_query(q)  # SIMPLE needs node identities to emit

    def test_exists_repeat_reads_are_deps(self) -> None:
        body = PathCompose(
            VerbHop("e", "linked", "g", table="graph_edge", on="g.src = e.id"),
            BridgeHop("g", "to", "m", table="employee", on="m.id = g.dst"),
        )
        vp = PathPred(
            "e",
            Repeat(body, 1, UNBOUNDED, mode=RepeatMode.TRAIL),
            ScalarPred(PropRef("m", "title"), "=", "VP"),
        )
        q = DGMQuery().anchor("employee", "e").where(vp)
        ir = PlanIR(root=lower_query(q))
        assert deps(ir, CATALOG) == (Dependency("employee"), Dependency("graph_edge"))
        with pytest.raises(UnsupportedConstruct, match="per iteration"):
            planner().plan_query(q)
        with pytest.raises(UnknownRelation, match="graph_edge"):
            deps(ir, StaticCatalog(base=frozenset({"employee"})))

    def test_repeat_shape_is_in_the_hash(self) -> None:
        def h(r: Repeat) -> str:
            return dag_hash(PlanIR(root=lower_query(managers(r))))

        base = h(Repeat(reports_to(), 1, 3))
        assert h(Repeat(reports_to(), 1, 3)) == base
        assert h(Repeat(reports_to(), 1, 4)) != base
        assert h(Repeat(reports_to(), 0, 3)) != base
        assert h(Repeat(reports_to(), 1, UNBOUNDED)) != base
        assert h(Repeat(reports_to(), 1, 3, mode=RepeatMode.TRAIL)) != base
        assert h(Repeat(reports_to(), 1, 3, depth_alias="lvl")) != base

    def test_repeat_start_must_be_in_scope(self) -> None:
        q = managers(Repeat(reports_to(frm="x"), 1, 2, mode=RepeatMode.WALK))
        with pytest.raises(UnsupportedConstruct, match="not in scope"):
            lower_query(q)

    def test_iteration_alias_clash(self) -> None:
        q = (
            DGMQuery()
            .anchor("employee", "m__r1")
            .path_join(Repeat(reports_to(frm="m__r1"), 1, 2, mode=RepeatMode.WALK))
        )
        with pytest.raises(UnsupportedConstruct, match="clash"):
            lower_query(q)

    def test_repeat_under_as_of(self) -> None:
        q = managers(Repeat(reports_to(), 1, 2, mode=RepeatMode.WALK)).as_of(
            "2024-01-01"
        )
        with pytest.raises(UnsupportedConstruct, match="as_of"):
            lower_query(q)


class TestRepeatEmission:
    def test_walk_is_unrolled_and_exact(self) -> None:
        plan = planner().plan_query(
            managers(Repeat(reports_to(), 1, 3, mode=RepeatMode.WALK))
        )
        assert "LEFT JOIN LATERAL" in plan.query_text
        assert plan.query_text.count("UNION ALL") == 2
        rows = dict(employee_db().execute(plan.query_text).fetchall())
        # chain: up to three levels; cycle: a walk revisits 6 and 7
        assert rows == {1: 0, 2: 1, 3: 2, 4: 3, 5: 3, 6: 3, 7: 3}
        assert plan.deps(CATALOG) == (Dependency("employee"),)

    def test_bounds_select_iteration_counts(self) -> None:
        plan = planner().plan_query(
            managers(Repeat(reports_to(), 2, 2, mode=RepeatMode.WALK))
        )
        rows = dict(employee_db().execute(plan.query_text).fetchall())
        assert rows == {1: 0, 2: 0, 3: 1, 4: 1, 5: 1, 6: 1, 7: 1}

    def test_emitted_sql_reads_only_deps(self) -> None:
        from tests.query.test_dgm_plan_ir import referenced_tables

        q = managers(Repeat(reports_to(), 1, 2, mode=RepeatMode.WALK))
        plan = planner().plan_query(q)
        assert plan.ir is not None
        assert referenced_tables(plan.query_text) == set(relations_read(plan.ir))
        with pytest.raises(duckdb.Error):
            duckdb.connect().execute(plan.query_text).fetchall()

    @pytest.mark.parametrize(
        "r,msg",
        [
            (Repeat(reports_to(), 0, 2, mode=RepeatMode.WALK), "min = 0"),
            (
                Repeat(reports_to(), 1, 2, mode=RepeatMode.WALK, depth_alias="lvl"),
                "depth_alias",
            ),
            (
                Repeat(reports_to(), 1, MAX_UNROLL + 1, mode=RepeatMode.WALK),
                "MAX_UNROLL",
            ),
            (Repeat(reports_to(), 1, 2, mode=RepeatMode.TRAIL), "TRAIL"),
            (
                Repeat(
                    PathCompose(
                        VerbHop(
                            "e", "linked", "g", table="graph_edge", on="g.src = e.id"
                        ),
                        BridgeHop("g", "to", "m", table="employee", on="m.id = g.dst"),
                    ),
                    1,
                    2,
                    mode=RepeatMode.WALK,
                ),
                "one-hop",
            ),
        ],
    )
    def test_unsupported_forms_fail_closed(self, r: Repeat, msg: str) -> None:
        with pytest.raises(UnsupportedConstruct, match=msg):
            planner().plan_query(managers(r))

    def test_body_must_end_at_its_start_relation(self) -> None:  # R1
        hop = BridgeHop("c", "reports_to", "m", table="employee", on="m.id = c.id")
        q = (
            DGMQuery()
            .anchor("dim_customer", "c")
            .path_join(Repeat(hop, 1, 2, mode=RepeatMode.WALK))
        )
        with pytest.raises(UnsupportedConstruct, match="R1"):
            planner().plan_query(q)

    def test_rename_aliases_only_touches_qualifiers(self) -> None:
        f = rename_aliases(
            Fragment("m.id = e.manager_id AND 'e.x' = em.e"), {"e": "m__r1"}
        )
        assert f.sql == "m.id = m__r1.manager_id AND 'e.x' = em.e"
