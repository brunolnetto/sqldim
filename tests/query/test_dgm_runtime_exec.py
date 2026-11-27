"""§11.10 runtime APIs: the executor and the recommender (#149)."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest

from sqldim.core.query.dgm import DGMQuery, PropRef, ScalarPred
from sqldim.core.query.dgm.planner import (
    AsyncDecision,
    ClarifyDecision,
    ExecutionGate,
    ExportPlan,
    PaginateDecision,
    PlanIdentityUnavailable,
    ProceedDecision,
    RejectDecision,
    RewriteDecision,
    SampleDecision,
    SampleMethod,
    SinkTarget,
    StreamDecision,
    Substitution,
)
from sqldim.core.query.dgm.planner._gate import ClarifyKind
from sqldim.core.query.dgm.runtime import (
    DeclaredQuestion,
    DeclaredQuestionSpace,
    DuckDBBackend,
    InvalidRuntimeArgument,
    InvalidSpec,
    JobTicket,
    NoCandidates,
    NotConfigured,
    NotExecutable,
    PlanExecutor,
    PlanRef,
    ProposedQuestion,
    RefinementAtom,
    ResultKey,
    RuntimeRecommender,
    StaticDatasets,
    SubmissionFailed,
    Unauthorized,
    UnknownIdentifier,
    UnsupportedOperation,
    encode_spec,
)
from tests.query._runtime_fixtures import (
    CATALOG,
    DATASETS,
    FilterCostModel,
    RecordingSink,
    Watermarks,
    alg_union,
    auth,
    budget,
    cost,
    database,
    make_dag,
    plan_of,
    q_contradiction,
    q_customers,
    q_eu,
    q_sales,
    spec_of,
)

# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


def executor(
    a: Any = None, sink: RecordingSink | None = None, **kw: Any
) -> PlanExecutor:
    args: dict[str, Any] = {
        "auth": a if a is not None else auth(write=("results",)),
        "datasets": StaticDatasets(DATASETS),
        "catalog": CATALOG,
        "backend": DuckDBBackend(database()),
        "job_sinks": {"DUCKDB": sink if sink is not None else RecordingSink()},
        "stream_batch_size": 3,
    }
    args.update(kw)
    return PlanExecutor(**args)


def all_rows(result: Any) -> list[tuple]:
    if result.rows is not None:
        return sorted(result.rows)
    return sorted(r for chunk in result.chunks for r in chunk)


class TestRun:
    def test_proceed_returns_the_whole_result(self) -> None:
        res = executor().run(plan_of(q_eu()), "PROCEED")
        assert res.mode == "PROCEED" and res.columns == ("id", "region")
        assert len(res.rows) == 5 and res.datasets == frozenset({"crm"})

    def test_stream_and_paginate_deliver_the_exact_result(self) -> None:
        ex = executor()
        plan = plan_of(q_sales())
        exact = all_rows(ex.run(plan, ProceedDecision()))
        streamed = ex.run(plan, StreamDecision())
        assert streamed.chunk_size == 3
        assert all_rows(streamed) == exact
        paged = ex.run(plan, PaginateDecision(page_size=1))
        chunks = list(paged.chunks)
        assert (
            all(len(c) == 1 for c in chunks)
            and sorted(r for c in chunks for r in c) == exact
        )
        assert all_rows(ex.run(plan, "PAGINATE", page_size=4)) == exact

    def test_rewrite_runs_its_plan_under_then_with_warning(self) -> None:
        rewritten = plan_of(q_eu())
        d = RewriteDecision(
            plan=rewritten,
            substitution=Substitution.DEPTH_LIMIT,
            warning="depth limited to 3",
            then=ProceedDecision(),
        )
        res = executor().run(None, d)
        assert res.warning == "depth limited to 3" and len(res.rows) == 5
        with pytest.raises(InvalidRuntimeArgument, match="not the plan"):
            executor().run(plan_of(q_customers()), d)

    def test_sample_carries_its_bound(self) -> None:
        d = SampleDecision(
            plan=plan_of(q_eu()),
            fraction=0.5,
            method=SampleMethod.RANDOM,
            error_bound=0.05,
            confidence=0.95,
            then=StreamDecision(),
        )
        res = executor().run(None, d)
        assert res.mode == "STREAM" and "95% confidence" in (res.warning or "")

    @pytest.mark.parametrize(
        "mode",
        [
            "ASYNC",
            "REWRITE",
            "SAMPLE",
            "CLARIFY",
            AsyncDecision(sink=SinkTarget.DUCKDB, callback="cb", ttl=timedelta(1)),
            RejectDecision(reason="no"),
        ],
    )
    def test_non_deliveries_execute_nothing(self, mode: Any) -> None:
        with pytest.raises(NotExecutable):
            executor().run(plan_of(q_eu()), mode)

    def test_rewrite_whose_then_is_async_is_not_run(self) -> None:
        d = RewriteDecision(
            plan=plan_of(q_eu()),
            substitution=Substitution.DEPTH_LIMIT,
            warning="w",
            then=AsyncDecision(sink=SinkTarget.DUCKDB, callback="cb", ttl=timedelta(1)),
        )
        with pytest.raises(NotExecutable, match="ASYNC"):
            executor().run(None, d)

    def test_paginate_needs_its_page_size(self) -> None:
        with pytest.raises(InvalidRuntimeArgument):
            executor().run(plan_of(q_eu()), "PAGINATE")
        with pytest.raises(InvalidRuntimeArgument):
            executor().run(plan_of(q_eu()), "PROCEED", page_size=3)

    def test_without_read_grant(self) -> None:
        with pytest.raises(Unauthorized):
            executor(auth(read=("crm",))).run(plan_of(q_sales()), "PROCEED")

    def test_without_auth_context(self) -> None:
        with pytest.raises(Unauthorized):
            executor(a=object())

    def test_relation_outside_every_dataset(self) -> None:
        ex = executor(datasets=StaticDatasets({"dim_customer": "crm"}))
        with pytest.raises(UnknownIdentifier):
            ex.run(plan_of(q_sales()), "PROCEED")

    def test_plan_without_ir(self) -> None:
        text = ExportPlan(plan_of(q_eu()).query_target, "SELECT 1")
        with pytest.raises(PlanIdentityUnavailable):
            executor().run(text, "PROCEED")

    def test_write_is_not_performed(self) -> None:
        plan = replace(plan_of(q_eu()), sink_target=SinkTarget.DUCKDB)
        with pytest.raises(InvalidRuntimeArgument, match="materialise"):
            executor().run(plan, "PROCEED")

    def test_cancel_interrupts_the_backend(self) -> None:
        calls = []

        class Backend:
            target = plan_of(q_eu()).query_target

            def execute(self, sql: str, datasets: frozenset[str]) -> Any:
                raise AssertionError("not called")

            def interrupt(self) -> None:
                calls.append(1)

        executor(backend=Backend()).cancel()
        assert calls == [1]


class TestSubmit:
    @staticmethod
    def key(
        q: Any = None, marks: Watermarks | None = None
    ) -> tuple[ExportPlan, ResultKey]:
        dag, _ = make_dag(watermarks=marks or Watermarks())
        ref = PlanRef(dag.question_key(dag.make_from_spec(spec_of(q or q_sales()))))
        return dag.plan_for(ref), dag.result_key(ref)

    @staticmethod
    def submit(
        ex: PlanExecutor, plan: ExportPlan, key: ResultKey, **kw: Any
    ) -> JobTicket:
        args: dict[str, Any] = {
            "plan": plan,
            "sink": "DUCKDB",
            "callback": "https://host/cb",
            "ttl_s": 3600,
            "idempotency_key": key,
        }
        args.update(kw)
        return asyncio.run(ex.submit(**args))

    def test_ticket_and_idempotency(self) -> None:
        sink = RecordingSink()
        ex = executor(sink=sink)
        plan, key = self.key()
        t1 = self.submit(ex, plan, key)
        t2 = self.submit(ex, plan, key, sink=SinkTarget.DUCKDB)
        assert t1 == t2 == JobTicket("j_1", "QUEUED")
        assert len(sink.requests) == 1
        req = sink.requests[0]
        assert req.datasets == frozenset({"crm", "sales"}) and req.tenant_id == "acme"
        assert req.ttl == timedelta(hours=1) and req.idempotency_key == key

    def test_new_snapshot_is_a_new_job(self) -> None:
        sink = RecordingSink()
        ex = executor(sink=sink)
        marks = Watermarks()
        plan, k1 = self.key(marks=marks)
        marks.marks["fact_sale"] = 9
        _, k2 = self.key(marks=marks)
        assert self.submit(ex, plan, k1) != self.submit(ex, plan, k2)

    def test_unversioned_key_is_never_reused(self) -> None:
        sink = RecordingSink()
        ex = executor(sink=sink)
        plan, key = self.key(marks=Watermarks({"dim_customer": 1}))
        assert not key.versioned
        self.submit(ex, plan, key)
        self.submit(ex, plan, key)
        assert len(sink.requests) == 2

    @pytest.mark.parametrize("sink", [None, "", "PARQUET"])
    def test_no_default_sink(self, sink: Any) -> None:
        plan, key = self.key()
        with pytest.raises(NotConfigured):
            self.submit(executor(), plan, key, sink=sink)

    def test_missing_callback_or_ttl(self) -> None:
        plan, key = self.key()
        with pytest.raises(NotConfigured):
            self.submit(executor(), plan, key, callback=None)
        with pytest.raises(InvalidRuntimeArgument):
            self.submit(executor(), plan, key, ttl_s=0)

    def test_sink_needs_write(self) -> None:
        sink = RecordingSink()
        plan, key = self.key()
        with pytest.raises(Unauthorized):
            self.submit(executor(auth(), sink=sink), plan, key)
        assert sink.requests == []

    def test_plan_needs_read(self) -> None:
        sink = RecordingSink()
        plan, key = self.key()
        a = auth(read=("crm",), write=("results",))
        with pytest.raises(Unauthorized):
            self.submit(executor(a, sink=sink), plan, key)
        assert sink.requests == []

    def test_refused_enqueue_is_a_typed_failure(self) -> None:
        plan, key = self.key()
        sink = RecordingSink(fail=RuntimeError("queue full"))
        ex = executor(sink=sink)
        with pytest.raises(SubmissionFailed, match="queue full"):
            self.submit(ex, plan, key)
        sink.fail = None
        assert self.submit(ex, plan, key).job_id == "j_2"  # failure was not cached

    def test_malformed_ticket(self) -> None:
        plan, key = self.key()
        sink = RecordingSink(ticket=JobTicket("", "QUEUED"))
        with pytest.raises(SubmissionFailed):
            self.submit(executor(sink=sink), plan, key)

    def test_key_of_another_plan(self) -> None:
        plan, _ = self.key(q_sales())
        _, other = self.key(q_eu())
        with pytest.raises(InvalidRuntimeArgument, match="another plan"):
            self.submit(executor(), plan, other)
        with pytest.raises(InvalidRuntimeArgument):
            self.submit(executor(), plan, other.value)


# ---------------------------------------------------------------------------
# Recommender
# ---------------------------------------------------------------------------


def grounding(
    *aliases: str, unresolved: tuple[str, ...] = (), version: int = 1
) -> dict:
    return {
        "entity_result": {
            "utterance_version": version,
            "resolved": [{"alias": a, "prop": "region"} for a in aliases],
            "unresolved": list(unresolved),
            "node_aliases": {f"term_{a}": a for a in aliases},
        },
        "temporal_result": {"utterance_version": version},
        "compositional": {"utterance_version": version},
    }


SEGMENT = RefinementAtom(
    "c", {"cmp": ["c.region", "=", "EU"]}, "Restrict to EU customers"
)


def space(*extra: DeclaredQuestion) -> DeclaredQuestionSpace:
    return DeclaredQuestionSpace(
        questions_declared=(
            DeclaredQuestion(spec_of(q_customers()), "All customers"),
            DeclaredQuestion(spec_of(q_eu()), "EU customers"),
            DeclaredQuestion(spec_of(q_contradiction()), "Contradiction"),
            DeclaredQuestion(spec_of(q_sales()), "Revenue by region"),
            *extra,
        ),
        atoms=(SEGMENT,),
    )


def recommender(
    sp: Any = None, cm: FilterCostModel | None = None
) -> tuple[RuntimeRecommender, Any]:
    dag, _ = make_dag()
    rec = RuntimeRecommender(
        dag=dag, space=sp or space(), cost_model=cm or FilterCostModel()
    )
    return rec, dag


class TestTopK:
    def test_feasible_unique_ranked_candidates(self) -> None:
        rec, dag = recommender(
            space(DeclaredQuestion(spec_of(alg_union("x", "y")), "dup"))
        )
        items = rec.top_k_candidates(**grounding("c"), k=10)
        assert [c.description for c in items] == [
            "All customers",
            "EU customers",
            "Revenue by region",
            "dup",
        ]
        assert items[2].band_coverage == ["B1", "B2"]
        assert items[0].cost_estimate == 15_000.0
        assert len({c.candidate_id for c in items}) == 4
        node = dag.make_from_spec(items[1].spec)  # a candidate is a buildable spec
        assert dag.question_key(node) == dag.key_for_spec(spec_of(q_eu()))

    def test_equivalent_questions_are_offered_once(self) -> None:
        rec, _ = recommender(space(DeclaredQuestion(spec_of(q_eu()), "EU again")))
        items = rec.top_k_candidates(**grounding("c"), k=10)
        assert [c.description for c in items].count("EU customers") == 1
        assert "EU again" not in [c.description for c in items]

    def test_paging(self) -> None:
        rec, _ = recommender()
        first = rec.top_k_candidates(**grounding("c"), k=2, page=0)
        second = rec.top_k_candidates(**grounding("c"), k=2, page=1)
        assert [c.description for c in first + second] == [
            "All customers",
            "EU customers",
            "Revenue by region",
        ]
        with pytest.raises(NoCandidates, match="beyond page 1"):
            rec.top_k_candidates(**grounding("c"), k=2, page=2)

    def test_grounding_failures(self) -> None:
        rec, _ = recommender()
        with pytest.raises(NoCandidates, match="unresolved"):
            rec.top_k_candidates(**grounding("c", unresolved=("churn",)), k=3)
        with pytest.raises(NoCandidates, match="nothing was resolved"):
            rec.top_k_candidates(**grounding(), k=3)
        with pytest.raises(NoCandidates, match="no feasible"):
            rec.top_k_candidates(**grounding("z"), k=3)
        g = grounding("c")
        g["temporal_result"] = {"utterance_version": 2}
        with pytest.raises(InvalidRuntimeArgument, match="versions"):
            rec.top_k_candidates(**g, k=3)
        with pytest.raises(InvalidRuntimeArgument):
            rec.top_k_candidates(**grounding("c"), k=0)
        with pytest.raises(InvalidRuntimeArgument):
            rec.top_k_candidates(**{**grounding("c"), "temporal_result": None}, k=3)

    def test_unpriced_candidate_is_not_offered(self) -> None:
        rec, _ = recommender(cm=FilterCostModel(unknown=True))
        with pytest.raises(UnsupportedOperation, match="no estimate"):
            rec.top_k_candidates(**grounding("c"), k=3)

    def test_malformed_declaration_fails_at_construction(self) -> None:
        with pytest.raises(InvalidSpec):
            DeclaredQuestionSpace(
                questions_declared=(DeclaredQuestion({"x": 1}, "bad"),)
            )
        with pytest.raises(Exception):
            DeclaredQuestionSpace(
                atoms=(RefinementAtom("c", {"raw": "z.id > 0"}, "bad"),)
            )


class TestRefinementsAndClarify:
    def setup_gate(self) -> tuple[RuntimeRecommender, Any, PlanRef]:
        rec, dag = recommender()
        gate = ExecutionGate(refinements=rec)
        rec.attach_gate(gate)
        ref = PlanRef(dag.question_key(dag.make_from_spec(spec_of(q_customers()))))
        return rec, dag, ref

    def test_refinements_back_the_gate(self) -> None:
        rec, dag, ref = self.setup_gate()
        plan = dag.plan_for(ref)
        refs = rec.refinements(plan, budget())
        assert len(refs) == 1 and refs[0].plan.cost_estimate == cost(50)
        d = ExecutionGate(refinements=rec).gate(plan, budget())
        assert isinstance(d, ClarifyDecision)
        assert d.options[0].kind is ClarifyKind.REFINEMENT

    def test_clarification_options_end_in_a_delivery(self) -> None:
        rec, dag, ref = self.setup_gate()
        options = rec.clarification_options(ref, budget=budget())
        assert [o.kind for o in options] == ["REFINEMENT"]
        assert options[0].description == "Restrict to EU customers"
        assert options[0].cost_estimate == 50.0
        node = dag.make_from_spec(options[0].spec)  # chosen: a new question
        chosen = dag.plan_for(PlanRef(dag.question_key(node)))
        assert isinstance(ExecutionGate().gate(chosen, budget()), ProceedDecision)

    def test_no_clarification_for_an_executable_plan(self) -> None:
        rec, dag, _ = self.setup_gate()
        ref = PlanRef(dag.question_key(dag.make_from_spec(spec_of(q_eu()))))
        with pytest.raises(NoCandidates, match="PROCEED"):
            rec.clarification_options(ref, budget=budget())

    def test_without_a_gate(self) -> None:
        rec, dag = recommender()
        ref = PlanRef(dag.question_key(dag.make_from_spec(spec_of(q_customers()))))
        with pytest.raises(NotConfigured):
            rec.clarification_options(ref, budget=budget())
        rec.attach_gate(ExecutionGate())
        with pytest.raises(InvalidRuntimeArgument):
            rec.attach_gate(ExecutionGate())

    def test_already_filtered_atom_is_not_proposed(self) -> None:
        rec, dag = recommender()
        assert rec.refinements(plan_of(q_eu()), budget()) == []

    def test_non_descent_proposal_is_refused(self) -> None:
        class Space:
            def questions(self, g: Any) -> list:
                return []

            def refinements(self, ir: Any) -> list[ProposedQuestion]:
                return [ProposedQuestion(spec_of(q_sales()), "not a narrowing")]

        rec, _ = recommender(sp=Space())
        with pytest.raises(UnsupportedOperation, match="descent"):
            rec.refinements(plan_of(q_customers()), budget())

    def test_composed_question_refinement_is_unsupported(self) -> None:
        class Space:
            def questions(self, g: Any) -> list:
                return []

            def refinements(self, ir: Any) -> list[ProposedQuestion]:
                return [ProposedQuestion(spec_of(q_eu()), "x")]

        rec, _ = recommender(sp=Space())
        with pytest.raises(UnsupportedOperation, match="composed"):
            rec.refinements(plan_of(alg_union()), budget())

    def test_plan_without_ir(self) -> None:
        rec, _ = recommender()
        with pytest.raises(PlanIdentityUnavailable):
            rec.refinements(
                ExportPlan(plan_of(q_eu()).query_target, "SELECT 1"), budget()
            )

    def test_descent_must_narrow(self) -> None:
        base = plan_of(
            DGMQuery()
            .anchor("dim_customer", "c")
            .where(ScalarPred(PropRef("c", "region"), "=", "EU"))
        )
        assert base.ir is not None
        same = encode_spec(base.ir)

        class Space:
            def questions(self, g: Any) -> list:
                return []

            def refinements(self, ir: Any) -> list[ProposedQuestion]:
                return [ProposedQuestion(same, "no-op")]

        rec, _ = recommender(sp=Space())
        with pytest.raises(UnsupportedOperation):
            rec.refinements(base, budget())
