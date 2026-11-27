"""§6.3 / D40 / ADR 0010: the pre-execution gate.

The boundary cases T1–T20 are normative.  Budget: streaming 100, async 1 000,
max 10 000, HARD_CEILING_FACTOR 2 (ceiling 20 000).  T1–T15 allow all five
substitutions; T16–T20 use the default ∅.  Rewrites, samples, refinements and
approximations are injected through the provider protocols.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

import pytest

from sqldim.core.query.dgm.planner._gate import (
    HARD_CEILING_FACTOR,
    MAX_CLARIFY_OPTIONS,
    SAMPLE_DELTA,
    SAMPLE_MAX_ERROR,
    Approximation,
    AsyncDecision,
    AsyncNotConfigured,
    AsyncTarget,
    BudgetDecisionKind,
    ClarifyDecision,
    ClarifyKind,
    ClarifyOption,
    ExecutionBudget,
    ExecutionGate,
    InvalidBudget,
    PaginateDecision,
    ProceedDecision,
    Refinement,
    RejectDecision,
    RewriteCandidate,
    RewriteDecision,
    SampleDecision,
    SampleMethod,
    SamplingPlan,
    StreamDecision,
    Substitution,
    Zone,
    zone,
)
from sqldim.core.query.dgm.planner._targets import (
    CostEstimate,
    ExportPlan,
    PreComputation,
    QueryTarget,
    SinkTarget,
)

ALL = frozenset(Substitution)
NONE: frozenset[Substitution] = frozenset()
S = Substitution


def cost(n: float) -> CostEstimate:
    return CostEstimate(cpu_ops=n, io_ops=0)  # type: ignore[arg-type]


def budget(subs: frozenset[Substitution] = ALL, **kw: Any) -> ExecutionBudget:
    fields: dict[str, Any] = {
        "max_estimated_cost": cost(10_000),
        "max_result_rows": 1_000,
        "max_wall_time": timedelta(seconds=30),
        "max_precompute_time": timedelta(seconds=10),
        "streaming_threshold": cost(100),
        "async_threshold": cost(1_000),
        "substitutions": subs,
    }
    fields.update(kw)
    return ExecutionBudget(**fields)


def plan(
    est: float | None, *, rows: int | None = 10, name: str = "q", pcs: int = 0
) -> ExportPlan:
    return ExportPlan(
        query_target=QueryTarget.SQL_DUCKDB,
        query_text=name,
        cost_estimate=None if est is None else cost(est),
        estimated_rows=rows,
        pre_compute=[PreComputation(name=f"pc{i}", query="x") for i in range(pcs)],
    )


class Rewrites:
    def __init__(self, *pairs: tuple[Substitution, float]) -> None:
        self.pairs = pairs

    def candidates(self, p: ExportPlan) -> Sequence[RewriteCandidate]:
        return [
            RewriteCandidate(plan(c, name=f"rw:{s.value}"), s, f"{s.value} warning")
            for s, c in self.pairs
        ]


class Sampling:
    def __init__(self, sampled_cost: float | None) -> None:
        self.sampled_cost = sampled_cost

    def sampling_plan(self, p: ExportPlan, b: ExecutionBudget) -> SamplingPlan | None:
        if self.sampled_cost is None:  # S1–S4 fail
            return None
        return SamplingPlan(plan(self.sampled_cost, name="sample"), fraction=0.01)


class Refinements:
    def __init__(self, *costs: float) -> None:
        self.costs = costs

    def refinements(self, p: ExportPlan, b: ExecutionBudget) -> Sequence[Refinement]:
        return [
            Refinement(plan(c, name=f"ref{i}"), rank=i)
            for i, c in enumerate(self.costs)
        ]


class Precompute:
    def __init__(
        self, *, slow: bool = True, approx: float | None = None, known: bool = True
    ) -> None:
        self.slow, self.approx, self.known = slow, approx, known

    def estimated_time(self, pc: PreComputation) -> timedelta | None:
        if not self.known:
            return None
        return timedelta(seconds=60 if self.slow else 1)

    def approximate(
        self, p: ExportPlan, slow: Sequence[PreComputation]
    ) -> Approximation | None:
        if self.approx is None:
            return None
        return Approximation(plan(self.approx, name="approx"), "approximate algorithm")


TARGET = AsyncTarget(sink=SinkTarget.PARQUET, callback="https://host/cb")


def gate(**providers: Any) -> ExecutionGate:
    providers.setdefault("async_target", TARGET)
    return ExecutionGate(**providers)


# ---------------------------------------------------------------------------
# T1–T20
# ---------------------------------------------------------------------------


def _t(
    est: float,
    subs: frozenset[Substitution] = ALL,
    *,
    rows: int = 10,
    rewrites: Rewrites | None = None,
    sampling: Sampling | None = None,
    refinements: Refinements | None = None,
    precompute: Precompute | None = None,
    pcs: int = 0,
) -> Any:
    g = gate(
        rewrites=rewrites,
        sampling=sampling,
        refinements=refinements,
        precompute=precompute,
    )
    return g.gate(plan(est, rows=rows, pcs=pcs), budget(subs))


def _kinds(d: ClarifyDecision) -> list[tuple[str, float]]:
    return [
        (o.substitution.value if o.substitution else "REFINEMENT", o.cost.cpu_ops)
        for o in d.options
    ]


class TestBoundaryCases:
    def test_T1_equal_to_streaming_threshold_stays_sync(self) -> None:
        assert _t(100) == ProceedDecision()

    def test_T2_rows_over_limit_paginates(self) -> None:
        assert _t(100, rows=5_000) == PaginateDecision(page_size=1_000)

    def test_T3_stream(self) -> None:
        assert _t(101) == StreamDecision()

    def test_T4_equal_to_async_threshold_streams(self) -> None:
        assert _t(1_000) == StreamDecision()

    def test_T5_async(self) -> None:
        d = _t(1_001)
        assert isinstance(d, AsyncDecision)
        assert (d.sink, d.callback) == (TARGET.sink, TARGET.callback)

    def test_T6_equal_to_max_cost_is_async(self) -> None:
        assert isinstance(_t(10_000), AsyncDecision)

    def test_T7_rewrite_then_proceed(self) -> None:
        d = _t(5_000, rewrites=Rewrites((S.STRATEGY_SHORTEST, 80)))
        assert isinstance(d, RewriteDecision)
        assert d.substitution is S.STRATEGY_SHORTEST
        assert d.then == ProceedDecision()
        assert d.warning == "STRATEGY_SHORTEST warning"

    def test_T8_rewrite_then_stream(self) -> None:
        d = _t(5_000, rewrites=Rewrites((S.STRATEGY_SHORTEST, 600)))
        assert isinstance(d, RewriteDecision) and d.then == StreamDecision()

    def test_T9_rewrite_in_same_zone_is_not_viable(self) -> None:
        d = _t(5_000, rewrites=Rewrites((S.STRATEGY_SHORTEST, 1_200)))
        assert isinstance(d, AsyncDecision)

    def test_T10_rewrite_precedes_sample(self) -> None:
        d = _t(
            15_000,
            rewrites=Rewrites((S.CONE_SCOPE, 900)),
            sampling=Sampling(40),
            refinements=Refinements(700, 4_000),
        )
        assert isinstance(d, RewriteDecision)
        assert d.substitution is S.CONE_SCOPE and d.then == StreamDecision()

    def test_T11_sample_when_rewrite_stays_over(self) -> None:
        d = _t(
            15_000,
            rewrites=Rewrites((S.CONE_SCOPE, 12_000)),
            sampling=Sampling(40),
            refinements=Refinements(700, 4_000),
        )
        assert isinstance(d, SampleDecision)
        assert d.then == ProceedDecision()
        assert d.plan.query_text == "sample"
        assert d.method is SampleMethod.RANDOM
        assert d.error_bound == SAMPLE_MAX_ERROR
        assert d.confidence == 1 - SAMPLE_DELTA

    def test_T12_clarify_options_in_cost_order(self) -> None:
        d = _t(15_000, sampling=Sampling(None), refinements=Refinements(4_000, 700))
        assert isinstance(d, ClarifyDecision)
        assert _kinds(d) == [("REFINEMENT", 700), ("REFINEMENT", 4_000)]

    def test_T13_no_option_rejects(self) -> None:
        d = _t(15_000, sampling=Sampling(None), refinements=Refinements())
        assert isinstance(d, RejectDecision)
        assert "admits no allowed or offerable reduction" in d.reason

    def test_T14_equal_to_hard_ceiling_is_still_over(self) -> None:
        d = _t(20_000, sampling=Sampling(None), refinements=Refinements(700))
        assert isinstance(d, ClarifyDecision) and len(d.options) == 1

    def test_T15_hard_ceiling_precedes_everything(self) -> None:
        calls: list[str] = []

        class Spy(Rewrites):
            def candidates(self, p: ExportPlan) -> Sequence[RewriteCandidate]:
                calls.append("rewrite")
                return super().candidates(p)

        d = _t(
            20_001,
            rewrites=Spy((S.STRATEGY_SHORTEST, 50)),
            sampling=Sampling(40),
            refinements=Refinements(700, 900),
        )
        assert isinstance(d, RejectDecision)
        assert "hard cost ceiling" in d.reason
        assert calls == []  # nothing else is evaluated

    def test_T16_rewrite_not_allowed_delivers_exact_plan(self) -> None:
        d = _t(5_000, NONE, rewrites=Rewrites((S.STRATEGY_SHORTEST, 80)))
        assert isinstance(d, AsyncDecision)

    def test_T17_unallowed_reductions_are_offered(self) -> None:
        d = _t(
            15_000,
            NONE,
            rewrites=Rewrites((S.CONE_SCOPE, 900)),
            sampling=Sampling(40),
            refinements=Refinements(700),
        )
        assert isinstance(d, ClarifyDecision)
        assert _kinds(d) == [("SAMPLE", 40), ("REFINEMENT", 700), ("CONE_SCOPE", 900)]
        sample = d.options[0]
        assert sample.kind is ClarifyKind.SUBSTITUTION and sample.warning

    def test_T18_no_option_rejects_under_default(self) -> None:
        d = _t(15_000, NONE, sampling=Sampling(None), refinements=Refinements())
        assert isinstance(d, RejectDecision)

    @pytest.mark.parametrize("allowed", [False, True])
    def test_T19_slow_precomputation_approximation(self, allowed: bool) -> None:
        subs = frozenset({S.APPROX_ALGORITHM}) if allowed else NONE
        d = _t(50, subs, precompute=Precompute(approx=60), pcs=1)
        if allowed:
            assert isinstance(d, RewriteDecision)
            assert d.substitution is S.APPROX_ALGORITHM
            assert d.then == ProceedDecision()
        else:
            assert isinstance(d, ClarifyDecision)
            assert _kinds(d) == [("APPROX_ALGORITHM", 60)]

    @pytest.mark.parametrize("subs", [NONE, ALL])
    def test_T20_slow_precomputation_without_approximation_rejects(
        self, subs: frozenset[Substitution]
    ) -> None:
        d = _t(50, subs, precompute=Precompute(approx=None), pcs=1)
        assert isinstance(d, RejectDecision)
        assert "max_precompute_time" in d.reason


# ---------------------------------------------------------------------------
# OVER zone (the #150 bug) and unknowns
# ---------------------------------------------------------------------------


class TestOverZoneNeverExecutes:
    @pytest.mark.parametrize("est", [10_001, 15_000, 20_000])
    @pytest.mark.parametrize("subs", [NONE, ALL])
    def test_over_zone_without_providers_rejects(
        self, est: float, subs: frozenset[Substitution]
    ) -> None:
        d = gate().gate(plan(est), budget(subs))
        assert isinstance(d, RejectDecision)

    def test_over_zone_never_delivers_the_submitted_plan(self) -> None:
        g = gate(refinements=Refinements(700), sampling=Sampling(None))
        d = g.gate(plan(15_000, name="submitted"), budget())
        assert isinstance(d, ClarifyDecision)
        assert all(o.plan.query_text != "submitted" for o in d.options)


class TestUnknownsAreNotAdmitted:
    def test_no_cost_estimate_is_rejected(self) -> None:
        d = gate().gate(plan(None), budget())
        assert isinstance(d, RejectDecision) and "no cost estimate" in d.reason

    def test_precomputation_without_time_estimator_is_rejected(self) -> None:
        d = gate().gate(plan(50, pcs=1), budget())
        assert isinstance(d, RejectDecision)

    def test_precomputation_with_unknown_time_is_rejected(self) -> None:
        d = gate(precompute=Precompute(known=False)).gate(plan(50, pcs=1), budget())
        assert isinstance(d, RejectDecision) and "no time estimate" in d.reason

    def test_fast_precomputation_passes(self) -> None:
        d = gate(precompute=Precompute(slow=False)).gate(plan(50, pcs=1), budget())
        assert d == ProceedDecision()

    def test_unknown_rows_paginates_the_exact_result(self) -> None:
        assert gate().gate(plan(50, rows=None), budget()) == PaginateDecision(1_000)

    def test_deferred_without_async_target_is_an_error(self) -> None:
        with pytest.raises(AsyncNotConfigured):
            ExecutionGate().gate(plan(5_000), budget())

    def test_async_decision_has_no_default_sink(self) -> None:
        with pytest.raises(AsyncNotConfigured):
            AsyncDecision(sink=None, callback="", ttl=timedelta(hours=1))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Rewrite precedence and CLARIFY ordering
# ---------------------------------------------------------------------------


class TestPrecedence:
    def test_best_rewrite_lowest_zone_then_rule_order(self) -> None:
        d = _t(
            5_000,
            rewrites=Rewrites(
                (S.CONE_SCOPE, 50), (S.DEPTH_LIMIT, 90), (S.STRATEGY_SHORTEST, 500)
            ),
        )
        assert isinstance(d, RewriteDecision)
        assert d.substitution is S.DEPTH_LIMIT  # SYNC beats STREAMING; rule order

    def test_only_allowed_substitutions_are_applied(self) -> None:
        d = _t(
            5_000,
            frozenset({S.CONE_SCOPE}),
            rewrites=Rewrites((S.STRATEGY_SHORTEST, 50), (S.CONE_SCOPE, 600)),
        )
        assert isinstance(d, RewriteDecision) and d.substitution is S.CONE_SCOPE

    def test_sync_plan_is_never_rewritten(self) -> None:
        d = _t(50, rewrites=Rewrites((S.STRATEGY_SHORTEST, 10)))
        assert d == ProceedDecision()

    def test_non_executable_options_are_dropped_and_truncated(self) -> None:
        d = _t(
            15_000,
            NONE,
            sampling=Sampling(None),
            refinements=Refinements(12_000, 300, 200, 100, 50),
        )
        assert isinstance(d, ClarifyDecision)
        assert len(d.options) == MAX_CLARIFY_OPTIONS
        assert [o.cost.cpu_ops for o in d.options] == [50, 100, 200]

    def test_refinement_precedes_substitution_at_equal_cost(self) -> None:
        d = _t(
            15_000,
            NONE,
            rewrites=Rewrites((S.CONE_SCOPE, 700)),
            sampling=Sampling(None),
            refinements=Refinements(700),
        )
        assert isinstance(d, ClarifyDecision)
        assert [o.kind for o in d.options] == [
            ClarifyKind.REFINEMENT,
            ClarifyKind.SUBSTITUTION,
        ]


# ---------------------------------------------------------------------------
# Budgets and types
# ---------------------------------------------------------------------------


class TestBudgetInvariants:
    @pytest.mark.parametrize(
        "kw",
        [
            pytest.param(
                {"streaming_threshold": cost(200), "async_threshold": cost(100)},
                id="inverted",
            ),
            pytest.param({"streaming_threshold": cost(-1)}, id="negative"),
            pytest.param({"async_threshold": cost(20_000)}, id="above-ceiling"),
            pytest.param({"streaming_threshold": cost(float("nan"))}, id="nan"),
            pytest.param({"max_estimated_cost": cost(float("inf"))}, id="inf"),
        ],
    )
    def test_invalid_budgets_are_rejected_at_construction(
        self, kw: dict[str, Any]
    ) -> None:
        with pytest.raises(InvalidBudget):
            budget(**kw)

    def test_equal_streaming_and_async_thresholds_are_valid(self) -> None:
        b = budget(streaming_threshold=cost(100), async_threshold=cost(100))
        g = gate()
        assert g.gate(plan(100), b) == ProceedDecision()
        assert isinstance(g.gate(plan(101), b), AsyncDecision)

    def test_default_substitutions_are_empty(self) -> None:
        b = ExecutionBudget(
            max_estimated_cost=cost(10),
            max_result_rows=1,
            max_wall_time=timedelta(1),
            max_precompute_time=timedelta(1),
            streaming_threshold=cost(1),
            async_threshold=cost(2),
        )
        assert b.substitutions == frozenset()

    def test_budget_is_immutable(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            budget().substitutions = ALL  # type: ignore[misc]

    def test_substitutions_must_be_a_frozenset(self) -> None:
        with pytest.raises(InvalidBudget):
            budget({S.SAMPLE})  # type: ignore[arg-type]

    def test_hard_ceiling(self) -> None:
        assert budget().hard_ceiling == 10_000 * HARD_CEILING_FACTOR

    @pytest.mark.parametrize(
        "est,expected",
        [
            (0, Zone.SYNC),
            (100, Zone.SYNC),
            (101, Zone.STREAMING),
            (1_000, Zone.STREAMING),
            (1_001, Zone.DEFERRED),
            (10_000, Zone.DEFERRED),
            (10_001, Zone.OVER),
            (20_000, Zone.OVER),
            (20_001, Zone.FORBIDDEN),
        ],
    )
    def test_zone_is_inclusive_on_the_cheaper_side(
        self, est: float, expected: Zone
    ) -> None:
        assert zone(cost(est), budget()) is expected


class TestDecisionTypes:
    def test_eight_kinds(self) -> None:
        assert len(BudgetDecisionKind) == 8

    def test_rewrite_then_must_be_a_delivery(self) -> None:
        with pytest.raises(TypeError):
            RewriteDecision(
                plan=plan(1),
                substitution=S.CONE_SCOPE,
                warning="w",
                then=RejectDecision(reason="x"),  # type: ignore[arg-type]
            )

    def test_clarify_needs_an_option(self) -> None:
        with pytest.raises(ValueError):
            ClarifyDecision(options=())

    @pytest.mark.parametrize(
        "kind,sub,warning",
        [
            (ClarifyKind.REFINEMENT, S.SAMPLE, None),
            (ClarifyKind.REFINEMENT, None, "w"),
            (ClarifyKind.SUBSTITUTION, None, "w"),
            (ClarifyKind.SUBSTITUTION, S.SAMPLE, None),
        ],
    )
    def test_clarify_option_warning_iff_substitution(
        self, kind: ClarifyKind, sub: Substitution | None, warning: str | None
    ) -> None:
        with pytest.raises(ValueError):
            ClarifyOption(plan(1), cost(1), kind, sub, warning)

    def test_sample_fraction_in_open_unit_interval(self) -> None:
        with pytest.raises(ValueError):
            SampleDecision(
                plan=plan(1),
                fraction=1.0,
                method=SampleMethod.RANDOM,
                error_bound=0.05,
                confidence=0.95,
                then=ProceedDecision(),
            )


class TestChildBudgets:
    def test_child_budget_inherits_substitutions_unchanged(self) -> None:
        from datetime import datetime, timezone

        from sqldim.core.query.dgm.nl.supervisor._dispatch import child_budget

        parent = budget(frozenset({S.SAMPLE}))
        child = child_budget(
            parent, 0.5, datetime.now(timezone.utc) + timedelta(minutes=1)
        )
        assert child.substitutions == parent.substitutions
        assert child.max_estimated_cost.cpu_ops == 5_000
