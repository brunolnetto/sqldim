"""RED → GREEN tests for §6.3 — ExecutionBudget and pre-execution gate.

Covers:
  - BudgetDecision variant types (PROCEED, STREAM, ASYNC, REWRITE, SAMPLE,
    PAGINATE, CLARIFY, REJECT)
  - ExecutionBudget dataclass
  - ExecutionGate.gate() decision procedure
  - Hard-ceiling REJECT
  - Async-threshold ASYNC
  - Streaming-threshold STREAM
  - Cost-rewrite REWRITE (viable rewrite candidates)
  - Result-size PAGINATE
  - PROCEED for within-budget plans
  - HARD_CEILING_FACTOR constant
"""

from __future__ import annotations

import math
from datetime import timedelta


from sqldim.core.query.dgm.planner import (
    CostEstimate,
    ExportPlan,
    QueryTarget,
    SinkTarget,
)
from sqldim.core.query.dgm.planner._gate import (
    HARD_CEILING_FACTOR,
    AsyncDecision,
    BudgetDecision,
    BudgetDecisionKind,
    ClarifyDecision,
    ExecutionBudget,
    ExecutionGate,
    PaginateDecision,
    ProceedDecision,
    RejectDecision,
    RewriteDecision,
    SampleDecision,
    SampleMethod,
    StreamDecision,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cost(cpu: int, io: int = 0) -> CostEstimate:
    return CostEstimate(cpu_ops=cpu, io_ops=io)


def _plan(cost: CostEstimate | None = None, estimated_rows: int = 100) -> ExportPlan:
    p = ExportPlan(
        query_target=QueryTarget.SQL_DUCKDB,
        query_text="SELECT 1",
        cost_estimate=cost,
    )
    p.estimated_rows = estimated_rows  # type: ignore[attr-defined]
    return p


def _budget(
    max_cost_cpu: int = 1_000,
    max_rows: int = 10_000,
    streaming_cpu: int = 500,
    async_cpu: int = 800,
) -> ExecutionBudget:
    return ExecutionBudget(
        max_estimated_cost=_cost(max_cost_cpu),
        max_result_rows=max_rows,
        max_wall_time=timedelta(seconds=60),
        max_precompute_time=timedelta(seconds=10),
        streaming_threshold=_cost(streaming_cpu),
        async_threshold=_cost(async_cpu),
    )


# ---------------------------------------------------------------------------
# BudgetDecisionKind enum
# ---------------------------------------------------------------------------


class TestBudgetDecisionKind:
    def test_has_proceed(self):
        assert BudgetDecisionKind.PROCEED.value == "PROCEED"

    def test_has_stream(self):
        assert BudgetDecisionKind.STREAM.value == "STREAM"

    def test_has_async(self):
        assert BudgetDecisionKind.ASYNC.value == "ASYNC"

    def test_has_rewrite(self):
        assert BudgetDecisionKind.REWRITE.value == "REWRITE"

    def test_has_sample(self):
        assert BudgetDecisionKind.SAMPLE.value == "SAMPLE"

    def test_has_paginate(self):
        assert BudgetDecisionKind.PAGINATE.value == "PAGINATE"

    def test_has_clarify(self):
        assert BudgetDecisionKind.CLARIFY.value == "CLARIFY"

    def test_has_reject(self):
        assert BudgetDecisionKind.REJECT.value == "REJECT"

    def test_all_eight_kinds(self):
        assert len(BudgetDecisionKind) == 8


# ---------------------------------------------------------------------------
# SampleMethod enum
# ---------------------------------------------------------------------------


class TestSampleMethod:
    def test_has_random(self):
        assert SampleMethod.RANDOM.value == "RANDOM"

    def test_has_stratified(self):
        assert SampleMethod.STRATIFIED.value == "STRATIFIED"

    def test_has_reservoir(self):
        assert SampleMethod.RESERVOIR.value == "RESERVOIR"

    def test_all_three_methods(self):
        assert len(SampleMethod) == 3


# ---------------------------------------------------------------------------
# ExecutionBudget dataclass
# ---------------------------------------------------------------------------


class TestExecutionBudget:
    def test_stores_max_cost(self):
        b = _budget(max_cost_cpu=500)
        assert b.max_estimated_cost.cpu_ops == 500

    def test_stores_max_rows(self):
        b = _budget(max_rows=5_000)
        assert b.max_result_rows == 5_000

    def test_stores_wall_time(self):
        b = _budget()
        assert b.max_wall_time == timedelta(seconds=60)

    def test_stores_streaming_threshold(self):
        b = _budget(streaming_cpu=300)
        assert b.streaming_threshold.cpu_ops == 300

    def test_stores_async_threshold(self):
        b = _budget(async_cpu=700)
        assert b.async_threshold.cpu_ops == 700

    def test_inequality_streaming_lt_async(self):
        b = _budget(streaming_cpu=300, async_cpu=600)
        assert b.streaming_threshold.cpu_ops < b.async_threshold.cpu_ops


# ---------------------------------------------------------------------------
# BudgetDecision variants
# ---------------------------------------------------------------------------


class TestProceedDecision:
    def test_kind(self):
        d = ProceedDecision()
        assert d.kind is BudgetDecisionKind.PROCEED

    def test_is_budget_decision(self):
        assert isinstance(ProceedDecision(), BudgetDecision)


class TestStreamDecision:
    def test_kind(self):
        d = StreamDecision()
        assert d.kind is BudgetDecisionKind.STREAM


class TestAsyncDecision:
    def test_kind(self):
        d = AsyncDecision(
            sink=SinkTarget.PARQUET, callback="http://cb", ttl=timedelta(hours=24)
        )
        assert d.kind is BudgetDecisionKind.ASYNC

    def test_stores_sink(self):
        d = AsyncDecision(sink=SinkTarget.DUCKDB, callback="cb", ttl=timedelta(hours=1))
        assert d.sink is SinkTarget.DUCKDB

    def test_stores_callback(self):
        d = AsyncDecision(
            sink=SinkTarget.DUCKDB, callback="http://notify.me", ttl=timedelta(hours=1)
        )
        assert d.callback == "http://notify.me"

    def test_stores_ttl(self):
        ttl = timedelta(hours=6)
        d = AsyncDecision(sink=SinkTarget.DUCKDB, callback="cb", ttl=ttl)
        assert d.ttl == ttl


class TestRewriteDecision:
    def test_kind(self):
        p = _plan(_cost(50))
        d = RewriteDecision(plan=p, warning="shorter paths only")
        assert d.kind is BudgetDecisionKind.REWRITE

    def test_stores_warning(self):
        p = _plan(_cost(50))
        d = RewriteDecision(plan=p, warning="approximated")
        assert d.warning == "approximated"

    def test_stores_plan(self):
        p = _plan(_cost(50))
        d = RewriteDecision(plan=p, warning="w")
        assert d.plan is p


class TestSampleDecision:
    def test_kind(self):
        d = SampleDecision(
            fraction=0.1, method=SampleMethod.STRATIFIED, error_bound=0.03
        )
        assert d.kind is BudgetDecisionKind.SAMPLE

    def test_fraction_range(self):
        d = SampleDecision(fraction=0.5, method=SampleMethod.RANDOM, error_bound=0.02)
        assert 0.0 < d.fraction <= 1.0

    def test_hoeffding_error_bound(self):
        # error_bound = sqrt(log(2/δ) / (2 * f * N))
        f, n, delta = 0.1, 10_000, 0.05
        expected = math.sqrt(math.log(2 / delta) / (2 * f * n))
        d = SampleDecision(
            fraction=f, method=SampleMethod.STRATIFIED, error_bound=expected
        )
        assert abs(d.error_bound - expected) < 1e-10


class TestPaginateDecision:
    def test_kind(self):
        d = PaginateDecision(page_size=1000)
        assert d.kind is BudgetDecisionKind.PAGINATE

    def test_stores_page_size(self):
        d = PaginateDecision(page_size=500)
        assert d.page_size == 500


class TestClarifyDecision:
    def test_kind(self):
        p1, p2 = _plan(_cost(10)), _plan(_cost(20))
        d = ClarifyDecision(options=[p1, p2], cost_per_option=[_cost(10), _cost(20)])
        assert d.kind is BudgetDecisionKind.CLARIFY

    def test_stores_options(self):
        p1, p2 = _plan(_cost(10)), _plan(_cost(20))
        d = ClarifyDecision(options=[p1, p2], cost_per_option=[_cost(10), _cost(20)])
        assert len(d.options) == 2

    def test_options_and_costs_same_length(self):
        p1 = _plan(_cost(10))
        d = ClarifyDecision(options=[p1], cost_per_option=[_cost(10)])
        assert len(d.options) == len(d.cost_per_option)


class TestRejectDecision:
    def test_kind(self):
        d = RejectDecision(reason="too expensive")
        assert d.kind is BudgetDecisionKind.REJECT

    def test_stores_reason(self):
        d = RejectDecision(reason="exceeds hard ceiling")
        assert "ceiling" in d.reason


# ---------------------------------------------------------------------------
# HARD_CEILING_FACTOR constant
# ---------------------------------------------------------------------------


class TestHardCeilingFactor:
    def test_is_greater_than_one(self):
        assert HARD_CEILING_FACTOR > 1.0

    def test_is_float(self):
        assert isinstance(HARD_CEILING_FACTOR, float)


# ---------------------------------------------------------------------------
# ExecutionGate.gate() — decision procedure
# ---------------------------------------------------------------------------


class TestExecutionGateGate:
    """Tests for the core decision procedure in §6.3."""

    def test_no_cost_estimate_proceeds(self):
        plan = _plan(cost=None)
        gate = ExecutionGate()
        decision = gate.gate(plan, _budget())
        assert isinstance(decision, ProceedDecision)

    def test_within_budget_proceeds(self):
        plan = _plan(_cost(100))
        gate = ExecutionGate()
        decision = gate.gate(plan, _budget(max_cost_cpu=1_000, streaming_cpu=500))
        assert isinstance(decision, ProceedDecision)

    def test_above_hard_ceiling_rejects(self):
        # cpu_ops > max_estimated_cost * HARD_CEILING_FACTOR → REJECT
        budget = _budget(max_cost_cpu=1_000)
        plan = _plan(_cost(int(1_000 * HARD_CEILING_FACTOR) + 1))
        gate = ExecutionGate()
        decision = gate.gate(plan, budget)
        assert isinstance(decision, RejectDecision)
        assert decision.reason  # non-empty reason

    def test_above_async_threshold_returns_async(self):
        budget = _budget(max_cost_cpu=1_000, async_cpu=800)
        plan = _plan(_cost(850))  # above async, below hard ceiling
        gate = ExecutionGate()
        decision = gate.gate(plan, budget)
        assert isinstance(decision, AsyncDecision)

    def test_above_streaming_threshold_returns_stream(self):
        budget = _budget(max_cost_cpu=1_000, streaming_cpu=500, async_cpu=800)
        plan = _plan(_cost(600))  # above streaming, below async
        gate = ExecutionGate()
        decision = gate.gate(plan, budget)
        assert isinstance(decision, StreamDecision)

    def test_exceeds_max_rows_paginates(self):
        budget = _budget(max_rows=500, streaming_cpu=500)
        plan = _plan(_cost(100), estimated_rows=1_000)
        gate = ExecutionGate()
        decision = gate.gate(plan, budget)
        assert isinstance(decision, PaginateDecision)

    def test_paginate_page_size_positive(self):
        budget = _budget(max_rows=100)
        plan = _plan(_cost(50), estimated_rows=5_000)
        gate = ExecutionGate()
        decision = gate.gate(plan, budget)
        assert isinstance(decision, PaginateDecision)
        assert decision.page_size > 0

    def test_reject_takes_priority_over_async(self):
        # above hard ceiling — must REJECT even if above async threshold too
        budget = _budget(max_cost_cpu=100, async_cpu=50)
        plan = _plan(_cost(int(100 * HARD_CEILING_FACTOR) + 1))
        gate = ExecutionGate()
        decision = gate.gate(plan, budget)
        assert isinstance(decision, RejectDecision)

    def test_async_takes_priority_over_stream(self):
        # above both async and streaming — must ASYNC
        budget = _budget(max_cost_cpu=2_000, streaming_cpu=300, async_cpu=600)
        plan = _plan(_cost(700))
        gate = ExecutionGate()
        decision = gate.gate(plan, budget)
        assert isinstance(decision, AsyncDecision)

    def test_proceed_for_exact_budget(self):
        # exactly at max_estimated_cost (not above) — PROCEED
        budget = _budget(max_cost_cpu=1_000, streaming_cpu=500, async_cpu=800)
        plan = _plan(_cost(1_000))  # == max, not above hard ceiling
        gate = ExecutionGate()
        decision = gate.gate(plan, budget)
        # May be PROCEED, STREAM, or ASYNC depending on thresholds — not REJECT
        assert not isinstance(decision, RejectDecision)


# ---------------------------------------------------------------------------
# ExecutionGate — cost comparison helper
# ---------------------------------------------------------------------------


class TestCostComparison:
    def test_cost_exceeds_other_by_cpu(self):
        gate = ExecutionGate()
        big = _cost(1_000)
        small = _cost(500)
        assert gate._cost_exceeds(big, small)

    def test_cost_does_not_exceed_equal(self):
        gate = ExecutionGate()
        c = _cost(500)
        assert not gate._cost_exceeds(c, c)

    def test_cost_does_not_exceed_smaller(self):
        gate = ExecutionGate()
        assert not gate._cost_exceeds(_cost(300), _cost(500))

    def test_cost_total_is_cpu_plus_io(self):
        gate = ExecutionGate()
        assert gate._cost_total(_cost(100, 200)) == 300
