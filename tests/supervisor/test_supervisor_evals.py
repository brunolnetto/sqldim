"""Domain eval utterances through the supervisor (demo mode, D47).

The canonical eval registry (``EVAL_SUITE``) drives the keyword demo planner
and the §12.4 graph.  Graph-level cases use a scripted ``dgm_query`` so that
they test routing and reporting, not the heuristic NL graph; one integration
case runs the real demo tool end to end.
"""

from __future__ import annotations

import pytest

from sqldim.application.evals.cases import EVAL_SUITE, EvalCase
from sqldim.core.query.dgm.nl.supervisor._auth import Action
from sqldim.core.query.dgm.nl.supervisor._graph import run_supervisor
from sqldim.core.query.dgm.nl.supervisor._planner import (
    KeywordDemoPlanner,
    PlannerView,
)
from sqldim.core.query.dgm.nl.supervisor._tools import build_registry
from sqldim.core.query.dgm.nl.supervisor._types import CallStatus, Outcome
from tests.supervisor._fakes import FakeTool, auth, registry, runtime

_ALL_DATASETS = sorted({c.dataset for c in EVAL_SUITE})
_DEMO_TOOLS = tuple(
    t.capability_schema
    for t in build_registry(demo=True).values()
    if t.capability_schema.available
)


def _case_id(c: EvalCase) -> str:
    return c.id


def _plan(case: EvalCase):  # type: ignore[no-untyped-def]
    view = PlannerView(
        goal=case.utterance,
        plan_id="p",
        version=1,
        datasets=(case.dataset,),
        tools=_DEMO_TOOLS,
    )
    return KeywordDemoPlanner().propose(view)


def _first_per_dataset() -> list[EvalCase]:
    seen: set[str] = set()
    out: list[EvalCase] = []
    for c in EVAL_SUITE:
        if c.dataset not in seen:
            seen.add(c.dataset)
            out.append(c)
    return out


class TestPlannerOnDomainCases:
    @pytest.mark.parametrize("case", EVAL_SUITE, ids=_case_id)
    def test_routes_to_dgm_query_on_its_dataset(self, case: EvalCase) -> None:
        plan = _plan(case)
        assert [s.tool_name for s in plan.steps] == ["dgm_query"], plan.reasoning
        assert plan.steps[0].arguments["dataset"] == case.dataset

    @pytest.mark.parametrize("case", EVAL_SUITE, ids=_case_id)
    def test_confidence_and_budget(self, case: EvalCase) -> None:
        plan = _plan(case)
        assert plan.confidence >= 0.5
        assert plan.total_budget_fraction <= 1.0


class TestGraphOnDomainCases:
    @pytest.mark.parametrize("case", _first_per_dataset(), ids=_case_id)
    def test_complete_and_marked_demo(self, case: EvalCase) -> None:
        tool = FakeTool("dgm_query")
        rt = runtime(
            registry(tool),
            KeywordDemoPlanner(),
            the_auth=auth({case.dataset: {Action.READ}}),
            demo=True,
        )
        state = run_supervisor(case.utterance, rt, thread_id=case.id)
        assert state.outcome is Outcome.COMPLETE
        assert state.synthesis.startswith("[demo mode")
        assert case.dataset in state.synthesis
        assert tool.calls[0].datasets == [case.dataset]

    @pytest.mark.parametrize("dataset", _ALL_DATASETS)
    def test_ungranted_dataset_is_never_queried(self, dataset: str) -> None:
        others = [d for d in _ALL_DATASETS if d != dataset]
        tool = FakeTool("dgm_query")
        rt = runtime(
            registry(tool),
            KeywordDemoPlanner(),
            the_auth=auth({d: {Action.READ} for d in others}),
            demo=True,
        )
        case = next(c for c in EVAL_SUITE if c.dataset == dataset)
        state = run_supervisor(case.utterance, rt, thread_id=f"deny-{dataset}")
        assert all(dataset not in (c.datasets or []) for c in tool.calls)
        assert state.outcome in (Outcome.FAILED, Outcome.COMPLETE)


def test_real_demo_query_end_to_end() -> None:
    """The real demo dgm_query on a bundled dataset: demo-marked, never contract."""
    from sqldim.application.supervisor_host import local_supervisor_runtime

    rt = local_supervisor_runtime(["commerce"], demo=True)
    state = run_supervisor("how many orders are there", rt, thread_id="e2e")
    result = state.results["q1"]
    assert result.demo is True
    assert not result.is_contract_success
    if result.status is CallStatus.SUCCEEDED:
        assert state.outcome is Outcome.COMPLETE
        assert result.data["rows"] is not None
    else:
        assert result.error is not None
