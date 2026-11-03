"""Tool synergy eval suite — systematic 2^N combination testing.

Evaluates all 2^9 − 1 = 511 non-empty subsets of the 9 DGM tools:
  0. Composability graph structural validity
  1. Single-tool execution correctness (9 tools × 3 checks)
  2. Pairwise composition for 12 declared composable_with pairs
  3. Power-set execution: all 511 subsets execute cleanly
  4. Power-set composition: 502 multi-tool subsets compose sequentially
  5. Power-set graph: all 511 subsets run through the supervisor graph
"""

from __future__ import annotations

import uuid
from itertools import combinations
from typing import Any
from unittest.mock import patch

import pytest

from sqldim.core.query.dgm.nl.supervisor._tools import TOOL_REGISTRY
from sqldim.core.query.dgm.nl.supervisor._types import (
    SupervisorContext,
    SupervisorPlan,
    SupervisorState,
    ToolCallRequest,
    ToolCallResult,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALL_TOOL_NAMES: list[str] = sorted(TOOL_REGISTRY.keys())

MINIMAL_ARGS: dict[str, dict[str, Any]] = {
    "dgm_correlate": {"left": {}, "right": {}},
    "dgm_explore_from": {"result": {}, "direction": "forward"},
    "dgm_map_analytical_space": {"dataset": "test_ds"},
    "dgm_query": {"dataset": "test_ds", "question": "test question"},
    "dgm_retrieve_related": {"query": "test", "max_results": 5},
    "dgm_sensitivity_sweep": {"dataset": "test_ds", "question": "q", "param": "x"},
    "dgm_store_finding": {"finding": "test finding", "tags": ["test"]},
    "dgm_validate_invariant": {"invariant": "inv", "dataset": "test_ds"},
    "dgm_what_can_i_answer": {"dataset": "__all__"},
}

_CTX = SupervisorContext(available_datasets=["test_ds", "social_network"])


def _power_set(items: list[str]) -> list[tuple[str, ...]]:
    """All non-empty subsets, ordered by cardinality then lexicographically."""
    result: list[tuple[str, ...]] = []
    for r in range(1, len(items) + 1):
        for combo in combinations(items, r):
            result.append(combo)
    return result


ALL_SUBSETS: list[tuple[str, ...]] = _power_set(ALL_TOOL_NAMES)
MULTI_SUBSETS: list[tuple[str, ...]] = [c for c in ALL_SUBSETS if len(c) >= 2]

# Directed composable pairs (A declares B in composable_with)
COMPOSABLE_PAIRS: list[tuple[str, str]] = sorted(
    {
        (name, partner)
        for name, tool in TOOL_REGISTRY.items()
        for partner in tool.capability_schema.composable_with
    }
)


def _short(name: str) -> str:
    return name.replace("dgm_", "")


def _subset_id(combo: tuple[str, ...]) -> str:
    return "+".join(_short(n) for n in combo)


def _pair_id(pair: tuple[str, str]) -> str:
    return f"{_short(pair[0])}>{_short(pair[1])}"


def _make_plan(combo: tuple[str, ...]) -> SupervisorPlan:
    """Build a plan with equal budget per tool, confidence above reflect threshold."""
    n = len(combo)
    return SupervisorPlan(
        goal="synergy eval",
        steps=[
            ToolCallRequest(
                tool_name=name,
                arguments=MINIMAL_ARGS[name],
                budget_fraction=1.0 / n,
            )
            for name in combo
        ],
        confidence=0.8,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def supervisor_graph() -> Any:
    """Module-scoped compiled graph (reused across all 511 invocations)."""
    from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph

    return build_supervisor_graph(context=_CTX)


# ---------------------------------------------------------------------------
# 0. Composability graph structural validity
# ---------------------------------------------------------------------------


class TestComposabilityGraph:
    """The composable_with declarations form a valid directed graph."""

    def test_all_references_are_registered_tools(self) -> None:
        for name, tool in TOOL_REGISTRY.items():
            for partner in tool.capability_schema.composable_with:
                assert partner in TOOL_REGISTRY, (
                    f"{name} declares composable_with '{partner}' "
                    "which is not registered"
                )

    def test_graph_has_edges(self) -> None:
        total = sum(
            len(t.capability_schema.composable_with) for t in TOOL_REGISTRY.values()
        )
        assert total == len(COMPOSABLE_PAIRS)
        assert total > 0, "composability graph has no edges"

    def test_minimal_args_cover_all_tools(self) -> None:
        assert set(MINIMAL_ARGS.keys()) == set(ALL_TOOL_NAMES)


# ---------------------------------------------------------------------------
# 1. Single-tool execution
# ---------------------------------------------------------------------------


class TestSingleToolExecution:
    """Each of the 9 tools executes independently with minimal arguments."""

    @pytest.mark.parametrize(
        "tool_name",
        ALL_TOOL_NAMES,
        ids=[_short(n) for n in ALL_TOOL_NAMES],
    )
    def test_execute_success(self, tool_name: str) -> None:
        r = TOOL_REGISTRY[tool_name].execute(
            MINIMAL_ARGS[tool_name],
            context=_CTX,
        )
        assert isinstance(r, ToolCallResult)
        assert r.success is True
        assert r.tool_name == tool_name

    @pytest.mark.parametrize(
        "tool_name",
        ALL_TOOL_NAMES,
        ids=[_short(n) for n in ALL_TOOL_NAMES],
    )
    def test_cost_estimate_non_negative(self, tool_name: str) -> None:
        assert TOOL_REGISTRY[tool_name].cost_estimate(MINIMAL_ARGS[tool_name]) >= 0

    @pytest.mark.parametrize(
        "tool_name",
        ALL_TOOL_NAMES,
        ids=[_short(n) for n in ALL_TOOL_NAMES],
    )
    def test_explain_non_empty(self, tool_name: str) -> None:
        text = TOOL_REGISTRY[tool_name].explain(MINIMAL_ARGS[tool_name])
        assert isinstance(text, str) and len(text) > 0


# ---------------------------------------------------------------------------
# 2. Declared compose pairs
# ---------------------------------------------------------------------------


class TestDeclaredComposePairs:
    """All 12 directed composable_with pairs compose cleanly."""

    @pytest.mark.parametrize(
        "pair",
        COMPOSABLE_PAIRS,
        ids=[_pair_id(p) for p in COMPOSABLE_PAIRS],
    )
    def test_compose_succeeds(self, pair: tuple[str, str]) -> None:
        a, b = pair
        ar = TOOL_REGISTRY[a].execute(MINIMAL_ARGS[a], context=_CTX)
        br = TOOL_REGISTRY[b].execute(MINIMAL_ARGS[b], context=_CTX)
        c = TOOL_REGISTRY[a].compose(ar, br)
        assert isinstance(c, ToolCallResult)
        assert c.success is True

    @pytest.mark.parametrize(
        "pair",
        COMPOSABLE_PAIRS,
        ids=[_pair_id(p) for p in COMPOSABLE_PAIRS],
    )
    def test_compose_names_both_tools(self, pair: tuple[str, str]) -> None:
        a, b = pair
        ar = TOOL_REGISTRY[a].execute(MINIMAL_ARGS[a], context=_CTX)
        br = TOOL_REGISTRY[b].execute(MINIMAL_ARGS[b], context=_CTX)
        c = TOOL_REGISTRY[a].compose(ar, br)
        assert a in c.tool_name and b in c.tool_name

    @pytest.mark.parametrize(
        "pair",
        COMPOSABLE_PAIRS,
        ids=[_pair_id(p) for p in COMPOSABLE_PAIRS],
    )
    def test_compose_cost_additive(self, pair: tuple[str, str]) -> None:
        a, b = pair
        ar = TOOL_REGISTRY[a].execute(MINIMAL_ARGS[a], context=_CTX)
        br = TOOL_REGISTRY[b].execute(MINIMAL_ARGS[b], context=_CTX)
        c = TOOL_REGISTRY[a].compose(ar, br)
        assert c.cost_actual == ar.cost_actual + br.cost_actual


# ---------------------------------------------------------------------------
# 3. Power-set: tool-level execution (511 combos)
# ---------------------------------------------------------------------------


class TestPowerSetExecution:
    """All 2^9 − 1 = 511 non-empty subsets execute without error."""

    @pytest.mark.parametrize(
        "combo",
        ALL_SUBSETS,
        ids=[_subset_id(c) for c in ALL_SUBSETS],
    )
    def test_all_tools_succeed(self, combo: tuple[str, ...]) -> None:
        for name in combo:
            r = TOOL_REGISTRY[name].execute(MINIMAL_ARGS[name], context=_CTX)
            assert r.success, f"{name} failed in {_subset_id(combo)}"


# ---------------------------------------------------------------------------
# 4. Power-set: sequential composition (502 multi-tool combos)
# ---------------------------------------------------------------------------


class TestPowerSetComposition:
    """All 502 multi-tool subsets compose sequentially without error."""

    @pytest.mark.parametrize(
        "combo",
        MULTI_SUBSETS,
        ids=[_subset_id(c) for c in MULTI_SUBSETS],
    )
    def test_compose_chain(self, combo: tuple[str, ...]) -> None:
        results = [
            TOOL_REGISTRY[n].execute(MINIMAL_ARGS[n], context=_CTX) for n in combo
        ]
        acc = results[0]
        composer = TOOL_REGISTRY[combo[0]]
        for r in results[1:]:
            acc = composer.compose(acc, r)
        assert isinstance(acc, ToolCallResult)
        assert acc.success is True


# ---------------------------------------------------------------------------
# 5. Power-set: full graph execution (511 combos)
# ---------------------------------------------------------------------------


class TestPowerSetGraphExecution:
    """All 511 subsets execute end-to-end through the supervisor graph."""

    @pytest.mark.parametrize(
        "combo",
        ALL_SUBSETS,
        ids=[_subset_id(c) for c in ALL_SUBSETS],
    )
    def test_graph_end_to_end(
        self,
        combo: tuple[str, ...],
        supervisor_graph: Any,
    ) -> None:
        plan = _make_plan(combo)
        with patch(
            "sqldim.core.query.dgm.nl.supervisor._planner.decompose_question",
            return_value=plan,
        ):
            result = supervisor_graph.invoke(
                SupervisorState(utterance="synergy eval").model_dump(),
                config={
                    "configurable": {"thread_id": uuid.uuid4().hex},
                    "recursion_limit": 50,
                },
            )

        # Graph produced synthesis
        assert result["synthesis"] is not None
        assert len(result["synthesis"]) > 0

        # Correct number of results
        results = result["results"]
        assert len(results) == len(combo), (
            f"expected {len(combo)} results, got {len(results)}"
        )

        # All results successful
        for r in results:
            success = r["success"] if isinstance(r, dict) else r.success
            tool = r["tool_name"] if isinstance(r, dict) else r.tool_name
            assert success, f"{tool} failed in graph execution"
