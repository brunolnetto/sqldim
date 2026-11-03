"""Per-tool eval suite — each of 8 non-query supervisor tools tested across
all 14 domains.

Tests are driven by ``tool_evals.json`` files discovered in each domain's
``artifacts/`` folder.  Each eval case defines:

- The tool to invoke
- Input arguments
- Expected output (success, tool_name, data keys, data checks)

Four test classes:

- **TestToolExecution**:  Direct ``tool.execute(arguments, context)`` →
  verify success, tool_name, data shape, and domain-specific assertions.
- **TestToolContract**: Every tool satisfies the 5-point contract
  (explain, cost_estimate).
- **TestToolComposition**: Composable pairs compose cleanly.
- **TestToolCoverage**: Every registered tool appears in at least one case.
"""

from __future__ import annotations

from typing import Any

import pytest

from sqldim.application.evals.tool_eval_loader import (
    ToolEvalCase,
    load_all_tool_eval_cases,
)
from sqldim.core.query.dgm.nl.supervisor._tools import TOOL_REGISTRY
from sqldim.core.query.dgm.nl.supervisor._types import (
    SupervisorContext,
    ToolCallResult,
)

# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

ALL_CASES: list[ToolEvalCase] = load_all_tool_eval_cases()
_ALL_DATASETS = sorted({c.domain for c in ALL_CASES})
_CTX = SupervisorContext(available_datasets=_ALL_DATASETS)


def _case_id(c: ToolEvalCase) -> str:
    return c.id


def _check_data(data: Any, checks: dict[str, Any]) -> None:
    """Apply ``data_checks`` assertions on tool output data."""
    if not checks or not isinstance(data, dict):
        return

    if "datasets_includes" in checks:
        datasets = data.get("datasets", [])
        for expected_ds in checks["datasets_includes"]:
            assert expected_ds in datasets, (
                f"Expected '{expected_ds}' in datasets {datasets}"
            )

    if "stored_equals" in checks:
        assert data.get("stored") == checks["stored_equals"], (
            f"Expected stored={checks['stored_equals']}, got {data.get('stored')}"
        )

    if "tools_non_empty" in checks and checks["tools_non_empty"]:
        tools = data.get("tools", [])
        assert len(tools) > 0, "Expected non-empty tools list"


# ---------------------------------------------------------------------------
# Composition pairs — computed at import time
# ---------------------------------------------------------------------------


def _build_composable_pairs() -> list[tuple[ToolEvalCase, ToolEvalCase]]:
    """Find (left, right) pairs where left declares right in composable_with
    and both appear in tool_evals (same domain)."""
    by_domain_tool: dict[tuple[str, str], ToolEvalCase] = {}
    for c in ALL_CASES:
        by_domain_tool[(c.domain, c.tool)] = c

    pairs: list[tuple[ToolEvalCase, ToolEvalCase]] = []
    for c in ALL_CASES:
        schema = TOOL_REGISTRY[c.tool].capability_schema
        for partner in schema.composable_with:
            key = (c.domain, partner)
            if key in by_domain_tool:
                pairs.append((c, by_domain_tool[key]))
    return pairs


COMPOSABLE_PAIRS = _build_composable_pairs()


def _pair_id(pair: tuple[ToolEvalCase, ToolEvalCase]) -> str:
    short_r = pair[1].tool.replace("dgm_", "")
    return f"{pair[0].id}>{short_r}"


# ===================================================================
# Tests
# ===================================================================


class TestToolExecution:
    """Each tool eval case executes correctly."""

    @pytest.mark.parametrize("case", ALL_CASES, ids=_case_id)
    def test_execute_succeeds(self, case: ToolEvalCase) -> None:
        tool = TOOL_REGISTRY[case.tool]
        result = tool.execute(case.arguments, context=_CTX)
        assert isinstance(result, ToolCallResult)
        if case.expected_success:
            assert result.success, (
                f"[{case.id}] Expected success=True, got error: {result.error}"
            )

    @pytest.mark.parametrize("case", ALL_CASES, ids=_case_id)
    def test_tool_name_matches(self, case: ToolEvalCase) -> None:
        tool = TOOL_REGISTRY[case.tool]
        result = tool.execute(case.arguments, context=_CTX)
        assert result.tool_name == case.expected_tool_name, (
            f"[{case.id}] Expected tool_name='{case.expected_tool_name}', "
            f"got '{result.tool_name}'"
        )

    @pytest.mark.parametrize("case", ALL_CASES, ids=_case_id)
    def test_data_keys_present(self, case: ToolEvalCase) -> None:
        if not case.expected_data_keys:
            return
        tool = TOOL_REGISTRY[case.tool]
        result = tool.execute(case.arguments, context=_CTX)
        assert isinstance(result.data, dict), (
            f"[{case.id}] Expected dict data, got {type(result.data)}"
        )
        for key in case.expected_data_keys:
            assert key in result.data, (
                f"[{case.id}] Missing key '{key}' in data: {list(result.data.keys())}"
            )

    @pytest.mark.parametrize("case", ALL_CASES, ids=_case_id)
    def test_data_checks(self, case: ToolEvalCase) -> None:
        if not case.expected_data_checks:
            return
        tool = TOOL_REGISTRY[case.tool]
        result = tool.execute(case.arguments, context=_CTX)
        _check_data(result.data, case.expected_data_checks)


class TestToolContract:
    """Every tool satisfies the 5-point contract: explain, cost_estimate."""

    @pytest.mark.parametrize("case", ALL_CASES, ids=_case_id)
    def test_explain_returns_string(self, case: ToolEvalCase) -> None:
        tool = TOOL_REGISTRY[case.tool]
        explanation = tool.explain(case.arguments)
        assert isinstance(explanation, str) and len(explanation) > 0

    @pytest.mark.parametrize("case", ALL_CASES, ids=_case_id)
    def test_cost_estimate_non_negative(self, case: ToolEvalCase) -> None:
        tool = TOOL_REGISTRY[case.tool]
        cost = tool.cost_estimate(case.arguments)
        assert cost >= 0


class TestToolComposition:
    """Composable pairs compose cleanly within the same domain."""

    @pytest.mark.parametrize("pair", COMPOSABLE_PAIRS, ids=_pair_id)
    def test_compose_produces_result(
        self, pair: tuple[ToolEvalCase, ToolEvalCase]
    ) -> None:
        left_case, right_case = pair
        left_tool = TOOL_REGISTRY[left_case.tool]
        lr = left_tool.execute(left_case.arguments, context=_CTX)
        rr = TOOL_REGISTRY[right_case.tool].execute(right_case.arguments, context=_CTX)
        composed = left_tool.compose(lr, rr)
        assert isinstance(composed, ToolCallResult)
        assert composed.success


class TestToolCoverage:
    """Structural assertions — every registered tool has eval cases, and
    all 14 domains are represented."""

    def test_all_non_query_tools_have_eval_cases(self) -> None:
        tools_with_cases = {c.tool for c in ALL_CASES}
        # dgm_query is covered by the existing per-domain evals.json
        expected = set(TOOL_REGISTRY.keys()) - {"dgm_query"}
        missing = expected - tools_with_cases
        assert not missing, f"Tools without eval cases: {sorted(missing)}"

    def test_all_domains_represented(self) -> None:
        domains_with_cases = {c.domain for c in ALL_CASES}
        expected_domains = {
            "commerce",
            "devops",
            "data_models",
            "growth",
            "fintech",
            "entertainment",
            "nba_analytics",
            "telemetry",
        }
        missing = expected_domains - domains_with_cases
        assert not missing, f"Domains without tool eval cases: {sorted(missing)}"


class TestToolEvalCaseLoader:
    """Unit tests for the tool_eval_loader module."""

    def test_load_tool_eval_cases_returns_empty_for_missing_domain(self) -> None:
        from sqldim.application.evals.tool_eval_loader import load_tool_eval_cases

        cases = load_tool_eval_cases("nonexistent_domain_xyz")
        assert cases == []

    def test_load_tool_eval_cases_returns_cases_for_commerce(self) -> None:
        from sqldim.application.evals.tool_eval_loader import load_tool_eval_cases

        cases = load_tool_eval_cases("commerce")
        assert len(cases) > 0
        assert all(c.domain == "commerce" for c in cases)
