"""Coverage tests for uncovered supervisor graph branches.

Targets:
- sqldim/core/query/dgm/nl/supervisor/_graph.py  lines 181, 183-194, 232-317, 432
  * _stub_synthesize: dgm_query with explanation (181) and query_result branches (183-194)
  * _llm_synthesize: full function body (232-317) and the call site (432)
"""

from __future__ import annotations


import pytest

pytestmark = pytest.mark.filterwarnings(
    "ignore::logfire._internal.config.LogfireNotConfiguredWarning"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_state(**kwargs):
    """Return a SupervisorState dict with sensible defaults."""
    from sqldim.core.query.dgm.nl.supervisor._types import (
        SupervisorState,
    )

    base = SupervisorState(utterance="test")
    d = base.model_dump()
    d.update(kwargs)
    return d


def _state_with_results(results):
    """Return a SupervisorState pydantic object with the given ToolCallResult list."""
    from sqldim.core.query.dgm.nl.supervisor._types import (
        SupervisorState,
    )

    state = SupervisorState(utterance="test query")
    object.__setattr__(state, "results", results)
    return state


# ---------------------------------------------------------------------------
# _stub_synthesize — dgm_query explanation branch (line 181)
# ---------------------------------------------------------------------------


class TestStubSynthesizeExplanation:
    """_stub_synthesize covers the `if explanation:` branch (line 181)."""

    def _call(self, results, *, has_model=False):
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorState,
        )

        state = SupervisorState(utterance="test query", results=results)
        return _stub_synthesize(state, has_model=has_model)

    def test_explanation_is_appended(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._types import ToolCallResult

        r = ToolCallResult(
            tool_name="dgm_query",
            success=True,
            data={
                "dataset": "ecommerce",
                "question": "revenue",
                "explanation": "Revenue is $1M",
            },
        )
        out = self._call([r])
        assert "Revenue is $1M" in out["synthesis"]

    def test_explanation_with_no_query_result(self) -> None:
        """When explanation present and no query_result, only explanation appears."""
        from sqldim.core.query.dgm.nl.supervisor._types import ToolCallResult

        r = ToolCallResult(
            tool_name="dgm_query",
            success=True,
            data={
                "dataset": "ecommerce",
                "question": "test",
                "explanation": "The answer is 42",
                "query_result": None,  # explicit None
            },
        )
        out = self._call([r])
        assert "The answer is 42" in out["synthesis"]
        assert "returned no rows" not in out["synthesis"]

    def test_both_explanation_and_query_result(self) -> None:
        """When both explanation and query_result present, both are included."""
        from sqldim.core.query.dgm.nl.supervisor._types import ToolCallResult

        r = ToolCallResult(
            tool_name="dgm_query",
            success=True,
            data={
                "dataset": "ecommerce",
                "question": "top customers",
                "explanation": "Here are top customers",
                "query_result": {
                    "count": 2,
                    "columns": ["name", "orders"],
                    "rows": [["Alice", 10], ["Bob", 8]],
                },
            },
        )
        out = self._call([r])
        assert "Here are top customers" in out["synthesis"]
        assert "Alice" in out["synthesis"] or "2 rows" in out["synthesis"]


# ---------------------------------------------------------------------------
# _stub_synthesize — dgm_query query_result branches (lines 183-194)
# ---------------------------------------------------------------------------


class TestStubSynthesizeQueryResult:
    """Tests for the query_result block inside _stub_synthesize (lines 183-194)."""

    def _synthesize_with_query_result(self, query_result, **extra_data):
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorState,
            ToolCallResult,
        )

        data = {
            "dataset": "ecommerce",
            "question": "test",
            "query_result": query_result,
        }
        data.update(extra_data)
        r = ToolCallResult(tool_name="dgm_query", success=True, data=data)
        state = SupervisorState(utterance="test", results=[r])
        return _stub_synthesize(state)

    def test_query_result_with_rows_shows_count_and_columns(self) -> None:
        """Lines 186-192 — non-empty rows triggered."""
        out = self._synthesize_with_query_result(
            {
                "count": 3,
                "columns": ["product", "revenue"],
                "rows": [["A", 100], ["B", 200], ["C", 300]],
            }
        )
        assert "3 rows" in out["synthesis"]
        assert "product" in out["synthesis"] or "revenue" in out["synthesis"]

    def test_query_result_with_no_rows_shows_no_rows_message(self) -> None:
        """Lines 193-194 — empty rows triggers 'returned no rows'."""
        out = self._synthesize_with_query_result(
            {
                "count": 0,
                "columns": ["product"],
                "rows": [],
            }
        )
        assert "returned no rows" in out["synthesis"]

    def test_query_result_with_large_table_shows_truncation_suffix(self) -> None:
        """count > 5 triggers the '... (N more rows)' suffix (lines 188-192)."""
        rows = [[f"item{i}", i * 100] for i in range(10)]
        out = self._synthesize_with_query_result(
            {
                "count": 10,
                "columns": ["name", "val"],
                "rows": rows,
            }
        )
        assert "more rows" in out["synthesis"]

    def test_query_result_exactly_5_rows_no_suffix(self) -> None:
        """Exactly 5 rows: count=5, no '... (N more rows)' suffix."""
        rows = [[f"item{i}"] for i in range(5)]
        out = self._synthesize_with_query_result(
            {
                "count": 5,
                "columns": ["name"],
                "rows": rows,
            }
        )
        assert "more rows" not in out["synthesis"]
        assert "5 rows" in out["synthesis"]

    def test_failed_result_shows_warning(self) -> None:
        """Failed ToolCallResult triggers the ⚠ error line."""
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorState,
            ToolCallResult,
        )

        r = ToolCallResult(
            tool_name="dgm_query",
            success=False,
            error="Connection refused",
        )
        state = SupervisorState(utterance="test", results=[r])
        out = _stub_synthesize(state)
        assert "Connection refused" in out["synthesis"] or "⚠" in out["synthesis"]

    def test_unknown_tool_in_else_branch(self) -> None:
        """The catch-all else branch for tools other than the 3 known ones."""
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorState,
            ToolCallResult,
        )

        r = ToolCallResult(
            tool_name="dgm_custom_tool",
            success=True,
            data={"key1": "val1", "key2": "val2"},
        )
        state = SupervisorState(utterance="test", results=[r])
        out = _stub_synthesize(state)
        assert "dgm_custom_tool" in out["synthesis"]

    def test_is_reflected_wraps_with_intro(self) -> None:
        """When 'reflect' is in visited_nodes, synthesis gets orientation intro."""
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorState,
            ToolCallResult,
        )

        r = ToolCallResult(
            tool_name="dgm_what_can_i_answer",
            success=True,
            data={"datasets": ["ecommerce"], "questions": ["q1", "q2"]},
        )
        state = SupervisorState(
            utterance="test",
            results=[r],
            visited_nodes=["plan", "reflect", "synthesize"],
        )
        out = _stub_synthesize(state)
        assert "DGM Supervisor" in out["synthesis"]

    def test_no_results_produces_no_results_message(self) -> None:
        """Empty results → 'No results produced.'"""
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        state = SupervisorState(utterance="test", results=[])
        out = _stub_synthesize(state)
        assert "No results produced" in out["synthesis"]

    def test_has_model_true_no_explanation_shows_different_message(self) -> None:
        """With has_model=True and no explanation, message omits '(stub)' hint."""
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorState,
            ToolCallResult,
        )

        r = ToolCallResult(
            tool_name="dgm_query",
            success=True,
            data={"dataset": "ecommerce", "question": "revenue"},
        )
        state = SupervisorState(utterance="test", results=[r])
        out = _stub_synthesize(state, has_model=True)
        assert "ecommerce" in out["synthesis"]
        assert "stub" not in out["synthesis"].lower()


# ---------------------------------------------------------------------------
# _stub_synthesize — dgm_what_can_i_answer edge cases (existing coverage gaps)
# ---------------------------------------------------------------------------


class TestStubSynthesizeDgmWhatCanIAnswer:
    def _synthesize_dgm_what(self, data, *, has_model=False):
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorState,
            ToolCallResult,
        )

        r = ToolCallResult(
            tool_name="dgm_what_can_i_answer",
            success=True,
            data=data,
        )
        state = SupervisorState(utterance="test", results=[r])
        return _stub_synthesize(state, has_model=has_model)

    def test_single_dataset_with_questions(self) -> None:
        out = self._synthesize_dgm_what(
            {
                "datasets": ["ecommerce"],
                "questions": ["What is revenue?", "Top customers?"],
            }
        )
        assert "Questions for 'ecommerce'" in out["synthesis"]

    def test_single_dataset_no_questions_no_model(self) -> None:
        out = self._synthesize_dgm_what({"datasets": ["ecommerce"], "questions": []})
        assert "ecommerce" in out["synthesis"]
        assert "pass --model-provider" in out["synthesis"]

    def test_single_dataset_no_questions_has_model(self) -> None:
        out = self._synthesize_dgm_what(
            {"datasets": ["ecommerce"], "questions": []}, has_model=True
        )
        assert "ecommerce" in out["synthesis"]
        assert "pass --model-provider" not in out["synthesis"]

    def test_multiple_datasets(self) -> None:
        out = self._synthesize_dgm_what(
            {"datasets": ["ecommerce", "fintech"], "questions": []}
        )
        assert "Datasets I can see" in out["synthesis"]

    def test_empty_datasets(self) -> None:
        out = self._synthesize_dgm_what({"datasets": [], "questions": []})
        assert "No datasets are currently available" in out["synthesis"]


# ---------------------------------------------------------------------------
# _stub_synthesize — dgm_map_analytical_space edge cases
# ---------------------------------------------------------------------------


class TestStubSynthesizeDgmMapAnalyticalSpace:
    def _synthesize_dgm_map(self, data):
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorState,
            ToolCallResult,
        )

        r = ToolCallResult(
            tool_name="dgm_map_analytical_space",
            success=True,
            data=data,
        )
        state = SupervisorState(utterance="test", results=[r])
        return _stub_synthesize(state)

    def test_with_tools(self) -> None:
        out = self._synthesize_dgm_map(
            {
                "tools": [
                    {"name": "dgm_query", "description": "Run NL queries"},
                    {"name": "dgm_correlate", "description": "Correlate datasets"},
                ]
            }
        )
        assert "Tools available" in out["synthesis"]
        assert "dgm_query" in out["synthesis"]

    def test_empty_tools(self) -> None:
        out = self._synthesize_dgm_map({"tools": []})
        assert "No tools registered" in out["synthesis"]


# ---------------------------------------------------------------------------
# _llm_synthesize — lines 232-317 and the call site (line 432)
# ---------------------------------------------------------------------------


class TestLlmSynthesize:
    """_llm_synthesize is called when context.model is not None.

    We mock pydantic_ai.Agent to avoid real LLM calls.
    """

    def _build_graph_with_model(self, mock_model="fake-model"):
        from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorContext

        context = SupervisorContext(
            available_datasets=["ecommerce"],
            model=mock_model,
        )
        return build_supervisor_graph(context=context)

    def test_llm_synthesize_success_path(self, monkeypatch) -> None:
        """When Agent.run_sync succeeds, synthesis contains the LLM output."""

        class FakeResult:
            output = "LLM generated answer about revenue"

        class FakeAgent:
            def __init__(self, *a, **kw):
                pass

            def run_sync(self, msg, **kw):
                return FakeResult()

        monkeypatch.setattr("pydantic_ai.Agent", FakeAgent)

        graph = self._build_graph_with_model()
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        result = graph.invoke(
            SupervisorState(utterance="show me revenue").model_dump(),
            config={
                "configurable": {"thread_id": "test-llm-synth"},
                "recursion_limit": 25,
            },
        )
        assert result.get("synthesis") == "LLM generated answer about revenue"

    def test_llm_synthesize_exception_fallback(self, monkeypatch) -> None:
        """When Agent.run_sync raises, falls back to _stub_synthesize(has_model=True)."""

        class FakeAgent:
            def __init__(self, *a, **kw):
                pass

            def run_sync(self, msg, **kw):
                raise RuntimeError("LLM API failure")

        monkeypatch.setattr("pydantic_ai.Agent", FakeAgent)

        graph = self._build_graph_with_model()
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        result = graph.invoke(
            SupervisorState(utterance="show me revenue").model_dump(),
            config={
                "configurable": {"thread_id": "test-llm-fallback"},
                "recursion_limit": 25,
            },
        )
        # Fallback synthesis should produce something from _stub_synthesize
        synthesis = result.get("synthesis", "")
        assert synthesis  # not empty
        # In stub mode with has_model=True, no "(stub)" hint
        assert "stub" not in synthesis.lower()

    def test_llm_synthesize_no_tool_results_falls_back_to_stub(
        self, monkeypatch
    ) -> None:
        """When parts list is empty, falls back to _stub_synthesize(has_model=True)."""

        class FakeAgent:
            def __init__(self, *a, **kw):
                pass

            def run_sync(self, msg, **kw):
                raise RuntimeError("no tool results")

        monkeypatch.setattr("pydantic_ai.Agent", FakeAgent)

        graph = self._build_graph_with_model()
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        result = graph.invoke(
            SupervisorState(utterance="hello").model_dump(),
            config={
                "configurable": {"thread_id": "test-no-results-llm"},
                "recursion_limit": 25,
            },
        )
        assert result.get("synthesis")

    def test_llm_synthesize_covers_dgm_query_explanation_path(
        self, monkeypatch
    ) -> None:
        """LLM synthesize builds parts from dgm_query explanation."""
        parts_captured = []

        class FakeAgent:
            def __init__(self, *a, **kw):
                pass

            def run_sync(self, msg, **kw):
                # Capture the prompt to verify explanation was included
                parts_captured.append(msg)

                class R:
                    output = "Revenue is $5M based on query"

                return R()

        monkeypatch.setattr("pydantic_ai.Agent", FakeAgent)

        # Patch DgmQueryTool.execute to return a result with explanation
        from sqldim.core.query.dgm.nl.supervisor._types import ToolCallResult

        def fake_execute(self, arguments, *, context=None):
            return ToolCallResult(
                tool_name="dgm_query",
                success=True,
                data={
                    "dataset": "ecommerce",
                    "question": arguments.get("question", ""),
                    "explanation": "Revenue totals $5M across categories",
                    "query_result": {
                        "count": 3,
                        "columns": ["category", "revenue"],
                        "rows": [["A", 1000], ["B", 2000], ["C", 2000]],
                    },
                },
            )

        from sqldim.core.query.dgm.nl.supervisor import _tools

        monkeypatch.setattr(_tools.DgmQueryTool, "execute", fake_execute)

        graph = self._build_graph_with_model()
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        result = graph.invoke(
            SupervisorState(utterance="show revenue by category").model_dump(),
            config={
                "configurable": {"thread_id": "test-llm-explanation"},
                "recursion_limit": 25,
            },
        )
        assert result.get("synthesis") == "Revenue is $5M based on query"
        # The prompt should contain the explanation
        assert any("Revenue totals" in p for p in parts_captured)

    def test_llm_synthesize_conversation_history_included(self, monkeypatch) -> None:
        """Prior conversation history is included in the LLM prompt."""
        prompts = []

        class FakeAgent:
            def __init__(self, *a, **kw):
                pass

            def run_sync(self, msg, **kw):
                prompts.append(msg)

                class R:
                    output = "follow-up answer"

                return R()

        monkeypatch.setattr("pydantic_ai.Agent", FakeAgent)

        graph = self._build_graph_with_model()
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        result = graph.invoke(
            SupervisorState(
                utterance="and what about margins?",
                conversation_history=[
                    {"role": "user", "content": "show revenue"},
                    {"role": "assistant", "content": "Revenue is $5M"},
                ],
            ).model_dump(),
            config={
                "configurable": {"thread_id": "test-history"},
                "recursion_limit": 25,
            },
        )
        assert result.get("synthesis") == "follow-up answer"
        # Prior conversation should be in the prompt
        assert any("Revenue is $5M" in p for p in prompts)


# ---------------------------------------------------------------------------
# _stub_execute_tool — unknown tool branch
# (This is the standalone function, not the graph's _execute_node)
# ---------------------------------------------------------------------------


class TestStubExecuteToolUnknownTool:
    """_stub_execute_tool returns a failure result for unknown tool names."""

    def test_unknown_tool_name_returns_failure(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_execute_tool
        from sqldim.core.query.dgm.nl.supervisor._types import (
            SupervisorPlan,
            SupervisorState,
            ToolCallRequest,
        )

        plan = SupervisorPlan(
            goal="test",
            steps=[
                ToolCallRequest(
                    tool_name="dgm_nonexistent_tool_xyz",
                    arguments={"question": "test"},
                    budget_fraction=1.0,
                )
            ],
        )
        state = SupervisorState(utterance="test", plan=plan, current_step=0)
        result = _stub_execute_tool(state)
        # Should return a failed ToolCallResult for the unknown tool
        assert len(result["results"]) == 1
        r = result["results"][0]
        assert r.success is False
        assert "Unknown tool" in r.error

    def test_empty_plan_returns_empty(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._graph import _stub_execute_tool
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        state = SupervisorState(utterance="test", plan=None)
        result = _stub_execute_tool(state)
        assert result == {}


# ---------------------------------------------------------------------------
# _llm_synthesize — missing branch coverage
# Lines 258, 260, 269, 281-285, 288
# ---------------------------------------------------------------------------


class TestLlmSynthesizeMissingBranches:
    """Direct unit tests for _llm_synthesize to cover uncovered branches."""

    def _call(self, results, monkeypatch):
        """Mock pydantic_ai.Agent to capture the prompt and call _llm_synthesize."""
        captured: list[str] = []

        class FakeResult:
            output = "synthesized answer"

        class FakeAgent:
            def __init__(self, *a, **kw):
                pass

            def run_sync(self, msg, **kw):
                captured.append(msg)
                return FakeResult()

        monkeypatch.setattr("pydantic_ai.Agent", FakeAgent)

        from sqldim.core.query.dgm.nl.supervisor._graph import _llm_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        state = SupervisorState(utterance="test", results=results)
        out = _llm_synthesize(state, model="fake-model")
        return out, captured

    def test_dgm_query_empty_rows_appends_no_rows_message(self, monkeypatch) -> None:
        """Line 258: dgm_query with query_result containing empty rows list."""
        from sqldim.core.query.dgm.nl.supervisor._types import ToolCallResult

        r = ToolCallResult(
            tool_name="dgm_query",
            success=True,
            data={
                "dataset": "ecommerce",
                "explanation": "",
                "query_result": {"rows": [], "count": 0, "columns": ["col1"]},
            },
        )
        out, prompts = self._call([r], monkeypatch)
        # "returned no rows" is in the tool_context sent to the LLM
        assert any("returned no rows" in p for p in prompts)

    def test_dgm_query_no_result_no_explanation_appends_completed(
        self, monkeypatch
    ) -> None:
        """Line 260: dgm_query with no explanation and no query_result."""
        from sqldim.core.query.dgm.nl.supervisor._types import ToolCallResult

        r = ToolCallResult(
            tool_name="dgm_query",
            success=True,
            data={"dataset": "ecommerce"},
        )
        out, prompts = self._call([r], monkeypatch)
        assert any("query completed (no data)" in p for p in prompts)

    def test_dgm_what_can_i_answer_with_questions_appended(self, monkeypatch) -> None:
        """Line 269: dgm_what_can_i_answer with non-empty questions list."""
        from sqldim.core.query.dgm.nl.supervisor._types import ToolCallResult

        r = ToolCallResult(
            tool_name="dgm_what_can_i_answer",
            success=True,
            data={"datasets": [], "questions": ["What is revenue?", "Top customers?"]},
        )
        out, prompts = self._call([r], monkeypatch)
        assert any("Sample questions" in p for p in prompts)

    def test_unknown_tool_with_non_empty_summary_appended(self, monkeypatch) -> None:
        """Lines 281-285: unknown tool name with non-empty, non-stub data dict."""
        from sqldim.core.query.dgm.nl.supervisor._types import ToolCallResult

        r = ToolCallResult(
            tool_name="dgm_custom_analytics",
            success=True,
            data={"result": "some data", "count": 5},
        )
        out, prompts = self._call([r], monkeypatch)
        assert any("dgm_custom_analytics" in p for p in prompts)

    def test_empty_results_falls_back_to_stub_synthesize(self) -> None:
        """Line 288: no results → parts is empty → _stub_synthesize(has_model=True)."""
        from sqldim.core.query.dgm.nl.supervisor._graph import _llm_synthesize
        from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

        state = SupervisorState(utterance="test", results=[])
        out = _llm_synthesize(state, model="fake-model")
        # Should produce a non-empty synthesis via stub fallback
        assert out.get("synthesis")
