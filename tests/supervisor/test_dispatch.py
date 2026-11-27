"""§12.4 execute_tool: every refusal is typed, nothing is substituted."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from sqldim.core.query.dgm.nl.supervisor._auth import Action
from sqldim.core.query.dgm.nl.supervisor._dispatch import child_budget, execute_tool
from sqldim.core.query.dgm.nl.supervisor._tools import (
    TOOL_REGISTRY,
    DemoQueryTool,
    build_registry,
)
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    CancelToken,
    ToolCallRequest,
    ToolCallResult,
    ToolContext,
    ToolErrorKind,
)
from sqldim.core.query.dgm.nl.supervisor._validation import bind_plan
from tests.supervisor._fakes import (
    FakeTool,
    Opener,
    auth,
    budget,
    plan,
    query_turn,
    registry,
    step,
    succeed,
    tool_context,
)


# Tools whose D43 contract is implemented; every other tool fails closed.
_IMPLEMENTED = {"dgm_validate_invariant", "dgm_map_analytical_space"}


def _bound(*steps: ToolCallRequest) -> list[ToolCallRequest]:
    bound, errors = bind_plan(plan(*steps))
    assert errors == []
    return bound.steps


def _run(
    request: ToolCallRequest, reg: Any, ctx: ToolContext, **kw: Any
) -> ToolCallResult:
    return asyncio.run(execute_tool(request, reg, ctx, **kw))


class TestRefusals:
    def test_unbound_request_is_refused(self) -> None:
        tool = FakeTool("dgm_query")
        r = _run(step("a"), registry(tool), tool_context())
        assert r.status is CallStatus.FAILED
        assert r.error.kind is ToolErrorKind.INVALID_ARGUMENTS
        assert tool.calls == []

    def test_unknown_tool(self) -> None:
        req = step("a").model_copy(
            update={"tool_name": "dgm_magic", "datasets": [], "idempotency_key": "k"}
        )
        r = _run(req, registry(), tool_context())
        assert r.error.kind is ToolErrorKind.UNKNOWN_TOOL

    def test_tool_removed_from_registry_is_unknown_not_substituted(self) -> None:
        [req] = _bound(step("a"))
        r = _run(req, {}, tool_context())
        assert r.error.kind is ToolErrorKind.UNKNOWN_TOOL

    def test_unavailable_tool_is_not_implemented_and_does_not_run(self) -> None:
        [req] = _bound(step("a"))
        opener = Opener()
        r = _run(req, TOOL_REGISTRY, tool_context(opener=opener))
        assert r.status is CallStatus.FAILED
        assert r.error.kind is ToolErrorKind.NOT_IMPLEMENTED
        assert not r.error.retryable
        assert "§11.10" in r.error.context["missing"]
        assert r.data is None
        assert opener.opened == []

    @pytest.mark.parametrize("name", sorted(set(TOOL_REGISTRY) - _IMPLEMENTED))
    def test_every_unimplemented_tool_is_unavailable(self, name: str) -> None:
        assert TOOL_REGISTRY[name].capability_schema.available is False

    @pytest.mark.parametrize("name", sorted(_IMPLEMENTED))
    def test_implemented_tools_are_contract_tools(self, name: str) -> None:
        schema = TOOL_REGISTRY[name].capability_schema
        assert schema.available is True and "+demo" not in schema.version

    def test_invalid_arguments_rechecked_at_execution(self) -> None:
        tool = FakeTool("dgm_query")
        req = _bound(step("a"))[0].model_copy(
            update={"arguments": {"dataset": "sales"}}
        )
        r = _run(req, registry(tool), tool_context())
        assert r.error.kind is ToolErrorKind.INVALID_ARGUMENTS
        assert tool.calls == []

    def test_unauthorized_reads_no_data_and_is_final(self) -> None:
        tool = FakeTool("dgm_query")
        opener = Opener()
        [req] = _bound(step("a", arguments={"dataset": "secret", "utterance": "x"}))
        r = _run(req, registry(tool), tool_context(opener=opener))
        assert r.status is CallStatus.FAILED
        assert r.error.kind is ToolErrorKind.UNAUTHORIZED
        assert r.error.message == "not authorized or not found"
        assert not r.error.retryable
        assert tool.calls == [] and opener.opened == []

    def test_grant_revoked_after_validation(self) -> None:
        tool = FakeTool("dgm_query")
        [req] = _bound(step("a"))
        revoked = auth({"other": {Action.READ}})
        r = _run(req, registry(tool), tool_context(revoked))
        assert r.error.kind is ToolErrorKind.UNAUTHORIZED
        assert tool.calls == []

    def test_cancelled_run_does_not_start_call(self) -> None:
        tool = FakeTool("dgm_query")
        cancel = CancelToken()
        cancel.cancel()
        [req] = _bound(step("a"))
        r = _run(req, registry(tool), tool_context(cancel=cancel))
        assert r.status is CallStatus.CANCELLED
        assert r.error.kind is ToolErrorKind.CANCELLED
        assert tool.calls == []

    def test_deadline_passed(self) -> None:
        tool = FakeTool("dgm_query")
        [req] = _bound(step("a"))
        past = datetime.now(timezone.utc) - timedelta(seconds=1)
        r = _run(req, registry(tool), tool_context(deadline=past))
        assert r.error.kind is ToolErrorKind.TIMEOUT
        assert r.error.retryable
        assert tool.calls == []


class TestExecution:
    def test_success_records_provenance_and_timing(self) -> None:
        tool = FakeTool("dgm_query")
        [req] = _bound(step("a"))
        r = _run(req, registry(tool), tool_context(), attempt=2)
        assert r.status is CallStatus.SUCCEEDED
        assert r.is_contract_success
        assert r.datasets == ["sales"]
        assert r.attempts == 2
        assert r.started_at is not None and r.finished_at is not None

    def test_tool_sees_only_its_request_datasets(self) -> None:
        seen: dict[str, Any] = {}

        def probe(request: ToolCallRequest, ctx: ToolContext, attempt: int) -> Any:
            seen["open_own"] = ctx.data.open("sales")
            try:
                ctx.data.open("other")
            except PermissionError as exc:
                seen["other"] = str(exc)
            return succeed(query_turn())(request, ctx, attempt)

        opener = Opener()
        tool = FakeTool("dgm_query", probe)
        [req] = _bound(step("a"))
        grants = auth({"sales": {Action.READ}, "other": {Action.READ}})
        r = _run(req, registry(tool), tool_context(grants, opener))
        assert r.status is CallStatus.SUCCEEDED
        assert seen == {
            "open_own": "handle:sales",
            "other": "not authorized or not found",
        }
        assert opener.opened == ["sales"]

    def test_tool_receives_child_budget(self) -> None:
        tool = FakeTool("dgm_query")
        [req] = _bound(step("a", fraction=0.25))
        _run(req, registry(tool), tool_context())
        assert tool.contexts[0].budget.max_estimated_cost.cpu_ops == 250

    def test_timeout_is_retryable_failure(self) -> None:
        tool = FakeTool("dgm_query", delay=0.5)
        [req] = _bound(step("a"))
        ctx = tool_context(
            deadline=datetime.now(timezone.utc) + timedelta(seconds=0.05)
        )
        r = _run(req, registry(tool), ctx)
        assert r.status is CallStatus.FAILED
        assert r.error.kind is ToolErrorKind.TIMEOUT
        assert r.error.retryable

    def test_raising_tool_is_tool_failure_not_success(self) -> None:
        def boom(request: ToolCallRequest, ctx: ToolContext, attempt: int) -> Any:
            raise ValueError("kaput")

        [req] = _bound(step("a"))
        r = _run(req, registry(FakeTool("dgm_query", boom)), tool_context())
        assert r.status is CallStatus.FAILED
        assert r.error.kind is ToolErrorKind.TOOL_FAILURE
        assert r.error.context["exception"] == "ValueError"
        assert not r.error.retryable

    def test_output_violating_schema_is_invalid_output(self) -> None:
        [req] = _bound(step("a"))
        tool = FakeTool("dgm_query", succeed({"stub": True}))
        r = _run(req, registry(tool), tool_context())
        assert r.status is CallStatus.FAILED
        assert r.error.kind is ToolErrorKind.INVALID_OUTPUT
        assert r.data is None

    def test_result_for_another_call_is_invalid_output(self) -> None:
        def other(request: ToolCallRequest, ctx: ToolContext, attempt: int) -> Any:
            return succeed(query_turn())(
                request.model_copy(update={"call_id": "zz"}), ctx, attempt
            )

        [req] = _bound(step("a"))
        r = _run(req, registry(FakeTool("dgm_query", other)), tool_context())
        assert r.error.kind is ToolErrorKind.INVALID_OUTPUT

    def test_demo_result_outside_demo_mode_is_invalid_output(self) -> None:
        def demo(request: ToolCallRequest, ctx: ToolContext, attempt: int) -> Any:
            r = succeed(query_turn())(request, ctx, attempt)
            return r.model_copy(update={"demo": True})

        [req] = _bound(step("a"))
        r = _run(req, registry(FakeTool("dgm_query", demo)), tool_context())
        assert r.error.kind is ToolErrorKind.INVALID_OUTPUT

    def test_async_turn_must_carry_job(self) -> None:
        turn = {**query_turn(), "decision": "ASYNC"}
        [req] = _bound(step("a"))
        r = _run(req, registry(FakeTool("dgm_query", succeed(turn))), tool_context())
        assert r.error.kind is ToolErrorKind.INVALID_OUTPUT


class TestChildBudget:
    def test_scales_cost_fields_only(self) -> None:
        b = budget(1000)
        deadline = datetime.now(timezone.utc) + timedelta(hours=1)
        child = child_budget(b, 0.5, deadline)
        assert child.max_estimated_cost.cpu_ops == 500
        assert child.streaming_threshold.cpu_ops == 250
        assert child.async_threshold.cpu_ops == 250
        assert child.max_result_rows == b.max_result_rows
        assert child.max_precompute_time == b.max_precompute_time
        assert child.max_wall_time == b.max_wall_time

    def test_wall_time_bounded_by_deadline(self) -> None:
        now = datetime.now(timezone.utc)
        child = child_budget(budget(), 1.0, now + timedelta(seconds=2), now)
        assert child.max_wall_time == timedelta(seconds=2)

    def test_deadline_in_past_gives_zero(self) -> None:
        now = datetime.now(timezone.utc)
        child = child_budget(budget(), 1.0, now - timedelta(seconds=2), now)
        assert child.max_wall_time == timedelta(0)


class TestDemoRegistry:
    def test_demo_registry_adds_only_demo_query(self) -> None:
        demo = build_registry(demo=True)
        available = [
            n
            for n, t in demo.items()
            if t.capability_schema.available and n not in _IMPLEMENTED
        ]
        assert available == ["dgm_query"]
        assert isinstance(demo["dgm_query"], DemoQueryTool)
        assert demo["dgm_query"].capability_schema.version.endswith("+demo")

    def test_registry_is_immutable(self) -> None:
        with pytest.raises(TypeError):
            TOOL_REGISTRY["dgm_query"] = DemoQueryTool()  # type: ignore[index]

    def test_demo_query_refuses_outside_demo_mode(self) -> None:
        [req] = _bound(step("a"))
        tool = DemoQueryTool()
        r = asyncio.run(tool.execute(req, tool_context(demo=False)))
        assert r.error.kind is ToolErrorKind.NOT_IMPLEMENTED

    def test_demo_query_failure_is_reported_and_marked(self) -> None:
        [req] = _bound(step("a"))
        tool = DemoQueryTool()
        ctx = tool_context(demo=True)
        r = asyncio.run(tool.execute(req, ctx))  # unscoped data access is denied
        assert r.status is CallStatus.FAILED
        assert r.error.kind is ToolErrorKind.UNAUTHORIZED
        assert r.demo is True

    def test_unavailable_tool_has_no_cost_estimate(self) -> None:
        from sqldim.core.query.dgm.nl.supervisor._tools import ToolUnavailable

        with pytest.raises(ToolUnavailable):
            TOOL_REGISTRY["dgm_correlate"].cost_estimate({}, tool_context())

    def test_default_compose_reports_composition_failure(self) -> None:
        ok = ToolCallResult(
            call_id="a",
            tool_name="dgm_query",
            status=CallStatus.SUCCEEDED,
            data=query_turn(),
            attempts=1,
        )
        r = FakeTool("dgm_query").compose(ok, ok)
        assert r.status is CallStatus.FAILED
        assert r.error.kind is ToolErrorKind.COMPOSITION_FAILURE
