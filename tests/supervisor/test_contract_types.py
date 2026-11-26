"""§12.2 contract types: fail-closed invariants of results, errors and plans."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    SupervisorPlan,
    ToolCallRequest,
    ToolCallResult,
    ToolError,
    ToolErrorKind,
)
from tests.supervisor._fakes import step


def _error(kind: ToolErrorKind = ToolErrorKind.TOOL_FAILURE) -> ToolError:
    return ToolError(kind=kind, message="m")


class TestToolCallResultInvariants:
    def test_succeeded_requires_data(self) -> None:
        with pytest.raises(ValidationError, match="must carry data"):
            ToolCallResult(
                call_id="a", tool_name="t", status=CallStatus.SUCCEEDED, attempts=1
            )

    def test_empty_collection_is_data(self) -> None:
        r = ToolCallResult(
            call_id="a", tool_name="t", status=CallStatus.SUCCEEDED, data=[], attempts=1
        )
        assert r.is_contract_success

    def test_failure_must_not_carry_data(self) -> None:
        with pytest.raises(ValidationError, match="only a SUCCEEDED"):
            ToolCallResult(
                call_id="a",
                tool_name="t",
                status=CallStatus.FAILED,
                data={"stub": True},
                error=_error(),
                attempts=1,
            )

    @pytest.mark.parametrize(
        "status",
        [
            CallStatus.FAILED,
            CallStatus.REJECTED,
            CallStatus.CANCELLED,
            CallStatus.SKIPPED,
        ],
    )
    def test_non_success_requires_error(self, status: CallStatus) -> None:
        with pytest.raises(ValidationError, match="must carry an error"):
            ToolCallResult(call_id="a", tool_name="t", status=status, attempts=1)

    def test_success_must_not_carry_error(self) -> None:
        with pytest.raises(ValidationError, match="must not carry an error"):
            ToolCallResult(
                call_id="a",
                tool_name="t",
                status=CallStatus.SUCCEEDED,
                data={},
                error=_error(),
                attempts=1,
            )

    @pytest.mark.parametrize("status", [CallStatus.PENDING, CallStatus.RUNNING])
    def test_non_terminal_status_rejected(self, status: CallStatus) -> None:
        with pytest.raises(ValidationError, match="not a terminal"):
            ToolCallResult(
                call_id="a", tool_name="t", status=status, error=_error(), attempts=1
            )

    def test_skipped_has_no_attempts(self) -> None:
        with pytest.raises(ValidationError, match="SKIPPED result has no attempts"):
            ToolCallResult(
                call_id="a",
                tool_name="t",
                status=CallStatus.SKIPPED,
                error=_error(ToolErrorKind.DEPENDENCY_NOT_MET),
                attempts=1,
            )

    def test_executed_result_has_an_attempt(self) -> None:
        with pytest.raises(ValidationError, match="at least one attempt"):
            ToolCallResult(
                call_id="a",
                tool_name="t",
                status=CallStatus.FAILED,
                error=_error(),
                attempts=0,
            )

    def test_cancelled_before_dispatch_may_have_no_attempt(self) -> None:
        r = ToolCallResult.failure(
            step("a"),
            CallStatus.CANCELLED,
            ToolErrorKind.CANCELLED,
            "cancelled",
            attempts=0,
        )
        assert r.attempts == 0

    def test_budget_rejected_before_dispatch_may_have_no_attempt(self) -> None:
        r = ToolCallResult.failure(
            step("a"),
            CallStatus.REJECTED,
            ToolErrorKind.BUDGET_REJECTED,
            "reservation does not fit",
            attempts=0,
        )
        assert r.attempts == 0

    def test_demo_success_is_not_a_contract_success(self) -> None:
        r = ToolCallResult(
            call_id="a",
            tool_name="t",
            status=CallStatus.SUCCEEDED,
            data={},
            attempts=1,
            demo=True,
        )
        assert not r.is_contract_success

    def test_failure_helper_copies_request_identity(self) -> None:
        req = step("a").model_copy(update={"datasets": ["sales"]})
        r = ToolCallResult.failure(
            req, CallStatus.FAILED, ToolErrorKind.UNSUPPORTED, "no"
        )
        assert (r.call_id, r.tool_name, r.datasets) == ("a", "dgm_query", ["sales"])
        assert r.data is None and r.error is not None


class TestToolError:
    @pytest.mark.parametrize(
        "kind", [ToolErrorKind.TIMEOUT, ToolErrorKind.TOOL_FAILURE]
    )
    def test_transient_kinds_may_be_retryable(self, kind: ToolErrorKind) -> None:
        assert ToolError(kind=kind, message="m", retryable=True).retryable

    @pytest.mark.parametrize(
        "kind",
        [
            k
            for k in ToolErrorKind
            if k not in (ToolErrorKind.TIMEOUT, ToolErrorKind.TOOL_FAILURE)
        ],
    )
    def test_other_kinds_are_never_retryable(self, kind: ToolErrorKind) -> None:
        with pytest.raises(ValidationError, match="never retryable"):
            ToolError(kind=kind, message="m", retryable=True)

    def test_kinds_match_spec(self) -> None:
        assert {k.value for k in ToolErrorKind} == {
            "UNKNOWN_TOOL",
            "INVALID_ARGUMENTS",
            "UNAUTHORIZED",
            "NOT_IMPLEMENTED",
            "UNSUPPORTED",
            "BUDGET_REJECTED",
            "NEEDS_CLARIFICATION",
            "TIMEOUT",
            "CANCELLED",
            "TOOL_FAILURE",
            "INVALID_OUTPUT",
            "DEPENDENCY_NOT_MET",
            "COMPOSITION_FAILURE",
        }


class TestRequestAndPlan:
    def test_budget_fraction_bounds(self) -> None:
        with pytest.raises(ValidationError):
            step("a", fraction=1.5)
        with pytest.raises(ValidationError):
            step("a", fraction=-0.1)

    def test_request_needs_call_id(self) -> None:
        with pytest.raises(ValidationError):
            ToolCallRequest(call_id="", tool_name="dgm_query", budget_fraction=0.1)

    def test_request_is_frozen(self) -> None:
        with pytest.raises(ValidationError):
            step("a").datasets = ["x"]  # type: ignore[misc]

    def test_invalid_plan_is_representable(self) -> None:
        p = SupervisorPlan(
            plan_id="p",
            version=1,
            goal="g",
            steps=[step("a", fraction=0.8), step("a", fraction=0.8)],
            confidence=0.5,
        )
        assert p.total_budget_fraction == pytest.approx(1.6)

    def test_plan_version_starts_at_one(self) -> None:
        with pytest.raises(ValidationError):
            SupervisorPlan(plan_id="p", version=0, goal="g", confidence=0.5)

    def test_step_lookup(self) -> None:
        p = SupervisorPlan(
            plan_id="p", version=1, goal="g", steps=[step("a")], confidence=0.5
        )
        assert p.step("a").call_id == "a"
        with pytest.raises(KeyError):
            p.step("zz")
