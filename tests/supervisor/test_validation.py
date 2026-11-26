"""§12.2 plan validity (P1–P6), binding, and §12.5 replan constraints."""

from __future__ import annotations

from sqldim.core.query.dgm.nl.supervisor._auth import Action
from sqldim.core.query.dgm.nl.supervisor._tools import TOOL_REGISTRY, UnavailableTool
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    ToolCallResult,
    ToolErrorKind,
)
from sqldim.core.query.dgm.nl.supervisor._validation import (
    bind_plan,
    check_replan,
    idempotency_key,
    required_actions,
    validate_plan,
)
from tests.supervisor._fakes import FakeTool, auth, plan, registry, step

CORRELATE_ARGS = {"left": "a", "right": "b", "keys": ["region"]}


def _rules(check) -> list[tuple[str, ToolErrorKind]]:  # type: ignore[no-untyped-def]
    return [(v.context.get("rule"), v.kind) for v in check.violations]


def _reg():  # type: ignore[no-untyped-def]
    return registry(FakeTool("dgm_query"), FakeTool("dgm_correlate"))


class TestValidPlan:
    def test_valid_two_step_plan(self) -> None:
        check = validate_plan(plan(step("a"), step("b")), _reg(), auth())
        assert check.valid, check.violations

    def test_empty_plan_is_valid(self) -> None:
        assert validate_plan(plan(), _reg(), auth()).valid


class TestP1UniqueIds:
    def test_duplicate_call_id(self) -> None:
        check = validate_plan(plan(step("a"), step("a")), _reg(), auth())
        assert ("P1", ToolErrorKind.INVALID_ARGUMENTS) in _rules(check)


class TestP2Dependencies:
    def test_unknown_dependency(self) -> None:
        check = validate_plan(plan(step("a", depends_on=["zz"])), _reg(), auth())
        assert ("P2", ToolErrorKind.INVALID_ARGUMENTS) in _rules(check)

    def test_forward_dependency(self) -> None:
        check = validate_plan(
            plan(step("a", depends_on=["b"]), step("b")), _reg(), auth()
        )
        assert ("P2", ToolErrorKind.INVALID_ARGUMENTS) in _rules(check)

    def test_self_dependency(self) -> None:
        check = validate_plan(plan(step("a", depends_on=["a"])), _reg(), auth())
        assert ("P2", ToolErrorKind.INVALID_ARGUMENTS) in _rules(check)


class TestP3Tools:
    def test_unknown_tool(self) -> None:
        check = validate_plan(plan(step("a", tool="dgm_magic")), _reg(), auth())
        assert ("P3", ToolErrorKind.UNKNOWN_TOOL) in _rules(check)

    def test_no_case_insensitive_resolution(self) -> None:
        check = validate_plan(plan(step("a", tool="DGM_QUERY")), _reg(), auth())
        assert ("P3", ToolErrorKind.UNKNOWN_TOOL) in _rules(check)

    def test_unavailable_tool(self) -> None:
        check = validate_plan(plan(step("a")), TOOL_REGISTRY, auth())
        assert _rules(check) == [("P3", ToolErrorKind.NOT_IMPLEMENTED)]
        assert check.violations[0].context["missing"]

    def test_invalid_arguments(self) -> None:
        check = validate_plan(
            plan(step("a", arguments={"dataset": "sales", "question": "x"})),
            _reg(),
            auth(),
        )
        assert ("P3", ToolErrorKind.INVALID_ARGUMENTS) in _rules(check)

    def test_policy_bound_is_invalid_not_truncated(self) -> None:
        reg = registry(FakeTool("dgm_what_can_i_answer"))
        args = {"dataset": "sales", "anchors": ["a"], "max_hops": 99, "limit": 10}
        check = validate_plan(
            plan(step("a", "dgm_what_can_i_answer", args)), reg, auth()
        )
        assert ("P3", ToolErrorKind.INVALID_ARGUMENTS) in _rules(check)

    def test_reference_must_be_a_dependency(self) -> None:
        reg = registry(FakeTool("dgm_query"), FakeTool("dgm_correlate"))
        p = plan(
            step("a", fraction=0.3),
            step("b", fraction=0.3),
            step("c", "dgm_correlate", CORRELATE_ARGS, depends_on=["a"], fraction=0.3),
        )
        check = validate_plan(p, reg, auth({"sales": {Action.READ, Action.CORRELATE}}))
        assert ("P3", ToolErrorKind.INVALID_ARGUMENTS) in _rules(check)


class TestP4Composability:
    def test_incompatible_dependency(self) -> None:
        reg = registry(FakeTool("dgm_query"), FakeTool("dgm_map_analytical_space"))
        p = plan(
            step("a"),
            step(
                "b",
                "dgm_map_analytical_space",
                {"dataset": "sales"},
                depends_on=["a"],
            ),
        )
        assert ("P4", ToolErrorKind.INVALID_ARGUMENTS) in _rules(
            validate_plan(p, reg, auth())
        )

    def test_declared_composition_is_valid(self) -> None:
        p = plan(
            step("a", fraction=0.3),
            step("b", fraction=0.3),
            step(
                "c",
                "dgm_correlate",
                CORRELATE_ARGS,
                depends_on=["a", "b"],
                fraction=0.3,
            ),
        )
        check = validate_plan(
            p, _reg(), auth({"sales": {Action.READ, Action.CORRELATE}})
        )
        assert check.valid, check.violations


class TestP5Budget:
    def test_fractions_over_one(self) -> None:
        check = validate_plan(
            plan(step("a", fraction=0.6), step("b", fraction=0.6)), _reg(), auth()
        )
        assert ("P5", ToolErrorKind.INVALID_ARGUMENTS) in _rules(check)

    def test_fractions_exactly_one(self) -> None:
        check = validate_plan(
            plan(step("a", fraction=0.5), step("b", fraction=0.5)), _reg(), auth()
        )
        assert check.valid


class TestP6Grants:
    def test_ungranted_dataset(self) -> None:
        p = plan(step("a", arguments={"dataset": "secret", "utterance": "x"}))
        check = validate_plan(p, _reg(), auth())
        assert _rules(check) == [("P6", ToolErrorKind.UNAUTHORIZED)]
        assert check.violations[0].message == "not authorized or not found"
        assert "secret" not in str(check.violations[0].context)

    def test_correlate_needs_correlate_on_both(self) -> None:
        p = plan(
            step("a", fraction=0.3),
            step("b", arguments={"dataset": "other", "utterance": "x"}, fraction=0.3),
            step(
                "c",
                "dgm_correlate",
                CORRELATE_ARGS,
                depends_on=["a", "b"],
                fraction=0.3,
            ),
        )
        grants = {"sales": {Action.READ, Action.CORRELATE}, "other": {Action.READ}}
        check = validate_plan(p, _reg(), auth(grants))
        assert ("P6", ToolErrorKind.UNAUTHORIZED) in _rules(check)
        assert check.violations[0].context["call_id"] == "c"

    def test_store_finding_needs_write_kb(self) -> None:
        reg = registry(FakeTool("dgm_store_finding"))
        args = {
            "finding": {
                "question_key": "q",
                "datasets": ["sales"],
                "content": "revenue fell",
            }
        }
        p = plan(step("s", "dgm_store_finding", args))
        assert ("P6", ToolErrorKind.UNAUTHORIZED) in _rules(
            validate_plan(p, reg, auth())
        )
        ok = validate_plan(p, reg, auth({"sales": {Action.READ, Action.WRITE_KB}}))
        assert ok.valid

    def test_required_actions_table(self) -> None:
        assert required_actions("dgm_query") == {Action.READ}
        assert required_actions("dgm_correlate") == {Action.READ, Action.CORRELATE}
        assert required_actions("dgm_store_finding") == {Action.READ, Action.WRITE_KB}


class TestBinding:
    def test_planner_values_are_discarded(self) -> None:
        forged = step("a").model_copy(
            update={"datasets": ["secret"], "idempotency_key": "forged"}
        )
        bound, errors = bind_plan(plan(forged))
        assert errors == []
        assert bound.steps[0].datasets == ["sales"]
        assert bound.steps[0].idempotency_key != "forged"

    def test_references_inherit_datasets(self) -> None:
        p = plan(
            step("a", fraction=0.3),
            step("b", arguments={"dataset": "other", "utterance": "x"}, fraction=0.3),
            step(
                "c",
                "dgm_correlate",
                CORRELATE_ARGS,
                depends_on=["a", "b"],
                fraction=0.3,
            ),
        )
        bound, _ = bind_plan(p)
        assert bound.step("c").datasets == ["other", "sales"]

    def test_wildcard_pointer(self) -> None:
        args = {
            "finding": {"question_key": "q", "datasets": ["b", "a"], "content": "c"}
        }
        bound, _ = bind_plan(plan(step("s", "dgm_store_finding", args)))
        assert bound.steps[0].datasets == ["a", "b"]

    def test_idempotency_key_is_deterministic_and_argument_sensitive(self) -> None:
        a = idempotency_key("p", step("a"))
        assert a == idempotency_key("p", step("a"))
        assert a != idempotency_key("q", step("a"))
        other = step("a", arguments={"dataset": "sales", "utterance": "other"})
        assert a != idempotency_key("p", other)

    def test_unavailable_tool_reports_only_p3(self) -> None:
        reg = {"dgm_query": UnavailableTool("dgm_query")}
        check = validate_plan(plan(step("a")), reg, auth())
        assert [v.context["rule"] for v in check.violations] == ["P3"]


def _ok(call_id: str) -> ToolCallResult:
    return ToolCallResult(
        call_id=call_id,
        tool_name="dgm_query",
        status=CallStatus.SUCCEEDED,
        data={},
        attempts=1,
    )


def _rejected(call_id: str) -> ToolCallResult:
    return ToolCallResult.failure(
        step(call_id), CallStatus.REJECTED, ToolErrorKind.BUDGET_REJECTED, "too big"
    )


class TestReplan:
    def test_valid_replan(self) -> None:
        v1 = plan(step("a", fraction=0.3), step("b", fraction=0.3))
        b2 = step(
            "b2", arguments={"dataset": "sales", "utterance": "smaller"}, fraction=0.3
        )
        v2 = plan(step("a", fraction=0.3), b2, version=2)
        out = check_replan(
            v2, v1, {"a": _ok("a"), "b": _rejected("b")}, _rejected("b"), 0.7
        )
        assert out == []

    def test_must_keep_plan_id_and_advance_version(self) -> None:
        v1 = plan(step("a"))
        out = check_replan(
            plan(step("a"), plan_id="other", version=2), v1, {}, None, 1.0
        )
        assert [v.context["rule"] for v in out] == ["R0"]
        out = check_replan(plan(step("a"), version=1), v1, {}, None, 1.0)
        assert [v.context["rule"] for v in out] == ["R0"]

    def test_succeeded_call_must_be_kept(self) -> None:
        v1 = plan(step("a"), step("b"))
        v2 = plan(step("b"), version=2)
        out = check_replan(v2, v1, {"a": _ok("a")}, None, 1.0)
        assert [v.context["rule"] for v in out] == ["R1"]

    def test_succeeded_call_must_be_unchanged(self) -> None:
        v1 = plan(step("a"))
        changed = step("a", arguments={"dataset": "sales", "utterance": "changed"})
        out = check_replan(plan(changed, version=2), v1, {"a": _ok("a")}, None, 1.0)
        assert [v.context["rule"] for v in out] == ["R1"]

    def test_rejected_call_cannot_reappear_unchanged(self) -> None:
        v1 = plan(step("a"))
        v2 = plan(step("a2"), version=2)  # same tool, arguments and fraction
        out = check_replan(v2, v1, {"a": _rejected("a")}, _rejected("a"), 1.0)
        assert [v.context["rule"] for v in out] == ["R2"]

    def test_pending_fractions_bounded_by_remaining_budget(self) -> None:
        v1 = plan(step("a", fraction=0.5))
        v2 = plan(step("a", fraction=0.5), step("b", fraction=0.5), version=2)
        out = check_replan(v2, v1, {"a": _ok("a")}, None, 0.4)
        assert [v.context["rule"] for v in out] == ["R3"]
