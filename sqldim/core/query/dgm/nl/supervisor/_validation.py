"""DGM Supervisor — plan binding and validation (§12.2 P1–P6, §12.5, §12.10).

``bind_plan`` derives what the planner model may not set — each request's
datasets and idempotency key — and ``validate_plan`` checks P1–P6.  An invalid
plan never runs a step (§12.4).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from sqldim.core.query.dgm.nl.supervisor._auth import Action, AuthContext
from sqldim.core.query.dgm.nl.supervisor._schemas import TOOL_SCHEMAS
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    SupervisorPlan,
    ToolCallRequest,
    ToolCallResult,
    ToolError,
    ToolErrorKind,
)

if TYPE_CHECKING:
    from sqldim.core.query.dgm.nl.supervisor._tools import BaseTool

__all__ = [
    "PlanCheck",
    "bind_plan",
    "check_replan",
    "derive_datasets",
    "idempotency_key",
    "required_actions",
    "validate_plan",
]

_BUDGET_EPSILON = 1e-9


class _Unresolvable(ValueError):
    """A ``reads`` pointer or call reference that names no dataset."""


@dataclass(frozen=True)
class PlanCheck:
    """Result of :func:`validate_plan`: the bound plan, or why it is invalid."""

    plan: SupervisorPlan
    violations: tuple[ToolError, ...]

    @property
    def valid(self) -> bool:
        return not self.violations


def _violation(
    kind: ToolErrorKind, rule: str, message: str, **context: Any
) -> ToolError:
    return ToolError(kind=kind, message=message, context={"rule": rule, **context})


# ---------------------------------------------------------------------------
# Derivations
# ---------------------------------------------------------------------------


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def idempotency_key(plan_id: str, request: ToolCallRequest) -> str:
    """``H(plan_id, call_id, tool_name, canonical(arguments))`` (§12.2)."""
    payload = canonical_json(
        [plan_id, request.call_id, request.tool_name, request.arguments]
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _resolve_pointer(document: Any, pointer: str) -> list[Any]:
    """Values at a JSON pointer; ``*`` selects every element of a list."""
    nodes = [document]
    for token in [t for t in pointer.split("/") if t]:
        nxt: list[Any] = []
        for node in nodes:
            if token == "*" and isinstance(node, list):
                nxt.extend(node)
            elif isinstance(node, dict) and token in node:
                nxt.append(node[token])
            else:
                raise _Unresolvable(f"no value at {pointer}")
        nodes = nxt
    return nodes


def derive_datasets(
    request: ToolCallRequest, known: Mapping[str, list[str]]
) -> list[str]:
    """Datasets named at the tool's ``reads`` pointers, plus those inherited
    from the steps its call-id arguments reference (§12.2, §12.10).

    *known* maps an earlier call_id to its datasets.  Raises ``_Unresolvable``
    when a pointer or reference names nothing.
    """
    spec = TOOL_SCHEMAS[request.tool_name]
    found: set[str] = set()
    for pointer in spec.reads:
        for value in _resolve_pointer(request.arguments, pointer):
            if not isinstance(value, str) or not value:
                raise _Unresolvable(f"{pointer} does not name a dataset")
            found.add(value)
    for field in spec.references:
        ref = request.arguments.get(field)
        if not isinstance(ref, str) or ref not in request.depends_on:
            raise _Unresolvable(f"{field} must be the call_id of a dependency")
        if ref not in known:
            raise _Unresolvable(f"{field} references a step whose datasets are unknown")
        found.update(known[ref])
    return sorted(found)


def required_actions(tool_name: str) -> frozenset[Action]:
    """Actions needed on every dataset of a request (§12.10 table)."""
    actions = {Action.READ}
    if tool_name == "dgm_correlate":
        actions.add(Action.CORRELATE)
    if TOOL_SCHEMAS[tool_name].writes_kb:
        actions.add(Action.WRITE_KB)
    return frozenset(actions)


def authorization_error(
    auth: AuthContext, tool_name: str, datasets: list[str]
) -> ToolError | None:
    """UNAUTHORIZED, without saying which dataset failed or whether it exists."""
    from sqldim.core.query.dgm.nl.supervisor._auth import NOT_AUTHORIZED_OR_NOT_FOUND

    needed = required_actions(tool_name)
    if all(auth.allows(ds, needed) for ds in datasets):
        return None
    return ToolError(
        kind=ToolErrorKind.UNAUTHORIZED,
        message=NOT_AUTHORIZED_OR_NOT_FOUND,
        context={"rule": "P6"},
    )


# ---------------------------------------------------------------------------
# P1–P6
# ---------------------------------------------------------------------------


def _check_p1(plan: SupervisorPlan) -> list[ToolError]:
    seen: set[str] = set()
    out: list[ToolError] = []
    for step in plan.steps:
        if step.call_id in seen:
            out.append(
                _violation(
                    ToolErrorKind.INVALID_ARGUMENTS,
                    "P1",
                    f"call_id {step.call_id!r} is not unique",
                    call_id=step.call_id,
                )
            )
        seen.add(step.call_id)
    return out


def _check_p2(plan: SupervisorPlan) -> list[ToolError]:
    earlier: set[str] = set()
    out: list[ToolError] = []
    for step in plan.steps:
        for dep in step.depends_on:
            if dep not in earlier:
                out.append(
                    _violation(
                        ToolErrorKind.INVALID_ARGUMENTS,
                        "P2",
                        f"{step.call_id!r} depends on {dep!r}, which is not an "
                        "earlier step",
                        call_id=step.call_id,
                        depends_on=dep,
                    )
                )
        earlier.add(step.call_id)
    return out


def _check_p3(
    step: ToolCallRequest, registry: Mapping[str, "BaseTool"]
) -> ToolError | None:
    tool = registry.get(step.tool_name)
    if tool is None or step.tool_name not in TOOL_SCHEMAS:
        return _violation(
            ToolErrorKind.UNKNOWN_TOOL,
            "P3",
            f"unknown tool {step.tool_name!r}",
            call_id=step.call_id,
        )
    schema = tool.capability_schema
    if not schema.available:
        return _violation(
            ToolErrorKind.NOT_IMPLEMENTED,
            "P3",
            f"{step.tool_name} is not available in this deployment",
            call_id=step.call_id,
            missing=getattr(tool, "missing", None),
        )
    try:
        TOOL_SCHEMAS[step.tool_name].args.model_validate(step.arguments)
    except ValidationError as exc:
        return _violation(
            ToolErrorKind.INVALID_ARGUMENTS,
            "P3",
            f"arguments of {step.call_id!r} do not match {step.tool_name}",
            call_id=step.call_id,
            errors=[{"loc": list(e["loc"]), "msg": e["msg"]} for e in exc.errors()],
        )
    return None


def _check_p4(
    plan: SupervisorPlan, registry: Mapping[str, "BaseTool"]
) -> list[ToolError]:
    by_id = {s.call_id: s for s in plan.steps}
    out: list[ToolError] = []
    for step in plan.steps:
        for dep in step.depends_on:
            dep_step = by_id.get(dep)
            dep_tool = registry.get(dep_step.tool_name) if dep_step else None
            if dep_tool is None:
                continue  # reported by P2/P3
            if step.tool_name not in dep_tool.capability_schema.composable_with:
                out.append(
                    _violation(
                        ToolErrorKind.INVALID_ARGUMENTS,
                        "P4",
                        f"{step.tool_name} cannot consume the output of "
                        f"{dep_step.tool_name if dep_step else dep}",
                        call_id=step.call_id,
                        depends_on=dep,
                    )
                )
    return out


def _check_p5(plan: SupervisorPlan) -> list[ToolError]:
    total = plan.total_budget_fraction
    if total <= 1.0 + _BUDGET_EPSILON:
        return []
    return [
        _violation(
            ToolErrorKind.INVALID_ARGUMENTS,
            "P5",
            f"budget fractions sum to {total:.6f} > 1",
            total=total,
        )
    ]


def bind_plan(
    plan: SupervisorPlan,
) -> tuple[SupervisorPlan, list[ToolError]]:
    """Derive ``datasets`` and ``idempotency_key`` of every step, in plan order.

    Planner-supplied values for both fields are discarded.
    """
    known: dict[str, list[str]] = {}
    bound: list[ToolCallRequest] = []
    errors: list[ToolError] = []
    for step in plan.steps:
        datasets: list[str] | None = None
        if step.tool_name in TOOL_SCHEMAS:
            try:
                datasets = derive_datasets(step, known)
            except _Unresolvable as exc:
                errors.append(
                    _violation(
                        ToolErrorKind.INVALID_ARGUMENTS,
                        "P3",
                        str(exc),
                        call_id=step.call_id,
                    )
                )
        if datasets is not None:
            known[step.call_id] = datasets
        bound.append(
            step.model_copy(
                update={
                    "datasets": datasets,
                    "idempotency_key": idempotency_key(plan.plan_id, step),
                }
            )
        )
    return plan.model_copy(update={"steps": bound}), errors


def _check_p6(
    bound: SupervisorPlan, auth: AuthContext, skip: set[Any]
) -> list[ToolError]:
    out: list[ToolError] = []
    for step in bound.steps:
        if step.datasets is None or step.call_id in skip:
            continue
        denied = authorization_error(auth, step.tool_name, step.datasets)
        if denied is not None:
            context = {**denied.context, "call_id": step.call_id}
            out.append(denied.model_copy(update={"context": context}))
    return out


def validate_plan(
    plan: SupervisorPlan,
    registry: Mapping[str, "BaseTool"],
    auth: AuthContext,
) -> PlanCheck:
    """Bind *plan* and check P1–P6 against *registry* and *auth* (§12.2)."""
    p3 = [v for v in (_check_p3(s, registry) for s in plan.steps) if v is not None]
    p3_calls = {v.context.get("call_id") for v in p3}
    bound, bind_errors = bind_plan(plan)
    violations = (
        _check_p1(plan)
        + _check_p2(plan)
        + p3
        + _check_p4(plan, registry)
        + _check_p5(plan)
        + [e for e in bind_errors if e.context.get("call_id") not in p3_calls]
        + _check_p6(bound, auth, p3_calls)
    )
    return PlanCheck(plan=bound, violations=tuple(violations))


# ---------------------------------------------------------------------------
# Replan constraints (§12.5)
# ---------------------------------------------------------------------------


def _check_r0(new: SupervisorPlan, previous: SupervisorPlan) -> list[ToolError]:
    if new.plan_id == previous.plan_id and new.version > previous.version:
        return []
    return [
        _violation(
            ToolErrorKind.INVALID_ARGUMENTS,
            "R0",
            "a replan keeps plan_id and increments version",
        )
    ]


def _signature(step: ToolCallRequest) -> tuple[str, Any]:
    return step.tool_name, step.arguments


def _check_r1(
    new: SupervisorPlan, previous: SupervisorPlan, succeeded: set[str]
) -> list[ToolError]:
    """Every SUCCEEDED call is kept unchanged with its call_id."""
    kept = {s.call_id: s for s in new.steps}
    return [
        _violation(
            ToolErrorKind.INVALID_ARGUMENTS,
            "R1",
            f"SUCCEEDED call {call_id!r} must be kept unchanged",
            call_id=call_id,
        )
        for call_id in sorted(succeeded)
        if call_id not in kept
        or _signature(kept[call_id]) != _signature(previous.step(call_id))
    ]


def _check_r2(
    new: SupervisorPlan,
    previous: SupervisorPlan,
    rejected: ToolCallResult | None,
    succeeded: set[str],
) -> list[ToolError]:
    """The rejected call does not reappear with the same tool, arguments, fraction."""
    if rejected is None:
        return []
    old = previous.step(rejected.call_id)
    same = (*_signature(old), old.budget_fraction)
    return [
        _violation(
            ToolErrorKind.INVALID_ARGUMENTS,
            "R2",
            "the rejected call reappears unchanged",
            call_id=step.call_id,
        )
        for step in new.steps
        if step.call_id not in succeeded  # kept results are reused, not re-run
        and (*_signature(step), step.budget_fraction) == same
    ]


def _check_r3(
    new: SupervisorPlan, succeeded: set[str], remaining_fraction: float
) -> list[ToolError]:
    pending = sum(s.budget_fraction for s in new.steps if s.call_id not in succeeded)
    if pending <= remaining_fraction + _BUDGET_EPSILON:
        return []
    return [
        _violation(
            ToolErrorKind.INVALID_ARGUMENTS,
            "R3",
            f"new and pending steps need {pending:.6f} of the budget; "
            f"{remaining_fraction:.6f} remains",
        )
    ]


def check_replan(
    new: SupervisorPlan,
    previous: SupervisorPlan,
    results: Mapping[str, ToolCallResult],
    rejected: ToolCallResult | None,
    remaining_fraction: float,
) -> list[ToolError]:
    """Constraints a replan must meet besides P1–P6 (§12.5)."""
    succeeded = {cid for cid, r in results.items() if r.status is CallStatus.SUCCEEDED}
    return (
        _check_r0(new, previous)
        + _check_r1(new, previous, succeeded)
        + _check_r2(new, previous, rejected, succeeded)
        + _check_r3(new, succeeded, remaining_fraction)
    )
