"""§12.7 knowledge base: tenant partition, idempotent store, move-distance
retrieval (ADR 0007) and dataset authorization (§12.10, D44)."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Iterable, Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from sqldim.core.query.dgm.nl.supervisor._auth import Action
from sqldim.core.query.dgm.nl.supervisor._dispatch import execute_tool
from sqldim.core.query.dgm.nl.supervisor._graph import make_runtime, run_supervisor
from sqldim.core.query.dgm.nl.supervisor._kb import (
    FindingConflict,
    SQLiteKnowledgeBase,
    TenantKnowledgeBase,
    finding_id_for,
    tenant_handle,
)
from sqldim.core.query.dgm.nl.supervisor._schemas import RelatedFinding
from sqldim.core.query.dgm.nl.supervisor._tools import TOOL_REGISTRY, build_registry
from sqldim.core.query.dgm.nl.supervisor._tools_kb import (
    MISSING_KB,
    MISSING_MOVE_GRAPH,
    MoveGraphError,
    RetrieveRelatedTool,
    StoreFindingTool,
    d_P,
    kb_tools,
    within_distance,
)
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    Outcome,
    ToolCallRequest,
    ToolCallResult,
    ToolContext,
    ToolErrorKind,
)
from sqldim.core.query.dgm.nl.supervisor._validation import bind_plan
from tests.supervisor._fakes import (
    Opener,
    ScriptedPlanner,
    auth,
    budget,
    plan,
    runtime,
    step,
    tool_context,
)

ALL = {Action.READ, Action.WRITE_KB}


class Clock:
    """Deterministic, strictly increasing timestamps."""

    def __init__(self) -> None:
        self._t = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        self._t += timedelta(seconds=1)
        return self._t


class Moves:
    """Declared undirected move graph for tests (vertices = question keys).

    ADR 0007 example: ``rr`` = revenue by region Where segment='retail';
    ``r`` = revenue by region; ``cr`` = revenue by country Where retail;
    ``c`` = revenue by country; ``oc`` = order count by country.
    ``x`` / ``y`` form a separate component (another anchor set).
    """

    EDGES = [("rr", "r"), ("rr", "cr"), ("cr", "c"), ("r", "c"), ("c", "oc")]
    OTHER = [("x", "y")]

    def __init__(self, fail_on: str | None = None) -> None:
        self.adj: dict[str, set[str]] = {}
        for a, b in self.EDGES + self.OTHER:
            self.adj.setdefault(a, set()).add(b)
            self.adj.setdefault(b, set()).add(a)
        self.fail_on = fail_on
        self.expanded: list[str] = []

    def contains(self, dataset: str, question_key: str) -> bool:
        return dataset == "sales" and question_key in self.adj

    def neighbours(self, dataset: str, question_key: str) -> Iterable[str]:
        if question_key == self.fail_on:
            raise MoveGraphError("move generator failed")
        self.expanded.append(question_key)
        return self.adj[question_key]


@pytest.fixture
def kb() -> Iterator[SQLiteKnowledgeBase]:
    base = SQLiteKnowledgeBase(clock=Clock())
    yield base
    base.close()


def _ctx(base: Any, grants: dict[str, set[Action]], tenant: str = "t1") -> ToolContext:
    a = auth(grants, tenant=tenant)
    return dataclasses.replace(tool_context(a), kb=tenant_handle(base, tenant))


def _store_step(call_id: str, qk: str, **finding: Any) -> ToolCallRequest:
    body = {
        "question_key": qk,
        "parameters": finding.pop("parameters", {}),
        "datasets": finding.pop("datasets", ["sales"]),
        "content": finding.pop("content", f"finding at {qk}"),
    }
    return step(call_id, "dgm_store_finding", {"finding": body})


def _retrieve_step(qk: str, k: int = 2, **params: Any) -> ToolCallRequest:
    return step(
        "r",
        "dgm_retrieve_related",
        {"dataset": "sales", "question_key": qk, "parameters": params, "k": k},
    )


def _bound(request: ToolCallRequest, plan_id: str = "p1") -> ToolCallRequest:
    bound, errors = bind_plan(plan(request, plan_id=plan_id))
    assert errors == []
    return bound.steps[0]


def _run(request: ToolCallRequest, reg: Any, ctx: ToolContext) -> ToolCallResult:
    return asyncio.run(execute_tool(_bound(request), reg, ctx))


def _store(base: Any, call_id: str, qk: str, tenant: str = "t1", **finding: Any) -> str:
    datasets = finding.get("datasets", ["sales"])
    ctx = _ctx(base, {ds: ALL for ds in datasets}, tenant)
    r = _run(_store_step(call_id, qk, **finding), build_registry(kb=base), ctx)
    assert r.status is CallStatus.SUCCEEDED, r.error
    return str(r.data["finding_id"])


def _retrieve(
    base: Any,
    request: ToolCallRequest,
    grants: dict[str, set[Action]] | None = None,
    tenant: str = "t1",
    moves: Any = None,
) -> ToolCallResult:
    reg = build_registry(kb=base, moves=moves or Moves())
    return _run(request, reg, _ctx(base, grants or {"sales": {Action.READ}}, tenant))


# ---------------------------------------------------------------------------
# Backend
# ---------------------------------------------------------------------------


class TestBackend:
    def test_store_persists_every_field(self, kb: SQLiteKnowledgeBase) -> None:
        row = kb.store(
            "t1",
            "key-1",
            question_key="rr",
            parameters={"segment": "retail"},
            datasets=["sales", "crm"],
            content="retail revenue grew",
        )
        assert row.finding_id == finding_id_for("t1", "key-1")
        assert row.idempotency_key == "key-1"
        assert row.datasets == ("crm", "sales")
        assert row.parameters == {"segment": "retail"}
        assert row.stored_at == datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
        assert kb.findings_at("t1", ["rr"]) == [row]

    def test_same_key_stores_once(self, kb: SQLiteKnowledgeBase) -> None:
        args = dict(question_key="q", parameters={}, datasets=["s"], content="c")
        first = kb.store("t1", "k", **args)  # type: ignore[arg-type]
        again = kb.store("t1", "k", **args)  # type: ignore[arg-type]
        assert again == first
        assert len(kb.findings_at("t1", ["q"])) == 1

    def test_same_key_different_finding_is_a_conflict(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        kb.store(
            "t1", "k", question_key="q", parameters={}, datasets=["s"], content="a"
        )
        with pytest.raises(FindingConflict):
            kb.store(
                "t1", "k", question_key="q", parameters={}, datasets=["s"], content="b"
            )
        assert [f.content for f in kb.findings_at("t1", ["q"])] == ["a"]

    def test_tenants_are_partitioned(self, kb: SQLiteKnowledgeBase) -> None:
        args = dict(question_key="q", parameters={}, datasets=["s"], content="c")
        a = kb.store("t1", "k", **args)  # type: ignore[arg-type]
        b = kb.store("t2", "k", **args)  # type: ignore[arg-type]
        assert a.finding_id != b.finding_id
        assert kb.findings_at("t1", ["q"]) == [a]
        assert kb.for_tenant("t2").findings_at(["q"]) == [b]

    def test_survives_process_recreation(self, tmp_path: Path) -> None:
        path = tmp_path / "kb.sqlite"
        first = SQLiteKnowledgeBase(path)
        stored = first.store(
            "t1",
            "k",
            question_key="q",
            parameters={"p": 1},
            datasets=["s"],
            content="c",
        )
        first.close()
        reopened = SQLiteKnowledgeBase(path)
        try:
            assert reopened.findings_at("t1", ["q"]) == [stored]
            again = reopened.store(
                "t1",
                "k",
                question_key="q",
                parameters={"p": 1},
                datasets=["s"],
                content="c",
            )
            assert again == stored
            assert len(reopened.findings_at("t1", ["q"])) == 1
        finally:
            reopened.close()

    def test_tenant_handle(self, kb: SQLiteKnowledgeBase) -> None:
        assert tenant_handle(None, "t1") is None
        handle = tenant_handle(kb, "t1")
        assert isinstance(handle, TenantKnowledgeBase) and handle.tenant_id == "t1"
        with pytest.raises(TypeError):
            tenant_handle(object(), "t1")


# ---------------------------------------------------------------------------
# Availability
# ---------------------------------------------------------------------------


class TestAvailability:
    @pytest.mark.parametrize("name", ["dgm_store_finding", "dgm_retrieve_related"])
    def test_no_kb_configured_means_unavailable(self, name: str) -> None:
        assert TOOL_REGISTRY[name].capability_schema.available is False
        assert "knowledge base" in TOOL_REGISTRY[name].missing  # type: ignore[attr-defined]

    def test_kb_without_move_graph_keeps_retrieval_unavailable(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        reg = build_registry(kb=kb)
        assert reg["dgm_store_finding"].capability_schema.available is True
        retrieve = reg["dgm_retrieve_related"]
        assert retrieve.capability_schema.available is False
        assert retrieve.missing == MISSING_MOVE_GRAPH  # type: ignore[attr-defined]
        r = _run(_retrieve_step("rr"), reg, _ctx(kb, {"sales": {Action.READ}}))
        assert r.error.kind is ToolErrorKind.NOT_IMPLEMENTED
        assert r.error.context["missing"] == MISSING_MOVE_GRAPH

    def test_kb_and_move_graph_make_both_available(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        reg = build_registry(kb=kb, moves=Moves())
        assert reg["dgm_retrieve_related"].capability_schema.available is True
        others = [
            n
            for n, t in reg.items()
            if t.capability_schema.available
            and n
            not in {
                "dgm_store_finding",
                "dgm_retrieve_related",
                "dgm_validate_invariant",  # needs no KB (#134)
                "dgm_map_analytical_space",  # needs no KB (#132)
            }
        ]
        assert others == []

    def test_moves_must_be_a_move_graph(self) -> None:
        with pytest.raises(TypeError):
            kb_tools(object())

    def test_make_runtime_wires_and_scopes_the_kb(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        rt = make_runtime(auth(tenant="t9"), Opener(), budget(), kb=kb)
        names = [s.name for s in rt.available_tools()]
        assert names == [
            "dgm_map_analytical_space",
            "dgm_store_finding",
            "dgm_validate_invariant",
        ]
        handle = rt.tool_context().kb
        assert isinstance(handle, TenantKnowledgeBase) and handle.tenant_id == "t9"
        assert make_runtime(auth(), Opener(), budget()).tool_context().kb is None

    def test_tool_without_kb_in_context_fails_closed(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        ctx = tool_context(auth({"sales": ALL}))
        r = _run(_store_step("s", "rr"), build_registry(kb=kb), ctx)
        assert r.status is CallStatus.FAILED
        assert r.error.kind is ToolErrorKind.NOT_IMPLEMENTED
        assert r.error.context["missing"] == MISSING_KB

    def test_unscoped_kb_in_context_fails_closed(self, kb: SQLiteKnowledgeBase) -> None:
        ctx = dataclasses.replace(tool_context(auth({"sales": ALL})), kb=kb)
        r = _run(_store_step("s", "rr"), build_registry(kb=kb), ctx)
        assert r.error.kind is ToolErrorKind.TOOL_FAILURE
        assert kb.findings_at("t1", ["rr"]) == []


# ---------------------------------------------------------------------------
# dgm_store_finding
# ---------------------------------------------------------------------------


class TestStoreFinding:
    def test_returns_the_real_stored_id(self, kb: SQLiteKnowledgeBase) -> None:
        request = _bound(_store_step("s", "rr", parameters={"segment": "retail"}))
        ctx = _ctx(kb, {"sales": ALL})
        r = asyncio.run(execute_tool(request, build_registry(kb=kb), ctx))
        assert r.status is CallStatus.SUCCEEDED and not r.demo
        [stored] = kb.findings_at("t1", ["rr"])
        assert r.data == {"finding_id": stored.finding_id}
        assert stored.finding_id == finding_id_for("t1", request.idempotency_key)
        assert stored.idempotency_key == request.idempotency_key

    def test_retry_with_same_key_does_not_duplicate(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        request = _bound(_store_step("s", "rr"))
        reg, ctx = build_registry(kb=kb), _ctx(kb, {"sales": ALL})
        first = asyncio.run(execute_tool(request, reg, ctx))
        retry = asyncio.run(execute_tool(request, reg, ctx, attempt=2))
        assert retry.status is CallStatus.SUCCEEDED
        assert retry.data == first.data
        assert len(kb.findings_at("t1", ["rr"])) == 1

    def test_missing_write_kb_is_unauthorized_and_stores_nothing(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        ctx = _ctx(kb, {"sales": {Action.READ}})
        r = _run(_store_step("s", "rr"), build_registry(kb=kb), ctx)
        assert r.error.kind is ToolErrorKind.UNAUTHORIZED
        assert kb.findings_at("t1", ["rr"]) == []

    def test_missing_read_on_one_source_dataset_is_unauthorized(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        ctx = _ctx(kb, {"sales": ALL, "crm": {Action.WRITE_KB}})
        request = _store_step("s", "rr", datasets=["sales", "crm"])
        r = _run(request, build_registry(kb=kb), ctx)
        assert r.error.kind is ToolErrorKind.UNAUTHORIZED
        assert kb.findings_at("t1", ["rr"]) == []

    def test_tool_rechecks_grants_when_called_directly(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        request = _bound(_store_step("s", "rr"))
        ctx = _ctx(kb, {"sales": {Action.READ}})
        r = asyncio.run(StoreFindingTool().execute(request, ctx))
        assert r.error.kind is ToolErrorKind.UNAUTHORIZED
        assert kb.findings_at("t1", ["rr"]) == []

    def test_handle_of_another_tenant_is_refused(self, kb: SQLiteKnowledgeBase) -> None:
        ctx = dataclasses.replace(
            _ctx(kb, {"sales": ALL}, tenant="t1"), kb=kb.for_tenant("t2")
        )
        r = _run(_store_step("s", "rr"), build_registry(kb=kb), ctx)
        assert r.error.kind is ToolErrorKind.UNAUTHORIZED
        assert kb.findings_at("t2", ["rr"]) == []

    def test_unbound_request_has_no_key_and_is_refused(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        request = _store_step("s", "rr")
        r = asyncio.run(StoreFindingTool().execute(request, _ctx(kb, {"sales": ALL})))
        assert r.error.kind is ToolErrorKind.INVALID_ARGUMENTS
        assert kb.findings_at("t1", ["rr"]) == []

    def test_backend_failure_is_a_retryable_failure(self, tmp_path: Path) -> None:
        base = SQLiteKnowledgeBase(tmp_path / "kb.sqlite")
        base._con.execute("DROP TABLE findings")  # noqa: SLF001
        r = _run(
            _store_step("s", "rr"), build_registry(kb=base), _ctx(base, {"sales": ALL})
        )
        base.close()
        assert r.status is CallStatus.FAILED
        assert r.error.kind is ToolErrorKind.TOOL_FAILURE and r.error.retryable


# ---------------------------------------------------------------------------
# dgm_retrieve_related
# ---------------------------------------------------------------------------


class TestDistance:
    def test_bfs_gives_shortest_paths_within_k(self) -> None:
        moves = Moves()
        assert within_distance(moves, "sales", "rr", 3) == {
            "rr": 0, "r": 1, "cr": 1, "c": 2, "oc": 3,
        }  # fmt: skip
        assert within_distance(moves, "sales", "rr", 1) == {"rr": 0, "r": 1, "cr": 1}

    def test_bfs_never_expands_past_depth_k(self) -> None:
        moves = Moves()
        within_distance(moves, "sales", "rr", 1)
        assert moves.expanded == ["rr"]
        moves.expanded.clear()
        within_distance(moves, "sales", "rr", 0)
        assert moves.expanded == []

    def test_other_component_is_at_infinite_distance(self) -> None:
        assert "x" not in within_distance(Moves(), "sales", "rr", 10)

    def test_d_p_counts_differing_slots(self) -> None:
        assert d_P({"a": 1, "b": 2}, {"a": 1, "b": 2}) == 0
        assert d_P({"a": 1, "b": 2}, {"a": 1, "b": 3}) == 1
        assert d_P({"a": 1}, {"b": 1}) == 2

    def test_related_finding_has_d_p_only_at_origin(self) -> None:
        RelatedFinding(finding_id="f", d_S=0, d_P=0)
        RelatedFinding(finding_id="f", d_S=1, d_P=None)
        with pytest.raises(ValidationError):
            RelatedFinding(finding_id="f", d_S=1, d_P=0)
        with pytest.raises(ValidationError):
            RelatedFinding(finding_id="f", d_S=0)


class TestRetrieveRelated:
    def test_adr_example_distances_and_radius(self, kb: SQLiteKnowledgeBase) -> None:
        f_r = _store(kb, "a", "r")
        f_cr = _store(kb, "b", "cr")
        f_oc = _store(kb, "c", "oc")
        r = _retrieve(kb, _retrieve_step("rr", k=3))
        assert r.status is CallStatus.SUCCEEDED
        got = [(f["finding_id"], f["d_S"]) for f in r.data["findings"]]
        # d_S ties are broken by recency: cr was stored after r.
        assert got == [(f_cr, 1), (f_r, 1), (f_oc, 3)]
        within_two = _retrieve(kb, _retrieve_step("rr", k=2)).data["findings"]
        assert [f["finding_id"] for f in within_two] == [f_cr, f_r]

    def test_order_d_s_then_d_p_then_recency_then_id(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        far = _store(kb, "a", "r")
        exact = _store(kb, "b", "rr", parameters={"segment": "retail"})
        off_by_one = _store(kb, "c", "rr", parameters={"segment": "online"})
        newer_exact = _store(kb, "d", "rr", parameters={"segment": "retail"})
        r = _retrieve(kb, _retrieve_step("rr", segment="retail"))
        assert r.data["findings"] == [
            {"finding_id": newer_exact, "d_S": 0, "d_P": 0},
            {"finding_id": exact, "d_S": 0, "d_P": 0},
            {"finding_id": off_by_one, "d_S": 0, "d_P": 1},
            {"finding_id": far, "d_S": 1, "d_P": None},
        ]

    def test_equal_timestamps_are_ordered_by_id(self) -> None:
        fixed = datetime(2026, 1, 1, tzinfo=timezone.utc)
        base = SQLiteKnowledgeBase(clock=lambda: fixed)
        try:
            ids = [_store(base, c, "r") for c in ("a", "b", "c")]
            r = _retrieve(base, _retrieve_step("rr"))
            assert [f["finding_id"] for f in r.data["findings"]] == sorted(ids)
        finally:
            base.close()

    def test_other_anchor_set_is_never_retrieved(self, kb: SQLiteKnowledgeBase) -> None:
        _store(kb, "a", "x")
        r = _retrieve(kb, _retrieve_step("rr", k=50))
        assert r.status is CallStatus.SUCCEEDED and r.data["findings"] == []

    def test_findings_with_an_ungranted_dataset_are_never_returned(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        visible = _store(kb, "a", "r", datasets=["sales"])
        _store(kb, "b", "r", datasets=["sales", "hr"])
        r = _retrieve(kb, _retrieve_step("rr"), grants={"sales": {Action.READ}})
        assert [f["finding_id"] for f in r.data["findings"]] == [visible]
        both = _retrieve(
            kb,
            _retrieve_step("rr"),
            grants={"sales": {Action.READ}, "hr": {Action.READ}},
        )
        assert len(both.data["findings"]) == 2

    def test_write_kb_alone_does_not_grant_retrieval(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        _store(kb, "a", "r", datasets=["sales", "hr"])
        r = _retrieve(
            kb,
            _retrieve_step("rr"),
            grants={"sales": {Action.READ}, "hr": {Action.WRITE_KB}},
        )
        assert r.data["findings"] == []

    def test_another_tenant_never_sees_the_findings(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        _store(kb, "a", "r", tenant="t1")
        other = _retrieve(kb, _retrieve_step("rr"), tenant="t2")
        assert other.status is CallStatus.SUCCEEDED and other.data["findings"] == []
        own = _retrieve(kb, _retrieve_step("rr"), tenant="t1")
        assert len(own.data["findings"]) == 1

    def test_empty_retrieval_is_a_success(self, kb: SQLiteKnowledgeBase) -> None:
        r = _retrieve(kb, _retrieve_step("rr"))
        assert r.status is CallStatus.SUCCEEDED
        assert r.data == {"findings": []}

    def test_backend_failure_is_a_failure_not_an_empty_result(
        self, tmp_path: Path
    ) -> None:
        base = SQLiteKnowledgeBase(tmp_path / "kb.sqlite")
        base._con.execute("DROP TABLE findings")  # noqa: SLF001
        r = _retrieve(base, _retrieve_step("rr"))
        base.close()
        assert r.status is CallStatus.FAILED and r.data is None
        assert r.error.kind is ToolErrorKind.TOOL_FAILURE and r.error.retryable

    def test_move_graph_failure_is_a_failure(self, kb: SQLiteKnowledgeBase) -> None:
        _store(kb, "a", "r")
        r = _retrieve(kb, _retrieve_step("rr"), moves=Moves(fail_on="r"))
        assert r.status is CallStatus.FAILED
        assert r.error.kind is ToolErrorKind.TOOL_FAILURE

    def test_unknown_question_key_is_invalid(self, kb: SQLiteKnowledgeBase) -> None:
        r = _retrieve(kb, _retrieve_step("not-a-skeleton"))
        assert r.error.kind is ToolErrorKind.INVALID_ARGUMENTS

    def test_missing_read_on_dataset_is_unauthorized(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        r = _retrieve(kb, _retrieve_step("rr"), grants={"crm": {Action.READ}})
        assert r.error.kind is ToolErrorKind.UNAUTHORIZED

    def test_tool_rechecks_dataset_grant_when_called_directly(
        self, kb: SQLiteKnowledgeBase
    ) -> None:
        tool = RetrieveRelatedTool(Moves())
        ctx = _ctx(kb, {"crm": {Action.READ}})
        r = asyncio.run(tool.execute(_bound(_retrieve_step("rr")), ctx))
        assert r.error.kind is ToolErrorKind.UNAUTHORIZED


# ---------------------------------------------------------------------------
# Through the supervisor
# ---------------------------------------------------------------------------


class TestSupervisorRun:
    def test_store_then_retrieve_in_one_plan(self, kb: SQLiteKnowledgeBase) -> None:
        store = _store_step("s", "r")
        retrieve = ToolCallRequest(
            **{**_retrieve_step("rr").model_dump(), "depends_on": ["s"]}
        )
        planner = ScriptedPlanner(
            lambda view: plan(
                store, retrieve, plan_id=view.plan_id, version=view.version
            )
        )
        rt = runtime(
            build_registry(kb=kb, moves=Moves()),
            planner,
            the_auth=auth({"sales": ALL}),
            kb=kb,
        )
        state = run_supervisor("remember and recall", rt)
        assert state.outcome is Outcome.COMPLETE, state
        stored_id = state.results["s"].data["finding_id"]
        assert state.results["r"].data["findings"] == [
            {"finding_id": stored_id, "d_S": 1, "d_P": None}
        ]
