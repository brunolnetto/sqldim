"""The plan executor of §11.10: ``run`` delivered plans, ``submit`` ASYNC ones.

:class:`PlanExecutor` is an entry point in the sense of §12.10 (D44): it opens
data by identifier — the relations of ``deps(plan)`` — so it is built with an
:class:`AuthContext` and checks the grants on every call before any data is
touched.

* ``run(plan, mode)`` executes a plan under a delivery: ``PROCEED`` (the
  whole result), ``STREAM`` (batches as they are fetched) or ``PAGINATE``
  (pages of the exact result).  A ``REWRITE`` or ``SAMPLE`` decision runs its
  own plan under its ``then`` and carries its warning.  ``ASYNC`` is never run
  synchronously, and ``CLARIFY``/``REJECT`` execute nothing.
* ``submit(...)`` hands an ASYNC plan to a host-configured job sink and
  returns its :class:`JobTicket`.  It is idempotent on the ``result_key``
  (§8.14, §11.9): a second submission of the same versioned key for the same
  tenant returns the first ticket.  A key with an unversioned dependency is
  never reused (D41).  There is no default sink; an unknown sink, a refused
  enqueue or a malformed ticket is :class:`SubmissionFailed` or
  :class:`NotConfigured`, never a synchronous run.

Authorization: ``READ`` on the dataset of every relation in ``deps(plan)``;
an ASYNC job materialises its result in the sink (§6.3 Strategy 4), so
``submit`` also needs ``WRITE`` on the sink's dataset.  A PipelineArtifact
write (§2.4, D46) is a typed ``ArtifactPlan`` executed by ``materialise``
through the host's :class:`ArtifactRuntime`; ``run`` refuses an
``ExportPlan`` that carries a sink target or a text write plan.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol

from sqldim.core.query.dgm.nl.supervisor._auth import Action, AuthContext
from sqldim.core.query.dgm.planner._gate import (
    AsyncDecision,
    BudgetDecision,
    ClarifyDecision,
    PaginateDecision,
    ProceedDecision,
    RejectDecision,
    RewriteDecision,
    SampleDecision,
    StreamDecision,
)
from sqldim.core.query.dgm.planner._ir import PlanIdentityUnavailable
from sqldim.core.query.dgm.planner._ir_identity import RelationCatalog, dag_hash
from sqldim.core.query.dgm.planner._targets import ExportPlan, QueryTarget, SinkTarget
from sqldim.core.query.dgm.runtime._dag import ResultKey
from sqldim.core.query.dgm.runtime._errors import (
    InvalidRuntimeArgument,
    NotConfigured,
    NotExecutable,
    NotImplementedCapability,
    SubmissionFailed,
    Unauthorized,
    UnknownIdentifier,
    UnsupportedOperation,
)

if TYPE_CHECKING:
    from sqldim.core.query.dgm.runtime._artifact import (
        ArtifactOutcome,
        ArtifactRuntime,
    )
    from sqldim.core.query.dgm.runtime._artifact_plan import ArtifactPlan

__all__ = [
    "DatasetResolver",
    "StaticDatasets",
    "Cursor",
    "QueryBackend",
    "DuckDBBackend",
    "JobRequest",
    "JobTicket",
    "JobSink",
    "ExecutionResult",
    "PlanExecutor",
]


# ---------------------------------------------------------------------------
# Host-supplied dependencies
# ---------------------------------------------------------------------------


class DatasetResolver(Protocol):
    def dataset_of(self, relation: str) -> str | None:
        """The dataset that holds base relation *relation*; ``None`` if unknown."""
        ...


@dataclass(frozen=True)
class StaticDatasets:
    """A :class:`DatasetResolver` from an explicit ``relation → dataset`` map."""

    by_relation: Mapping[str, str]

    def dataset_of(self, relation: str) -> str | None:
        return self.by_relation.get(relation)


class Cursor(Protocol):
    @property
    def description(self) -> Any: ...

    def fetchall(self) -> list[tuple]: ...

    def fetchmany(self, size: int) -> list[tuple]: ...


class QueryBackend(Protocol):
    target: QueryTarget

    def execute(self, sql: str, datasets: frozenset[str]) -> Cursor:
        """Run *sql* over a connection that holds exactly *datasets*."""
        ...

    def interrupt(self) -> None:
        """Cancel the statement in progress (§11.10 *Sync and async contract*)."""
        ...


class DuckDBBackend:
    """A :class:`QueryBackend` over one DuckDB connection the host opened.

    The connection must already hold the datasets; the executor has checked
    the grants for them before :meth:`execute` is called.
    """

    target = QueryTarget.SQL_DUCKDB

    def __init__(self, connection: Any) -> None:
        if connection is None:
            raise NotConfigured("DuckDBBackend needs a connection")
        self._con = connection
        self._active: Any = None

    def execute(self, sql: str, datasets: frozenset[str]) -> Cursor:
        cur = self._con.cursor()
        self._active = cur
        cur.execute(sql)
        return cur

    def interrupt(self) -> None:
        if self._active is not None:
            self._active.interrupt()


@dataclass(frozen=True)
class JobTicket:
    """What the executor accepted (§11.10 ``JobTicket``)."""

    job_id: str
    status: str


@dataclass(frozen=True)
class JobRequest:
    """One ASYNC job as handed to a :class:`JobSink`."""

    query_text: str
    query_target: QueryTarget
    datasets: frozenset[str]
    sink_dataset: str
    callback: str
    ttl: timedelta
    idempotency_key: ResultKey
    principal_id: str
    tenant_id: str


class JobSink(Protocol):
    """A host-configured destination for ASYNC jobs (§6.3 Strategy 4)."""

    dataset_id: str  # where the job materialises its result

    async def enqueue(self, request: JobRequest) -> JobTicket: ...


@dataclass
class ExecutionResult:
    """A delivered result.

    ``PROCEED`` fills ``rows``; ``STREAM`` and ``PAGINATE`` fill ``chunks``,
    an iterator of row lists of at most ``chunk_size`` rows that together are
    the exact result.  ``warning`` is the substitution's note, if any.
    """

    mode: str
    columns: tuple[str, ...]
    datasets: frozenset[str]
    rows: list[tuple] | None = None
    chunks: Iterator[list[tuple]] | None = None
    chunk_size: int | None = None
    warning: str | None = None


# ---------------------------------------------------------------------------
# Delivery resolution
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Delivery:
    plan: ExportPlan
    mode: str
    page_size: int | None = None
    warning: str | None = None


def _positive(value: object, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidRuntimeArgument(f"{what} must be a positive int, got {value!r}")
    return value


def _sample_warning(d: SampleDecision) -> str:
    return (
        f"Estimated (±{d.error_bound * 100:.1f}% of range at "
        f"{d.confidence * 100:.0f}% confidence)"
    )


def _from_string(plan: ExportPlan, mode: str, page_size: int | None) -> _Delivery:
    if mode in ("PROCEED", "STREAM"):
        if page_size is not None:
            raise InvalidRuntimeArgument(
                f"page_size applies only to PAGINATE, not {mode}"
            )
        return _Delivery(plan, mode)
    if mode == "PAGINATE":
        if page_size is None:
            raise InvalidRuntimeArgument("PAGINATE needs the page_size of the decision")
        return _Delivery(plan, mode, _positive(page_size, "page_size"))
    if mode == "ASYNC":
        raise NotExecutable("ASYNC is submitted as a job, never run synchronously")
    if mode in ("REWRITE", "SAMPLE"):
        raise NotExecutable(
            f"{mode} runs under its `then`; pass that delivery or the decision itself"
        )
    raise NotExecutable(f"{mode!r} is not a delivery mode")


def _then(d: RewriteDecision | SampleDecision, plan: ExportPlan | None) -> _Delivery:
    if plan is not None and plan != d.plan:
        raise InvalidRuntimeArgument(f"the plan is not the plan of the {d.kind.value}")
    warning = d.warning if isinstance(d, RewriteDecision) else _sample_warning(d)
    inner = _from_decision(d.plan, d.then, None)
    return _Delivery(inner.plan, inner.mode, inner.page_size, warning)


def _from_decision(
    plan: ExportPlan | None, d: BudgetDecision, page_size: int | None
) -> _Delivery:
    if isinstance(d, (RewriteDecision, SampleDecision)):
        return _then(d, plan)
    if isinstance(d, AsyncDecision):
        raise NotExecutable("ASYNC is submitted as a job, never run synchronously")
    if isinstance(d, (ClarifyDecision, RejectDecision)):
        raise NotExecutable(f"{d.kind.value} executes nothing")
    if plan is None:
        raise InvalidRuntimeArgument(f"{d.kind.value} needs the plan to run")
    if isinstance(d, PaginateDecision):
        if page_size is not None and page_size != d.page_size:
            raise InvalidRuntimeArgument("page_size disagrees with the decision")
        return _Delivery(plan, "PAGINATE", _positive(d.page_size, "page_size"))
    if isinstance(d, (ProceedDecision, StreamDecision)):
        return _from_string(plan, d.kind.value, page_size)
    raise NotExecutable(f"{type(d).__name__} is not a delivery")


def _resolve_delivery(
    plan: ExportPlan | None, mode: object, page_size: int | None
) -> _Delivery:
    if isinstance(mode, BudgetDecision):
        return _from_decision(plan, mode, page_size)
    if not isinstance(mode, str):
        raise InvalidRuntimeArgument(f"mode is a str or a BudgetDecision, got {mode!r}")
    if plan is None:
        raise InvalidRuntimeArgument(f"{mode} needs the plan to run")
    return _from_string(plan, mode, page_size)


def _chunks(cur: Cursor, size: int) -> Iterator[list[tuple]]:
    while True:
        rows = cur.fetchmany(size)
        if not rows:
            return
        yield rows


# ---------------------------------------------------------------------------
# PlanExecutor
# ---------------------------------------------------------------------------


class PlanExecutor:
    """``executor`` of §11.10, bound to one principal's :class:`AuthContext`."""

    def __init__(
        self,
        *,
        auth: AuthContext,
        datasets: DatasetResolver,
        catalog: RelationCatalog,
        backend: QueryBackend,
        job_sinks: Mapping[str, JobSink],
        stream_batch_size: int,
        artifacts: ArtifactRuntime | None = None,
    ) -> None:
        if not isinstance(auth, AuthContext):
            raise Unauthorized()
        for name, dep in (
            ("datasets", datasets),
            ("catalog", catalog),
            ("backend", backend),
            ("job_sinks", job_sinks),
        ):
            if dep is None:
                raise NotConfigured(f"PlanExecutor needs {name}")
        self._auth = auth
        self._datasets = datasets
        self._catalog = catalog
        self._backend = backend
        self._sinks = dict(job_sinks)
        self._batch = _positive(stream_batch_size, "stream_batch_size")
        self._tickets: dict[tuple[str, str], JobTicket] = {}
        self._lock = asyncio.Lock()
        if artifacts is not None and artifacts.auth != auth:
            raise NotConfigured("the ArtifactRuntime acts for another principal")
        self._artifacts = artifacts

    # -- checks --------------------------------------------------------------

    def _plan_datasets(self, plan: ExportPlan) -> frozenset[str]:
        out = set()
        for dep in plan.deps(self._catalog):  # no IR → PlanIdentityUnavailable
            ds = self._datasets.dataset_of(dep.relation)
            if not ds:
                raise UnknownIdentifier(
                    f"relation {dep.relation!r} belongs to no known dataset"
                )
            out.add(ds)
        return frozenset(out)

    def _require(self, datasets: frozenset[str], action: Action) -> None:
        if not all(self._auth.allows(ds, {action}) for ds in datasets):
            raise Unauthorized()

    # -- result-cache hook (§8.14, D44; see ``_cache``) -----------------------

    @property
    def tenant_id(self) -> str:
        return self._auth.tenant_id

    def authorize_read(
        self, plan: ExportPlan, also: frozenset[str] = frozenset()
    ) -> frozenset[str]:
        """``READ`` on the datasets of ``deps(plan)`` and on *also*; the plan's datasets."""
        datasets = self._plan_datasets(plan)
        self._require(datasets | also, Action.READ)
        return datasets

    def _admit(self, plan: ExportPlan) -> frozenset[str]:
        """Authorize *plan* for READ and refuse what this executor cannot run."""
        if not isinstance(plan, ExportPlan):
            raise InvalidRuntimeArgument(f"expected an ExportPlan, got {plan!r}")
        if plan.ir is None:
            raise PlanIdentityUnavailable(
                "the plan has no IR, so its datasets cannot be authorized"
            )
        datasets = self._plan_datasets(plan)
        self._require(datasets, Action.READ)
        if plan.write_plan is not None or plan.sink_target is not None:
            raise InvalidRuntimeArgument(
                "the plan writes to a sink; a PipelineArtifact write is an "
                "ArtifactPlan executed by materialise (§2.4, D46)"
            )
        if plan.pre_compute:
            raise NotImplementedCapability(
                "PreComputation steps are not executed by this executor (#148)"
            )
        if plan.query_target is not self._backend.target:
            raise UnsupportedOperation(
                f"the backend runs {self._backend.target.value}, the plan is "
                f"{plan.query_target.value}"
            )
        return datasets

    # -- run -----------------------------------------------------------------

    def run(
        self,
        plan: ExportPlan | None,
        mode: str | BudgetDecision,
        *,
        page_size: int | None = None,
    ) -> ExecutionResult:
        """Execute *plan* under the delivery *mode* (§11.10 ``execution``)."""
        d = _resolve_delivery(plan, mode, page_size)
        datasets = self._admit(d.plan)
        cur = self._backend.execute(d.plan.query_text, datasets)
        columns = tuple(c[0] for c in (cur.description or ()))
        result = ExecutionResult(d.mode, columns, datasets, warning=d.warning)
        if d.mode == "PROCEED":
            result.rows = list(cur.fetchall())
            return result
        size = self._batch if d.mode == "STREAM" else d.page_size
        assert size is not None
        result.chunks = _chunks(cur, size)
        result.chunk_size = size
        return result

    def materialise(self, plan: ArtifactPlan, *, cancel: Any = None) -> ArtifactOutcome:
        """Execute a PipelineArtifact plan by D46 (§2.4); no runtime, no write."""
        if self._artifacts is None:
            raise NotConfigured("no ArtifactRuntime is configured; nothing is written")
        return self._artifacts.execute(plan, cancel=cancel)

    def cancel(self) -> None:
        """Interrupt the statement the backend is running."""
        self._backend.interrupt()

    # -- submit --------------------------------------------------------------

    def _sink(self, sink: object) -> JobSink:
        name = sink.value if isinstance(sink, SinkTarget) else sink
        if not isinstance(name, str) or not name:
            raise NotConfigured("the ASYNC decision names no sink")
        target = self._sinks.get(name)
        if target is None:
            raise NotConfigured(f"no async sink named {name!r} is configured")
        return target

    @staticmethod
    def _check_key(plan: ExportPlan, key: object) -> ResultKey:
        if not isinstance(key, ResultKey):
            raise InvalidRuntimeArgument("idempotency_key must be a ResultKey")
        assert plan.ir is not None
        if dag_hash(plan.ir) != key.dag_hash:
            raise InvalidRuntimeArgument(
                "the idempotency key was computed for another plan"
            )
        return key

    def _request(
        self,
        plan: ExportPlan,
        sink: object,
        callback: object,
        ttl_s: object,
        key: object,
    ) -> tuple[JobSink, JobRequest]:
        datasets = self._admit(plan)
        target = self._sink(sink)
        self._require(frozenset({target.dataset_id}), Action.WRITE)
        if not isinstance(callback, str) or not callback:
            raise NotConfigured("the ASYNC decision names no callback")
        ttl = timedelta(seconds=_positive(ttl_s, "ttl_s"))
        rkey = self._check_key(plan, key)
        req = JobRequest(
            plan.query_text,
            plan.query_target,
            datasets,
            target.dataset_id,
            callback,
            ttl,
            rkey,
            self._auth.principal_id,
            self._auth.tenant_id,
        )
        return target, req

    async def submit(
        self,
        *,
        plan: ExportPlan,
        sink: str | SinkTarget | None,
        callback: str | None,
        ttl_s: int | None,
        idempotency_key: ResultKey,
    ) -> JobTicket:
        """Submit an ASYNC plan; idempotent on a versioned ``result_key``."""
        target, req = self._request(plan, sink, callback, ttl_s, idempotency_key)
        slot = (self._auth.tenant_id, req.idempotency_key.value)
        reuse = req.idempotency_key.versioned
        async with self._lock:
            if reuse and slot in self._tickets:
                return self._tickets[slot]
            ticket = await _enqueue(target.enqueue, req)
            if reuse:
                self._tickets[slot] = ticket
            return ticket


async def _enqueue(enqueue: Callable[[JobRequest], Any], req: JobRequest) -> JobTicket:
    try:
        ticket = await enqueue(req)
    except Exception as exc:  # noqa: BLE001 — reported as a typed failure
        raise SubmissionFailed(f"the sink refused the job: {exc}") from exc
    if not isinstance(ticket, JobTicket) or not ticket.job_id or not ticket.status:
        raise SubmissionFailed(f"the sink returned no valid ticket: {ticket!r}")
    return ticket
