"""Shared fixtures for the §11.10 runtime API tests (#149)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import duckdb

from sqldim.core.query.dgm import DGMQuery, NOT, AND, PropRef, ScalarPred, VerbHop
from sqldim.core.query.dgm.algebra import ComposeOp, QuestionAlgebra
from sqldim.core.query.dgm.nl.supervisor._auth import Action, AuthContext, grants_for
from sqldim.core.query.dgm.planner import (
    CostEstimate,
    DGMPlanner,
    ExecutionBudget,
    ExportPlan,
    QueryTarget,
    StaticCatalog,
)
from sqldim.core.query.dgm.planner._ir_identity import Dependency
from sqldim.core.query.dgm.runtime import (
    InMemoryQuestionStore,
    JobRequest,
    JobTicket,
    PlanEstimate,
    QueryDAG,
    encode_spec,
)

SCHEMA_VERSION = "schema-v1"
CATALOG = StaticCatalog(base=frozenset({"dim_customer", "fact_sale"}))


def planner() -> DGMPlanner:
    return DGMPlanner(None, None, None, None, QueryTarget.SQL_DUCKDB)  # type: ignore[arg-type]


def q_customers() -> DGMQuery:
    return DGMQuery().anchor("dim_customer", "c")


def q_eu() -> DGMQuery:
    return q_customers().where(ScalarPred(PropRef("c", "region"), "=", "EU"))


def q_sales() -> DGMQuery:
    return (
        DGMQuery()
        .anchor("dim_customer", "c")
        .path_join(
            VerbHop("c", "placed", "s", table="fact_sale", on="c.id = s.customer_id")
        )
        .group_by("c.region")
        .agg(total="SUM(s.revenue)")
    )


def q_contradiction() -> DGMQuery:
    p = ScalarPred(PropRef("c", "region"), "=", "EU")
    return q_customers().where(AND(p, NOT(p)))


def alg_union(left: str = "a", right: str = "b") -> tuple[QuestionAlgebra, str]:
    alg = QuestionAlgebra()
    alg.add(left, q_customers())
    alg.add(right, q_eu())
    alg.compose(left, ComposeOp.UNION, right, name="u")
    return alg, "u"


def alg_except() -> tuple[QuestionAlgebra, str]:
    alg = QuestionAlgebra()
    alg.add("a", q_customers())
    alg.add("b", q_eu())
    alg.compose("a", ComposeOp.EXCEPT, "b", name="x")
    return alg, "x"


def plan_of(q: DGMQuery | tuple[QuestionAlgebra, str]) -> ExportPlan:
    if isinstance(q, tuple):
        alg, final = q
        return planner().plan_query(alg, final=final)
    return planner().plan_query(q)


def spec_of(q: DGMQuery | tuple[QuestionAlgebra, str], **params: Any) -> dict:
    plan = plan_of(q)
    assert plan.ir is not None
    return encode_spec(plan.ir, params)


def cost(total: float) -> CostEstimate:
    return CostEstimate(cpu_ops=int(total), io_ops=0)


def budget(**kw: Any) -> ExecutionBudget:
    fields: dict[str, Any] = {
        "max_estimated_cost": cost(10_000),
        "max_result_rows": 1_000,
        "max_wall_time": timedelta(seconds=30),
        "max_precompute_time": timedelta(seconds=10),
        "streaming_threshold": cost(100),
        "async_threshold": cost(1_000),
    }
    fields.update(kw)
    return ExecutionBudget(**fields)


@dataclass
class FilterCostModel:
    """An unfiltered question is OVER (15 000); a filtered one is SYNC (50)."""

    unknown: bool = False
    calls: int = 0

    def estimate(self, plan: ExportPlan) -> PlanEstimate:
        self.calls += 1
        if self.unknown:
            return PlanEstimate(None, None)
        root = plan.ir.root if plan.ir is not None else None
        filtered = getattr(root, "where", None) is not None
        return PlanEstimate(cost(50 if filtered else 15_000), 10)


@dataclass
class Watermarks:
    marks: dict[str, str | int | None] = field(
        default_factory=lambda: {"dim_customer": 7, "fact_sale": 3}
    )

    def watermark(self, dep: Dependency) -> str | int | None:
        return self.marks.get(dep.relation)


def make_dag(
    store: InMemoryQuestionStore | None = None, **kw: Any
) -> tuple[QueryDAG, InMemoryQuestionStore]:
    store = store if store is not None else InMemoryQuestionStore()
    args: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "store": store,
        "catalog": CATALOG,
        "watermarks": Watermarks(),
        "cost_model": FilterCostModel(),
    }
    args.update(kw)
    return QueryDAG(**args), store


DATASETS = {"dim_customer": "crm", "fact_sale": "sales"}


def auth(
    read: tuple[str, ...] = ("crm", "sales"), write: tuple[str, ...] = ()
) -> AuthContext:
    grants = set(grants_for([d for d in read if d not in write], {Action.READ}))
    grants |= set(grants_for(write, {Action.READ, Action.WRITE}))
    return AuthContext("alice", "acme", frozenset(grants))


def database() -> Any:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE dim_customer AS SELECT range AS id, "
        "CASE WHEN range % 2 = 0 THEN 'EU' ELSE 'US' END AS region "
        "FROM range(10)"
    )
    con.execute(
        "CREATE TABLE fact_sale AS SELECT range AS id, range % 10 AS customer_id, "
        "range * 1.0 AS revenue FROM range(40)"
    )
    return con


@dataclass
class RecordingSink:
    dataset_id: str = "results"
    fail: Exception | None = None
    ticket: Any = None
    requests: list[JobRequest] = field(default_factory=list)

    async def enqueue(self, request: JobRequest) -> JobTicket:
        self.requests.append(request)
        if self.fail is not None:
            raise self.fail
        if self.ticket is not None:
            return self.ticket
        return JobTicket(job_id=f"j_{len(self.requests)}", status="QUEUED")
