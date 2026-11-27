"""Query targets with no emitter refuse every query (fail-closed).

``planner-refusals`` builds each case's query exactly as the DuckDB runners
do and plans it for every :class:`QueryTarget` whose profile has no emitter
(``SQL_POSTGRESQL``, ``CYPHER``, ``SPARQL``, ``DGM_JSON``, ``DGM_YAML``).  The
case is REJECTED only when every one of them raised ``UnsupportedCapability``;
a target that returns a plan, or raises anything else, fails the case.  A
case whose query is rejected while it is built, before planning, says
nothing about the targets and is not expressible here.
"""

from __future__ import annotations

from typing import Any

from sqldim.core.query.dgm import DGMPlanner, DGMQuery
from sqldim.core.query.dgm.planner import (
    QUERY_PROFILES,
    UnsupportedCapability,
)
from sqldim.exceptions import SemanticError
from tests.conformance.cases import Case
from tests.conformance.runners import Backend, NotExpressible
from tests.conformance.runners.duckdb_sql import EXECUTED_AREAS, Engine, backend

TARGETS = tuple(t for t, p in QUERY_PROFILES.items() if p.emitter is None)

_planned: list[DGMQuery] = []


def _refuse_all(q: DGMQuery) -> str:
    _planned.append(q)
    for target in TARGETS:
        planner = DGMPlanner(None, None, None, None, target)  # type: ignore[arg-type]
        try:
            planner.plan_query(q)
        except UnsupportedCapability:
            continue
        except SemanticError as exc:  # pragma: no cover - a failure of the case
            raise AssertionError(f"{target.value}: {exc!r}") from exc
        raise AssertionError(f"{target.value} planned a query it declares unsupported")
    raise UnsupportedCapability("refused by every target without an emitter")


_INNER = backend("refusals-inner", "", Engine(_refuse_all), areas=EXECUTED_AREAS)


def _run(case: Case) -> Any:
    _planned.clear()
    result = _INNER.run(case)
    if not _planned:
        raise NotExpressible("the query is rejected while it is built, before planning")
    return result


BACKEND = Backend(
    "planner-refusals",
    "DGMPlanner.plan_query() for SQL_POSTGRESQL, CYPHER, SPARQL, DGM_JSON and "
    "DGM_YAML: every query must raise UnsupportedCapability",
    EXECUTED_AREAS,
    _run,
)
