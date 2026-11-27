"""Backends: adapters that run a case against one implementation.

A backend declares the areas it can be asked about and a ``run(case)`` that
returns a value in the oracle's format.  The contract:

* the implementation rejecting the case (a construction, lowering or
  declaration error) is returned as :data:`REJECTED` — it is compared with the
  oracle like any other value, so a rejection the oracle does not make is a
  refusal and must be declared in :mod:`tests.conformance.expectations`;
* a case the implementation offers no construct for raises
  :class:`NotExpressible`; that too must be declared, with its reason;
* nothing else is caught: an error is a failure of the case.

A backend that needs an external service declares ``requires``: it returns
``None`` when the service is reachable, else the reason it is not.  Its
cases are then skipped locally and are an error under CI (``CI=true``); a
case that did not run is never reported as passing.

Adding a backend (for #138) is adding one :class:`Backend` to :data:`BACKENDS`
and declaring its expectations; the cases and the oracle do not change.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from tests.conformance.cases import Case
from tests.conformance.oracle import REJECTED

__all__ = ["BACKENDS", "Backend", "NotExpressible", "REJECTED", "backend"]


class NotExpressible(Exception):
    """The implementation has no construct for this case."""


def _available() -> str | None:
    return None


@dataclass(frozen=True)
class Backend:
    name: str
    description: str
    areas: frozenset[str]
    run: Callable[[Case], Any]
    requires: Callable[[], str | None] | None = None

    def unavailable(self) -> str | None:
        return (self.requires or _available)()


def _backends() -> tuple[Backend, ...]:
    from tests.conformance.runners import bdd_impl, duckdb_sql, model_impl, ordering
    from tests.conformance.runners import postgres, refusals, sinks, write_plan

    return (
        duckdb_sql.LEGACY,
        duckdb_sql.PLAN_IR,
        ordering.BACKEND,
        bdd_impl.BACKEND,
        model_impl.MODEL,
        model_impl.ANNOTATIONS,
        postgres.PG_DUCKDB,
        refusals.BACKEND,
        write_plan.BACKEND,
        sinks.DUCKDB,
        sinks.PARQUET,
        sinks.POSTGRESQL,
        sinks.MOTHERDUCK,
        sinks.DELTA,
    )


BACKENDS: tuple[Backend, ...] = _backends()


def backend(name: str) -> Backend:
    return next(b for b in BACKENDS if b.name == name)
