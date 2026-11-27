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


@dataclass(frozen=True)
class Backend:
    name: str
    description: str
    areas: frozenset[str]
    run: Callable[[Case], Any]


def _backends() -> tuple[Backend, ...]:
    from tests.conformance.runners import bdd_impl, duckdb_sql, model_impl, ordering

    return (
        duckdb_sql.LEGACY,
        duckdb_sql.PLAN_IR,
        ordering.BACKEND,
        bdd_impl.BACKEND,
        model_impl.MODEL,
        model_impl.ANNOTATIONS,
    )


BACKENDS: tuple[Backend, ...] = _backends()


def backend(name: str) -> Backend:
    return next(b for b in BACKENDS if b.name == name)
