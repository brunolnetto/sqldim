"""The per-backend matrix (#138) is current, and declarations match the evidence."""

from __future__ import annotations

import pytest

from sqldim.core.query.dgm.planner import QueryTarget, SinkTarget, Support
from tests.conformance import backend_matrix as bm
from tests.conformance.expectations import Status
from tests.conformance.oracle import REJECTED
from tests.conformance.reference import oracle_result
from tests.conformance.runners import BACKENDS

CELLS = [
    pytest.param(r, c, id=f"{r.label}::{c.value}")
    for r in bm.ROWS
    for c in (bm.QUERY_CAPS if r in bm.QUERY_ROWS else bm.SINK_CAPS)
]


def test_committed_matrix_is_current() -> None:
    assert bm.committed() == bm.render(), (
        "regenerate: PYTHONPATH=. python -m tests.conformance.backend_matrix --write"
    )


def test_every_target_and_sink_has_a_row() -> None:
    names = {r.profile.name for r in bm.ROWS}
    assert {t.value for t in QueryTarget} <= names
    assert {s.value for s in SinkTarget} <= names


def test_evidence_names_existing_backends() -> None:
    known = {b.name for b in BACKENDS}
    for r in bm.ROWS:
        assert set(r.evidence) <= known, r.label
        if not r.evidence:
            assert "not executed" in r.executed, r.label


@pytest.mark.parametrize(("row", "cap"), CELLS)
def test_declaration_agrees_with_evidence(row: bm.Row, cap: bm.Capability) -> None:
    support = row.profile.declared(cap).support
    for name, case, st in bm.evidence(row, cap):
        where = f"{row.label} {cap.value}: {name}::{case.id} is {st.name}"
        if support is Support.SUPPORTED:
            assert st is Status.PASS, where
        elif support is Support.UNSUPPORTED:
            # a refusal, or a pass where the specification itself rejects
            assert st in (Status.REFUSED, Status.NOT_EXPRESSIBLE) or (
                st is Status.PASS and oracle_result(case) == REJECTED
            ), where
