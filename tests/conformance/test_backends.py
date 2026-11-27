"""Every backend against the oracle, case by case.

The status of each (case, backend) pair comes from
:mod:`tests.conformance.expectations`:

* PASS — the backend's value equals the oracle's;
* XFAIL — a known non-conformance, ``xfail(strict=True)``: the case passes
  the moment the implementation conforms, and the expectation must then go;
* REFUSED — the implementation rejects the construct wholesale (fail-closed):
  either a case the specification accepts, or a rejection the specification
  asks for but made for an unrelated reason; checked to be a rejection;
* NOT_EXPRESSIBLE — the implementation has no construct for the case;
  checked to raise :class:`NotExpressible`.
"""

from __future__ import annotations

import os

import pytest

from tests.conformance.cases import Case, all_cases
from tests.conformance.expectations import Status, reason, status
from tests.conformance.reference import oracle_result
from tests.conformance.runners import BACKENDS, REJECTED, NotExpressible, backend


def _params() -> list:
    out = []
    for b in BACKENDS:
        for c in all_cases():
            st = status(c, b.name)
            if st is None:
                continue
            marks = []
            if st is Status.XFAIL:
                marks.append(pytest.mark.xfail(strict=True, reason=reason(c, b.name)))
            out.append(pytest.param(c, b.name, st, id=f"{b.name}::{c.id}", marks=marks))
    return out


@pytest.mark.parametrize(("case", "backend_name", "st"), _params())
def test_backend_conforms(case: Case, backend_name: str, st: Status) -> None:
    b = backend(backend_name)
    missing = b.unavailable()
    if missing:
        # a case that did not run is never reported as passing; CI must run it
        if os.environ.get("CI") == "true":
            pytest.fail(f"{backend_name} cannot run under CI: {missing}")
        pytest.skip(missing)
    run = b.run
    if st is Status.NOT_EXPRESSIBLE:
        with pytest.raises(NotExpressible):
            run(case)
        return
    result = run(case)
    if st is Status.REFUSED:
        # rejected wholesale: no evidence for or against the rule, either way
        assert result == REJECTED, f"expected a refusal, got {result!r}"
        return
    assert result == oracle_result(case), f"{case.clause}: {case.rule}"
