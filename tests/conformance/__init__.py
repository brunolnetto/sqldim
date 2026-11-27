"""Semantic conformance suite for the DGM core (issue #137).

* :mod:`tests.conformance.oracle` — a reference semantics written from the
  specification's definitions.  It imports nothing from ``sqldim``.
* :mod:`tests.conformance.cases` — the cases, as data: clause, fixture,
  inputs, the rule exercised and, where the specification states it, the
  expected value.
* :mod:`tests.conformance.runners` — adapters that run a case against one
  implementation (a *backend*) and return a value comparable with the oracle.
* :mod:`tests.conformance.expectations` — the known non-conformances
  (strict xfail) and the constructs a backend refuses (fail-closed).
* :mod:`tests.conformance.matrix` — the traceability matrix, generated from
  the above and checked against ``docs/reference/dgm-conformance.md``.
"""
