"""Conformance cases, as data.

A :class:`Case` names the clause it traces to, the rule it exercises, the
fixture it reads and its inputs.  ``expected`` is the value the
specification itself states (a worked example or a table); a case whose
expected value is derived from the definitions instead has
``expected = DERIVED`` and is decided by the oracle alone.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

DERIVED = "<derived by the oracle>"


@dataclass(frozen=True)
class Case:
    id: str
    area: str
    clause: str
    rule: str
    fixture: str
    input: Mapping[str, Any] = field(default_factory=dict)
    expected: Any = DERIVED

    def __str__(self) -> str:
        return self.id


def all_cases() -> list[Case]:
    from tests.conformance.cases import (
        aggregation,
        algebra,
        anchors,
        bdd,
        cones,
        ctl,
        export,
        query,
        scd,
        temporal,
    )

    out: list[Case] = []
    mods = (ctl, query, temporal, scd, aggregation, anchors, bdd, export, cones)
    for mod in mods + (algebra,):
        out.extend(mod.CASES)
    ids = [c.id for c in out]
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    if dupes:
        raise AssertionError(f"duplicate case ids: {dupes}")
    return out
