"""The declared model: well-formedness of ``G_S`` and ``Σ`` (§2.1, §2.4, §2.6).

A declaration is rejected when built if it is ill-formed: a type or edge type
declared twice, an edge type or a Σ argument that names no type of the kind
§2.4 requires (§2.6 *Declared model*).  The structural invariants of §2.1
(``V_S ⊆ D_S × F_S``) are not construction checks.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from tests.conformance.oracle import REJECTED

ACCEPTED = "ACCEPTED"

# The argument kinds of the §2.4 grammar (node arguments only).
SIGNATURES: dict[str, tuple[str, ...]] = {
    "Conformed": ("dim", "fact*"),
    "Grain": ("fact",),
    "SCDType": ("dim",),
    "RolePlaying": ("dim",),
    "ProjectsFrom": ("dim", "dim"),
    "FactlessFact": ("fact",),
    "Hierarchy": ("dim",),
    "MedallionLayer": ("node",),
}


def _kind_ok(required: str, kind: str | None) -> bool:
    req = required.rstrip("*")
    return kind is not None and (req == "node" or req == kind)


def declaration(case_input: Mapping[str, Any]) -> str:
    nodes: Mapping[str, str] = case_input["nodes"]
    names = list(case_input.get("node_list", nodes))
    if len(names) != len(set(names)):
        return REJECTED
    triples = [tuple(e[:3]) for e in case_input.get("edges", ())]
    if len(triples) != len(set(triples)):
        return REJECTED
    if any(e[0] not in nodes or e[1] not in nodes for e in case_input.get("edges", ())):
        return REJECTED
    for kind, args in case_input.get("sigma", ()):
        sig = SIGNATURES[kind]
        for required, arg in zip(sig, args):
            values = arg if required.endswith("*") else [arg]
            if not all(_kind_ok(required, nodes.get(v)) for v in values):
                return REJECTED
    return ACCEPTED
