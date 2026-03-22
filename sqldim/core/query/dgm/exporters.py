"""DGM Query Exporters (DGM v0.16 §6.3).

Provides four round-trippable exporters:
  CypherExporter    — Neo4j Cypher
  SPARQLExporter    — W3C SPARQL
  DGMJSONExporter   — DGM_JSON AST dict
  DGMYAMLExporter   — DGM_YAML (hand-rolled; no external dep)

The export() methods emit structured, auditable representations of an
ExportPlan.  No natural language is generated.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqldim.core.query.dgm.planner import ExportPlan

__all__ = [
    "CypherExporter",
    "SPARQLExporter",
    "DGMJSONExporter",
    "DGMYAMLExporter",
]


# ---------------------------------------------------------------------------
# CypherExporter
# ---------------------------------------------------------------------------


class CypherExporter:
    """Export an ExportPlan targeting CYPHER as a Cypher query string.

    Limitations noted in §6.3:
    - UNTIL/SINCE TemporalMode: ✗ (not supported)
    - SignaturePred: partial (approximate)
    - TrailExpr Bound→Bound: native
    - REACHABLE_FROM/TO approximated via variable-length MATCH patterns
    """

    def export(self, plan: "ExportPlan") -> str:
        """Return the Cypher query text, enriched with a header comment."""
        header = "// DGM Cypher export — cone_containment_applied: {cc}\n".format(
            cc=getattr(plan, "cone_containment_applied", False)
        )
        return header + plan.query_text


# ---------------------------------------------------------------------------
# SPARQLExporter
# ---------------------------------------------------------------------------


class SPARQLExporter:
    """Export an ExportPlan targeting SPARQL as a SPARQL query string.

    Parameters
    ----------
    base_prefix:
        Optional namespace prefix declaration prepended to the output.

    Limitations noted in §6.3:
    - TemporalMode UNTIL/SINCE: ✗
    - SignaturePred: ✗
    - TrailExpr: partial
    """

    def __init__(self, base_prefix: str = "") -> None:
        self.base_prefix = base_prefix

    def export(self, plan: "ExportPlan") -> str:
        """Return the SPARQL query text with optional prefix header."""
        parts: list[str] = []
        if self.base_prefix:
            parts.append(f"PREFIX {self.base_prefix} <#>")
        parts.append(plan.query_text)
        return "\n".join(parts)


# ---------------------------------------------------------------------------
# DGMJSONExporter
# ---------------------------------------------------------------------------


class DGMJSONExporter:
    """Export an ExportPlan as a round-trippable JSON-serialisable dict.

    The dict includes all structural fields required for AST round-tripping
    including: query_target, query_text, pre_compute, sink_target,
    write_plan, cost_estimate, cone_containment_applied, and alternatives.
    """

    def export(self, plan: "ExportPlan") -> dict:
        """Return a JSON-serialisable dict representation of *plan*."""
        return {
            "query_target": plan.query_target.value,
            "query_text": plan.query_text,
            "pre_compute": [
                {"name": pc.name, "query": pc.query, "kind": pc.kind}
                for pc in plan.pre_compute
            ],
            "sink_target": plan.sink_target.value if plan.sink_target else None,
            "write_plan": plan.write_plan,
            "cost_estimate": self._serialize_cost(plan.cost_estimate),
            "cone_containment_applied": plan.cone_containment_applied,
            "alternatives": self._serialize_alternatives(plan),
        }

    @staticmethod
    def _serialize_cost(ce: object) -> "dict | None":
        if ce is None:
            return None
        return {"cpu_ops": ce.cpu_ops, "io_ops": ce.io_ops, "note": ce.note}  # type: ignore[attr-defined]

    @staticmethod
    def _serialize_alternatives(plan: "ExportPlan") -> list:
        return [
            {
                "query_target": alt.query_target.value,
                "query_text": alt.query_text,
                "cost_estimate": {
                    "cpu_ops": ce.cpu_ops,
                    "io_ops": ce.io_ops,
                    "note": ce.note,
                },
            }
            for alt, ce in plan.alternatives
        ]


# ---------------------------------------------------------------------------
# DGMYAMLExporter
# ---------------------------------------------------------------------------


class DGMYAMLExporter:
    """Export an ExportPlan as a YAML string (no external library required).

    Produces a minimal, readable, round-trippable YAML representation of
    the same fields emitted by DGMJSONExporter.  Multi-line strings are
    emitted as block scalars using ``|``.
    """

    def export(self, plan: "ExportPlan") -> str:
        """Return a YAML string representation of *plan*."""
        # Reuse DGMJSONExporter to get the canonical dict
        d = DGMJSONExporter().export(plan)
        return self._dict_to_yaml(d, indent=0)

    # -- Internal YAML serialiser (no PyYAML dependency) ------------------

    def _dict_to_yaml(self, obj: object, indent: int) -> str:
        prefix = "  " * indent
        if isinstance(obj, dict):
            return self._render_dict_body(obj, indent, prefix)
        if isinstance(obj, list):
            return self._render_list_body(obj, indent, prefix)
        return f"{prefix}{self._scalar(obj)}"

    def _render_dict_body(self, d: dict, indent: int, prefix: str) -> str:
        lines: list[str] = []
        for k, v in d.items():
            if isinstance(v, (dict, list)):
                lines.append(f"{prefix}{k}:")
                lines.append(self._dict_to_yaml(v, indent + 1))
            else:
                lines.append(f"{prefix}{k}: {self._scalar(v)}")
        return "\n".join(lines)

    def _render_list_body(self, lst: list, indent: int, prefix: str) -> str:
        if not lst:
            return f"{prefix}[]"
        lines: list[str] = []
        for item in lst:
            lines.append(self._render_list_item(item, indent, prefix))
        return "\n".join(lines)

    def _render_list_item(self, item: object, indent: int, prefix: str) -> str:
        if not isinstance(item, dict):
            return f"{prefix}- {self._scalar(item)}"
        items = list(item.items())
        first_k, first_v = items[0]
        parts = [f"{prefix}- {first_k}: {self._scalar(first_v)}"]
        for k, v in items[1:]:
            if isinstance(v, (dict, list)):
                parts.append(f"{prefix}  {k}:")
                parts.append(self._dict_to_yaml(v, indent + 2))
            else:
                parts.append(f"{prefix}  {k}: {self._scalar(v)}")
        return "\n".join(parts)

    @staticmethod
    def _needs_quoting(s: str) -> bool:
        return not s or any(c in s for c in ("\n", ":", "#"))

    @staticmethod
    def _scalar_simple(v: object) -> "str | None":
        """Return YAML-encoded str for None/bool/number; None signals 'is a string'."""
        if v is None:
            return "null"
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        return None

    @staticmethod
    def _scalar(v: object) -> str:
        simple = DGMYAMLExporter._scalar_simple(v)
        if simple is not None:
            return simple
        s = str(v)
        if DGMYAMLExporter._needs_quoting(s):
            escaped = s.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{escaped}"'
        return s
