"""The user-facing capability page, generated from the code (#140, AUD-12).

``docs/reference/capabilities.md`` states what sqldim implements, from the
declarations the code acts on, never from a hand-kept list:

- the natural-language interface: whether ``build_nl_graph`` builds the §11.10
  pipeline or raises ``NLPipelineUnavailable``, and the help of every
  ``sqldim nl`` command;
- the supervisor tools of §12.3: ``build_registry`` with the default
  configuration, with a knowledge base, with a knowledge base and a move
  graph, and in the explicit demo mode (D47, ADR 0013);
- the query targets and write sinks: the profiles of
  ``sqldim/core/query/dgm/planner/_capabilities.py``;
- the theoretical claims: the status counts of ``claims.py``.

The page is kept between two markers; ``test_capabilities.py`` fails when the
committed text differs.  Regenerate with::

    PYTHONPATH=. python -m tests.conformance.capabilities --write
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from tests.conformance import claims

DOC = Path(__file__).resolve().parents[2] / "docs" / "reference" / "capabilities.md"
BEGIN = "<!-- BEGIN GENERATED: tests/conformance/capabilities.py -->"
END = "<!-- END GENERATED -->"


# --------------------------------------------------------------------------
# Probes (each reads a declaration the code itself acts on)
# --------------------------------------------------------------------------


def nl_pipeline_missing() -> tuple[str, ...]:
    """The components ``build_nl_graph()`` reports missing; ``()`` if it builds."""
    from sqldim.core.query.dgm.nl._graph import NLPipelineUnavailable, build_nl_graph

    try:
        build_nl_graph()
    except NLPipelineUnavailable as exc:
        return tuple(exc.missing)
    return ()


def nl_commands() -> list[tuple[str, str]]:
    """``(name, help)`` of every ``sqldim nl`` subcommand, in parser order."""
    from sqldim.cli import build_parser

    def sub(parser: argparse.ArgumentParser) -> Any:
        return next(
            a for a in parser._actions if isinstance(a, argparse._SubParsersAction)
        )

    nl = sub(build_parser()).choices["nl"]
    return [(a.dest, a.help or "") for a in sub(nl)._choices_actions]


class _ProbeMoves:
    """A move graph that answers nothing: it only makes retrieval registrable."""

    def contains(self, dataset: str, question_key: str) -> bool:
        return False

    def neighbours(self, dataset: str, question_key: str) -> Iterable[str]:
        return ()


def registries() -> dict[str, Mapping[str, Any]]:
    from sqldim.core.query.dgm.nl.supervisor._tools import build_registry

    kb = object()  # build_registry only checks that a knowledge base is configured
    return {
        "default": build_registry(),
        "kb": build_registry(kb=kb),
        "kb+moves": build_registry(kb=kb, moves=_ProbeMoves()),
        "demo": build_registry(demo=True),
    }


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------


def _avail(tool: Any) -> str:
    schema = tool.capability_schema
    if not schema.available:
        return "**unavailable**"
    return "demo only" if schema.version.endswith("+demo") else "available"


def _impl(regs: dict[str, Mapping[str, Any]], name: str) -> str:
    """The implementing class of the widest configuration, or what is missing."""
    for key in ("kb+moves", "default"):
        tool = regs[key][name]
        if tool.capability_schema.available:
            return f"`{type(tool).__module__}.{type(tool).__name__}`"
    return "missing: " + regs["default"][name].missing


def _nl_section() -> list[str]:
    missing = nl_pipeline_missing()
    lines = ["### Natural-language interface (§11)", ""]
    if missing:
        lines += [
            "**Demo only.** `build_nl_graph()` raises `NLPipelineUnavailable` "
            "(`NOT_IMPLEMENTED`) unless the caller selects the demo mode "
            "(`demo=True`, `--demo`). The §11.10 pipeline is missing:",
            "",
            *(f"- {m}" for m in missing),
        ]
    else:
        lines.append("`build_nl_graph()` builds the §11.10 pipeline.")
    lines += ["", "| Command | Help |", "|---|---|"]
    lines += [f"| `sqldim nl {name}` | {text} |" for name, text in nl_commands()]
    return lines


def _tool_section() -> list[str]:
    from sqldim.core.query.dgm.nl.supervisor._schemas import TOOL_SCHEMAS

    regs = registries()
    lines = [
        "### Supervisor tools (§12.3)",
        "",
        "Columns are `build_registry()` with the default configuration, with a "
        "knowledge base (`kb=`), with a knowledge base and a move graph "
        "(`kb=`, `moves=`), and with `demo=True`. A tool that is unavailable "
        "returns `NOT_IMPLEMENTED` without running.",
        "",
        "| Tool | Default | With a knowledge base | With a knowledge base and a "
        "move graph | Demo mode | Implementation |",
        "|---|---|---|---|---|---|",
    ]
    for name in TOOL_SCHEMAS:
        cells = " | ".join(
            _avail(regs[k][name]) for k in ("default", "kb", "kb+moves", "demo")
        )
        lines.append(f"| `{name}` | {cells} | {_impl(regs, name)} |")
    return lines


def _profile_table(rows: Iterable[Any], capabilities: Iterable[Any]) -> list[str]:
    caps = list(capabilities)
    lines = [
        "| Backend | Declared by | Emitter | "
        + " | ".join(c.value for c in caps)
        + " | Executed in CI |",
        "|---|---|---|" + "---|" * len(caps) + "---|",
    ]
    for row in rows:
        p = row.profile
        cells = " | ".join(p.declared(c).support.value for c in caps)
        emitter = p.emitter or "**none**"
        executed = row.executed.replace(
            "*Reproducing* below", "*Reproducing* in `dgm-backend-matrix.md`"
        )
        lines.append(f"| {row.label} | `{p.name}` | {emitter} | {cells} | {executed} |")
    return lines


def _backend_section() -> list[str]:
    from sqldim.core.query.dgm.planner import _capabilities as cap
    from tests.conformance import backend_matrix

    return [
        "### Query targets (§6.4)",
        "",
        "Declared support per capability, from the profiles the planner reads. "
        "A target with no emitter raises `UnsupportedCapability` and is never "
        "routed to another target. *Partial* is explained per cell, with the "
        "cases that ran, in `dgm-backend-matrix.md`.",
        "",
        *_profile_table(backend_matrix.QUERY_ROWS, cap.QUERY_CAPABILITIES),
        "",
        "### Write sinks (§6.2 Rule 8)",
        "",
        *_profile_table(backend_matrix.SINK_ROWS, cap.SINK_CAPABILITIES),
    ]


def _claims_section() -> list[str]:
    lines = [
        "### Theoretical claims",
        "",
        "| Status | Claims |",
        "|---|---|",
    ]
    for status in claims.Status:
        ids = [f"`{c.id}`" for c in claims.CLAIMS if c.status is status]
        lines.append(f"| {status.value} | {', '.join(ids) or '—'} |")
    return lines


def render() -> str:
    lines = [BEGIN, ""]
    for section in (_nl_section, _tool_section, _backend_section, _claims_section):
        lines += [*section(), ""]
    lines.append(END)
    return "\n".join(lines) + "\n"


def committed() -> str:
    text = DOC.read_text(encoding="utf-8")
    start, end = text.index(BEGIN), text.index(END) + len(END)
    return text[start:end] + "\n"


def write() -> None:
    text = DOC.read_text(encoding="utf-8")
    start, end = text.index(BEGIN), text.index(END) + len(END)
    DOC.write_text(text[:start] + render().rstrip("\n") + text[end:], encoding="utf-8")


if __name__ == "__main__":
    if "--write" in sys.argv:
        write()
    else:
        sys.stdout.write(render())
