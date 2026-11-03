"""Per-tool eval loader — discovers tool eval cases from every domain.

Each domain may place eval cases in two locations (both are loaded):

1. ``artifacts/tool_evals.json`` — legacy single-file format containing all
   non-query tools.
2. ``artifacts/evals/<tool_name>.json`` — per-tool eval files (preferred).

Usage::

    from sqldim.application.evals.tool_eval_loader import (
        load_all_tool_eval_cases,
        load_tool_eval_cases,
        load_cases_for_tool,
    )

    cases = load_all_tool_eval_cases()              # all domains, all tools
    ec = load_tool_eval_cases("ecommerce")          # one domain, all tools
    corr = load_cases_for_tool("dgm_correlate")     # all domains, one tool
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_DOMAINS_ROOT = Path(__file__).resolve().parent.parent / "datasets" / "domains"

__all__ = [
    "ToolEvalCase",
    "load_tool_eval_cases",
    "load_all_tool_eval_cases",
    "load_cases_for_tool",
]


@dataclass(frozen=True)
class ToolEvalCase:
    """A single per-tool evaluation case."""

    id: str
    tool: str
    domain: str
    description: str
    arguments: dict[str, Any]
    expected_success: bool
    expected_tool_name: str
    expected_data_keys: list[str]
    expected_data_checks: dict[str, Any]
    tags: list[str] = field(default_factory=list)


def _parse_case(d: dict[str, Any], domain: str) -> ToolEvalCase:
    exp = d["expected"]
    return ToolEvalCase(
        id=d["id"],
        tool=d["tool"],
        domain=domain,
        description=d.get("description", ""),
        arguments=d.get("arguments", {}),
        expected_success=exp.get("success", True),
        expected_tool_name=exp.get("tool_name", d["tool"]),
        expected_data_keys=exp.get("data_keys", []),
        expected_data_checks=exp.get("data_checks", {}),
        tags=d.get("tags", []),
    )


def load_tool_eval_cases(domain: str) -> list[ToolEvalCase]:
    """Load tool eval cases for a single *domain*.

    Merges cases from both ``tool_evals.json`` (legacy) and per-tool
    files in ``evals/``.  Deduplicates by case id.
    """
    seen_ids: set[str] = set()
    result: list[ToolEvalCase] = []

    # Files that live in evals/ but are not per-tool eval files
    _NON_TOOL_FILES = {"nl_cases.json", "drift.json", "tool_evals.json"}

    # 1. Per-tool files in evals/ directory (preferred)
    evals_dir = _DOMAINS_ROOT / domain / "artifacts" / "evals"
    if evals_dir.is_dir():
        for tool_file in sorted(evals_dir.glob("*.json")):
            if tool_file.name in _NON_TOOL_FILES:
                continue
            with tool_file.open() as f:
                data = json.load(f)
            for c in data.get("cases", []):
                case = _parse_case(c, domain)
                if case.id not in seen_ids:
                    seen_ids.add(case.id)
                    result.append(case)

    # 2. Legacy tool_evals.json (now in evals/ subfolder)
    path = _DOMAINS_ROOT / domain / "artifacts" / "evals" / "tool_evals.json"
    if path.exists():
        with path.open() as f:
            data = json.load(f)
        for c in data.get("tool_cases", []):
            case = _parse_case(c, domain)
            if case.id not in seen_ids:
                seen_ids.add(case.id)
                result.append(case)

    return result


def load_all_tool_eval_cases() -> list[ToolEvalCase]:
    """Load and concatenate tool eval cases from all domains."""
    result: list[ToolEvalCase] = []
    seen_domains: set[str] = set()

    # Discover domains from evals/ directories
    for evals_dir in sorted(_DOMAINS_ROOT.glob("*/artifacts/evals")):
        if evals_dir.is_dir():
            domain = evals_dir.parent.parent.name
            seen_domains.add(domain)
            result.extend(load_tool_eval_cases(domain))

    # Also pick up domains that only have the legacy tool_evals.json
    for artifact in sorted(_DOMAINS_ROOT.glob("*/artifacts/evals/tool_evals.json")):
        domain = artifact.parent.parent.parent.name
        if domain not in seen_domains:  # pragma: no cover
            seen_domains.add(domain)
            result.extend(load_tool_eval_cases(domain))

    return result


def load_cases_for_tool(tool_name: str) -> list[ToolEvalCase]:
    """Load eval cases for a specific *tool_name* across all domains.

    Looks for ``artifacts/evals/<tool_name>.json`` in each domain, then
    falls back to filtering ``tool_evals.json``.
    """
    result: list[ToolEvalCase] = []
    seen_ids: set[str] = set()

    for domain_dir in sorted(_DOMAINS_ROOT.iterdir()):
        if not domain_dir.is_dir():
            continue
        domain = domain_dir.name

        # Per-tool file
        tool_file = domain_dir / "artifacts" / "evals" / f"{tool_name}.json"
        if tool_file.exists():
            with tool_file.open() as f:
                data = json.load(f)
            for c in data.get("cases", []):
                case = _parse_case(c, domain)
                if case.id not in seen_ids:
                    seen_ids.add(case.id)
                    result.append(case)
            continue

        # Fallback: filter from tool_evals.json (now in evals/ subfolder)
        legacy = domain_dir / "artifacts" / "evals" / "tool_evals.json"
        if legacy.exists():
            with legacy.open() as f:
                data = json.load(f)
            for c in data.get("tool_cases", []):
                if c.get("tool") == tool_name:
                    case = _parse_case(c, domain)
                    if case.id not in seen_ids:
                        seen_ids.add(case.id)
                        result.append(case)

    return result
    return result
