"""sqldim NL interface evaluation framework (§11.10 evals).

Evaluates the end-to-end agentic workflow — entity resolution → candidate
generation → ranking → execution → explanation — across all bundled datasets
using a cost-effective LLM backend (default: ``openai:gpt-4o-mini``).

Modules
-------
cases
    Typed ``EvalCase`` dataclass + per-dataset eval suite registry.
runner
    ``EvalRunner`` executes cases, captures hop traces and latency, and
    emits ``EvalResult`` records.
metrics
    Pass/fail scoring helpers (column presence, row presence, hop budget).

Quick start::

    from sqldim.application.evals.runner import EvalRunner
    report = EvalRunner().run()
    print(report.summary())
"""

from __future__ import annotations

__all__ = [
    "EvalCase",
    "EVAL_SUITE",
    "EvalRunner",
    "EvalResult",
    "ToolEvalCase",
    "load_all_tool_eval_cases",
    "load_tool_eval_cases",
]

_LAZY: dict[str, str] = {
    "EvalCase": "sqldim.application.evals.cases",
    "EVAL_SUITE": "sqldim.application.evals.cases",
    "EvalRunner": "sqldim.application.evals.runner",
    "EvalResult": "sqldim.application.evals.runner",
    "ToolEvalCase": "sqldim.application.evals.tool_eval_loader",
    "load_all_tool_eval_cases": "sqldim.application.evals.tool_eval_loader",
    "load_tool_eval_cases": "sqldim.application.evals.tool_eval_loader",
}


def __getattr__(name: str) -> object:
    if name in _LAZY:
        import importlib

        mod = importlib.import_module(_LAZY[name])
        obj = getattr(mod, name)
        globals()[name] = obj
        return obj
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
