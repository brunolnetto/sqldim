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

from sqldim.application.evals.cases import EvalCase, EVAL_SUITE
from sqldim.application.evals.runner import EvalRunner, EvalResult

__all__ = ["EvalCase", "EVAL_SUITE", "EvalRunner", "EvalResult"]
