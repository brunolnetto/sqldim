"""Supervisor entry point over every bundled dataset.

Not registered as a subcommand; the CLI route is ``sqldim nl ask --mode
supervisor``.  ``dgm_query`` runs only in the explicit demo mode (``--demo``,
D47).
"""

from __future__ import annotations

import argparse

from sqldim.cli.ask import (
    _build_llm_model,
    _invoke_supervisor,
    _outcome_exit_code,
    _print_verbose,
    _resolve_datasets,
)


def cmd_supervisor(args: argparse.Namespace) -> int:
    """Run the supervisor agent across all known datasets."""
    from sqldim.application.ask import KNOWN_DATASETS

    prefix = "sqldim supervisor"
    all_datasets = sorted(KNOWN_DATASETS.keys())
    available = _resolve_datasets(getattr(args, "datasets", None), all_datasets, prefix)
    if available is None:
        return 1
    model_name: str | None = getattr(args, "model", None)
    model_provider: str | None = getattr(args, "model_provider", None)
    llm_model = _build_llm_model(model_provider, model_name, prefix)

    result = _invoke_supervisor(
        available, llm_model, args.question, demo=bool(getattr(args, "demo", False))
    )
    print(result.synthesis or f"Outcome: {result.outcome}")

    if args.verbose:
        _print_verbose(result, prefix)

    return _outcome_exit_code(result)
