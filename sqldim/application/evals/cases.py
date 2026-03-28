"""EvalCase registry — auto-discovered from per-domain ``artifacts/evals.json``.

Each ``EvalCase`` captures:
- the natural-language utterance
- which dataset to run it against
- what constitutes a *passing* result (columns present, rows returned, etc.)
- semantic tags for grouping reports

The ``EVAL_SUITE`` list is the authoritative registry consumed by
:class:`~sqldim.evals.runner.EvalRunner`.  It is populated at import time by
scanning every ``domains/*/artifacts/evals.json`` file via
:func:`~sqldim.application.evals.loader.load_eval_suite`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class EvalCase:
    """A single NL interface evaluation case.

    Parameters
    ----------
    id:
        Unique slug, e.g. ``"ecommerce.01.customers_by_tier"``.
    dataset:
        Dataset name as accepted by ``sqldim ask`` (e.g. ``"ecommerce"``).
    utterance:
        Natural-language question posed to the NL interface.
    expect_result:
        When ``True`` the case expects at least one result row.
    expect_columns:
        Subset of column names that must appear in the result (case-insensitive).
        A case with an empty list only checks that a query ran (no column assertion).
    expect_table:
        If set, the generated SQL must reference this table name.
    max_hops:
        Maximum number of graph node hops considered acceptable.
        The happy path is 8 hops (entity_resolution → … → explanation_rendering).
        Queries with clarification loops may use up to 11.
    tags:
        Free-form labels for filtering reports (e.g. ``["entity", "filter"]``).
    """

    id: str
    dataset: str
    utterance: str
    expect_result: bool = True
    expect_columns: list[str] = field(default_factory=list)
    expect_table: str | None = None
    max_hops: int = 11
    tags: list[str] = field(default_factory=list)
    expected_sql: str | None = None
    """Canonical ground-truth SQL executed against the dataset for result comparison."""
    pipeline_source_factory: Any = field(default=None, hash=False, compare=False)
    """Optional factory ``() -> PipelineSource``.  When set, bypasses
    ``DatasetPipelineSource(load_dataset(...))`` so the case can supply its
    own medallion pipeline source (e.g. :class:`RetailMedallionPipelineSource`).
    The ``dataset`` field is still used for reporting and filtering."""


# ---------------------------------------------------------------------------
# Master registry — populated from per-domain artifacts/evals.json
# ---------------------------------------------------------------------------

from sqldim.application.evals.loader import load_eval_suite  # noqa: E402

#: All eval cases across all datasets. Consumed by :class:`EvalRunner`.
EVAL_SUITE: list[EvalCase] = load_eval_suite()

__all__ = ["EvalCase", "EVAL_SUITE"]
