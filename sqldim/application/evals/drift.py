"""Drift-eval primitives — run NL queries before and after a pipeline event.

A *drift eval* answers the question:
    "How does the agent's answer change when the underlying data changes?"

Typical usage::

    from sqldim.application.evals.drift import DriftEvalCase
    from sqldim.application.evals.runner import EvalRunner

    runner = EvalRunner(provider="openai", model_name="gpt-4o-mini")
    report = runner.run_drift_suite(RETAIL_DRIFT_CASES)
    print(report.summary())
    report.to_json("drift_report.json")

Design
------
A :class:`DriftEvalCase` bundles:

* ``pipeline_source_factory`` — callable that builds a fresh
  :class:`~sqldim.application._pipeline_sources.PipelineSource`.
* ``event_fn(con)`` — a function that mutates the live DuckDB connection
  (e.g. inserts new orders, applies price changes, re-materialises gold
  tables after an SCD update).
* ``event_description`` — human-readable label shown in the report.
* ``cases`` — one or more :class:`~sqldim.application.evals.cases.EvalCase`
  instances to probe the agent before *and* after the event.

The runner keeps the connection alive across the before/after runs so the
agent in the "after" pass observes the mutated data without rebuilding the
entire pipeline.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable

from sqldim.application.evals.cases import EvalCase
from sqldim.application.datasets.events import AggregateState, DomainEvent


# ---------------------------------------------------------------------------
# make_domain_event_fn — bridge DomainEvent ↔ live DuckDB connection
# ---------------------------------------------------------------------------


def make_domain_event_fn(
    events: list[tuple[DomainEvent, dict[str, Any]]],
    raw_tables: list[str],
    rebuild_fn: Callable[[Any], None] | None = None,
) -> Callable[[Any], None]:
    """Return an ``event_fn(con)`` that applies *events* via the OLTP layer.

    Design
    ------
    Instead of writing custom DuckDB SQL for each mutation, this bridge reuses
    the existing :class:`~sqldim.application.datasets.events.DomainEvent`
    subclasses (which contain the business rules) and round-trips the data
    through an :class:`~sqldim.application.datasets.events.AggregateState`:

    1. Read *raw_tables* from the live DuckDB connection into Python dicts.
    2. Wrap them in an :class:`AggregateState`.
    3. Apply each ``(DomainEvent, kwargs)`` pair in order.
    4. Write the mutated tables back to DuckDB (replacing them in-place).
    5. If *rebuild_fn* is given, call it to cascade changes through any
       downstream layers (e.g. re-run silver→gold for a medallion pipeline).

    Parameters
    ----------
    events:
        List of ``(domain_event_instance, kwargs_dict)`` pairs.  Events are
        applied in sequence so earlier mutations are visible to later ones.
    raw_tables:
        Table names to snapshot into the :class:`AggregateState`.  Only
        tables that the events actually read or write need to be listed.
    rebuild_fn:
        Optional ``(con) -> None`` called after the tables are reloaded.
        Use this to re-run downstream pipeline transforms so the NL agent
        sees updated gold/analytics tables.

    Returns
    -------
    Callable[[DuckDBPyConnection], None]
        A function suitable for use as :attr:`DriftEvalCase.event_fn`.
    """

    def event_fn(con: Any) -> None:
        import pandas as pd

        # 1. Snapshot DuckDB → Python dicts
        state_data: dict[str, list[dict]] = {}
        for table in raw_tables:
            df = con.execute(f"SELECT * FROM {table}").df()
            state_data[table] = df.to_dict("records")

        state = AggregateState(state_data)

        # 2. Apply all domain events
        for event, kwargs in events:
            event.apply(state, **kwargs)

        # 3. Write mutated tables back to DuckDB
        for table in raw_tables:
            rows = state.get(table)
            if not rows:
                continue
            df = pd.DataFrame(rows)
            con.execute(f"DROP TABLE IF EXISTS {table}")
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")

        # 4. Cascade through downstream pipeline layers
        if rebuild_fn is not None:
            rebuild_fn(con)

    return event_fn


# ---------------------------------------------------------------------------
# DriftEvalCase
# ---------------------------------------------------------------------------


@dataclass
class DriftEvalCase:
    """A before/after evaluation that measures agentic answer drift.

    Parameters
    ----------
    id:
        Unique slug, e.g. ``"retail.drift.price_increase"``.
    pipeline_source_factory:
        Zero-argument callable that returns a fresh
        :class:`~sqldim.application._pipeline_sources.PipelineSource`.
        Called once per drift case; the source is held open across both the
        *before* and *after* runs.
    event_fn:
        Function ``(con: DuckDBPyConnection) -> None`` applied to the live
        DuckDB connection between the before and after eval runs.  It should
        mutate the data (e.g. insert rows, update prices, re-materialise a
        gold table) without closing the connection.
    event_description:
        Short human-readable label for the event (e.g.
        ``"20% product price increase"``).
    cases:
        Eval cases to run before *and* after the event.  Identical cases are
        used for both passes so the comparison is apples-to-apples.
    """

    id: str
    pipeline_source_factory: Callable[[], Any]
    event_fn: Callable[[Any], None]
    event_description: str
    cases: list[EvalCase] = field(default_factory=list)


# ---------------------------------------------------------------------------
# DriftPairResult
# ---------------------------------------------------------------------------


@dataclass
class DriftPairResult:
    """Before/after pair for one :class:`DriftEvalCase` × one :class:`EvalCase`.

    Attributes
    ----------
    drift_id:
        Corresponds to :attr:`DriftEvalCase.id`.
    event_description:
        Human-readable event label.
    before:
        :class:`~sqldim.application.evals.runner.EvalResult` from the
        pre-event run.
    after:
        :class:`~sqldim.application.evals.runner.EvalResult` from the
        post-event run.
    """

    drift_id: str
    event_description: str
    before: Any  # EvalResult — typed as Any to avoid circular import
    after: Any   # EvalResult


    @property
    def sql_changed(self) -> bool:
        """True when the generated SQL differs between before and after."""
        return self.before.sql_generated != self.after.sql_generated

    @property
    def answer_changed(self) -> bool:
        """True when the result rows differ between before and after."""
        b_rows = sorted(
            str(r) for r in (self.before.result_rows or [])
        )
        a_rows = sorted(
            str(r) for r in (self.after.result_rows or [])
        )
        return b_rows != a_rows

    @property
    def row_count_delta(self) -> int:
        """Change in number of result rows (after − before)."""
        return self.after.row_count - self.before.row_count


# ---------------------------------------------------------------------------
# DriftEvalReport
# ---------------------------------------------------------------------------


@dataclass
class DriftEvalReport:
    """Aggregated results from :meth:`EvalRunner.run_drift_suite`.

    Attributes
    ----------
    run_at:
        ISO-8601 timestamp of the run.
    provider / model_name:
        LLM provider and model used.
    pairs:
        All :class:`DriftPairResult` instances.
    """

    run_at: str
    provider: str
    model_name: str
    pairs: list[DriftPairResult] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    @property
    def total(self) -> int:
        return len(self.pairs)

    @property
    def changed(self) -> int:
        """Number of pairs where the agent's answer changed."""
        return sum(1 for p in self.pairs if p.answer_changed)

    @property
    def sql_drifted(self) -> int:
        """Number of pairs where the generated SQL changed."""
        return sum(1 for p in self.pairs if p.sql_changed)

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def summary(self) -> str:
        lines: list[str] = [
            f"\nDriftEvalReport [{self.run_at}]",
            f"  provider={self.provider}  model={self.model_name}",
            f"  {self.changed}/{self.total} answers changed  "
            f"{self.sql_drifted}/{self.total} SQL drifted",
            "",
        ]

        # Group by drift_id
        by_drift: dict[str, list[DriftPairResult]] = {}
        for p in self.pairs:
            by_drift.setdefault(p.drift_id, []).append(p)

        for drift_id, pairs in by_drift.items():
            event_desc = pairs[0].event_description
            n = len(pairs)
            before_pass = sum(1 for p in pairs if p.before.passed)
            after_pass = sum(1 for p in pairs if p.after.passed)
            lines.append(f"  ── Event [{drift_id}]")
            lines.append(f"     {event_desc}")
            lines.append(
                f"     pass: {before_pass}/{n} before → {after_pass}/{n} after"
            )
            for p in pairs:
                utterance = p.before.utterance
                changed_icon = "~" if p.answer_changed else "="
                notes: list[str] = []
                if p.answer_changed:
                    notes.append("answer changed")
                if p.sql_changed:
                    notes.append("SQL drifted")
                note_str = "  [" + ", ".join(notes) + "]" if notes else ""
                b_score = f"{p.before.score:.2f}"
                a_score = f"{p.after.score:.2f}"
                lines.append(
                    f"     {changed_icon} [{b_score}→{a_score}]  '{utterance}'{note_str}"
                )
                if p.answer_changed:
                    cols = p.before.columns or p.after.columns
                    b_rows = p.before.result_rows[:4]
                    a_rows = p.after.result_rows[:4]
                    if cols and (b_rows or a_rows):
                        col_w = [
                            max(len(c), max(
                                (len(str(r[i])) for rows in (b_rows, a_rows)
                                 for r in rows if i < len(r)),
                                default=0,
                            ))
                            for i, c in enumerate(cols)
                        ]
                        _sep = "  "
                        header = _sep.join(c.ljust(col_w[i]) for i, c in enumerate(cols))
                        sep_line = _sep.join("-" * col_w[i] for i in range(len(cols)))
                        lines.append(f"       {header}")
                        lines.append(f"       {sep_line}")
                        for br, ar in zip(b_rows, a_rows):
                            b_cells = _sep.join(
                                str(br[i] if i < len(br) else "").ljust(col_w[i])
                                for i in range(len(cols))
                            )
                            a_cells = _sep.join(
                                str(ar[i] if i < len(ar) else "").ljust(col_w[i])
                                for i in range(len(cols))
                            )
                            lines.append(f"       before  {b_cells}")
                            lines.append(f"       after   {a_cells}")
                        # any extra rows from the longer side
                        for remaining in b_rows[len(a_rows):]:
                            b_cells = _sep.join(
                                str(remaining[i] if i < len(remaining) else "").ljust(col_w[i])
                                for i in range(len(cols))
                            )
                            lines.append(f"       before  {b_cells}")
                        for remaining in a_rows[len(b_rows):]:
                            a_cells = _sep.join(
                                str(remaining[i] if i < len(remaining) else "").ljust(col_w[i])
                                for i in range(len(cols))
                            )
                            lines.append(f"       after   {a_cells}")
            lines.append("")

        return "\n".join(lines)

    def to_json(self, path: str) -> None:
        """Serialise the report to *path* as JSON."""
        data = {
            "run_at": self.run_at,
            "provider": self.provider,
            "model_name": self.model_name,
            "total": self.total,
            "changed": self.changed,
            "sql_drifted": self.sql_drifted,
            "pairs": [
                {
                    "drift_id": p.drift_id,
                    "event_description": p.event_description,
                    "answer_changed": p.answer_changed,
                    "sql_changed": p.sql_changed,
                    "row_count_delta": p.row_count_delta,
                    "before": {
                        "case_id": p.before.case_id,
                        "sql_generated": p.before.sql_generated,
                        "row_count": p.before.row_count,
                        "result_rows": p.before.result_rows,
                    },
                    "after": {
                        "case_id": p.after.case_id,
                        "sql_generated": p.after.sql_generated,
                        "row_count": p.after.row_count,
                        "result_rows": p.after.result_rows,
                    },
                }
                for p in self.pairs
            ],
        }
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, default=str)


__all__ = ["DriftEvalCase", "DriftPairResult", "DriftEvalReport", "make_domain_event_fn"]
