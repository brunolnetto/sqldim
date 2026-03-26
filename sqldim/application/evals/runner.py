"""EvalRunner — end-to-end evaluation runner for the NL interface (§11.10 evals).

Runs each :class:`~sqldim.evals.cases.EvalCase` in :data:`EVAL_SUITE` against
the real NL graph, captures per-hop tracing, latency, and result quality, and
aggregates results into an :class:`EvalReport`.

Default model: ``openai:gpt-4o-mini`` (cost-effective; change via
``EvalRunner(provider="...", model="...")``)

Usage::

    from sqldim.evals.runner import EvalRunner

    report = EvalRunner().run()              # all cases
    report = EvalRunner().run(tags=["entity"])  # subset by tag
    report = EvalRunner().run(datasets=["ecommerce"])  # subset by dataset

    print(report.summary())
    report.to_json("evals_report.json")
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqldim.application.ask import (
    DatasetPipelineSource,
    build_registry_from_schema,
    load_dataset,
    make_default_budget,
)
from sqldim.core.query.dgm.nl._agent_types import DGMContext, NLInterfaceState
from sqldim.core.query.dgm.nl._agents import make_model
from sqldim.core.query.dgm.nl._graph import build_nl_graph
from sqldim.application.evals.cases import EVAL_SUITE, EvalCase
from sqldim.application.evals.metrics import (
    check_columns_present,
    check_has_result,
    check_hop_budget,
    check_table_referenced,
    score_case,
)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class EvalResult:
    """Outcome of a single :class:`EvalCase` run.

    Attributes
    ----------
    case_id:
        Corresponds to :attr:`EvalCase.id`.
    dataset:
        Dataset name the case ran against.
    utterance:
        The NL question that was posed.
    passed:
        ``True`` when every enabled assertion passes.
    score:
        Weighted [0.0, 1.0] quality score.
    visited_nodes:
        Ordered list of graph node names that were executed.
    hop_count:
        ``len(visited_nodes)`` — convenience alias.
    latency_ms:
        Wall-clock time from graph invocation to completion (milliseconds).
    row_count:
        Number of rows in the query result (0 when no result).
    columns:
        Column names returned by the query.
    sql_generated:
        The SQL string built by the ``dag_construction`` node (via ``q_current``).
    explanation:
        Natural-language explanation produced by the ``explanation_rendering`` node.
    check_details:
        Human-readable pass/fail messages for each assertion.
    error:
        Exception message if the run raised unexpectedly (``None`` on success).
    tags:
        Propagated from the source :class:`EvalCase`.
    """

    case_id: str
    dataset: str
    utterance: str
    passed: bool
    score: float
    visited_nodes: list[str]
    hop_count: int
    latency_ms: float
    row_count: int
    columns: list[str]
    sql_generated: str | None
    explanation: str | None
    check_details: dict[str, str]
    error: str | None
    tags: list[str]


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclass
class EvalReport:
    """Aggregated results from an :class:`EvalRunner` run.

    Attributes
    ----------
    run_at:
        ISO-8601 UTC timestamp of when the run started.
    provider:
        LLM provider used.
    model_name:
        LLM model name used.
    results:
        Ordered list of :class:`EvalResult` objects, one per case.
    """

    run_at: str
    provider: str
    model_name: str
    results: list[EvalResult] = field(default_factory=list)

    # ── Statistics ──────────────────────────────────────────────────────────

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed(self) -> int:
        return self.total - self.passed

    @property
    def pass_rate(self) -> float:
        if not self.total:
            return 0.0
        return round(self.passed / self.total, 4)

    @property
    def avg_score(self) -> float:
        if not self.results:
            return 0.0
        return round(sum(r.score for r in self.results) / len(self.results), 4)

    @property
    def avg_hops(self) -> float:
        if not self.results:
            return 0.0
        return round(sum(r.hop_count for r in self.results) / len(self.results), 2)

    @property
    def avg_latency_ms(self) -> float:
        if not self.results:
            return 0.0
        return round(sum(r.latency_ms for r in self.results) / len(self.results), 1)

    def by_dataset(self) -> dict[str, list[EvalResult]]:
        """Group results by dataset name."""
        out: dict[str, list[EvalResult]] = {}
        for r in self.results:
            out.setdefault(r.dataset, []).append(r)
        return out

    def by_tag(self) -> dict[str, list[EvalResult]]:
        """Group results by tag (a result may appear under multiple tags)."""
        out: dict[str, list[EvalResult]] = {}
        for r in self.results:
            for tag in r.tags:
                out.setdefault(tag, []).append(r)
        return out

    def summary(self) -> str:
        """One-line human-readable summary."""
        return (
            f"EvalReport [{self.run_at}] "
            f"provider={self.provider} model={self.model_name} | "
            f"{self.passed}/{self.total} passed ({self.pass_rate:.0%}) | "
            f"avg_score={self.avg_score:.3f} "
            f"avg_hops={self.avg_hops:.1f} "
            f"avg_latency={self.avg_latency_ms:.0f}ms"
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialise report to a plain dict (JSON-safe)."""

        def _ds_summary(rs: list[EvalResult]) -> dict[str, Any]:
            return {
                "total": len(rs),
                "passed": sum(1 for r in rs if r.passed),
                "avg_hops": round(sum(r.hop_count for r in rs) / len(rs), 2),
            }

        return {
            "run_at": self.run_at,
            "provider": self.provider,
            "model_name": self.model_name,
            "summary": {
                "total": self.total,
                "passed": self.passed,
                "failed": self.failed,
                "pass_rate": self.pass_rate,
                "avg_score": self.avg_score,
                "avg_hops": self.avg_hops,
                "avg_latency_ms": self.avg_latency_ms,
            },
            "by_dataset": {ds: _ds_summary(rs) for ds, rs in self.by_dataset().items()},
            "results": [asdict(r) for r in self.results],
        }

    def to_json(self, path: str, indent: int = 2) -> None:
        """Write the report as a JSON file to *path*."""
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(
                self.to_dict(), fh, indent=indent, default=str, ensure_ascii=False
            )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


class EvalRunner:
    """Runs :class:`EvalCase` objects against the live NL graph.

    Parameters
    ----------
    provider:
        LLM provider name accepted by :func:`~sqldim.core.query.dgm.nl._agents.make_model`.
        Default: ``"openai"``.
    model_name:
        Model identifier for the chosen provider.
        Default: ``"gpt-4o-mini"``.
    recursion_limit:
        LangGraph recursion limit per invocation.  Increase for complex queries
        that legitimately cycle through clarification or rewrite loops.
        Default: ``25``.
    verbose:
        Print per-case progress to stdout.
    """

    def __init__(
        self,
        provider: str = "openai",
        model_name: str = "gpt-4o-mini",
        *,
        recursion_limit: int = 25,
        verbose: bool = True,
    ) -> None:
        self.provider = provider
        self.model_name = model_name
        self.recursion_limit = recursion_limit
        self.verbose = verbose

    # ── Public API ─────────────────────────────────────────────────────────

    @staticmethod
    def _filter_suite(
        cases: list[EvalCase] | None,
        datasets: list[str] | None,
        tags: list[str] | None,
    ) -> list[EvalCase]:
        """Apply dataset and tag filters.  Defaults to :data:`EVAL_SUITE` when *cases* is ``None``."""
        suite: list[EvalCase] = EVAL_SUITE if cases is None else list(cases)

        def _passes(c: EvalCase) -> bool:
            return (not datasets or c.dataset in datasets) and (
                not tags or bool(set(tags).intersection(c.tags))
            )

        return [c for c in suite if _passes(c)]

    def _log_case_result(self, result: "EvalResult") -> None:
        """Print a one-line progress update for *result* when verbose."""
        status = "PASS" if result.passed else "FAIL"
        print(
            f"{status} | score={result.score:.2f} "
            f"hops={result.hop_count} "
            f"rows={result.row_count} "
            f"latency={result.latency_ms:.0f}ms"
        )

    def run(
        self,
        cases: list[EvalCase] | None = None,
        *,
        datasets: list[str] | None = None,
        tags: list[str] | None = None,
    ) -> EvalReport:
        """Execute all (or filtered) eval cases and return an :class:`EvalReport`.

        Parameters
        ----------
        cases:
            Override the case list.  Uses :data:`EVAL_SUITE` when ``None``.
        datasets:
            Restrict to cases whose ``dataset`` is in this list.
        tags:
            Restrict to cases that carry at least one of the given tags.
        """
        suite = self._filter_suite(cases, datasets, tags)

        model = make_model(self.provider, self.model_name)
        report = EvalReport(
            run_at=datetime.now(timezone.utc).isoformat(),
            provider=self.provider,
            model_name=self.model_name,
        )

        for case in suite:
            if self.verbose:
                print(f"  [{case.id}] {case.utterance!r} ...", end=" ", flush=True)
            result = self._run_one(case, model)
            report.results.append(result)
            if self.verbose:
                self._log_case_result(result)

        if self.verbose:
            print(f"\n{report.summary()}")

        return report

    # ── Internal helpers ────────────────────────────────────────────────────

    @staticmethod
    def _extract_state_fields(
        raw_state: dict[str, Any],
    ) -> tuple[
        list[str], dict[str, Any] | None, str | None, str | None, int, list[str]
    ]:
        """Unpack the six result fields from *raw_state*."""
        result_dict: dict[str, Any] | None = raw_state.get("result")
        res = result_dict if result_dict is not None else {}
        return (
            raw_state.get("visited_nodes", []),
            result_dict,
            raw_state.get("explanation"),
            raw_state.get("q_current"),
            res.get("count", 0),
            res.get("columns", []),
        )

    @staticmethod
    def _compute_pass_score(
        checks: dict[str, tuple[bool, str]], error: str | None
    ) -> tuple[bool, float]:
        """Return ``(all_passed, score)`` for *checks* after a graph run."""
        all_passed = error is None and all(ok for ok, _ in checks.values())
        score = score_case(*checks.values()) if not error else 0.0
        return all_passed, score

    def _run_one(self, case: EvalCase, model: Any) -> EvalResult:
        """Execute a single case and return an :class:`EvalResult`."""
        try:
            dataset = load_dataset(case.dataset)
        except KeyError as exc:
            return EvalResult(
                case_id=case.id,
                dataset=case.dataset,
                utterance=case.utterance,
                passed=False,
                score=0.0,
                visited_nodes=[],
                hop_count=0,
                latency_ms=0.0,
                row_count=0,
                columns=[],
                sql_generated=None,
                explanation=None,
                check_details={"dataset_load": f"KeyError: {exc}"},
                error=str(exc),
                tags=list(case.tags),
            )

        source = DatasetPipelineSource(dataset)
        source.setup()
        try:
            con = source.get_connection()
            table_names = source.get_table_names()
            registry = build_registry_from_schema(con, table_names)
            budget = make_default_budget()
            ctx = DGMContext(entity_registry=registry, budget=budget, con=con)

            graph = build_nl_graph(context=ctx, model=model)

            t0 = time.perf_counter()
            raw_state, error = self._invoke_graph_safe(
                graph, case.utterance, f"eval-{case.id}", self.recursion_limit
            )
            latency_ms = (time.perf_counter() - t0) * 1000
        finally:
            source.teardown()

        (
            visited_nodes,
            result_dict,
            explanation,
            sql_generated,
            row_count,
            columns,
        ) = self._extract_state_fields(raw_state)
        checks = self._build_checks(case, result_dict, sql_generated, visited_nodes)
        all_passed, score = self._compute_pass_score(checks, error)

        return EvalResult(
            case_id=case.id,
            dataset=case.dataset,
            utterance=case.utterance,
            passed=all_passed,
            score=score,
            visited_nodes=visited_nodes,
            hop_count=len(visited_nodes),
            latency_ms=round(latency_ms, 1),
            row_count=row_count,
            columns=columns,
            sql_generated=sql_generated,
            explanation=explanation,
            check_details={k: detail for k, (_, detail) in checks.items()},
            error=error,
            tags=list(case.tags),
        )

    @staticmethod
    def _invoke_graph_safe(
        graph: Any,
        utterance: str,
        thread_id: str,
        recursion_limit: int,
    ) -> tuple[dict[str, Any], str | None]:
        """Invoke *graph* and catch recursion / unexpected errors.

        Returns ``(raw_state, error)`` where *error* is ``None`` on success.
        """
        from langgraph.errors import GraphRecursionError

        try:
            raw_state = graph.invoke(
                NLInterfaceState(utterance=utterance).model_dump(),
                config={
                    "configurable": {"thread_id": thread_id},
                    "recursion_limit": recursion_limit,
                },
            )
            return raw_state, None
        except GraphRecursionError:
            return {}, "GraphRecursionError"
        except Exception as exc:  # noqa: BLE001
            return {}, repr(exc)

    @staticmethod
    def _build_checks(
        case: EvalCase,
        result_dict: dict[str, Any] | None,
        sql_generated: str | None,
        visited_nodes: list[str],
    ) -> dict[str, tuple[bool, str]]:
        """Build the assertion map for *case* given graph output fields."""
        checks: dict[str, tuple[bool, str]] = {}
        if case.expect_result:
            checks["has_result"] = check_has_result(result_dict)
        if case.expect_columns:
            checks["columns_present"] = check_columns_present(
                result_dict, case.expect_columns
            )
        if case.expect_table:
            checks["table_referenced"] = check_table_referenced(
                sql_generated, case.expect_table
            )
        checks["hop_budget"] = check_hop_budget(visited_nodes, case.max_hops)
        return checks


__all__ = ["EvalResult", "EvalReport", "EvalRunner"]
