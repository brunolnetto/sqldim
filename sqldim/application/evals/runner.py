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

import concurrent.futures
import json
import time
import warnings
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
    check_result_matches_expected,
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
    node_timings: dict[str, float] = field(default_factory=dict)
    result_rows: list[list[str]] = field(default_factory=list)
    check_passed: dict[str, bool] = field(default_factory=dict)
    expected_sql: str | None = None
    expected_result: dict[str, Any] | None = None


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
        """Multi-line human-readable summary with per-dataset breakdown."""
        header = (
            f"EvalReport [{self.run_at}]\n"
            f"  provider={self.provider}  model={self.model_name}\n"
            f"  {self.passed}/{self.total} passed ({self.pass_rate:.0%})  "
            f"avg_score={self.avg_score:.3f}  "
            f"avg_hops={self.avg_hops:.1f}  "
            f"avg_latency={self.avg_latency_ms:.0f}ms"
        )
        lines = [header, ""]

        # ── per-dataset table ───────────────────────────────────────────────
        lines.append(f"  {'Dataset':<22} {'Pass':>6} {'Total':>6} {'Score':>7} {'Hops':>5} {'Latency':>10}")
        lines.append("  " + "-" * 60)
        for ds, rs in sorted(self.by_dataset().items()):
            n = len(rs)
            p = sum(1 for r in rs if r.passed)
            sc = round(sum(r.score for r in rs) / n, 3)
            hops = round(sum(r.hop_count for r in rs) / n, 1)
            lat = round(sum(r.latency_ms for r in rs) / n)
            flag = "✓" if p == n else "✗"
            lines.append(
                f"  {flag} {ds:<20} {p:>6}/{n:<4} {sc:>7.3f} {hops:>5.1f} {lat:>8}ms"
            )

        # ── failed cases ────────────────────────────────────────────────────
        failures = [r for r in self.results if not r.passed]
        if failures:
            lines.append("")
            lines.append("  Failed cases:")
            for r in failures:
                lines.append(f"    ✗ [{r.case_id}] {r.utterance!r}")
                for check, detail in r.check_details.items():
                    ok = r.check_passed.get(check, True)
                    icon = "✓" if ok else "✗"
                    lines.append(f"        {icon} {check}: {detail}")

        return "\n".join(lines)

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
    case_timeout_s:
        Hard wall-clock limit in seconds for a single case.  When the graph
        invocation exceeds this limit the case is marked failed with a
        ``Timeout`` error.  ``None`` (default) disables the limit.
    verbose:
        Print per-case progress to stdout.
    """

    def __init__(
        self,
        provider: str = "openai",
        model_name: str = "gpt-4o-mini",
        *,
        recursion_limit: int = 25,
        case_timeout_s: int | None = None,
        verbose: bool = True,
    ) -> None:
        self.provider = provider
        self.model_name = model_name
        self.recursion_limit = recursion_limit
        self.case_timeout_s = case_timeout_s
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
        """Print a detailed profiling block for *result* when verbose."""
        status = "PASS" if result.passed else "FAIL"
        icon = "✓" if result.passed else "✗"
        # ── status line ────────────────────────────────────────────────────
        print(
            f"{icon} {status} | score={result.score:.2f} "
            f"hops={result.hop_count} "
            f"rows={result.row_count} "
            f"latency={result.latency_ms:.0f}ms"
        )
        # ── node trace with per-node timing ────────────────────────────────
        if result.visited_nodes:
            if result.node_timings:
                parts = [
                    f"{n}({result.node_timings.get(n, 0):.0f}ms)"
                    for n in result.visited_nodes
                ]
            else:
                parts = result.visited_nodes
            print("      nodes : " + " → ".join(parts))
        # ── SQL ────────────────────────────────────────────────────────────
        if result.sql_generated:
            print(f"      sql   : {result.sql_generated}")
        # ── result rows mini-table ─────────────────────────────────────────
        if result.result_rows and result.columns:
            col_w = [
                max(len(c), max((len(str(r[i])) for r in result.result_rows if i < len(r)), default=0))
                for i, c in enumerate(result.columns)
            ]
            header = "  ".join(c.ljust(col_w[i]) for i, c in enumerate(result.columns))
            sep = "  ".join("-" * col_w[i] for i in range(len(result.columns)))
            print(f"      answer:")
            print(f"        {header}")
            print(f"        {sep}")
            for row in result.result_rows:
                cells = "  ".join(
                    str(row[i] if i < len(row) else "").ljust(col_w[i])
                    for i in range(len(result.columns))
                )
                print(f"        {cells}")
            if result.row_count > len(result.result_rows):
                print(f"        ... ({result.row_count} total rows)")
        # ── expected answer (shown when result_matches_expected fails) ──────
        exp_ok = result.check_passed.get("result_matches_expected", True)
        if not exp_ok and result.expected_result:
            exp_res = result.expected_result
            exp_cols: list[str] = exp_res.get("columns", [])
            exp_rows_data: list[list[str]] = exp_res.get("rows", [])[:5]
            if exp_cols and exp_rows_data:
                ecol_w = [
                    max(len(c), max((len(str(r[i])) for r in exp_rows_data if i < len(r)), default=0))
                    for i, c in enumerate(exp_cols)
                ]
                eheader = "  ".join(c.ljust(ecol_w[i]) for i, c in enumerate(exp_cols))
                esep = "  ".join("-" * ecol_w[i] for i in range(len(exp_cols)))
                print(f"      expected:")
                print(f"        {eheader}")
                print(f"        {esep}")
                for erow in exp_rows_data:
                    ecells = "  ".join(
                        str(erow[i] if i < len(erow) else "").ljust(ecol_w[i])
                        for i in range(len(exp_cols))
                    )
                    print(f"        {ecells}")
                exp_total = exp_res.get("count", 0)
                if exp_total > len(exp_rows_data):
                    print(f"        ... ({exp_total} total rows)")
        # ── per-check breakdown ────────────────────────────────────────────
        for check, detail in result.check_details.items():
            ck_pass = result.check_passed.get(check, True)
            ck_icon = "✓" if ck_pass else "✗"
            print(f"      {ck_icon} {check:<25} {detail}")
        # ── explanation ────────────────────────────────────────────────────
        if result.explanation:
            print(f"      explain: {result.explanation}")
        # ── error ──────────────────────────────────────────────────────────
        if result.error:
            print(f"      ERROR  : {result.error}")
        print()

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

        # Suppress logfire "not configured" warnings — they are expected in
        # environments where logfire has not been set up (e.g. CI / evals).
        _logfire_warning: type | None = None
        try:
            from logfire._internal.config import LogfireNotConfiguredWarning
            _logfire_warning = LogfireNotConfiguredWarning
        except ImportError:
            pass

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*logfire.*")
            if _logfire_warning is not None:
                warnings.filterwarnings("ignore", category=_logfire_warning)

            model = make_model(self.provider, self.model_name)
            report = EvalReport(
                run_at=datetime.now(timezone.utc).isoformat(),
                provider=self.provider,
                model_name=self.model_name,
            )

            for case in suite:
                if self.verbose:
                    print(f"  [{case.id}] {case.utterance!r}")
                result = self._run_one(case, model)
                report.results.append(result)
                if self.verbose:
                    self._log_case_result(result)

        if self.verbose:
            print(report.summary())

        return report

    def run_drift_suite(
        self,
        drift_cases: "list[DriftEvalCase]",
    ) -> "DriftEvalReport":
        """Run before/after evaluations around a pipeline event.

        For each :class:`DriftEvalCase`:

        1. Build the pipeline with ``pipeline_source_factory()``.
        2. Run every ``EvalCase`` in ``cases`` → *before* snapshot.
        3. Execute ``event_fn(con)`` to mutate the live connection.
        4. Re-run the same cases → *after* snapshot.
        5. Collect results into a :class:`DriftEvalReport`.

        The connection is kept open across steps 2-4 so the agent observes the
        mutated state in step 4 without rebuilding the full pipeline.
        """
        from sqldim.application.evals.drift import DriftEvalReport, DriftPairResult

        model = make_model(self.provider, self.model_name)
        report = DriftEvalReport(
            run_at=datetime.now(timezone.utc).isoformat(),
            provider=self.provider,
            model_name=self.model_name,
        )

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*logfire.*")
            try:
                from logfire._internal.config import LogfireNotConfiguredWarning
                warnings.filterwarnings("ignore", category=LogfireNotConfiguredWarning)
            except ImportError:
                pass

            for dc in drift_cases:
                source = dc.pipeline_source_factory()
                source.setup()
                try:
                    con = source.get_connection()
                    table_names = source.get_table_names()

                    before_results: list = []
                    for case in dc.cases:
                        if self.verbose:
                            print(f"  [{dc.id}] BEFORE  {case.utterance!r}")
                        before = self._run_on_connection(
                            case, con, table_names, model, thread_id_suffix="-before"
                        )
                        if self.verbose:
                            self._log_case_result(before)
                        before_results.append(before)

                    # Apply the event — mutates the live connection
                    dc.event_fn(con)

                    for case, before in zip(dc.cases, before_results):
                        if self.verbose:
                            print(
                                f"  [{dc.id}] AFTER   {case.utterance!r}"
                                f"  (event: {dc.event_description})"
                            )
                        after = self._run_on_connection(
                            case, con, table_names, model, thread_id_suffix="-after"
                        )
                        if self.verbose:
                            self._log_case_result(after)

                        pair = DriftPairResult(
                            drift_id=dc.id,
                            event_description=dc.event_description,
                            before=before,
                            after=after,
                        )
                        report.pairs.append(pair)
                finally:
                    source.teardown()

        if self.verbose:
            print(report.summary())

        return report

    # ── Internal helpers ────────────────────────────────────────────────────

    @staticmethod
    def _extract_state_fields(
        raw_state: dict[str, Any],
    ) -> tuple[
        list[str], dict[str, Any] | None, str | None, str | None, int, list[str],
        dict[str, float], list[list[str]]
    ]:
        """Unpack the eight result fields from *raw_state*."""
        result_dict: dict[str, Any] | None = raw_state.get("result")
        res = result_dict if result_dict is not None else {}
        return (
            raw_state.get("visited_nodes", []),
            result_dict,
            raw_state.get("explanation"),
            raw_state.get("q_current"),
            res.get("count", 0),
            res.get("columns", []),
            raw_state.get("node_timings", {}),
            res.get("rows", [])[:50],
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
        if case.pipeline_source_factory is not None:
            source = case.pipeline_source_factory()
        else:
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
                    check_passed={"dataset_load": False},
                )
            source = DatasetPipelineSource(dataset)
        source.setup()
        try:
            con = source.get_connection()
            table_names = source.get_table_names()
            result = self._run_on_connection(case, con, table_names, model)
        finally:
            source.teardown()
        return result

    def _run_on_connection(
        self,
        case: EvalCase,
        con: Any,
        table_names: list[str],
        model: Any,
        *,
        thread_id_suffix: str = "",
    ) -> EvalResult:
        """Run *case* against an *already-open* DuckDB connection.

        This is the inner workhorse shared by :meth:`_run_one` (which manages
        source setup/teardown) and :meth:`run_drift_suite` (which reuses a
        single connection across before/after event snapshots).
        """
        registry = build_registry_from_schema(con, table_names)
        budget = make_default_budget()
        ctx = DGMContext(entity_registry=registry, budget=budget, con=con)

        graph = build_nl_graph(context=ctx, model=model)

        tid = f"eval-{case.id}{thread_id_suffix}"
        t0 = time.perf_counter()
        raw_state, error = self._invoke_graph_safe(
            graph, case.utterance, tid,
            self.recursion_limit, self.case_timeout_s,
        )
        latency_ms = (time.perf_counter() - t0) * 1000

        expected_result_dict: dict[str, Any] | None = None
        if case.expected_sql:
            try:
                cur = con.execute(case.expected_sql)
                cols = [c[0] for c in cur.description] if cur.description else []
                rows = cur.fetchall()
                expected_result_dict = {
                    "columns": cols,
                    "rows": [[str(v) for v in r] for r in rows],
                    "count": len(rows),
                }
            except Exception:  # noqa: BLE001
                pass

        (
            visited_nodes,
            result_dict,
            explanation,
            sql_generated,
            row_count,
            columns,
            node_timings,
            result_rows,
        ) = self._extract_state_fields(raw_state)
        checks = self._build_checks(case, result_dict, sql_generated, visited_nodes, expected_result_dict)
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
            node_timings=node_timings,
            result_rows=result_rows,
            check_passed={k: ok for k, (ok, _) in checks.items()},
            expected_sql=case.expected_sql,
            expected_result=expected_result_dict,
        )

    @staticmethod
    def _invoke_graph_safe(
        graph: Any,
        utterance: str,
        thread_id: str,
        recursion_limit: int,
        timeout_s: int | None = None,
    ) -> tuple[dict[str, Any], str | None]:
        """Invoke *graph* and catch recursion / unexpected errors.

        Returns ``(raw_state, error)`` where *error* is ``None`` on success.
        """
        from langgraph.errors import GraphRecursionError

        def _run() -> dict[str, Any]:
            from langgraph.errors import GraphRecursionError as _GRE  # noqa: F401
            return graph.invoke(
                NLInterfaceState(utterance=utterance).model_dump(),
                config={
                    "configurable": {"thread_id": thread_id},
                    "recursion_limit": recursion_limit,
                },
            )

        try:
            if timeout_s is not None:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    fut = pool.submit(_run)
                    try:
                        raw_state = fut.result(timeout=timeout_s)
                    except concurrent.futures.TimeoutError:
                        return {}, f"Timeout after {timeout_s}s"
            else:
                raw_state = _run()
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
        expected_result_dict: dict[str, Any] | None = None,
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
        if case.expected_sql is not None:
            checks["result_matches_expected"] = check_result_matches_expected(
                result_dict, expected_result_dict
            )
        return checks


__all__ = ["EvalResult", "EvalReport", "EvalRunner", "DriftEvalCase"]
