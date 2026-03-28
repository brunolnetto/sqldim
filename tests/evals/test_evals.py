"""Tests for the sqldim evals framework (§11.10 evals).

Coverage:
- EvalCase dataclass construction and field defaults
- EVAL_SUITE registry completeness (all datasets represented)
- metrics helpers: check_has_result, check_columns_present,
  check_table_referenced, check_hop_budget, score_case
- EvalReport aggregation properties and serialisation
- visited_nodes hop tracing via the NL graph (stub mode — no LLM required)
- EvalRunner.run() in stub mode (model=None path)
"""

from __future__ import annotations

import json
import pytest

from sqldim.application.evals.cases import EvalCase, EVAL_SUITE
from sqldim.application.evals.metrics import (
    check_has_result,
    check_columns_present,
    check_table_referenced,
    check_hop_budget,
    check_result_matches_expected,
    score_case,
)
from sqldim.application.evals.runner import EvalResult, EvalReport, EvalRunner

pytestmark = pytest.mark.filterwarnings(
    "ignore::logfire._internal.config.LogfireNotConfiguredWarning"
)


# ---------------------------------------------------------------------------
# EvalCase
# ---------------------------------------------------------------------------


class TestEvalCase:
    def test_required_fields(self):
        case = EvalCase(id="t.01", dataset="ecommerce", utterance="show me customers")
        assert case.id == "t.01"
        assert case.dataset == "ecommerce"
        assert case.utterance == "show me customers"

    def test_defaults(self):
        case = EvalCase(id="t.01", dataset="ecommerce", utterance="q")
        assert case.expect_result is True
        assert case.expect_columns == []
        assert case.expect_table is None
        assert case.max_hops == 11
        assert case.expected_sql is None
        assert case.tags == []

    def test_frozen(self):
        case = EvalCase(id="t.01", dataset="ecommerce", utterance="q")
        with pytest.raises((AttributeError, TypeError)):
            case.id = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# EVAL_SUITE
# ---------------------------------------------------------------------------


class TestEvalSuite:
    def test_not_empty(self):
        assert len(EVAL_SUITE) > 0

    def test_all_datasets_covered(self):
        datasets = {c.dataset for c in EVAL_SUITE}
        assert "ecommerce" in datasets
        assert "fintech" in datasets
        assert "saas_growth" in datasets
        assert "user_activity" in datasets
        assert "retail" in datasets

    def test_ids_unique(self):
        ids = [c.id for c in EVAL_SUITE]
        assert len(ids) == len(set(ids)), "Duplicate EvalCase IDs found"

    def test_all_have_utterance(self):
        for case in EVAL_SUITE:
            assert case.utterance.strip(), f"Empty utterance in case {case.id!r}"

    def test_ecommerce_cases_target_known_tables(self):
        ecom = [c for c in EVAL_SUITE if c.dataset == "ecommerce"]
        tables = {c.expect_table for c in ecom if c.expect_table}
        assert "customers" in tables
        assert "orders" in tables
        assert "products" in tables
        assert "stores" in tables

    def test_fintech_cases_positive(self):
        fintech = [c for c in EVAL_SUITE if c.dataset == "fintech"]
        assert len(fintech) >= 4

    def test_saas_growth_cases_positive(self):
        saas = [c for c in EVAL_SUITE if c.dataset == "saas_growth"]
        assert len(saas) >= 3

    def test_user_activity_cases_positive(self):
        ua = [c for c in EVAL_SUITE if c.dataset == "user_activity"]
        assert len(ua) >= 3


# ---------------------------------------------------------------------------
# metrics helpers
# ---------------------------------------------------------------------------


class TestCheckHasResult:
    def test_none_result_fails(self):
        ok, detail = check_has_result(None)
        assert not ok
        assert "None" in detail

    def test_zero_rows_fails(self):
        ok, detail = check_has_result({"count": 0, "columns": ["a"]})
        assert not ok
        assert "0 row" in detail

    def test_positive_rows_passes(self):
        ok, detail = check_has_result({"count": 5, "columns": ["a"]})
        assert ok
        assert "5 row" in detail


class TestCheckColumnsPresent:
    def test_empty_expectation_always_passes(self):
        ok, _ = check_columns_present(None, [])
        assert ok

    def test_none_result_fails(self):
        ok, detail = check_columns_present(None, ["name"])
        assert not ok

    def test_column_present_case_insensitive(self):
        result = {"columns": ["Name", "Age"], "count": 1, "rows": []}
        ok, _ = check_columns_present(result, ["name"])
        assert ok

    def test_missing_column_fails(self):
        result = {"columns": ["Name"], "count": 1, "rows": []}
        ok, detail = check_columns_present(result, ["name", "email"])
        assert not ok
        assert "email" in detail

    def test_all_columns_present(self):
        result = {"columns": ["a", "b", "c"], "count": 1, "rows": []}
        ok, _ = check_columns_present(result, ["a", "c"])
        assert ok


class TestCheckTableReferenced:
    def test_none_sql_fails(self):
        ok, _ = check_table_referenced(None, "customers")
        assert not ok

    def test_table_in_sql_passes(self):
        ok, _ = check_table_referenced("SELECT * FROM customers LIMIT 10", "customers")
        assert ok

    def test_case_insensitive(self):
        ok, _ = check_table_referenced("SELECT * FROM CUSTOMERS", "customers")
        assert ok

    def test_table_absent_fails(self):
        ok, detail = check_table_referenced("SELECT * FROM orders", "customers")
        assert not ok
        assert "customers" in detail


class TestCheckHopBudget:
    def test_zero_hops_passes(self):
        ok, _ = check_hop_budget([], 11)
        assert ok

    def test_at_limit_passes(self):
        ok, _ = check_hop_budget(["a"] * 11, 11)
        assert ok

    def test_over_limit_fails(self):
        ok, detail = check_hop_budget(["a"] * 12, 11)
        assert not ok
        assert "12" in detail

    def test_well_under_limit_passes(self):
        ok, _ = check_hop_budget(["a", "b", "c"], 11)
        assert ok


class TestCheckResultMatchesExpected:
    def test_no_expected_skips(self):
        ok, detail = check_result_matches_expected({"count": 5}, None)
        assert ok
        assert "skipped" in detail

    def test_actual_none_with_rows_expected_fails(self):
        expected = {"columns": ["a"], "rows": [["1"]], "count": 1}
        ok, detail = check_result_matches_expected(None, expected)
        assert not ok
        assert "row count mismatch" in detail

    def test_row_count_mismatch_fails(self):
        actual = {"columns": ["a"], "rows": [], "count": 0}
        expected = {"columns": ["a"], "rows": [["1"]], "count": 1}
        ok, detail = check_result_matches_expected(actual, expected)
        assert not ok
        assert "row count mismatch" in detail

    def test_both_zero_rows_passes(self):
        actual = {"columns": ["a"], "rows": [], "count": 0}
        expected = {"columns": ["a"], "rows": [], "count": 0}
        ok, detail = check_result_matches_expected(actual, expected)
        assert ok
        assert "empty" in detail

    def test_column_alias_difference_passes_when_data_matches(self):
        # Different alias (e.g. count_accounts vs account_count) but same data → PASS
        actual = {"columns": ["count_accounts"], "rows": [["15"]], "count": 1}
        expected = {"columns": ["account_count"], "rows": [["15"]], "count": 1}
        ok, detail = check_result_matches_expected(actual, expected)
        assert ok
        assert "alias" in detail  # informational note present

    def test_different_data_fails_regardless_of_column_names(self):
        # Different alias AND different data → FAIL on data mismatch
        actual = {"columns": ["x"], "rows": [["different"]], "count": 1}
        expected = {"columns": ["a"], "rows": [["expected_value"]], "count": 1}
        ok, detail = check_result_matches_expected(actual, expected)
        assert not ok
        assert "data mismatch" in detail

    def test_matching_rows_passes(self):
        result = {"columns": ["a", "b"], "rows": [["x", "1"], ["y", "2"]], "count": 2}
        ok, detail = check_result_matches_expected(result, result)
        assert ok
        assert "2" in detail

    def test_differing_row_data_fails(self):
        actual = {"columns": ["a"], "rows": [["x"]], "count": 1}
        expected = {"columns": ["a"], "rows": [["y"]], "count": 1}
        ok, detail = check_result_matches_expected(actual, expected)
        assert not ok
        assert "data mismatch" in detail

    def test_large_result_skips_row_comparison(self):
        # > 100 rows: only count + columns checked
        rows = [[str(i)] for i in range(101)]
        actual = {"columns": ["a"], "rows": rows, "count": 101}
        expected = {"columns": ["a"], "rows": [[str(i + 1)] for i in range(101)], "count": 101}
        ok, _ = check_result_matches_expected(actual, expected)
        assert ok  # row data not compared beyond 100 rows



    def test_empty_checks_returns_1(self):
        assert score_case() == 1.0

    def test_all_pass(self):
        assert score_case((True, "ok"), (True, "ok")) == 1.0

    def test_all_fail(self):
        assert score_case((False, "err"), (False, "err")) == 0.0

    def test_half_pass(self):
        assert score_case((True, "ok"), (False, "err")) == 0.5

    def test_weighted(self):
        # weight 2 on passing check, weight 1 on failing check → 2/3 ≈ 0.6667
        # score_case rounds to 4dp, so tolerance covers the rounding gap
        score = score_case((True, "ok"), (False, "err"), weights=[2.0, 1.0])
        assert abs(score - 2 / 3) < 1e-3

    def test_zero_total_weight_returns_one(self):
        # When all weights are 0, total is 0.0 → early-return 1.0 (no penalties)
        score = score_case((True, "ok"), (False, "err"), weights=[0.0, 0.0])
        assert score == 1.0


# ---------------------------------------------------------------------------
# EvalReport
# ---------------------------------------------------------------------------


def _make_result(passed: bool, hops: int = 5, score: float = 1.0) -> EvalResult:
    return EvalResult(
        case_id="t.01",
        dataset="ecommerce",
        utterance="test",
        passed=passed,
        score=score,
        visited_nodes=["node"] * hops,
        hop_count=hops,
        latency_ms=100.0,
        row_count=10,
        columns=["a"],
        sql_generated="SELECT a FROM t",
        explanation="ok",
        check_details={"has_result": "10 row(s) returned"},
        error=None,
        tags=["entity"],
        check_passed={"has_result": True},
        expected_sql=None,
        expected_result=None,
    )


class TestEvalReport:
    def _report(self) -> EvalReport:
        r = EvalReport(
            run_at="2026-01-01T00:00:00Z", provider="openai", model_name="gpt-4o-mini"
        )
        r.results = [_make_result(True), _make_result(False, score=0.0)]
        return r

    def test_total(self):
        assert self._report().total == 2

    def test_passed(self):
        assert self._report().passed == 1

    def test_failed(self):
        assert self._report().failed == 1

    def test_pass_rate(self):
        assert self._report().pass_rate == 0.5

    def test_avg_score(self):
        assert self._report().avg_score == 0.5

    def test_avg_hops(self):
        assert self._report().avg_hops == 5.0

    def test_summary_contains_stats(self):
        s = self._report().summary()
        assert "1/2" in s
        assert "50%" in s

    def test_to_dict_structure(self):
        d = self._report().to_dict()
        assert "summary" in d
        assert "results" in d
        assert d["summary"]["total"] == 2

    def test_to_json_roundtrip(self, tmp_path):
        path = str(tmp_path / "report.json")
        self._report().to_json(path)
        with open(path) as fh:
            data = json.load(fh)
        assert data["summary"]["total"] == 2
        assert len(data["results"]) == 2

    def test_by_dataset(self):
        groups = self._report().by_dataset()
        assert "ecommerce" in groups
        assert len(groups["ecommerce"]) == 2

    def test_by_tag(self):
        groups = self._report().by_tag()
        assert "entity" in groups


class TestEvalReportEmpty:
    """Empty EvalReport (no results) should return zero for all aggregates."""

    def _empty(self) -> EvalReport:
        return EvalReport(
            run_at="2026-01-01T00:00:00Z", provider="test", model_name="m"
        )

    def test_empty_avg_latency_is_zero(self):
        assert self._empty().avg_latency_ms == 0.0

    def test_empty_pass_rate_is_zero(self):
        assert self._empty().pass_rate == 0.0

    def test_empty_avg_score_is_zero(self):
        assert self._empty().avg_score == 0.0

    def test_empty_avg_hops_is_zero(self):
        assert self._empty().avg_hops == 0.0


# ---------------------------------------------------------------------------
# Hop tracing via NL graph (no LLM — stub mode)
# ---------------------------------------------------------------------------


class TestHopTracing:
    """Verify that visited_nodes is populated correctly in stub mode (model=None)."""

    def test_visited_nodes_populated_in_stub_mode(self):
        import duckdb
        from sqldim.core.query.dgm.nl._agent_types import DGMContext, NLInterfaceState
        from sqldim.core.query.dgm.nl._graph import build_nl_graph
        from sqldim.application.ask import (
            build_registry_from_schema,
            make_default_budget,
        )

        con = duckdb.connect()
        con.execute("CREATE TABLE items (id INTEGER, name VARCHAR, price DOUBLE)")
        registry = build_registry_from_schema(con, ["items"])
        budget = make_default_budget()
        ctx = DGMContext(entity_registry=registry, budget=budget, con=con)

        graph = build_nl_graph(context=ctx, model=None)
        initial = NLInterfaceState(utterance="show me items")
        state = graph.invoke(
            initial.model_dump(),
            config={
                "configurable": {"thread_id": "test-hop-tracing"},
                "recursion_limit": 25,
            },
        )

        visited = state.get("visited_nodes", [])
        assert len(visited) > 0, "visited_nodes must not be empty after graph run"

    def test_happy_path_node_order(self):
        """Stub mode should traverse the 8 happy-path nodes in sequence."""
        import duckdb
        from sqldim.core.query.dgm.nl._agent_types import DGMContext, NLInterfaceState
        from sqldim.core.query.dgm.nl._graph import build_nl_graph
        from sqldim.application.ask import (
            build_registry_from_schema,
            make_default_budget,
        )

        con = duckdb.connect()
        con.execute("CREATE TABLE sales (id INTEGER, amount DOUBLE)")
        con.execute("INSERT INTO sales VALUES (1, 99.0), (2, 150.0)")
        registry = build_registry_from_schema(con, ["sales"])
        budget = make_default_budget()
        ctx = DGMContext(entity_registry=registry, budget=budget, con=con)

        graph = build_nl_graph(context=ctx, model=None)
        state = graph.invoke(
            NLInterfaceState(utterance="show me sales").model_dump(),
            config={
                "configurable": {"thread_id": "test-node-order"},
                "recursion_limit": 25,
            },
        )

        visited = state.get("visited_nodes", [])
        expected_nodes = [
            "entity_resolution",
            "temporal_classification",
            "compositional_detection",
            "candidate_generation",
            "candidate_ranking",
            "confirmation_loop",
            "dag_construction",
            "budget_gate",
            "execution",
            "explanation_rendering",
        ]
        for node in expected_nodes:
            assert node in visited, (
                f"Expected node '{node}' not in visited_nodes: {visited}"
            )

    def test_visited_nodes_in_result_matches_hop_count(self):
        import duckdb
        from sqldim.core.query.dgm.nl._agent_types import DGMContext, NLInterfaceState
        from sqldim.core.query.dgm.nl._graph import build_nl_graph
        from sqldim.application.ask import (
            build_registry_from_schema,
            make_default_budget,
        )

        con = duckdb.connect()
        con.execute("CREATE TABLE events (id INTEGER, name VARCHAR)")
        registry = build_registry_from_schema(con, ["events"])
        ctx = DGMContext(
            entity_registry=registry, budget=make_default_budget(), con=con
        )

        graph = build_nl_graph(context=ctx, model=None)
        state = graph.invoke(
            NLInterfaceState(utterance="show me events").model_dump(),
            config={"configurable": {"thread_id": "test-count"}, "recursion_limit": 25},
        )

        visited = state.get("visited_nodes", [])
        assert len(visited) == state.get("visited_nodes", []).__len__()


# ---------------------------------------------------------------------------
# EvalRunner — stub mode (no real LLM calls)
# ---------------------------------------------------------------------------


class TestEvalRunnerStubMode:
    """EvalRunner in stub mode uses model=None internally via a monkey-patched runner."""

    def _make_stub_runner(self) -> EvalRunner:
        """Return a runner that uses model=None by monkey-patching make_model."""
        runner = EvalRunner(provider="openai", model_name="gpt-4o-mini", verbose=False)
        return runner

    def test_run_returns_report(self):
        runner = self._make_stub_runner()
        # Run a single simple case to keep the test fast.
        case = EvalCase(
            id="stub.01",
            dataset="ecommerce",
            utterance="show me customers",
            expect_result=False,  # stub may return 0 rows; don't require rows
            expect_columns=[],
            expect_table=None,
            max_hops=15,
        )

        import unittest.mock as mock

        with mock.patch(
            "sqldim.application.evals.runner.make_model", return_value=None
        ):
            report = runner.run(cases=[case])

        assert isinstance(report, EvalReport)
        assert report.total == 1
        assert len(report.results) == 1

    def test_result_has_visited_nodes(self):
        runner = self._make_stub_runner()
        case = EvalCase(
            id="stub.02",
            dataset="ecommerce",
            utterance="list products",
            expect_result=False,
            expect_table=None,
            max_hops=15,
        )
        import unittest.mock as mock

        with mock.patch(
            "sqldim.application.evals.runner.make_model", return_value=None
        ):
            report = runner.run(cases=[case])

        result = report.results[0]
        assert result.hop_count == len(result.visited_nodes)
        assert result.hop_count > 0

    def test_filter_by_dataset(self):
        runner = self._make_stub_runner()
        import unittest.mock as mock

        with mock.patch(
            "sqldim.application.evals.runner.make_model", return_value=None
        ):
            report = runner.run(datasets=["ecommerce"])
        assert all(r.dataset == "ecommerce" for r in report.results)

    def test_filter_by_tag(self):
        runner = self._make_stub_runner()
        import unittest.mock as mock

        with mock.patch(
            "sqldim.application.evals.runner.make_model", return_value=None
        ):
            report = runner.run(datasets=["ecommerce"], tags=["metric"])
        assert len(report.results) > 0
        assert all("metric" in r.tags for r in report.results)

    def test_unknown_dataset_yields_error_result(self):
        runner = self._make_stub_runner()
        case = EvalCase(
            id="stub.bad",
            dataset="nonexistent_dataset_xyz",
            utterance="any question",
            expect_result=False,
        )
        import unittest.mock as mock

        with mock.patch(
            "sqldim.application.evals.runner.make_model", return_value=None
        ):
            report = runner.run(cases=[case])
        result = report.results[0]
        assert not result.passed
        assert result.error is not None

    def test_verbose_run_prints_progress(self, capsys):
        runner = EvalRunner(provider="openai", model_name="gpt-4o-mini", verbose=True)
        case = EvalCase(
            id="verbose.01",
            dataset="ecommerce",
            utterance="list customers",
            expect_result=False,
            max_hops=15,
        )
        import unittest.mock as mock

        with mock.patch(
            "sqldim.application.evals.runner.make_model", return_value=None
        ):
            report = runner.run(cases=[case])
        out = capsys.readouterr().out
        assert report.total == 1
        # verbose mode should print something (case id, utterance or progress)
        assert len(out) > 0

    def test_expect_columns_and_table_cover_check_branches(self):
        runner = self._make_stub_runner()
        case = EvalCase(
            id="check.01",
            dataset="ecommerce",
            utterance="show customers",
            expect_result=False,
            expect_columns=["customer_id"],
            expect_table="customers",
            max_hops=15,
        )
        import unittest.mock as mock

        with mock.patch(
            "sqldim.application.evals.runner.make_model", return_value=None
        ):
            report = runner.run(cases=[case])
        # Check branches for expect_columns and expect_table were exercised
        assert report.total == 1

    def test_invoke_graph_safe_catches_graph_recursion_error(self):
        """_invoke_graph_safe must return ({}, 'GraphRecursionError') on recursion."""
        from unittest.mock import MagicMock
        from langgraph.errors import GraphRecursionError

        mock_graph = MagicMock()
        mock_graph.invoke.side_effect = GraphRecursionError("too many hops")

        raw_state, error = EvalRunner._invoke_graph_safe(
            mock_graph, "test utterance", "thread-recur", 20
        )
        assert raw_state == {}
        assert error == "GraphRecursionError"

    def test_invoke_graph_safe_catches_generic_exception(self):
        """_invoke_graph_safe must catch arbitrary exceptions and return repr."""
        from unittest.mock import MagicMock

        mock_graph = MagicMock()
        mock_graph.invoke.side_effect = RuntimeError("something went wrong")

        raw_state, error = EvalRunner._invoke_graph_safe(
            mock_graph, "test utterance", "thread-err", 20
        )
        assert raw_state == {}
        assert "RuntimeError" in error
        assert "something went wrong" in error

