"""Tests for `sqldim bench` CLI subcommand.

Covers:
- `sqldim bench run`         — stub-mode NL graph latency benchmarking
- Parser flag handling        — --dataset, --runs, --output
"""

from __future__ import annotations

import json
import pytest

pytestmark = pytest.mark.filterwarnings(
    "ignore::logfire._internal.config.LogfireNotConfiguredWarning"
)

from sqldim.cli import build_parser, main


# ---------------------------------------------------------------------------
# sqldim bench run — parser flags
# ---------------------------------------------------------------------------


class TestBenchRunParser:
    def test_dataset_flag_accepted(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run", "--dataset", "ecommerce"])
        assert args.dataset == "ecommerce"

    def test_dataset_default_is_none(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run"])
        assert args.dataset is None

    def test_runs_flag_accepted(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run", "--runs", "5"])
        assert args.runs == 5

    def test_runs_default_is_three(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run"])
        assert args.runs == 3

    def test_output_flag_accepted(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run", "--output", "results.json"])
        assert args.output == "results.json"

    def test_output_default_is_none(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run"])
        assert args.output is None


# ---------------------------------------------------------------------------
# sqldim bench run — behaviour
# ---------------------------------------------------------------------------


class TestBenchRunCommand:
    def test_bench_run_ecommerce_returns_zero(self, capsys):
        rc = main(["bench", "run", "--dataset", "ecommerce", "--runs", "1"])
        assert rc == 0

    def test_bench_run_prints_case_count(self, capsys):
        main(["bench", "run", "--dataset", "ecommerce", "--runs", "1"])
        out = capsys.readouterr().out
        assert "case(s)" in out

    def test_bench_run_prints_stub_mode_label(self, capsys):
        main(["bench", "run", "--dataset", "ecommerce", "--runs", "1"])
        out = capsys.readouterr().out
        assert "stub mode" in out.lower() or "no LLM" in out

    def test_bench_run_prints_latency_stats(self, capsys):
        main(["bench", "run", "--dataset", "ecommerce", "--runs", "1"])
        out = capsys.readouterr().out
        assert "mean=" in out

    def test_bench_run_prints_overall_mean(self, capsys):
        main(["bench", "run", "--dataset", "ecommerce", "--runs", "1"])
        out = capsys.readouterr().out
        assert "overall mean latency" in out

    def test_bench_run_unknown_dataset_returns_one(self, capsys):
        rc = main(["bench", "run", "--dataset", "nonexistent_ds_xyz", "--runs", "1"])
        assert rc == 1
        out = capsys.readouterr().out
        assert "No cases found" in out

    def test_bench_run_writes_json_output(self, tmp_path, capsys):
        output_path = str(tmp_path / "bench.json")
        rc = main(
            ["bench", "run", "--dataset", "ecommerce", "--runs", "1", "--output", output_path]
        )
        assert rc == 0
        assert (tmp_path / "bench.json").exists()
        with open(output_path) as fh:
            data = json.load(fh)
        assert "results" in data
        assert data["runs_per_case"] == 1

    def test_bench_run_json_result_fields(self, tmp_path, capsys):
        output_path = str(tmp_path / "bench.json")
        main(
            ["bench", "run", "--dataset", "ecommerce", "--runs", "1", "--output", output_path]
        )
        with open(output_path) as fh:
            data = json.load(fh)
        first = data["results"][0]
        assert "case_id" in first
        assert "dataset" in first
        assert "mean_ms" in first
        assert "min_ms" in first
        assert "max_ms" in first
        assert "p95_ms" in first

    def test_bench_run_prints_output_path_confirmation(self, tmp_path, capsys):
        output_path = str(tmp_path / "bench.json")
        main(
            ["bench", "run", "--dataset", "ecommerce", "--runs", "1", "--output", output_path]
        )
        out = capsys.readouterr().out
        assert "Results written to" in out
        assert output_path in out

    def test_bench_run_multiple_runs_returns_zero(self, capsys):
        rc = main(["bench", "run", "--dataset", "ecommerce", "--runs", "2"])
        assert rc == 0

    def test_bench_run_skips_unknown_dataset_in_all_mode(self, capsys, monkeypatch):
        """When --dataset is omitted, cases for unknown datasets are SKIPped."""
        from sqldim.application.evals.cases import EvalCase
        from sqldim.cli import _bench_all_datasets

        fake_case = EvalCase(
            id="fake.01",
            dataset="nonexistent_dataset_xyz",
            utterance="show me data",
            expect_result=False,
        )
        results = _bench_all_datasets([fake_case], n_runs=1)
        out = capsys.readouterr().out
        assert "SKIP" in out
        assert results == []

    def test_bench_case_stats_p95_with_twenty_runs(self):
        """p95_ms uses the 95th percentile when len(timings) >= 20."""
        from sqldim.application.evals.cases import EvalCase
        from sqldim.cli import _bench_case_stats

        case = EvalCase(
            id="p95.01",
            dataset="ecommerce",
            utterance="show me customers",
            expect_result=False,
        )
        timings = list(range(1, 21))  # 20 distinct values: 1..20
        stats = _bench_case_stats(case, "ecommerce", 20, timings)
        assert "p95_ms" in stats
        # 95th percentile of 1..20: index int(20*0.95)=19 → value 20
        assert stats["p95_ms"] == 20.0

    def test_bench_case_timings_handles_graph_recursion_error(self):
        """GraphRecursionError during graph.invoke is swallowed; timing still recorded (lines 290-291)."""
        from unittest.mock import MagicMock
        from sqldim.application.evals.cases import EvalCase
        from sqldim.cli import _bench_case_timings
        from langgraph.errors import GraphRecursionError

        case = EvalCase(
            id="rec.01",
            dataset="ecommerce",
            utterance="show me data",
            expect_result=False,
        )
        mock_graph = MagicMock()
        mock_graph.invoke.side_effect = GraphRecursionError("too deep")
        timings = _bench_case_timings(mock_graph, case, n_runs=1)
        assert len(timings) == 1
        assert timings[0] >= 0.0

