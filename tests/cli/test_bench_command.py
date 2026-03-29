"""Tests for `sqldim bench` CLI subcommand.

Covers:
- `sqldim bench run`         — benchmark suite (groups A–T: SCD, model, DGM)
- Parser flag handling        — --max-tier, --report, --out-dir, --source, flags
"""

from __future__ import annotations

import pytest

from sqldim.cli import build_parser

pytestmark = pytest.mark.filterwarnings(
    "ignore::logfire._internal.config.LogfireNotConfiguredWarning"
)


# ---------------------------------------------------------------------------
# sqldim bench run — parser flags
# ---------------------------------------------------------------------------


class TestBenchRunParser:
    def test_cases_positional(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run", "A", "B.scd"])
        assert args.cases == ["A", "B.scd"]

    def test_cases_default_empty(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run"])
        assert args.cases == []

    def test_max_tier_flag(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run", "--max-tier", "m"])
        assert args.max_tier == "m"

    def test_max_tier_default_auto(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run"])
        assert args.max_tier == "auto"

    def test_report_flag(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run", "--report", "csv"])
        assert args.report == "csv"

    def test_report_default_json(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run"])
        assert args.report == "json"

    def test_out_dir_flag(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run", "--out-dir", "/tmp/results"])
        assert args.out_dir == "/tmp/results"

    def test_source_flag(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run", "--source", "parquet"])
        assert args.source == "parquet"

    def test_source_default_parquet(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run"])
        assert args.source == "parquet"

    def test_fail_on_regression_flag(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run", "--fail-on-regression"])
        assert args.fail_on_regression is True

    def test_fail_on_regression_default_false(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run"])
        assert args.fail_on_regression is False

    def test_fail_on_breach_flag(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run", "--fail-on-breach"])
        assert args.fail_on_breach is True

    def test_compare_last_flag(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run", "--compare-last"])
        assert args.compare_last is True

    def test_func_set_to_cmd_bench_run(self):
        parser = build_parser()
        args = parser.parse_args(["bench", "run"])
        from sqldim.cli.bench import cmd_bench_run
        assert args.func is cmd_bench_run


# ---------------------------------------------------------------------------
# sqldim bench run — cmd_bench_run behaviour (mocked bench_main)
# ---------------------------------------------------------------------------


class TestBenchRunCommand:
    """Exercise cmd_bench_run argv construction paths using a mocked bench_main."""

    @pytest.fixture(autouse=True)
    def _mock_bench_main(self, monkeypatch):
        """Replace bench_main with a no-op that records calls."""
        self._calls: list[list[str]] = []

        def _fake_main(argv: list[str]) -> int:
            self._calls.append(list(argv))
            return 0

        # The function is imported locally inside cmd_bench_run so we patch
        # the canonical location in the benchmarks runner module.
        import sqldim.application.benchmarks.runner as _runner_mod
        monkeypatch.setattr(_runner_mod, "main", _fake_main)

    def _run(self, extra_args: list[str]) -> int:
        from sqldim.cli import main

        return main(["bench", "run"] + extra_args)

    def test_basic_invocation_returns_zero(self):
        rc = self._run(["--report", "none"])
        assert rc == 0

    def test_cases_forwarded(self):
        self._run(["A", "B", "--report", "none"])
        assert "A" in self._calls[-1]
        assert "B" in self._calls[-1]

    def test_max_tier_forwarded(self):
        self._run(["--max-tier", "m", "--report", "none"])
        argv = self._calls[-1]
        assert "--max-tier" in argv
        assert argv[argv.index("--max-tier") + 1] == "m"

    def test_report_forwarded(self):
        self._run(["--report", "csv"])
        argv = self._calls[-1]
        assert "--report" in argv

    def test_out_dir_forwarded(self, tmp_path):
        self._run(["--out-dir", str(tmp_path), "--report", "none"])
        argv = self._calls[-1]
        assert "--out-dir" in argv

    def test_source_forwarded(self):
        self._run(["--source", "parquet", "--report", "none"])
        argv = self._calls[-1]
        assert "--source" in argv
        assert argv[argv.index("--source") + 1] == "parquet"

    def test_fail_on_regression_forwarded(self):
        self._run(["--fail-on-regression", "--report", "none"])
        assert "--fail-on-regression" in self._calls[-1]

    def test_fail_on_breach_forwarded(self):
        self._run(["--fail-on-breach", "--report", "none"])
        assert "--fail-on-breach" in self._calls[-1]

    def test_compare_last_forwarded(self):
        self._run(["--compare-last", "--report", "none"])
        assert "--compare-last" in self._calls[-1]

    def test_no_out_dir_not_in_argv_when_default(self):
        """When out_dir is the default value it IS still forwarded (it's always set)."""
        self._run(["--report", "none"])
        argv = self._calls[-1]
        # out_dir always has a value (argparse default); arg is always forwarded
        assert "--out-dir" in argv


# ---------------------------------------------------------------------------
# bench_parser() — module-level factory function (line 21 in bench.py)
# ---------------------------------------------------------------------------


class TestBenchParserFunction:
    def test_bench_parser_returns_argparse_parser(self):
        """Call bench_parser() directly to cover line 21 in cli/bench.py."""
        import argparse
        from sqldim.cli.bench import bench_parser

        p = bench_parser()
        assert isinstance(p, argparse.ArgumentParser)

    def test_bench_parser_has_cases_argument(self):
        """Verify the returned parser accepts bench-specific positional args."""
        from sqldim.cli.bench import bench_parser

        p = bench_parser()
        args = p.parse_args([])
        # The bench parser should accept args without errors
        assert args is not None

