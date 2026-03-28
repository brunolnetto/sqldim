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

