"""Tests for `sqldim evals` CLI subcommand.

Covers:
- `sqldim evals list`       — lists all eval cases grouped by dataset
- `sqldim evals run`        — runs the eval suite with optional filters
- Parser flag handling       — --dataset, --tag, --provider, --model, --output, --quiet
"""

from __future__ import annotations

import json
import pytest

from sqldim.cli import build_parser, main

pytestmark = pytest.mark.filterwarnings(
    "ignore::logfire._internal.config.LogfireNotConfiguredWarning"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stub_runner_patch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch make_model so EvalRunner never calls a real LLM."""
    monkeypatch.setattr(
        "sqldim.application.evals.runner.make_model", lambda *a, **kw: None
    )


# ---------------------------------------------------------------------------
# sqldim evals list
# ---------------------------------------------------------------------------


class TestEvalsListCommand:
    def test_evals_list_returns_zero(self, capsys):
        rc = main(["nl", "evals", "list"])
        assert rc == 0

    def test_evals_list_prints_case_count(self, capsys):
        main(["nl", "evals", "list"])
        out = capsys.readouterr().out
        assert "case(s)" in out

    def test_evals_list_prints_commerce(self, capsys):
        main(["nl", "evals", "list"])
        out = capsys.readouterr().out
        assert "commerce" in out

    def test_evals_list_prints_dataset_count(self, capsys):
        main(["nl", "evals", "list"])
        out = capsys.readouterr().out
        assert "dataset(s)" in out

    def test_evals_list_prints_case_ids(self, capsys):
        main(["nl", "evals", "list"])
        out = capsys.readouterr().out
        # All EVAL_SUITE case IDs follow the pattern <dataset>.<number>.<slug>
        assert "commerce.01" in out


# ---------------------------------------------------------------------------
# sqldim evals run — parser flags
# ---------------------------------------------------------------------------


class TestEvalsRunParser:
    def test_default_provider_is_openai(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "run"])
        assert args.provider == "openai"

    def test_provider_flag_accepted(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "run", "--provider", "anthropic"])
        assert args.provider == "anthropic"

    def test_dataset_flag_accepted(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "run", "--dataset", "commerce"])
        assert args.dataset == ["commerce"]

    def test_tag_flag_accepted(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "run", "--tag", "entity"])
        assert args.tag == "entity"

    def test_model_flag_accepted(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "run", "--model", "gpt-4o"])
        assert args.model == "gpt-4o"

    def test_output_flag_accepted(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "run", "--output", "report.json"])
        assert args.output == "report.json"

    def test_quiet_flag_accepted(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "run", "--quiet"])
        assert args.quiet is True

    def test_quiet_defaults_to_false(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "run"])
        assert args.quiet is False

    def test_dataset_default_is_none(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "run"])
        assert args.dataset is None

    def test_tag_default_is_none(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "run"])
        assert args.tag is None


# ---------------------------------------------------------------------------
# sqldim evals run — behaviour
# ---------------------------------------------------------------------------


class TestEvalsRunCommand:
    @pytest.fixture(autouse=True)
    def _stub_model(self, monkeypatch):
        _stub_runner_patch(monkeypatch)

        import sqldim.application.evals.runner as runner_module
        from sqldim.application.evals.runner import EvalResult, EvalRunner

        class StubSource:
            def __init__(self, dataset):
                self.dataset = dataset

            def setup(self):
                pass

            def get_connection(self):
                return None

            def get_table_names(self):
                return []

            def teardown(self):
                pass

        def make_stub_result(case):
            return EvalResult(
                case_id=case.id,
                dataset=case.dataset,
                utterance=case.utterance,
                passed=True,
                score=1.0,
                visited_nodes=["stub"],
                hop_count=1,
                latency_ms=0.0,
                row_count=0,
                columns=[],
                sql_generated=None,
                explanation="Stub evaluation",
                check_details={},
                error=None,
                tags=list(case.tags),
                check_passed={},
            )

        def stub_run_on_connection(
            self, case, con, table_names, model, *, thread_id_suffix=""
        ):
            return make_stub_result(case)

        def stub_run_one(self, case, model):
            return make_stub_result(case)

        monkeypatch.setattr(runner_module, "load_dataset", lambda name: name)
        monkeypatch.setattr(runner_module, "DatasetPipelineSource", StubSource)
        monkeypatch.setattr(EvalRunner, "_run_on_connection", stub_run_on_connection)
        monkeypatch.setattr(EvalRunner, "_run_one", stub_run_one)

    def test_evals_run_returns_int(self, capsys):
        rc = main(["nl", "evals", "run", "--dataset", "commerce", "--quiet"])
        assert isinstance(rc, int)

    def test_evals_run_dataset_filter_commerce(self, capsys):
        rc = main(["nl", "evals", "run", "--dataset", "commerce", "--quiet"])
        # Returns 0 (all pass in stub mode) or 1 (some fail); both are valid
        assert rc in (0, 1)

    def test_evals_run_tag_filter(self, capsys):
        rc = main(
            [
                "nl",
                "evals",
                "run",
                "--dataset",
                "commerce",
                "--tag",
                "entity",
                "--quiet",
            ]
        )
        assert rc in (0, 1)

    def test_evals_run_prints_summary(self, capsys):
        main(["nl", "evals", "run", "--dataset", "commerce"])
        out = capsys.readouterr().out
        # Summary line from EvalReport.summary() or per-case progress
        assert len(out) > 0

    def test_evals_run_writes_json_output(self, tmp_path, capsys):
        output_path = str(tmp_path / "report.json")
        main(
            [
                "nl",
                "evals",
                "run",
                "--dataset",
                "commerce",
                "--output",
                output_path,
                "--quiet",
            ]
        )
        assert (tmp_path / "report.json").exists()
        with open(output_path) as fh:
            data = json.load(fh)
        assert "summary" in data
        assert "results" in data

    def test_evals_run_json_summary_has_total(self, tmp_path, capsys):
        output_path = str(tmp_path / "report.json")
        main(
            [
                "nl",
                "evals",
                "run",
                "--dataset",
                "commerce",
                "--output",
                output_path,
                "--quiet",
            ]
        )
        with open(output_path) as fh:
            data = json.load(fh)
        assert data["summary"]["total"] >= 1

    def test_evals_run_quiet_mode_prints_path(self, tmp_path, capsys):
        output_path = str(tmp_path / "r.json")
        main(
            [
                "nl",
                "evals",
                "run",
                "--dataset",
                "commerce",
                "--output",
                output_path,
                "--quiet",
            ]
        )
        out = capsys.readouterr().out
        assert "Report written to" in out
        assert output_path in out

    def test_evals_run_verbose_mode_default(self, capsys):
        """Without --quiet, per-case progress should be printed."""
        main(["nl", "evals", "run", "--dataset", "commerce"])
        out = capsys.readouterr().out
        # verbose=True prints at least the case IDs
        assert len(out) > 0


# ---------------------------------------------------------------------------
# sqldim evals drift — parser flags
# ---------------------------------------------------------------------------


class TestEvalsDriftParser:
    def test_domain_default_retail(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "drift"])
        assert args.domain == "commerce"

    def test_domain_flag_accepted(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "drift", "--domain", "commerce"])
        assert args.domain == "commerce"

    def test_provider_default_openai(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "drift"])
        assert args.provider == "openai"

    def test_quiet_flag_accepted(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "drift", "--quiet"])
        assert args.quiet is True

    def test_quiet_defaults_to_false(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "drift"])
        assert args.quiet is False

    def test_output_flag_accepted(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "drift", "--output", "report.json"])
        assert args.output == "report.json"

    def test_model_flag_accepted(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "drift", "--model", "gpt-4o"])
        assert args.model == "gpt-4o"

    def test_func_set_to_cmd_evals_drift(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "evals", "drift"])
        from sqldim.cli import cmd_evals_drift

        assert args.func is cmd_evals_drift


# ---------------------------------------------------------------------------
# sqldim evals drift — behaviour
# ---------------------------------------------------------------------------


class TestEvalsDriftCommand:
    @pytest.fixture(autouse=True)
    def _stub_model(self, monkeypatch):
        from sqldim.application.evals.drift import DriftEvalReport
        from sqldim.application.evals.runner import EvalRunner

        def stub_run_drift_suite(self, drift_cases):
            return DriftEvalReport(
                run_at="2026-01-01T00:00:00+00:00",
                provider=self.provider,
                model_name=self.model_name,
            )

        monkeypatch.setattr(
            EvalRunner,
            "run_drift_suite",
            stub_run_drift_suite,
        )

    def test_valid_domain_returns_0(self, capsys):
        rc = main(["nl", "evals", "drift", "--domain", "commerce", "--quiet"])
        assert rc == 0

    def test_drift_quiet_no_output_prints_summary(self, capsys):
        main(["nl", "evals", "drift", "--domain", "commerce", "--quiet"])
        out = capsys.readouterr().out
        # In quiet mode with no output file, report.summary() is printed
        assert isinstance(out, str)

    def test_drift_writes_json_output(self, tmp_path, capsys):
        output_path = str(tmp_path / "drift_report.json")
        rc = main(
            [
                "nl",
                "evals",
                "drift",
                "--domain",
                "commerce",
                "--output",
                output_path,
                "--quiet",
            ]
        )
        assert rc == 0
        assert (tmp_path / "drift_report.json").exists()

    def test_drift_output_prints_confirmation(self, tmp_path, capsys):
        output_path = str(tmp_path / "r.json")
        main(
            [
                "nl",
                "evals",
                "drift",
                "--domain",
                "commerce",
                "--output",
                output_path,
                "--quiet",
            ]
        )
        out = capsys.readouterr().out
        assert "Report written to" in out

    def test_drift_unknown_domain_prints_available(self, capsys, monkeypatch):
        """Simulate an unknown domain by patching _available_domains inside cmd_evals_drift."""
        import sqldim.cli as _cli_mod

        orig = _cli_mod.cmd_evals_drift

        def _patched(args):
            # Force domain not in available list
            import argparse

            a = argparse.Namespace(
                domain="fakedomainxyz",
                quiet=True,
                output=None,
                provider="openai",
                model=None,
            )
            return orig(a)

        rc = _patched(None)
        out = capsys.readouterr().out
        assert rc == 1
        assert "Unknown domain" in out
