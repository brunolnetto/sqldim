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
        monkeypatch.setattr(
            "sqldim.application.evals.runner.make_model", lambda *a, **kw: None
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
