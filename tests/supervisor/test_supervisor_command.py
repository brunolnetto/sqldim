"""The supervisor CLI: fail closed by default, demo only on request."""

from __future__ import annotations

import argparse

import pytest


def _args(**kw: object) -> argparse.Namespace:
    base: dict[str, object] = {
        "question": "how many orders were placed?",
        "model": None,
        "model_provider": None,
        "datasets": ["commerce"],
        "verbose": False,
        "demo": False,
    }
    return argparse.Namespace(**{**base, **kw})


class TestParser:
    def test_supervisor_mode_and_demo_flag(self) -> None:
        from sqldim.cli import build_parser

        args = build_parser().parse_args(
            ["nl", "ask", "q", "--mode", "supervisor", "--demo", "--datasets", "a", "b"]
        )
        assert args.mode == "supervisor"
        assert args.demo is True
        assert args.datasets == ["a", "b"]

    def test_demo_is_off_by_default(self) -> None:
        from sqldim.cli import build_parser

        for argv in (["nl", "ask", "q"], ["nl", "chat"]):
            assert build_parser().parse_args(argv).demo is False


class TestCmdSupervisor:
    def test_without_planner_fails_closed(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from sqldim.cli.supervisor import cmd_supervisor

        rc = cmd_supervisor(_args())
        out = capsys.readouterr().out
        assert rc == 1
        assert "Outcome: FAILED" in out
        assert "NOT_IMPLEMENTED" in out
        assert "demo" not in out.split("\n")[0]

    def test_invalid_provider_warns_and_still_fails_closed(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from sqldim.cli.supervisor import cmd_supervisor

        rc = cmd_supervisor(_args(model_provider="invalid_provider"))
        out = capsys.readouterr().out
        assert rc == 1
        assert "Warning" in out
        assert "Outcome: FAILED" in out

    def test_unknown_dataset_returns_1(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from sqldim.cli.supervisor import cmd_supervisor

        assert cmd_supervisor(_args(datasets=["nonexistent_xyz"])) == 1
        assert "Unknown" in capsys.readouterr().out

    def test_demo_discovery_is_answered_from_grants(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from sqldim.cli.supervisor import cmd_supervisor

        rc = cmd_supervisor(
            _args(question="which datasets are available?", demo=True, verbose=True)
        )
        out = capsys.readouterr().out
        assert rc == 1  # no tool ran: FAILED, not a fictitious success
        assert out.startswith("[demo mode")
        assert "commerce" in out
        assert "Nodes visited" in out

    def test_demo_unavailable_capability_is_reported(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from sqldim.cli.supervisor import cmd_supervisor

        cmd_supervisor(_args(question="validate that orders are positive", demo=True))
        assert "not available: dgm_validate_invariant" in capsys.readouterr().out


class TestHopFormatting:
    def test_failed_hop_shows_kind(self) -> None:
        from sqldim.cli.ask import _format_hop
        from sqldim.core.query.dgm.nl.supervisor._types import (
            CallStatus,
            ToolCallResult,
            ToolErrorKind,
        )
        from tests.supervisor._fakes import step

        hop = ToolCallResult.failure(
            step("a"), CallStatus.FAILED, ToolErrorKind.NOT_IMPLEMENTED, "missing"
        )
        text = _format_hop(hop)
        assert "[a] dgm_query" in text
        assert "FAILED NOT_IMPLEMENTED: missing" in text

    def test_demo_hop_is_marked(self) -> None:
        from sqldim.cli.ask import _format_hop
        from sqldim.core.query.dgm.nl.supervisor._types import (
            CallStatus,
            ToolCallResult,
        )

        hop = ToolCallResult(
            call_id="a",
            tool_name="dgm_query",
            status=CallStatus.SUCCEEDED,
            data={},
            attempts=1,
            demo=True,
        )
        assert "(demo)" in _format_hop(hop)
