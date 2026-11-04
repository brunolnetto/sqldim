"""Coverage tests for sqldim/application/evals/ uncovered branches.

Targets:
- sqldim/application/evals/__init__.py  lines 49-53  (AttributeError branch)
- sqldim/application/evals/loader.py    lines 38-41  (commerce_medallion factory)
- sqldim/application/evals/tool_eval_loader.py  lines 105-106, 127-128, 139-171
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# evals/__init__.py — lazy __getattr__ AttributeError branch
# ---------------------------------------------------------------------------


class TestEvalsInitLazy:
    def test_getattr_raises_for_unknown_name(self) -> None:
        import sqldim.application.evals as evals_pkg

        with pytest.raises(AttributeError, match="no attribute 'NonExistent'"):
            _ = evals_pkg.NonExistent  # type: ignore[attr-defined]

    def test_getattr_resolves_eval_case(self) -> None:
        import sqldim.application.evals as evals_pkg

        EvalCase = evals_pkg.EvalCase  # noqa: N806
        assert EvalCase is not None

    def test_getattr_resolves_load_all_tool_eval_cases(self) -> None:
        import sqldim.application.evals as evals_pkg

        fn = evals_pkg.load_all_tool_eval_cases
        assert callable(fn)


# ---------------------------------------------------------------------------
# evals/loader.py — commerce_medallion pipeline_source_factory branch
# ---------------------------------------------------------------------------


class TestDictToEvalCaseCommerceMedallion:
    def test_commerce_medallion_creates_factory(self) -> None:
        from sqldim.application.evals.loader import _dict_to_eval_case

        d = {
            "id": "commerce.test.001",
            "dataset": "commerce",
            "utterance": "top customers",
            "pipeline_source": "commerce_medallion",
        }
        case = _dict_to_eval_case(d)
        assert case.pipeline_source_factory is not None
        assert callable(case.pipeline_source_factory)

    def test_commerce_medallion_factory_body_executes(self, monkeypatch) -> None:
        """Calling the factory covers the lazy import body."""
        import types

        sentinel = object()
        fake_cls = lambda: sentinel  # noqa: E731
        fake_mod = types.ModuleType("fake_medallion")
        fake_mod.CommerceMedallionPipelineSource = fake_cls  # type: ignore[attr-defined]
        monkeypatch.setitem(
            __import__("sys").modules,
            "sqldim.application.datasets.domains.commerce.sources.medallion",
            fake_mod,
        )
        from sqldim.application.evals.loader import _dict_to_eval_case

        d = {
            "id": "commerce.test.002",
            "dataset": "commerce",
            "utterance": "top customers",
            "pipeline_source": "commerce_medallion",
        }
        case = _dict_to_eval_case(d)
        result = case.pipeline_source_factory()
        assert result is sentinel

    def test_no_pipeline_source_leaves_factory_none(self) -> None:
        from sqldim.application.evals.loader import _dict_to_eval_case

        d = {
            "id": "commerce.test.001",
            "dataset": "commerce",
            "utterance": "show customers",
        }
        case = _dict_to_eval_case(d)
        assert case.pipeline_source_factory is None


# ---------------------------------------------------------------------------
# evals/tool_eval_loader.py — load_tool_eval_cases duplicate-dedup (lines 105-106)
# ---------------------------------------------------------------------------


class TestLoadToolEvalCasesDedup:
    def test_duplicate_id_in_legacy_is_skipped(self, tmp_path: Path) -> None:
        """A case already seen via per-tool file is not added again from legacy."""
        from sqldim.application.evals import tool_eval_loader as tl

        # Set up fake domain tree
        evals_dir = tmp_path / "mydom" / "artifacts" / "evals"
        evals_dir.mkdir(parents=True)

        shared_case = {
            "id": "mydom.test.01.dup",
            "tool": "my_tool",
            "description": "dup test",
            "arguments": {},
            "expected": {
                "success": True,
                "tool_name": "my_tool",
                "data_keys": [],
                "data_checks": {},
            },
        }
        per_tool_file = evals_dir / "my_tool.json"
        per_tool_file.write_text(json.dumps({"cases": [shared_case]}))

        # Legacy tool_evals.json has the same ID
        legacy_case = dict(shared_case)
        legacy_file = evals_dir / "tool_evals.json"
        legacy_file.write_text(json.dumps({"tool_cases": [legacy_case]}))

        old_root = tl._DOMAINS_ROOT
        tl._DOMAINS_ROOT = tmp_path
        try:
            cases = tl.load_tool_eval_cases("mydom")
        finally:
            tl._DOMAINS_ROOT = old_root

        ids = [c.id for c in cases]
        assert ids.count("mydom.test.01.dup") == 1, "Duplicate should be deduplicated"


# ---------------------------------------------------------------------------
# evals/tool_eval_loader.py — load_all_tool_eval_cases legacy-only domain (127-128)
# ---------------------------------------------------------------------------


class TestLoadAllToolEvalCasesLegacyOnly:
    def test_legacy_only_domain_is_included(self, tmp_path: Path) -> None:
        """Domain with only tool_evals.json (no per-tool files) is loaded."""
        from sqldim.application.evals import tool_eval_loader as tl

        # Domain with an evals/ dir but no per-tool files — only tool_evals.json
        evals_dir = tmp_path / "legacydom" / "artifacts" / "evals"
        evals_dir.mkdir(parents=True)
        legacy_case = {
            "id": "legacydom.test.01.only",
            "tool": "legacy_tool",
            "description": "legacy only",
            "arguments": {},
            "expected": {
                "success": True,
                "tool_name": "legacy_tool",
                "data_keys": [],
                "data_checks": {},
            },
        }
        (evals_dir / "tool_evals.json").write_text(
            json.dumps({"tool_cases": [legacy_case]})
        )

        old_root = tl._DOMAINS_ROOT
        tl._DOMAINS_ROOT = tmp_path
        try:
            all_cases = tl.load_all_tool_eval_cases()
        finally:
            tl._DOMAINS_ROOT = old_root

        ids = [c.id for c in all_cases]
        assert "legacydom.test.01.only" in ids


# ---------------------------------------------------------------------------
# evals/tool_eval_loader.py — load_cases_for_tool (lines 139-171)
# ---------------------------------------------------------------------------


class TestLoadCasesForTool:
    def test_returns_cases_for_specific_tool(self, tmp_path: Path) -> None:
        from sqldim.application.evals import tool_eval_loader as tl

        evals_dir = tmp_path / "dom1" / "artifacts" / "evals"
        evals_dir.mkdir(parents=True)
        case = {
            "id": "dom1.mytool.01",
            "tool": "my_tool",
            "description": "tool test",
            "arguments": {"x": 1},
            "expected": {
                "success": True,
                "tool_name": "my_tool",
                "data_keys": ["value"],
                "data_checks": {},
            },
        }
        (evals_dir / "my_tool.json").write_text(json.dumps({"cases": [case]}))

        old_root = tl._DOMAINS_ROOT
        tl._DOMAINS_ROOT = tmp_path
        try:
            cases = tl.load_cases_for_tool("my_tool")
        finally:
            tl._DOMAINS_ROOT = old_root

        assert len(cases) == 1
        assert cases[0].id == "dom1.mytool.01"
        assert cases[0].tool == "my_tool"

    def test_filters_by_tool_from_legacy(self, tmp_path: Path) -> None:
        """Falls back to tool_evals.json when no per-tool file exists."""
        from sqldim.application.evals import tool_eval_loader as tl

        evals_dir = tmp_path / "dom2" / "artifacts" / "evals"
        evals_dir.mkdir(parents=True)
        cases_data = [
            {
                "id": "dom2.tool_a.01",
                "tool": "tool_a",
                "description": "a test",
                "arguments": {},
                "expected": {
                    "success": True,
                    "tool_name": "tool_a",
                    "data_keys": [],
                    "data_checks": {},
                },
            },
            {
                "id": "dom2.tool_b.01",
                "tool": "tool_b",
                "description": "b test",
                "arguments": {},
                "expected": {
                    "success": True,
                    "tool_name": "tool_b",
                    "data_keys": [],
                    "data_checks": {},
                },
            },
        ]
        (evals_dir / "tool_evals.json").write_text(
            json.dumps({"tool_cases": cases_data})
        )

        old_root = tl._DOMAINS_ROOT
        tl._DOMAINS_ROOT = tmp_path
        try:
            cases = tl.load_cases_for_tool("tool_a")
        finally:
            tl._DOMAINS_ROOT = old_root

        assert len(cases) == 1
        assert cases[0].tool == "tool_a"

    def test_returns_empty_for_unknown_tool(self, tmp_path: Path) -> None:
        from sqldim.application.evals import tool_eval_loader as tl

        (tmp_path / "dom3" / "artifacts" / "evals").mkdir(parents=True)

        old_root = tl._DOMAINS_ROOT
        tl._DOMAINS_ROOT = tmp_path
        try:
            cases = tl.load_cases_for_tool("nonexistent_tool_xyz")
        finally:
            tl._DOMAINS_ROOT = old_root

        assert cases == []

    def test_deduplicates_across_domains(self, tmp_path: Path) -> None:
        """Same ID appearing in two domains is deduplicated."""
        from sqldim.application.evals import tool_eval_loader as tl

        for dom in ("dom4", "dom5"):
            evals_dir = tmp_path / dom / "artifacts" / "evals"
            evals_dir.mkdir(parents=True)
            case = {
                "id": "shared.tool.01",  # same id in both domains
                "tool": "shared_tool",
                "description": f"from {dom}",
                "arguments": {},
                "expected": {
                    "success": True,
                    "tool_name": "shared_tool",
                    "data_keys": [],
                    "data_checks": {},
                },
            }
            (evals_dir / "shared_tool.json").write_text(json.dumps({"cases": [case]}))

        old_root = tl._DOMAINS_ROOT
        tl._DOMAINS_ROOT = tmp_path
        try:
            cases = tl.load_cases_for_tool("shared_tool")
        finally:
            tl._DOMAINS_ROOT = old_root

        ids = [c.id for c in cases]
        assert ids.count("shared.tool.01") == 1
