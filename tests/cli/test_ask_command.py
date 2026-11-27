"""Tests for `sqldim ask` CLI command — dataset → EntityRegistry → NL graph (§11.10).

Integration point: *directly to dataset*.  The NL agent builds an EntityRegistry
from the DuckDB schema (table names → node_terms, column names → prop_terms) and
invokes ``build_nl_graph(context=ctx)`` with a populated ``DGMContext``.

No gold/silver/bronze layer mediates the connection; the DGM synthesis workflow
derives the query plan from the entity registry.
"""

from __future__ import annotations

import pytest
import duckdb

from sqldim.application.ask import (
    KNOWN_DATASETS,
    _discover_datasets,
    _load_one_dataset,
    load_dataset,
    build_registry_from_schema,
    make_default_budget,
    run_ask,
)
from sqldim.application.supervisor_host import local_data_access
from sqldim.cli import build_parser, main

pytestmark = pytest.mark.filterwarnings(
    "ignore::logfire._internal.config.LogfireNotConfiguredWarning"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_con_with_tables() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with two test tables (sales_fact, date_dim)."""
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE date_dim (date_key INTEGER, full_date DATE, year INTEGER)"
    )
    con.execute(
        "CREATE TABLE sales_fact (sales_key INTEGER, date_key INTEGER, amount DOUBLE)"
    )
    return con


# ---------------------------------------------------------------------------
# KNOWN_DATASETS registry
# ---------------------------------------------------------------------------


class TestKnownDatasets:
    def test_contains_commerce(self):
        assert "commerce" in KNOWN_DATASETS

    def test_contains_fintech(self):
        assert "fintech" in KNOWN_DATASETS

    def test_contains_nba_analytics(self):
        assert "nba_analytics" in KNOWN_DATASETS

    def test_contains_growth(self):
        assert "growth" in KNOWN_DATASETS

    def test_contains_telemetry(self):
        assert "telemetry" in KNOWN_DATASETS

    def test_contains_all_eight_domains(self):
        expected = {
            "commerce",
            "fintech",
            "nba_analytics",
            "growth",
            "telemetry",
            "entertainment",
            "devops",
            "data_models",
        }
        assert expected <= set(KNOWN_DATASETS)

    def test_entries_are_two_tuples(self):
        for name, entry in KNOWN_DATASETS.items():
            assert isinstance(entry, tuple) and len(entry) == 2, (
                f"{name}: expected (module_path, attr) tuple, got {entry!r}"
            )

    def test_module_paths_end_in_dataset(self):
        for name, (mod_path, _attr) in KNOWN_DATASETS.items():
            assert mod_path.endswith(".dataset"), (
                f"{name}: module_path {mod_path!r} should end in '.dataset'"
            )

    def test_attr_names_end_in_dataset_suffix(self):
        for name, (_mod_path, attr) in KNOWN_DATASETS.items():
            assert attr.endswith("_dataset"), (
                f"{name}: attr {attr!r} should end in '_dataset'"
            )


# ---------------------------------------------------------------------------
# _discover_datasets / _load_one_dataset  (discovery pattern)
# ---------------------------------------------------------------------------


class TestDiscoverDatasets:
    def test_discover_returns_dict(self):
        result = _discover_datasets()
        assert isinstance(result, dict)

    def test_discover_finds_commerce(self):
        result = _discover_datasets()
        assert "commerce" in result

    def test_discover_finds_all_eight(self):
        result = _discover_datasets()
        expected = {
            "commerce",
            "fintech",
            "nba_analytics",
            "growth",
            "telemetry",
            "entertainment",
            "devops",
            "data_models",
        }
        assert expected <= set(result)

    def test_each_entry_two_tuple(self):
        for name, entry in _discover_datasets().items():
            assert isinstance(entry, tuple) and len(entry) == 2, (
                f"{name}: expected 2-tuple, got {entry!r}"
            )

    def test_load_one_dataset_commerce(self):
        result = _load_one_dataset("sqldim.application.datasets.domains", "commerce")
        assert result is not None
        name, (mod_path, attr) = result
        assert name == "commerce"
        assert "commerce" in mod_path
        assert attr == "commerce_dataset"

    def test_load_one_dataset_missing_module(self):
        result = _load_one_dataset(
            "sqldim.application.datasets.domains", "nonexistent_domain_xyz"
        )
        assert result is None

    def test_load_one_dataset_no_metadata(self, tmp_path, monkeypatch):
        """A domain package without DATASET_METADATA is silently skipped."""
        # Simulate by patching importlib to return a module without the attr
        import importlib
        import types

        bare_mod = types.ModuleType("fake.domain.dataset")
        # No DATASET_METADATA attribute

        orig_import = importlib.import_module

        def _patched(name, *args, **kwargs):
            if name == "sqldim.application.datasets.domains.bare_domain.dataset":
                return bare_mod
            return orig_import(name, *args, **kwargs)

        monkeypatch.setattr(importlib, "import_module", _patched)
        result = _load_one_dataset("sqldim.application.datasets.domains", "bare_domain")
        assert result is None

    def test_load_one_dataset_metadata_without_dataset_attr(self, monkeypatch):
        """DATASET_METADATA present but missing dataset_attr key → return None."""
        import importlib
        import types

        mod = types.ModuleType("fake.no_attr.dataset")
        mod.DATASET_METADATA = {"name": "no_attr"}  # type: ignore[attr-defined]
        # no 'dataset_attr' key → attr='' → early return None

        orig_import = importlib.import_module

        def _patched(name, *args, **kwargs):
            if name == "sqldim.application.datasets.domains.no_attr.dataset":
                return mod
            return orig_import(name, *args, **kwargs)

        monkeypatch.setattr(importlib, "import_module", _patched)
        result = _load_one_dataset("sqldim.application.datasets.domains", "no_attr")
        assert result is None

    def test_discover_datasets_import_error_returns_empty(self, monkeypatch):
        """When the domains package itself cannot be imported, return empty dict."""
        import importlib

        orig_import = importlib.import_module

        def _raise_on_pkg(name, *args, **kwargs):
            if name == "sqldim.application.datasets.domains":
                raise ImportError("simulated import failure")
            return orig_import(name, *args, **kwargs)

        monkeypatch.setattr(importlib, "import_module", _raise_on_pkg)
        result = _discover_datasets()
        assert result == {}

    def test_discover_datasets_skips_non_packages(self, monkeypatch):
        """Entries where ``ispkg=False`` are silently skipped."""
        import pkgutil

        def _fake_iter(path):
            # Return one non-package entry that should be skipped
            yield pkgutil.ModuleInfo(None, "not_a_pkg", False)  # type: ignore[arg-type]

        monkeypatch.setattr(pkgutil, "iter_modules", _fake_iter)
        result = _discover_datasets()
        # Non-packages must be ignored → result is empty (or just existing cached values)
        assert isinstance(result, dict)
        # The non-package entry must NOT appear as a key
        assert "not_a_pkg" not in result


# ---------------------------------------------------------------------------
# load_dataset
# ---------------------------------------------------------------------------


class TestLoadDataset:
    def test_commerce_returns_dataset(self):
        from sqldim.application.datasets.dataset import Dataset

        ds = load_dataset("commerce")
        assert isinstance(ds, Dataset)

    def test_commerce_has_expected_tables(self):
        ds = load_dataset("commerce")
        tables = ds.table_names()
        assert len(tables) >= 2

    def test_unknown_dataset_raises_key_error(self):
        with pytest.raises(KeyError, match="Unknown dataset"):
            load_dataset("no_such_dataset_xyz")


# ---------------------------------------------------------------------------
# build_registry_from_schema
# ---------------------------------------------------------------------------


class TestBuildRegistryFromSchema:
    def test_table_names_become_node_terms(self):
        con = _make_con_with_tables()
        registry = build_registry_from_schema(con, ["date_dim", "sales_fact"])
        assert "date_dim" in registry.node_terms
        assert "sales_fact" in registry.node_terms

    def test_columns_registered_as_prop_terms(self):
        con = _make_con_with_tables()
        registry = build_registry_from_schema(con, ["date_dim"])
        assert "date_key" in registry.prop_terms
        assert "full_date" in registry.prop_terms
        assert "year" in registry.prop_terms

    def test_propref_is_qualified_table_column(self):
        con = _make_con_with_tables()
        registry = build_registry_from_schema(con, ["date_dim"])
        assert registry.prop_terms["date_key"] == "date_dim.date_key"
        assert registry.prop_terms["year"] == "date_dim.year"

    def test_empty_table_list_returns_empty_registry(self):
        con = duckdb.connect(":memory:")
        registry = build_registry_from_schema(con, [])
        assert not registry.node_terms
        assert not registry.prop_terms

    def test_multiple_tables_all_columns_registered(self):
        con = _make_con_with_tables()
        registry = build_registry_from_schema(con, ["date_dim", "sales_fact"])
        # sales_fact has: sales_key, date_key, amount
        assert "amount" in registry.prop_terms
        assert registry.prop_terms["amount"] == "sales_fact.amount"

    def test_node_alias_matches_table_name(self):
        con = _make_con_with_tables()
        registry = build_registry_from_schema(con, ["date_dim"])
        assert registry.node_terms["date_dim"] == "date_dim"


# ---------------------------------------------------------------------------
# PipelineSource.label base implementation
# ---------------------------------------------------------------------------


class TestPipelineSourceLabel:
    def test_base_label_returns_class_name(self):
        """Covers ask.py PipelineSource.label base property."""
        from sqldim.application.ask import PipelineSource

        class _Minimal(PipelineSource):
            def setup(self) -> None:
                pass

            def get_connection(self):
                return None

            def get_table_names(self):
                return []

            def teardown(self) -> None:
                pass

        assert _Minimal().label == "_Minimal"


# ---------------------------------------------------------------------------
# make_default_budget
# ---------------------------------------------------------------------------


class TestMakeDefaultBudget:
    def test_returns_execution_budget(self):
        from sqldim.core.query.dgm.planner._gate import ExecutionBudget

        budget = make_default_budget()
        assert isinstance(budget, ExecutionBudget)

    def test_max_result_rows_positive(self):
        budget = make_default_budget()
        assert budget.max_result_rows > 0

    def test_max_wall_time_positive(self):
        from datetime import timedelta

        budget = make_default_budget()
        assert budget.max_wall_time > timedelta(0)


# ---------------------------------------------------------------------------
# run_ask
# ---------------------------------------------------------------------------


def _granted(name: str):
    """The local host's access: READ on *name* if it is a bundled dataset."""
    return local_data_access([name] if name in KNOWN_DATASETS else [])


class TestRunAsk:
    def test_valid_dataset_returns_zero(self, capsys):
        rc = run_ask(
            "show me customers", "commerce", data=_granted("commerce"), demo=True
        )
        assert rc == 0

    def test_unknown_dataset_returns_one(self, capsys):
        rc = run_ask(
            "show me customers",
            "no_such_dataset",
            data=_granted("no_such_dataset"),
            demo=True,
        )
        assert rc == 1

    def test_unknown_dataset_prints_error(self, capsys):
        run_ask("test", "no_such_dataset", data=_granted("no_such_dataset"), demo=True)
        out = capsys.readouterr().out
        assert "UNAUTHORIZED: not authorized or not found" in out

    def test_valid_run_prints_info(self, capsys):
        run_ask("show me customers", "commerce", data=_granted("commerce"), demo=True)
        out = capsys.readouterr().out
        assert len(out) > 0

    def test_verbose_prints_table_names(self, capsys):
        run_ask(
            "show me customers",
            "commerce",
            data=_granted("commerce"),
            verbose=True,
            demo=True,
        )
        out = capsys.readouterr().out
        assert "Tables" in out
        assert "customers" in out

    def test_verbose_prints_prop_count(self, capsys):
        run_ask(
            "show me customers",
            "commerce",
            data=_granted("commerce"),
            verbose=True,
            demo=True,
        )
        out = capsys.readouterr().out
        assert "PropTerms" in out

    def test_run_ask_handles_graph_recursion_error(self, monkeypatch):
        """GraphRecursionError is reported as TOOL_FAILURE, not success."""
        from unittest.mock import MagicMock
        from langgraph.errors import GraphRecursionError

        mock_graph = MagicMock()
        mock_graph.invoke.side_effect = GraphRecursionError("too many steps")
        monkeypatch.setattr(
            "sqldim.application.ask.build_nl_graph", lambda **kw: mock_graph
        )
        rc = run_ask(
            "show me customers", "commerce", data=_granted("commerce"), demo=True
        )
        assert rc == 1  # a turn that does not converge is a failure

    def test_run_ask_prints_explanation(self, monkeypatch, capsys):
        """When explanation is returned, it is printed to stdout."""
        from unittest.mock import MagicMock

        mock_graph = MagicMock()
        mock_graph.invoke.return_value = {
            "explanation": "Here are your top customers.",
            "visited_nodes": [],
        }
        monkeypatch.setattr(
            "sqldim.application.ask.build_nl_graph", lambda **kw: mock_graph
        )
        rc = run_ask(
            "show me customers", "commerce", data=_granted("commerce"), demo=True
        )
        assert rc == 0
        out = capsys.readouterr().out
        assert "Here are your top customers." in out

    def test_run_ask_prints_result_rows_and_truncation(self, monkeypatch, capsys):
        """Result rows are printed; >5 rows shows truncation message."""
        from unittest.mock import MagicMock

        mock_graph = MagicMock()
        rows = [[str(i)] for i in range(7)]
        mock_graph.invoke.return_value = {
            "result": {"columns": ["id"], "rows": rows, "count": 7},
            "visited_nodes": [],
        }
        monkeypatch.setattr(
            "sqldim.application.ask.build_nl_graph", lambda **kw: mock_graph
        )
        rc = run_ask(
            "show me customers", "commerce", data=_granted("commerce"), demo=True
        )
        assert rc == 0
        out = capsys.readouterr().out
        assert "7 row(s)" in out
        assert "more rows" in out


# ---------------------------------------------------------------------------
# CLI parser integration
# ---------------------------------------------------------------------------


class TestAskParser:
    def test_parser_has_ask_subcommand(self):
        parser = build_parser()
        # Verify ask subcommand is registered (will error cleanly on unknown)
        args = parser.parse_args(
            ["nl", "ask", "show customers", "--dataset", "commerce"]
        )
        assert args.command == "nl"

    def test_ask_args_stores_dataset(self):
        parser = build_parser()
        args = parser.parse_args(
            ["nl", "ask", "how are my customers?", "--dataset", "commerce"]
        )
        assert args.dataset == "commerce"

    def test_ask_args_stores_question(self):
        parser = build_parser()
        args = parser.parse_args(
            ["nl", "ask", "how are my customers?", "--dataset", "commerce"]
        )
        assert args.question == "how are my customers?"

    def test_ask_verbose_flag(self):
        parser = build_parser()
        args = parser.parse_args(
            ["nl", "ask", "test", "--dataset", "commerce", "--verbose"]
        )
        assert args.verbose is True

    def test_ask_verbose_defaults_to_false(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "ask", "test", "--dataset", "commerce"])
        assert args.verbose is False

    def test_ask_mode_defaults_to_ask(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "ask", "test", "--dataset", "commerce"])
        assert args.mode == "ask"

    def test_ask_supervisor_mode(self):
        parser = build_parser()
        args = parser.parse_args(["nl", "ask", "test", "--mode", "supervisor"])
        assert args.mode == "supervisor"


# ---------------------------------------------------------------------------
# main() integration
# ---------------------------------------------------------------------------


class TestMainAsk:
    @pytest.fixture(autouse=True)
    def _no_ollama(self, monkeypatch):
        monkeypatch.setattr("sqldim.application.ask.make_model", lambda *a, **kw: None)

    def test_main_ask_valid_returns_zero(self, capsys):
        rc = main(["nl", "ask", "--demo", "show me customers", "--dataset", "commerce"])
        assert rc == 0

    def test_main_ask_unknown_dataset_returns_one(self, capsys):
        rc = main(["nl", "ask", "--demo", "test", "--dataset", "unknown_ds"])
        assert rc == 1

    def test_main_ask_missing_dataset_returns_one(self, capsys):
        rc = main(["nl", "ask", "--demo", "test"])
        assert rc == 1
        out = capsys.readouterr().out
        assert "--dataset is required" in out


# ---------------------------------------------------------------------------
# _build_pipeline_source — unknown source type
# ---------------------------------------------------------------------------


class TestBuildPipelineSource:
    def test_unknown_source_type_prints_error_and_returns_one(
        self, capsys, monkeypatch
    ):
        """cmd_ask returns 1 when _build_pipeline_source gets an unknown source."""
        from sqldim.cli.ask import _build_pipeline_source
        import argparse

        args = argparse.Namespace(source="nonexistent_source_type_xyz")
        result = _build_pipeline_source(args)
        out = capsys.readouterr().out
        assert result is None
        assert "Unknown source type" in out
        assert "nonexistent_source_type_xyz" in out

    def test_cmd_ask_with_unknown_source_returns_one(self, monkeypatch, capsys):
        """cmd_ask pipeline returns 1 when the source factory is unknown."""
        monkeypatch.setattr("sqldim.application.ask.make_model", lambda *a, **kw: None)
        # Invoke cmd_ask via main with a custom source type not in the registry
        # The CLI parser uses --source to distinguish source types; default is "dataset"
        # We inject directly via _build_pipeline_source to test the None path
        from sqldim.cli.ask import _build_pipeline_source
        import argparse

        fake_args = argparse.Namespace(source="bogus_type")
        source = _build_pipeline_source(fake_args)
        assert source is None


# ---------------------------------------------------------------------------
# Fail closed without demo mode (ADR 0012, D47)
# ---------------------------------------------------------------------------


class TestFailClosedWithoutDemo:
    def test_run_ask_without_demo_is_not_implemented_and_opens_nothing(
        self, monkeypatch, capsys
    ):
        from sqldim.core.query.dgm.nl.supervisor._auth import Action, DataAccess
        from tests.supervisor._fakes import Opener, auth

        class Source:
            def setup(self):
                raise AssertionError("nothing may run without demo mode")

        opener = Opener()
        data = DataAccess(auth({"commerce": {Action.READ}}), lambda d: Source())
        rc = run_ask("show me customers", "commerce", data=data)
        assert rc == 2
        out = capsys.readouterr().out
        assert "NOT_IMPLEMENTED" in out and "--demo" in out
        assert opener.opened == []

    def test_cli_without_demo_is_not_implemented(self, monkeypatch, capsys):
        monkeypatch.setattr("sqldim.application.ask.make_model", lambda *a, **kw: None)
        rc = main(["nl", "ask", "show me customers", "--dataset", "commerce"])
        assert rc == 2
        assert "NOT_IMPLEMENTED" in capsys.readouterr().out

    def test_graph_without_demo_names_what_is_missing(self):
        from sqldim.core.query.dgm.nl._graph import (
            CONTRACT_PIPELINE_MISSING,
            NLPipelineUnavailable,
            build_nl_graph,
        )

        with pytest.raises(NLPipelineUnavailable) as exc:
            build_nl_graph()
        assert exc.value.kind == "NOT_IMPLEMENTED"
        assert exc.value.missing == CONTRACT_PIPELINE_MISSING
        assert "D26" in str(exc.value)

    def test_demo_run_is_announced(self, monkeypatch, capsys):
        monkeypatch.setattr("sqldim.application.ask.make_model", lambda *a, **kw: None)
        rc = main(["nl", "ask", "--demo", "show me customers", "--dataset", "commerce"])
        assert rc == 0
        assert "[demo mode" in capsys.readouterr().out
