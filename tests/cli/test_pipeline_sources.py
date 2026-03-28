"""Tests for PipelineSource implementations and run_ask_from_source (§11.10).

Covers:
- DatasetPipelineSource  (wraps a Dataset)
- MedallionPipelineSource (wraps a pre-existing DuckDB connection)
- ObservatoryPipelineSource (wraps a DriftObservatory)
- run_ask_from_source generic driver
- make_model() provider factory
- CLI --source / --layer / --db-path / --model-provider / --model flags
"""

from __future__ import annotations

import pytest
import duckdb

from sqldim.application.ask import (
    DatasetPipelineSource,
    MedallionPipelineSource,
    ObservatoryPipelineSource,
    PipelineSource,
    build_registry_from_schema,
    load_dataset,
    run_ask_from_source,
)
from sqldim.cli import build_parser, main

pytestmark = pytest.mark.filterwarnings(
    "ignore::logfire._internal.config.LogfireNotConfiguredWarning"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_medallion_con() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB pre-populated with two gold-layer tables."""
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE customer_dim (customer_key INTEGER, full_name VARCHAR, region VARCHAR)"
    )
    con.execute(
        "CREATE TABLE revenue_fact (fact_key INTEGER, customer_key INTEGER, revenue DOUBLE)"
    )
    return con


def _make_obs_con() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB suitable for a DriftObservatory."""
    return duckdb.connect(":memory:")


# ---------------------------------------------------------------------------
# PipelineSource ABC
# ---------------------------------------------------------------------------


class TestPipelineSourceABC:
    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            PipelineSource()  # type: ignore[abstract]

    def test_data_dataset_is_pipeline_source(self):
        ds = load_dataset("ecommerce")
        src = DatasetPipelineSource(ds)
        assert isinstance(src, PipelineSource)

    def test_medallion_is_pipeline_source(self):
        con = _make_medallion_con()
        src = MedallionPipelineSource(con)
        assert isinstance(src, PipelineSource)

    def test_observatory_is_pipeline_source(self):
        from sqldim.observability.drift import DriftObservatory

        obs_con = _make_obs_con()
        obs = DriftObservatory(obs_con)
        src = ObservatoryPipelineSource(obs)
        assert isinstance(src, PipelineSource)


# ---------------------------------------------------------------------------
# DatasetPipelineSource
# ---------------------------------------------------------------------------


class TestDatasetPipelineSource:
    def test_label_contains_dataset_class_name(self):
        ds = load_dataset("ecommerce")
        src = DatasetPipelineSource(ds)
        assert src.label.startswith("dataset:")

    def test_setup_creates_connection(self):
        ds = load_dataset("ecommerce")
        src = DatasetPipelineSource(ds)
        src.setup()
        try:
            con = src.get_connection()
            assert con is not None
        finally:
            src.teardown()

    def test_get_table_names_non_empty(self):
        ds = load_dataset("ecommerce")
        src = DatasetPipelineSource(ds)
        src.setup()
        try:
            tables = src.get_table_names()
            assert len(tables) >= 1
        finally:
            src.teardown()

    def test_get_connection_before_setup_raises(self):
        ds = load_dataset("ecommerce")
        src = DatasetPipelineSource(ds)
        with pytest.raises(RuntimeError, match="setup()"):
            src.get_connection()

    def test_teardown_idempotent(self):
        ds = load_dataset("ecommerce")
        src = DatasetPipelineSource(ds)
        src.setup()
        src.teardown()
        src.teardown()  # second teardown must not raise

    def test_schema_can_be_built_from_source(self):
        ds = load_dataset("ecommerce")
        src = DatasetPipelineSource(ds)
        src.setup()
        try:
            registry = build_registry_from_schema(
                src.get_connection(), src.get_table_names()
            )
            assert len(registry.node_terms) >= 1
        finally:
            src.teardown()


# ---------------------------------------------------------------------------
# MedallionPipelineSource
# ---------------------------------------------------------------------------


class TestMedallionPipelineSource:
    def test_label_contains_layer(self):
        con = _make_medallion_con()
        src = MedallionPipelineSource(con, layer="gold")
        assert "gold" in src.label

    def test_default_layer_is_gold(self):
        con = _make_medallion_con()
        src = MedallionPipelineSource(con)
        assert src.label == "medallion:gold"

    def test_setup_is_noop(self):
        con = _make_medallion_con()
        src = MedallionPipelineSource(con)
        src.setup()  # must not raise

    def test_teardown_is_noop(self):
        con = _make_medallion_con()
        src = MedallionPipelineSource(con)
        src.setup()
        src.teardown()  # must not raise; connection is still open

    def test_get_connection_returns_same_con(self):
        con = _make_medallion_con()
        src = MedallionPipelineSource(con)
        assert src.get_connection() is con

    def test_get_table_names_without_registry(self):
        con = _make_medallion_con()
        src = MedallionPipelineSource(con)
        tables = src.get_table_names()
        assert "customer_dim" in tables
        assert "revenue_fact" in tables

    def test_get_table_names_with_registry_filters_by_layer(self):
        """When a MedallionRegistry is provided only its layer's datasets appear."""
        from unittest.mock import MagicMock

        from sqldim.medallion.layer import Layer

        registry = MagicMock()
        registry.datasets_in.return_value = ["revenue_fact"]

        con = _make_medallion_con()
        src = MedallionPipelineSource(con, layer="gold", registry=registry)
        tables = src.get_table_names()

        registry.datasets_in.assert_called_once_with(Layer("gold"))
        assert tables == ["revenue_fact"]

    def test_get_table_names_registry_fallback_on_error(self):
        """When registry.datasets_in raises, fall through to SHOW TABLES."""
        from unittest.mock import MagicMock

        registry = MagicMock()
        registry.datasets_in.side_effect = ValueError("boom")

        con = _make_medallion_con()
        src = MedallionPipelineSource(con, layer="gold", registry=registry)
        tables = src.get_table_names()
        # Falls back to all tables
        assert "customer_dim" in tables

    def test_schema_can_be_built_from_medallion_source(self):
        con = _make_medallion_con()
        src = MedallionPipelineSource(con)
        registry = build_registry_from_schema(
            src.get_connection(), src.get_table_names()
        )
        assert "customer_dim" in registry.node_terms
        assert "revenue" in registry.prop_terms

    def test_silver_layer_label(self):
        con = _make_medallion_con()
        src = MedallionPipelineSource(con, layer="silver")
        assert src.label == "medallion:silver"


# ---------------------------------------------------------------------------
# ObservatoryPipelineSource
# ---------------------------------------------------------------------------


class TestObservatoryPipelineSource:
    def test_label_is_observatory(self):
        from sqldim.observability.drift import DriftObservatory

        obs_con = _make_obs_con()
        obs = DriftObservatory(obs_con)
        src = ObservatoryPipelineSource(obs)
        assert src.label == "observatory"

    def test_setup_is_noop(self):
        from sqldim.observability.drift import DriftObservatory

        obs = DriftObservatory(_make_obs_con())
        src = ObservatoryPipelineSource(obs)
        src.setup()  # must not raise

    def test_teardown_is_noop(self):
        from sqldim.observability.drift import DriftObservatory

        obs = DriftObservatory(_make_obs_con())
        src = ObservatoryPipelineSource(obs)
        src.setup()
        src.teardown()  # must not raise

    def test_get_connection_returns_observatory_con(self):
        from sqldim.observability.drift import DriftObservatory

        obs_con = _make_obs_con()
        obs = DriftObservatory(obs_con)
        src = ObservatoryPipelineSource(obs)
        assert src.get_connection() is obs._con

    def test_get_table_names_returns_six_tables(self):
        from sqldim.observability.drift import DriftObservatory

        obs = DriftObservatory(_make_obs_con())
        src = ObservatoryPipelineSource(obs)
        tables = src.get_table_names()
        assert len(tables) == 6

    def test_get_table_names_contains_all_obs_tables(self):
        from sqldim.observability.drift import DriftObservatory

        obs = DriftObservatory(_make_obs_con())
        src = ObservatoryPipelineSource(obs)
        tables = src.get_table_names()
        expected = {
            "obs_dataset_dim",
            "obs_evolution_type_dim",
            "obs_rule_dim",
            "obs_pipeline_run_dim",
            "obs_schema_evolution_fact",
            "obs_quality_drift_fact",
        }
        assert expected == set(tables)

    def test_schema_can_be_built_from_obs_source(self):
        from sqldim.observability.drift import DriftObservatory

        obs = DriftObservatory(_make_obs_con())
        src = ObservatoryPipelineSource(obs)
        registry = build_registry_from_schema(
            src.get_connection(), src.get_table_names()
        )
        assert "obs_dataset_dim" in registry.node_terms
        assert len(registry.prop_terms) > 0


# ---------------------------------------------------------------------------
# run_ask_from_source
# ---------------------------------------------------------------------------


class TestRunAskFromSource:
    @pytest.fixture(autouse=True)
    def _no_ollama(self, monkeypatch):
        def _raise():
            raise RuntimeError("no Ollama in tests")

        monkeypatch.setattr("sqldim.application.ask.make_ollama_model", _raise)
        monkeypatch.setattr("sqldim.application.ask.make_model", lambda *a, **kw: None)

    def test_dataset_source_returns_zero(self, capsys):
        ds = load_dataset("ecommerce")
        src = DatasetPipelineSource(ds)
        rc = run_ask_from_source("show me customers", src)
        assert rc == 0

    def test_medallion_source_returns_zero(self, capsys):
        con = _make_medallion_con()
        src = MedallionPipelineSource(con)
        rc = run_ask_from_source("show me revenue by customer", src)
        assert rc == 0

    def test_observatory_source_returns_zero(self, capsys):
        from sqldim.observability.drift import DriftObservatory

        obs = DriftObservatory(_make_obs_con())
        src = ObservatoryPipelineSource(obs)
        rc = run_ask_from_source(
            "which datasets have the highest breaking-change rate?", src
        )
        assert rc == 0

    def test_verbose_prints_source_label(self, capsys):
        con = _make_medallion_con()
        src = MedallionPipelineSource(con, layer="gold")
        run_ask_from_source("test", src, verbose=True)
        out = capsys.readouterr().out
        assert "medallion:gold" in out

    def test_verbose_prints_prop_count(self, capsys):
        ds = load_dataset("ecommerce")
        src = DatasetPipelineSource(ds)
        run_ask_from_source("test", src, verbose=True)
        out = capsys.readouterr().out
        assert "PropTerms" in out

    def test_teardown_called_even_on_success(self, capsys):
        """DatasetPipelineSource connection should be None after the call."""
        ds = load_dataset("ecommerce")
        src = DatasetPipelineSource(ds)
        run_ask_from_source("test", src)
        # After teardown the internal connection should be gone
        assert src._con is None


# ---------------------------------------------------------------------------
# CLI --source / --layer / --db-path parser flags
# ---------------------------------------------------------------------------


class TestAskParserNewFlags:
    def test_source_defaults_to_dataset(self):
        parser = build_parser()
        args = parser.parse_args(["ask", "ecommerce", "test"])
        assert args.source == "dataset"

    def test_source_medallion_accepted(self):
        parser = build_parser()
        args = parser.parse_args(["ask", "ecommerce", "test", "--source", "medallion"])
        assert args.source == "medallion"

    def test_source_observatory_accepted(self):
        parser = build_parser()
        args = parser.parse_args(
            ["ask", "ecommerce", "test", "--source", "observatory"]
        )
        assert args.source == "observatory"

    def test_source_invalid_rejected(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["ask", "ecommerce", "test", "--source", "invalid"])

    def test_layer_defaults_to_gold(self):
        parser = build_parser()
        args = parser.parse_args(["ask", "ecommerce", "test", "--source", "medallion"])
        assert args.layer == "gold"

    def test_layer_bronze_accepted(self):
        parser = build_parser()
        args = parser.parse_args(
            ["ask", "ecommerce", "test", "--source", "medallion", "--layer", "bronze"]
        )
        assert args.layer == "bronze"

    def test_layer_silver_accepted(self):
        parser = build_parser()
        args = parser.parse_args(
            ["ask", "ecommerce", "test", "--source", "medallion", "--layer", "silver"]
        )
        assert args.layer == "silver"

    def test_layer_invalid_rejected(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(
                [
                    "ask",
                    "ecommerce",
                    "test",
                    "--source",
                    "medallion",
                    "--layer",
                    "platinum",
                ]
            )

    def test_db_path_default_is_none(self):
        parser = build_parser()
        args = parser.parse_args(["ask", "ecommerce", "test"])
        assert args.db_path is None

    def test_db_path_accepts_string(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "ask",
                "ecommerce",
                "test",
                "--source",
                "medallion",
                "--db-path",
                "/tmp/dw.db",
            ]
        )
        assert args.db_path == "/tmp/dw.db"


# ---------------------------------------------------------------------------
# main() integration with --source
# ---------------------------------------------------------------------------


class TestMainAskWithSource:
    @pytest.fixture(autouse=True)
    def _no_ollama(self, monkeypatch):
        def _raise():
            raise RuntimeError("no Ollama in tests")

        monkeypatch.setattr("sqldim.application.ask.make_ollama_model", _raise)
        monkeypatch.setattr("sqldim.application.ask.make_model", lambda *a, **kw: None)

    def test_main_ask_dataset_source_returns_zero(self, capsys):
        rc = main(["ask", "ecommerce", "show me customers", "--source", "dataset"])
        assert rc == 0

    def test_main_ask_unknown_dataset_returns_one(self, capsys):
        rc = main(["ask", "unknown_ds", "test", "--source", "dataset"])
        assert rc == 1

    def test_main_ask_model_creation_fails_returns_one(self, capsys):
        """make_model raising → cmd_ask returns 1."""
        # Override the autouse fixture's make_model patch to raise instead
        import unittest.mock as mock

        with mock.patch(
            "sqldim.application.ask.make_model",
            side_effect=RuntimeError("bad provider"),
        ):
            rc = main(["ask", "ecommerce", "test", "--model-provider", "openai"])
        assert rc == 1
        out = capsys.readouterr().out
        assert "Could not create model" in out

    def test_main_ask_medallion_source_returns_zero(self, capsys):
        """--source medallion builds MedallionPipelineSource from :memory: db."""
        rc = main(["ask", "ecommerce", "show revenue", "--source", "medallion"])
        assert rc == 0

    def test_main_ask_observatory_source_returns_zero(self, capsys):
        """--source observatory builds DriftObservatory from :memory: db."""
        rc = main(["ask", "ecommerce", "show datasets", "--source", "observatory"])
        assert rc == 0


# ---------------------------------------------------------------------------
# make_model factory — unit tests (no live network calls)
# ---------------------------------------------------------------------------


@pytest.fixture()
def fake_api_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    """Inject fake API keys so provider objects can be constructed without
    real credentials; actual network calls are never made in these tests."""
    monkeypatch.setenv("OPENAI_API_KEY", "fake-key-for-testing")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "fake-key-for-testing")
    monkeypatch.setenv("GROQ_API_KEY", "fake-key-for-testing")
    monkeypatch.setenv("GEMINI_API_KEY", "fake-key-for-testing")
    monkeypatch.setenv("GOOGLE_API_KEY", "fake-key-for-testing")
    monkeypatch.setenv("MISTRAL_API_KEY", "fake-key-for-testing")


class TestMakeModel:
    """Tests that make_model constructs the right pydantic-ai object types."""

    def test_ollama_returns_openai_chat_model(self):
        from pydantic_ai.models.openai import OpenAIChatModel
        from sqldim.core.query.dgm.nl._agents import make_model

        m = make_model("ollama")
        assert isinstance(m, OpenAIChatModel)

    def test_ollama_uses_default_model_name(self):
        from sqldim.core.query.dgm.nl._agents import _PROVIDER_DEFAULTS, make_model

        m = make_model("ollama")
        assert m._model_name == _PROVIDER_DEFAULTS["ollama"]

    def test_ollama_custom_model_name(self):
        from sqldim.core.query.dgm.nl._agents import make_model

        m = make_model("ollama", "mistral:7b")
        assert m._model_name == "mistral:7b"

    def test_openai_returns_openai_chat_model(self, fake_api_keys):
        from pydantic_ai.models.openai import OpenAIChatModel
        from sqldim.core.query.dgm.nl._agents import make_model

        m = make_model("openai")
        assert isinstance(m, OpenAIChatModel)

    def test_openai_default_model_name(self, fake_api_keys):
        from sqldim.core.query.dgm.nl._agents import _PROVIDER_DEFAULTS, make_model

        m = make_model("openai")
        assert m._model_name == _PROVIDER_DEFAULTS["openai"]

    def test_anthropic_returns_anthropic_model(self, fake_api_keys):
        from pydantic_ai.models.anthropic import AnthropicModel
        from sqldim.core.query.dgm.nl._agents import make_model

        m = make_model("anthropic")
        assert isinstance(m, AnthropicModel)

    def test_groq_returns_groq_model(self, fake_api_keys):
        from pydantic_ai.models.groq import GroqModel
        from sqldim.core.query.dgm.nl._agents import make_model

        m = make_model("groq")
        assert isinstance(m, GroqModel)

    def test_gemini_returns_gemini_model(self, fake_api_keys):
        from pydantic_ai.models.google import GoogleModel
        from sqldim.core.query.dgm.nl._agents import make_model

        m = make_model("gemini")
        assert isinstance(m, GoogleModel)

    def test_mistral_returns_mistral_model(self, fake_api_keys):
        try:
            from pydantic_ai.models.mistral import MistralModel  # noqa: F401
        except ImportError:
            pytest.skip(
                "pydantic_ai.models.mistral not available (mistralai package incompatible)"
            )
        from sqldim.core.query.dgm.nl._agents import make_model

        m = make_model("mistral")
        assert isinstance(m, MistralModel)

    def test_unknown_provider_raises_value_error(self):
        from sqldim.core.query.dgm.nl._agents import make_model

        with pytest.raises(ValueError, match="Unknown provider"):
            make_model("nonexistent_provider_xyz")

    def test_provider_names_are_case_insensitive(self):
        from pydantic_ai.models.openai import OpenAIChatModel
        from sqldim.core.query.dgm.nl._agents import make_model

        m = make_model("OLLAMA")
        assert isinstance(m, OpenAIChatModel)

    def test_make_ollama_model_uses_make_model(self):
        from pydantic_ai.models.openai import OpenAIChatModel
        from sqldim.core.query.dgm.nl._agents import make_ollama_model

        m = make_ollama_model()
        assert isinstance(m, OpenAIChatModel)

    def test_make_model_exported_from_nl_package(self):
        from sqldim.core.query.dgm.nl import make_model

        assert callable(make_model)

    def test_make_model_exported_from_ask(self):
        from sqldim.application.ask import make_model

        assert callable(make_model)


# ---------------------------------------------------------------------------
# CLI --model-provider / --model flags
# ---------------------------------------------------------------------------


class TestAskParserModelFlags:
    def test_model_provider_defaults_to_ollama(self):
        parser = build_parser()
        args = parser.parse_args(["ask", "ecommerce", "test"])
        assert args.model_provider == "ollama"

    def test_model_provider_openai_accepted(self):
        parser = build_parser()
        args = parser.parse_args(
            ["ask", "ecommerce", "test", "--model-provider", "openai"]
        )
        assert args.model_provider == "openai"

    def test_model_provider_anthropic_accepted(self):
        parser = build_parser()
        args = parser.parse_args(
            ["ask", "ecommerce", "test", "--model-provider", "anthropic"]
        )
        assert args.model_provider == "anthropic"

    def test_model_provider_gemini_accepted(self):
        parser = build_parser()
        args = parser.parse_args(
            ["ask", "ecommerce", "test", "--model-provider", "gemini"]
        )
        assert args.model_provider == "gemini"

    def test_model_provider_groq_accepted(self):
        parser = build_parser()
        args = parser.parse_args(
            ["ask", "ecommerce", "test", "--model-provider", "groq"]
        )
        assert args.model_provider == "groq"

    def test_model_provider_mistral_accepted(self):
        parser = build_parser()
        args = parser.parse_args(
            ["ask", "ecommerce", "test", "--model-provider", "mistral"]
        )
        assert args.model_provider == "mistral"

    def test_model_provider_invalid_rejected(self):
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(
                ["ask", "ecommerce", "test", "--model-provider", "cohere"]
            )

    def test_model_name_defaults_to_none(self):
        parser = build_parser()
        args = parser.parse_args(["ask", "ecommerce", "test"])
        assert args.model_name is None

    def test_model_name_custom_accepted(self):
        parser = build_parser()
        args = parser.parse_args(
            ["ask", "ecommerce", "test", "--model", "llama3.3:latest"]
        )
        assert args.model_name == "llama3.3:latest"

    def test_model_name_with_provider(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "ask",
                "ecommerce",
                "test",
                "--model-provider",
                "openai",
                "--model",
                "gpt-4o",
            ]
        )
        assert args.model_provider == "openai"
        assert args.model_name == "gpt-4o"
