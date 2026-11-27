"""The DGM models the bundled datasets declare (§2.6), and both tools on them.

Each declaration is written by hand from its domain's source definitions.
These tests open every declared dataset the way the CLI does
(``DatasetPipelineSource`` through ``DataAccess``) and check that every
declared relation, column and foreign key exists with a conforming type, and
that both model-reading tools succeed end to end on a bundled dataset.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import duckdb
import pytest

from sqldim.application._pipeline_sources import DatasetPipelineSource
from sqldim.application.ask import KNOWN_DATASETS, load_dataset
from sqldim.application.supervisor_host import local_auth_context
from sqldim.core.query.dgm.model import (
    DGMModel,
    ForeignKey,
    Holder,
    ModelNotDeclared,
    NodeTable,
)
from sqldim.core.query.dgm.nl.supervisor._dispatch import execute_tool
from sqldim.core.query.dgm.nl.supervisor._invariant_core import conforms
from sqldim.core.query.dgm.nl.supervisor._schemas import (
    MapAnalyticalSpaceOutput,
    ValidateInvariantOutput,
)
from sqldim.core.query.dgm.nl.supervisor._tools import build_registry
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    ToolCallResult,
    ToolErrorKind,
)
from sqldim.core.query.dgm.nl.supervisor._validation import bind_plan
from tests.supervisor._fakes import plan, step, tool_context

DECLARED = ("devops", "fintech", "growth", "nba_analytics", "telemetry")
"""Bundled datasets that declare a model; each is reviewed per dataset."""

UNDECLARED = ("commerce", "data_models", "entertainment")
"""Bundled datasets whose structure cannot be declared faithfully (see PR)."""


def _open(name: str) -> DatasetPipelineSource:
    return DatasetPipelineSource(load_dataset(name))


def test_every_bundled_dataset_is_classified() -> None:
    assert set(DECLARED) | set(UNDECLARED) == set(KNOWN_DATASETS)


@pytest.mark.parametrize("name", UNDECLARED)
def test_undeclared_dataset_raises_model_not_declared(name: str) -> None:
    with pytest.raises(ModelNotDeclared):
        _open(name).dgm_model()


# ---------------------------------------------------------------------------
# Declarations match the real tables
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", params=DECLARED)
def opened(request: Any) -> Iterator[tuple[DGMModel, duckdb.DuckDBPyConnection]]:
    source = _open(request.param)
    model = source.dgm_model()
    source.setup()
    try:
        yield model, source.get_connection()
    finally:
        source.teardown()


def _columns(con: duckdb.DuckDBPyConnection, relation: str) -> dict[str, str]:
    rows = con.execute(f'DESCRIBE "{relation}"').fetchall()
    return {str(r[0]): str(r[1]) for r in rows}


def _mapped(model: DGMModel) -> Iterator[tuple[str, NodeTable]]:
    for node in model.node_types:
        assert node.mapping is not None, f"{node.name} has no NodeTable"
        yield node.name, node.mapping


def test_every_relation_column_and_type_exists(opened: Any) -> None:
    model, con = opened
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    for name, table in _mapped(model):
        assert table.relation in tables, f"{name}: no relation {table.relation}"
        cols = _columns(con, table.relation)
        missing = [c for c in table.columns() if c not in cols]
        assert not missing, f"{name}: missing columns {missing}"
        node = model.node(name)
        assert node is not None
        for key, type_ in node.properties.items():
            column = cols[table.props[key]]
            assert conforms(column, type_), f"{name}.{key}: {column} is not {type_}"
        if table.event_time is not None:
            assert cols[table.event_time] in ("DATE", "TIMESTAMP")


def test_entity_keys_identify_rows(opened: Any) -> None:
    model, con = opened
    for name, table in _mapped(model):
        key = ", ".join(f'"{k}"' for k in table.entity_key)
        rows, distinct = con.execute(
            f'SELECT COUNT(*), COUNT(DISTINCT ({key})) FROM "{table.relation}"'
        ).fetchone()
        assert rows == distinct, f"{name}: entity_key {table.entity_key} repeats"


def test_every_foreign_key_column_exists_with_the_key_type(opened: Any) -> None:
    model, con = opened
    for edge in model.edge_types:
        fk = edge.mapping
        assert isinstance(fk, ForeignKey), edge.key
        holder, referenced = (
            (edge.target, edge.source)
            if fk.holder is Holder.TARGET
            else (edge.source, edge.target)
        )
        held = dict(_mapped(model))[holder]
        ref = dict(_mapped(model))[referenced]
        held_cols, ref_cols = _columns(con, held.relation), _columns(con, ref.relation)
        assert len(fk.columns) == len(ref.entity_key)
        for column, key in zip(fk.columns, ref.entity_key):
            assert column in held_cols, f"{edge.key}: no column {column}"
            assert held_cols[column] == ref_cols[key], f"{edge.key}: {column} type"


# ---------------------------------------------------------------------------
# Both tools succeed on a bundled dataset, end to end
# ---------------------------------------------------------------------------


def _call(tool: str, arguments: dict[str, Any], dataset: str) -> ToolCallResult:
    bound, errors = bind_plan(plan(step("s", tool, arguments, fraction=1.0)))
    assert errors == []
    ctx = tool_context(local_auth_context([dataset]), _open)  # type: ignore[arg-type]
    ctx = dataclasses.replace(
        ctx, budget=dataclasses.replace(ctx.budget, max_wall_time=timedelta(minutes=5))
    )
    return asyncio.run(execute_tool(bound.steps[0], build_registry(), ctx))


def test_map_analytical_space_on_nba_analytics() -> None:
    result = _call(
        "dgm_map_analytical_space", {"dataset": "nba_analytics"}, "nba_analytics"
    )
    assert result.status is CallStatus.SUCCEEDED, result.error
    MapAnalyticalSpaceOutput.model_validate(result.data)
    vocab = result.data["vocabularies"]
    assert vocab["V_N"] == ["Game", "GameDetail", "PlayerSeason", "Team"]
    assert "(Team, home, Game)" in vocab["V_E"]
    assert "RolePlaying(dim=Team, roles={home, visitor})" in vocab["Σ"]
    assert ["Team@home", "Team@visitor"] in result.data["anchor_sets"]


@pytest.mark.parametrize("invariant", ["PARTITION", "M1", "H3", "M3", "M7"])
def test_validate_invariant_on_fintech(invariant: str) -> None:
    result = _call(
        "dgm_validate_invariant",
        {"dataset": "fintech", "invariant": invariant, "max_witnesses": 5},
        "fintech",
    )
    assert result.status is CallStatus.SUCCEEDED, result.error
    data = ValidateInvariantOutput.model_validate(result.data)
    assert data.holds is True


def test_tools_on_an_undeclared_bundled_dataset_are_unsupported() -> None:
    for tool, args in (
        ("dgm_map_analytical_space", {"dataset": "commerce"}),
        (
            "dgm_validate_invariant",
            {"dataset": "commerce", "invariant": "M1", "max_witnesses": 5},
        ),
    ):
        result = _call(tool, args, "commerce")
        assert result.status is CallStatus.FAILED and result.data is None
        assert result.error is not None
        assert result.error.kind is ToolErrorKind.UNSUPPORTED
