"""The DGM models the bundled datasets declare (§2.6), and the tools on them.

Each declaration is written by hand from its domain's source definitions.
These tests open every declared dataset the way the CLI does
(``DatasetPipelineSource`` through ``DataAccess``) and check that every
declared relation, column and foreign key exists with a conforming type, that
the Σ kinds every declaration assesses (Grain, Conformed, BridgeSemantics, …)
follow the bundled conventions, and that the model-reading tools succeed end
to end on a bundled dataset.
"""

from __future__ import annotations

import asyncio
import dataclasses
import re
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb
import pytest

from sqldim.application._pipeline_sources import DatasetPipelineSource
from sqldim.application.ask import KNOWN_DATASETS, load_dataset
from sqldim.application.datasets.model_decl import ASSESSED_KINDS
from sqldim.application.supervisor_host import local_auth_context
from sqldim.core.query.dgm.annotations import (
    Additivity,
    AdditivityKind,
    BridgeSemantics,
    Conformed,
    Degenerate,
    Grain,
)
from sqldim.core.query.dgm.lattice import QuestionLattice, first
from sqldim.core.query.dgm.lattice._context import JoinCtx
from sqldim.core.query.dgm.model import (
    DGMModel,
    EdgeKind,
    EdgeType,
    ForeignKey,
    Holder,
    ModelNotDeclared,
    NodeKind,
    NodeTable,
    RelationTable,
)
from sqldim.core.query.dgm.nl.supervisor._dispatch import execute_tool
from sqldim.core.query.dgm.nl.supervisor._invariant_core import conforms
from sqldim.core.query.dgm.nl.supervisor._schemas import (
    MapAnalyticalSpaceOutput,
    ValidateInvariantOutput,
    WhatCanIAnswerOutput,
)
from sqldim.core.query.dgm.nl.supervisor._tools import build_registry
from sqldim.core.query.dgm.nl.supervisor._types import (
    CallStatus,
    ToolCallResult,
    ToolErrorKind,
)
from sqldim.core.query.dgm.nl.supervisor._validation import bind_plan
from tests.supervisor._fakes import plan, step, tool_context

DECLARED = (
    "commerce",
    "data_models",
    "devops",
    "entertainment",
    "fintech",
    "growth",
    "nba_analytics",
    "telemetry",
)
"""Bundled datasets that declare a model; each is reviewed per dataset."""


def _open(name: str) -> DatasetPipelineSource:
    return DatasetPipelineSource(load_dataset(name))


def _open_without_model(name: str) -> DatasetPipelineSource:
    return DatasetPipelineSource(dataclasses.replace(load_dataset(name), model=None))


def test_every_bundled_dataset_declares_a_model() -> None:
    assert set(DECLARED) == set(KNOWN_DATASETS)


def test_a_dataset_without_a_model_raises_model_not_declared() -> None:
    with pytest.raises(ModelNotDeclared):
        _open_without_model("commerce").dgm_model()


# ---------------------------------------------------------------------------
# Declarations match the real tables
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", params=DECLARED)
def opened(request: Any) -> Iterator[tuple[DGMModel, duckdb.DuckDBPyConnection]]:
    source = _open(request.param)
    model = source.dgm_model()
    source.setup()
    try:
        con = source.get_connection()
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        # The registered tables are the tables setup() creates (#165).
        assert set(source.get_table_names()) == tables
        yield model, con
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
        nulls = " OR ".join(f'"{k}" IS NULL' for k in table.entity_key)
        rows, distinct, null = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT ({key})), COUNT(*) FILTER ({nulls}) "
            f'FROM "{table.relation}"'
        ).fetchone()
        assert rows > 0, f"{name}: {table.relation} is empty"
        assert null == 0, f"{name}: entity_key {table.entity_key} has NULLs"
        assert rows == distinct, f"{name}: entity_key {table.entity_key} repeats"


def _references(model: DGMModel, edge: EdgeType) -> list[tuple[str, tuple, str]]:
    """``(relation, columns, referenced type)`` for each stored reference."""
    mapping = edge.mapping
    if isinstance(mapping, RelationTable):
        return [
            (mapping.relation, mapping.source, edge.source),
            (mapping.relation, mapping.target, edge.target),
        ]
    assert isinstance(mapping, ForeignKey), edge.key
    holder, referenced = (
        (edge.target, edge.source)
        if mapping.holder is Holder.TARGET
        else (edge.source, edge.target)
    )
    return [(dict(_mapped(model))[holder].relation, mapping.columns, referenced)]


INTENTIONAL_DANGLING: dict[tuple[str, tuple[str, ...]], str] = {}
"""``(relation, columns) → reason`` for references that may name no entity.

Empty: every bundled reference resolves (#168).  A dangling reference is
accepted only when it is recorded here with the reason it is intentional.
"""


def _dangling(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    columns: tuple[str, ...],
    referenced: NodeTable,
) -> int:
    """Rows of *relation* whose non-NULL *columns* name no *referenced* row."""
    on = " AND ".join(
        f'h."{c}" = t."{k}"' for c, k in zip(columns, referenced.entity_key)
    )
    present = " AND ".join(f'h."{c}" IS NOT NULL' for c in columns)
    row = con.execute(
        f'SELECT COUNT(*) FROM "{relation}" h WHERE {present} AND NOT EXISTS '
        f'(SELECT 1 FROM "{referenced.relation}" t WHERE {on})'
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_no_declared_reference_dangles(opened: Any) -> None:
    """Every stored reference of every declared edge names an entity (#168)."""
    model, con = opened
    tables = dict(_mapped(model))
    dangling = {}
    for edge in model.edge_types:
        for relation, columns, referenced in _references(model, edge):
            count = _dangling(con, relation, tuple(columns), tables[referenced])
            if count and (relation, tuple(columns)) not in INTENTIONAL_DANGLING:
                dangling[f"{edge.key} {relation}.{columns}"] = count
    assert dangling == {}


def test_the_dangling_measure_counts_unresolved_references() -> None:
    con = duckdb.connect()
    con.execute("CREATE TABLE p AS SELECT * FROM range(1, 4) t(id)")
    con.execute("CREATE TABLE c AS SELECT * FROM (VALUES (1), (3), (7), (NULL)) t(p)")
    parent = NodeTable(relation="p", entity_key=("id",))
    assert _dangling(con, "c", ("p",), parent) == 1


# ---------------------------------------------------------------------------
# Generation does not depend on the day it runs (#168)
# ---------------------------------------------------------------------------

OBSERVED: dict[str, frozenset[str]] = {
    "telemetry": frozenset({"pipeline_spans", "metric_samples"}),
}
"""Relations that record a real run of the pipelines (spans, OTel samples),
not generated data: their timestamps and durations are observations."""


def _shift_faker_clock(monkeypatch: pytest.MonkeyPatch, days: int) -> None:
    """Move the clock Faker's date providers read by *days*."""
    import faker.providers.date_time as faker_dt

    real_datetime, real_date = faker_dt.datetime, faker_dt.dtdate
    delta = timedelta(days=days)

    class _Datetime(real_datetime):  # type: ignore[misc,valid-type]
        @classmethod
        def now(cls, tz: Any = None) -> Any:
            moved = real_datetime.now(tz) + delta
            return cls.combine(moved.date(), moved.timetz())

    class _Date(real_date):  # type: ignore[misc,valid-type]
        @classmethod
        def today(cls) -> Any:
            return cls.fromordinal((real_date.today() + delta).toordinal())

    monkeypatch.setattr(faker_dt, "datetime", _Datetime)
    monkeypatch.setattr(faker_dt, "dtdate", _Date)


def _fresh_dataset(name: str) -> Any:
    """Build *name*'s dataset again, outside the cached module singleton."""
    import importlib
    import importlib.util

    mod_path, attr = KNOWN_DATASETS[name]
    origin = importlib.import_module(mod_path).__file__
    assert origin is not None
    spec = importlib.util.spec_from_file_location(f"_fresh_{name}", origin)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, attr)


def _sql(make: Any) -> str | None:
    try:
        return str(make().as_sql(None))
    except NotImplementedError:
        return None


def _generated(src: Any, table: str) -> dict[str, Any]:
    """What *src* generates: its snapshot and event SQL or, for a source
    that fills its own tables in ``setup()``, the rows of those tables."""
    snapshot = _sql(src.snapshot)
    if snapshot is not None:
        return {"snapshot": snapshot, "events": _sql(lambda: src.event_batch(1))}
    con = duckdb.connect()
    try:
        src.setup(con, table)
        return {
            t: con.execute(f'SELECT * FROM "{t}" ORDER BY ALL').fetchall()
            for t in src.tables(table)
        }
    finally:
        con.close()


def _contents(dataset: Any) -> dict[str, Any]:
    skip = OBSERVED.get(dataset.name, frozenset())
    return {
        table: _generated(src, table)
        for src, table in dataset.sources
        if table not in skip
    }


@pytest.mark.parametrize("name", DECLARED)
def test_generation_does_not_depend_on_the_day(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dataset generated 400 days later is the same dataset."""
    today = _contents(load_dataset(name))
    _shift_faker_clock(monkeypatch, days=400)
    later = _contents(_fresh_dataset(name))
    assert later == today


def test_the_clock_shift_moves_faker_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    """The probe above would catch a generator that read Faker's clock."""
    from faker import Faker

    fake = Faker()
    Faker.seed(0)
    before = fake.date_this_year()
    _shift_faker_clock(monkeypatch, days=400)
    Faker.seed(0)
    assert fake.date_this_year() != before


def test_every_edge_column_exists_with_the_key_type(opened: Any) -> None:
    model, con = opened
    tables = dict(_mapped(model))
    for edge in model.edge_types:
        for relation, columns, referenced in _references(model, edge):
            held_cols = _columns(con, relation)
            ref = tables[referenced]
            ref_cols = _columns(con, ref.relation)
            assert len(columns) == len(ref.entity_key)
            for column, key in zip(columns, ref.entity_key):
                assert column in held_cols, f"{edge.key}: no column {column}"
                assert held_cols[column] == ref_cols[key], f"{edge.key}: {column}"
        if isinstance(edge.mapping, RelationTable):
            cols = _columns(con, edge.mapping.relation)
            for key, type_ in edge.properties.items():
                column = cols[edge.mapping.props[key]]
                assert conforms(column, type_), f"{edge.key}.{key}: {column}"


# ---------------------------------------------------------------------------
# Σ: the kinds every bundled declaration assesses
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", params=DECLARED)
def model(request: Any) -> DGMModel:
    return _open(request.param).dgm_model()


def _of_kind(model: DGMModel, kind: NodeKind) -> set[str]:
    return {n.name for n in model.node_types if n.kind is kind}


def _verb_facts(model: DGMModel, dim: str) -> frozenset[str]:
    return frozenset(
        e.target
        for e in model.edge_types
        if e.kind is EdgeKind.VERB and e.source == dim
    )


def test_every_declaration_assesses_the_bundled_kinds(model: DGMModel) -> None:
    assert model.sigma_kinds >= ASSESSED_KINDS
    assert {"Grain", "Conformed", "BridgeSemantics"} <= ASSESSED_KINDS
    assert {"Additivity", "Degenerate"} <= ASSESSED_KINDS


def test_every_numeric_fact_property_is_a_measure_or_degenerate(
    model: DGMModel,
) -> None:
    """Each has exactly one explicit Additivity or Degenerate (§2.4)."""
    marked = [
        (a.fact, a.key) for a in model.sigma if isinstance(a, (Additivity, Degenerate))
    ]
    numeric = [
        (t.name, k)
        for t in model.node_types
        if t.kind is NodeKind.FACT
        for k, v in t.properties.items()
        if v.name in ("Integer", "Real")
    ]
    assert sorted(marked) == sorted(numeric)


def test_every_fact_type_has_one_grain(model: DGMModel) -> None:
    grains = [a.fact for a in model.annotations(Grain)]
    assert sorted(grains) == sorted(_of_kind(model, NodeKind.FACT))


def test_every_bridge_edge_type_has_one_bridge_semantics(model: DGMModel) -> None:
    labels = [a.bridge for a in model.annotations(BridgeSemantics)]
    bridges = {e.label for e in model.edge_types if e.kind is EdgeKind.BRIDGE}
    assert sorted(labels) == sorted(bridges)


def test_conformed_where_a_dimension_serves_two_fact_types(model: DGMModel) -> None:
    """§2.4: ``Conformed(d, Fs)`` — d reaches each type of Fs by a verb edge."""
    declared = {a.dim: a.fact_types for a in model.annotations(Conformed)}
    shared = {
        d: facts
        for d in _of_kind(model, NodeKind.DIM)
        if len(facts := _verb_facts(model, d)) >= 2
    }
    assert declared == shared


# ---------------------------------------------------------------------------
# The tools on a bundled dataset, end to end
# ---------------------------------------------------------------------------


def _call(
    tool: str, arguments: dict[str, Any], dataset: str, *, opener: Any = _open
) -> ToolCallResult:
    bound, errors = bind_plan(plan(step("s", tool, arguments, fraction=1.0)))
    assert errors == []
    ctx = tool_context(local_auth_context([dataset]), opener)
    ctx = dataclasses.replace(
        ctx,
        budget=dataclasses.replace(ctx.budget, max_wall_time=timedelta(minutes=5)),
        deadline=datetime.now(timezone.utc) + timedelta(minutes=5),
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


@pytest.mark.parametrize("dataset", ["commerce", "data_models", "entertainment"])
@pytest.mark.parametrize("invariant", ["PARTITION", "M1", "H3", "M3", "M4"])
def test_invariants_hold_on_the_new_declarations(dataset: str, invariant: str) -> None:
    result = _call(
        "dgm_validate_invariant",
        {"dataset": dataset, "invariant": invariant, "max_witnesses": 5},
        dataset,
    )
    assert result.status is CallStatus.SUCCEEDED, result.error
    data = ValidateInvariantOutput.model_validate(result.data)
    assert data.holds is True, data


def _anchors(dataset: str, anchors: list[str]) -> dict[str, Any]:
    return {"dataset": dataset, "anchors": anchors, "max_hops": 1, "limit": 5}


def test_tools_on_a_dataset_without_a_model_are_unsupported() -> None:
    for tool, args in (
        ("dgm_map_analytical_space", {"dataset": "commerce"}),
        (
            "dgm_validate_invariant",
            {"dataset": "commerce", "invariant": "M1", "max_witnesses": 5},
        ),
        ("dgm_what_can_i_answer", _anchors("commerce", ["Customer", "Order"])),
    ):
        result = _call(tool, args, "commerce", opener=_open_without_model)
        assert result.status is CallStatus.FAILED and result.data is None
        assert result.error is not None
        assert result.error.kind is ToolErrorKind.UNSUPPORTED


@pytest.mark.parametrize(
    ("dataset", "anchors"),
    [
        ("nba_analytics", ["Game", "Team"]),
        ("commerce", ["Customer", "Order"]),
        ("data_models", ["Customer", "Sale"]),
        ("entertainment", ["Leitura", "Pessoa"]),
    ],
)
def test_what_can_i_answer_over_a_numeric_fact(
    dataset: str, anchors: list[str]
) -> None:
    """Grain and Additivity are declared, so SUM is decidable (§12.3)."""
    args = {**_anchors(dataset, anchors), "limit": 200}
    result = _call("dgm_what_can_i_answer", args, dataset)
    assert result.status is CallStatus.SUCCEEDED, result.error
    data = WhatCanIAnswerOutput.model_validate(result.data)
    assert len(data.questions) == 200 and data.complete is False
    model = _open(dataset).dgm_model()
    for q in data.questions:
        _assert_only_additive_sums(model, q.skeleton)
    _assert_aggregate_universe(model, anchors)


_SUMMING = ("SUM", "AVG", "PROD")
_AGG = re.compile(r"\b(SUM|AVG|PROD)\((a\d+)\.(\w+)\)")


def _additive(model: DGMModel) -> set[tuple[str, str]]:
    return {
        (a.fact, a.key)
        for a in model.annotations(Additivity)
        if a.kind is AdditivityKind.ADDITIVE
    }


def _dim_attribute(model: DGMModel, type_: str) -> bool:
    node = model.node(type_)
    return node is not None and node.kind is NodeKind.DIM


def _assert_only_additive_sums(model: DGMModel, skeleton: str) -> None:
    """SUM and PROD only over an ADDITIVE measure (AD1–AD4); AVG also over a
    numeric dimension attribute, read over ENTITIES (§2.4)."""
    head = skeleton.split(" | ")[0].removeprefix("ANCHORS ")
    alias_type = {a: t for t, a in (part.split() for part in head.split(", "))}
    for fn, alias, key in _AGG.findall(skeleton):
        type_ = alias_type[alias]
        if fn == "AVG" and _dim_attribute(model, type_):
            continue
        assert (type_, key) in _additive(model), skeleton


def _assert_aggregate_universe(model: DGMModel, anchors: list[str]) -> None:
    """Every aggregate and running window the enumeration may emit for the
    first Join: SUM/PROD only over ADDITIVE measures, AVG also over a
    dimension attribute but never in a window, and nothing but COUNT over a
    Degenerate identifier."""
    lattice = QuestionLattice(model, max_anchors=3, max_hops=1)
    (question,), _ = first(lattice.enumerate(anchors, max_hops=1), 1)
    ctx = JoinCtx(lattice.space(anchors, max_hops=1), question.skeleton.join)
    additive = _additive(model)
    aggs = [(t[1], t[2]) for t in ctx.agg_terms]
    running = [
        (w[1][1], w[1][2])
        for w in ctx.window_terms(ctx.prop_refs, 2)
        if w[1][0] == "running"
    ]
    for fn, ref in aggs + running:
        if fn not in _SUMMING or ref[0] != "prop":
            continue
        type_ = ctx.alias_type[ref[1]]
        if fn == "AVG" and (fn, ref) in aggs and _dim_attribute(model, type_):
            continue
        assert (type_, ref[2]) in additive, (fn, ref)
    degenerate = {(a.fact, a.key) for a in model.annotations(Degenerate)}
    for fn, ref in aggs + running:
        if ref[0] == "prop" and (ctx.alias_type[ref[1]], ref[2]) in degenerate:
            assert fn == "COUNT", (fn, ref)


def test_a_numeric_dimension_attribute_takes_min_max_avg_count() -> None:
    """§2.4: ``Pessoa.idade`` is a numeric property of a dimension type, not a
    measure: MIN, MAX, AVG and COUNT over ENTITIES, never SUM; a window over
    the tuples of C still takes COUNT only."""
    args = {**_anchors("entertainment", ["Leitura", "Pessoa"]), "limit": 200}
    result = _call("dgm_what_can_i_answer", args, "entertainment")
    assert result.status is CallStatus.SUCCEEDED, result.error
    data = WhatCanIAnswerOutput.model_validate(result.data)
    over_idade = {
        fn
        for q in data.questions
        for fn in re.findall(r"\b(\w+)\(a2\.idade\)", q.skeleton)
    }
    assert over_idade == {"AVG", "COUNT", "MAX", "MIN"}
    model = _open("entertainment").dgm_model()
    lattice = QuestionLattice(model, max_anchors=3, max_hops=1)
    (question,), _ = first(lattice.enumerate(["Leitura", "Pessoa"], max_hops=1), 1)
    ctx = JoinCtx(
        lattice.space(["Leitura", "Pessoa"], max_hops=1), question.skeleton.join
    )
    idade = ("prop", "a2", "idade")
    assert {t[1] for t in ctx.agg_terms if t[2] == idade} == {
        "AVG",
        "COUNT",
        "MAX",
        "MIN",
    }
    windows = {
        w[1][1]
        for w in ctx.window_terms(ctx.prop_refs, 2)
        if w[1][0] == "running" and w[1][2] == idade
    }
    assert windows == {"COUNT"}


def _open_withholding(kind: str) -> Any:
    """Open a bundled dataset whose declaration leaves *kind* undeclared."""

    def opener(name: str) -> DatasetPipelineSource:
        dataset = load_dataset(name)
        build = dataset.model
        assert build is not None

        def model() -> DGMModel:
            declared = build()
            return dataclasses.replace(
                declared,
                sigma=tuple(a for a in declared.sigma if type(a).__name__ != kind),
                sigma_kinds=declared.sigma_kinds - {kind},
            )

        return DatasetPipelineSource(dataclasses.replace(dataset, model=model))

    return opener


@pytest.mark.parametrize("kind", ["Grain", "Additivity", "Degenerate"])
@pytest.mark.parametrize(
    ("dataset", "anchors"),
    [("nba_analytics", ["Game", "Team"]), ("commerce", ["Customer", "Order"])],
)
def test_without_grain_or_additivity_a_numeric_fact_is_unsupported(
    dataset: str, anchors: list[str], kind: str
) -> None:
    """The same anchor sets fail closed when a kind deciding SUM is withheld."""
    result = _call(
        "dgm_what_can_i_answer",
        _anchors(dataset, anchors),
        dataset,
        opener=_open_withholding(kind),
    )
    assert result.status is CallStatus.FAILED and result.data is None
    assert result.error is not None
    assert result.error.kind is ToolErrorKind.UNSUPPORTED
    assert kind in result.error.message


def test_a_semi_additive_measure_is_resolved_without_sum() -> None:
    """balance is SEMI_ADDITIVE: AD4 aggregates only, SUM not enumerated (§12.3)."""
    from sqldim.core.query.dgm.lattice._space import _BY_KIND, _measures, _typed_props

    model = _open("growth").dgm_model()
    props = _typed_props(model, frozenset({"AccountBalance"}))
    measures = _measures(model.sigma, props, ["AccountBalance"])
    kinds = {measures[m] for m in measures if m[1] == "balance"}
    assert kinds == {"SEMI_ADDITIVE"}
    assert _BY_KIND["SEMI_ADDITIVE"] == {"COUNT", "MIN", "MAX", "AVG"}
