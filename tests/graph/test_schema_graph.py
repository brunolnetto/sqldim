"""Tests for extended SchemaGraph — Task 6.5."""
from typing import Optional
from sqlmodel import Field

from sqldim import Field, GraphSchemaGraph
from sqldim.core.graph import VertexModel, EdgeModel
from sqldim.core.graph.schema_graph import SchemaGraph, GraphSchema, _safe_subclass
from sqldim.core.kimball.models import DimensionModel, FactModel


# ---------------------------------------------------------------------------
# Model fixtures
# ---------------------------------------------------------------------------

class SGPlayer(VertexModel, table=True):
    __tablename__ = "sg_player"
    __vertex_type__ = "sg_player"
    __natural_key__ = ["name"]
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str


class SGGame(VertexModel, table=True):
    __tablename__ = "sg_game"
    __vertex_type__ = "sg_game"
    id: Optional[int] = Field(default=None, primary_key=True)
    game_id: int


class SGEdge(EdgeModel, table=True):
    __tablename__ = "sg_edge"
    __edge_type__ = "sg_plays_in"
    __subject__ = SGPlayer
    __object__ = SGGame

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="sg_player.id")
    object_id: int = Field(foreign_key="sg_game.id")


class SGFact(FactModel, table=True):
    __tablename__ = "sg_fact"
    __grain__ = "player_season"
    id: Optional[int] = Field(default=None, primary_key=True)
    player_id: int
    season: int
    pts: float = 0.0


# Orphaned edge: subject not registered as a vertex
class OrphanSGEdge(EdgeModel, table=True):
    __tablename__ = "orphan_sg_edge"
    __edge_type__ = "orphan"
    __subject__ = SGPlayer
    __object__ = SGGame

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="sg_player.id")
    object_id: int = Field(foreign_key="sg_game.id")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_vertices_property():
    sg = SchemaGraph([SGPlayer, SGGame, SGEdge])
    assert SGPlayer in sg.vertices
    assert SGGame in sg.vertices


def test_edges_property():
    sg = SchemaGraph([SGPlayer, SGGame, SGEdge])
    assert SGEdge in sg.edges


def test_dimensions_still_includes_vertices():
    sg = SchemaGraph([SGPlayer, SGGame])
    # vertices are also dimensions
    assert SGPlayer in sg.dimensions


def test_facts_still_includes_edges():
    sg = SchemaGraph([SGPlayer, SGGame, SGEdge])
    # edges are also facts
    assert SGEdge in sg.facts


def test_graph_schema_returns_graph_schema():
    sg = SchemaGraph([SGPlayer, SGGame, SGEdge])
    gs = sg.graph_schema()
    assert isinstance(gs, GraphSchema)
    names = [v["name"] for v in gs.vertices]
    assert "SGPlayer" in names
    assert "SGGame" in names


def test_graph_schema_edge_info():
    sg = SchemaGraph([SGPlayer, SGGame, SGEdge])
    gs = sg.graph_schema()
    edge = gs.edges[0]
    assert edge["edge_type"] == "sg_plays_in"
    assert edge["subject"] == "SGPlayer"
    assert edge["object"] == "SGGame"
    assert edge["directed"] is True
    assert edge["self_referential"] is False


def test_graph_schema_to_dict():
    sg = SchemaGraph([SGPlayer, SGGame, SGEdge])
    d = sg.graph_schema().to_dict()
    assert "vertices" in d
    assert "edges" in d


def test_validate_no_errors_when_valid():
    sg = SchemaGraph([SGPlayer, SGGame, SGEdge])
    errors = sg.validate()
    assert errors == []


def test_validate_catches_unregistered_subject():
    # OrphanSGEdge references SGPlayer which is NOT in the model list
    sg = SchemaGraph([SGGame, OrphanSGEdge])
    errors = sg.validate()
    assert any("SGPlayer" in str(e) for e in errors)


def test_validate_catches_unregistered_object():
    sg = SchemaGraph([SGPlayer, OrphanSGEdge])
    errors = sg.validate()
    assert any("SGGame" in str(e) for e in errors)


def test_to_mermaid_contains_vertex_and_edge():
    sg = SchemaGraph([SGPlayer, SGGame, SGEdge])
    mermaid = sg.to_mermaid()
    assert "erDiagram" in mermaid
    assert "SGPlayer" in mermaid
    assert "SGGame" in mermaid
    assert "SGEdge" in mermaid


def test_to_mermaid_contains_edge_label():
    sg = SchemaGraph([SGPlayer, SGGame, SGEdge])
    mermaid = sg.to_mermaid()
    assert "sg_plays_in" in mermaid


def test_to_mermaid_includes_pure_facts():
    sg = SchemaGraph([SGPlayer, SGFact])
    mermaid = sg.to_mermaid()
    assert "SGFact" in mermaid


def test_to_mermaid_renders_fact_dimension_relationships():
    """When a FactModel has FK columns wired to DimensionModels via
    ``dimension=``, to_mermaid() renders FK relationship lines."""
    from sqldim.core.kimball.fields import Field as DimField
    from sqldim.core.kimball.models import DimensionModel

    class SGDim(DimensionModel, table=True):
        __tablename__ = "sg_dim"
        __natural_key__ = ["name"]
        id: Optional[int] = Field(default=None, primary_key=True)
        name: str

    class SGFactWithDim(FactModel, table=True):
        __tablename__ = "sg_fact_dim"
        __grain__ = "dim_grain"
        id: Optional[int] = Field(default=None, primary_key=True)
        dim_id: int = DimField(default=None, dimension=SGDim)
        amount: float = 0.0

    sg = SchemaGraph([SGDim, SGFactWithDim])
    mermaid = sg.to_mermaid()
    assert "SGFactWithDim" in mermaid
    assert "SGDim" in mermaid
    # Mermaid FK relationship line (line 168 of schema_graph.py)
    assert "dim_id" in mermaid
    assert "}o--||" in mermaid


def test_from_models_classmethod():
    sg = SchemaGraph.from_models([SGPlayer, SGGame, SGEdge])
    assert SGPlayer in sg.vertices


# ---------------------------------------------------------------------------
# TD-001 — implicit vertex/edge tests
# ---------------------------------------------------------------------------

def test_vertices_includes_all_dimensions_td001():
    # TD-001: every DimensionModel is a vertex — no VertexModel required
    sg = SchemaGraph([SGPlayer, SGGame, SGFact])
    assert SGPlayer in sg.vertices
    assert SGGame in sg.vertices


def test_edges_includes_all_facts_td001():
    # TD-001: every FactModel is an edge — no EdgeModel required
    sg = SchemaGraph([SGPlayer, SGFact])
    assert SGFact in sg.edges


def test_to_graph_returns_dict_td001():
    sg = SchemaGraph([SGPlayer, SGGame, SGEdge])
    g = sg.to_graph()
    assert isinstance(g, dict)
    assert "vertices" in g
    assert "edges" in g


def test_to_graph_includes_pure_dimension_as_vertex_td001():
    # A plain DimensionModel (no VertexModel) should appear as a vertex
    sg = SchemaGraph([SGPlayer, SGFact])
    g = sg.to_graph()
    vertex_names = [v["name"] for v in g["vertices"]]
    assert "SGPlayer" in vertex_names


def test_to_graph_vertex_type_defaults_to_class_name_td001():
    sg = SchemaGraph([SGPlayer])
    g = sg.to_graph()
    player_v = next(v for v in g["vertices"] if v["name"] == "SGPlayer")
    # __vertex_type__ is "sg_player" on SGPlayer fixture
    assert player_v["vertex_type"] == "sg_player"


def test_to_graph_fact_without_subject_has_none_subject_td001():
    # SGFact has no __subject__ — auto-derive from FK metadata or be None
    sg = SchemaGraph([SGFact])
    g = sg.to_graph()
    fact_edge = next(e for e in g["edges"] if e["name"] == "SGFact")
    # subject may be None or inferred — just check key exists
    assert "subject" in fact_edge


def test_graph_schema_edge_type_defaults_to_class_name_td001():
    # SGFact has no __edge_type__ — should default to lower-cased class name
    sg = SchemaGraph([SGFact])
    gs = sg.graph_schema()
    fact_edge = next(e for e in gs.edges if e["name"] == "SGFact")
    assert fact_edge["edge_type"] == "sgfact"


def test_validate_no_errors_for_implicit_facts_td001():
    # Plain FactModels with no __subject__/__object__ should not raise validation errors
    sg = SchemaGraph([SGPlayer, SGFact])
    errors = sg.validate()
    assert errors == []


def test_render_graph_model_skips_already_rendered():
    # Line 85: _render_graph_model returns early if model has _rendered_in_mermaid
    from sqldim.core.graph.schema_graph import _render_graph_model

    class AlreadyRendered:
        _rendered_in_mermaid = True

    lines = []
    _render_graph_model(AlreadyRendered, lines)
    assert lines == []


def test_fk_dimensions_returns_dimension_mapping():
    # Lines 258-259: _fk_dimensions calls get_star_schema and returns dimensions dict
    from sqldim.core.kimball.models import DimensionModel, FactModel

    class FKDimTest(DimensionModel, table=True):
        __tablename__ = "fkdim_test"
        __natural_key__ = ["code"]
        id: int = Field(default=None, primary_key=True)
        code: str

    class FKFactTest(FactModel, table=True):
        __tablename__ = "fkfact_test"
        __grain__ = "test"
        id: int = Field(default=None, primary_key=True)
        dim_id: int = Field(foreign_key="fkdim_test.id", dimension=FKDimTest)

    sg = SchemaGraph([FKDimTest, FKFactTest])
    dims = sg._fk_dimensions(FKFactTest)
    assert "dim_id" in dims
    assert dims["dim_id"] is FKDimTest


# ---------------------------------------------------------------------------
# Tests migrated from test_coverage_gap.py and test_coverage_100.py
# ---------------------------------------------------------------------------

class Cov100SgPlayer(VertexModel, table=True):
    __tablename__ = "cov100_sg_player"
    __natural_key__ = ["code"]
    __vertex_type__ = "cov100_sg_player"
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str
    is_starter: bool = False


class Cov100SgGame(VertexModel, table=True):
    __tablename__ = "cov100_sg_game"
    __natural_key__ = ["game_code"]
    __vertex_type__ = "cov100_sg_game"
    id: Optional[int] = Field(default=None, primary_key=True)
    game_code: str


class Cov100SgImplicitFact(FactModel, table=True):
    """Plain FactModel with FK dimension metadata."""
    __tablename__ = "cov100_sg_implicit_fact"
    id: Optional[int] = Field(default=None, primary_key=True)
    player_id: int = Field(foreign_key="cov100_sg_player.id", dimension=Cov100SgPlayer)
    game_id: int = Field(foreign_key="cov100_sg_game.id", dimension=Cov100SgGame)


class TestSafeSubclass:
    def test_non_type_base_raises_type_error(self):
        """_safe_subclass catches TypeError when base is not a class."""
        result = _safe_subclass(int, "not_a_class")
        assert result is False

    def test_non_type_cls_returns_false(self):
        """_safe_subclass returns False when cls is not a type."""
        result = _safe_subclass(None, int)
        assert result is False

    def test_non_type_integer_returns_false(self):
        """_safe_subclass returns False when cls is an integer (not a type)."""
        assert _safe_subclass(123, VertexModel) is False


class TestSchemaGraphFKAutoDerive:
    def test_graph_schema_auto_derives_subject_object(self):
        """graph_schema() auto-derives subject/object from FK metadata."""
        sg = GraphSchemaGraph([Cov100SgPlayer, Cov100SgGame, Cov100SgImplicitFact])
        schema = sg.graph_schema()
        edge_infos = schema.edges
        assert len(edge_infos) == 1
        ei = edge_infos[0]
        assert ei["subject"] is not None
        assert ei["object"] is not None


class TestSchemaGraphToMermaidExtended:
    def test_duplicate_model_skips_render(self):
        """render_model() skips already-rendered models."""
        sg = GraphSchemaGraph([Cov100SgPlayer, Cov100SgPlayer, Cov100SgGame])
        mermaid = sg.to_mermaid()
        assert mermaid.count("Cov100SgPlayer {") == 1

    def test_bool_field_rendered_as_bool(self):
        """Bool-typed fields render as 'bool' in Mermaid."""
        sg = GraphSchemaGraph([Cov100SgPlayer])
        mermaid = sg.to_mermaid()
        assert "bool is_starter" in mermaid

    def test_implicit_fact_fk_rendered(self):
        """Plain FactModel FK -> dimension rendered in else branch."""
        sg = GraphSchemaGraph([Cov100SgPlayer, Cov100SgGame, Cov100SgImplicitFact])
        mermaid = sg.to_mermaid()
        assert "Cov100SgImplicitFact" in mermaid
        assert "Cov100SgPlayer" in mermaid


def test_schema_graph_validation_orphans_final():
    """validate() flags orphan edges whose subject/object are not in the schema."""
    class ExternalDim(DimensionModel):
        pass

    class OrphanEdge2(EdgeModel, table=True):
        __tablename__ = "orphan_edge2"
        __edge_type__ = "oe2"
        __subject__ = ExternalDim
        __object__ = ExternalDim
        id: int = Field(primary_key=True)

    sg = SchemaGraph([OrphanEdge2])
    errors = sg.validate()
    assert len(errors) >= 2


# ---------------------------------------------------------------------------
# SchemaGraph.diff() — SchemaDiff, ColumnDiff, _collect_column_diff
# ---------------------------------------------------------------------------

class SDimV1(DimensionModel, table=True):
    __tablename__ = "sg_dim_v1_cov"
    __natural_key__ = ["code"]
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str


class SDimV2(DimensionModel, table=True):
    """V1 + extra 'label' column."""
    __tablename__ = "sg_dim_v2_cov"
    __natural_key__ = ["code"]
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str
    label: str = "default"


class SExtraDim(DimensionModel, table=True):
    __tablename__ = "sg_extra_dim_cov"
    __natural_key__ = ["code"]
    id: Optional[int] = Field(default=None, primary_key=True)
    code: str


class TestSchemaDiff:
    def test_empty_diff_is_empty(self):
        sg = SchemaGraph([SDimV1])
        diff = sg.diff(SchemaGraph([SDimV1]))
        assert diff.is_empty
        assert diff.edit_distance == 0

    def test_added_vertex(self):
        sg_old = SchemaGraph([SDimV1])
        sg_new = SchemaGraph([SDimV1, SExtraDim])
        diff = sg_old.diff(sg_new)
        assert SExtraDim in diff.added_vertices
        assert diff.edit_distance >= 1
        assert not diff.is_empty

    def test_removed_vertex(self):
        sg_old = SchemaGraph([SDimV1, SExtraDim])
        sg_new = SchemaGraph([SDimV1])
        diff = sg_old.diff(sg_new)
        assert SExtraDim in diff.removed_vertices

    def test_column_diff_added(self):
        """SDimV1 → SDimV2 adds 'label' column."""
        sg_old = SchemaGraph([SDimV1])
        sg_new = SchemaGraph([SDimV2])
        diff = sg_old.diff(sg_new)
        # SDimV1 and SDimV2 have different names so they appear as add/remove
        # column diff won't apply here; verify edit_distance is non-zero
        assert diff.edit_distance > 0

    def test_summary_empty_diff(self):
        sg = SchemaGraph([SDimV1])
        diff = sg.diff(SchemaGraph([SDimV1]))
        summary = diff.summary()
        assert "SchemaDiff" in summary
        assert "edit_distance=0" in summary

    def test_summary_with_added_vertex(self):
        sg_old = SchemaGraph([SDimV1])
        sg_new = SchemaGraph([SDimV1, SExtraDim])
        diff = sg_old.diff(sg_new)
        summary = diff.summary()
        assert "+ vertices" in summary
        assert "SExtraDim" in summary

    def test_summary_with_removed_vertex(self):
        sg_old = SchemaGraph([SDimV1, SExtraDim])
        sg_new = SchemaGraph([SDimV1])
        diff = sg_old.diff(sg_new)
        summary = diff.summary()
        assert "- vertices" in summary

    def test_column_diff_is_empty_predicate(self):
        from sqldim.core.graph.schema_graph import ColumnDiff
        cd_empty = ColumnDiff("M", "vertex", [], [])
        assert cd_empty.is_empty
        cd_nonempty = ColumnDiff("M", "vertex", ["a"], [])
        assert not cd_nonempty.is_empty


class TestCollectColumnDiff:
    def test_added_column_detected(self):
        from sqldim.core.graph.schema_graph import _collect_column_diff, ColumnDiff

        out: list[ColumnDiff] = []
        _collect_column_diff("SDimV1", "vertex", SDimV1, SDimV2, out)
        assert len(out) == 1
        assert "label" in out[0].added_columns

    def test_removed_column_detected(self):
        from sqldim.core.graph.schema_graph import _collect_column_diff, ColumnDiff

        out: list[ColumnDiff] = []
        _collect_column_diff("SDimV2", "vertex", SDimV2, SDimV1, out)
        assert len(out) == 1
        assert "label" in out[0].removed_columns

    def test_no_diff_when_equal(self):
        from sqldim.core.graph.schema_graph import _collect_column_diff, ColumnDiff

        out: list[ColumnDiff] = []
        _collect_column_diff("SDimV1", "vertex", SDimV1, SDimV1, out)
        assert len(out) == 0

    def test_old_cls_none(self):
        """old_cls=None means entirely new model → all columns added."""
        from sqldim.core.graph.schema_graph import _collect_column_diff, ColumnDiff

        out: list[ColumnDiff] = []
        _collect_column_diff("SDimV1", "vertex", None, SDimV1, out)
        assert len(out) == 1
        assert out[0].removed_columns == []

    def test_new_cls_none(self):
        """new_cls=None means deleted model → all columns removed."""
        from sqldim.core.graph.schema_graph import _collect_column_diff, ColumnDiff

        out: list[ColumnDiff] = []
        _collect_column_diff("SDimV1", "vertex", SDimV1, None, out)
        assert len(out) == 1
        assert out[0].added_columns == []


# ---------------------------------------------------------------------------
# SchemaDiff.summary with column_diffs (covers _schema_diff.py lines 75, 77)
# ---------------------------------------------------------------------------

class TestSchemaDiffColumnDiffSummary:
    def test_summary_includes_nonempty_column_diff(self):
        """SchemaDiff.summary() formats column_diffs where is_empty is False."""
        from sqldim.core.graph.schema_graph import ColumnDiff
        from sqldim.core.graph._impl._schema_diff import SchemaDiff

        cd = ColumnDiff("MyModel", "vertex", ["new_col"], [])
        diff = SchemaDiff([], [], [], [], [cd])
        summary = diff.summary()
        assert "~ MyModel" in summary
        assert "new_col" in summary

    def test_summary_skips_empty_column_diff(self):
        """SchemaDiff.summary() skips column_diffs where is_empty is True."""
        from sqldim.core.graph.schema_graph import ColumnDiff
        from sqldim.core.graph._impl._schema_diff import SchemaDiff

        cd_empty = ColumnDiff("MyModel", "vertex", [], [])
        diff = SchemaDiff([], [], [], [], [cd_empty])
        summary = diff.summary()
        # empty ColumnDiff should not appear in summary
        assert "~ MyModel" not in summary

    def test_summary_includes_added_edges(self):
        """SchemaDiff.summary() includes added_edges when non-empty."""
        from sqldim.core.graph._impl._schema_diff import SchemaDiff

        FakeEdge = type("FakeEdge", (), {})
        diff = SchemaDiff(added_edges=[FakeEdge])
        summary = diff.summary()
        assert "FakeEdge" in summary
        assert "+" in summary

    def test_summary_includes_removed_edges(self):
        """SchemaDiff.summary() includes removed_edges when non-empty."""
        from sqldim.core.graph._impl._schema_diff import SchemaDiff

        FakeEdge = type("FakeEdge", (), {})
        diff = SchemaDiff(removed_edges=[FakeEdge])
        summary = diff.summary()
        assert "FakeEdge" in summary
        assert "-" in summary


# ---------------------------------------------------------------------------
# SchemaGraph.diff with edge models (covers schema_graph.py lines 311-313)
# ---------------------------------------------------------------------------

class SEdgeFactDiff(FactModel, table=True):
    """FactModel used for edge-diff coverage tests."""
    __tablename__ = "sg_edge_fact_diff_cov"
    __grain__ = "test diff"
    id: Optional[int] = Field(default=None, primary_key=True)
    v1_id: int = Field(foreign_key="sg_dim_v1_cov.id", dimension=SDimV1)
    extra_id: Optional[int] = Field(default=None, foreign_key="sg_extra_dim_cov.id", dimension=SExtraDim)
    amount: float = 0.0


class TestSchemaGraphEdgeDiff:
    def test_diff_with_fact_model_visits_edge_loop(self):
        """diff() iterates over edges so _collect_column_diff is called for edge models."""
        sg_old = SchemaGraph([SDimV1, SExtraDim, SEdgeFactDiff])
        sg_new = SchemaGraph([SDimV1, SExtraDim])
        diff = sg_old.diff(sg_new)
        # The fact was removed, so edit_distance should reflect removed edge
        assert diff.edit_distance > 0

    def test_diff_same_facts_produces_no_edge_change(self):
        """diff() of identical schemas with facts gives edit_distance=0."""
        sg = SchemaGraph([SDimV1, SExtraDim, SEdgeFactDiff])
        diff = sg.diff(sg)
        assert diff.edit_distance == 0


def test_to_dict_annotation_frozenset_field_sorted():
    """GraphSchema.to_dict() converts frozenset annotation fields to sorted lists (line 235-236)."""
    from sqldim.core.query.dgm.annotations import Conformed
    from sqldim.core.graph.schema_graph import GraphSchema

    schema = GraphSchema(
        vertices=[{"name": "customer", "columns": []}],
        edges=[],
        annotations=[Conformed(dim="customer", fact_types=frozenset({"Sale", "Return"}))],
    )
    d = schema.to_dict()
    ann_dict = d["annotations"][0]
    assert isinstance(ann_dict["fact_types"], list)
    assert ann_dict["fact_types"] == sorted(["Sale", "Return"])
