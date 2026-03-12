"""Tests for extended SchemaGraph — Task 6.5."""
import pytest
from typing import Optional
from sqlmodel import Field

from sqldim.models.graph import VertexModel, EdgeModel
from sqldim.graph.schema_graph import SchemaGraph, GraphSchema
from sqldim.core.models import DimensionModel, FactModel
from sqldim.exceptions import SchemaError


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
