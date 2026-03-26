"""Tests for VertexModel base class — Task 6.1."""

import pytest
from typing import Optional
from sqlmodel import Field

from sqldim.core.graph import VertexModel, Vertex
from sqldim.core.kimball.models import DimensionModel
from sqldim.exceptions import SchemaError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class SimplePlayer(VertexModel, table=True):
    __tablename__ = "simple_player"
    __vertex_type__ = "player"
    __natural_key__ = ["name"]
    __scd_type__ = 1

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    score: float = 0.0


class LimitedPropsPlayer(VertexModel, table=True):
    __tablename__ = "limited_props_player"
    __vertex_type__ = "lpp"
    __vertex_properties__ = ["name"]

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    secret: str = "hidden"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_vertex_model_is_also_dimension_model():
    assert issubclass(SimplePlayer, DimensionModel)


def test_vertex_model_is_vertex_model():
    assert issubclass(SimplePlayer, VertexModel)


def test_vertex_type_classmethod():
    assert SimplePlayer.vertex_type() == "player"


def test_as_vertex_returns_vertex_dataclass():
    instance = SimplePlayer(id=1, name="Jordan", score=30.1)
    v = SimplePlayer.as_vertex(instance)
    assert isinstance(v, Vertex)
    assert v.id == 1
    assert v.type == "player"
    assert v.properties["name"] == "Jordan"
    assert v.properties["score"] == 30.1


def test_as_vertex_respects_vertex_properties():
    instance = LimitedPropsPlayer(id=2, name="Bird", secret="classified")
    v = LimitedPropsPlayer.as_vertex(instance)
    assert "name" in v.properties
    assert "secret" not in v.properties


def test_vertex_dataclass_repr():
    v = Vertex(id=5, type="player")
    assert "5" in repr(v)
    assert "player" in repr(v)


def test_missing_vertex_type_is_allowed_td001():
    # TD-001: __vertex_type__ is now optional — no SchemaError on declaration
    class ImplicitVertex(VertexModel, table=True):
        __tablename__ = "implicit_vertex"
        id: Optional[int] = Field(default=None, primary_key=True)
        name: str

    # as_vertex() falls back to class name lower-cased
    instance = ImplicitVertex(id=3, name="Bird")
    v = ImplicitVertex.as_vertex(instance)
    assert v.type == "implicitvertex"
    assert v.id == 3


def test_vertex_type_classmethod_raises_for_missing():
    # VertexModel base itself has no __vertex_type__ — calling vertex_type() raises
    with pytest.raises(SchemaError):
        VertexModel.vertex_type()


def test_natural_key_inherited():
    assert SimplePlayer.__natural_key__ == ["name"]


def test_scd_type_inherited():
    assert SimplePlayer.__scd_type__ == 1
