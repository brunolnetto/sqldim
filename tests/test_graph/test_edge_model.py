"""Tests for EdgeModel base class — Task 6.2."""
import pytest
from typing import Optional
from sqlmodel import Field

from sqldim.models.graph import VertexModel, EdgeModel
from sqldim.core.models import FactModel
from sqldim.exceptions import SchemaError


# ---------------------------------------------------------------------------
# Vertex fixtures
# ---------------------------------------------------------------------------

class VPlayer(VertexModel, table=True):
    __tablename__ = "v_player"
    __vertex_type__ = "v_player"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str


class VGame(VertexModel, table=True):
    __tablename__ = "v_game"
    __vertex_type__ = "v_game"

    id: Optional[int] = Field(default=None, primary_key=True)
    game_id: int


# ---------------------------------------------------------------------------
# Edge fixtures
# ---------------------------------------------------------------------------

class PlaysInE(EdgeModel, table=True):
    __tablename__ = "plays_in_e"
    __edge_type__ = "plays_in"
    __subject__ = VPlayer
    __object__ = VGame
    __grain__ = "player_game"

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="v_player.id")
    object_id: int = Field(foreign_key="v_game.id")
    pts: float = 0.0


class SelfRefEdge(EdgeModel, table=True):
    __tablename__ = "self_ref_edge"
    __edge_type__ = "plays_against"
    __subject__ = VPlayer
    __object__ = VPlayer
    __directed__ = False

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = Field(foreign_key="v_player.id")
    object_id: int = Field(foreign_key="v_player.id")
    num_games: int = 0


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_edge_model_is_also_fact_model():
    assert issubclass(PlaysInE, FactModel)


def test_edge_type_classmethod():
    assert PlaysInE.edge_type() == "plays_in"


def test_directed_default_true():
    assert PlaysInE.__directed__ is True


def test_undirected_edge():
    assert SelfRefEdge.__directed__ is False


def test_self_referential_detection():
    assert SelfRefEdge.is_self_referential() is True
    assert PlaysInE.is_self_referential() is False


def test_missing_edge_type_allowed_td001():
    # TD-001: __edge_type__ is now optional — declaration succeeds without it
    class ImplicitEdge(EdgeModel, table=True):
        __tablename__ = "implicit_edge_td001"
        __subject__ = VPlayer
        __object__ = VGame
        id: Optional[int] = Field(default=None, primary_key=True)

    # edge_type() still raises since it is not set
    with pytest.raises(SchemaError):
        ImplicitEdge.edge_type()


def test_missing_subject_allowed_td001():
    # TD-001: __subject__ is optional on EdgeModel declaration
    class ImplicitSubject(EdgeModel, table=True):
        __tablename__ = "implicit_subject_td001"
        __edge_type__ = "some_edge"
        __object__ = VGame
        id: Optional[int] = Field(default=None, primary_key=True)

    assert getattr(ImplicitSubject, "__subject__", None) is None


def test_missing_object_allowed_td001():
    # TD-001: __object__ is optional on EdgeModel declaration
    class ImplicitObject(EdgeModel, table=True):
        __tablename__ = "implicit_object_td001"
        __edge_type__ = "another_edge"
        __subject__ = VPlayer
        id: Optional[int] = Field(default=None, primary_key=True)

    assert getattr(ImplicitObject, "__object__", None) is None


def test_edge_type_raises_when_not_set():
    # EdgeModel base class itself has no __edge_type__ set — calling edge_type() raises
    with pytest.raises(SchemaError):
        EdgeModel.edge_type()


def test_grain_inherited():
    assert PlaysInE.__grain__ == "player_game"
