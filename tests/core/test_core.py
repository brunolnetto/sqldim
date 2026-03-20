import pytest
from typing import List, Optional
from datetime import date
from sqlmodel import SQLModel
from sqldim import DimensionModel, FactModel, Field, SchemaGraph, SCD2Mixin, SCD3Mixin, CumulativeMixin, DatelistMixin

class MockDimension(DimensionModel, table=True):
    id: int = Field(primary_key=True, surrogate_key=True)
    code: str = Field(natural_key=True)

class MockFact(FactModel, table=True):
    id: int = Field(primary_key=True)
    dim_id: int = Field(foreign_key="mockdimension.id", dimension=MockDimension)
    amount: float = Field(measure=True, additive=True)

def test_schema_graph_detection():
    graph = SchemaGraph.from_models([MockDimension, MockFact])
    assert MockDimension in graph.dimensions
    assert MockFact in graph.facts

def test_star_schema_introspection():
    graph = SchemaGraph.from_models([MockDimension, MockFact])
    star = graph.get_star_schema(MockFact)
    
    assert star["fact"] == MockFact
    assert "dim_id" in star["dimensions"]
    assert star["dimensions"]["dim_id"] == MockDimension

def test_field_metadata():
    # Verify metadata is correctly stored in SQLAlchemy column info
    column = MockDimension.__table__.columns["id"]
    assert column.info["surrogate_key"] is True
    
    natural_key_col = MockDimension.__table__.columns["code"]
    assert natural_key_col.info["natural_key"] is True

def test_json_schema_extra_fallback():
    # Test the fallback branch for json_schema_extra in graph.py
    class ExtraDimension(DimensionModel):
        pass
    
    class ExtraFact(FactModel):
        dim_id: int = Field(dimension=ExtraDimension)

    # Manually inject into model_fields to simulate modern Pydantic behavior
    # which we can't easily trigger in this older environment version
    ExtraFact.model_fields["dim_id"].json_schema_extra = {"dimension": ExtraDimension}
    
    graph = SchemaGraph.from_models([ExtraDimension, ExtraFact])
    star = graph.get_star_schema(ExtraFact)
    assert star["dimensions"]["dim_id"] == ExtraDimension


def test_col_dimension_no_table():
    # Line 14: _col_dimension returns None when model has no __table__
    from sqldim.core.graph import _col_dimension
    class NoTable:
        pass
    assert _col_dimension(NoTable, "some_col") is None


def test_role_ref_from_col_no_info():
    # Line 50: _role_ref_from_col returns None when col.info is falsy
    from sqldim.core.graph import _role_ref_from_col
    class FakeCol:
        info = {}
        name = "col"
    assert _role_ref_from_col(FakeCol()) is None


# ---------------------------------------------------------------------------
# DatelistMixin — bitmask helpers
# ---------------------------------------------------------------------------

class _CovActivityUser(DatelistMixin, SQLModel):
    dates_active: List[date] = []


def test_datelist_mixin_bitmask():
    user = _CovActivityUser(dates_active=[date(2024, 1, 1), date(2024, 1, 5)])
    ref = date(2024, 1, 7)
    assert bin(user.to_bitmask(ref, window=10)).count("1") == 2
    assert user.l7(ref) == 2
    assert user.l28(ref) == 2
    assert user.activity_in_window(7, ref) is True
    empty = _CovActivityUser(dates_active=[])
    assert empty.to_bitmask(ref) == 0


class TestDatelistMixinStringDates:
    def test_string_dates_converted(self):
        """DatelistMixin.to_bitmask converts ISO string dates."""
        class DL(DatelistMixin, SQLModel):
            dates_active: Optional[list] = None

        obj = DL()
        obj.dates_active = ["2024-01-01", "2024-01-03"]
        mask = obj.to_bitmask(date(2024, 1, 5), window=10)
        assert mask > 0


# ---------------------------------------------------------------------------
# SCD3Mixin corner cases
# ---------------------------------------------------------------------------

class TestSCD3MixinLines:
    def test_non_scd3_subclass_returns_early(self):
        """SCD3Mixin.__init_subclass__ returns early when __scd_type__ != 3."""
        class NonSCD3(SCD3Mixin, SQLModel):
            __scd_type__ = 2
            x: str = ""
        assert getattr(NonSCD3, "__scd_type__", None) == 2

    def test_orphan_prev_column_raises(self):
        """SCD3Mixin raises TypeError for prev_* without matching current column."""
        with pytest.raises(TypeError, match="prev_ghost"):
            class BadSCD3(SCD3Mixin, SQLModel):
                __scd_type__ = 3
                prev_ghost: Optional[str] = None


# ---------------------------------------------------------------------------
# CumulativeMixin corner cases
# ---------------------------------------------------------------------------

class TestCumulativeMixinLines:
    def test_current_value(self):
        class CumDim(CumulativeMixin, SQLModel):
            seasons: Optional[list] = None

        obj = CumDim()
        obj.seasons = [{"pts": 10}, {"pts": 20}]
        assert obj.current_value("seasons") == {"pts": 20}
        assert obj.current_value("nonexistent") is None

    def test_first_value(self):
        class CumDim2(CumulativeMixin, SQLModel):
            seasons: Optional[list] = None

        obj = CumDim2()
        obj.seasons = [{"pts": 5}, {"pts": 15}]
        assert obj.first_value("seasons") == {"pts": 5}
        assert obj.first_value("empty_col") is None


# ---------------------------------------------------------------------------
# SchemaGraph base — to_mermaid() coverage (float, bool, relationships)
# ---------------------------------------------------------------------------

def test_base_schema_graph_to_mermaid_all_types():
    """Covers _annotation_type (float/bool/string), _render_mermaid_model,
    and to_mermaid() relationship rendering in sqldim.core.kimball.schema_graph."""
    from sqldim.core.kimball.schema_graph import SchemaGraph as BaseSchemaGraph

    class MetricsDim(DimensionModel, table=True):
        __tablename__ = "metrics_dim_base"
        __natural_key__ = ["code"]
        id: int = Field(primary_key=True, surrogate_key=True)
        code: str
        score: float
        active: bool

    class MetricsFact(FactModel, table=True):
        __tablename__ = "metrics_fact_base"
        id: int = Field(primary_key=True)
        value: float

    g = BaseSchemaGraph.from_models([MetricsDim, MetricsFact])
    diagram = g.to_mermaid()
    assert "erDiagram" in diagram
    assert "MetricsDim" in diagram
    assert "MetricsFact" in diagram
    assert "float" in diagram
    assert "bool" in diagram


def test_base_schema_graph_to_mermaid_renders_fk_relationships():
    """to_mermaid() renders fact-to-dimension FK relationship lines (L168 in schema_graph.py)."""
    from sqldim.core.kimball.schema_graph import SchemaGraph as BaseSchemaGraph

    class FKMermaidDim(DimensionModel, table=True):
        __tablename__ = "fk_mermaid_dim"
        __natural_key__ = ["code"]
        id: int = Field(primary_key=True, surrogate_key=True)
        code: str

    class FKMermaidFact(FactModel, table=True):
        __tablename__ = "fk_mermaid_fact"
        id: int = Field(primary_key=True)
        dim_id: int = Field(dimension=FKMermaidDim)

    g = BaseSchemaGraph.from_models([FKMermaidDim, FKMermaidFact])
    diagram = g.to_mermaid()
    # The FK relationship line: "FactName }o--|| DimName : fk_col"
    assert "}o--||" in diagram
    assert "FKMermaidDim" in diagram
    assert "FKMermaidFact" in diagram


# ---------------------------------------------------------------------------
# DimensionModel.__init_subclass__ guards (models.py lines 38-42, 54)
# ---------------------------------------------------------------------------

class TestDimensionModelGuards:
    def test_duplicate_mixin_role_raises(self):
        """Lines 38-42: two mixins with the same __dim_mixin_role__ must raise TypeError."""
        from sqlmodel import SQLModel as _SQLModel

        class SecondValidityMixin(_SQLModel):
            __dim_mixin_role__ = "scd_validity_columns"

        with pytest.raises(TypeError, match="multiple mixins with role"):
            class BadDim(DimensionModel, SCD2Mixin, SecondValidityMixin):
                pass

    def test_incompatible_scd_type_raises(self):
        """Line 54: __scd_type__ not in the mixin's compatible set must raise TypeError."""
        with pytest.raises(TypeError, match="compatible with SCD types"):
            class BadTypeDim(DimensionModel, SCD2Mixin):
                __scd_type__ = 4  # SCD2Mixin compatible_types = {1, 2, 3, 6}
                code: str = ""


# ---------------------------------------------------------------------------
# DimensionModel.table_name() and FactModel.table_name() (lines 78, 179)
# ---------------------------------------------------------------------------

class TestTableNameClassmethod:
    def test_dimension_model_table_name_uses_tablename(self):
        """DimensionModel.table_name() returns __tablename__ when set (line 78)."""
        class NamedDim(DimensionModel):
            __tablename__ = "named_dim_table"

        assert NamedDim.table_name() == "named_dim_table"

    def test_dimension_model_table_name_falls_back_to_class(self):
        """DimensionModel.table_name() falls back to lowercased class name."""
        class UnnamedDim(DimensionModel):
            pass

        assert UnnamedDim.table_name() == "unnameddim"

    def test_fact_model_table_name_uses_tablename(self):
        """FactModel.table_name() returns __tablename__ when set (line 179)."""
        class NamedFact(FactModel):
            __tablename__ = "named_fact_table"

        assert NamedFact.table_name() == "named_fact_table"

    def test_fact_model_table_name_falls_back_to_class(self):
        """FactModel.table_name() falls back to lowercased class name."""
        class UnnamedFact(FactModel):
            pass

        assert UnnamedFact.table_name() == "unnamedfact"
