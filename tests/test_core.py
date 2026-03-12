import pytest
from sqldim import DimensionModel, FactModel, Field, SchemaGraph

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
