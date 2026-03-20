from sqldim import DimensionModel, FactModel, Field
from sqldim.core.graph import SchemaGraph, RolePlayingRef

class DateDimX(DimensionModel, table=True):
    __natural_key__ = ["date_val"]
    __scd_type__ = 1
    id: int = Field(primary_key=True, surrogate_key=True)
    date_val: str
    year: int

class FlightFact(FactModel, table=True):
    __grain__ = "one row per flight"
    id: int = Field(primary_key=True)
    departure_date_id: int = Field(foreign_key="datedimx.id", dimension=DateDimX, role="departure_date")
    arrival_date_id: int = Field(foreign_key="datedimx.id", dimension=DateDimX, role="arrival_date")
    revenue: float = Field(measure=True, additive=True)

def test_role_playing_dimensions():
    graph = SchemaGraph.from_models([DateDimX, FlightFact])
    roles = graph.get_role_playing_dimensions(FlightFact)
    assert len(roles) == 2
    role_names = {r.role for r in roles}
    assert "departure_date" in role_names
    assert "arrival_date" in role_names

def test_role_playing_ref_repr():
    r = RolePlayingRef(dimension=DateDimX, role="booking_date", fk_column="booking_date_id")
    assert "booking_date" in repr(r)
    assert "DateDimX" in repr(r)

def test_role_playing_no_table():
    # Model without __table__ (non-table SQLModel)
    class NoTable(FactModel):
        id: int = Field(primary_key=True)

    graph = SchemaGraph.from_models([NoTable])
    refs = graph.get_role_playing_dimensions(NoTable)
    assert refs == []

def test_to_dict():
    graph = SchemaGraph.from_models([DateDimX, FlightFact])
    d = graph.to_dict()
    assert "facts" in d
    assert "dimensions" in d
    dim_names = [x["name"] for x in d["dimensions"]]
    assert "DateDimX" in dim_names
    fact_names = [x["name"] for x in d["facts"]]
    assert "FlightFact" in fact_names
    flight = next(f for f in d["facts"] if f["name"] == "FlightFact")
    assert flight["grain"] == "one row per flight"
    assert len(flight["role_playing"]) == 2

def test_to_mermaid():
    graph = SchemaGraph.from_models([DateDimX, FlightFact])
    diagram = graph.to_mermaid()
    assert "erDiagram" in diagram
    assert "DateDimX" in diagram
    assert "FlightFact" in diagram
    assert "departure_date_id" in diagram or "arrival_date_id" in diagram

def test_to_mermaid_float_and_bool_types():
    # Covers float and bool annotation branches in to_mermaid()
    from sqldim import SCD2Mixin
    class MetricsDim(DimensionModel, SCD2Mixin, table=True):
        __natural_key__ = ["code"]
        id: int = Field(primary_key=True)
        code: str
        score: float
        active: bool

    class MetricsFact(FactModel, table=True):
        id: int = Field(primary_key=True)
        value: float

    graph = SchemaGraph.from_models([MetricsDim, MetricsFact])
    diagram = graph.to_mermaid()
    assert "float" in diagram
    assert "bool" in diagram
