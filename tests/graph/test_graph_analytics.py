"""
Tests for ADR: Graph Analytics Roadmap — Metadata-Driven Property Graph.

Red-green TDD: tests define intended behavior, then implementation makes them pass.

Tier 1: Metadata-Aware Lookup
  - Natural key lookup         → GraphModel.get_vertex_by_key()
  - Additive guard             → neighbor_aggregation(validate_additive=True)
  - Grain compatibility        → GraphModel.validate_grain_join()

Tier 2: Temporal Graph Traversal
  - Point-in-time neighbors    → neighbors(as_of=<datetime>)
  - SCD type dispatch          → Type 1 = no filter, Type 2 = temporal filter
  - Temporal SQL generation    → TraversalEngine.neighbors_sql_at()

Tier 3: Role-Playing & Bridge-Aware Traversal
  - Role-aware edge discovery  → GraphModel.discover_role_edges(schema_graph)
  - Weighted bridge traversal  → aggregate_sql(weighted=True)
  - Strategy-guided freshness  → GraphModel.freshness(edge_type)
"""
from __future__ import annotations

import pytest
from datetime import date
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

from sqldim import Field as DimField
from sqlmodel import Field
from sqldim.core.graph import VertexModel, EdgeModel
from sqldim.core.graph.registry import GraphModel
from sqldim.core.graph.traversal import TraversalEngine, DuckDBTraversalEngine
from sqldim.core.kimball.models import DimensionModel
from sqldim.core.kimball.schema_graph import SchemaGraph
from sqldim.exceptions import SchemaError, SemanticError, GrainCompatibilityError


# ===========================================================================
# Model definitions — unique table names to avoid conflicts
# ===========================================================================

class GAPersonDim(VertexModel, table=True):
    """SCD Type 1 person dimension — used for natural key lookup tests."""
    __tablename__ = "ga_person"
    __vertex_type__ = "ga_person"
    __natural_key__ = ["email"]
    __scd_type__ = 1

    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = ""
    name: str = ""


class GAProductDim(VertexModel, table=True):
    """SCD Type 1 product — used for additive guard tests."""
    __tablename__ = "ga_product"
    __vertex_type__ = "ga_product"
    __natural_key__ = ["sku"]
    __scd_type__ = 1

    id: Optional[int] = Field(default=None, primary_key=True)
    sku: str = ""
    name: str = ""


class GACustomerDim(VertexModel, table=True):
    """SCD Type 2 customer — used for temporal traversal tests."""
    __tablename__ = "ga_customer_scd2"
    __vertex_type__ = "ga_customer"
    __natural_key__ = ["customer_code"]
    __scd_type__ = 2

    id: Optional[int] = Field(default=None, primary_key=True)
    customer_code: str = ""
    name: str = ""
    is_current: bool = True
    effective_from: Optional[str] = None
    effective_to: Optional[str] = None


class GAPurchaseEdge(EdgeModel, table=True):
    """Edge with additive + non-additive measures."""
    __tablename__ = "ga_purchase"
    __edge_type__ = "ga_purchased"
    __subject__ = GAPersonDim
    __object__ = GAProductDim
    __directed__ = True

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = DimField(default=0, foreign_key="ga_person.id")
    object_id: int = DimField(default=0, foreign_key="ga_product.id")
    amount: float = DimField(default=0.0, measure=True, additive=True)
    discount: float = DimField(default=0.0, measure=True, additive=False)


class GATemporalEdge(EdgeModel, table=True):
    """Edge connecting Person → Customer (SCD2) for temporal tests."""
    __tablename__ = "ga_temporal_edge"
    __edge_type__ = "ga_temporal"
    __subject__ = GAPersonDim
    __object__ = GACustomerDim
    __directed__ = True

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = DimField(default=0, foreign_key="ga_person.id")
    object_id: int = DimField(default=0, foreign_key="ga_customer_scd2.id")
    value: float = DimField(default=0.0, measure=True, additive=True)


class GAEdgeGrainA(EdgeModel, table=True):
    """Edge with grain declaration A."""
    __tablename__ = "ga_edge_grain_a"
    __edge_type__ = "ga_grain_a"
    __grain__ = "one row per transaction"
    __subject__ = GAPersonDim
    __object__ = GAProductDim

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = DimField(default=0, foreign_key="ga_person.id")
    object_id: int = DimField(default=0, foreign_key="ga_product.id")


class GAEdgeGrainB(EdgeModel, table=True):
    """Edge with incompatible grain declaration B."""
    __tablename__ = "ga_edge_grain_b"
    __edge_type__ = "ga_grain_b"
    __grain__ = "one row per day"
    __subject__ = GAPersonDim
    __object__ = GAProductDim

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = DimField(default=0, foreign_key="ga_person.id")
    object_id: int = DimField(default=0, foreign_key="ga_product.id")


class GAEdgeNoGrain(EdgeModel, table=True):
    """Edge without grain declaration — skipped in grain validation."""
    __tablename__ = "ga_edge_no_grain"
    __edge_type__ = "ga_no_grain"
    __subject__ = GAPersonDim
    __object__ = GAProductDim

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = DimField(default=0, foreign_key="ga_person.id")
    object_id: int = DimField(default=0, foreign_key="ga_product.id")


class GAWeightedEdge(EdgeModel, table=True):
    """Edge with weight column for bridge-style weighted aggregation."""
    __tablename__ = "ga_weighted_edge"
    __edge_type__ = "ga_weighted"
    __subject__ = GAPersonDim
    __object__ = GAProductDim
    __directed__ = True

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = DimField(default=0, foreign_key="ga_person.id")
    object_id: int = DimField(default=0, foreign_key="ga_product.id")
    revenue: float = DimField(default=0.0, measure=True, additive=True)
    weight: float = Field(default=1.0)


class GABulkEdge(EdgeModel, table=True):
    """Edge with bulk load strategy — freshness returns MAX(updated_at)."""
    __tablename__ = "ga_bulk_edge"
    __edge_type__ = "ga_bulk"
    __strategy__ = "bulk"
    __subject__ = GAPersonDim
    __object__ = GAProductDim

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = DimField(default=0, foreign_key="ga_person.id")
    object_id: int = DimField(default=0, foreign_key="ga_product.id")
    updated_at: Optional[str] = None


class GAUpsertEdge(EdgeModel, table=True):
    """Edge with upsert strategy — freshness always returns None."""
    __tablename__ = "ga_upsert_edge"
    __edge_type__ = "ga_upsert"
    __strategy__ = "upsert"
    __subject__ = GAPersonDim
    __object__ = GAProductDim

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = DimField(default=0, foreign_key="ga_person.id")
    object_id: int = DimField(default=0, foreign_key="ga_product.id")


class GAUndirectedSCD2Edge(EdgeModel, table=True):
    """Undirected edge pointing to SCD2 Customer vertex — forces direction='both' in temporal SQL."""
    __tablename__ = "ga_undirected_scd2"
    __edge_type__ = "ga_undir_scd2"
    __subject__ = GACustomerDim
    __object__ = GACustomerDim
    __directed__ = False

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = DimField(default=0, foreign_key="ga_customer_scd2.id")
    object_id: int = DimField(default=0, foreign_key="ga_customer_scd2.id")


# Role-playing dimension models
class GADateDim(DimensionModel, table=True):
    __tablename__ = "ga_date_dim"
    __natural_key__ = ["date_key"]
    __scd_type__ = 1

    id: Optional[int] = Field(default=None, primary_key=True)
    date_key: str = ""
    year: int = 0


class GAShipmentEdge(EdgeModel, table=True):
    """Edge with two role-playing FK columns pointing to GADateDim."""
    __tablename__ = "ga_shipment_edge"
    __edge_type__ = "ga_shipment"
    __grain__ = "one row per shipment"
    __subject__ = GAPersonDim
    __object__ = GAProductDim

    id: Optional[int] = Field(default=None, primary_key=True)
    subject_id: int = DimField(default=0, foreign_key="ga_person.id")
    object_id: int = DimField(default=0, foreign_key="ga_product.id")
    ship_date_id: int = DimField(default=0, foreign_key="ga_date_dim.id", dimension=GADateDim, role="ship_date")
    order_date_id: int = DimField(default=0, foreign_key="ga_date_dim.id", dimension=GADateDim, role="order_date")
    weight_kg: float = DimField(default=0.0, measure=True, additive=True)


# ===========================================================================
# Helpers
# ===========================================================================

def _mock_session(first_rows=None, second_rows=None):
    """Build a minimal async session mock."""
    session = AsyncMock()
    if second_rows is None:
        # Single-call path (get_vertex, aggregation, degree, freshness)
        result = MagicMock()
        result.mappings.return_value.first.return_value = first_rows[0] if first_rows else None
        result.fetchone.return_value = first_rows[0] if first_rows else (None,)
        result.fetchall.return_value = first_rows or []
        result.mappings.return_value.fetchall.return_value = first_rows or []
        session.execute.return_value = result
    else:
        # Two-call path (neighbors: edge query + vertex hydration)
        r1 = MagicMock()
        r1.fetchall.return_value = [(r.get("id", 0),) for r in (first_rows or [])]
        r2 = MagicMock()
        r2.mappings.return_value.fetchall.return_value = second_rows or []
        session.execute.side_effect = [r1, r2]
    return session


# ===========================================================================
# Tier 1: Natural Key Lookup
# ===========================================================================

class TestNaturalKeyLookup:

    async def test_get_vertex_by_key_returns_instance(self):
        row = {"id": 42, "email": "alice@example.com", "name": "Alice"}
        session = _mock_session(first_rows=[row])
        g = GraphModel(GAPersonDim, session=session)

        result = await g.get_vertex_by_key(GAPersonDim, email="alice@example.com")

        assert result is not None
        assert result.id == 42
        assert result.email == "alice@example.com"

    async def test_get_vertex_by_key_not_found_returns_none(self):
        session = AsyncMock()
        result_mock = MagicMock()
        result_mock.mappings.return_value.first.return_value = None
        session.execute.return_value = result_mock

        g = GraphModel(GAPersonDim, session=session)
        result = await g.get_vertex_by_key(GAPersonDim, email="ghost@example.com")

        assert result is None

    async def test_get_vertex_by_key_model_without_natural_key_raises(self):
        class GANoKeyDim(VertexModel, table=True):
            __tablename__ = "ga_no_key_dim"
            __vertex_type__ = "ga_no_key"
            __natural_key__ = []

            id: Optional[int] = Field(default=None, primary_key=True)
            name: str = ""

        g = GraphModel(GANoKeyDim, session=AsyncMock())
        with pytest.raises(SchemaError, match="no __natural_key__"):
            await g.get_vertex_by_key(GANoKeyDim, name="X")

    async def test_get_vertex_by_key_unregistered_raises(self):
        g = GraphModel(GAProductDim, session=AsyncMock())
        with pytest.raises(SchemaError):
            await g.get_vertex_by_key(GAPersonDim, email="x@example.com")

    async def test_get_vertex_by_key_uses_natural_key_columns_in_sql(self):
        session = AsyncMock()
        result_mock = MagicMock()
        result_mock.mappings.return_value.first.return_value = None
        session.execute.return_value = result_mock

        g = GraphModel(GAPersonDim, session=session)
        await g.get_vertex_by_key(GAPersonDim, email="test@example.com")

        call_args = session.execute.call_args
        sql_text = str(call_args[0][0])
        assert "email" in sql_text
        assert "ga_person" in sql_text


# ===========================================================================
# Tier 1: Additive Guard on neighbor_aggregation
# ===========================================================================

class TestAdditiveGuard:

    async def test_additive_measure_sum_allowed(self):
        """SUM on additive=True column is always allowed."""
        session = AsyncMock()
        result = MagicMock()
        result.fetchone.return_value = (100.0,)
        session.execute.return_value = result

        g = GraphModel(GAPersonDim, GAProductDim, GAPurchaseEdge, session=session)
        person = GAPersonDim(id=1, email="x@x.com", name="X")
        total = await g.neighbor_aggregation(
            person, GAPurchaseEdge, measure="amount", agg="sum", validate_additive=True
        )
        assert total == 100.0

    async def test_non_additive_measure_sum_raises(self):
        """SUM on additive=False column raises SemanticError when validated."""
        g = GraphModel(GAPersonDim, GAProductDim, GAPurchaseEdge, session=AsyncMock())
        person = GAPersonDim(id=1, email="x@x.com", name="X")

        with pytest.raises(SemanticError, match="non-additive"):
            await g.neighbor_aggregation(
                person, GAPurchaseEdge, measure="discount", agg="sum", validate_additive=True
            )

    async def test_non_additive_measure_avg_raises(self):
        """AVG on additive=False column raises SemanticError when validated."""
        g = GraphModel(GAPersonDim, GAProductDim, GAPurchaseEdge, session=AsyncMock())
        person = GAPersonDim(id=1, email="x@x.com", name="X")

        with pytest.raises(SemanticError, match="non-additive"):
            await g.neighbor_aggregation(
                person, GAPurchaseEdge, measure="discount", agg="avg", validate_additive=True
            )

    async def test_non_additive_measure_count_allowed(self):
        """COUNT on non-additive column is always semantically valid."""
        session = AsyncMock()
        result = MagicMock()
        result.fetchone.return_value = (5.0,)
        session.execute.return_value = result

        g = GraphModel(GAPersonDim, GAProductDim, GAPurchaseEdge, session=session)
        person = GAPersonDim(id=1, email="x@x.com", name="X")
        count = await g.neighbor_aggregation(
            person, GAPurchaseEdge, measure="discount", agg="count", validate_additive=True
        )
        assert count == 5.0

    async def test_non_additive_measure_max_allowed(self):
        """MAX on non-additive column is always semantically valid."""
        session = AsyncMock()
        result = MagicMock()
        result.fetchone.return_value = (25.0,)
        session.execute.return_value = result

        g = GraphModel(GAPersonDim, GAProductDim, GAPurchaseEdge, session=session)
        person = GAPersonDim(id=1, email="x@x.com", name="X")
        val = await g.neighbor_aggregation(
            person, GAPurchaseEdge, measure="discount", agg="max", validate_additive=True
        )
        assert val == 25.0

    async def test_additive_guard_off_by_default(self):
        """Without validate_additive=True, non-additive SUM is silently allowed."""
        session = AsyncMock()
        result = MagicMock()
        result.fetchone.return_value = (50.0,)
        session.execute.return_value = result

        g = GraphModel(GAPersonDim, GAProductDim, GAPurchaseEdge, session=session)
        person = GAPersonDim(id=1, email="x@x.com", name="X")
        # No validate_additive=True → no error raised
        val = await g.neighbor_aggregation(
            person, GAPurchaseEdge, measure="discount", agg="sum"
        )
        assert val == 50.0


# ===========================================================================
# Tier 1: Grain Compatibility Validation
# ===========================================================================

class TestGrainValidation:

    def test_same_grains_passes(self):
        """Two edges with the same grain are compatible."""
        g = GraphModel(GAPersonDim, GAProductDim, session=AsyncMock())
        g.validate_grain_join(GAEdgeGrainA, GAEdgeGrainA)  # idempotent

    def test_different_grains_raises(self):
        """Two edges with different grains raise GrainCompatibilityError."""
        g = GraphModel(GAPersonDim, GAProductDim, session=AsyncMock())
        with pytest.raises(GrainCompatibilityError, match="[Ii]ncompatible"):
            g.validate_grain_join(GAEdgeGrainA, GAEdgeGrainB)

    def test_missing_grain_skips_validation(self):
        """An edge without __grain__ is ignored in grain checks."""
        g = GraphModel(GAPersonDim, GAProductDim, session=AsyncMock())
        # GAEdgeNoGrain has no __grain__ — should not raise
        g.validate_grain_join(GAEdgeGrainA, GAEdgeNoGrain)

    def test_single_edge_always_passes(self):
        """A single edge model trivially passes grain validation."""
        g = GraphModel(GAPersonDim, GAProductDim, session=AsyncMock())
        g.validate_grain_join(GAEdgeGrainA)

    def test_no_edges_passes(self):
        """Empty call passes silently."""
        g = GraphModel(GAPersonDim, session=AsyncMock())
        g.validate_grain_join()

    def test_error_message_includes_model_names(self):
        """Error message names the conflicting models."""
        g = GraphModel(GAPersonDim, GAProductDim, session=AsyncMock())
        with pytest.raises(GrainCompatibilityError) as exc_info:
            g.validate_grain_join(GAEdgeGrainA, GAEdgeGrainB)
        msg = str(exc_info.value)
        assert "GAEdgeGrainA" in msg or "GAEdgeGrainB" in msg


# ===========================================================================
# Tier 2: Temporal SQL Generation
# ===========================================================================

class TestTemporalSQLGeneration:

    def test_neighbors_sql_at_scd2_direction_out(self):
        """Temporal neighbors SQL includes effective_from/effective_to JOIN for SCD2."""
        te = TraversalEngine()
        sql = te.neighbors_sql_at(
            GATemporalEdge, GACustomerDim, start_id=1,
            as_of=date(2022, 6, 1), direction="out",
        )
        assert "effective_from" in sql
        assert "2022-06-01" in sql
        assert "effective_to" in sql
        assert "ga_customer_scd2" in sql
        assert "ga_temporal_edge" in sql

    def test_neighbors_sql_at_scd2_direction_in(self):
        """Temporal in-neighbors SQL for SCD1 vertex has no temporal filter."""
        te = TraversalEngine()
        sql = te.neighbors_sql_at(
            GATemporalEdge, GAPersonDim, start_id=1,
            as_of=date(2022, 6, 1), direction="in",
        )
        # GAPersonDim is SCD1 — no temporal predicates added
        assert "ga_temporal_edge" in sql
        assert "effective_from" not in sql
        assert "effective_to" not in sql

    def test_neighbors_sql_at_scd1_no_temporal_filter(self):
        """SCD Type 1 vertex model does not receive a temporal filter."""
        te = TraversalEngine()
        sql = te.neighbors_sql_at(
            GAPurchaseEdge, GAProductDim, start_id=1,
            as_of=date(2022, 6, 1), direction="out",
        )
        # GAProductDim is SCD1 — no temporal predicates
        assert "effective_from" not in sql
        assert "effective_to" not in sql

    def test_neighbors_sql_at_no_as_of_falls_back_to_plain(self):
        """When as_of=None, the temporal method returns plain neighbor SQL."""
        te = TraversalEngine()
        te.neighbors_sql(GAPurchaseEdge, start_id=1, direction="out")
        temporal = te.neighbors_sql_at(
            GAPurchaseEdge, GAProductDim, start_id=1, as_of=None, direction="out"
        )
        assert "effective_from" not in temporal
        assert "ga_purchase" in temporal

    def test_neighbors_sql_at_accepts_datetime_object(self):
        """as_of accepts a datetime.date object and formats it correctly."""
        from datetime import datetime
        te = TraversalEngine()
        sql = te.neighbors_sql_at(
            GATemporalEdge, GACustomerDim, start_id=1,
            as_of=datetime(2023, 3, 15, 12, 0, 0), direction="out",
        )
        assert "2023-03-15" in sql

    def test_neighbors_sql_at_scd2_vertex_direction_in(self):
        """SCD2 vertex with direction='in' generates in-neighbor SQL (no temporal join on subjects)."""
        te = TraversalEngine()
        sql = te.neighbors_sql_at(
            GATemporalEdge, GACustomerDim, start_id=2,
            as_of=date(2022, 6, 1), direction="in",
        )
        # The "in" branch returns subject_id without a temporal vertex JOIN
        assert "ga_temporal_edge" in sql
        assert "object_id = 2" in sql
        assert "subject_id" in sql

    def test_neighbors_sql_at_scd2_vertex_direction_both(self):
        """SCD2 vertex with direction='both' produces a UNION of temporal out + in."""
        te = TraversalEngine()
        sql = te.neighbors_sql_at(
            GATemporalEdge, GACustomerDim, start_id=2,
            as_of=date(2022, 6, 1), direction="both",
        )
        assert "UNION" in sql
        assert "effective_from" in sql
        assert "2022-06-01" in sql

    def test_neighbors_sql_at_undirected_edge_forces_both(self):
        """Undirected edge overrides direction to 'both' even when 'out' is requested."""
        te = TraversalEngine()
        sql = te.neighbors_sql_at(
            GAUndirectedSCD2Edge, GACustomerDim, start_id=1,
            as_of=date(2022, 6, 1), direction="out",
        )
        # __directed__ = False forces "both", producing UNION with temporal JOIN
        assert "UNION" in sql
        assert "effective_from" in sql


# ===========================================================================
# Tier 2: GraphModel.neighbors() with as_of
# ===========================================================================

class TestGraphModelTemporalNeighbors:

    async def test_neighbors_as_of_injects_temporal_clause(self):
        """neighbors() with as_of passes temporal where clause to hydration SQL."""
        session = AsyncMock()
        r1 = MagicMock()
        r1.fetchall.return_value = [(2,)]
        r2 = MagicMock()
        r2.mappings.return_value.fetchall.return_value = [
            {"id": 2, "customer_code": "C01", "name": "Alice",
             "is_current": True, "effective_from": "2020-01-01", "effective_to": None}
        ]
        session.execute.side_effect = [r1, r2]

        g = GraphModel(GAPersonDim, GACustomerDim, GATemporalEdge, session=session)
        person = GAPersonDim(id=1, email="a@a.com", name="A")
        await g.neighbors(person, edge_type=GATemporalEdge, direction="out", as_of=date(2022, 6, 1))

        # Second call (vertex hydration) SQL should contain temporal predicates
        hydration_sql = str(session.execute.call_args_list[1][0][0])
        assert "effective_from" in hydration_sql
        assert "2022-06-01" in hydration_sql

    async def test_neighbors_no_as_of_no_temporal_clause(self):
        """neighbors() without as_of does not add temporal predicates."""
        session = AsyncMock()
        r1 = MagicMock()
        r1.fetchall.return_value = [(2,)]
        r2 = MagicMock()
        r2.mappings.return_value.fetchall.return_value = [
            {"id": 2, "customer_code": "C01", "name": "Alice",
             "is_current": True, "effective_from": "2020-01-01", "effective_to": None}
        ]
        session.execute.side_effect = [r1, r2]

        g = GraphModel(GAPersonDim, GACustomerDim, GATemporalEdge, session=session)
        person = GAPersonDim(id=1, email="a@a.com", name="A")
        await g.neighbors(person, edge_type=GATemporalEdge, direction="out")

        hydration_sql = str(session.execute.call_args_list[1][0][0])
        assert "effective_from" not in hydration_sql

    async def test_neighbors_as_of_scd1_vertex_no_temporal_clause(self):
        """neighbors() with as_of on SCD1 vertex does not add temporal predicates."""
        session = AsyncMock()
        r1 = MagicMock()
        r1.fetchall.return_value = [(2,)]
        r2 = MagicMock()
        r2.mappings.return_value.fetchall.return_value = [
            {"id": 2, "sku": "P001", "name": "Widget"}
        ]
        session.execute.side_effect = [r1, r2]

        g = GraphModel(GAPersonDim, GAProductDim, GAPurchaseEdge, session=session)
        person = GAPersonDim(id=1, email="a@a.com", name="A")
        await g.neighbors(person, edge_type=GAPurchaseEdge, direction="out", as_of=date(2022, 6, 1))

        hydration_sql = str(session.execute.call_args_list[1][0][0])
        # GAProductDim is SCD1 — no temporal filter
        assert "effective_from" not in hydration_sql


# ===========================================================================
# Tier 2: DuckDB temporal neighbors integration
# ===========================================================================

@pytest.fixture
def ddb_scd2_con():
    """DuckDB in-memory DB with SCD2 customer and temporal edge data."""
    import duckdb
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE ga_customer_scd2 (
            id INT, customer_code VARCHAR, name VARCHAR,
            is_current BOOL, effective_from VARCHAR, effective_to VARCHAR
        )
    """)
    # Customer C001: v1 active 2019-2022, v2 active 2022+
    con.execute("""
        INSERT INTO ga_customer_scd2 VALUES
        (1, 'C001', 'Alice v1', false, '2019-01-01', '2022-01-01'),
        (2, 'C001', 'Alice v2', true,  '2022-01-01', NULL),
        (3, 'C002', 'Bob',      true,  '2019-01-01', NULL)
    """)
    con.execute("""
        CREATE TABLE ga_temporal_edge (
            id INT, subject_id INT, object_id INT, value DOUBLE
        )
    """)
    # Person 10 connects to both alice versions and bob
    con.execute("""
        INSERT INTO ga_temporal_edge VALUES
        (1, 10, 1, 100.0),
        (2, 10, 2, 200.0),
        (3, 10, 3, 50.0)
    """)
    yield con
    con.close()


class TestDuckDBTemporalNeighbors:

    def test_neighbors_at_scd2_past_date_filters_to_old_version(self, ddb_scd2_con):
        """As-of 2020 returns old version (id=1) of Alice, not new (id=2)."""
        te = DuckDBTraversalEngine(ddb_scd2_con)
        neighbors = te.neighbors_at(
            GATemporalEdge, GACustomerDim,
            start_id=10, as_of=date(2020, 6, 1), direction="out",
        )
        assert 1 in neighbors
        assert 2 not in neighbors

    def test_neighbors_at_scd2_future_date_returns_current(self, ddb_scd2_con):
        """As-of 2023 returns new version (id=2) of Alice, not old (id=1)."""
        te = DuckDBTraversalEngine(ddb_scd2_con)
        neighbors = te.neighbors_at(
            GATemporalEdge, GACustomerDim,
            start_id=10, as_of=date(2023, 3, 1), direction="out",
        )
        assert 2 in neighbors
        assert 1 not in neighbors
        # Bob is SCD1-like (single version) — always returned
        assert 3 in neighbors

    def test_neighbors_at_scd1_vertex_ignores_as_of(self, ddb_scd2_con):
        """SCD Type 1 vertex returns all matches regardless of as_of."""
        te = DuckDBTraversalEngine(ddb_scd2_con)
        con = ddb_scd2_con
        con.execute("""
            CREATE TABLE ga_person (id INT, email VARCHAR, name VARCHAR)
        """)
        con.execute("INSERT INTO ga_person VALUES (10, 'p@p.com', 'Person')")
        # This should not apply temporal filters since GAPersonDim.__scd_type__ = 1
        neighbors = te.neighbors_at(
            GATemporalEdge, GAPersonDim,
            start_id=2, as_of=date(2022, 6, 1), direction="in",
        )
        assert 10 in neighbors


# ===========================================================================
# Tier 3: Role-Aware Edge Discovery
# ===========================================================================

class TestRoleAwareEdgeDiscovery:

    def test_discover_role_edges_returns_dict(self):
        """discover_role_edges returns a dict keyed by fact class name."""
        g = GraphModel(GAPersonDim, GAProductDim, GAShipmentEdge, session=AsyncMock())
        schema = SchemaGraph.from_models([GAPersonDim, GAProductDim, GADateDim, GAShipmentEdge])
        result = g.discover_role_edges(schema)
        assert isinstance(result, dict)

    def test_discover_role_edges_finds_shipment_roles(self):
        """discover_role_edges finds ship_date and order_date roles."""
        g = GraphModel(GAPersonDim, GAProductDim, GAShipmentEdge, session=AsyncMock())
        schema = SchemaGraph.from_models([GAPersonDim, GAProductDim, GADateDim, GAShipmentEdge])
        result = g.discover_role_edges(schema)

        assert "GAShipmentEdge" in result
        refs = result["GAShipmentEdge"]
        assert len(refs) == 2
        role_names = {r.role for r in refs}
        assert "ship_date" in role_names
        assert "order_date" in role_names

    def test_discover_role_edges_no_roles_excluded(self):
        """Edges without role-playing dims are excluded from result."""
        g = GraphModel(GAPersonDim, GAProductDim, GAPurchaseEdge, session=AsyncMock())
        schema = SchemaGraph.from_models([GAPersonDim, GAProductDim, GAPurchaseEdge])
        result = g.discover_role_edges(schema)
        assert "GAPurchaseEdge" not in result

    def test_discover_role_edges_empty_for_no_facts(self):
        """Returns empty dict when schema graph has no relevant facts."""
        g = GraphModel(GAPersonDim, session=AsyncMock())
        schema = SchemaGraph.from_models([GAPersonDim])
        result = g.discover_role_edges(schema)
        assert result == {}

    def test_discover_role_edges_refs_have_correct_dimension(self):
        """RolePlayingRef instances reference the GADateDim dimension."""
        g = GraphModel(GAPersonDim, GAProductDim, GAShipmentEdge, session=AsyncMock())
        schema = SchemaGraph.from_models([GAPersonDim, GAProductDim, GADateDim, GAShipmentEdge])
        result = g.discover_role_edges(schema)
        refs = result["GAShipmentEdge"]
        for ref in refs:
            assert ref.dimension is GADateDim

    def test_discover_role_edges_includes_edge_models_absent_from_schema_facts(self):
        """Edge models in GraphModel but not in schema_graph.facts are appended and scanned."""
        g = GraphModel(GAPersonDim, GAProductDim, GAShipmentEdge, session=AsyncMock())
        # Schema has no facts — GAShipmentEdge is absent from schema.facts,
        # triggering the append branch in discover_role_edges.
        schema = SchemaGraph.from_models([GAPersonDim, GAProductDim, GADateDim])
        result = g.discover_role_edges(schema)
        assert "GAShipmentEdge" in result
        assert len(result["GAShipmentEdge"]) == 2


# ===========================================================================
# Tier 3: Weighted Bridge Traversal (SQL generation)
# ===========================================================================

class TestWeightedAggregationSQL:

    def test_aggregate_sql_weighted_sum(self):
        """Weighted SUM multiplies measure by weight column."""
        te = TraversalEngine()
        sql = te.aggregate_sql(
            GAWeightedEdge, start_id=1, measure="revenue", agg="sum",
            direction="out", weighted=True,
        )
        assert "revenue * weight" in sql or "weight * revenue" in sql
        assert "SUM" in sql.upper()

    def test_aggregate_sql_weighted_avg(self):
        """Weighted AVG multiplies measure by weight column."""
        te = TraversalEngine()
        sql = te.aggregate_sql(
            GAWeightedEdge, start_id=1, measure="revenue", agg="avg",
            direction="out", weighted=True,
        )
        assert "revenue * weight" in sql or "weight * revenue" in sql

    def test_aggregate_sql_unweighted_no_weight_column(self):
        """Without weighted=True, SQL aggregates measure directly (no * weight)."""
        te = TraversalEngine()
        sql = te.aggregate_sql(
            GAWeightedEdge, start_id=1, measure="revenue", agg="sum",
            direction="out", weighted=False,
        )
        assert "SUM(revenue)" in sql
        assert "* weight" not in sql  # no bridge multiplication

    def test_aggregate_sql_count_not_affected_by_weighted(self):
        """COUNT(*) remains unaffected when weighted=True."""
        te = TraversalEngine()
        sql = te.aggregate_sql(
            GAWeightedEdge, start_id=1, measure="*", agg="count",
            direction="out", weighted=True,
        )
        assert "COUNT(*)" in sql
        assert "* weight" not in sql  # COUNT(*) is not multiplied by weight


@pytest.fixture
def ddb_weighted_con():
    """DuckDB in-memory DB with weighted edge data."""
    import duckdb
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE ga_weighted_edge (
            id INT, subject_id INT, object_id INT, revenue DOUBLE, weight DOUBLE
        )
    """)
    # Two edges from vertex 1: revenue=100 w=0.5, revenue=200 w=0.25
    con.execute("INSERT INTO ga_weighted_edge VALUES (1,1,2,100.0,0.5),(2,1,3,200.0,0.25)")
    yield con
    con.close()


class TestWeightedAggregationDuckDB:

    def test_duckdb_weighted_sum_correct(self, ddb_weighted_con):
        """Weighted SUM = 100*0.5 + 200*0.25 = 100."""
        te = DuckDBTraversalEngine(ddb_weighted_con)
        result = te.aggregate(
            GAWeightedEdge, start_id=1, measure="revenue", agg="sum",
            direction="out", weighted=True,
        )
        assert result == pytest.approx(100.0)  # 50 + 50

    def test_duckdb_unweighted_sum_correct(self, ddb_weighted_con):
        """Unweighted SUM = 100 + 200 = 300."""
        te = DuckDBTraversalEngine(ddb_weighted_con)
        result = te.aggregate(
            GAWeightedEdge, start_id=1, measure="revenue", agg="sum",
            direction="out", weighted=False,
        )
        assert result == pytest.approx(300.0)


# ===========================================================================
# Tier 3: Strategy-Guided Freshness
# ===========================================================================

class TestStrategyGuidedFreshness:

    async def test_freshness_upsert_returns_none_without_db_query(self):
        """upsert strategy means data is always current — returns None immediately."""
        session = AsyncMock()
        g = GraphModel(GAPersonDim, GAProductDim, GAUpsertEdge, session=session)
        result = await g.freshness(GAUpsertEdge)
        assert result is None
        session.execute.assert_not_called()

    async def test_freshness_merge_returns_none(self):
        """merge strategy also returns None without querying."""
        class GAMergeEdge(EdgeModel, table=True):
            __tablename__ = "ga_merge_edge"
            __edge_type__ = "ga_merge"
            __strategy__ = "merge"
            __subject__ = GAPersonDim
            __object__ = GAProductDim

            id: Optional[int] = Field(default=None, primary_key=True)
            subject_id: int = DimField(default=0, foreign_key="ga_person.id")
            object_id: int = DimField(default=0, foreign_key="ga_product.id")

        session = AsyncMock()
        g = GraphModel(GAPersonDim, GAProductDim, GAMergeEdge, session=session)
        result = await g.freshness(GAMergeEdge)
        assert result is None
        session.execute.assert_not_called()

    async def test_freshness_bulk_queries_max_timestamp(self):
        """bulk strategy queries MAX(updated_at) from edge table."""
        session = AsyncMock()
        result_mock = MagicMock()
        result_mock.fetchone.return_value = ("2026-01-15T10:00:00",)
        session.execute.return_value = result_mock

        g = GraphModel(GAPersonDim, GAProductDim, GABulkEdge, session=session)
        result = await g.freshness(GABulkEdge, timestamp_column="updated_at")
        assert result == "2026-01-15T10:00:00"
        session.execute.assert_called_once()

    async def test_freshness_bulk_empty_table_returns_none(self):
        """bulk strategy with empty table returns None."""
        session = AsyncMock()
        result_mock = MagicMock()
        result_mock.fetchone.return_value = (None,)
        session.execute.return_value = result_mock

        g = GraphModel(GAPersonDim, GAProductDim, GABulkEdge, session=session)
        result = await g.freshness(GABulkEdge)
        assert result is None

    async def test_freshness_no_strategy_queries_table(self):
        """Without __strategy__, freshness still queries MAX timestamp."""
        class GANoStrategyEdge(EdgeModel, table=True):
            __tablename__ = "ga_no_strategy_edge"
            __edge_type__ = "ga_no_strategy"
            __subject__ = GAPersonDim
            __object__ = GAProductDim

            id: Optional[int] = Field(default=None, primary_key=True)
            subject_id: int = DimField(default=0, foreign_key="ga_person.id")
            object_id: int = DimField(default=0, foreign_key="ga_product.id")
            updated_at: Optional[str] = None

        session = AsyncMock()
        result_mock = MagicMock()
        result_mock.fetchone.return_value = ("2026-03-01",)
        session.execute.return_value = result_mock

        g = GraphModel(GAPersonDim, GAProductDim, GANoStrategyEdge, session=session)
        result = await g.freshness(GANoStrategyEdge)
        assert result == "2026-03-01"

    async def test_freshness_unregistered_edge_raises(self):
        """Calling freshness with an unregistered edge raises SchemaError."""
        g = GraphModel(GAPersonDim, session=AsyncMock())
        with pytest.raises(SchemaError):
            await g.freshness(GABulkEdge)


# ===========================================================================
# _check_additive edge cases (defensive returns)
# ===========================================================================

class TestCheckAdditiveEdgeCases:

    def test_check_additive_class_without_table_returns_silently(self):
        """_check_additive returns without error when edge_type has no __table__."""
        class NotATable:
            """Plain Python class — no __table__ attribute."""

        # Must not raise — the permissive early return fires
        GraphModel._check_additive(NotATable, "amount", "sum")

    def test_check_additive_nonexistent_measure_returns_silently(self):
        """_check_additive returns without error when measure column isn't in the table."""
        # GAPurchaseEdge has 'amount' and 'discount', but not 'nonexistent'
        GraphModel._check_additive(GAPurchaseEdge, "nonexistent_column", "sum")
