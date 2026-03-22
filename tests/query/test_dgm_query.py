"""
TDD tests for the DGM (Dimensional Graph Model) query builder.

Tests are ordered to cover:
  § Ref types         — PropRef, AggRef, WinRef
  § Predicate types   — ScalarPred, AND, OR, NOT, PathPred
  § Path types        — VerbHop, BridgeHop, Compose
  § DGMQuery B1       — anchor / path_join / temporal_join / where
  § DGMQuery B2       — group_by / agg / having + validation
  § DGMQuery B3       — window / qualify  + validation
  § DGMQuery full     — B1 ∘ B2 ∘ B3 SQL + execution
  § Integration       — execute against in-memory DuckDB
"""
from __future__ import annotations

import pytest
import duckdb

from sqldim.core.query.dgm import (
    PropRef, AggRef, WinRef,
    ScalarPred, PathPred,
    AND, OR, NOT,
    VerbHop, BridgeHop, Compose,
    DGMQuery,
)
from sqldim.exceptions import SemanticError


# ---------------------------------------------------------------------------
# Fixtures — in-memory DuckDB with a small star schema
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def con():
    """DuckDB connection with three tables: dim_customer, dim_product, fact_sales."""
    c = duckdb.connect()
    c.execute("""
        CREATE TABLE dim_customer (
            id      INTEGER PRIMARY KEY,
            name    VARCHAR,
            segment VARCHAR,
            region  VARCHAR,
            valid_from DATE,
            valid_to   DATE
        )
    """)
    c.execute("""
        CREATE TABLE dim_product (
            id       INTEGER PRIMARY KEY,
            name     VARCHAR,
            category VARCHAR
        )
    """)
    c.execute("""
        CREATE TABLE fact_sales (
            id          INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id  INTEGER,
            revenue     DOUBLE,
            quantity    INTEGER,
            sale_year   INTEGER
        )
    """)
    # Customers
    c.execute("""INSERT INTO dim_customer VALUES
        (1, 'Alice', 'retail',    'US', '2020-01-01', NULL),
        (2, 'Bob',   'wholesale', 'EU', '2020-01-01', NULL),
        (3, 'Carol', 'retail',    'US', '2020-01-01', NULL)
    """)
    # Products
    c.execute("""INSERT INTO dim_product VALUES
        (1, 'Widget', 'electronics'),
        (2, 'Gadget', 'clearance'),
        (3, 'Donut',  'food')
    """)
    # Sales: Alice 5200 total, Bob 4000, Carol 2600
    c.execute("""INSERT INTO fact_sales VALUES
        (1, 1, 1, 1500.0, 3, 2024),
        (2, 1, 2,  200.0, 1, 2024),
        (3, 1, 3, 3500.0, 5, 2024),
        (4, 2, 1, 4000.0, 8, 2024),
        (5, 3, 1, 2000.0, 4, 2024),
        (6, 3, 3,  600.0, 2, 2024)
    """)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# § Ref types
# ---------------------------------------------------------------------------

class TestPropRef:
    def test_two_arg_form(self):
        r = PropRef("c", "segment")
        assert r.to_sql() == "c.segment"

    def test_dotted_string_form(self):
        r = PropRef("c.segment")
        assert r.to_sql() == "c.segment"

    def test_repr(self):
        assert "segment" in repr(PropRef("c", "segment"))


class TestAggRef:
    def test_to_sql(self):
        assert AggRef("total_rev").to_sql() == "total_rev"

    def test_repr(self):
        assert "total_rev" in repr(AggRef("total_rev"))


class TestWinRef:
    def test_to_sql(self):
        assert WinRef("rn").to_sql() == "rn"

    def test_repr(self):
        assert "rn" in repr(WinRef("rn"))


# ---------------------------------------------------------------------------
# § ScalarPred
# ---------------------------------------------------------------------------

class TestScalarPred:
    def test_eq_string(self):
        p = ScalarPred(PropRef("c", "segment"), "=", "retail")
        assert p.to_sql() == "c.segment = 'retail'"

    def test_gt_number(self):
        p = ScalarPred(AggRef("total_rev"), ">", 5000)
        assert p.to_sql() == "total_rev > 5000"

    def test_lte_win_ref(self):
        p = ScalarPred(WinRef("rank"), "<=", 2)
        assert p.to_sql() == "rank <= 2"

    def test_ne_string(self):
        p = ScalarPred(PropRef("d", "category"), "!=", "clearance")
        assert p.to_sql() == "d.category != 'clearance'"

    def test_is_null(self):
        p = ScalarPred(PropRef("c", "valid_to"), "IS", None)
        assert p.to_sql() == "c.valid_to IS NULL"

    def test_float_value(self):
        p = ScalarPred(AggRef("score"), ">=", 0.5)
        assert "0.5" in p.to_sql()

    def test_none_value_equality(self):
        # _format_value(None) branch: operator is not IS/IS NOT
        p = ScalarPred(PropRef("x", "val"), "=", None)
        assert p.to_sql() == "x.val = NULL"


# ---------------------------------------------------------------------------
# § Boolean tree (AND, OR, NOT)
# ---------------------------------------------------------------------------

class TestBooleanTree:
    def test_and_single(self):
        p = AND(ScalarPred(PropRef("c", "segment"), "=", "retail"))
        assert p.to_sql() == "c.segment = 'retail'"

    def test_and_multiple(self):
        sql = AND(
            ScalarPred(PropRef("c", "segment"), "=", "retail"),
            ScalarPred(PropRef("c", "region"),  "=", "US"),
        ).to_sql()
        assert "AND" in sql
        assert "c.segment = 'retail'" in sql
        assert "c.region = 'US'" in sql

    def test_or_multiple(self):
        sql = OR(
            ScalarPred(PropRef("c", "region"), "=", "US"),
            ScalarPred(PropRef("c", "region"), "=", "EU"),
        ).to_sql()
        assert "OR" in sql

    def test_not_simple(self):
        inner = ScalarPred(PropRef("d", "category"), "=", "clearance")
        sql = NOT(inner).to_sql()
        assert sql.startswith("NOT")
        assert "clearance" in sql

    def test_not_not_cancels(self):
        inner = ScalarPred(PropRef("c", "segment"), "=", "retail")
        double_not = NOT(NOT(inner))
        # NOT(NOT(T)) ≡ T — returns the inner pred, not a wrapped NOT
        assert double_not.to_sql() == inner.to_sql()

    def test_nested_and_or(self):
        sql = AND(
            ScalarPred(PropRef("c", "segment"), "=", "retail"),
            OR(
                ScalarPred(PropRef("c", "region"), "=", "US"),
                ScalarPred(PropRef("c", "region"), "=", "CA"),
            ),
        ).to_sql()
        assert "AND" in sql
        assert "OR" in sql


# ---------------------------------------------------------------------------
# § PathPred
# ---------------------------------------------------------------------------

class TestPathPred:
    def test_single_hop_sql(self):
        hop = VerbHop("s", "included_in", "d",
                      table="dim_product", on="d.id = s.product_id")
        pp = PathPred(
            anchor="s",
            path=hop,
            sub_filter=ScalarPred(PropRef("d", "category"), "=", "electronics"),
        )
        sql = pp.to_sql()
        assert "EXISTS" in sql
        assert "dim_product d" in sql
        assert "d.category = 'electronics'" in sql
        assert "d.id = s.product_id" in sql

    def test_composed_path_sql(self):
        hop1 = VerbHop("s", "included_in", "d",
                       table="dim_product", on="d.id = s.product_id")
        hop2 = BridgeHop("d", "belongs_to", "seg",
                         table="dim_segment", on="seg.id = d.segment_id")
        path = Compose(hop1, hop2)
        pp = PathPred(
            anchor="s",
            path=path,
            sub_filter=ScalarPred(PropRef("seg", "name"), "=", "premium"),
        )
        sql = pp.to_sql()
        assert "EXISTS" in sql
        assert "dim_product d" in sql
        assert "dim_segment seg" in sql
        assert "seg.name = 'premium'" in sql


# ---------------------------------------------------------------------------
# § Hop types
# ---------------------------------------------------------------------------

class TestHopTypes:
    def test_verb_hop_kind(self):
        h = VerbHop("s", "included_in", "d", table="dim_product", on="d.id = s.product_id")
        assert h.kind == "verb"
        assert h.from_alias == "s"
        assert h.to_alias == "d"
        assert h.table == "dim_product"
        assert h.label == "included_in"

    def test_bridge_hop_kind(self):
        h = BridgeHop("d", "promoted_in", "st",
                      table="dim_store", on="st.id = d.store_id")
        assert h.kind == "bridge"
        assert h.from_alias == "d"

    def test_compose_preserves_order(self):
        h1 = VerbHop("a", "l1", "b", table="t1", on="b.id = a.fk")
        h2 = BridgeHop("b", "l2", "c", table="t2", on="c.id = b.fk")
        c = Compose(h1, h2)
        assert c.left is h1
        assert c.right is h2


# ---------------------------------------------------------------------------
# § DGMQuery — B1 (Context band)
# ---------------------------------------------------------------------------

class TestDGMQueryB1:
    def test_anchor_alone_sql(self):
        q = DGMQuery().anchor("fact_sales", "s")
        sql = q.to_sql()
        assert "SELECT *" in sql
        assert "FROM fact_sales s" in sql

    def test_anchor_default_alias(self):
        q = DGMQuery().anchor("fact_sales")
        sql = q.to_sql()
        assert "FROM fact_sales" in sql

    def test_where_scalar_pred(self):
        q = (DGMQuery()
             .anchor("fact_sales", "s")
             .where(ScalarPred(PropRef("s", "sale_year"), "=", 2024)))
        sql = q.to_sql()
        assert "WHERE s.sale_year = 2024" in sql

    def test_path_join_verb(self):
        hop = VerbHop("s", "placed_by", "c",
                      table="dim_customer", on="c.id = s.customer_id")
        q = (DGMQuery()
             .anchor("fact_sales", "s")
             .path_join(hop))
        sql = q.to_sql()
        assert "LEFT JOIN dim_customer c ON c.id = s.customer_id" in sql

    def test_path_join_bridge(self):
        hop = BridgeHop("d", "promoted_in", "st",
                        table="dim_store", on="st.id = d.store_id")
        q = (DGMQuery()
             .anchor("dim_product", "d")
             .path_join(hop))
        sql = q.to_sql()
        assert "LEFT JOIN dim_store st ON st.id = d.store_id" in sql

    def test_temporal_join_adds_scd2_condition(self):
        hop = VerbHop("s", "placed_by", "c",
                      table="dim_customer", on="c.id = s.customer_id")
        q = (DGMQuery()
             .anchor("fact_sales", "s")
             .path_join(hop)
             .temporal_join("2024-06-30"))
        sql = q.to_sql()
        assert "valid_from" in sql
        assert "2024-06-30" in sql

    def test_multiple_joins(self):
        hop_c = VerbHop("s", "placed_by", "c",
                        table="dim_customer", on="c.id = s.customer_id")
        hop_d = VerbHop("s", "includes", "d",
                        table="dim_product", on="d.id = s.product_id")
        q = (DGMQuery()
             .anchor("fact_sales", "s")
             .path_join(hop_c)
             .path_join(hop_d))
        sql = q.to_sql()
        assert "dim_customer c" in sql
        assert "dim_product d" in sql

    def test_b1_path_pred_in_where(self, con):
        hop = VerbHop("s", "includes", "d",
                      table="dim_product", on="d.id = s.product_id")
        pp = PathPred(
            anchor="s",
            path=hop,
            sub_filter=ScalarPred(PropRef("d", "category"), "=", "electronics"),
        )
        q = (DGMQuery()
             .anchor("fact_sales", "s")
             .where(pp))
        sql = q.to_sql()
        assert "EXISTS" in sql
        rows = con.execute(sql).fetchall()
        # Only sales of electronics (product_id=1: Widget)
        assert len(rows) == 3  # sales 1, 4, 5

    def test_no_anchor_raises(self):
        with pytest.raises(SemanticError, match="anchor"):
            DGMQuery().to_sql()


# ---------------------------------------------------------------------------
# § DGMQuery — B2 (Aggregation band)
# ---------------------------------------------------------------------------

class TestDGMQueryB2:
    def test_group_agg_sql(self):
        q = (DGMQuery()
             .anchor("fact_sales", "s")
             .group_by("s.customer_id")
             .agg(total_rev="SUM(s.revenue)", cnt="COUNT(*)"))
        sql = q.to_sql()
        assert "SELECT s.customer_id, SUM(s.revenue) AS total_rev" in sql
        assert "GROUP BY s.customer_id" in sql

    def test_having_sql(self):
        q = (DGMQuery()
             .anchor("fact_sales", "s")
             .group_by("s.customer_id")
             .agg(total_rev="SUM(s.revenue)")
             .having(ScalarPred(AggRef("total_rev"), ">", 4000)))
        sql = q.to_sql()
        assert "HAVING total_rev > 4000" in sql

    def test_group_without_agg_is_valid(self):
        """group_by() without agg() is valid SQL (deduplication / pre-window use)."""
        sql = DGMQuery().anchor("fact_sales", "s").group_by("s.customer_id").to_sql()
        assert "GROUP BY s.customer_id" in sql

    def test_agg_without_group_is_valid(self):
        """agg() without group_by() is a valid global aggregate (e.g. COUNT(*))."""
        sql = DGMQuery().anchor("fact_sales", "s").agg(total="SUM(revenue)").to_sql()
        assert "SUM(revenue) AS total" in sql

    def test_having_without_b2_raises(self):
        with pytest.raises(SemanticError):
            (DGMQuery()
             .anchor("fact_sales", "s")
             .having(ScalarPred(AggRef("x"), ">", 1))
             .to_sql())

    def test_b2_no_having_is_valid(self):
        # GROUP BY + AGG without HAVING is valid (B2 without Having)
        sql = (DGMQuery()
               .anchor("fact_sales", "s")
               .group_by("s.customer_id")
               .agg(total="SUM(s.revenue)")
               .to_sql())
        assert "GROUP BY" in sql
        assert "HAVING" not in sql


# ---------------------------------------------------------------------------
# § DGMQuery — B3 (Ranking band)
# ---------------------------------------------------------------------------

class TestDGMQueryB3:
    def test_window_qualify_sql(self):
        q = (DGMQuery()
             .anchor("fact_sales", "s")
             .window(rn="ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.revenue DESC)")
             .qualify(ScalarPred(WinRef("rn"), "=", 1)))
        sql = q.to_sql()
        assert "ROW_NUMBER() OVER" in sql
        assert "rn AS rn" in sql or "AS rn" in sql
        assert "QUALIFY rn = 1" in sql

    def test_window_without_qualify_is_valid(self):
        """window() without qualify() is valid (e.g. NTILE for bucketing)."""
        sql = (
            DGMQuery()
            .anchor("fact_sales", "s")
            .window(rn="ROW_NUMBER() OVER (ORDER BY s.revenue DESC)")
            .to_sql()
        )
        assert "ROW_NUMBER() OVER" in sql

    def test_qualify_without_window_raises(self):
        with pytest.raises(SemanticError):
            (DGMQuery()
             .anchor("fact_sales", "s")
             .qualify(ScalarPred(WinRef("rn"), "=", 1))
             .to_sql())


# ---------------------------------------------------------------------------
# § DGMQuery — Full B1 ∘ B2 ∘ B3
# ---------------------------------------------------------------------------

class TestDGMQueryFull:
    def test_full_sql_structure(self):
        hop = VerbHop("s", "placed_by", "c",
                      table="dim_customer", on="c.id = s.customer_id")
        q = (DGMQuery()
             .anchor("fact_sales", "s")
             .path_join(hop)
             .where(ScalarPred(PropRef("c", "segment"), "=", "retail"))
             .group_by("c.id", "c.region")
             .agg(total_rev="SUM(s.revenue)", cnt="COUNT(*)")
             .having(ScalarPred(AggRef("total_rev"), ">", 4000))
             .window(rnk="RANK() OVER (ORDER BY SUM(s.revenue) DESC)")
             .qualify(ScalarPred(WinRef("rnk"), "<=", 2)))
        sql = q.to_sql()
        assert "SELECT" in sql
        assert "FROM fact_sales s" in sql
        assert "LEFT JOIN dim_customer c" in sql
        assert "WHERE c.segment = 'retail'" in sql
        assert "GROUP BY" in sql
        assert "HAVING" in sql
        assert "QUALIFY" in sql

    def test_b1_b3_no_aggregation(self):
        # B1 ∘ B3 — window on raw rows (no GROUP BY)
        q = (DGMQuery()
             .anchor("fact_sales", "s")
             .where(ScalarPred(PropRef("s", "sale_year"), "=", 2024))
             .window(latest="ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.revenue DESC)")
             .qualify(ScalarPred(WinRef("latest"), "=", 1)))
        sql = q.to_sql()
        assert "SELECT *" in sql
        assert "ROW_NUMBER() OVER" in sql
        assert "QUALIFY latest = 1" in sql
        assert "GROUP BY" not in sql


# ---------------------------------------------------------------------------
# § Integration — execute against DuckDB
# ---------------------------------------------------------------------------

class TestDGMQueryExecution:
    def test_execute_b1_only(self, con):
        """B1: filter fact_sales to retail customers."""
        hop = VerbHop("s", "placed_by", "c",
                      table="dim_customer", on="c.id = s.customer_id")
        rows = (DGMQuery()
                .anchor("fact_sales", "s")
                .path_join(hop)
                .where(ScalarPred(PropRef("c", "segment"), "=", "retail"))
                .execute(con))
        # Retail: Alice (3 sales) + Carol (2 sales) = 5
        assert len(rows) == 5

    def test_execute_b1_b2_having(self, con):
        """B2: total revenue per customer, having > 4000."""
        hop = VerbHop("s", "placed_by", "c",
                      table="dim_customer", on="c.id = s.customer_id")
        rows = (DGMQuery()
                .anchor("fact_sales", "s")
                .path_join(hop)
                .group_by("c.id", "c.name")
                .agg(total_rev="SUM(s.revenue)")
                .having(ScalarPred(AggRef("total_rev"), ">", 4000))
                .execute(con))
        # Alice: 5200, Bob: 4000 (not >4000), Carol: 2600
        assert len(rows) == 1
        names = [r[1] for r in rows]
        assert "Alice" in names

    def test_execute_b1_b3_top1_per_customer(self, con):
        """B3: top-1 sale per customer by revenue."""
        rows = (DGMQuery()
                .anchor("fact_sales", "s")
                .window(rn="ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.revenue DESC)")
                .qualify(ScalarPred(WinRef("rn"), "=", 1))
                .execute(con))
        # One row per customer — 3 customers
        assert len(rows) == 3

        # The top sale for customer 1 (Alice) should be sale 3 (3500)
        alice_row = next(r for r in rows if r[1] == 1)  # customer_id == 1
        assert alice_row[3] == 3500.0  # revenue (id, customer_id, product_id, revenue, ...)

    def test_execute_full_b1_b2_b3(self, con):
        """B1∘B2∘B3: top-1 customer group by revenue, only retail."""
        hop = VerbHop("s", "placed_by", "c",
                      table="dim_customer", on="c.id = s.customer_id")
        rows = (DGMQuery()
                .anchor("fact_sales", "s")
                .path_join(hop)
                .where(ScalarPred(PropRef("c", "segment"), "=", "retail"))
                .group_by("c.id", "c.name")
                .agg(total_rev="SUM(s.revenue)")
                .having(ScalarPred(AggRef("total_rev"), ">", 1000))
                .window(rnk="RANK() OVER (ORDER BY SUM(s.revenue) DESC)")
                .qualify(ScalarPred(WinRef("rnk"), "=", 1))
                .execute(con))
        # Retail: Alice 5200, Carol 2600 — rank=1 is Alice
        assert len(rows) == 1
        assert rows[0][1] == "Alice"

    def test_execute_b1_where_and(self, con):
        """B1: compound WHERE with AND."""
        hop = VerbHop("s", "placed_by", "c",
                      table="dim_customer", on="c.id = s.customer_id")
        rows = (DGMQuery()
                .anchor("fact_sales", "s")
                .path_join(hop)
                .where(AND(
                    ScalarPred(PropRef("c", "segment"), "=", "retail"),
                    ScalarPred(PropRef("c", "region"),  "=", "US"),
                ))
                .execute(con))
        # Alice (US, retail): 3 sales; Carol (US, retail): 2 sales
        assert len(rows) == 5

    def test_execute_not_pred(self, con):
        """B1: WHERE NOT — exclude clearance products."""
        hop = VerbHop("s", "includes", "d",
                      table="dim_product", on="d.id = s.product_id")
        rows = (DGMQuery()
                .anchor("fact_sales", "s")
                .path_join(hop)
                .where(NOT(ScalarPred(PropRef("d", "category"), "=", "clearance")))
                .execute(con))
        # Sale 2 (Gadget, clearance) excluded → 5 rows
        assert len(rows) == 5


# ---------------------------------------------------------------------------
# § Model-first API / Ref-kind enforcement / Band aliases / Deprecation
# ---------------------------------------------------------------------------

# Minimal SQLModel classes for FK-inference tests (shared SQLModel metadata registry).
# We reuse the ecommerce example models to avoid duplicate table definitions.
from sqldim.examples.real_world.ecommerce.models import (  # noqa: E402
    Customer as _Customer,
    OrderFact as _OrderFact,
)


class TestModelFirstAnchor:
    """DGMQuery.anchor() / context() accept SQLModel classes."""

    def test_anchor_model_sets_table_name(self):
        q = DGMQuery().anchor(_Customer, alias="c")
        assert q._anchor_table == _Customer.table_name()
        assert q._anchor_alias == "c"

    def test_anchor_model_registers_alias(self):
        q = DGMQuery().anchor(_Customer, alias="c")
        assert q._alias_registry["c"] is _Customer

    def test_anchor_model_no_alias_uses_table_as_key(self):
        q = DGMQuery().anchor(_Customer)
        assert q._alias_registry[_Customer.table_name()] is _Customer

    def test_anchor_string_unchanged(self):
        q = DGMQuery().anchor("dim_customer", alias="c")
        assert q._anchor_table == "dim_customer"
        assert q._alias_registry == {}

    def test_context_is_alias_for_anchor(self):
        q1 = DGMQuery().anchor(_Customer, alias="c")
        q2 = DGMQuery().context(_Customer, alias="c")
        assert q1._anchor_table == q2._anchor_table
        assert q1._anchor_alias == q2._anchor_alias
        assert q1._alias_registry == q2._alias_registry


class TestModelFirstPathJoin:
    """DGMQuery.path_join() model-first form infers FK ON clause."""

    def test_path_join_model_appends_join(self):
        q = (DGMQuery()
             .anchor(_Customer, alias="c")
             .path_join(_OrderFact, from_alias="c", to_alias="s"))
        assert len(q._joins) == 1
        table, alias, on = q._joins[0]
        assert table == _OrderFact.table_name()
        assert alias == "s"
        assert "c.id" in on
        assert "s.customer_id" in on

    def test_path_join_model_registers_to_alias(self):
        q = (DGMQuery()
             .anchor(_Customer, alias="c")
             .path_join(_OrderFact, from_alias="c", to_alias="s"))
        assert q._alias_registry["s"] is _OrderFact

    def test_path_join_model_missing_from_alias_raises(self):
        with pytest.raises(SemanticError, match="from_alias"):
            (DGMQuery()
             .anchor(_Customer, alias="c")
             .path_join(_OrderFact, from_alias="c"))  # to_alias missing

    def test_path_join_unregistered_from_alias_raises(self):
        with pytest.raises(SemanticError, match="not registered"):
            (DGMQuery()
             .anchor(_Customer, alias="c")
             .path_join(_OrderFact, from_alias="UNKNOWN", to_alias="s"))

    def test_path_join_no_fk_raises(self):
        # _Customer has no FK pointing at itself
        with pytest.raises(SemanticError, match="No FK"):
            (DGMQuery()
             .anchor(_Customer, alias="c")
             .path_join(_Customer, from_alias="c", to_alias="c2"))

    def test_path_join_hop_with_explicit_table_on_unchanged(self):
        """Backward-compat: hop with explicit table/on still works."""
        hop = VerbHop("c", "placed", "s",
                      table="fact_order", on="c.id = s.customer_id")
        q = DGMQuery().anchor("dim_customer", "c").path_join(hop)
        assert q._joins[0] == ("fact_order", "s", "c.id = s.customer_id")


class TestRefKindEnforcement:
    """Band violations raise SemanticError at construction, not at .to_sql()."""

    def test_where_rejects_aggref(self):
        with pytest.raises(SemanticError, match="AggRef"):
            DGMQuery().anchor("t").where(ScalarPred(AggRef("x"), ">", 1))

    def test_where_rejects_winref(self):
        with pytest.raises(SemanticError, match="WinRef"):
            DGMQuery().anchor("t").where(ScalarPred(WinRef("rn"), "=", 1))

    def test_where_accepts_propref(self):
        q = DGMQuery().anchor("t").where(ScalarPred(PropRef("t.x"), "=", 1))
        assert q._where_pred is not None

    def test_having_rejects_propref(self):
        with pytest.raises(SemanticError, match="PropRef"):
            (DGMQuery().anchor("t")
             .group_by("t.x").agg(n="COUNT(*)")
             .having(ScalarPred(PropRef("t.x"), "=", "a")))

    def test_having_rejects_winref(self):
        with pytest.raises(SemanticError, match="WinRef"):
            (DGMQuery().anchor("t")
             .group_by("t.x").agg(n="COUNT(*)")
             .having(ScalarPred(WinRef("rn"), ">", 1)))

    def test_having_rejects_path_pred(self):
        hop = VerbHop("c", "placed", "s", table="fact_t", on="c.id = s.c_id")
        pp = PathPred("c", hop, ScalarPred(PropRef("s.x"), "=", 1))
        with pytest.raises(SemanticError, match="PathPred"):
            (DGMQuery().anchor("t")
             .group_by("t.x").agg(n="COUNT(*)")
             .having(pp))

    def test_qualify_rejects_propref(self):
        with pytest.raises(SemanticError, match="PropRef"):
            (DGMQuery().anchor("t")
             .window(rn="ROW_NUMBER() OVER ()")
             .qualify(ScalarPred(PropRef("t.x"), "=", 1)))

    def test_qualify_rejects_aggref(self):
        with pytest.raises(SemanticError, match="AggRef"):
            (DGMQuery().anchor("t")
             .window(rn="ROW_NUMBER() OVER ()")
             .qualify(ScalarPred(AggRef("total"), ">", 5)))

    def test_qualify_rejects_path_pred(self):
        hop = VerbHop("c", "placed", "s", table="fact_t", on="c.id = s.c_id")
        pp = PathPred("c", hop, ScalarPred(PropRef("s.x"), "=", 1))
        with pytest.raises(SemanticError, match="PathPred"):
            (DGMQuery().anchor("t")
             .window(rn="ROW_NUMBER() OVER ()")
             .qualify(pp))


class TestBandAliases:
    """context(), aggregate(), rank() are spec-vocabulary aliases."""

    def test_aggregate_sets_group_by_and_agg(self):
        q = (DGMQuery()
             .anchor("fact_t", "f")
             .aggregate("f.region", total="SUM(f.revenue)"))
        assert q._group_by_cols == ["f.region"]
        assert q._agg_exprs == {"total": "SUM(f.revenue)"}
        assert q._having_pred is None

    def test_aggregate_with_having(self):
        pred = ScalarPred(AggRef("total"), ">", 100)
        q = (DGMQuery()
             .anchor("fact_t", "f")
             .aggregate("f.region", total="SUM(f.revenue)", having=pred))
        assert q._having_pred is pred

    def test_rank_sets_window_and_qualify(self):
        pred = ScalarPred(WinRef("rn"), "=", 1)
        q = (DGMQuery()
             .anchor("fact_t", "f")
             .rank(rn="ROW_NUMBER() OVER (PARTITION BY f.id)", qualify=pred))
        assert q._window_exprs == {"rn": "ROW_NUMBER() OVER (PARTITION BY f.id)"}
        assert q._qualify_pred is pred

    def test_rank_without_qualify(self):
        q = DGMQuery().anchor("t").rank(rn="ROW_NUMBER() OVER ()")
        assert q._qualify_pred is None
        assert "rn" in q._window_exprs


# ---------------------------------------------------------------------------
# § Coverage — previously uncovered branches in dgm.py
# ---------------------------------------------------------------------------


class TestCoverageBranches:
    """Tests that hit branches missed in prior coverage runs."""

    def test_collect_fk_matches_no_table_raises(self):
        """line: _collect_fk_matches — class without __table__."""
        from sqldim.core.query.dgm import _collect_fk_matches

        class NoTable:
            __name__ = "NoTable"

        with pytest.raises(SemanticError, match="has no SQLAlchemy table metadata"):
            _collect_fk_matches(NoTable, "sometable")

    def test_infer_on_no_fk_via_hop_raises(self):
        """lines: _resolve_hop_join + _infer_on 'No FK' — hop on=None, no FK."""
        # _Customer has no FK to itself => raises SemanticError
        with pytest.raises(SemanticError, match="No FK"):
            hop = VerbHop("c", "self", "c2",
                          model=_Customer, table=_Customer.table_name(), on=None)
            (DGMQuery()
             .anchor(_Customer, alias="c")
             .path_join(hop))

    def test_resolve_hop_join_infers_on_and_registers_model(self):
        """lines 599-604: VerbHop with model + on=None uses FK inference
        and registers the model in _alias_registry."""
        hop = VerbHop("c", "placed", "s", model=_OrderFact, on=None)
        q = (DGMQuery()
             .anchor(_Customer, alias="c")
             .path_join(hop))
        # FK was inferred successfully
        _table, alias, on = q._joins[0]
        assert alias == "s"
        assert "c.id" in on
        # model registered at to_alias
        assert q._alias_registry["s"] is _OrderFact

    def test_has_path_pred_true_for_and_containing_path_pred(self):
        """line: _has_path_pred AND branch — returns True for AND(PathPred, ...)."""
        from sqldim.core.query.dgm import _has_path_pred

        hop = VerbHop("c", "lbl", "s", table="t", on="c.id = s.fk")
        pp = PathPred("c", hop, ScalarPred(PropRef("s.x"), "=", 1))
        compound = AND(ScalarPred(PropRef("c.y"), ">", 0), pp)
        assert _has_path_pred(compound) is True

    def test_has_path_pred_true_for_not_wrapping_path_pred(self):
        """line: _has_path_pred NOT branch — return True for NOT(PathPred(...))."""
        from sqldim.core.query.dgm import _has_path_pred

        hop = VerbHop("c", "lbl", "s", table="t", on="c.id = s.fk")
        pp = PathPred("c", hop, ScalarPred(PropRef("s.x"), "=", 1))
        assert _has_path_pred(NOT(pp)) is True

    def test_where_wraps_raw_string_in_raw_pred(self):
        """line 628: DGMQuery.where(string) wraps the string in RawPred."""
        from sqldim.core.query.dgm import RawPred

        q = DGMQuery().anchor("fact_t", "f").where("f.amount > 100")
        assert isinstance(q._where_pred, RawPred)
        assert "f.amount > 100" in q.to_sql()

    def test_infer_on_ambiguous_fk_raises(self):
        """line 381: _infer_on — two FK columns pointing at same table → SemanticError."""
        from sqldim.core.query.dgm import _infer_on
        from unittest.mock import MagicMock

        def _fk(target):
            fk = MagicMock()
            fk.target_fullname = target
            return fk

        col1 = MagicMock()
        col1.foreign_keys = [_fk("dim_customer.id")]
        col2 = MagicMock()
        col2.foreign_keys = [_fk("dim_customer.id")]

        table_mock = MagicMock()
        table_mock.columns.items.return_value = [("from_id", col1), ("to_id", col2)]

        class AmbigModel:
            __name__ = "AmbigModel"
            __table__ = table_mock

        with pytest.raises(SemanticError, match="Ambiguous FK"):
            _infer_on("c", "s", AmbigModel, "dim_customer")

    def test_comm_algs_to_sql(self):
        """lines 589, 596: LABEL_PROPAGATION and CONNECTED_COMPONENTS to_sql()."""
        from sqldim.core.query.dgm import LABEL_PROPAGATION, CONNECTED_COMPONENTS

        assert LABEL_PROPAGATION().to_sql() == "label_propagation_community()"
        assert CONNECTED_COMPONENTS().to_sql() == "connected_components()"

    def test_where_called_twice_chains_and(self):
        """line 903: second where() call wraps existing pred in AND(existing, new)."""
        p1 = ScalarPred(PropRef("f.amount"), ">", 0)
        p2 = ScalarPred(PropRef("f.qty"), "<", 100)
        q = DGMQuery().anchor("fact_t", "f").where(p1).where(p2)
        assert isinstance(q._where_pred, AND)
        sql = q.to_sql()
        assert "f.amount" in sql
        assert "f.qty" in sql


# ---------------------------------------------------------------------------
# § New DGM features — Strategy, Quantifier, PathAgg, GraphExpr (red-green TDD)
# ---------------------------------------------------------------------------


class TestStrategyTypes:
    """DGM §18.4 — explicit path strategy (ALL / SHORTEST / K_SHORTEST / MIN_WEIGHT)."""

    def test_imports_succeed(self):
        from sqldim.core.query.dgm import ALL, SHORTEST, K_SHORTEST, MIN_WEIGHT, Strategy  # noqa: F401

    def test_all_is_strategy(self):
        from sqldim.core.query.dgm import ALL, Strategy
        assert isinstance(ALL(), Strategy)

    def test_shortest_is_strategy(self):
        from sqldim.core.query.dgm import SHORTEST, Strategy
        assert isinstance(SHORTEST(), Strategy)

    def test_k_shortest_carries_k(self):
        from sqldim.core.query.dgm import K_SHORTEST
        ks = K_SHORTEST(5)
        assert ks.k == 5

    def test_k_shortest_k_1(self):
        from sqldim.core.query.dgm import K_SHORTEST
        assert K_SHORTEST(1).k == 1

    def test_min_weight_is_strategy(self):
        from sqldim.core.query.dgm import MIN_WEIGHT, Strategy
        assert isinstance(MIN_WEIGHT(), Strategy)

    def test_all_is_not_k_shortest(self):
        from sqldim.core.query.dgm import ALL, K_SHORTEST
        assert not isinstance(ALL(), K_SHORTEST)


class TestQuantifier:
    """DGM §18.3 — explicit quantifier (EXISTS / FORALL)."""

    def test_exists_value(self):
        from sqldim.core.query.dgm import Quantifier
        assert Quantifier.EXISTS.value == "EXISTS"

    def test_forall_value(self):
        from sqldim.core.query.dgm import Quantifier
        assert Quantifier.FORALL.value == "FORALL"

    def test_enum_members_complete(self):
        from sqldim.core.query.dgm import Quantifier
        assert {q.value for q in Quantifier} == {"EXISTS", "FORALL"}


class TestPathPredWithQuantifierStrategy:
    """PathPred gains optional quantifier= and strategy= (§18.3 / §18.4)."""

    def test_default_quantifier_is_none(self):
        from sqldim.core.query.dgm import RawPred
        hop = VerbHop("c", "lbl", "s", table="t", on="c.id = s.fk")
        pp = PathPred("c", hop, RawPred("1=1"))
        assert pp.quantifier is None

    def test_default_strategy_is_none(self):
        from sqldim.core.query.dgm import RawPred
        hop = VerbHop("c", "lbl", "s", table="t", on="c.id = s.fk")
        pp = PathPred("c", hop, RawPred("1=1"))
        assert pp.strategy is None

    def test_exists_quantifier_stored(self):
        from sqldim.core.query.dgm import Quantifier, ALL, RawPred
        hop = VerbHop("c", "lbl", "s", table="t", on="c.id = s.fk")
        pp = PathPred("c", hop, RawPred("1=1"),
                      quantifier=Quantifier.EXISTS, strategy=ALL())
        assert pp.quantifier is Quantifier.EXISTS

    def test_forall_quantifier_and_k_shortest(self):
        from sqldim.core.query.dgm import Quantifier, K_SHORTEST, RawPred
        hop = VerbHop("c", "lbl", "s", table="t", on="c.id = s.fk")
        pp = PathPred("c", hop, RawPred("1=1"),
                      quantifier=Quantifier.FORALL, strategy=K_SHORTEST(3))
        assert pp.quantifier is Quantifier.FORALL
        assert pp.strategy.k == 3

    def test_strategy_stored(self):
        from sqldim.core.query.dgm import SHORTEST, RawPred
        hop = VerbHop("c", "lbl", "s", table="t", on="c.id = s.fk")
        pp = PathPred("c", hop, RawPred("1=1"), strategy=SHORTEST())
        assert isinstance(pp.strategy, SHORTEST)

    def test_to_sql_backward_compat(self):
        """Existing to_sql() still works when quantifier/strategy absent."""
        hop = VerbHop("c", "lbl", "s", table="dim_s", on="c.id = s.fk")
        pp = PathPred("c", hop, ScalarPred(PropRef("s.x"), "=", 1))
        sql = pp.to_sql()
        assert "EXISTS" in sql


class TestPathAgg:
    """PathAgg — path-traversal aggregation with explicit strategy (§16.2)."""

    def test_path_agg_attributes(self):
        from sqldim.core.query.dgm import PathAgg, ALL
        hop = VerbHop("c", "placed", "s", table="fact_s", on="c.id = s.cid")
        pa = PathAgg("c", hop, ALL(), "SUM", "s.revenue")
        assert pa.fn == "SUM"
        assert pa.ref == "s.revenue"
        assert isinstance(pa.strategy, ALL)

    def test_path_agg_to_sql_contains_fn(self):
        from sqldim.core.query.dgm import PathAgg, SHORTEST
        hop = VerbHop("c", "placed", "s", table="fact_s", on="c.id = s.cid")
        sql = PathAgg("c", hop, SHORTEST(), "AVG", "s.amount").to_sql()
        assert "AVG" in sql

    def test_path_agg_to_sql_contains_ref(self):
        from sqldim.core.query.dgm import PathAgg, MIN_WEIGHT
        hop = VerbHop("c", "placed", "s", table="fact_s", on="c.id = s.cid")
        sql = PathAgg("c", hop, MIN_WEIGHT(), "MAX", "s.weight").to_sql()
        assert "s.weight" in sql

    def test_path_agg_str_representable(self):
        from sqldim.core.query.dgm import PathAgg, K_SHORTEST
        hop = VerbHop("c", "placed", "s", table="fact_s", on="c.id = s.cid")
        pa = PathAgg("c", hop, K_SHORTEST(2), "COUNT", "s.id")
        assert isinstance(pa.to_sql(), str)


class TestGraphAlgorithmHierarchy:
    """GraphAlgorithm type hierarchy — §16.4 Phase 4."""

    def test_page_rank_is_node_alg_and_graph_algorithm(self):
        from sqldim.core.query.dgm import PAGE_RANK, NodeAlg, GraphAlgorithm
        pr = PAGE_RANK(damping=0.85, iterations=20)
        assert isinstance(pr, NodeAlg)
        assert isinstance(pr, GraphAlgorithm)

    def test_page_rank_parameters(self):
        from sqldim.core.query.dgm import PAGE_RANK
        pr = PAGE_RANK(damping=0.7, iterations=10)
        assert pr.damping == 0.7
        assert pr.iterations == 10

    def test_betweenness_centrality_is_node_alg(self):
        from sqldim.core.query.dgm import BETWEENNESS_CENTRALITY, NodeAlg
        assert isinstance(BETWEENNESS_CENTRALITY(), NodeAlg)

    def test_closeness_centrality_is_node_alg(self):
        from sqldim.core.query.dgm import CLOSENESS_CENTRALITY, NodeAlg
        assert isinstance(CLOSENESS_CENTRALITY(), NodeAlg)

    def test_degree_is_node_alg(self):
        from sqldim.core.query.dgm import DEGREE, NodeAlg
        assert isinstance(DEGREE(), NodeAlg)

    def test_community_label_default_uses_louvain(self):
        from sqldim.core.query.dgm import COMMUNITY_LABEL, LOUVAIN
        cl = COMMUNITY_LABEL()
        assert isinstance(cl.algorithm, LOUVAIN)

    def test_community_label_accepts_louvain(self):
        from sqldim.core.query.dgm import COMMUNITY_LABEL, LOUVAIN, CommAlg
        cl = COMMUNITY_LABEL(LOUVAIN())
        assert isinstance(cl.algorithm, CommAlg)

    def test_louvain_is_comm_alg_and_node_alg(self):
        from sqldim.core.query.dgm import LOUVAIN, CommAlg, NodeAlg
        assert isinstance(LOUVAIN(), CommAlg)
        assert isinstance(LOUVAIN(), NodeAlg)

    def test_label_propagation_is_comm_alg(self):
        from sqldim.core.query.dgm import LABEL_PROPAGATION, CommAlg
        assert isinstance(LABEL_PROPAGATION(), CommAlg)

    def test_connected_components_is_comm_alg(self):
        from sqldim.core.query.dgm import CONNECTED_COMPONENTS, CommAlg
        assert isinstance(CONNECTED_COMPONENTS(), CommAlg)

    def test_shortest_path_length_is_pair_alg(self):
        from sqldim.core.query.dgm import SHORTEST_PATH_LENGTH, PairAlg, GraphAlgorithm
        spl = SHORTEST_PATH_LENGTH()
        assert isinstance(spl, PairAlg)
        assert isinstance(spl, GraphAlgorithm)

    def test_min_weight_path_length_is_pair_alg(self):
        from sqldim.core.query.dgm import MIN_WEIGHT_PATH_LENGTH, PairAlg
        assert isinstance(MIN_WEIGHT_PATH_LENGTH(), PairAlg)

    def test_reachable_is_pair_alg(self):
        from sqldim.core.query.dgm import REACHABLE, PairAlg
        assert isinstance(REACHABLE(), PairAlg)

    def test_density_is_subgraph_alg(self):
        from sqldim.core.query.dgm import DENSITY, SubgraphAlg, GraphAlgorithm
        assert isinstance(DENSITY(), SubgraphAlg)
        assert isinstance(DENSITY(), GraphAlgorithm)

    def test_diameter_is_subgraph_alg(self):
        from sqldim.core.query.dgm import DIAMETER, SubgraphAlg
        assert isinstance(DIAMETER(), SubgraphAlg)

    def test_max_flow_fields_and_type(self):
        from sqldim.core.query.dgm import MAX_FLOW, SubgraphAlg
        mf = MAX_FLOW(source="a", sink="b")
        assert mf.source == "a"
        assert mf.sink == "b"
        assert isinstance(mf, SubgraphAlg)

    def test_all_node_algs_have_to_sql(self):
        from sqldim.core.query.dgm import (
            PAGE_RANK, BETWEENNESS_CENTRALITY, CLOSENESS_CENTRALITY,
            DEGREE, COMMUNITY_LABEL, LOUVAIN,
        )
        for cls in [BETWEENNESS_CENTRALITY, CLOSENESS_CENTRALITY, DEGREE, LOUVAIN]:
            assert isinstance(cls().to_sql(), str)
        assert isinstance(PAGE_RANK(damping=0.85, iterations=20).to_sql(), str)
        assert isinstance(COMMUNITY_LABEL().to_sql(), str)

    def test_all_pair_algs_have_to_sql(self):
        from sqldim.core.query.dgm import (
            SHORTEST_PATH_LENGTH, MIN_WEIGHT_PATH_LENGTH, REACHABLE,
        )
        for cls in [SHORTEST_PATH_LENGTH, MIN_WEIGHT_PATH_LENGTH, REACHABLE]:
            assert isinstance(cls().to_sql(), str)

    def test_subgraph_algs_have_to_sql(self):
        from sqldim.core.query.dgm import DENSITY, DIAMETER, MAX_FLOW
        for inst in [DENSITY(), DIAMETER(), MAX_FLOW(source="s", sink="t")]:
            assert isinstance(inst.to_sql(), str)


class TestGraphExpr:
    """GraphExpr — dimensionally-grounded graph algorithm expressions (§11)."""

    def test_imports_succeed(self):
        from sqldim.core.query.dgm import (  # noqa: F401
            GraphExpr, NodeExpr, PairExpr, SubgraphExpr,
        )

    def test_node_expr_to_sql_is_str(self):
        from sqldim.core.query.dgm import GraphExpr, NodeExpr, PAGE_RANK
        ge = GraphExpr(NodeExpr(PAGE_RANK(damping=0.85, iterations=20), "c"))
        assert isinstance(ge.to_sql(), str)

    def test_pair_expr_to_sql_is_str(self):
        from sqldim.core.query.dgm import GraphExpr, PairExpr, SHORTEST_PATH_LENGTH
        ge = GraphExpr(PairExpr(SHORTEST_PATH_LENGTH(), "src", "tgt"))
        assert isinstance(ge.to_sql(), str)

    def test_subgraph_expr_to_sql_is_str(self):
        from sqldim.core.query.dgm import GraphExpr, SubgraphExpr, DENSITY
        ge = GraphExpr(SubgraphExpr(DENSITY()))
        assert isinstance(ge.to_sql(), str)

    def test_graph_expr_inner_accessible(self):
        from sqldim.core.query.dgm import GraphExpr, NodeExpr, BETWEENNESS_CENTRALITY
        inner = NodeExpr(BETWEENNESS_CENTRALITY(), "x")
        ge = GraphExpr(inner)
        assert ge.inner is inner

    def test_community_label_node_expr_sql(self):
        from sqldim.core.query.dgm import GraphExpr, NodeExpr, COMMUNITY_LABEL, LOUVAIN
        ge = GraphExpr(NodeExpr(COMMUNITY_LABEL(LOUVAIN()), "c"))
        sql = ge.to_sql()
        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_pair_expr_src_tgt_stored(self):
        from sqldim.core.query.dgm import PairExpr, REACHABLE
        pe = PairExpr(REACHABLE(), "a", "b")
        assert pe.src == "a"
        assert pe.tgt == "b"

    def test_node_expr_alias_stored(self):
        from sqldim.core.query.dgm import NodeExpr, DEGREE
        ne = NodeExpr(DEGREE(), "v")
        assert ne.alias == "v"


# ---------------------------------------------------------------------------
# § TARJAN_SCC — new CommAlg for directed SCC (DGM §8.3, §18.7)
# ---------------------------------------------------------------------------

class TestTarjanSCC:
    """TARJAN_SCC — directed strongly-connected-components algorithm."""

    def test_tarjan_scc_is_comm_alg(self):
        from sqldim.core.query.dgm import TARJAN_SCC, CommAlg
        assert isinstance(TARJAN_SCC(), CommAlg)

    def test_tarjan_scc_is_node_alg(self):
        from sqldim.core.query.dgm import TARJAN_SCC, NodeAlg
        from sqldim.core.query.dgm import NodeAlg
        assert isinstance(TARJAN_SCC(), NodeAlg)

    def test_tarjan_scc_is_graph_algorithm(self):
        from sqldim.core.query.dgm import TARJAN_SCC, GraphAlgorithm
        assert isinstance(TARJAN_SCC(), GraphAlgorithm)

    def test_tarjan_scc_to_sql(self):
        from sqldim.core.query.dgm import TARJAN_SCC
        assert TARJAN_SCC().to_sql() == "tarjan_scc()"

    def test_community_label_with_tarjan_scc(self):
        from sqldim.core.query.dgm import COMMUNITY_LABEL, TARJAN_SCC
        cl = COMMUNITY_LABEL(TARJAN_SCC())
        sql = cl.to_sql()
        assert "tarjan_scc" in sql

    def test_community_label_tarjan_scc_node_expr(self):
        from sqldim.core.query.dgm import GraphExpr, NodeExpr, COMMUNITY_LABEL, TARJAN_SCC
        ge = GraphExpr(NodeExpr(COMMUNITY_LABEL(TARJAN_SCC()), "s"))
        sql = ge.to_sql()
        assert "tarjan_scc" in sql
        assert isinstance(sql, str)

    def test_tarjan_scc_distinct_from_connected_components(self):
        """TARJAN_SCC respects edge direction; CONNECTED_COMPONENTS does not."""
        from sqldim.core.query.dgm import TARJAN_SCC, CONNECTED_COMPONENTS
        assert TARJAN_SCC().to_sql() != CONNECTED_COMPONENTS().to_sql()


# ---------------------------------------------------------------------------
# § DEGREE(direction) — directional degree parameter (DGM §8.3)
# ---------------------------------------------------------------------------

class TestDegreeDirection:
    """DEGREE now accepts an explicit direction: IN, OUT, or BOTH."""

    def test_degree_default_is_both(self):
        from sqldim.core.query.dgm import DEGREE
        d = DEGREE()
        assert d.direction == "BOTH"

    def test_degree_in_direction(self):
        from sqldim.core.query.dgm import DEGREE
        d = DEGREE(direction="IN")
        assert d.direction == "IN"
        assert "IN" in d.to_sql()

    def test_degree_out_direction(self):
        from sqldim.core.query.dgm import DEGREE
        d = DEGREE(direction="OUT")
        assert d.direction == "OUT"
        assert "OUT" in d.to_sql()

    def test_degree_both_in_sql(self):
        from sqldim.core.query.dgm import DEGREE
        sql = DEGREE(direction="BOTH").to_sql()
        assert "BOTH" in sql

    def test_degree_to_sql_is_string(self):
        from sqldim.core.query.dgm import DEGREE
        assert isinstance(DEGREE().to_sql(), str)


# ---------------------------------------------------------------------------
# § ArithExpr — arithmetic expression in the expression language (DGM §8)
# ---------------------------------------------------------------------------

class TestArithExpr:
    """ArithExpr(left, op, right) — arithmetic combine refs in expressions."""

    def test_arith_expr_multiply(self):
        from sqldim.core.query.dgm import ArithExpr, PropRef
        expr = ArithExpr(PropRef("s", "revenue"), "*", PropRef("promo", "weight"))
        sql = expr.to_sql()
        assert "s.revenue" in sql
        assert "promo.weight" in sql
        assert "*" in sql

    def test_arith_expr_add(self):
        from sqldim.core.query.dgm import ArithExpr, PropRef
        expr = ArithExpr(PropRef("a", "x"), "+", PropRef("b", "y"))
        sql = expr.to_sql()
        assert "+" in sql
        assert "a.x" in sql
        assert "b.y" in sql

    def test_arith_expr_subtract(self):
        from sqldim.core.query.dgm import ArithExpr, PropRef
        expr = ArithExpr(PropRef("a", "total"), "-", PropRef("b", "cost"))
        sql = expr.to_sql()
        assert "-" in sql

    def test_arith_expr_divide(self):
        from sqldim.core.query.dgm import ArithExpr, PropRef
        expr = ArithExpr(PropRef("a", "total"), "/", PropRef("b", "count"))
        sql = expr.to_sql()
        assert "/" in sql

    def test_arith_expr_nested(self):
        """Nested ArithExpr: (a.x + b.y) * c.z."""
        from sqldim.core.query.dgm import ArithExpr, PropRef
        inner = ArithExpr(PropRef("a", "x"), "+", PropRef("b", "y"))
        outer = ArithExpr(inner, "*", PropRef("c", "z"))
        sql = outer.to_sql()
        assert "a.x" in sql
        assert "b.y" in sql
        assert "c.z" in sql
        assert "*" in sql

    def test_arith_expr_with_const(self):
        """ArithExpr can incorporate raw SQL strings (e.g. numeric literals)."""
        from sqldim.core.query.dgm import ArithExpr, PropRef
        from sqldim.core.query.dgm.refs import _ConstExpr
        expr = ArithExpr(PropRef("s", "revenue"), "*", _ConstExpr(1.5))
        sql = expr.to_sql()
        assert "s.revenue" in sql
        assert "1.5" in sql


# ---------------------------------------------------------------------------
# § SubgraphExpr with partition (DGM §8.3, §18.9)
# ---------------------------------------------------------------------------

class TestSubgraphExprPartition:
    """SubgraphExpr now accepts an optional partition=[PropRef, ...] arg."""

    def test_subgraph_expr_no_partition(self):
        from sqldim.core.query.dgm import SubgraphExpr, DENSITY
        se = SubgraphExpr(DENSITY())
        assert se.partition is None

    def test_subgraph_expr_with_partition(self):
        from sqldim.core.query.dgm import SubgraphExpr, DENSITY, PropRef
        se = SubgraphExpr(DENSITY(), partition=[PropRef("scc", "label")])
        assert se.partition is not None
        assert len(se.partition) == 1

    def test_subgraph_expr_partition_in_sql(self):
        from sqldim.core.query.dgm import SubgraphExpr, DENSITY, PropRef
        se = SubgraphExpr(DENSITY(), partition=[PropRef("s", "scc_label")])
        sql = se.to_sql()
        assert "scc_label" in sql

    def test_subgraph_expr_no_partition_backward_compat(self):
        """Original SubgraphExpr(DENSITY()) form still works."""
        from sqldim.core.query.dgm import SubgraphExpr, DENSITY, GraphExpr
        ge = GraphExpr(SubgraphExpr(DENSITY()))
        assert isinstance(ge.to_sql(), str)

    def test_subgraph_expr_diameter_with_partition(self):
        from sqldim.core.query.dgm import SubgraphExpr, DIAMETER, PropRef
        se = SubgraphExpr(DIAMETER(), partition=[PropRef("c", "region"), PropRef("c", "segment")])
        assert len(se.partition) == 2
        sql = se.to_sql()
        assert "region" in sql


# ---------------------------------------------------------------------------
# § TrimJoin + TrimCriterion (DGM §10.1, §18.8)
# ---------------------------------------------------------------------------

class TestTrimCriterion:
    """TrimCriterion variants: REACHABLE_BETWEEN, MIN_DEGREE, SINK_FREE, SOURCE_FREE."""

    def test_sink_free_is_trim_criterion(self):
        from sqldim.core.query.dgm import SINK_FREE, TrimCriterion
        assert isinstance(SINK_FREE(), TrimCriterion)

    def test_source_free_is_trim_criterion(self):
        from sqldim.core.query.dgm import SOURCE_FREE, TrimCriterion
        assert isinstance(SOURCE_FREE(), TrimCriterion)

    def test_sink_free_to_sql(self):
        from sqldim.core.query.dgm import SINK_FREE
        sql = SINK_FREE().to_sql()
        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_source_free_to_sql(self):
        from sqldim.core.query.dgm import SOURCE_FREE
        sql = SOURCE_FREE().to_sql()
        assert isinstance(sql, str)

    def test_min_degree_stores_params(self):
        from sqldim.core.query.dgm import MIN_DEGREE, TrimCriterion
        md = MIN_DEGREE(n=1, direction="OUT")
        assert md.n == 1
        assert md.direction == "OUT"
        assert isinstance(md, TrimCriterion)

    def test_min_degree_to_sql(self):
        from sqldim.core.query.dgm import MIN_DEGREE
        sql = MIN_DEGREE(n=2, direction="IN").to_sql()
        assert "2" in sql
        assert "IN" in sql

    def test_reachable_between_stores_params(self):
        from sqldim.core.query.dgm import REACHABLE_BETWEEN, TrimCriterion
        rb = REACHABLE_BETWEEN(source="c", target="st")
        assert rb.source == "c"
        assert rb.target == "st"
        assert isinstance(rb, TrimCriterion)

    def test_reachable_between_to_sql(self):
        from sqldim.core.query.dgm import REACHABLE_BETWEEN
        sql = REACHABLE_BETWEEN(source="c", target="st").to_sql()
        assert "c" in sql
        assert "st" in sql


class TestTrimJoin:
    """TrimJoin wraps a Join expression with a TrimCriterion."""

    def test_trim_join_stores_join_and_criterion(self):
        from sqldim.core.query.dgm import TrimJoin, SINK_FREE
        sentinel = object()
        tj = TrimJoin(sentinel, SINK_FREE())
        assert tj.join is sentinel
        assert isinstance(tj.criterion, SINK_FREE)

    def test_trim_join_nested(self):
        from sqldim.core.query.dgm import TrimJoin, SINK_FREE, REACHABLE_BETWEEN
        inner = TrimJoin(object(), REACHABLE_BETWEEN("c", "st"))
        outer = TrimJoin(inner, SINK_FREE())
        assert isinstance(outer.join, TrimJoin)
        assert isinstance(outer.criterion, SINK_FREE)

    def test_trim_join_min_degree(self):
        from sqldim.core.query.dgm import TrimJoin, MIN_DEGREE
        tj = TrimJoin(object(), MIN_DEGREE(n=1, direction="OUT"))
        assert isinstance(tj.criterion, MIN_DEGREE)
        assert tj.criterion.n == 1

    def test_trim_join_source_free(self):
        from sqldim.core.query.dgm import TrimJoin, SOURCE_FREE
        tj = TrimJoin(object(), SOURCE_FREE())
        assert isinstance(tj.criterion, SOURCE_FREE)

    def test_trim_join_to_sql(self):
        from sqldim.core.query.dgm import TrimJoin, MIN_DEGREE
        tj = TrimJoin(object(), MIN_DEGREE(n=1, direction="BOTH"))
        sql = tj.to_sql()
        assert isinstance(sql, str)
        assert "MIN_DEGREE" in sql or "min_degree" in sql


# ---------------------------------------------------------------------------
# § Coverage — all new alg to_sql() + helpers
# ---------------------------------------------------------------------------

class TestNewCoverage:
    """Ensure all new types hit their to_sql() branches."""

    def test_tarjan_scc_to_sql_branch(self):
        from sqldim.core.query.dgm import TARJAN_SCC
        assert TARJAN_SCC().to_sql() == "tarjan_scc()"

    def test_degree_direction_all_values(self):
        from sqldim.core.query.dgm import DEGREE
        for d in ("IN", "OUT", "BOTH"):
            sql = DEGREE(direction=d).to_sql()
            assert d in sql

    def test_reachable_between_to_sql_coverage(self):
        from sqldim.core.query.dgm import REACHABLE_BETWEEN
        sql = REACHABLE_BETWEEN("alpha", "beta").to_sql()
        assert "alpha" in sql and "beta" in sql

    def test_min_degree_to_sql_coverage(self):
        from sqldim.core.query.dgm import MIN_DEGREE
        sql = MIN_DEGREE(n=3, direction="IN").to_sql()
        assert "3" in sql

    def test_arith_expr_op_stored(self):
        from sqldim.core.query.dgm import ArithExpr, PropRef
        e = ArithExpr(PropRef("a", "x"), "-", PropRef("b", "y"))
        assert e.op == "-"

    def test_subgraph_expr_empty_partition_to_sql(self):
        from sqldim.core.query.dgm import SubgraphExpr, DENSITY
        se = SubgraphExpr(DENSITY(), partition=[])
        sql = se.to_sql()
        assert isinstance(sql, str)
