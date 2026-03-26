"""
Tests for sqldim/examples/datasets/ — BaseSource, DatasetFactory,
SourceProvider, and all 8 registered OLTP source simulators.

Coverage targets
----------------
sqldim/examples/datasets/__init__.py
sqldim/examples/datasets/base.py
sqldim/examples/datasets/ecommerce.py
sqldim/examples/datasets/enterprise.py
sqldim/examples/datasets/media.py
sqldim/examples/datasets/devops.py
"""

from __future__ import annotations
import pytest
import duckdb

# Import the public API — this also triggers all @DatasetFactory.register() calls
from sqldim.application.datasets import (
    BaseSource,
    DatasetFactory,
    SourceProvider,
    ProductsSource,
    CustomersSource,
    StoresSource,
    OrdersSource,
    EmployeesSource,
    AccountsSource,
    MoviesSource,
    GitHubIssuesSource,
)


# ---------------------------------------------------------------------------
# SourceProvider
# ---------------------------------------------------------------------------


class TestSourceProvider:
    def test_describe_with_all_fields(self):
        sp = SourceProvider(
            name="Test System",
            description="A test provider",
            url="https://example.com",
            auth_required=True,
            requires=["oauth2"],
        )
        desc = sp.describe()
        assert "Test System" in desc
        assert "A test provider" in desc
        assert "https://example.com" in desc

    def test_describe_minimal(self):
        sp = SourceProvider(name="Minimal")
        desc = sp.describe()
        assert "Minimal" in desc

    def test_describe_no_auth(self):
        sp = SourceProvider(name="Public", auth_required=False)
        desc = sp.describe()
        assert "Public" in desc

    def test_describe_with_requires(self):
        sp = SourceProvider(name="X", requires=["dep1", "dep2"])
        desc = sp.describe()
        assert "dep1" in desc or "requires" in desc.lower()


# ---------------------------------------------------------------------------
# BaseSource (abstract interface)
# ---------------------------------------------------------------------------


class TestBaseSource:
    def test_subclass_must_implement_snapshot(self):
        with pytest.raises(TypeError):
            BaseSource()  # abstract class

    def test_concrete_subclass_works(self):
        class MySource(BaseSource):
            DIM_DDL = "CREATE TABLE IF NOT EXISTS {table} (id INTEGER)"

            def snapshot(self):
                return None

        src = MySource()
        assert src is not None

    def test_default_setup_creates_table(self):
        class MySource(BaseSource):
            DIM_DDL = "CREATE TABLE IF NOT EXISTS {table} (id INTEGER, name VARCHAR)"

            def snapshot(self):
                return None

        con = duckdb.connect()
        src = MySource()
        src.setup(con, "test_dim")
        # Table should exist
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "test_dim" in tables

    def test_default_teardown_drops_table(self):
        class MySource(BaseSource):
            DIM_DDL = "CREATE TABLE IF NOT EXISTS {table} (id INTEGER)"

            def snapshot(self):
                return None

        con = duckdb.connect()
        src = MySource()
        src.setup(con, "test_drop")
        src.teardown(con, "test_drop")
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "test_drop" not in tables

    def test_describe_provider_no_provider(self):
        class MySource(BaseSource):
            DIM_DDL = ""
            provider = None

            def snapshot(self):
                return None

        src = MySource()
        desc = src.describe_provider()
        assert "synthetic" in desc.lower() or "MySource" in desc

    def test_describe_provider_with_provider(self):
        class MySource(BaseSource):
            DIM_DDL = ""
            provider = SourceProvider(name="Test System")

            def snapshot(self):
                return None

        src = MySource()
        desc = src.describe_provider()
        assert "Test System" in desc

    def test_event_batch_raises_not_implemented(self):
        class MySource(BaseSource):
            DIM_DDL = ""

            def snapshot(self):
                return None

        src = MySource()
        with pytest.raises(NotImplementedError):
            src.event_batch()


# ---------------------------------------------------------------------------
# DatasetFactory
# ---------------------------------------------------------------------------


class TestDatasetFactory:
    def test_available_returns_all_registered(self):
        available = DatasetFactory.available()
        for name in (
            "products",
            "customers",
            "stores",
            "orders",
            "employees",
            "accounts",
            "movies",
            "github_issues",
        ):
            assert name in available

    def test_create_products(self):
        src = DatasetFactory.create("products", n=3, seed=7)
        assert isinstance(src, ProductsSource)

    def test_create_unknown_raises(self):
        with pytest.raises(KeyError, match="not_a_dataset"):
            DatasetFactory.create("not_a_dataset")

    def test_setup_all(self):
        con = duckdb.connect()
        specs = [
            (ProductsSource(n=2), "dim_products"),
            (CustomersSource(n=2), "dim_customers"),
        ]
        DatasetFactory.setup_all(con, specs)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dim_products" in tables
        assert "dim_customers" in tables

    def test_teardown_all(self):
        con = duckdb.connect()
        src = ProductsSource(n=2)
        specs = [(src, "dim_teardown")]
        DatasetFactory.setup_all(con, specs)
        DatasetFactory.teardown_all(con, specs)
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dim_teardown" not in tables


# ---------------------------------------------------------------------------
# ProductsSource
# ---------------------------------------------------------------------------


class TestProductsSource:
    def test_snapshot_returns_sql_source(self):
        from sqldim.sources.batch.sql import SQLSource

        src = ProductsSource(n=5, seed=42)
        snap = src.snapshot()
        assert isinstance(snap, SQLSource)

    def test_event_batch_returns_sql_source(self):
        from sqldim.sources.batch.sql import SQLSource

        src = ProductsSource(n=5, seed=42)
        batch = src.event_batch(1)
        assert isinstance(batch, SQLSource)

    def test_snapshot_has_correct_rows(self):
        src = ProductsSource(n=5, seed=42)
        con = duckdb.connect()
        snap = src.snapshot()
        rows = con.execute(snap.as_sql(con)).fetchdf()
        assert len(rows) == 5

    def test_setup_and_teardown(self):
        con = duckdb.connect()
        src = ProductsSource(n=3)
        src.setup(con, "dim_prod")
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dim_prod" in tables
        src.teardown(con, "dim_prod")
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dim_prod" not in tables

    def test_provider_is_set(self):
        src = ProductsSource()
        assert src.provider is not None
        assert isinstance(src.provider, SourceProvider)

    def test_initial_and_events(self):
        src = ProductsSource(n=3, seed=1)
        assert len(src.initial) == 3
        assert len(src.events) > 0


# ---------------------------------------------------------------------------
# CustomersSource
# ---------------------------------------------------------------------------


class TestCustomersSource:
    def test_snapshot_not_empty(self):
        src = CustomersSource(n=4, seed=42)
        con = duckdb.connect()
        rows = con.execute(src.snapshot().as_sql(con)).fetchdf()
        assert len(rows) == 4

    def test_event_batch_not_empty(self):
        src = CustomersSource(n=4, seed=42)
        con = duckdb.connect()
        rows = con.execute(src.event_batch(1).as_sql(con)).fetchdf()
        assert len(rows) > 0

    def test_moved_count(self):
        src = CustomersSource(n=10, seed=5)
        assert src.moved_count() >= 0

    def test_provider_set(self):
        assert CustomersSource().provider is not None

    def test_initial_and_events(self):
        src = CustomersSource(n=4, seed=2)
        assert len(src.initial) == 4
        assert len(src.events) > 0


# ---------------------------------------------------------------------------
# StoresSource
# ---------------------------------------------------------------------------


class TestStoresSource:
    def test_snapshot_not_empty(self):
        src = StoresSource(n=3, seed=42)
        con = duckdb.connect()
        rows = con.execute(src.snapshot().as_sql(con)).fetchdf()
        assert len(rows) == 3

    def test_event_batch(self):
        src = StoresSource(n=3, seed=42)
        con = duckdb.connect()
        rows = con.execute(src.event_batch(1).as_sql(con)).fetchdf()
        assert len(rows) >= 0

    def test_provider_set(self):
        assert StoresSource().provider is not None

    def test_initial_and_events(self):
        src = StoresSource(n=3, seed=3)
        assert len(src.initial) == 3
        assert len(src.events) > 0


# ---------------------------------------------------------------------------
# OrdersSource
# ---------------------------------------------------------------------------


class TestOrdersSource:
    def test_snapshot(self):
        from sqldim.sources.batch.sql import SQLSource

        src = OrdersSource(n=4, seed=42)
        assert isinstance(src.snapshot(), SQLSource)

    def test_placed_events(self):
        src = OrdersSource(n=4, seed=42)
        con = duckdb.connect()
        rows = con.execute(src.placed_events().as_sql(con)).fetchdf()
        assert len(rows) == 4

    def test_paid_events(self):
        src = OrdersSource(n=4, seed=42)
        con = duckdb.connect()
        rows = con.execute(src.paid_events().as_sql(con)).fetchdf()
        assert len(rows) > 0

    def test_shipped_events(self):
        src = OrdersSource(n=4, seed=42)
        con = duckdb.connect()
        rows = con.execute(src.shipped_events().as_sql(con)).fetchdf()
        assert len(rows) > 0

    def test_provider_set(self):
        assert OrdersSource().provider is not None

    def test_setup(self):
        con = duckdb.connect()
        src = OrdersSource(n=2)
        src.setup(con, "dim_orders")
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dim_orders" in tables

    def test_oltp_ddl_property(self):
        src = OrdersSource(n=2)
        assert "order_id" in src.OLTP_DDL
        assert "{table}" in src.OLTP_DDL


# ---------------------------------------------------------------------------
# EmployeesSource
# ---------------------------------------------------------------------------


class TestEmployeesSource:
    def test_snapshot(self):
        src = EmployeesSource(n=4, seed=42)
        con = duckdb.connect()
        rows = con.execute(src.snapshot().as_sql(con)).fetchdf()
        assert len(rows) == 4

    def test_event_batch(self):
        src = EmployeesSource(n=4, seed=42)
        con = duckdb.connect()
        rows = con.execute(src.event_batch(1).as_sql(con)).fetchdf()
        assert len(rows) > 0

    def test_provider_set(self):
        assert EmployeesSource().provider is not None

    def test_initial_events(self):
        src = EmployeesSource(n=3, seed=7)
        assert len(src.initial) == 3
        assert len(src.events) > 0


# ---------------------------------------------------------------------------
# AccountsSource
# ---------------------------------------------------------------------------


class TestAccountsSource:
    def test_snapshot(self):
        src = AccountsSource(n=3, seed=42)
        con = duckdb.connect()
        rows = con.execute(src.snapshot().as_sql(con)).fetchdf()
        assert len(rows) == 3

    def test_snapshot_for_date(self):
        src = AccountsSource(n=3, seed=42)
        con = duckdb.connect()
        rows = con.execute(src.snapshot_for_date("2024-06-01").as_sql(con)).fetchdf()
        assert len(rows) == 3

    def test_provider_set(self):
        assert AccountsSource().provider is not None

    def test_accounts_list(self):
        src = AccountsSource(n=4, seed=5)
        assert len(src.accounts) == 4

    def test_oltp_ddl_property(self):
        src = AccountsSource(n=2)
        assert "account_id" in src.OLTP_DDL
        assert "{table}" in src.OLTP_DDL


# ---------------------------------------------------------------------------
# MoviesSource
# ---------------------------------------------------------------------------


class TestMoviesSource:
    def test_snapshot_alias(self):
        from sqldim.sources.batch.sql import SQLSource

        src = MoviesSource(n_movies=3)
        assert isinstance(src.snapshot(), SQLSource)

    def test_cast_snapshot(self):
        src = MoviesSource(n_movies=3, n_actors=5, seed=42)
        con = duckdb.connect()
        rows = con.execute(src.cast_snapshot().as_sql(con)).fetchdf()
        assert len(rows) > 0

    def test_new_releases(self):
        src = MoviesSource(n_movies=3, seed=42)
        con = duckdb.connect()
        rows = con.execute(src.new_releases(1).as_sql(con)).fetchdf()
        assert len(rows) > 0

    def test_custom_setup_teardown(self):
        con = duckdb.connect()
        src = MoviesSource(n_actors=4, n_movies=3, seed=42)
        src.setup(con, "dim_edge", "dim_actors")
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dim_edge" in tables
        assert "dim_actors" in tables
        src.teardown(con, "dim_edge", "dim_actors")
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dim_edge" not in tables

    def test_provider_set(self):
        assert MoviesSource().provider is not None

    def test_actor_map(self):
        src = MoviesSource(n_actors=4, n_movies=3, seed=42)
        am = src.actor_map()
        assert len(am) == 5  # n_actors + 1 event actor

    def test_movies_list(self):
        src = MoviesSource(n_movies=5, seed=42)
        assert len(src.movies) == 6  # n_movies + 1 event movie

    def test_cast_list(self):
        src = MoviesSource(n_actors=4, n_movies=3, seed=42)
        assert len(src.cast) > 0

    def test_ddl_properties(self):
        src = MoviesSource(n_actors=4, n_movies=3, seed=42)
        assert "actor_id" in src.OLTP_DDL
        assert "subject_id" in src.EDGE_DDL
        assert "actor_id" not in src.EDGE_DDL
        assert "name" in src.ACTORS_DDL


# ---------------------------------------------------------------------------
# GitHubIssuesSource
# ---------------------------------------------------------------------------


class TestGitHubIssuesSource:
    def test_snapshot(self):
        from sqldim.sources.batch.sql import SQLSource

        src = GitHubIssuesSource(n=5, seed=42)
        assert isinstance(src.snapshot(), SQLSource)

    def test_event_batch(self):
        src = GitHubIssuesSource(n=5, seed=42)
        con = duckdb.connect()
        rows = con.execute(src.event_batch(1).as_sql(con)).fetchdf()
        assert len(rows) > 0

    def test_seed_staging(self, tmp_path):
        src = GitHubIssuesSource(n=5, seed=42)
        path = str(tmp_path / "staging.duckdb")
        src.seed_staging(path, "initial")
        con = duckdb.connect(path)
        tables = (
            con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'github_staging'"
            )
            .fetchdf()["table_name"]
            .tolist()
        )
        assert "issues" in tables

    def test_seed_staging_updated(self, tmp_path):
        src = GitHubIssuesSource(n=5, seed=42)
        path = str(tmp_path / "staging2.duckdb")
        src.seed_staging(path, "updated")
        con = duckdb.connect(path)
        tables = (
            con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'github_staging'"
            )
            .fetchdf()["table_name"]
            .tolist()
        )
        assert "issues" in tables

    def test_provider_set(self):
        assert GitHubIssuesSource().provider is not None

    def test_initial_and_events(self):
        src = GitHubIssuesSource(n=5, seed=42)
        assert len(src.initial) == 5
        assert len(src.events) > 0


# ── event_batch raises ValueError for n > 1 ──────────────────────────────────


class TestEventBatchValueError:
    def test_products_raises(self):
        from sqldim.application.datasets.domains.ecommerce import ProductsSource

        with pytest.raises(ValueError, match="1 event batch"):
            ProductsSource(n=3, seed=1).event_batch(2)

    def test_customers_raises(self):
        from sqldim.application.datasets.domains.ecommerce import CustomersSource

        with pytest.raises(ValueError, match="1 event batch"):
            CustomersSource(n=3, seed=1).event_batch(2)

    def test_stores_raises(self):
        from sqldim.application.datasets.domains.ecommerce import StoresSource

        with pytest.raises(ValueError, match="1 event batch"):
            StoresSource(n=3, seed=1).event_batch(2)

    def test_employees_raises(self):
        from sqldim.application.datasets.domains.enterprise import EmployeesSource

        with pytest.raises(ValueError, match="1 event batch"):
            EmployeesSource(n=3, seed=1).event_batch(2)

    def test_movies_raises(self):
        from sqldim.application.datasets.domains.media import MoviesSource

        with pytest.raises(ValueError, match="1 event batch"):
            MoviesSource().new_releases(2)

    def test_githubissues_raises(self):
        from sqldim.application.datasets.domains.devops import GitHubIssuesSource

        with pytest.raises(ValueError, match="1 event batch"):
            GitHubIssuesSource(n=3, seed=1).event_batch(2)


# ── BaseSource.setup() raises NotImplementedError when DIM_DDL is empty ──────


def test_base_source_setup_raises_not_implemented():
    import duckdb
    from sqldim.application.datasets.base import BaseSource

    class NoddlSource(BaseSource):
        def snapshot(self):
            pass

    with pytest.raises(NotImplementedError, match="DIM_DDL"):
        NoddlSource().setup(duckdb.connect(), "some_table")


# ── OrdersSource.orders property ─────────────────────────────────────────────


def test_orders_source_orders_property():
    from sqldim.application.datasets.domains.ecommerce import OrdersSource

    src = OrdersSource(n=3, seed=42)
    result = src.orders
    assert isinstance(result, list)
    assert len(result) == 3


# =============================================================================
# Schema primitives — FieldSpec, EntitySchema, ChangeRule, EventSpec
# =============================================================================


class TestFieldSpec:
    """Unit tests for every FieldSpec.kind and helper methods."""

    def _fake(self, seed=0):
        from faker import Faker
        import random

        f = Faker()
        Faker.seed(seed)
        random.seed(seed)
        return f

    def test_seq_default(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec("x", "INTEGER", kind="seq")
        fake = self._fake()
        assert spec.generate(fake, 0) == 1
        assert spec.generate(fake, 4) == 5

    def test_seq_start_step(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec("x", "INTEGER", kind="seq", start=10, step=10)
        fake = self._fake()
        assert spec.generate(fake, 0) == 10
        assert spec.generate(fake, 2) == 30

    def test_faker_method(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec("n", "VARCHAR", kind="faker", method="first_name")
        result = spec.generate(self._fake(), 0)
        assert isinstance(result, str) and len(result) > 0

    def test_faker_with_pattern(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec(
            "phone", "VARCHAR", kind="faker", method="numerify", pattern="555-####"
        )
        result = spec.generate(self._fake(), 0)
        assert result.startswith("555-")

    def test_faker_with_transform(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec("w", "VARCHAR", kind="faker", method="word", transform="upper")
        result = spec.generate(self._fake(), 0)
        assert result == result.upper()

    def test_choices(self):
        from sqldim.application.datasets.schema import FieldSpec

        opts = ["a", "b", "c"]
        spec = FieldSpec("v", "VARCHAR", kind="choices", choices=opts)
        result = spec.generate(self._fake(), 0)
        assert result in opts

    def test_uniform(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec("p", "DOUBLE", kind="uniform", low=1.0, high=2.0)
        result = spec.generate(self._fake(), 0)
        assert 1.0 <= result <= 2.0

    def test_uniform_with_precision(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec(
            "p", "DOUBLE", kind="uniform", low=0.0, high=100.0, precision=2
        )
        result = spec.generate(self._fake(), 0)
        assert round(result, 2) == result

    def test_randint(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec("n", "INTEGER", kind="randint", low=1, high=10)
        result = spec.generate(self._fake(), 0)
        assert 1 <= result <= 10

    def test_const(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec("ts", "TIMESTAMP", kind="const", value="2024-01-01")
        assert spec.generate(self._fake(), 0) == "2024-01-01"

    def test_computed(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec("v", "VARCHAR", kind="computed", fn=lambda fake, i: f"row-{i}")
        assert spec.generate(self._fake(), 3) == "row-3"

    def test_post_callable(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec(
            "v",
            "VARCHAR",
            kind="const",
            value="hello",
            post=lambda v, fake, i: v.upper(),
        )
        assert spec.generate(self._fake(), 0) == "HELLO"

    def test_unknown_kind_raises(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec("x", "INTEGER", kind="nonexistent")
        with pytest.raises(ValueError, match="Unknown FieldSpec kind"):
            spec.generate(self._fake(), 0)

    def test_as_literal_varchar(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec("s", "VARCHAR", kind="const", value="hello")
        assert spec.as_literal("it's") == "'it''s'"

    def test_as_literal_integer(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec("n", "INTEGER", kind="seq")
        assert spec.as_literal(42) == "42"

    def test_as_literal_none(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec("n", "INTEGER", kind="seq")
        assert spec.as_literal(None) == "NULL"

    def test_ddl_col(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec("product_id", "INTEGER", kind="seq")
        col = spec.ddl_col()
        assert "product_id" in col and "INTEGER" in col

    def test_sql_export_false_excluded_from_to_sql(self):
        from sqldim.application.datasets.schema import EntitySchema, FieldSpec

        schema = EntitySchema(
            "t",
            fields=[
                FieldSpec("a", "INTEGER", kind="seq"),
                FieldSpec("b", "TIMESTAMP", kind="const", value="ts", sql_export=False),
            ],
        )
        rows = schema.generate(1, self._fake())
        assert "b" in rows[0]  # present in row dict
        sql = schema.to_sql(rows)
        assert "b" not in sql  # absent from SQL SELECT


class TestEntitySchema:
    def _fake(self, seed=0):
        from faker import Faker
        import random

        f = Faker()
        Faker.seed(seed)
        random.seed(seed)
        return f

    def _simple_schema(self):
        from sqldim.application.datasets.schema import EntitySchema, FieldSpec

        return EntitySchema(
            "item",
            fields=[
                FieldSpec("item_id", "INTEGER", kind="seq"),
                FieldSpec("name", "VARCHAR", kind="const", value="widget"),
            ],
        )

    def test_oltp_ddl_contains_fields(self):
        schema = self._simple_schema()
        ddl = schema.oltp_ddl()
        assert "item_id" in ddl and "name" in ddl
        assert "valid_from" not in ddl

    def test_dim_ddl_has_scd_audit_cols(self):
        schema = self._simple_schema()
        ddl = schema.dim_ddl()
        for col in ("valid_from", "valid_to", "is_current", "checksum"):
            assert col in ddl

    def test_dim_ddl_with_dim_extra(self):
        from sqldim.application.datasets.schema import EntitySchema, FieldSpec

        schema = EntitySchema(
            "cust",
            fields=[FieldSpec("id", "INTEGER", kind="seq")],
            dim_extra=[("prev_city", "VARCHAR")],
        )
        ddl = schema.dim_ddl()
        assert "prev_city" in ddl
        # prev_city must appear before valid_from
        assert ddl.index("prev_city") < ddl.index("valid_from")

    def test_generate_returns_n_rows(self):
        rows = self._simple_schema().generate(5, self._fake())
        assert len(rows) == 5

    def test_generate_row_has_all_fields(self):
        row = self._simple_schema().generate_row(self._fake(), 0)
        assert set(row) == {"item_id", "name"}

    def test_to_sql_union_all(self):
        schema = self._simple_schema()
        rows = schema.generate(3, self._fake())
        sql = schema.to_sql(rows)
        assert sql.count("UNION ALL") == 2

    def test_to_sql_excludes_sql_export_false(self):
        from sqldim.application.datasets.schema import EntitySchema, FieldSpec

        schema = EntitySchema(
            "t",
            fields=[
                FieldSpec("id", "INTEGER", kind="seq"),
                FieldSpec(
                    "hidden", "TIMESTAMP", kind="const", value="x", sql_export=False
                ),
            ],
        )
        rows = schema.generate(1, self._fake())
        sql = schema.to_sql(rows)
        assert "id" in sql
        assert "hidden" not in sql

    def test_oltp_ddl_custom_table(self):
        ddl = self._simple_schema().oltp_ddl("my_table")
        assert "my_table" in ddl

    def test_dim_ddl_custom_table(self):
        ddl = self._simple_schema().dim_ddl("dim_item")
        assert "dim_item" in ddl


class TestEventSpec:
    def _fake(self, seed=0):
        from faker import Faker
        import random

        f = Faker()
        Faker.seed(seed)
        random.seed(seed)
        return f

    def _rows(self, n=4):
        return [{"id": i, "val": i * 10, "ts": "old"} for i in range(n)]

    def test_apply_mutates_matching_rows(self):
        from sqldim.application.datasets.schema import ChangeRule, EventSpec

        spec = EventSpec(
            changes=[
                ChangeRule(
                    "val",
                    condition=lambda i, r: i % 2 == 0,
                    mutate=lambda v, r, fake: v + 1,
                ),
            ]
        )
        result = spec.apply(self._rows(4), self._fake())
        assert len(result) == 2
        assert all(r["val"] % 10 == 1 for r in result)

    def test_apply_skips_unchanged(self):
        from sqldim.application.datasets.schema import ChangeRule, EventSpec

        spec = EventSpec(
            changes=[
                ChangeRule(
                    "val", condition=lambda i, r: False, mutate=lambda v, r, fake: v
                ),
            ]
        )
        assert spec.apply(self._rows(4), self._fake()) == []

    def test_apply_stamps_timestamp(self):
        from sqldim.application.datasets.schema import ChangeRule, EventSpec

        spec = EventSpec(
            changes=[
                ChangeRule(
                    "val",
                    condition=lambda i, r: i == 0,
                    mutate=lambda v, r, fake: v + 1,
                ),
            ],
            timestamp_field="ts",
            event_ts="2024-09-01",
        )
        result = spec.apply(self._rows(3), self._fake())
        assert result[0]["ts"] == "2024-09-01"

    def test_apply_no_timestamp_field(self):
        from sqldim.application.datasets.schema import ChangeRule, EventSpec

        spec = EventSpec(
            changes=[
                ChangeRule(
                    "val",
                    condition=lambda i, r: i == 0,
                    mutate=lambda v, r, fake: v + 1,
                ),
            ]
        )
        result = spec.apply(self._rows(2), self._fake())
        assert result[0]["ts"] == "old"  # unchanged

    def test_apply_new_rows_fn(self):
        from sqldim.application.datasets.schema import EventSpec

        spec = EventSpec(
            changes=[],
            new_rows_fn=lambda rows, fake: [{"id": 99, "val": 0, "ts": "new"}],
        )
        result = spec.apply(self._rows(2), self._fake())
        assert any(r["id"] == 99 for r in result)

    def test_apply_empty_input(self):
        from sqldim.application.datasets.schema import ChangeRule, EventSpec

        spec = EventSpec(
            changes=[
                ChangeRule(
                    "val", condition=lambda i, r: True, mutate=lambda v, r, fake: v + 1
                ),
            ]
        )
        assert spec.apply([], self._fake()) == []

    def test_stamp_ts_no_field(self):
        from sqldim.application.datasets.schema import EventSpec

        spec = EventSpec(changes=[], timestamp_field=None)
        row = {"val": 1}
        spec._stamp_ts(row)  # should be a no-op
        assert row == {"val": 1}


# ---------------------------------------------------------------------------
# DatasetSpec
# ---------------------------------------------------------------------------


class TestDatasetSpec:
    def _two_schema_spec(self):
        from sqldim.application.datasets.schema import (
            DatasetSpec,
            EntitySchema,
            FieldSpec,
        )

        return DatasetSpec(
            "widget",
            {
                "source": EntitySchema(
                    "widget",
                    fields=[
                        FieldSpec("id", "INTEGER"),
                        FieldSpec("label", "VARCHAR"),
                    ],
                ),
                "target": EntitySchema(
                    "widget_dim",
                    fields=[
                        FieldSpec("id", "INTEGER"),
                        FieldSpec("label", "VARCHAR"),
                    ],
                ),
            },
        )

    def test_role_access_returns_entity_schema(self):
        from sqldim.application.datasets.schema import EntitySchema

        spec = self._two_schema_spec()
        assert isinstance(spec.source, EntitySchema)
        assert isinstance(spec.target, EntitySchema)

    def test_role_name_distinguished(self):
        spec = self._two_schema_spec()
        assert spec.source.name == "widget"
        assert spec.target.name == "widget_dim"

    def test_missing_role_raises_attribute_error(self):
        spec = self._two_schema_spec()
        with pytest.raises(AttributeError, match="no schema role 'typo'"):
            _ = spec.typo

    def test_error_message_lists_available_roles(self):
        spec = self._two_schema_spec()
        with pytest.raises(AttributeError, match="source"):
            _ = spec.missing

    def test_repr_contains_name_and_roles(self):
        spec = self._two_schema_spec()
        r = repr(spec)
        assert "widget" in r
        assert "source" in r
        assert "target" in r

    def test_oltp_ddl_via_role(self):
        spec = self._two_schema_spec()
        ddl = spec.source.oltp_ddl()
        assert "CREATE TABLE" in ddl
        assert "id" in ddl

    def test_three_role_spec(self):
        from sqldim.application.datasets.schema import (
            DatasetSpec,
            EntitySchema,
            FieldSpec,
        )

        spec = DatasetSpec(
            "film",
            {
                "cast": EntitySchema("cast", fields=[FieldSpec("actor_id", "INTEGER")]),
                "edge": EntitySchema("edge", fields=[FieldSpec("src", "INTEGER")]),
                "actors": EntitySchema("actors", fields=[FieldSpec("id", "INTEGER")]),
            },
        )
        assert spec.cast.name == "cast"
        assert spec.edge.name == "edge"
        assert spec.actors.name == "actors"

    def test_orders_spec_source_ddl(self):
        """Smoke-test the real _ORDERS_SPEC used by OrdersSource."""
        from sqldim.application.datasets.domains.ecommerce import _ORDERS_SPEC

        ddl = _ORDERS_SPEC.source.oltp_ddl()
        assert "order_id" in ddl
        assert "{table}" in ddl

    def test_orders_spec_fact_ddl(self):
        from sqldim.application.datasets.domains.ecommerce import _ORDERS_SPEC

        ddl = _ORDERS_SPEC.fact.oltp_ddl()
        assert "placed_at" in ddl

    def test_accounts_spec_snapshot_ddl(self):
        from sqldim.application.datasets.domains.enterprise.sources import (
            _ACCOUNTS_SPEC,
        )

        ddl = _ACCOUNTS_SPEC.snapshot.oltp_ddl()
        assert "snapshot_date" in ddl

    def test_movies_spec_roles(self):
        from sqldim.application.datasets.domains.media.sources import _MOVIES_SPEC

        assert _MOVIES_SPEC.cast.name == "cast"
        assert _MOVIES_SPEC.edge.name == "edge"
        assert _MOVIES_SPEC.actors.name == "actors"

    def test_events_field_is_none_by_default(self):
        spec = self._two_schema_spec()
        assert spec.events is None

    def test_events_field_holds_event_spec(self):
        from sqldim.application.datasets.schema import (
            ChangeRule,
            DatasetSpec,
            EntitySchema,
            EventSpec,
            FieldSpec,
        )

        ev = EventSpec(
            changes=[
                ChangeRule(
                    "id", condition=lambda i, r: True, mutate=lambda v, r, fake: v + 1
                ),
            ]
        )
        spec = DatasetSpec(
            "x",
            {
                "source": EntitySchema("x", fields=[FieldSpec("id", "INTEGER")]),
            },
            events=ev,
        )
        assert spec.events is ev

    def test_reserved_role_name_raises(self):
        from sqldim.application.datasets.schema import (
            DatasetSpec,
            EntitySchema,
            FieldSpec,
        )

        with pytest.raises(ValueError, match="'events' is a reserved"):
            DatasetSpec(
                "bad",
                {
                    "events": EntitySchema("e", fields=[FieldSpec("id", "INTEGER")]),
                },
            )

    def test_repr_shows_events_suffix(self):
        from sqldim.application.datasets.schema import (
            DatasetSpec,
            EntitySchema,
            EventSpec,
            FieldSpec,
        )

        spec = DatasetSpec(
            "x",
            {
                "source": EntitySchema("x", fields=[FieldSpec("id", "INTEGER")]),
            },
            events=EventSpec(changes=[]),
        )
        assert "EventSpec" in repr(spec)

    def test_repr_no_events_suffix(self):
        spec = self._two_schema_spec()
        assert "EventSpec" not in repr(spec)

    def test_products_spec_has_events(self):
        from sqldim.application.datasets.domains.ecommerce.sources import _PRODUCTS_SPEC
        from sqldim.application.datasets.schema import EventSpec

        assert isinstance(_PRODUCTS_SPEC.events, EventSpec)
        assert _PRODUCTS_SPEC.source.name == "product"

    def test_employees_spec_has_events(self):
        from sqldim.application.datasets.domains.enterprise.sources import (
            _EMPLOYEES_SPEC,
        )
        from sqldim.application.datasets.schema import EventSpec

        assert isinstance(_EMPLOYEES_SPEC.events, EventSpec)

    def test_github_spec_has_events(self):
        from sqldim.application.datasets.domains.devops.sources import _GITHUB_SPEC
        from sqldim.application.datasets.schema import EventSpec

        assert isinstance(_GITHUB_SPEC.events, EventSpec)

    def test_orders_spec_has_no_events(self):
        from sqldim.application.datasets.domains.ecommerce import _ORDERS_SPEC

        assert _ORDERS_SPEC.events is None

    def test_movies_spec_has_no_events(self):
        from sqldim.application.datasets.domains.media.sources import _MOVIES_SPEC

        assert _MOVIES_SPEC.events is None


# ---------------------------------------------------------------------------
# SchematicSource
# ---------------------------------------------------------------------------


class TestSchematicSource:
    """Integration tests for SchematicSource using a minimal concrete subclass."""

    def _make_source(self, with_events=True, n=3, seed=0):
        from sqldim.application.datasets.schema import (
            ChangeRule,
            DatasetSpec,
            EntitySchema,
            EventSpec,
            FieldSpec,
        )
        from sqldim.application.datasets.base import SchematicSource

        schema = EntitySchema(
            "item",
            fields=[
                FieldSpec("item_id", "INTEGER", kind="seq"),
                FieldSpec(
                    "value", "DOUBLE", kind="uniform", low=1.0, high=10.0, precision=1
                ),
                FieldSpec("label", "VARCHAR", kind="const", value="X"),
            ],
        )
        ev = (
            EventSpec(
                changes=[
                    ChangeRule(
                        "value",
                        condition=lambda i, r: i == 0,
                        mutate=lambda v, r, fake: v + 100.0,
                    )
                ],
            )
            if with_events
            else None
        )

        spec = DatasetSpec("item", {"source": schema}, events=ev)

        class _TestSrc(SchematicSource):
            _spec = spec

        return _TestSrc(n=n, seed=seed)

    def test_initial_length(self):
        src = self._make_source(n=4)
        assert len(src.initial) == 4

    def test_events_populated(self):
        src = self._make_source(n=3)
        assert len(src.events) == 1  # only row 0 changes

    def test_no_event_spec_gives_empty_events(self):
        src = self._make_source(with_events=False)
        assert src.events == []

    def test_snapshot_returns_sql_source(self):
        from sqldim.sources.batch.sql import SQLSource

        src = self._make_source()
        assert isinstance(src.snapshot(), SQLSource)

    def test_event_batch_returns_sql_source(self):
        from sqldim.sources.batch.sql import SQLSource

        src = self._make_source()
        assert isinstance(src.event_batch(1), SQLSource)

    def test_event_batch_invalid_n_raises(self):
        src = self._make_source()
        with pytest.raises(ValueError, match="1 event batch"):
            src.event_batch(2)

    def test_dim_ddl_from_schema(self):
        src = self._make_source()
        ddl = src.DIM_DDL
        assert "valid_from" in ddl
        assert "item_id" in ddl

    def test_oltp_ddl_from_schema(self):
        src = self._make_source()
        ddl = src.OLTP_DDL
        assert "item_id" in ddl
        assert "valid_from" not in ddl

    def test_setup_creates_table(self):
        import duckdb

        src = self._make_source()
        con = duckdb.connect()
        src.setup(con, "dim_test")
        tables = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "dim_test" in tables

    def test_snapshot_row_count(self):
        import duckdb

        src = self._make_source(n=5)
        con = duckdb.connect()
        rows = con.execute(src.snapshot().as_sql(con)).fetchdf()
        assert len(rows) == 5
