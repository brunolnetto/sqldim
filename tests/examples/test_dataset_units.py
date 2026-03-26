"""Unit tests for examples/datasets modules — covers branches skipped by test_datasets.py."""

import pytest
import duckdb
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# base.py — SourceProvider, BaseSource, DatasetFactory, SchematicSource
# ---------------------------------------------------------------------------


class TestSourceProvider:
    def test_describe_lines_with_description(self):
        from sqldim.application.datasets.base import SourceProvider

        sp = SourceProvider(
            name="TestSource",
            description="A test source",
        )
        lines = sp._describe_lines()
        assert any("TestSource" in ln for ln in lines)
        assert any("A test source" in ln for ln in lines)

    def test_describe_lines_without_description(self):
        from sqldim.application.datasets.base import SourceProvider

        sp = SourceProvider(name="TestSource")
        # description is empty → the empty-string branch is hit
        lines = sp._describe_lines()
        assert any("TestSource" in ln for ln in lines)

    def test_describe_returns_joined_string(self):
        from sqldim.application.datasets.base import SourceProvider

        sp = SourceProvider(name="TestSource", description="Desc", url="http://x.com")
        result = sp.describe()
        assert "TestSource" in result


class TestBaseSource:
    def test_setup_raises_not_implemented_when_no_dim_ddl(self):
        from sqldim.application.datasets.base import BaseSource

        class _NoDDL(BaseSource):
            def snapshot(self):
                return MagicMock()

        src = _NoDDL()
        con = duckdb.connect()
        with pytest.raises(NotImplementedError):
            src.setup(con, "my_table")

    def test_event_batch_raises_not_implemented(self):
        from sqldim.application.datasets.base import BaseSource

        class _StaticSrc(BaseSource):
            def snapshot(self):
                return MagicMock()

        src = _StaticSrc()
        with pytest.raises(
            NotImplementedError, match="does not model incremental events"
        ):
            src.event_batch()

    def test_describe_provider_without_provider(self):
        from sqldim.application.datasets.base import BaseSource

        class _NoProv(BaseSource):
            def snapshot(self):
                return MagicMock()

        src = _NoProv()
        result = src.describe_provider()
        assert "synthetic" in result or "_NoProv" in result

    def test_describe_provider_with_provider(self):
        from sqldim.application.datasets.base import BaseSource, SourceProvider

        class _WithProv(BaseSource):
            provider = SourceProvider(name="Live", description="Production DB")

            def snapshot(self):
                return MagicMock()

        src = _WithProv()
        result = src.describe_provider()
        assert "Live" in result


class TestBaseSourceSetup:
    def test_setup_executes_dim_ddl(self):
        from sqldim.application.datasets.base import BaseSource

        class _WithDDL(BaseSource):
            DIM_DDL = "CREATE TABLE IF NOT EXISTS {table} (id INTEGER)"

            def snapshot(self):
                return MagicMock()

        src = _WithDDL()
        con = duckdb.connect()
        src.setup(con, "test_dim_tbl")
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        assert "test_dim_tbl" in tables

    def test_setup_raises_when_dim_ddl_empty(self):
        from sqldim.application.datasets.base import BaseSource

        class _EmptyDDL(BaseSource):
            DIM_DDL = ""

            def snapshot(self):
                return MagicMock()

        src = _EmptyDDL()
        con = duckdb.connect()
        with pytest.raises(NotImplementedError):
            src.setup(con, "test_empty")


class TestDatasetFactory:
    def test_create_raises_for_unknown_name(self):
        from sqldim.application.datasets.base import DatasetFactory

        with pytest.raises(KeyError, match="No dataset registered"):
            DatasetFactory.create("__nonexistent_dataset__")

    def test_create_returns_instance_for_registered_name(self):
        from sqldim.application.datasets.base import DatasetFactory

        # "customers" is registered by ecommerce.py at import time
        from sqldim.application.datasets.domains import ecommerce  # noqa: F401 (trigger registration)

        names = DatasetFactory.available()
        if names:
            src = DatasetFactory.create(names[0])
            assert src is not None

    def test_available_returns_sorted_list(self):
        from sqldim.application.datasets.base import DatasetFactory

        result = DatasetFactory.available()
        assert isinstance(result, list)
        assert result == sorted(result)

    def test_setup_all_calls_setup_per_spec(self):
        from sqldim.application.datasets.base import DatasetFactory

        src1 = MagicMock()
        src2 = MagicMock()
        con = MagicMock()
        DatasetFactory.setup_all(con, [(src1, "t1"), (src2, "t2")])
        src1.setup.assert_called_once_with(con, "t1")
        src2.setup.assert_called_once_with(con, "t2")

    def test_teardown_all_calls_teardown_per_spec(self):
        from sqldim.application.datasets.base import DatasetFactory

        src1 = MagicMock()
        src2 = MagicMock()
        con = MagicMock()
        DatasetFactory.teardown_all(con, [(src1, "t1"), (src2, "t2")])
        src1.teardown.assert_called_once_with(con, "t1")
        src2.teardown.assert_called_once_with(con, "t2")


class TestSchematicSourceMethods:
    def test_dim_ddl_property(self):
        from sqldim.application.datasets.domains.ecommerce import CustomersSource

        src = CustomersSource(n=2)
        assert isinstance(src.DIM_DDL, str)
        assert len(src.DIM_DDL) > 0

    def test_oltp_ddl_property(self):
        from sqldim.application.datasets.domains.ecommerce import CustomersSource

        src = CustomersSource(n=2)
        assert isinstance(src.OLTP_DDL, str)

    def test_snapshot_returns_sql_source(self):
        from sqldim.application.datasets.domains.ecommerce import CustomersSource
        from sqldim.sources import SQLSource

        src = CustomersSource(n=3)
        result = src.snapshot()
        assert isinstance(result, SQLSource)

    def test_event_batch_returns_sql_source(self):
        from sqldim.application.datasets.domains.ecommerce import CustomersSource
        from sqldim.sources import SQLSource

        src = CustomersSource(n=3)
        result = src.event_batch(n=1)
        assert isinstance(result, SQLSource)

    def test_event_batch_raises_for_n_gt_1(self):
        from sqldim.application.datasets.domains.ecommerce import CustomersSource

        src = CustomersSource(n=3)
        with pytest.raises(ValueError, match="event batch"):
            src.event_batch(n=2)

    def test_initial_property(self):
        from sqldim.application.datasets.domains.ecommerce import CustomersSource

        src = CustomersSource(n=3)
        assert len(src.initial) == 3

    def test_events_property(self):
        from sqldim.application.datasets.domains.ecommerce import CustomersSource

        src = CustomersSource(n=4)
        # events list is populated in constructor
        events = src.events
        assert isinstance(events, list)


# ---------------------------------------------------------------------------
# ecommerce.py
# ---------------------------------------------------------------------------


class TestCustomersSource:
    def test_moved_count(self):
        from sqldim.application.datasets.domains.ecommerce import CustomersSource

        src = CustomersSource(n=6)
        # every even-indexed customer has moved (3 out of 6)
        assert src.moved_count() == 3


class TestOrdersSource:
    def test_oltp_ddl_property(self):
        from sqldim.application.datasets.domains.ecommerce import OrdersSource

        src = OrdersSource(n=3)
        assert isinstance(src.OLTP_DDL, str)
        assert len(src.OLTP_DDL) > 0

    def test_dim_ddl_property(self):
        from sqldim.application.datasets.domains.ecommerce import OrdersSource

        src = OrdersSource(n=3)
        assert isinstance(src.DIM_DDL, str)
        assert len(src.DIM_DDL) > 0

    def test_snapshot_returns_placed_events(self):
        from sqldim.application.datasets.domains.ecommerce import OrdersSource
        from sqldim.sources import SQLSource

        src = OrdersSource(n=3)
        result = src.snapshot()
        assert isinstance(result, SQLSource)

    def test_orders_property(self):
        from sqldim.application.datasets.domains.ecommerce import OrdersSource

        src = OrdersSource(n=5)
        orders = src.orders
        assert isinstance(orders, list)
        assert len(orders) == 5


# ---------------------------------------------------------------------------
# enterprise.py
# ---------------------------------------------------------------------------


class TestAccountsSource:
    def test_oltp_ddl_property(self):
        from sqldim.application.datasets.domains.enterprise import AccountsSource

        src = AccountsSource(n=3)
        assert isinstance(src.OLTP_DDL, str)
        assert len(src.OLTP_DDL) > 0

    def test_snapshot_calls_snapshot_for_date(self):
        from sqldim.application.datasets.domains.enterprise import AccountsSource

        src = AccountsSource(n=3)
        result = src.snapshot()
        assert result is not None


# ---------------------------------------------------------------------------
# schema.py — FieldSpec, DatasetSpec, _gen_randint
# ---------------------------------------------------------------------------


class TestFieldSpecGenRandint:
    def test_gen_randint_returns_int_in_range(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec(
            name="count", kind="randint", sql_type="INTEGER", low=1, high=10
        )
        import faker as _faker

        fake = _faker.Faker()
        val = spec.generate(fake, 0)
        assert 1 <= val <= 10
        assert isinstance(val, int)


class TestFieldSpecGenerateErrors:
    def test_unknown_kind_raises_value_error(self):
        from sqldim.application.datasets.schema import FieldSpec

        spec = FieldSpec(name="x", kind="unknown_kind_xyz", sql_type="TEXT")
        import faker as _faker

        fake = _faker.Faker()
        with pytest.raises(ValueError, match="Unknown FieldSpec kind"):
            spec.generate(fake, 0)

    def test_post_function_invoked_when_not_none(self):
        from sqldim.application.datasets.schema import FieldSpec

        post_called = []

        def _post(v, fake, i):
            post_called.append(v)
            return v * 2

        spec = FieldSpec(
            name="x", kind="randint", sql_type="INTEGER", low=5, high=5, post=_post
        )
        import faker as _faker

        fake = _faker.Faker()
        result = spec.generate(fake, 0)
        assert result == 10
        assert post_called == [5]


class TestDatasetSpec:
    def test_post_init_rejects_events_key(self):
        from sqldim.application.datasets.schema import DatasetSpec

        with pytest.raises(ValueError, match="'events' is a reserved"):
            DatasetSpec(
                name="bad",
                schemas={"events": MagicMock()},
            )

    def test_getattr_raises_attribute_error_for_missing_role(self):
        from sqldim.application.datasets.schema import DatasetSpec

        spec = DatasetSpec(name="test", schemas={"source": MagicMock()})
        with pytest.raises(AttributeError, match="has no schema role"):
            _ = spec.nonexistent_role

    def test_repr_without_events(self):
        from sqldim.application.datasets.schema import DatasetSpec

        spec = DatasetSpec(name="myds", schemas={"source": MagicMock()})
        r = repr(spec)
        assert "myds" in r
        assert "events" not in r

    def test_repr_with_events(self):
        from sqldim.application.datasets.schema import DatasetSpec

        spec = DatasetSpec(
            name="myds", schemas={"source": MagicMock()}, events=MagicMock()
        )
        r = repr(spec)
        assert "myds" in r
        assert "events=<EventSpec>" in r

    def test_as_literal_none_returns_null(self):
        from sqldim.application.datasets.schema import FieldSpec

        fs = FieldSpec(name="col", sql_type="INTEGER", kind="seq")
        assert fs.as_literal(None) == "NULL"


# ---------------------------------------------------------------------------
# media.py
# ---------------------------------------------------------------------------


class TestMoviesSource:
    def test_oltp_ddl_property(self):
        from sqldim.application.datasets.domains.media import MoviesSource

        src = MoviesSource(n_movies=2, n_actors=3)
        assert isinstance(src.OLTP_DDL, str)

    def test_edge_ddl_property(self):
        from sqldim.application.datasets.domains.media import MoviesSource

        src = MoviesSource(n_movies=2, n_actors=3)
        assert isinstance(src.EDGE_DDL, str)

    def test_actors_ddl_property(self):
        from sqldim.application.datasets.domains.media import MoviesSource

        src = MoviesSource(n_movies=2, n_actors=3)
        assert isinstance(src.ACTORS_DDL, str)

    def test_snapshot_returns_cast_snapshot(self):
        from sqldim.application.datasets.domains.media import MoviesSource
        from sqldim.sources import SQLSource

        src = MoviesSource(n_movies=2, n_actors=3)
        result = src.snapshot()
        assert isinstance(result, SQLSource)

    def test_new_releases_returns_sql_source(self):
        from sqldim.application.datasets.domains.media import MoviesSource
        from sqldim.sources import SQLSource

        src = MoviesSource(n_movies=2, n_actors=3)
        result = src.new_releases(n=1)
        assert isinstance(result, SQLSource)

    def test_new_releases_raises_for_n_gt_1(self):
        from sqldim.application.datasets.domains.media import MoviesSource

        src = MoviesSource(n_movies=2, n_actors=3)
        with pytest.raises(ValueError, match="event batch"):
            src.new_releases(n=2)

    def test_actor_map(self):
        from sqldim.application.datasets.domains.media import MoviesSource

        src = MoviesSource(n_movies=2, n_actors=3)
        result = src.actor_map()
        assert isinstance(result, dict)
        assert len(result) >= 1

    def test_actors_property(self):
        from sqldim.application.datasets.domains.media import MoviesSource

        src = MoviesSource(n_movies=2, n_actors=3)
        assert isinstance(src.actors, list)
        assert len(src.actors) >= 1

    def test_movies_property(self):
        from sqldim.application.datasets.domains.media import MoviesSource

        src = MoviesSource(n_movies=2, n_actors=3)
        assert isinstance(src.movies, list)
        assert len(src.movies) >= 1

    def test_cast_property(self):
        from sqldim.application.datasets.domains.media import MoviesSource

        src = MoviesSource(n_movies=2, n_actors=3)
        assert isinstance(src.cast, list)


# ---------------------------------------------------------------------------
# Dataset dataclass
# ---------------------------------------------------------------------------


class TestDataset:
    """Full branch coverage for sqldim/examples/datasets/dataset.py."""

    def _make_src(self, *, has_snapshot=True, has_event_batch=True, has_provider=True):
        from unittest.mock import MagicMock
        from sqldim.application.datasets.base import SourceProvider

        src = MagicMock()
        if not has_snapshot:
            src.snapshot.side_effect = NotImplementedError
        else:
            src.snapshot.return_value = {"rows": [1, 2, 3]}
        if not has_event_batch:
            src.event_batch.side_effect = NotImplementedError
        else:
            src.event_batch.return_value = {"events": [4, 5]}
        if has_provider:
            src.provider = SourceProvider(name="TestSystem")
        else:
            src.provider = None
        return src

    def test_getitem_valid(self):
        from sqldim.application.datasets.dataset import Dataset

        src = self._make_src()
        ds = Dataset("test", [(src, "orders")])
        assert ds["orders"] is src

    def test_getitem_invalid_raises_key_error(self):
        from sqldim.application.datasets.dataset import Dataset

        src = self._make_src()
        ds = Dataset("test", [(src, "orders")])
        with pytest.raises(KeyError, match="No source registered for table 'missing'"):
            _ = ds["missing"]

    def test_table_names(self):
        from sqldim.application.datasets.dataset import Dataset

        src = self._make_src()
        ds = Dataset("test", [(src, "tbl_a"), (src, "tbl_b")])
        assert ds.table_names() == ["tbl_a", "tbl_b"]

    def test_setup_calls_each_source(self):
        from sqldim.application.datasets.dataset import Dataset

        s1, s2 = self._make_src(), self._make_src()
        con = duckdb.connect()
        ds = Dataset("test", [(s1, "t1"), (s2, "t2")])
        ds.setup(con)
        s1.setup.assert_called_once_with(con, "t1")
        s2.setup.assert_called_once_with(con, "t2")

    def test_teardown_calls_in_reverse(self):
        from sqldim.application.datasets.dataset import Dataset

        s1, s2 = self._make_src(), self._make_src()
        con = duckdb.connect()
        ds = Dataset("test", [(s1, "t1"), (s2, "t2")])
        calls = []
        s1.teardown.side_effect = lambda c, t: calls.append(("s1", t))
        s2.teardown.side_effect = lambda c, t: calls.append(("s2", t))
        ds.teardown(con)
        # reversed order
        assert calls == [("s2", "t2"), ("s1", "t1")]

    def test_snapshots_includes_implemented_sources(self):
        from sqldim.application.datasets.dataset import Dataset

        src = self._make_src(has_snapshot=True)
        ds = Dataset("test", [(src, "orders")])
        result = ds.snapshots()
        assert "orders" in result
        assert result["orders"] == {"rows": [1, 2, 3]}

    def test_snapshots_skips_not_implemented(self):
        from sqldim.application.datasets.dataset import Dataset

        good = self._make_src(has_snapshot=True)
        bad = self._make_src(has_snapshot=False)
        ds = Dataset("test", [(good, "good_tbl"), (bad, "bad_tbl")])
        result = ds.snapshots()
        assert "good_tbl" in result
        assert "bad_tbl" not in result

    def test_event_batches_includes_implemented_sources(self):
        from sqldim.application.datasets.dataset import Dataset

        src = self._make_src(has_event_batch=True)
        ds = Dataset("test", [(src, "orders")])
        result = ds.event_batches(n=1)
        assert "orders" in result
        assert result["orders"] == {"events": [4, 5]}

    def test_event_batches_skips_not_implemented(self):
        from sqldim.application.datasets.dataset import Dataset

        good = self._make_src(has_event_batch=True)
        bad = self._make_src(has_event_batch=False)
        ds = Dataset("test", [(good, "g"), (bad, "b")])
        result = ds.event_batches()
        assert "g" in result
        assert "b" not in result

    def test_describe_uses_provider_name(self):
        from sqldim.application.datasets.dataset import Dataset

        src = self._make_src(has_provider=True)
        ds = Dataset("test", [(src, "orders")])
        text = ds.describe()
        assert "orders" in text
        assert "TestSystem" in text

    def test_describe_fallback_to_class_name(self):
        from sqldim.application.datasets.dataset import Dataset

        src = self._make_src(has_provider=False)
        src.__class__.__name__ = "MockSource"
        ds = Dataset("test", [(src, "orders")])
        text = ds.describe()
        assert "orders" in text

    def test_repr(self):
        from sqldim.application.datasets.dataset import Dataset

        src = self._make_src()
        ds = Dataset("test_ds", [(src, "tbl1"), (src, "tbl2")])
        r = repr(ds)
        assert "test_ds" in r
        assert "tbl1" in r
        assert "tbl2" in r


# ---------------------------------------------------------------------------
# DGMShowcaseSource — teardown() and snapshot() branches
# ---------------------------------------------------------------------------


class TestDGMShowcaseSource:
    """Cover the two uncovered branches in DGMShowcaseSource."""

    def test_teardown_drops_all_tables(self):
        from sqldim.application.datasets.domains.dgm.sources import DGMShowcaseSource

        src = DGMShowcaseSource()
        con = duckdb.connect()
        src.setup(con)
        # Verify tables exist
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        assert "dgm_showcase_customer" in tables
        # Teardown drops them all
        src.teardown(con)
        tables_after = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        assert "dgm_showcase_customer" not in tables_after
        assert "dgm_showcase_sale" not in tables_after

    def test_snapshot_raises_not_implemented(self):
        from sqldim.application.datasets.domains.dgm.sources import DGMShowcaseSource

        src = DGMShowcaseSource()
        with pytest.raises(NotImplementedError, match="static fixture"):
            src.snapshot()
