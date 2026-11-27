"""§8.14 result cache: watermark invalidation, opt-in staleness, D44 (#136)."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
import uuid

import duckdb
import pytest

from sqldim.core.query.dgm import PropRef, ScalarPred
from sqldim.core.query.dgm.lattice._skeleton import Skeleton, question_key as sk_key
from sqldim.core.query.dgm.nl.supervisor._auth import AuthContext
from sqldim.core.query.dgm.planner import StaticCatalog
from sqldim.core.query.dgm.planner._deps import (
    Partition,
    PartitionKind,
    Partitioning,
)
from sqldim.core.query.dgm.planner._ir_identity import Dependency
from sqldim.core.query.dgm.runtime import (
    CacheEntry,
    CachePolicy,
    CacheStoreError,
    CommitCounters,
    DuckDBBackend,
    InMemoryResultStore,
    InvalidRuntimeArgument,
    MissReason,
    NotConfigured,
    NotStoredReason,
    PlanExecutor,
    PlanRef,
    ResultCache,
    SQLiteResultStore,
    StaticDatasets,
    Unauthorized,
    UnknownIdentifier,
    WatermarkReadError,
)
from sqldim.core.query.dgm.runtime._cache import _decode, _encode
from tests.query._runtime_fixtures import (
    CATALOG,
    DATASETS,
    Watermarks,
    auth,
    database,
    make_dag,
    q_customers,
    q_eu,
    q_sales,
    spec_of,
)

T0 = datetime(2026, 11, 27, 12, 0, tzinfo=timezone.utc)


@dataclass
class Clock:
    now: datetime = T0

    def __call__(self) -> datetime:
        return self.now


@dataclass
class CountingBackend:
    """A DuckDB backend that counts executions and can run a hook first."""

    inner: DuckDBBackend = field(default_factory=lambda: DuckDBBackend(database()))
    calls: int = 0
    before: Any = None

    @property
    def target(self) -> Any:
        return self.inner.target

    def execute(self, sql: str, datasets: frozenset[str]) -> Any:
        self.calls += 1
        if self.before is not None:
            self.before()
        return self.inner.execute(sql, datasets)

    def interrupt(self) -> None:
        self.inner.interrupt()


@dataclass
class World:
    cache: ResultCache
    dag: Any
    backend: CountingBackend
    marks: Any
    store: Any
    clock: Clock


def world(
    a: AuthContext | None = None,
    marks: Any = None,
    store: Any = None,
    catalog: Any = CATALOG,
    dag: Any = None,
) -> World:
    marks = marks if marks is not None else Watermarks()
    if dag is None:
        dag, _ = make_dag(watermarks=marks, catalog=catalog)
    backend = CountingBackend()
    ex = PlanExecutor(
        auth=a if a is not None else auth(),
        datasets=StaticDatasets(DATASETS),
        catalog=catalog,
        backend=backend,
        job_sinks={},
        stream_batch_size=3,
    )
    store = store if store is not None else InMemoryResultStore(capacity=16)
    clock = Clock()
    cache = ResultCache(dag=dag, executor=ex, store=store, clock=clock)
    return World(cache, dag, backend, marks, store, clock)


def ref(w: World, q: Any) -> PlanRef:
    return PlanRef(w.dag.question_key(w.dag.make_from_spec(spec_of(q))))


NO_STALE = CachePolicy()


# ---------------------------------------------------------------------------
# Versioned sources (ADR 0004)
# ---------------------------------------------------------------------------


class TestVersioned:
    def test_miss_then_hit_without_reexecution(self) -> None:
        w = world()
        r = ref(w, q_sales())
        first = w.cache.run(r, policy=NO_STALE)
        assert not first.from_cache and first.miss is MissReason.ABSENT
        assert first.stored and first.computed_at == T0
        second = w.cache.run(r, policy=NO_STALE)
        assert second.from_cache and not second.possibly_stale
        assert sorted(second.rows) == sorted(first.rows)
        assert second.columns == first.columns
        assert second.datasets == frozenset({"crm", "sales"})
        assert w.backend.calls == 1

    def test_advanced_dependency_watermark_invalidates(self) -> None:
        w = world()
        r = ref(w, q_sales())
        w.cache.run(r, policy=NO_STALE)
        w.marks.marks["fact_sale"] = 4
        look = w.cache.lookup(r, policy=NO_STALE)
        assert not look.hit and look.miss is MissReason.ABSENT
        again = w.cache.run(r, policy=NO_STALE)
        assert not again.from_cache and w.backend.calls == 2

    def test_change_outside_deps_does_not_invalidate(self) -> None:
        w = world()
        r = ref(w, q_eu())  # reads dim_customer only
        w.cache.run(r, policy=NO_STALE)
        w.marks.marks["fact_sale"] = 99
        assert w.cache.lookup(r, policy=NO_STALE).hit
        assert w.cache.run(r, policy=NO_STALE).from_cache

    def test_equivalent_questions_share_the_entry(self) -> None:
        w = world()
        w.cache.run(ref(w, q_eu()), policy=NO_STALE)
        same = q_customers().where(ScalarPred(PropRef("c", "region"), "=", "EU"))
        assert w.cache.run(ref(w, same), policy=NO_STALE).from_cache

    def test_watermark_advancing_during_execution_is_not_stored(self) -> None:
        """D45 rule 2: the result is returned but not cached."""
        w = world()
        r = ref(w, q_sales())

        def bump() -> None:
            w.marks.marks["dim_customer"] = 8

        w.backend.before = bump
        res = w.cache.run(r, policy=NO_STALE)
        assert res.rows and not res.stored
        assert res.not_stored is NotStoredReason.WATERMARK_ADVANCED
        assert len(w.store) == 0
        w.backend.before = None
        assert not w.cache.lookup(r, policy=NO_STALE).hit

    def test_policy_is_required(self) -> None:
        w = world()
        with pytest.raises(InvalidRuntimeArgument, match="CachePolicy"):
            w.cache.run(ref(w, q_eu()), policy=None)  # type: ignore[arg-type]

    def test_negative_accept_stale_is_invalid(self) -> None:
        with pytest.raises(InvalidRuntimeArgument):
            CachePolicy(accept_stale=timedelta(seconds=-1))
        with pytest.raises(InvalidRuntimeArgument):
            CachePolicy(accept_stale=5)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Unversioned sources (ADR 0011, D41)
# ---------------------------------------------------------------------------


class TestUnversioned:
    def setup_method(self) -> None:
        self.w = world(marks=Watermarks({"dim_customer": None, "fact_sale": 3}))
        self.r = ref(self.w, q_eu())
        first = self.w.cache.run(self.r, policy=NO_STALE)
        assert first.stored and not first.from_cache

    def test_not_served_by_default(self) -> None:
        look = self.w.cache.lookup(self.r, policy=NO_STALE)
        assert not look.hit and look.miss is MissReason.UNVERSIONED
        assert look.key.unversioned == ("dim_customer",)
        res = self.w.cache.run(self.r, policy=NO_STALE)
        assert not res.from_cache and not res.possibly_stale
        assert self.w.backend.calls == 2

    def test_opt_in_serves_flagged_possibly_stale(self) -> None:
        self.w.clock.now = T0 + timedelta(minutes=4)
        policy = CachePolicy(accept_stale=timedelta(minutes=5))
        res = self.w.cache.run(self.r, policy=policy)
        assert res.from_cache and res.possibly_stale
        assert res.computed_at == T0
        assert self.w.backend.calls == 1

    def test_opt_in_beyond_age_misses(self) -> None:
        self.w.clock.now = T0 + timedelta(minutes=6)
        policy = CachePolicy(accept_stale=timedelta(minutes=5))
        look = self.w.cache.lookup(self.r, policy=policy)
        assert not look.hit and look.miss is MissReason.TOO_OLD
        res = self.w.cache.run(self.r, policy=policy)
        assert not res.from_cache and not res.possibly_stale

    def test_versioned_hit_is_never_flagged(self) -> None:
        w = world()
        r = ref(w, q_eu())
        w.cache.run(r, policy=NO_STALE)
        res = w.cache.run(r, policy=CachePolicy(accept_stale=timedelta(hours=1)))
        assert res.from_cache and not res.possibly_stale


# ---------------------------------------------------------------------------
# Authorization and tenancy (D44, §12.10)
# ---------------------------------------------------------------------------


@dataclass
class SpyStore:
    inner: InMemoryResultStore = field(
        default_factory=lambda: InMemoryResultStore(capacity=8)
    )
    gets: int = 0

    def get(self, tenant_id: str, result_key: str) -> CacheEntry | None:
        self.gets += 1
        return self.inner.get(tenant_id, result_key)

    def put(self, entry: CacheEntry) -> None:
        self.inner.put(entry)


class TestAuthorization:
    def test_another_tenant_is_never_served(self) -> None:
        store = InMemoryResultStore(capacity=8)
        acme = world(store=store)
        acme.cache.run(ref(acme, q_eu()), policy=NO_STALE)
        other = AuthContext("bob", "globex", auth().grants)
        globex = world(a=other, store=store)
        r = ref(globex, q_eu())
        look = globex.cache.lookup(r, policy=NO_STALE)
        assert look.key.value == acme.cache.lookup(r, policy=NO_STALE).key.value
        assert not look.hit and look.miss is MissReason.ABSENT
        assert not globex.cache.run(r, policy=NO_STALE).from_cache

    def test_missing_read_is_refused_before_the_store(self) -> None:
        store = SpyStore()
        full = world(store=store)
        full.cache.run(ref(full, q_sales()), policy=NO_STALE)
        gets = store.gets
        crm_only = world(a=auth(read=("crm",)), store=store)
        r = ref(crm_only, q_sales())
        with pytest.raises(Unauthorized):
            crm_only.cache.lookup(r, policy=NO_STALE)
        with pytest.raises(Unauthorized):
            crm_only.cache.run(r, policy=NO_STALE)
        assert store.gets == gets and crm_only.backend.calls == 0

    def test_entry_datasets_need_read(self) -> None:
        w = world(a=auth(read=("crm",)))
        r = ref(w, q_eu())
        w.cache.run(r, policy=NO_STALE)
        key = w.cache.lookup(r, policy=NO_STALE).key
        e = w.store.get("acme", key.value)
        w.store.put(replace(e, datasets=frozenset({"crm", "sales"})))
        with pytest.raises(Unauthorized):
            w.cache.run(r, policy=NO_STALE)

    def test_executor_without_auth_context(self) -> None:
        with pytest.raises(Unauthorized):
            world(a="alice")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Store failures and integrity
# ---------------------------------------------------------------------------


@dataclass
class BrokenStore:
    get_result: Any = None
    get_error: Exception | None = None
    put_error: Exception | None = None

    def get(self, tenant_id: str, result_key: str) -> Any:
        if self.get_error is not None:
            raise self.get_error
        return self.get_result

    def put(self, entry: CacheEntry) -> None:
        if self.put_error is not None:
            raise self.put_error


def an_entry(**kw: Any) -> CacheEntry:
    fields: dict[str, Any] = {
        "tenant_id": "acme",
        "result_key": "k",
        "question_key": "q",
        "unversioned": (),
        "datasets": frozenset({"crm"}),
        "columns": ("id",),
        "rows": ((1,),),
        "computed_at": T0,
    }
    fields.update(kw)
    return CacheEntry(**fields)


class TestStoreFailures:
    def test_read_failure_is_typed_not_a_miss(self) -> None:
        w = world(store=BrokenStore(get_error=OSError("disk gone")))
        r = ref(w, q_eu())
        with pytest.raises(CacheStoreError, match="disk gone") as exc:
            w.cache.run(r, policy=NO_STALE)
        assert exc.value.code == "CACHE_STORE" and w.backend.calls == 0

    def test_write_failure_is_typed(self) -> None:
        w = world(store=BrokenStore(put_error=OSError("read-only")))
        with pytest.raises(CacheStoreError, match="read-only"):
            w.cache.run(ref(w, q_eu()), policy=NO_STALE)

    def test_garbage_is_not_a_hit(self) -> None:
        w = world(store=BrokenStore(get_result={"rows": []}))
        with pytest.raises(CacheStoreError, match="not a CacheEntry"):
            w.cache.lookup(ref(w, q_eu()), policy=NO_STALE)

    def test_entry_for_another_key_or_tenant_is_not_a_hit(self) -> None:
        w = world()
        r = ref(w, q_eu())
        key = w.cache.lookup(r, policy=NO_STALE).key
        good = an_entry(result_key=key.value, question_key=key.question_key)
        for bad in (
            replace(good, tenant_id="globex"),
            replace(good, result_key="other"),
            replace(good, question_key="other"),
            replace(good, unversioned=("dim_customer",)),
        ):
            w2 = world(store=BrokenStore(get_result=bad))
            with pytest.raises(CacheStoreError):
                w2.cache.lookup(ref(w2, q_eu()), policy=NO_STALE)
        w3 = world(store=BrokenStore(get_result=good))
        assert w3.cache.lookup(ref(w3, q_eu()), policy=NO_STALE).hit

    def test_naive_computed_at_is_refused(self) -> None:
        w = world()
        with pytest.raises(CacheStoreError, match="timezone"):
            w.store.put(an_entry(computed_at=datetime(2026, 1, 1)))

    def test_clock_must_be_aware(self) -> None:
        w = world()
        w.clock.now = datetime(2026, 1, 1)
        with pytest.raises(NotConfigured):
            w.cache.lookup(ref(w, q_eu()), policy=NO_STALE)

    def test_missing_dependencies(self) -> None:
        w = world()
        with pytest.raises(NotConfigured):
            ResultCache(dag=w.dag, executor=None, store=w.store)  # type: ignore[arg-type]
        with pytest.raises(NotConfigured):
            InMemoryResultStore(capacity=0)


class TestInMemoryStore:
    def test_capacity_evicts_least_recently_used(self) -> None:
        s = InMemoryResultStore(capacity=2)
        s.put(an_entry(result_key="a"))
        s.put(an_entry(result_key="b"))
        assert s.get("acme", "a") is not None
        s.put(an_entry(result_key="c"))
        assert s.get("acme", "b") is None and len(s) == 2
        assert s.get("acme", "a") is not None and s.get("globex", "a") is None


# ---------------------------------------------------------------------------
# SQLite store (durable, cross-process)
# ---------------------------------------------------------------------------


class TestSQLiteStore:
    def test_round_trip_of_typed_values(self, tmp_path: Path) -> None:
        values = (
            None,
            True,
            2**70,
            1.5,
            float("inf"),
            "x",
            Decimal("1.10"),
            date(2024, 1, 2),
            datetime(2024, 1, 2, 3, 4, tzinfo=timezone.utc),
            time(3, 4),
            timedelta(days=1, seconds=2),
            b"\x00\x01",
            uuid.UUID(int=7),
            [1, "a"],
            {"k": (1, 2)},
        )
        s = SQLiteResultStore(tmp_path / "c.db")
        s.put(an_entry(rows=(values,)))
        got = s.get("acme", "k")
        assert got is not None and got.rows == (values,)
        assert [type(v) for v in got.rows[0]] == [type(v) for v in values]
        assert got.computed_at == T0 and s.get("globex", "k") is None

    def test_unknown_value_type_is_refused(self, tmp_path: Path) -> None:
        s = SQLiteResultStore(tmp_path / "c.db")
        with pytest.raises(CacheStoreError, match="object"):
            s.put(an_entry(rows=((object(),),)))
        assert _decode(_encode([1])) == [1]

    def test_corrupt_record_is_typed(self, tmp_path: Path) -> None:
        import sqlite3

        path = tmp_path / "c.db"
        s = SQLiteResultStore(path)
        con = sqlite3.connect(path)
        con.execute("INSERT INTO sqldim_result_cache VALUES ('acme', 'k', '{}')")
        con.commit()
        con.close()
        with pytest.raises(CacheStoreError, match="unreadable"):
            s.get("acme", "k")

    def test_cross_session_hit(self, tmp_path: Path) -> None:
        """A new process — new DAG, new executor — finds the entry (§8.14)."""
        path = tmp_path / "c.db"
        w1 = world(store=SQLiteResultStore(path))
        first = w1.cache.run(ref(w1, q_sales()), policy=NO_STALE)
        w2 = world(store=SQLiteResultStore(path))
        second = w2.cache.run(ref(w2, q_sales()), policy=NO_STALE)
        assert second.from_cache and w2.backend.calls == 0
        assert sorted(second.rows) == sorted(first.rows)

    def test_unopenable_path(self, tmp_path: Path) -> None:
        with pytest.raises(CacheStoreError):
            SQLiteResultStore(tmp_path / "missing-dir" / "c.db")


# ---------------------------------------------------------------------------
# Identity: plan-IR keys only
# ---------------------------------------------------------------------------


class TestIdentity:
    def test_lattice_key_is_not_a_cache_key(self) -> None:
        w = world()
        sk = Skeleton(("dim_customer",), ())
        lattice_key = sk_key(sk, "schema-v1")
        assert lattice_key != ref(w, q_customers()).question_key
        with pytest.raises(UnknownIdentifier):
            w.cache.lookup(PlanRef(lattice_key), policy=NO_STALE)

    def test_key_is_the_plan_result_key(self) -> None:
        w = world()
        r = ref(w, q_eu())
        look = w.cache.lookup(r, policy=NO_STALE)
        assert look.key == w.dag.result_key(r)
        assert look.key.question_key == r.question_key


# ---------------------------------------------------------------------------
# Commit-counter watermarks and partition pruning (D45)
# ---------------------------------------------------------------------------

REGION = Partitioning(
    column="region",
    kind=PartitionKind.STRING,
    partitions=(
        Partition("eu", values=frozenset({"EU"})),
        Partition("us", values=frozenset({"US"})),
    ),
)
PART_CATALOG = StaticCatalog(
    base=frozenset({"dim_customer", "fact_sale"}),
    partitions={"dim_customer": REGION},
)


def counters() -> Any:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE sqldim_watermark (relation VARCHAR, partition VARCHAR, "
        "version BIGINT)"
    )
    con.execute(
        "INSERT INTO sqldim_watermark VALUES ('dim_customer', NULL, 1), "
        "('dim_customer', 'eu', 1), ('dim_customer', 'us', 1), "
        "('fact_sale', NULL, 1)"
    )
    return con


def bump(con: Any, relation: str, partition: str | None) -> None:
    con.execute(
        "UPDATE sqldim_watermark SET version = version + 1 "
        "WHERE relation = ? AND partition IS NOT DISTINCT FROM ?",
        [relation, partition],
    )


class TestCommitCounters:
    def test_pruned_partition_load_does_not_invalidate(self) -> None:
        con = counters()
        w = world(marks=CommitCounters(con), catalog=PART_CATALOG)
        r = ref(w, q_eu())  # WHERE c.region = 'EU' → reads partition eu only
        key = w.cache.lookup(r, policy=NO_STALE).key
        assert key.watermarks == (("dim_customer", ("eu",), "eu=1"),)
        w.cache.run(r, policy=NO_STALE)
        bump(con, "dim_customer", "us")
        bump(con, "dim_customer", None)
        bump(con, "fact_sale", None)
        assert w.cache.run(r, policy=NO_STALE).from_cache
        bump(con, "dim_customer", "eu")
        assert not w.cache.lookup(r, policy=NO_STALE).hit

    def test_unpruned_read_depends_on_every_partition(self) -> None:
        con = counters()
        w = world(marks=CommitCounters(con), catalog=PART_CATALOG)
        r = ref(w, q_customers())
        w.cache.run(r, policy=NO_STALE)
        bump(con, "dim_customer", "us")
        assert not w.cache.lookup(r, policy=NO_STALE).hit

    def test_missing_partition_row_uses_the_relation_row(self) -> None:
        con = counters()
        con.execute("DELETE FROM sqldim_watermark WHERE partition = 'us'")
        reader = CommitCounters(con)
        dep = Dependency("dim_customer", ("eu", "us"))
        assert reader.watermark(dep) == 1
        bump(con, "dim_customer", None)
        assert reader.watermark(dep) == 2

    def test_no_row_is_unversioned(self) -> None:
        con = counters()
        assert CommitCounters(con).watermark(Dependency("t_other")) is None

    def test_read_failure_is_typed_not_unversioned(self) -> None:
        con = duckdb.connect()
        with pytest.raises(WatermarkReadError):
            CommitCounters(con).watermark(Dependency("dim_customer"))
        w = world(marks=CommitCounters(con))
        with pytest.raises(WatermarkReadError):
            w.cache.run(ref(w, q_eu()), policy=NO_STALE)
        assert w.backend.calls == 0

    def test_bad_rows_are_typed(self) -> None:
        con = counters()
        con.execute("INSERT INTO sqldim_watermark VALUES ('fact_sale', NULL, 2)")
        with pytest.raises(WatermarkReadError, match="two rows"):
            CommitCounters(con).watermark(Dependency("fact_sale"))
        con2 = duckdb.connect()
        con2.execute(
            "CREATE TABLE sqldim_watermark AS SELECT 'r' AS relation, "
            "NULL::VARCHAR AS partition, 'x' AS version"
        )
        with pytest.raises(WatermarkReadError, match="not an integer"):
            CommitCounters(con2).watermark(Dependency("r"))

    def test_configuration(self) -> None:
        with pytest.raises(NotConfigured):
            CommitCounters(None)
        with pytest.raises(Exception):
            CommitCounters(duckdb.connect(), table="x; DROP TABLE y")
