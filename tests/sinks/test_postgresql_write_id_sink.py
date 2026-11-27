"""PostgreSQLWriteIdSink: PipelineArtifact materialisation into PostgreSQL (D46; #171).

The artifact store (source, artifact and transition tables) is DuckDB; the
sink relation and the commit log are in a real PostgreSQL server.  The tests
cover atomicity (a failed or interrupted write leaves neither rows nor a
commit record), recovery after a lost acknowledgement or a crash between the
write and its transition, and idempotent retry under one ``write_id``.

Server selection follows ``test_postgresql_sink.py``: ``SQLDIM_PG_HOST`` (CI
service container), else a local ``pg_ctl``; without either the module is
skipped locally and is an error under ``CI=true``.
"""

from __future__ import annotations

import glob
import os
from collections.abc import Iterator
from typing import Any

import duckdb
import pytest
from pytest_postgresql import factories

from sqldim.core.query.dgm.annotations import WriteModeKind
from sqldim.core.query.dgm.planner import SinkTarget
from sqldim.core.query.dgm.runtime import (
    ArtifactState,
    MaterialisationFailed,
    SinkWrite,
    TransitionNotRecorded,
    UnsupportedOperation,
    WriteOperation,
)
from sqldim.sinks.sql.postgresql import (
    CommitRecordMismatch,
    PostgreSQLWriteIdSink,
    SinkNotAttached,
    WriteAlreadyCommitted,
    WriteIdSinkError,
)
from tests.query.test_dgm_runtime_artifact import (
    W0,
    W1,
    Clock,
    assert_pa_holds,
    STORE_SQL,
    counts,
    model,
    plan_for,
    query,
    runtime,
    seed_complete_then_stale,
    state,
    write,
)


@pytest.fixture()
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect()
    for statement in STORE_SQL:
        c.execute(statement)
    yield c
    c.close()


def _find_pg_ctl() -> str | None:
    explicit = os.environ.get("SQLDIM_PG_CTL")
    if explicit:
        return explicit
    found = glob.glob("/usr/lib/postgresql/*/bin/pg_ctl")
    return max(found, key=lambda p: int(p.split("/")[4])) if found else None


PG_HOST = os.environ.get("SQLDIM_PG_HOST")
PG_EXE = None if PG_HOST else _find_pg_ctl()
if PG_HOST is None and PG_EXE is None:
    if os.environ.get("CI") == "true":
        raise RuntimeError("No PostgreSQL server: set SQLDIM_PG_HOST or install pg_ctl")
    pytest.skip("PostgreSQL server (pg_ctl) not installed", allow_module_level=True)

if PG_HOST:
    postgresql_proc = factories.postgresql_noproc(
        host=PG_HOST,
        port=int(os.environ.get("SQLDIM_PG_PORT", "5432")),
        user=os.environ.get("SQLDIM_PG_USER", "postgres"),
        password=os.environ.get("SQLDIM_PG_PASSWORD"),
        dbname=f"sqldim_wid_{os.environ.get('PYTEST_XDIST_WORKER', 'main')}",
    )
else:
    postgresql_proc = factories.postgresql_proc(executable=PG_EXE, port=15438)
postgresql = factories.postgresql("postgresql_proc")

PG = SinkTarget.POSTGRESQL


def _dsn(pg_proc: Any, pg_conn: Any) -> str:
    dsn = (
        f"host={pg_proc.host} port={pg_proc.port} "
        f"user={pg_proc.user} dbname={pg_conn.info.dbname}"
    )
    if pg_proc.password:
        dsn += f" password={pg_proc.password}"
    return dsn


class PG_:
    """The PostgreSQL side: the sink table ``daily`` and the commit log."""

    def __init__(self, conn: Any, dsn: str) -> None:
        self.conn = conn
        self.dsn = dsn

    def run(self, sql: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(sql)
        self.conn.commit()

    def one(self, sql: str) -> Any:
        self.conn.rollback()  # a fresh snapshot
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]

    def rows(self) -> int:
        return int(self.one("SELECT count(*) FROM daily"))

    def commits(self) -> int:
        return int(self.one("SELECT count(*) FROM sqldim_write_commits"))


@pytest.fixture()
def pg(postgresql_proc: Any, postgresql: Any) -> PG_:
    side = PG_(postgresql, _dsn(postgresql_proc, postgresql))
    side.run(
        "CREATE TABLE daily(id INTEGER, ts TIMESTAMP, "
        "amount DOUBLE PRECISION CHECK (amount < 100))"
    )
    return side


@pytest.fixture()
def sink(con: duckdb.DuckDBPyConnection, pg: PG_) -> Iterator[PostgreSQLWriteIdSink]:
    s = PostgreSQLWriteIdSink(pg.dsn)
    s.attach(con)
    s.create_commit_log(con)
    yield s
    con.execute("DETACH sqldim_pg_wid")


class LostAck(PostgreSQLWriteIdSink):
    """Commits, then loses the acknowledgement."""

    def write(self, con: Any, request: SinkWrite) -> int:
        super().write(con, request)
        raise TimeoutError("ack lost after commit")


class _CrashOnCommitRecord:
    """A connection that dies just before the commit record is inserted."""

    def __init__(self, con: Any) -> None:
        self._con = con

    def execute(self, sql: str, *args: Any) -> Any:
        if (
            sql.startswith("INSERT INTO sqldim_pg_wid")
            and "sqldim_write_commits" in sql
        ):
            raise KeyboardInterrupt("process killed mid-transaction")
        return self._con.execute(sql, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._con, name)


class CrashMidWrite(PostgreSQLWriteIdSink):
    def write(self, con: Any, request: SinkWrite) -> int:
        return super().write(_CrashOnCommitRecord(con), request)


def later() -> Clock:
    """A clock for a second process, past every instant the first one used."""
    c = Clock()
    c.calls = 10
    return c


def pg_plan(rt: Any, op: WriteOperation = WriteOperation.APPEND) -> Any:
    return plan_for(rt, w=write(op, sink=PG))


def request(write_id: str = "w1") -> SinkWrite:
    return SinkWrite(
        "daily", WriteOperation.APPEND, None, W0, W1, query().query_text, write_id
    )


# ---------------------------------------------------------------------------
# Runtime + PostgreSQL sink
# ---------------------------------------------------------------------------


class TestMaterialisation:
    def test_success_records_the_write_id(self, con: Any, pg: PG_, sink: Any) -> None:
        rt = runtime(con, sinks={PG: sink})
        p = pg_plan(rt)
        out = rt.execute(p)
        assert out.state is ArtifactState.COMPLETE and out.rows_written == 2
        assert pg.rows() == 2 and pg.commits() == 1
        assert pg.one("SELECT write_id FROM sqldim_write_commits") == p.write_id
        assert sink.committed("daily", p.write_id)
        assert counts(con) == (0, 1, 2)  # the DuckDB 'daily' is untouched
        assert_pa_holds(con)

    def test_failed_write_leaves_no_rows_and_no_record(
        self, con: Any, pg: PG_, sink: Any
    ) -> None:
        con.execute("INSERT INTO events VALUES (3, '2024-01-01 10:00', 150)")
        rt = runtime(con, sinks={PG: sink})
        p = pg_plan(rt)
        with pytest.raises(MaterialisationFailed) as info:
            rt.execute(p)
        assert info.value.state is ArtifactState.FAILED
        assert pg.rows() == 0 and pg.commits() == 0
        assert not sink.committed("daily", p.write_id)
        assert state(con) == "Failed"
        assert_pa_holds(con)

    def test_crash_mid_transaction_commits_nothing(self, con: Any, pg: PG_) -> None:
        crashing = CrashMidWrite(pg.dsn)
        crashing.attach(con)
        crashing.create_commit_log(con)
        rt = runtime(con, sinks={PG: crashing})
        p = pg_plan(rt)
        with pytest.raises(KeyboardInterrupt):
            rt.execute(p)
        assert state(con) == "In-flight"
        assert pg.rows() == 0 and pg.commits() == 0
        healthy = PostgreSQLWriteIdSink(pg.dsn)
        with pytest.raises(MaterialisationFailed):
            runtime(con, sinks={PG: healthy}, clock=later()).recover(p)
        assert state(con) == "Failed" and pg.rows() == 0
        assert_pa_holds(con)
        con.execute("DETACH sqldim_pg_wid")

    def test_merge_replaces_the_window_atomically(
        self, con: Any, pg: PG_, sink: Any
    ) -> None:
        seed_complete_then_stale(con)
        pg.run("INSERT INTO daily VALUES (9, '2024-01-01 07:00', 1)")
        m = model(WriteModeKind.REFRESH)
        rt = runtime(con, mode=WriteModeKind.REFRESH, sinks={PG: sink})
        out = rt.execute(pg_plan(rt, WriteOperation.MERGE))
        assert out.rows_written == 2
        assert pg.rows() == 2
        assert pg.one("SELECT count(*) FROM daily WHERE id = 9") == 0
        assert_pa_holds(con, m)

    def test_failed_merge_keeps_the_prior_rows(
        self, con: Any, pg: PG_, sink: Any
    ) -> None:
        seed_complete_then_stale(con)
        pg.run("INSERT INTO daily VALUES (9, '2024-01-01 07:00', 1)")
        con.execute("INSERT INTO events VALUES (3, '2024-01-01 10:00', 150)")
        m = model(WriteModeKind.REFRESH)
        rt = runtime(con, mode=WriteModeKind.REFRESH, sinks={PG: sink})
        with pytest.raises(MaterialisationFailed) as info:
            rt.execute(pg_plan(rt, WriteOperation.MERGE))
        assert info.value.state is ArtifactState.STALE
        assert pg.rows() == 1 and pg.commits() == 0
        assert pg.one("SELECT id FROM daily") == 9
        assert_pa_holds(con, m)


class TestRecovery:
    def test_lost_ack_after_commit_is_success(self, con: Any, pg: PG_) -> None:
        lossy = LostAck(pg.dsn)
        lossy.attach(con)
        lossy.create_commit_log(con)
        rt = runtime(con, sinks={PG: lossy})
        p = pg_plan(rt)
        out = rt.execute(p)
        assert out.recovered and out.state is ArtifactState.COMPLETE
        assert pg.rows() == 2 and pg.commits() == 1
        again = rt.execute(p)
        assert again.replayed and pg.rows() == 2
        assert_pa_holds(con)
        con.execute("DETACH sqldim_pg_wid")

    def test_recover_from_another_process(self, con: Any, pg: PG_, sink: Any) -> None:
        rt = runtime(con, sinks={PG: sink}, clock=Clock(fail_at=(2,)))
        p = pg_plan(rt)
        with pytest.raises(TransitionNotRecorded):
            rt.execute(p)  # committed in PostgreSQL, succeeded not appended
        assert state(con) == "In-flight" and pg.rows() == 2
        fresh = PostgreSQLWriteIdSink(pg.dsn)  # no attach: recovery only reads
        out = runtime(con, sinks={PG: fresh}, clock=later()).recover(p)
        assert out.recovered and state(con) == "Complete"
        assert pg.rows() == 2 and pg.commits() == 1
        assert_pa_holds(con)

    def test_retry_does_not_repeat_a_committed_write(
        self, con: Any, pg: PG_, sink: Any
    ) -> None:
        rt = runtime(con, sinks={PG: sink}, clock=Clock(fail_at=(2,)))
        p = pg_plan(rt)
        with pytest.raises(TransitionNotRecorded):
            rt.execute(p)
        out = rt.execute(p)
        assert out.recovered and out.rows_written is None
        assert pg.rows() == 2 and pg.commits() == 1
        assert_pa_holds(con)

    def test_unreadable_commit_log_leaves_the_run_unresolved(
        self, con: Any, pg: PG_
    ) -> None:
        nolog = PostgreSQLWriteIdSink(pg.dsn, commit_log="no_such_log")
        nolog.attach(con)
        rt = runtime(con, sinks={PG: nolog})
        with pytest.raises(TransitionNotRecorded):
            rt.execute(pg_plan(rt))
        assert pg.rows() == 0
        con.execute("DETACH sqldim_pg_wid")


# ---------------------------------------------------------------------------
# The sink on its own
# ---------------------------------------------------------------------------


class TestSink:
    def test_second_commit_of_a_write_id_is_refused(
        self, con: Any, pg: PG_, sink: Any
    ) -> None:
        assert sink.write(con, request()) == 2
        with pytest.raises(WriteAlreadyCommitted):
            sink.write(con, request())
        assert pg.rows() == 2 and pg.commits() == 1

    def test_write_id_of_another_relation_is_a_mismatch(
        self, con: Any, pg: PG_, sink: Any
    ) -> None:
        sink.write(con, request())
        with pytest.raises(CommitRecordMismatch):
            sink.committed("other", "w1")
        assert not sink.committed("daily", "w2")

    def test_write_needs_attach(self, pg: PG_) -> None:
        other = duckdb.connect()
        try:
            with pytest.raises(SinkNotAttached):
                PostgreSQLWriteIdSink(pg.dsn).write(other, request())
        finally:
            other.close()

    def test_missing_commit_log_is_an_error(self, pg: PG_) -> None:
        with pytest.raises(WriteIdSinkError, match="commit log"):
            PostgreSQLWriteIdSink(pg.dsn, commit_log="absent").committed("daily", "w1")


class TestNoPostgres:
    """Checks that need no server (they run where the module runs)."""

    @pytest.mark.parametrize(
        "kw",
        [{"schema": "a;b"}, {"commit_log": "x y"}, {"alias": "1x"}],
    )
    def test_identifiers_are_checked(self, kw: dict[str, str]) -> None:
        with pytest.raises(WriteIdSinkError):
            PostgreSQLWriteIdSink("host=x", **kw)

    def test_dsn_is_required(self) -> None:
        with pytest.raises(WriteIdSinkError):
            PostgreSQLWriteIdSink("")

    def test_bad_write_id_is_refused(self) -> None:
        with pytest.raises(WriteIdSinkError):
            PostgreSQLWriteIdSink("host=x").committed("daily", "x'; DROP")

    def test_unregistered_postgresql_is_unsupported(self, con: Any) -> None:
        rt = runtime(con)
        with pytest.raises(UnsupportedOperation, match="D46"):
            rt.execute(pg_plan(rt))
        assert counts(con) == (0, 0, 0)
