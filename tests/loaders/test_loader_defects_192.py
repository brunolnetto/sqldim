"""Lazy loaders against a real DuckDBSink: the defects of #192.

* the sink's connection is public (``sink.connection``) and a loader given
  the sink and no ``con`` writes through it;
* ``DuckDBSink.write`` inserts by name;
* a Parquet or CSV path is a valid source (no ``FROM (read_parquet(…))``);
* ``LazyCumulativeLoader`` reads only the previous period as yesterday.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from sqldim import (
    DuckDBSink,
    LazyArrayMetricLoader,
    LazyBitmaskLoader,
    LazyCumulativeLoader,
    LazyEdgeProjectionLoader,
)
from sqldim.core.kimball.dimensions.scd.processors import LazySCDProcessor
from sqldim.exceptions import IdempotencyError, LoadError, SchemaError
from sqldim.sinks import ParquetSink, SinkNotOpen
from sqldim.sinks._connection import connection_for
from sqldim.sources import from_item


@pytest.fixture()
def db(tmp_path: Path) -> str:
    return str(tmp_path / "w.duckdb")


def _ddl(path: str, *statements: str) -> None:
    con = duckdb.connect(path)
    for s in statements:
        con.execute(s)
    con.close()


# -- the public connection ----------------------------------------------------


def test_connection_is_public_and_only_open_inside_with(db: str) -> None:
    sink = DuckDBSink(db)
    with pytest.raises(SinkNotOpen, match="with DuckDBSink"):
        _ = sink.connection
    with sink:
        assert sink.connection.execute("SELECT 1").fetchone() == (1,)
    with pytest.raises(SinkNotOpen):
        _ = sink.connection


def test_connection_for() -> None:
    con = duckdb.connect()
    assert connection_for(object(), con) is con
    assert isinstance(
        connection_for(ParquetSink("/tmp/x"), None), duckdb.DuckDBPyConnection
    )
    with pytest.raises(SinkNotOpen):
        connection_for(DuckDBSink(":memory:"), None)


def test_loader_writes_through_the_sink_connection_by_default(
    db: str, tmp_path: Path
) -> None:
    _ddl(db, "CREATE TABLE edges (subject_id VARCHAR, object_id VARCHAR)")
    src = tmp_path / "facts.parquet"
    duckdb.sql(f"COPY (SELECT 'p1' AS player_id, 'g1' AS game_id) TO '{src}'")
    with DuckDBSink(db) as sink:
        loader = LazyEdgeProjectionLoader(
            table="edges", subject_key="player_id", object_key="game_id", sink=sink
        )
        assert loader.process(str(src)) == 1
        rows = sink.connection.execute("SELECT * FROM sqldim_ddb.edges").fetchall()
    assert rows == [("p1", "g1")]


def test_processor_writes_through_the_sink_connection(db: str, tmp_path: Path) -> None:
    _ddl(db, "CREATE TABLE dim_c (customer_id VARCHAR, tier VARCHAR, "
             "valid_from VARCHAR, valid_to VARCHAR, is_current BOOLEAN, checksum VARCHAR)")  # fmt: skip
    src = tmp_path / "c.parquet"
    duckdb.sql(f"COPY (SELECT 'c1' AS customer_id, 'free' AS tier) TO '{src}'")
    with DuckDBSink(db) as sink:
        result = LazySCDProcessor("customer_id", ["tier"], sink).process(
            str(src), "dim_c"
        )
    assert result.inserted == 1


def test_loader_on_a_closed_sink_is_refused(db: str) -> None:
    with pytest.raises(SinkNotOpen):
        LazyBitmaskLoader(
            table="t", partition_key="k", dates_column="d",
            reference_date="2024-01-31", sink=DuckDBSink(db),
        )  # fmt: skip


# -- DuckDBSink.write by name -------------------------------------------------


def test_write_inserts_by_name(db: str) -> None:
    _ddl(db, "CREATE TABLE t (a INTEGER, b VARCHAR, c INTEGER DEFAULT 7)")
    with DuckDBSink(db) as sink:
        con = sink.connection
        con.execute("CREATE VIEW v AS SELECT 'x' AS b, 1 AS a")
        assert sink.write(con, "v", "t") == 1
        assert con.execute("SELECT * FROM sqldim_ddb.t").fetchall() == [(1, "x", 7)]


def test_write_refuses_a_column_the_target_lacks(db: str) -> None:
    _ddl(db, "CREATE TABLE t (a INTEGER)")
    with DuckDBSink(db) as sink:
        con = sink.connection
        con.execute("CREATE VIEW v AS SELECT 1 AS a, 2 AS z")
        with pytest.raises(SchemaError, match=r"\['z'\]"):
            sink.write(con, "v", "t")
        assert con.execute("SELECT count(*) FROM sqldim_ddb.t").fetchone() == (0,)


# -- file paths as sources ----------------------------------------------------


def test_from_item() -> None:
    assert from_item("read_parquet('x.parquet')") == "read_parquet('x.parquet')"
    assert from_item("events") == "events"
    assert from_item(" SELECT * FROM t ") == "(SELECT * FROM t)"
    assert from_item("WITH a AS (SELECT 1) SELECT * FROM a").startswith("(WITH")


@pytest.mark.parametrize("fmt", ["parquet", "csv"])
def test_loaders_read_a_file_path(db: str, tmp_path: Path, fmt: str) -> None:
    _ddl(
        db,
        "CREATE TABLE bits (user_id VARCHAR, datelist_int BIGINT)",
        "CREATE TABLE arr (user_id VARCHAR, metric_name VARCHAR, "
        "month_start DATE, metric_array DOUBLE[])",
    )
    # A CSV holds no list, so the date lists stay in Parquet; the revenue
    # file is in the format under test.
    acts, rev = tmp_path / "acts.parquet", tmp_path / f"rev.{fmt}"
    duckdb.sql(
        f"COPY (SELECT 'u1' AS user_id, ['2024-01-31'] AS dates_active) TO '{acts}'"
    )
    duckdb.sql(f"COPY (SELECT 'u1' AS user_id, 5.0 AS revenue) TO '{rev}'")
    with DuckDBSink(db) as sink:
        LazyBitmaskLoader(
            table="bits", partition_key="user_id", dates_column="dates_active",
            reference_date="2024-01-31", sink=sink,
        ).process(str(acts))  # fmt: skip
        LazyArrayMetricLoader(
            table="arr", partition_key="user_id", value_column="revenue",
            metric_name="rev", month_start=date(2024, 1, 1), sink=sink,
        ).process(str(rev), target_date=date(2024, 1, 2))  # fmt: skip
        con = sink.connection
        assert con.execute("SELECT * FROM sqldim_ddb.bits").fetchall() == [
            ("u1", 2**31)
        ]
        got = con.execute("SELECT metric_array[2] FROM sqldim_ddb.arr").fetchall()
    assert got == [(5.0,)]


# -- LazyCumulativeLoader -----------------------------------------------------

_SEASONS = (
    "CREATE TABLE ps (player_name VARCHAR,"
    " seasons STRUCT(pts DOUBLE, period VARCHAR)[],"
    " is_active BOOLEAN, years_since_last_active INTEGER, current_season VARCHAR)"
)


def _season(con: duckdb.DuckDBPyConnection, name: str, rows: str) -> str:
    con.execute(
        f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM (VALUES {rows}) t(player_name, pts)"
    )
    return name


def _cumulative(sink: DuckDBSink) -> LazyCumulativeLoader:
    return LazyCumulativeLoader(
        table="ps", partition_key="player_name", cumulative_column="seasons",
        metric_columns=["pts"], sink=sink,
    )  # fmt: skip


def test_cumulative_reads_only_the_previous_period(db: str) -> None:
    _ddl(db, _SEASONS)
    with DuckDBSink(db) as sink:
        con, loader = sink.connection, _cumulative(sink)
        loader.process(_season(con, "s1", "('A', 1.0), ('B', 2.0)"), 2022)
        loader.process(_season(con, "s2", "('A', 3.0)"), 2023)
        loader.process(_season(con, "s3", "('A', 5.0)"), 2024, previous_period=2023)
        rows = con.execute(
            "SELECT player_name, current_season, len(seasons), years_since_last_active"
            " FROM sqldim_ddb.ps ORDER BY 2, 1"
        ).fetchall()
    # One row per player per period; history grows; B stays dormant.
    assert rows == [
        ("A", "2022", 1, 0), ("B", "2022", 1, 0),
        ("A", "2023", 2, 0), ("B", "2023", 1, 1),
        ("A", "2024", 3, 0), ("B", "2024", 1, 2),
    ]  # fmt: skip


def test_cumulative_needs_previous_period_when_ambiguous(db: str) -> None:
    _ddl(db, _SEASONS)
    with DuckDBSink(db) as sink:
        con, loader = sink.connection, _cumulative(sink)
        loader.process(_season(con, "s1", "('A', 1.0)"), 2022)
        loader.process(_season(con, "s2", "('A', 3.0)"), 2023)
        with pytest.raises(LoadError, match="previous_period"):
            loader.process(_season(con, "s3", "('A', 5.0)"), 2024)
        with pytest.raises(LoadError, match="no row of period '2020'"):
            loader.process("s3", 2024, previous_period=2020)
        with pytest.raises(IdempotencyError, match="already holds period '2023'"):
            loader.process("s3", 2023, previous_period=2022)
        assert con.execute("SELECT count(*) FROM sqldim_ddb.ps").fetchone() == (2,)
