"""Tests for MotherDuckSink using a local .duckdb file (no cloud required)."""

from sqldim.sinks.sql.motherduck import MotherDuckSink


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_db(tmp_path) -> str:
    """Return a temp .duckdb path that doesn't exist yet."""
    p = str(tmp_path / "md_test.duckdb")
    return p


def _create_scd_table(sink: MotherDuckSink, table_name: str) -> None:
    """Create a minimal SCD2 table inside the attached alias."""
    sink._con.execute(f"""
        CREATE TABLE IF NOT EXISTS {sink._alias}.{sink._schema}.{table_name} (
            id           INTEGER PRIMARY KEY,
            nk           VARCHAR,
            name         VARCHAR,
            checksum     VARCHAR,
            is_current   BOOLEAN DEFAULT TRUE,
            valid_from   VARCHAR,
            valid_to     VARCHAR
        )
    """)


def _create_mini_dim_table(sink: MotherDuckSink, table_name: str) -> None:
    """Create a minimal mini-dimension table inside the attached alias."""
    sink._con.execute(f"""
        CREATE TABLE IF NOT EXISTS {sink._alias}.{sink._schema}.{table_name} (
            id     INTEGER PRIMARY KEY,
            region VARCHAR,
            tier   VARCHAR
        )
    """)


# ── __init__ / path construction ─────────────────────────────────────────────

def test_local_path_is_used_as_is(tmp_path):
    path = _make_db(tmp_path)
    sink = MotherDuckSink(db=path)
    assert sink._path == path


def test_cloud_name_builds_md_uri():
    sink = MotherDuckSink(db="my_warehouse", token="tok123")
    assert sink._path.startswith("md:my_warehouse")
    assert "tok123" in sink._path


def test_cloud_name_without_token_skips_token(monkeypatch):
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
    sink = MotherDuckSink(db="my_warehouse")
    assert sink._path == "md:my_warehouse"


def test_cloud_name_uses_env_token(monkeypatch):
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "envtok")
    sink = MotherDuckSink(db="my_warehouse")
    assert "envtok" in sink._path


# ── Context manager ───────────────────────────────────────────────────────────

def test_context_manager_opens_and_closes(tmp_path):
    path = _make_db(tmp_path)
    with MotherDuckSink(db=path) as sink:
        assert sink._con is not None
    assert sink._con is None


def test_context_manager_attached_schema(tmp_path):
    path = _make_db(tmp_path)
    with MotherDuckSink(db=path) as sink:
        schemas = [r[0] for r in sink._con.execute("SHOW DATABASES").fetchall()]
        assert sink._alias in schemas


# ── current_state_sql ─────────────────────────────────────────────────────────

def test_current_state_sql(tmp_path):
    path = _make_db(tmp_path)
    with MotherDuckSink(db=path) as sink:
        sql = sink.current_state_sql("dim_customer")
        assert "sqldim_md" in sql
        assert "dim_customer" in sql


# ── write ─────────────────────────────────────────────────────────────────────

def test_write_inserts_rows(tmp_path):
    path = _make_db(tmp_path)
    with MotherDuckSink(db=path) as sink:
        _create_scd_table(sink, "dim_t")
        con = sink._con
        con.execute("""
            CREATE OR REPLACE VIEW incoming_view AS
            SELECT 1 AS id, 'NK1' AS nk, 'Alice' AS name,
                   'abc' AS checksum, TRUE AS is_current,
                   '2024-01-01' AS valid_from, NULL AS valid_to
        """)
        count = sink.write(con, "incoming_view", "dim_t")
        assert count == 1
        rows = con.execute(
            f"SELECT count(*) FROM {sink._alias}.{sink._schema}.dim_t"
        ).fetchone()[0]
        assert rows == 1


# ── close_versions ────────────────────────────────────────────────────────────

def test_close_versions(tmp_path):
    path = _make_db(tmp_path)
    with MotherDuckSink(db=path) as sink:
        _create_scd_table(sink, "dim_t")
        con = sink._con
        tbl = f"{sink._alias}.{sink._schema}.dim_t"
        con.execute(f"""
            INSERT INTO {tbl} VALUES
            (1, 'NK1', 'Alice', 'abc', TRUE, '2024-01-01', NULL),
            (2, 'NK2', 'Bob',   'def', TRUE, '2024-01-01', NULL)
        """)
        con.execute("CREATE OR REPLACE VIEW nk_view AS SELECT 'NK1' AS nk")
        closed = sink.close_versions(con, "dim_t", "nk", "nk_view", "2024-06-01")
        assert closed == 1
        row = con.execute(
            f"SELECT is_current, valid_to FROM {tbl} WHERE nk = 'NK1'"
        ).fetchone()
        assert row[0] is False
        assert row[1] == "2024-06-01"


# ── update_attributes ─────────────────────────────────────────────────────────

def test_update_attributes(tmp_path):
    path = _make_db(tmp_path)
    with MotherDuckSink(db=path) as sink:
        _create_scd_table(sink, "dim_t")
        con = sink._con
        tbl = f"{sink._alias}.{sink._schema}.dim_t"
        con.execute(f"""
            INSERT INTO {tbl} VALUES
            (1, 'NK1', 'Old Name', 'abc', TRUE, '2024-01-01', NULL)
        """)
        con.execute(
            "CREATE OR REPLACE VIEW upd_view AS "
            "SELECT 'NK1' AS nk, 'New Name' AS name"
        )
        count = sink.update_attributes(con, "dim_t", "nk", "upd_view", ["name"])
        assert count == 1
        name = con.execute(f"SELECT name FROM {tbl} WHERE nk = 'NK1'").fetchone()[0]
        assert name == "New Name"


# ── rotate_attributes ─────────────────────────────────────────────────────────

def test_rotate_attributes(tmp_path):
    path = _make_db(tmp_path)
    with MotherDuckSink(db=path) as sink:
        con = sink._con
        # Create a table with current/previous columns
        con.execute(f"""
            CREATE TABLE {sink._alias}.{sink._schema}.dim_addr (
                id        INTEGER,
                nk        VARCHAR,
                city      VARCHAR,
                prev_city VARCHAR,
                is_current BOOLEAN
            )
        """)
        tbl = f"{sink._alias}.{sink._schema}.dim_addr"
        con.execute(f"""
            INSERT INTO {tbl} VALUES
            (1, 'NK1', 'Boston', NULL, TRUE)
        """)
        con.execute(
            "CREATE OR REPLACE VIEW rot_view AS "
            "SELECT 'NK1' AS nk, 'NYC' AS city"
        )
        count = sink.rotate_attributes(
            con, "dim_addr", "nk", "rot_view", [("city", "prev_city")]
        )
        assert count == 1
        row = con.execute(f"SELECT city, prev_city FROM {tbl} WHERE nk = 'NK1'").fetchone()
        assert row[0] == "NYC"
        assert row[1] == "Boston"


# ── update_milestones ─────────────────────────────────────────────────────────

def test_update_milestones(tmp_path):
    path = _make_db(tmp_path)
    with MotherDuckSink(db=path) as sink:
        con = sink._con
        con.execute(f"""
            CREATE TABLE {sink._alias}.{sink._schema}.fact_acc (
                order_id   INTEGER,
                paid_at    VARCHAR,
                shipped_at VARCHAR
            )
        """)
        tbl = f"{sink._alias}.{sink._schema}.fact_acc"
        con.execute(f"INSERT INTO {tbl} VALUES (1, '2024-01-01', NULL)")
        con.execute(
            "CREATE OR REPLACE VIEW ms_view AS "
            "SELECT 1 AS order_id, '2024-01-05' AS shipped_at"
        )
        count = sink.update_milestones(
            con, "fact_acc", "order_id", "ms_view", ["shipped_at"]
        )
        assert count == 1
        val = con.execute(f"SELECT shipped_at FROM {tbl}").fetchone()[0]
        assert val == "2024-01-05"


# ── upsert ────────────────────────────────────────────────────────────────────

def test_upsert_inserts_new_combinations(tmp_path):
    path = _make_db(tmp_path)
    with MotherDuckSink(db=path) as sink:
        _create_mini_dim_table(sink, "dim_profile")
        con = sink._con
        con.execute("""
            CREATE OR REPLACE VIEW src_profiles AS
            SELECT * FROM (VALUES ('East', 'Gold'), ('West', 'Silver')) t(region, tier)
        """)
        count = sink.upsert(
            con,
            "src_profiles",
            "dim_profile",
            ["region", "tier"],
            "id",
            "profile_keys",
        )
        assert count == 2
        rows = con.execute(
            f"SELECT count(*) FROM {sink._alias}.{sink._schema}.dim_profile"
        ).fetchone()[0]
        assert rows == 2


def test_upsert_skips_existing_combinations(tmp_path):
    path = _make_db(tmp_path)
    with MotherDuckSink(db=path) as sink:
        _create_mini_dim_table(sink, "dim_profile")
        con = sink._con
        tbl = f"{sink._alias}.{sink._schema}.dim_profile"
        con.execute(f"INSERT INTO {tbl} VALUES (1, 'East', 'Gold')")
        con.execute("""
            CREATE OR REPLACE VIEW src_profiles AS
            SELECT * FROM (VALUES ('East', 'Gold'), ('East', 'Silver')) t(region, tier)
        """)
        count = sink.upsert(
            con,
            "src_profiles",
            "dim_profile",
            ["region", "tier"],
            "id",
            "profile_keys",
        )
        # Only 1 new combination inserted
        rows = con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
        assert rows == 2
        assert count == 2  # output_view has both (existing + new)


# ── __exit__ DETACH exception silenced ───────────────────────────────────────

def test_exit_silences_detach_exception(tmp_path):
    """DETACH failure in __exit__ should be silenced, not raised."""
    path = _make_db(tmp_path)
    with MotherDuckSink(db=path) as sink:
        assert sink._con is not None
        real_con = sink._con

        # Replace _con with a proxy that raises on DETACH but delegates all else
        class _FakeCon:
            def execute(self, sql, *a, **kw):
                if sql.startswith("DETACH"):
                    raise Exception("DETACH failed")
                return real_con.execute(sql, *a, **kw)

            def close(self):
                real_con.close()

        sink._con = _FakeCon()
    # __exit__ should swallow the DETACH Exception; _con set to None
    assert sink._con is None
