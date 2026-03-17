"""SCD Type 4 and Type 5 lazy processors."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import narwhals as nw


def _as_subquery(sql: str) -> str:
    """Wrap *sql* for use in a DuckDB FROM clause (see _lazy_type2 for details)."""
    return f"({sql})" if sql.strip().upper().startswith("SELECT") else sql


class LazyType4Processor:
    """
    SCD Type 4 processor — mini-dimension split.

    Rapidly-changing attributes (e.g. ``age_band``, ``income_band``) are
    stored in a compact *mini_dim_table* (typically only a few hundred rows).
    The base dimension is versioned (SCD2) on the remaining stable columns
    **plus** the profile FK, so any profile change also triggers a new
    version of the base row.  The fact table can carry both FKs.

    Parameters
    ----------
    natural_key      : Natural key column of the base dimension.
    base_columns     : Columns tracked by SCD2 on the base dim (do **not**
                       include *mini_dim_fk_col* — it is added automatically).
    mini_dim_columns : Rapidly-changing attributes split into the mini-dim.
    base_dim_table   : Name of the main dimension table (as seen by the sink).
    mini_dim_table   : Name of the mini-dimension table (as seen by the sink).
    mini_dim_fk_col  : Column name that carries the profile FK on the base dim.
    mini_dim_id_col  : PK column of the mini-dimension (default ``"id"``).
    sink             : SinkAdapter that also exposes ``upsert()``.
    batch_size       : DuckDB insert batch size.
    con              : Optional shared DuckDB connection.
    """

    def __init__(
        self,
        natural_key: str,
        base_columns: list[str],
        mini_dim_columns: list[str],
        base_dim_table: str,
        mini_dim_table: str,
        mini_dim_fk_col: str,
        mini_dim_id_col: str = "id",
        sink=None,
        batch_size: int = 100_000,
        con=None,
    ):
        import duckdb as _duckdb

        self.natural_key      = natural_key
        self.base_columns     = base_columns
        self.mini_dim_columns = mini_dim_columns
        self.base_dim_table   = base_dim_table
        self.mini_dim_table   = mini_dim_table
        self.mini_dim_fk_col  = mini_dim_fk_col
        self.mini_dim_id_col  = mini_dim_id_col
        self.sink             = sink
        self.batch_size       = batch_size
        self._con             = con or _duckdb.connect()

    def process(self, source) -> dict:
        """
        Process a full incoming batch.

        1. Upsert profile combinations into the mini-dimension.
        2. Join incoming rows with resolved profile SKs.
        3. Run SCD2 on the base dimension (checksum includes profile FK).

        Returns
        -------
        dict with keys: ``mini_dim_rows``, ``inserted``, ``versioned``,
        ``unchanged``.
        """
        from datetime import datetime, timezone
        from sqldim.sources import coerce_source

        now = datetime.now(timezone.utc).isoformat()
        src_sql = coerce_source(source).as_sql(self._con)

        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming AS SELECT * FROM {_as_subquery(src_sql)}
        """)

        # Step 1 — upsert distinct profile combos, resolve profile SKs
        mini_cols = ", ".join(self.mini_dim_columns)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW distinct_profiles AS
            SELECT DISTINCT {mini_cols} FROM incoming
        """)
        mini_dim_rows = self.sink.upsert(
            self._con,
            view_name     = "distinct_profiles",
            table_name    = self.mini_dim_table,
            conflict_cols = self.mini_dim_columns,
            returning_col = self.mini_dim_id_col,
            output_view   = "mini_dim_sks",
        )

        # Step 2 — attach profile SK to incoming rows
        join_on = " AND ".join(
            f"i.{c} = m.{c}" for c in self.mini_dim_columns
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming_with_profile_sk AS
            SELECT i.* EXCLUDE ({mini_cols}),
                   m.{self.mini_dim_id_col} AS {self.mini_dim_fk_col}
            FROM incoming i
            JOIN mini_dim_sks m ON {join_on}
        """)

        # Step 3 — SCD2 on base dim; checksum covers base_columns + profile FK
        track_cols = sorted(self.base_columns + [self.mini_dim_fk_col])
        cols_hash = " || '|' || ".join(
            f"coalesce(cast({c} as varchar), '')" for c in track_cols
        )
        nk = self.natural_key
        self._con.execute(f"""
            CREATE OR REPLACE VIEW incoming_checksummed AS
            SELECT *, md5({cols_hash}) AS _checksum
            FROM incoming_with_profile_sk
        """)

        current_sql = self.sink.current_state_sql(self.base_dim_table)
        self._con.execute(f"""
            CREATE OR REPLACE TABLE current_checksums AS
            SELECT {nk}, checksum AS _checksum
            FROM {_as_subquery(current_sql)}
            WHERE is_current = TRUE
        """)

        self._con.execute(f"""
            CREATE OR REPLACE TABLE classified AS
            SELECT i.*,
                   CASE
                       WHEN c.{nk} IS NULL             THEN 'new'
                       WHEN i._checksum != c._checksum THEN 'changed'
                       ELSE                                 'unchanged'
                   END AS _scd_class
            FROM incoming_checksummed i
            LEFT JOIN current_checksums c
                   ON cast(i.{nk} as varchar) = cast(c.{nk} as varchar)
        """)

        # Write new rows
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_rows AS
            SELECT * EXCLUDE (_scd_class, _checksum),
                   '{now}'::varchar AS valid_from,
                   NULL             AS valid_to,
                   TRUE             AS is_current,
                   _checksum        AS checksum
            FROM classified WHERE _scd_class = 'new'
        """)
        inserted = self.sink.write(
            self._con, "new_rows", self.base_dim_table, self.batch_size
        )

        # Close old versions and write new versions for changed rows
        self._con.execute(f"""
            CREATE OR REPLACE VIEW changed_nks AS
            SELECT {nk} FROM classified WHERE _scd_class = 'changed'
        """)
        self.sink.close_versions(
            self._con, self.base_dim_table, nk, "changed_nks", now
        )
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_versions AS
            SELECT * EXCLUDE (_scd_class, _checksum),
                   '{now}'::varchar AS valid_from,
                   NULL             AS valid_to,
                   TRUE             AS is_current,
                   _checksum        AS checksum
            FROM classified WHERE _scd_class = 'changed'
        """)
        versioned = self.sink.write(
            self._con, "new_versions", self.base_dim_table, self.batch_size
        )

        unchanged = self._con.execute(
            "SELECT count(*) FROM classified WHERE _scd_class = 'unchanged'"
        ).fetchone()[0]

        for v in [
            "incoming", "distinct_profiles", "mini_dim_sks",
            "incoming_with_profile_sk", "incoming_checksummed",
            "new_rows", "changed_nks", "new_versions",
        ]:
            try:
                self._con.execute(f"DROP VIEW IF EXISTS {v}")
            except Exception:
                pass
        self._con.execute("DROP TABLE IF EXISTS current_checksums")
        # 'classified' is intentionally left alive here so that
        # LazyType5Processor._apply_type1_pointer() can read from it.
        # LazyType5Processor.process() drops it explicitly after that step.

        return {
            "mini_dim_rows": mini_dim_rows,
            "inserted": inserted,
            "versioned": versioned,
            "unchanged": unchanged,
        }


# ---------------------------------------------------------------------------
# LazyType5Processor  (SCD Type 5 — mini-dimension + Type 1 current pointer)
# ---------------------------------------------------------------------------

class LazyType5Processor(LazyType4Processor):
    """
    SCD Type 5 processor — mini-dimension + Type 1 current-profile pointer.

    Extends :class:`LazyType4Processor` by additionally maintaining a
    **Type 1** column (*current_profile_fk_col*) on the base dimension.
    This column is overwritten in place whenever the profile changes, so
    callers can read the current profile directly from the base dimension
    without joining through the fact table.

    The SCD2 checksum still includes *mini_dim_fk_col* (inherited from
    Type 4), so a profile change also triggers a new version of the base row.
    *current_profile_fk_col* is updated via ``sink.update_attributes()``
    for all open (``is_current = TRUE``) rows that appear in the incoming
    batch — including rows whose SCD2 class is *unchanged*.

    Parameters
    ----------
    current_profile_fk_col : Column on the base dimension that always holds
                             the **current** profile FK (Type 1 behaviour).
    All other parameters are inherited from :class:`LazyType4Processor`.
    """

    def __init__(
        self,
        *args,
        current_profile_fk_col: str,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.current_profile_fk_col = current_profile_fk_col

    def process(self, source) -> dict:
        result = super().process(source)
        self._apply_type1_pointer()
        # Drop the classified TABLE that LazyType4Processor.process() intentionally
        # left alive for this step.
        self._con.execute("DROP TABLE IF EXISTS classified")
        self._con.execute("DROP VIEW IF EXISTS profile_pointer_updates")
        return result

    def _apply_type1_pointer(self) -> None:
        """
        Overwrite *current_profile_fk_col* on every open base-dim row
        (``is_current = TRUE``) whose natural key appears in the incoming batch.
        """
        nk = self.natural_key
        self._con.execute(f"""
            CREATE OR REPLACE VIEW profile_pointer_updates AS
            SELECT {nk}, {self.mini_dim_fk_col} AS {self.current_profile_fk_col}
            FROM classified
        """)
        self.sink.update_attributes(
            self._con,
            self.base_dim_table,
            nk,
            "profile_pointer_updates",
            [self.current_profile_fk_col],
        )
