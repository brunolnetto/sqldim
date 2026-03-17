"""SCD Type-2 processor for the metadata-bag column layout.

All non-NK source attributes are packed into a JSON ``metadata`` column.
Change detection uses ``md5(metadata::varchar)`` so individual attribute
columns never need to be tracked by name.  When a row changes, the *previous*
metadata snapshot is captured before the old version is expired and stored in
``metadata_diff`` — enabling full audit without touching Python memory.

Expected DB schema for each SCD2 table managed by this processor::

    sk             BIGSERIAL / SERIAL PK   (auto-generated — never in INSERT)
    <nk_cols>      individual columns      (natural key, indexed)
    valid_from     TIMESTAMPTZ
    valid_to       TIMESTAMPTZ NULL
    is_current     BOOLEAN
    metadata       JSONB / JSON            (all non-NK source attributes)
    metadata_diff  JSONB / JSON NULL       (previous metadata; set on changes)
    row_hash       TEXT                    (MD5 of ``metadata`` JSON string)
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

import duckdb

from sqldim.scd.handler import SCDResult

_log = logging.getLogger(__name__)
_SAFE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe(name: str) -> str:
    """Return *name* after validating it is a plain SQL identifier.

    Raises :class:`ValueError` when *name* contains characters that could
    enable SQL injection (anything outside ``[A-Za-z0-9_]`` or a leading
    digit).  All column names and table names are validated before being
    interpolated into SQL strings.
    """
    if not _SAFE_RE.match(name):
        raise ValueError(
            f"Invalid SQL identifier {name!r} — must match ^[A-Za-z_][A-Za-z0-9_]*$"
        )
    return name


def _as_subquery(sql: str) -> str:
    """Wrap *sql* in parens only when it is a SELECT subquery.

    DuckDB 1.5+ rejects ``FROM (table_function())`` — parentheses are only
    valid around ``SELECT …`` subqueries.  Table-function calls and bare
    table/view names must appear *without* parentheses in the FROM clause.
    """
    return f"({sql})" if sql.strip().upper().startswith("SELECT") else sql


class LazySCDMetadataProcessor:
    """SCD Type-2 processor for the metadata-bag schema layout.

    Source columns are split into two groups at processing time:

    * **Natural key** columns — stored as individual columns in the target
      table (``nk_cols``).  Used for joining, never packed into JSON.
    * **Metadata** columns — all remaining source attributes.  Packed into a
      single ``metadata`` JSON object via ``to_json(struct_pack(…))``, which
      preserves original data types (numbers stay numbers, etc.).

    Change detection:
        ``row_hash = md5(cast(metadata_json AS varchar))``

    Only rows whose ``row_hash`` differs from the current DB value
    (or that are entirely new) are written to the target table.  Unchanged
    rows are counted but ignored.

    On change:
        1. Snapshot the *current* ``metadata`` for all changed NKs (TABLE
           materialised before ``close_versions`` so the snapshot is
           consistent).
        2. Expire old versions (``is_current = FALSE``, ``valid_to = now``).
        3. Insert new versions with ``metadata_diff = old_metadata`` so the
           delta is preserved for audit.

    All SQL is executed inside DuckDB — no Python-side row materialisation.

    Usage (initial load)::

        import duckdb
        from sqldim.sinks import PostgreSQLSink
        from sqldim.scd.processors import LazySCDMetadataProcessor

        con = duckdb.connect()
        con.execute("LOAD postgres; ATTACH 'postgresql://…' AS sqldim_pg (TYPE postgres, READ_WRITE)")

        sink = PostgreSQLSink(dsn="postgresql://…", schema="public")
        sink._con = con          # share the ATTACH'd connection

        proc = LazySCDMetadataProcessor(
            natural_key=["cnpj_basico"],
            metadata_columns=["razao_social", "natureza_juridica", "capital_social"],
            sink=sink,
            con=con,
        )
        result = proc.process("empresa.parquet", "empresa")
        print(f"new={result.inserted}  changed={result.versioned}  unchanged={result.unchanged}")

    Composite natural key::

        proc = LazySCDMetadataProcessor(
            natural_key=["cnpj_basico", "cnpj_ordem", "cnpj_dv"],
            metadata_columns=["situacao_cadastral", "data_situacao", …],
            sink=sink,
            con=con,
        )
    """

    # Column names written after the NK columns — order must match DB schema.
    _EXTRA_COLS = [
        "valid_from",
        "valid_to",
        "is_current",
        "metadata",
        "metadata_diff",
        "row_hash",
    ]

    def __init__(
        self,
        natural_key: str | list[str],
        metadata_columns: list[str],
        sink,
        batch_size: int = 50_000,
        con: duckdb.DuckDBPyConnection | None = None,
    ):
        raw_nk = [natural_key] if isinstance(natural_key, str) else list(natural_key)
        self._nk_cols   = [_safe(c) for c in raw_nk]
        # Sort metadata columns for a stable, deterministic JSON key order and hash.
        self._meta_cols = sorted(_safe(c) for c in metadata_columns)
        self.sink       = sink
        self.batch_size = batch_size
        self._con       = con if con is not None else duckdb.connect()

    # ── Public API ────────────────────────────────────────────────────────

    def process(
        self,
        source,
        table_name: str,
        now: str | None = None,
    ) -> SCDResult:
        """Run the full SCD-2 cycle and return change counts.

        *source* — any value accepted by :func:`sqldim.sources.coerce_source`
        (a ``.parquet`` path string, a ``ParquetSource``, a DuckDB SQL
        expression, etc.).

        *table_name* — target dimension table in the sink.

        *now* — ISO-8601 timestamp string used for ``valid_from`` / ``valid_to``.
        Defaults to ``datetime.now(timezone.utc).isoformat()`` when ``None``.

        Returns an :class:`~sqldim.scd.handler.SCDResult` with
        ``inserted``, ``versioned``, and ``unchanged`` counts.
        """
        if now is None:
            now = datetime.now(timezone.utc).isoformat()
        _safe(table_name)

        _log.info(f"[sqldim] {table_name}: registering source …")
        self._register_source(source)        # TABLE: incoming (parquet → NK + metadata + hash)
        self._register_current_hashes(table_name)  # TABLE: current_hashes (PG → NK + hash, once)
        self._classify()                     # TABLE: classified (hash join, once; spill-eligible)

        # Drop source tables as soon as classified is built to free memory.
        # classified has everything we need; incoming and current_hashes are no
        # longer referenced by any downstream step.
        self._con.execute("DROP TABLE IF EXISTS incoming")
        self._con.execute("DROP TABLE IF EXISTS current_hashes")

        # Single scan over the already-materialised classified TABLE.
        stats = self._con.execute("""
            SELECT countif(_scd_class = 'new')       AS n_new,
                   countif(_scd_class = 'changed')   AS n_changed,
                   countif(_scd_class = 'unchanged') AS n_unchanged
            FROM classified
        """).fetchone()
        n_new, n_changed, n_unchanged = stats[0], stats[1], stats[2]
        _log.info(
            f"[sqldim] {table_name}: classified — "
            f"{n_new:,} new, {n_changed:,} changed, {n_unchanged:,} unchanged"
        )

        result           = SCDResult()
        result.inserted  = self._write_new(table_name, now, n_new)
        result.versioned = self._write_changed(table_name, now, n_changed)
        result.unchanged = n_unchanged

        # Drop classified after all writes are done.
        self._con.execute("DROP TABLE IF EXISTS classified")
        return result

    # ── Pipeline steps ────────────────────────────────────────────────────

    def _register_source(self, source) -> None:
        """Materialise the source into a DuckDB TABLE named ``incoming``.

        Projected columns:

        * NK columns — as-is.
        * ``_metadata`` — all metadata columns packed into JSON via
          ``to_json(struct_pack(…))``, which preserves original DuckDB types.
        * ``_row_hash``  — ``md5(cast(_metadata AS varchar))``.

        Rows where any NK column is NULL are silently dropped: such entities
        cannot be identified or versioned in an SCD2 dimension.

        A TABLE (not a VIEW) is used so that:

        1. The Parquet file is scanned and ``struct_pack`` / ``md5`` are
           computed **exactly once**.  With a VIEW, every downstream reference
           to ``incoming`` (from ``classified``, from ``countif``, from
           ``_write_new``, from ``_write_changed``) would repeat the full scan
           and hash computation.

        2. The materialised TABLE is **spill-eligible** — DuckDB can page it
           through ``temp_directory`` when memory is tight, which is not
           possible for a lazy VIEW pipeline.
        """
        from sqldim.sources import coerce_source

        src_sql   = coerce_source(source).as_sql(self._con)
        nk_select = ", ".join(self._nk_cols)

        if self._meta_cols:
            pairs    = ", ".join(f"{c} := {c}" for c in self._meta_cols)
            meta_sql = f"to_json(struct_pack({pairs}))"
        else:
            meta_sql = "'{}'"

        # Drop rows with NULL in any NK column — they cannot be tracked.
        nk_not_null = " AND ".join(f"{c} IS NOT NULL" for c in self._nk_cols)

        self._con.execute(f"""
            CREATE OR REPLACE TABLE incoming AS
            WITH _src AS (
                SELECT {nk_select},
                       {meta_sql} AS _metadata
                FROM {_as_subquery(src_sql)}
                WHERE {nk_not_null}
            )
            SELECT {nk_select},
                   _metadata,
                   md5(cast(_metadata AS varchar)) AS _row_hash
            FROM _src
        """)

    def _register_current_hashes(self, table_name: str) -> None:
        """Materialise a DuckDB TABLE of (NK cols, row_hash) for currently-open rows.

        A TABLE is used here intentionally (not a VIEW) for two reasons:

        1. **Single PostgreSQL scan** — the ``classified`` VIEW is evaluated
           three times per run (``countif``, ``_write_new``, ``_write_changed``).
           A VIEW over the remote table would re-fetch all current rows from
           PostgreSQL on every evaluation, rebuilding the LEFT JOIN hash table
           each time.  For ``estabelecimento`` (50M rows) that is 3 × ~4–5 GB
           of hash-table construction against a 512 MB budget → OOM / segfault.
           A materialised TABLE fetches from PostgreSQL exactly once; subsequent
           JOIN evaluations read from DuckDB's local storage.

        2. **Spill-to-disk eligibility** — once the rows live in a DuckDB
           TABLE the engine can spill them through ``temp_directory`` when
           memory is tight, which is not possible for VIEW-driven remote reads.
           Configure ``SET temp_directory`` on the DuckDB connection to enable
           this safety valve (see :func:`sqldim.processors.lazy_scd_loader.run_lazy_scd`).
        """
        nk_select = ", ".join(self._nk_cols)
        db_sql    = self.sink.current_state_sql(table_name)
        self._con.execute(f"""
            CREATE OR REPLACE TABLE current_hashes AS
            SELECT {nk_select}, row_hash AS _row_hash
            FROM {_as_subquery(db_sql)}
            WHERE is_current = TRUE
        """)

    def _classify(self) -> None:
        """Materialise a ``classified`` DuckDB TABLE tagging each incoming row.

        Tables are used for both sides of the join (``incoming`` and
        ``current_hashes`` are both already TABLEs) so DuckDB can execute the
        hash join once and cache the result.  Keeping this as a TABLE rather
        than a VIEW means the classification result is computed exactly once and
        is spill-eligible, so downstream references to ``classified``
        (``countif``, ``_write_new``, ``_write_changed``) all read from local
        storage instead of re-executing the join.

        Tags: ``'new'`` (NK absent in DB), ``'changed'`` (hash differs),
        ``'unchanged'`` (hash identical).
        """
        join_cond  = " AND ".join(
            f"cast(i.{c} AS varchar) = cast(c.{c} AS varchar)"
            for c in self._nk_cols
        )
        null_check = f"c.{self._nk_cols[0]} IS NULL"
        self._con.execute(f"""
            CREATE OR REPLACE TABLE classified AS
            SELECT
                i.*,
                CASE
                    WHEN {null_check}                 THEN 'new'
                    WHEN i._row_hash != c._row_hash   THEN 'changed'
                    ELSE                                   'unchanged'
                END AS _scd_class
            FROM incoming i
            LEFT JOIN current_hashes c
                   ON {join_cond}
        """)

    def _write_new(self, table_name: str, now: str, count: int) -> int:
        """Insert brand-new NK rows (``_scd_class = 'new'``) into *table_name*.

        ``metadata_diff`` is ``NULL`` for initial inserts — there is no
        previous version to diff against.  *count* is the pre-computed row
        count used purely for the progress log.
        """
        if count == 0:
            return 0
        nk_select = ", ".join(self._nk_cols)
        cols      = self._nk_cols + self._EXTRA_COLS
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_rows AS
            SELECT {nk_select},
                   '{now}'::TIMESTAMPTZ  AS valid_from,
                   NULL                  AS valid_to,
                   TRUE                  AS is_current,
                   _metadata             AS metadata,
                   NULL                  AS metadata_diff,
                   _row_hash             AS row_hash
            FROM classified
            WHERE _scd_class = 'new'
        """)
        return self.sink.write_named(
            self._con, "new_rows", table_name, cols, self.batch_size
        )

    def _write_changed(self, table_name: str, now: str, count: int) -> int:
        """Expire changed rows and insert new versions with ``metadata_diff``.

        Steps in strict order:

        1. Extract ``changed_nks`` TABLE — NKs whose hash changed (small subset).
        2. Fetch ``old_metadata_snapshot`` TABLE — only the NKs in step 1 are
           read from PostgreSQL, so this is a small targeted fetch.
        3. Call :meth:`~sqldim.sinks.base.SinkAdapter.close_versions` to
           expire old rows.
        4. Insert new versions; ``metadata_diff`` is set to the snapshotted
           old metadata.  Intermediate tables are dropped immediately after use.
        """
        if count == 0:
            return 0
        nk_select = ", ".join(self._nk_cols)

        # Step 1 — materialise changed NKs (small subset of classified)
        self._con.execute(f"""
            CREATE OR REPLACE TABLE changed_nks AS
            SELECT {nk_select}
            FROM classified
            WHERE _scd_class = 'changed'
        """)

        # Step 2 — fetch old metadata only for changed NKs (targeted PG read)
        db_sql   = self.sink.current_state_sql(table_name)
        nk_d_sel = ", ".join(f"d.{c}" for c in self._nk_cols)
        join_cnd = " AND ".join(f"d.{c} = c.{c}" for c in self._nk_cols)
        self._con.execute(f"""
            CREATE OR REPLACE TABLE old_metadata_snapshot AS
            SELECT {nk_d_sel},
                   d.metadata AS _old_metadata
            FROM {_as_subquery(db_sql)} d
            JOIN changed_nks c
              ON {join_cnd}
            WHERE d.is_current = TRUE
        """)

        # Step 3 — expire old versions
        self.sink.close_versions(
            self._con, table_name, self._nk_cols, "changed_nks", now
        )
        self._con.execute("DROP TABLE IF EXISTS changed_nks")

        # Step 4 — insert new versions with metadata_diff = old snapshot
        cols      = self._nk_cols + self._EXTRA_COLS
        nk_cl_sel = ", ".join(f"cl.{c}" for c in self._nk_cols)
        cl_join   = " AND ".join(f"cl.{c} = ms.{c}" for c in self._nk_cols)
        self._con.execute(f"""
            CREATE OR REPLACE VIEW new_versions AS
            SELECT {nk_cl_sel},
                   '{now}'::TIMESTAMPTZ  AS valid_from,
                   NULL                  AS valid_to,
                   TRUE                  AS is_current,
                   cl._metadata          AS metadata,
                   ms._old_metadata      AS metadata_diff,
                   cl._row_hash          AS row_hash
            FROM classified cl
            JOIN old_metadata_snapshot ms
              ON {cl_join}
            WHERE cl._scd_class = 'changed'
        """)
        written = self.sink.write_named(
            self._con, "new_versions", table_name, cols, self.batch_size
        )
        self._con.execute("DROP TABLE IF EXISTS old_metadata_snapshot")
        return written

    # ── Streaming helpers ─────────────────────────────────────────────────

    def _register_source_from_sql(self, sql_fragment: str) -> None:
        """Materialise *sql_fragment* as the ``incoming`` TABLE (streaming variant).

        Identical to :meth:`_register_source` but accepts a pre-resolved SQL
        string instead of a source object.  Used by :meth:`process_stream` to
        bypass :func:`~sqldim.sources.coerce_source`.
        """
        nk_select = ", ".join(self._nk_cols)

        if self._meta_cols:
            pairs    = ", ".join(f"{c} := {c}" for c in self._meta_cols)
            meta_sql = f"to_json(struct_pack({pairs}))"
        else:
            meta_sql = "'{}'"

        nk_not_null = " AND ".join(f"{c} IS NOT NULL" for c in self._nk_cols)

        self._con.execute(f"""
            CREATE OR REPLACE TABLE incoming AS
            WITH _src AS (
                SELECT {nk_select},
                       {meta_sql} AS _metadata
                FROM ({sql_fragment})
                WHERE {nk_not_null}
            )
            SELECT {nk_select},
                   _metadata,
                   md5(cast(_metadata AS varchar)) AS _row_hash
            FROM _src
        """)

    def _update_local_hashes_after_batch(self) -> None:
        """Sync local current_hashes TABLE with rows written in this batch.

        Enables correct cross-batch NK detection without re-scanning the
        remote sink.
        """
        nk_select = ", ".join(self._nk_cols)
        nk_match  = " AND ".join(
            f"ch.{c} = cl.{c}" for c in self._nk_cols
        )
        self._con.execute(f"""
            INSERT INTO current_hashes ({nk_select}, _row_hash)
            SELECT {nk_select}, _row_hash
            FROM classified
            WHERE _scd_class = 'new'
        """)
        self._con.execute(f"""
            UPDATE current_hashes ch
            SET _row_hash = cl._row_hash
            FROM classified cl
            WHERE {nk_match}
            AND cl._scd_class = 'changed'
        """)

    def process_stream(
        self,
        source,
        table_name: str,
        batch_size: int = 1_000_000,
        max_batches: int | None = None,
        on_batch=None,
        now: str | None = None,
    ):
        """Run the SCD-2 metadata cycle in streaming mode.

        Two-phase approach:

        * **Phase 1 (bootstrap)** — Materialise ``current_hashes`` TABLE once.
          Remote source (PostgreSQL, MotherDuck) is scanned exactly once.
        * **Phase 2 (stream)** — Each batch is classified against the local
          TABLE; no further remote scans occur regardless of batch count.

        Parameters
        ----------
        source      : Any object with a ``.stream(con, batch_size)`` generator
                      (e.g. :class:`~sqldim.sources.csv_stream.CSVStreamSource`).
        table_name  : Target dimension table in the sink.
        batch_size  : Rows per micro-batch.
        max_batches : Stop after *N* batches; ``None`` = run until exhausted.
        on_batch    : ``callback(batch_num: int, result: SCDResult)`` invoked
                      after each successful write.
        now         : Fixed ISO-8601 timestamp for ``valid_from`` / ``valid_to``;
                      defaults to current UTC time.

        Returns
        -------
        :class:`~sqldim.sources.stream.StreamResult`
        """
        import logging
        from sqldim.sources.stream import StreamResult
        from sqldim.scd.handler import SCDResult as _SCDResult

        _log  = logging.getLogger(__name__)
        _safe(table_name)

        if now is None:
            now = datetime.now(timezone.utc).isoformat()

        result = StreamResult()

        # Phase 1: Bootstrap slim fingerprint once.
        _log.info(f"[sqldim] {table_name}: bootstrapping hash fingerprint …")
        self._register_current_hashes(table_name)

        # Phase 2: Stream batches, each classified against the local TABLE.
        for i, sql_fragment in enumerate(source.stream(self._con, batch_size)):
            if max_batches is not None and i >= max_batches:
                break

            try:
                self._register_source_from_sql(sql_fragment)
                self._classify()

                # Drop incoming to free memory before writes.
                self._con.execute("DROP TABLE IF EXISTS incoming")

                stats = self._con.execute("""
                    SELECT countif(_scd_class = 'new')       AS n_new,
                           countif(_scd_class = 'changed')   AS n_changed,
                           countif(_scd_class = 'unchanged') AS n_unchanged
                    FROM classified
                """).fetchone()
                n_new, n_changed, n_unchanged = stats[0], stats[1], stats[2]
                _log.info(
                    f"[sqldim] {table_name}: batch {i+1} — "
                    f"{n_new:,} new, {n_changed:,} changed, {n_unchanged:,} unchanged"
                )

                batch_result           = _SCDResult()
                batch_result.inserted  = self._write_new(table_name, now, n_new)
                batch_result.versioned = self._write_changed(table_name, now, n_changed)
                batch_result.unchanged = n_unchanged

                # Keep local hashes in sync for cross-batch NK detection.
                self._update_local_hashes_after_batch()

                self._con.execute("DROP TABLE IF EXISTS classified")

                offset = source.checkpoint()
                source.commit(offset)

                result.accumulate(batch_result)
                result.batches_processed += 1

                if on_batch:
                    on_batch(i, batch_result)

            except Exception as exc:
                result.batches_failed += 1
                _log.error("Batch %d failed for table %s: %s", i, table_name, exc)
                self._con.execute("DROP TABLE IF EXISTS incoming")
                self._con.execute("DROP TABLE IF EXISTS classified")

        # Phase 3: Drop bootstrap TABLE after all batches.
        self._con.execute("DROP TABLE IF EXISTS current_hashes")
        return result
