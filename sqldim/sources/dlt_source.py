"""sqldim/sources/dlt_source.py

DltSource — wraps a dlt resource or pipeline dataset as a sqldim SourceAdapter.

dlt handles extraction, normalisation, and incremental loading.
sqldim reads from what dlt staged, registering it as a DuckDB VIEW
for zero-copy downstream processing.

Two usage modes
---------------
1. **Resource mode** — run dlt in-process, sharing sqldim's DuckDB connection::

       from sqldim.sources.dlt_source import DltSource
       from my_pipeline import salesforce_source

       with DuckDBSink("/tmp/warehouse.duckdb") as sink:
           proc = LazySCDProcessor("account_id", track_cols, sink)
           proc.process(
               DltSource(salesforce_source().accounts),
               "dim_account",
           )

2. **Dataset mode** — read from a dlt dataset that was already loaded into
   a DuckDB file, without running dlt again::

       DltSource.from_dataset(
           duckdb_path="/tmp/pipeline.duckdb",
           table="accounts",
           dataset_name="salesforce_staging",
       )

Optional dependency
-------------------
``dlt`` is not listed in sqldim's core dependencies.  Import errors are raised
at construction time (resource mode) or as_sql() time (dataset mode)
with a clear install hint, so the rest of sqldim is unaffected when dlt is
absent.
"""

from __future__ import annotations


def _require_dlt():
    try:
        import dlt as _dlt  # type: ignore[import-not-found]

        return _dlt
    except ImportError:  # pragma: no cover
        raise ImportError(
            "DltSource requires the 'dlt' package.  Install it with:  pip install dlt"
        ) from None


class DltSource:
    """
    Wrap a dlt resource as a sqldim SourceAdapter.

    dlt extracts and normalises; sqldim registers the result as a DuckDB VIEW
    for lazy, zero-copy downstream processing — no intermediate file written.

    Parameters
    ----------
    resource :
        A ``dlt.Resource`` (e.g. ``my_source().my_table``).
    pipeline_name :
        Name passed to ``dlt.pipeline()``.  Defaults to ``"sqldim_staging"``.
    dataset_name :
        dlt dataset (schema) name inside the DuckDB database.
        Defaults to ``pipeline_name``.

    Notes
    -----
    * When *resource* is a generator function decorated with ``@dlt.resource``,
      pass the *called* value (``resource()``) not the function itself.
    * dlt is run with ``destination="duckdb"`` and the active connection is
      shared so the staged table is visible to subsequent sqldim operations.
    """

    def __init__(
        self,
        resource,
        pipeline_name: str = "sqldim_staging",
        dataset_name: str | None = None,
    ) -> None:
        _require_dlt()
        self._resource = resource
        self._pipeline_name = pipeline_name
        self._dataset_name = dataset_name or pipeline_name

    # ------------------------------------------------------------------
    # SourceAdapter protocol
    # ------------------------------------------------------------------

    def as_sql(self, con) -> str:
        """
        Run the dlt resource into the shared DuckDB connection and return
        a fully-qualified ``schema.table`` reference usable in a FROM clause.
        """
        dlt = _require_dlt()

        pipeline = dlt.pipeline(
            pipeline_name=self._pipeline_name,
            destination="duckdb",
            dataset_name=self._dataset_name,
            credentials=con,
        )
        pipeline.run(self._resource)

        table = getattr(self._resource, "name", None) or self._resource.__name__
        return f"SELECT * FROM {self._dataset_name}.{table}"

    # ------------------------------------------------------------------
    # Alternative constructor — dataset mode
    # ------------------------------------------------------------------

    @classmethod
    def from_dataset(
        cls,
        duckdb_path: str,
        table: str,
        dataset_name: str = "sqldim_staging",
    ) -> "_DatasetSource":
        """
        Read from a dlt dataset that was already loaded into a DuckDB file.

        No dlt process is run; this is a thin wrapper over DuckDBSource
        that understands dlt's schema-qualified table naming.

        Parameters
        ----------
        duckdb_path :
            Path to the ``.duckdb`` file dlt wrote to.
        table :
            The dlt table name (typically the resource name).
        dataset_name :
            dlt dataset (schema) name.  Defaults to ``"sqldim_staging"``.

        Returns
        -------
        _DatasetSource
            A SourceAdapter that opens *duckdb_path* and reads the table.
        """
        return _DatasetSource(duckdb_path, table, dataset_name)


class _DatasetSource:
    """
    Read from an already-populated dlt DuckDB dataset.

    Returned by ``DltSource.from_dataset()``; not intended to be
    instantiated directly.
    """

    def __init__(self, duckdb_path: str, table: str, dataset_name: str) -> None:
        self._path = duckdb_path
        self._table = table
        self._dataset_name = dataset_name

    def as_sql(self, con) -> str:
        """
        Attach the dlt DuckDB file to *con* and return a qualified SELECT.

        The attached database alias is derived from the file path so that
        multiple datasets can be attached to the same connection at once.
        """
        import os

        alias = os.path.splitext(os.path.basename(self._path))[0]
        # ATTACH is idempotent when the alias already exists
        try:
            con.execute(f"ATTACH '{self._path}' AS {alias} (READ_ONLY)")
        except Exception:
            pass  # already attached
        return f"SELECT * FROM {alias}.{self._dataset_name}.{self._table}"
