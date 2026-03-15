from sqldim.sources.base         import SourceAdapter
from sqldim.sources.parquet      import ParquetSource
from sqldim.sources.csv          import CSVSource
from sqldim.sources.duckdb_source import DuckDBSource
from sqldim.sources.postgresql   import PostgreSQLSource
from sqldim.sources.delta        import DeltaSource
from sqldim.sources.sql          import SQLSource
from sqldim.sources.dlt_source   import DltSource, _DatasetSource

__all__ = [
    "SourceAdapter",
    "ParquetSource",
    "CSVSource",
    "DuckDBSource",
    "PostgreSQLSource",
    "DeltaSource",
    "SQLSource",
    "DltSource",
    "_DatasetSource",
    "coerce_source",
]


def _is_parquet_path(lower: str) -> bool:
    return any(lower.endswith(ext) for ext in (".parquet", ".parq")) or "parquet" in lower


def _is_csv_path(lower: str) -> bool:
    return lower.endswith(".csv") or lower.endswith(".tsv")


def coerce_source(source) -> SourceAdapter:
    """
    Backward-compatibility wrapper.

    Accepts a SourceAdapter or a bare string and returns a SourceAdapter.
    String heuristic:
      *.parquet / *.parq / glob containing "parquet" → ParquetSource
      *.csv / *.tsv                                  → CSVSource
      anything else                                  → DuckDBSource
                                                       (table/view name)

    Limitation: bare S3 prefixes (e.g. ``"s3://bucket/prefix/"``) cannot be
    heuristically classified — they carry no file extension.  Pass an explicit
    ``ParquetSource("s3://bucket/prefix/*.parquet")`` (or CSV/Delta variant)
    instead of relying on ``coerce_source`` for remote paths.

    All existing call sites that pass a string path continue to work
    without modification.
    """
    if isinstance(source, SourceAdapter):
        return source
    if isinstance(source, str):
        lower = source.lower()
        if _is_parquet_path(lower):
            return ParquetSource(source)
        if _is_csv_path(lower):
            return CSVSource(source)
        return DuckDBSource(source)
    raise TypeError(
        f"source must be a SourceAdapter or str, got {type(source).__name__}"
    )
