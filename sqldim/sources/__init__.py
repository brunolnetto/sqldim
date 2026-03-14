from sqldim.sources.base         import SourceAdapter
from sqldim.sources.parquet      import ParquetSource
from sqldim.sources.csv          import CSVSource
from sqldim.sources.duckdb_source import DuckDBSource
from sqldim.sources.postgresql   import PostgreSQLSource
from sqldim.sources.delta        import DeltaSource
from sqldim.sources.sql          import SQLSource

__all__ = [
    "SourceAdapter",
    "ParquetSource",
    "CSVSource",
    "DuckDBSource",
    "PostgreSQLSource",
    "DeltaSource",
    "SQLSource",
    "coerce_source",
]


def coerce_source(source) -> SourceAdapter:
    """
    Backward-compatibility wrapper.

    Accepts a SourceAdapter or a bare string and returns a SourceAdapter.
    String heuristic:
      *.parquet / *.parq / glob containing "parquet" → ParquetSource
      *.csv / *.tsv                                  → CSVSource
      anything else                                  → DuckDBSource
                                                       (table/view name)

    All existing call sites that pass a string path continue to work
    without modification.
    """
    if isinstance(source, SourceAdapter):
        return source
    if isinstance(source, str):
        lower = source.lower()
        if any(lower.endswith(ext) for ext in (".parquet", ".parq")) or "parquet" in lower:
            return ParquetSource(source)
        if lower.endswith(".csv") or lower.endswith(".tsv"):
            return CSVSource(source)
        return DuckDBSource(source)
    raise TypeError(
        f"source must be a SourceAdapter or str, got {type(source).__name__}"
    )
