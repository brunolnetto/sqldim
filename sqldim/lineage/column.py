"""Column-level lineage facets — Column-Level Lineage ADR.

Two components:
1. :class:`ColumnLineageEntry` / :class:`ColumnLineageFacet` — data model.
2. :func:`extract_declared_lineage` — reads ``source_column`` / ``source_columns``
   metadata from a SQLModel class and returns a :class:`ColumnLineageFacet`.

Both declarative mode (metadata-driven, zero deps) and automatic mode
(sqlglot-based SQL AST analysis, optional dep) are supported.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    pass


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class ColumnLineageEntry:
    """Describes how one output column is derived from one or more input columns.

    Parameters
    ----------
    output_column:
        The name of the column in the output (target) table.
    input_columns:
        One or more ``"table.column"`` or bare ``"column"`` references that
        contribute to *output_column*.
    transform_description:
        Human-readable description of the transformation (e.g.
        ``"UPPER(TRIM(raw_name))"``, ``"Direct mapping"``).
    transform_sql:
        Raw SQL fragment describing the transformation (Mode 2 / parsed only).
    confidence:
        ``"declared"`` for Mode 1 (user-annotated), ``"parsed"`` for Mode 2
        (sqlglot-derived), ``"inferred"`` for structural FK lineage.
    """

    output_column: str
    input_columns: list[str]
    transform_description: str = "Direct mapping"
    transform_sql: str | None = None
    confidence: str = "declared"

    def to_dict(self) -> dict[str, Any]:
        return {
            "outputColumn": self.output_column,
            "inputColumns": self.input_columns,
            "transformDescription": self.transform_description,
            "transformSql": self.transform_sql,
            "confidence": self.confidence,
        }


@dataclass
class ColumnLineageFacet:
    """Container for all column lineage entries on a single dataset.

    Attaches to :class:`~sqldim.lineage.events.DatasetRef` ``facets`` dict
    under the key ``"columnLineage"``.  Serialised into the OpenLineage
    ``SchemaDatasetFacet.fields[].inputFields`` format by
    :class:`~sqldim.lineage.emitter.OpenLineageEmitter`.
    """

    entries: list[ColumnLineageEntry] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "_producer": "sqldim",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
            "fields": [e.to_dict() for e in self.entries],
        }

    def to_openlineage_fields(self) -> list[dict[str, Any]]:
        """Return the OpenLineage ``SchemaDatasetFacet.fields`` list."""
        return [_entry_to_ol_field(e) for e in self.entries]


# ---------------------------------------------------------------------------
# OpenLineage field helpers
# ---------------------------------------------------------------------------


def _col_ref_parts(col: str) -> tuple[str, str]:
    """Return (namespace, name) for a column reference string."""
    if "." in col:
        return col.split(".")[0], col.split(".")[-1]
    return "", col


def _entry_to_ol_field(entry: ColumnLineageEntry) -> dict[str, Any]:
    """Convert one :class:`ColumnLineageEntry` to an OpenLineage field dict."""
    ol_field: dict[str, Any] = {"name": entry.output_column}
    if entry.input_columns:
        transform_type = (
            "IDENTITY" if entry.transform_description == "Direct mapping" else "CUSTOM"
        )
        ol_field["inputFields"] = [
            {
                "namespace": _col_ref_parts(col)[0],
                "name": _col_ref_parts(col)[1],
                "field": _col_ref_parts(col)[1],
                "transformDescription": entry.transform_description,
                "transformType": transform_type,
            }
            for col in entry.input_columns
        ]
    return ol_field


# ---------------------------------------------------------------------------
# Mode 1: Declarative extraction from SQLModel metadata
# ---------------------------------------------------------------------------


def _entry_from_col(col: Any) -> "ColumnLineageEntry | None":
    """Build a :class:`ColumnLineageEntry` from one SQLAlchemy column, or ``None``."""
    info: dict = col.info or {}
    src_col: str | None = info.get("source_column")
    src_cols: list | None = info.get("source_columns")
    transform: str = info.get("transform_description") or "Direct mapping"
    if src_col:
        return ColumnLineageEntry(
            output_column=col.name,
            input_columns=[src_col],
            transform_description=transform,
            confidence="declared",
        )
    if src_cols:
        return ColumnLineageEntry(
            output_column=col.name,
            input_columns=list(src_cols),
            transform_description=transform,
            confidence="declared",
        )
    return None


def extract_declared_lineage(model_cls: Any) -> ColumnLineageFacet:
    """Build a :class:`ColumnLineageFacet` from ``Field()`` metadata on *model_cls*.

    Reads ``source_column`` / ``source_columns`` / ``transform_description``
    stored in ``column.info`` by the dimensional ``Field()`` factory.

    Parameters
    ----------
    model_cls:
        A SQLModel class whose columns may carry lineage metadata declared
        with ``Field(source_column=...)``.

    Returns
    -------
    ColumnLineageFacet
        Possibly empty (no ``source_column`` declarations) — callers should
        check ``facet.entries`` before attaching.
    """
    table = getattr(model_cls, "__table__", None)
    if table is None:
        return ColumnLineageFacet()
    entries = [e for col in table.columns if (e := _entry_from_col(col)) is not None]
    return ColumnLineageFacet(entries=entries)


def extract_structural_lineage(model_cls: Any) -> ColumnLineageFacet:
    """Infer structural lineage from FK metadata on *model_cls* at no cost.

    Fact FK columns → mapped to the natural key of the referenced dimension,
    giving free structural lineage without any user declarations.

    Parameters
    ----------
    model_cls:
        A SQLModel class, typically a :class:`~sqldim.core.kimball.models.FactModel`.

    Returns
    -------
    ColumnLineageFacet
        Entries with ``confidence="inferred"`` for each FK column.
    """
    entries: list[ColumnLineageEntry] = []

    table = getattr(model_cls, "__table__", None)
    if table is None:
        return ColumnLineageFacet()

    for col in table.columns:
        info: dict = col.info or {}
        fk_target: str | None = info.get("foreign_key_target")
        if fk_target:
            entries.append(
                ColumnLineageEntry(
                    output_column=col.name,
                    input_columns=[fk_target],
                    transform_description=f"FK → {fk_target}",
                    confidence="inferred",
                )
            )

    return ColumnLineageFacet(entries=entries)


# ---------------------------------------------------------------------------
# Mode 2: Automatic SQL AST analysis (optional sqlglot dep)
# ---------------------------------------------------------------------------


def _col_ref_name(col_ref: Any) -> str:
    return f"{col_ref.table}.{col_ref.name}" if col_ref.table else col_ref.name


def _is_direct_mapping(source_cols: list, alias: str) -> bool:
    return len(source_cols) == 1 and source_cols[0].endswith(alias)


def _select_to_entry(select_expr: Any, sqlglot: Any) -> ColumnLineageEntry:
    """Convert one sqlglot SELECT expression to a :class:`ColumnLineageEntry`."""
    alias = select_expr.alias or str(select_expr)
    source_cols = [_col_ref_name(c) for c in select_expr.find_all(sqlglot.exp.Column)]
    transform_sql = select_expr.sql(dialect="duckdb")
    direct = _is_direct_mapping(source_cols, alias)
    return ColumnLineageEntry(
        output_column=alias,
        input_columns=source_cols,
        transform_description="Direct mapping" if direct else transform_sql,
        transform_sql=None if direct else transform_sql,
        confidence="parsed",
    )


def _parse_lineage_entries(sql: str, sqlglot: Any) -> list[ColumnLineageEntry]:
    """Parse *sql* AST and return lineage entries, or ``[]`` on failure."""
    try:
        parsed = sqlglot.parse_one(sql, dialect="duckdb")
        if parsed is None:
            return []
        return [_select_to_entry(sel, sqlglot) for sel in parsed.selects]
    except Exception:
        return []


def extract_sql_lineage(sql: str, output_table: str) -> ColumnLineageFacet:
    """Parse *sql* with sqlglot and extract column-level lineage.

    Requires the optional ``sqlglot`` dependency.  Raises :exc:`ImportError`
    with a helpful message if not installed.

    Parameters
    ----------
    sql:
        A ``SELECT`` statement (or CTE chain) whose output columns are being
        traced.  Typically the SQL emitted by a lazy processor.
    output_table:
        The logical name of the output dataset (used to qualify output columns
        in the returned entries).

    Returns
    -------
    ColumnLineageFacet
        Entries with ``confidence="parsed"`` for each resolved column.
    """
    try:
        import sqlglot
        import sqlglot.lineage  # noqa: F401 — ensures submodule is loaded
    except ImportError:
        raise ImportError(
            "sqlglot is required for automatic SQL lineage analysis. "
            "Install it with: pip install sqlglot"
        ) from None

    return ColumnLineageFacet(entries=_parse_lineage_entries(sql, sqlglot))
