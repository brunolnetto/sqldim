"""
sqldim/processors/sql_transforms.py

SQL-expression transform DSL for the DuckDB lazy processing path.

Unlike :class:`~sqldim.core.processors.transforms.TransformPipeline` (which uses
Narwhals and materialises data in Python), :class:`SQLTransform` and
:class:`SQLTransformPipeline` operate entirely inside DuckDB — zero Python
data movement regardless of table size.
"""
from __future__ import annotations


class SQLTransform:
    """A single SQL column expression applied to the DuckDB ``incoming`` view.

    The expression is evaluated inside DuckDB before checksum computation so
    transformations are reflected in change detection.

    Parameters
    ----------
    column     : Target column name to replace (must exist in the source).
    expression : Arbitrary DuckDB SQL expression that produces the new value,
                 e.g. ``"upper(trim(razao_social))"``,
                 ``"round(capital_social::double, 2)"``,
                 or a full ``CASE WHEN … END`` block.

    Usage::

        from sqldim.core.kimball.dimensions.scd.processors.sql_transforms import SQLTransform, SQLTransformPipeline

        pipeline = SQLTransformPipeline([
            SQLTransform("razao_social", "upper(trim(razao_social))"),
            SQLTransform("capital_social", "round(capital_social::double, 2)"),
            SQLTransform(
                "situacao_cadastral",
                "CASE situacao_cadastral "
                "WHEN '01' THEN 'NULA' "
                "WHEN '02' THEN 'ATIVA' "
                "ELSE situacao_cadastral END",
            ),
        ])
    """

    def __init__(self, column: str, expression: str) -> None:
        self.column     = column
        self.expression = expression

    def as_select_expr(self) -> str:
        """Return ``"<expression> AS <column>"`` for use in a SELECT list."""
        return f"{self.expression} AS {self.column}"


class SQLTransformPipeline:
    """Apply a list of :class:`SQLTransform` objects to a DuckDB view in-place.

    Rewrites *source_view* by replacing transformed columns while keeping all
    other columns unchanged.  The result is written back to *source_view* via
    ``CREATE OR REPLACE VIEW`` — no Python data materialisation.

    Parameters
    ----------
    transforms : Ordered list of :class:`SQLTransform` instances.

    Usage::

        pipeline = SQLTransformPipeline([
            SQLTransform("name", "upper(trim(name))"),
        ])
        pipeline.apply(con, "incoming")   # rewrites the ``incoming`` view
    """

    def __init__(self, transforms: list[SQLTransform]) -> None:
        self.transforms = transforms

    def apply(self, con, source_view: str = "incoming") -> None:
        """Rewrite *source_view* applying all transforms.

        If the pipeline has no transforms this is a no-op.
        The column order in *source_view* is preserved.
        """
        if not self.transforms:
            return
        transform_exprs = {t.column: t.as_select_expr() for t in self.transforms}
        # Materialise a snapshot TABLE to avoid self-referential view recursion.
        snap = f"_sqldim_xform_{source_view}"
        con.execute(f"DROP TABLE IF EXISTS {snap}")
        con.execute(f"CREATE TABLE {snap} AS SELECT * FROM {source_view}")
        columns = con.execute(f"DESCRIBE {snap}").fetchall()
        select_parts = []
        for col_name, *_ in columns:
            if col_name in transform_exprs:
                select_parts.append(transform_exprs[col_name])
            else:
                select_parts.append(col_name)
        con.execute(f"""
            CREATE OR REPLACE VIEW {source_view} AS
            SELECT {', '.join(select_parts)}
            FROM {snap}
        """)
