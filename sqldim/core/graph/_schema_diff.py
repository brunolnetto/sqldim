"""SchemaDiff — structural diff result for SchemaGraph.diff().

Extracted from schema_graph.py to keep file sizes manageable.
"""

from __future__ import annotations

from dataclasses import dataclass, field


def _names(models: list) -> list[str]:
    return [m.__name__ for m in models]


def _model_summary_lines(label: str, sign: str, models: list) -> list[str]:
    if not models:
        return []
    return [f"  {sign} {label}: {_names(models)}"]


def _nonempty_col_diff_lines(column_diffs: list) -> list[str]:
    return [
        f"  ~ {cd.model_name} ({cd.model_kind}): +{cd.added_columns} -{cd.removed_columns}"
        for cd in column_diffs
        if not cd.is_empty
    ]


def _model_cols(cls: "type | None") -> set:
    return (
        set(cls.model_fields.keys()) if cls and hasattr(cls, "model_fields") else set()
    )


@dataclass
class ColumnDiff:
    """Column-level change between two schema versions."""

    model_name: str
    model_kind: str  # "vertex" | "edge"
    added_columns: list[str]
    removed_columns: list[str]

    @property
    def is_empty(self) -> bool:
        return not self.added_columns and not self.removed_columns


@dataclass
class SchemaDiff:
    """
    Structural difference between two :class:`SchemaGraph` snapshots.

    Returned by :meth:`SchemaGraph.diff`.  Provides a summary of all
    model-level and column-level changes — a graph edit distance summary.

    Attributes
    ----------
    added_vertices, removed_vertices:
        DimensionModel classes that appeared or disappeared.
    added_edges, removed_edges:
        FactModel classes that appeared or disappeared.
    column_diffs:
        Per-model column-level changes (both vertices and edges).
    """

    added_vertices: list[type] = field(default_factory=list)
    removed_vertices: list[type] = field(default_factory=list)
    added_edges: list[type] = field(default_factory=list)
    removed_edges: list[type] = field(default_factory=list)
    column_diffs: list[ColumnDiff] = field(default_factory=list)

    @property
    def edit_distance(self) -> int:
        """
        Rough graph edit distance: count of added/removed nodes,
        added/removed edges, and column-level changes.
        """
        col_changes = sum(
            len(cd.added_columns) + len(cd.removed_columns) for cd in self.column_diffs
        )
        return (
            len(self.added_vertices)
            + len(self.removed_vertices)
            + len(self.added_edges)
            + len(self.removed_edges)
            + col_changes
        )

    @property
    def is_empty(self) -> bool:
        return self.edit_distance == 0

    def summary(self) -> str:
        lines = [f"SchemaDiff (edit_distance={self.edit_distance})"]
        lines += _model_summary_lines("vertices", "+", self.added_vertices)
        lines += _model_summary_lines("vertices", "-", self.removed_vertices)
        lines += _model_summary_lines("edges", "+", self.added_edges)
        lines += _model_summary_lines("edges", "-", self.removed_edges)
        lines += _nonempty_col_diff_lines(self.column_diffs)
        return "\n".join(lines)


def _collect_column_diff(
    name: str,
    kind: str,
    old_cls: "type | None",
    new_cls: "type | None",
    out: list[ColumnDiff],
) -> None:
    """Compute added/removed columns between two model classes."""
    old_cols = _model_cols(old_cls)
    new_cols = _model_cols(new_cls)
    added = sorted(new_cols - old_cols)
    removed = sorted(old_cols - new_cols)
    if added or removed:
        out.append(
            ColumnDiff(
                model_name=name,
                model_kind=kind,
                added_columns=added,
                removed_columns=removed,
            )
        )


__all__ = ["ColumnDiff", "SchemaDiff", "_collect_column_diff"]
