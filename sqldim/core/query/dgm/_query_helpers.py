"""Private helper functions for DGMQuery (DGM §3).

These functions are module-level helpers extracted from core.py to keep
DGMQuery's module within the 400-line budget.  They are not part of the
public API.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from sqldim.exceptions import SemanticError
from sqldim.core.query.dgm.refs import AggRef, PropRef, SignatureRef, WinRef
from sqldim.core.query.dgm.preds import (
    AND,
    NOT,
    OR,
    PathPred,
    RawPred,
    ScalarPred,
    SignaturePred,
)

if TYPE_CHECKING:
    from sqldim.core.query.dgm.core import DGMQuery


# ---------------------------------------------------------------------------
# Model-first FK inference
# ---------------------------------------------------------------------------


def _collect_fk_matches(to_model: type, from_table_name: str) -> list[str]:
    """Return FK column names on *to_model* that point at *from_table_name*."""
    if not hasattr(to_model, "__table__"):
        raise SemanticError(
            f"{to_model.__name__} has no SQLAlchemy table metadata. "
            "Is it a SQLModel class with table=True?"
        )
    matches: list[str] = []
    for col_name, col in to_model.__table__.columns.items():
        for fk in col.foreign_keys:
            if fk.target_fullname.rsplit(".", 1)[0] == from_table_name:
                matches.append(col_name)
    return matches


def _infer_on(
    from_alias: str,
    to_alias: str,
    to_model: type,
    from_table_name: str,
) -> str:
    """Derive an ON clause '{from_alias}.id = {to_alias}.fk' via FK metadata.

    Raises :class:`~sqldim.exceptions.SemanticError` when no FK is found or
    more than one FK column targets the same table (role-playing ambiguity —
    pass *on=* explicitly in that case).
    """
    matches = _collect_fk_matches(to_model, from_table_name)
    if not matches:
        raise SemanticError(
            f"No FK from {to_model.__name__!r} → {from_table_name!r} found. "
            "Specify on= explicitly."
        )
    if len(matches) > 1:
        raise SemanticError(
            f"Ambiguous FK: {to_model.__name__!r} has {len(matches)} FK columns "
            f"pointing at {from_table_name!r} ({', '.join(matches)}). "
            "Specify on= explicitly."
        )
    return f"{from_alias}.id = {to_alias}.{matches[0]}"


# ---------------------------------------------------------------------------
# Ref-kind validation helpers
# ---------------------------------------------------------------------------


def _iter_scalar_refs(pred: object) -> Iterator[object]:
    """Recursively yield every Ref object found inside *pred*'s ScalarPred leaves."""
    if isinstance(pred, ScalarPred):
        yield pred.ref
    elif isinstance(pred, (AND, OR)):
        for sub in pred.preds:
            yield from _iter_scalar_refs(sub)
    elif isinstance(pred, NOT):
        yield from _iter_scalar_refs(pred.pred)
    # PathPred has no Ref inside; its sub_filter refs would be band-scoped separately


def _has_path_pred(pred: object) -> bool:
    """Return True if *pred* contains any PathPred node."""
    if isinstance(pred, PathPred):
        return True
    if isinstance(pred, (AND, OR)):
        return any(_has_path_pred(sub) for sub in pred.preds)
    if isinstance(pred, NOT):
        return _has_path_pred(pred.pred)
    return False


def _has_signature_pred(pred: object) -> bool:
    """Return True if *pred* subtree contains any SignaturePred node."""
    if isinstance(pred, SignaturePred):
        return True
    if isinstance(pred, (AND, OR)):
        return any(_has_signature_pred(sub) for sub in pred.preds)
    if isinstance(pred, NOT):
        return _has_signature_pred(pred.pred)
    return False


def _check_where_pred(pred: object) -> None:
    if isinstance(pred, (str, RawPred)):
        return  # raw SQL strings bypass Ref-kind checking
    for ref in _iter_scalar_refs(pred):
        if isinstance(ref, (AggRef, WinRef)):
            raise SemanticError(
                f"B1 Where only admits PropRef; found {type(ref).__name__}. "
                "Use Having (B2) for AggRef or Qualify (B3) for WinRef."
            )


def _reject_band_pred(pred: object, band_label: str) -> None:
    """Raise SemanticError if *pred* contains PathPred or SignaturePred."""
    if _has_path_pred(pred):
        raise SemanticError(f"PathPred is not allowed in {band_label}.")
    if _has_signature_pred(pred):
        raise SemanticError(
            f"SignaturePred is not allowed in {band_label}; use Where (B1)."
        )


def _check_having_pred(pred: object) -> None:
    _reject_band_pred(pred, "Having (B2)")
    for ref in _iter_scalar_refs(pred):
        if isinstance(ref, (PropRef, WinRef)):
            raise SemanticError(
                f"B2 Having only admits AggRef; found {type(ref).__name__}. "
                "Use Where (B1) for PropRef or Qualify (B3) for WinRef."
            )
        if isinstance(ref, SignatureRef):
            raise SemanticError("SignatureRef is B1-only; not allowed in Having (B2).")


def _check_qualify_pred(pred: object) -> None:
    _reject_band_pred(pred, "Qualify (B3)")
    for ref in _iter_scalar_refs(pred):
        if isinstance(ref, (PropRef, AggRef)):
            raise SemanticError(
                f"B3 Qualify only admits WinRef; found {type(ref).__name__}. "
                "Use Where (B1) for PropRef or Having (B2) for AggRef."
            )
        if isinstance(ref, SignatureRef):
            raise SemanticError("SignatureRef is B1-only; not allowed in Qualify (B3).")


# ---------------------------------------------------------------------------
# DGMQuery helpers
# ---------------------------------------------------------------------------


def _validate_b2(q: "DGMQuery") -> None:
    if q._having_pred is not None and not q._agg_exprs:
        raise SemanticError("B2: .having() requires .agg() expressions")


def _validate_b3(q: "DGMQuery") -> None:
    if q._qualify_pred is not None and not q._window_exprs:
        raise SemanticError("B3: .qualify() requires .window() expressions")


def _validate_bands(q: "DGMQuery") -> None:
    if not q._anchor_table:
        raise SemanticError("DGMQuery requires anchor() (B1 mandatory)")
    _validate_b2(q)
    _validate_b3(q)


def _build_select_list(q: "DGMQuery") -> list[str]:
    if q._group_by_cols or q._agg_exprs:
        parts: list[str] = list(q._group_by_cols)
        parts += [f"{expr} AS {name}" for name, expr in q._agg_exprs.items()]
    else:
        parts = ["*"]
    parts += [f"{expr} AS {name}" for name, expr in q._window_exprs.items()]
    return parts


def _augment_on_with_scd2(on: str, alias: str, as_of: str) -> str:
    return (
        f"{on} AND {alias}.valid_from <= '{as_of}'"
        f" AND ({alias}.valid_to IS NULL OR {alias}.valid_to > '{as_of}')"
    )


def _semi_additive_expr(
    measure: str,
    fallback: str,
    forbidden: set,
    active: set,
) -> str:
    """Return the SQL expression for a semi-additive measure."""
    if not (forbidden & active):
        return f"SUM({measure})"
    if fallback == "last":
        order_col = next(iter(forbidden & active))
        return (
            f"LAST_VALUE({measure}) OVER "
            f"(PARTITION BY {order_col} ORDER BY {order_col} "
            f"ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
        )
    if fallback == "avg":
        return f"AVG({measure})"
    if fallback == "max":
        return f"MAX({measure})"
    return f"SUM({measure})"


def _dim_join_clause(
    anchor_alias: str,
    dim_table: str,
    alias: str,
    fact_fk: str,
    dim_pk: str,
    as_of: "str | None",
) -> str:
    """Build a LEFT JOIN clause for a SCD2-aware dimension join."""
    base = f"{anchor_alias}.{fact_fk} = {alias}.{dim_pk}"
    if as_of:
        return (
            f"LEFT JOIN {dim_table} {alias} ON {base}"
            f" AND {alias}.valid_from::DATE <= '{as_of}'::DATE"
            f" AND ({alias}.valid_to IS NULL"
            f" OR {alias}.valid_to::DATE > '{as_of}'::DATE)"
        )
    return f"LEFT JOIN {dim_table} {alias} ON {base} AND {alias}.is_current = TRUE"


def _scd2_join_lines(q: "DGMQuery", anchor_alias: str) -> list[str]:
    """Build LEFT JOIN clauses for all SCD2 dimension joins in *q*."""
    return [
        _dim_join_clause(anchor_alias, dim_table, alias, fact_fk, dim_pk, q._as_of)
        for dim_table, alias, fact_fk, dim_pk in q._scd2_joins
    ]


def _build_from_joins(q: "DGMQuery") -> str:
    anchor_alias: str = q._anchor_alias or q._anchor_table or ""
    lines = [f"FROM {q._anchor_table} {anchor_alias}"]
    for table, alias, on in q._joins:
        on_full = _augment_on_with_scd2(on, alias, q._as_of) if q._as_of else on
        lines.append(f"LEFT JOIN {table} {alias} ON {on_full}")
    lines.extend(_scd2_join_lines(q, anchor_alias))
    return "\n".join(lines)
