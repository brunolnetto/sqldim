"""DGM ref types — PropRef, AggRef, WinRef and value-formatting helpers."""

from __future__ import annotations


class PropRef:
    """Reference to a table property: alias.prop."""

    def __init__(self, alias_or_full: str, prop: str | None = None) -> None:
        if prop is None:
            parts = alias_or_full.split(".", 1)
            self.alias = parts[0]
            self.prop = parts[1] if len(parts) > 1 else alias_or_full
        else:
            self.alias = alias_or_full
            self.prop = prop

    def to_sql(self) -> str:
        return f"{self.alias}.{self.prop}"

    def __repr__(self) -> str:
        return f"PropRef({self.alias!r}, {self.prop!r})"


class AggRef:
    """Reference to a named aggregate result (B2 column)."""

    def __init__(self, name: str) -> None:
        self.name = name

    def to_sql(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"AggRef({self.name!r})"


class WinRef:
    """Reference to a named window result (B3 column)."""

    def __init__(self, name: str) -> None:
        self.name = name

    def to_sql(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"WinRef({self.name!r})"


def _format_value(v: object) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return f"'{v}'"
    return str(v)


def _paren_if_compound(parts: list[str], op: str) -> str:
    if len(parts) == 1:
        return parts[0]
    return "(" + f" {op} ".join(parts) + ")"


class SignatureRef:
    """Reference to the label-sequence signature of a named BoundPath instance.

    Valid only in B₁ (Where/PathPred context).  Renders as
    ``{path_alias}.signature`` and is expanded by the executor into the
    path-array CTE column holding the edge-label sequence.
    """

    def __init__(self, path_alias: str) -> None:
        self.path_alias = path_alias

    def to_sql(self) -> str:
        return f"{self.path_alias}.signature"

    def __repr__(self) -> str:
        return f"SignatureRef({self.path_alias!r})"


# ---------------------------------------------------------------------------
# Expression types  (DGM §8)
# ---------------------------------------------------------------------------


class _ConstExpr:
    """A literal constant expression — wraps a numeric value for use in ArithExpr."""

    def __init__(self, value: float | int) -> None:
        self.value = value

    def to_sql(self) -> str:
        return str(self.value)


class ArithExpr:
    """Arithmetic binary expression: ``left op right`` (DGM §8).

    Operands may be :class:`PropRef`, :class:`_ConstExpr`, or nested
    :class:`ArithExpr` instances.  SQL is emitted with parentheses to preserve
    evaluation order: ``(left op right)``.

    Parameters
    ----------
    left:
        Left-hand operand — any object with a ``to_sql()`` method.
    op:
        One of ``+``, ``-``, ``*``, ``/``.
    right:
        Right-hand operand — any object with a ``to_sql()`` method.
    """

    def __init__(self, left: object, op: str, right: object) -> None:
        self.left = left
        self.op = op
        self.right = right

    def to_sql(self) -> str:
        return f"({self.left.to_sql()} {self.op} {self.right.to_sql()})"
