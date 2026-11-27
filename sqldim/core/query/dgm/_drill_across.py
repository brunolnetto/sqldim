"""Drill-across of a constellation query (§5.2.2, DA1–DA4).

``DGMQuery`` reads no model, so it cannot tell a fact alias from a dimension
alias: the caller declares the fact branches with
:meth:`DGMQuery.drill_across`, each as the set of aliases it binds.  Every
other alias of the context is shared by all branches — the conformed
dimensions the branches are aligned on.  With two or more branches the query
is evaluated by the definition of §5.2.2, not row to row:

1. for each branch, the context restricted to the shared aliases and the
   branch's own aliases, the Where conjuncts that concern it, grouped by the
   GroupBy keys, with the aggregates that belong to it;
2. the per-branch results aligned by ``FULL OUTER JOIN`` on the keys (``ON
   TRUE`` without keys);
3. the named expressions evaluated on the aligned rows, then Having.

DA1–DA3 are checked when the SQL is built: an aggregate call references the
aliases of exactly one branch, a GroupBy key references shared aliases only,
and a Where conjunct references at most one branch.  DA4: a COUNT of an
absent branch is 0, any other aggregate NULL.  A division in step 3 by zero
is NULL (§4.1).

What the declaration cannot state is checked by nobody: that the shared
aliases are dimensions with ``Conformed(d, Fs)`` for every branch's fact
type.  Band 3, a Repeat PathJoin and an anchor inside a branch have no
rendering here and raise ``UnsupportedConstruct``.
"""

from __future__ import annotations

import copy
import re
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING

from sqldim.core.query.dgm._entities import _CALL, _close, _referenced
from sqldim.exceptions import SemanticError

if TYPE_CHECKING:
    from sqldim.core.query.dgm.core import DGMQuery

__all__ = ["check_branches", "drill_across_sql", "null_on_zero"]

_NAME = re.compile(r"^[A-Za-z_]\w*$")


def check_branches(branches: Sequence[Iterable[str]]) -> tuple[frozenset[str], ...]:
    """Two or more non-empty, disjoint branches of aliases."""
    out = tuple(frozenset(b) for b in branches)
    if len(out) < 2:
        raise SemanticError("drill_across needs two or more fact branches (§5.2.2)")
    if any(not b for b in out):
        raise SemanticError("drill_across: a fact branch binds at least one alias")
    seen: set[str] = set()
    for b in out:
        if seen & b:
            raise SemanticError(
                f"drill_across: alias {sorted(seen & b)} is in two fact branches"
            )
        seen |= b
    return out


def _unsupported(msg: str) -> Exception:
    from sqldim.core.query.dgm.planner._ir import UnsupportedConstruct

    return UnsupportedConstruct(msg)


# ---------------------------------------------------------------------------
# Context and DA checks
# ---------------------------------------------------------------------------


def _join_aliases(q: "DGMQuery") -> list[str]:
    from sqldim.core.query.dgm._dim_joins import dim_join_steps
    from sqldim.core.query.dgm.preds import Repeat

    anchor = q._anchor_alias or q._anchor_table or ""
    out = [anchor]
    for j in q._joins:
        if isinstance(j, Repeat):
            raise _unsupported("drill-across over a Repeat PathJoin is not rendered")
        out.append(j[1])
    for d in q._scd2_joins:
        out += [st.alias for st in dim_join_steps(d, anchor, q._as_of)]
    return out


def _branch_of(refs: Iterable[str], branches) -> set[int]:
    refs = set(refs)
    return {i for i, b in enumerate(branches) if b & refs}


def _conjuncts(pred: object) -> list[object]:
    from sqldim.core.query.dgm.preds import AND

    if pred is None:
        return []
    if isinstance(pred, AND):
        return [c for p in pred.preds for c in _conjuncts(p)]
    return [pred]


class _Constellation:
    """The branches of *q* and its DA1–DA3 checks."""

    def __init__(self, q: "DGMQuery") -> None:
        self.q = q
        self.branches: tuple[frozenset[str], ...] = q._branches
        self.aliases = _join_aliases(q)
        unknown = set().union(*self.branches) - set(self.aliases)
        if unknown:
            raise SemanticError(
                f"drill_across names {sorted(unknown)}, not aliases of the context"
            )
        if self.aliases[0] in set().union(*self.branches):
            raise _unsupported(
                "drill-across with the anchor inside a fact branch is not rendered: "
                "anchor the query on a conformed dimension"
            )
        if q._window_exprs or q._qualify_pred is not None:
            raise _unsupported("Band 3 over a drill-across is not rendered")
        self.where = self._where()
        self._check_keys()

    def refs(self, sql: str) -> list[str]:
        return _referenced(sql, self.aliases)

    def _where(self) -> list[list[object]]:
        """Where conjuncts per branch (DA3)."""
        out: list[list[object]] = [[] for _ in self.branches]
        for c in _conjuncts(self.q._where_pred):
            hit = _branch_of(self.refs(c.to_sql()), self.branches)  # type: ignore[attr-defined]
            if len(hit) > 1:
                raise SemanticError(
                    "DA3: a Where predicate references two fact branches; it would "
                    "pair instances of facts the constellation does not relate"
                )
            for i in hit or range(len(self.branches)):
                out[i].append(c)
        return out

    def _check_keys(self) -> None:
        for key in self.q._group_by_cols:
            if _branch_of(self.refs(key), self.branches):
                raise SemanticError(
                    f"DA2: GroupBy key {key!r} is not an attribute of a dimension "
                    "shared by every fact branch"
                )

    def branch_of_call(self, call: str, arg: str) -> int:
        """DA1: the one branch an aggregate call reads."""
        hit = _branch_of(self.refs(arg), self.branches)
        if len(hit) != 1:
            raise SemanticError(
                f"DA1: aggregate {call} references "
                f"{'no' if not hit else 'more than one'} fact branch"
            )
        return hit.pop()


# ---------------------------------------------------------------------------
# Step 3: aggregates → aligned columns
# ---------------------------------------------------------------------------


def _split(c: _Constellation) -> tuple[dict[str, str], list[dict[str, tuple]]]:
    """Each named expression with its calls replaced by aligned columns, and
    per branch the calls it computes: ``column → (call, over)``."""
    per_branch: list[dict[str, tuple]] = [{} for _ in c.branches]
    outer: dict[str, str] = {}
    n = 0
    for name, expr in c.q._agg_exprs.items():
        over = c.q._agg_over.get(name, "ENTITIES")
        parts, pos = [], 0
        while (m := _CALL.search(expr, pos)) is not None:
            end = _close(expr, m.end())
            call, arg = expr[m.start() : end + 1], expr[m.end() : end].strip()
            i = c.branch_of_call(call, arg)
            col = f"a{n}"
            n += 1
            per_branch[i][col] = (call, over)
            ref = f"b{i}.{col}"
            count = m.group(1).upper() == "COUNT"
            parts += [expr[pos : m.start()], f"COALESCE({ref}, 0)" if count else ref]
            pos = end + 1
        parts.append(expr[pos:])
        outer[name] = null_on_zero("".join(parts))
    return outer, per_branch


def _operand_end(sql: str, i: int) -> int:
    """End of the operand that starts at *i*: a parenthesis group, or a name
    or number optionally followed by a call's parentheses."""
    if sql[i] == "(":
        return _close(sql, i + 1) + 1
    j = i
    while j < len(sql) and (sql[j].isalnum() or sql[j] in "_."):
        j += 1
    if j == i:
        raise SemanticError(f"cannot read the divisor in {sql!r}")
    k = j
    while k < len(sql) and sql[k].isspace():
        k += 1
    if k < len(sql) and sql[k] == "(":
        return _close(sql, k + 1) + 1
    return j


def null_on_zero(sql: str) -> str:
    """*sql* with every divisor ``d`` written ``NULLIF(d, 0)`` (§4.1)."""
    out, pos = [], 0
    while (k := sql.find("/", pos)) != -1:
        i = k + 1
        while i < len(sql) and sql[i].isspace():
            i += 1
        end = _operand_end(sql, i)
        divisor = null_on_zero(sql[i:end])
        out += [sql[pos : k + 1], f" NULLIF({divisor}, 0)"]
        pos = end
    out.append(sql[pos:])
    return "".join(out)


# ---------------------------------------------------------------------------
# Steps 1 and 2
# ---------------------------------------------------------------------------


def _conj(preds: list[object]) -> object | None:
    from sqldim.core.query.dgm.preds import AND

    if not preds:
        return None
    return preds[0] if len(preds) == 1 else AND(*preds)  # type: ignore[arg-type]


def _branch_query(c: _Constellation, i: int, calls: dict[str, tuple]) -> str:
    q = c.q
    others = [b for j, b in enumerate(c.branches) if j != i]
    keep = set(c.aliases) - set().union(*others)
    sub = copy.copy(q)
    sub._branches = ()
    sub._joins = [j for j in q._joins if j[1] in keep]  # type: ignore[index]
    sub._scd2_joins = [d for d in q._scd2_joins if d.alias in keep]
    sub._where_pred = _conj(c.where[i])  # type: ignore[assignment]
    # a branch with no aggregate still yields its keys
    calls = calls or {"a_": ("COUNT(*)", "PATHS")}
    sub._agg_exprs = {col: call for col, (call, _) in calls.items()}
    sub._agg_over = {col: over for col, (_, over) in calls.items()}
    sub._having_pred = None
    return sub.to_sql()


def _key_name(key: str, n: int) -> str:
    last = key.rsplit(".", 1)[-1].strip()
    return last if _NAME.match(last) else f"k{n}"


def _coalesce(k: str, branches: range) -> str:
    return f"COALESCE({', '.join(f'b{i}.{k}' for i in branches)})"


def _aligned(sources: list[str], keys: list[str]) -> list[str]:
    """``FROM`` and the ``FULL OUTER JOIN`` of each further branch (step 2)."""
    lines = ["FROM " + sources[0]]
    for i in range(1, len(sources)):
        on = " AND ".join(
            f"{_coalesce(k, range(i))} IS NOT DISTINCT FROM b{i}.{k}" for k in keys
        )
        lines.append(f"FULL OUTER JOIN {sources[i]} ON {on or 'TRUE'}")
    return lines


def drill_across_sql(q: "DGMQuery") -> str:
    """The SQL of a constellation query by §5.2.2 steps 1–3."""
    c = _Constellation(q)
    outer, per_branch = _split(c)
    keys = [f"k{n}" for n in range(len(q._group_by_cols))]
    sources = [
        f"(\n{_branch_query(c, i, calls)}\n) AS b{i}"
        f"({', '.join(keys + (list(calls) or ['a_']))})"
        for i, calls in enumerate(per_branch)
    ]
    every = range(len(sources))
    select = [
        f"{_coalesce(k, every)} AS {_key_name(key, n)}"
        for n, (k, key) in enumerate(zip(keys, q._group_by_cols))
    ]
    select += [f"{expr} AS {name}" for name, expr in outer.items()]
    sql = "\n".join(["SELECT " + ", ".join(select), *_aligned(sources, keys)])
    if q._having_pred is not None:
        sql = f"SELECT * FROM (\n{sql}\n) AS da\nWHERE {q._having_pred.to_sql()}"
    return sql
