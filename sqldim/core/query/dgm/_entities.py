"""What an aggregate counts: ``over = ENTITIES | PATHS`` (§4.2).

A PathJoin puts one tuple in the context per path instance, so a node reached
by several paths appears in several tuples.  Under ``ENTITIES`` — the default
— each distinct combination of the instances bound to the aliases an
aggregate's argument references contributes once per group; under ``PATHS``
every tuple contributes, and the query must ask for it.

An aggregate expression of :class:`DGMQuery` is SQL text.  When the query has
a PathJoin, every ``SUM``, ``AVG`` and ``COUNT`` call in it whose argument
references an alias of the context is rewritten (DuckDB dialect) so that it
reads each instance once: the instance of an alias is its whole row.
``MIN`` and ``MAX`` are the same under both units and are kept; so are
``COUNT(*)`` (a tuple count, not an aggregate of §4.2), a ``DISTINCT``
argument and an argument that references no alias.  An aggregate with
``FILTER`` or ``OVER`` has no ENTITIES rendering and raises
``UnsupportedConstruct`` unless it is asked for with ``over="PATHS"``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from sqldim.exceptions import SemanticError

__all__ = ["ENTITIES", "PATHS", "check_over", "entity_aggregate"]

ENTITIES, PATHS = "ENTITIES", "PATHS"

_CALL = re.compile(r"\b(SUM|AVG|COUNT|MIN|MAX)\s*\(", re.IGNORECASE)
_TRAILER = re.compile(r"\s*(FILTER|OVER)\b", re.IGNORECASE)


def check_over(over: str) -> str:
    if over not in (ENTITIES, PATHS):
        raise SemanticError(f"over={over!r}: an aggregate counts ENTITIES or PATHS")
    return over


def _close(sql: str, start: int) -> int:
    """Index of the parenthesis closing the one opened just before *start*."""
    depth, i = 1, start
    while i < len(sql):
        ch = sql[i]
        if ch == "'":
            i = sql.index("'", i + 1) if "'" in sql[i + 1 :] else len(sql)
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    raise SemanticError(f"unbalanced parentheses in aggregate {sql!r}")


def _referenced(arg: str, aliases: Iterable[str]) -> list[str]:
    text = re.sub(r"'(?:[^']|'')*'", "''", arg)  # ignore string literals
    return [a for a in aliases if re.search(rf"(?<![\w.]){re.escape(a)}\.\w", text)]


def _rewrite(fn: str, arg: str, refs: list[str]) -> str:
    ident = refs[0] if len(refs) == 1 else f"row({', '.join(refs)})"
    if fn == "COUNT":
        return f"COUNT(DISTINCT CASE WHEN ({arg}) IS NOT NULL THEN {ident} END)"
    entries = (
        f"map_from_entries(list(DISTINCT row({ident}, {arg}))"
        f" FILTER (WHERE ({arg}) IS NOT NULL))"
    )
    head = "list_sum" if fn == "SUM" else "list_avg"
    return f"{head}(map_values({entries}))"


def _one_call(sql: str, m: re.Match, aliases: Iterable[str]) -> tuple[str, int]:
    """The ENTITIES text of the call matched at *m*, and where it ends."""
    from sqldim.core.query.dgm.planner._ir import UnsupportedConstruct

    fn = m.group(1).upper()
    end = _close(sql, m.end())
    arg = sql[m.end() : end].strip()
    original = sql[m.start() : end + 1]
    refs = _referenced(arg, aliases)
    distinct = arg.upper().startswith("DISTINCT")
    if fn in ("MIN", "MAX") or arg == "*" or distinct or not refs:
        return original, end + 1
    if _TRAILER.match(sql, end + 1):
        raise UnsupportedConstruct(
            f"{original}… with FILTER or OVER has no ENTITIES rendering (§4.2); "
            "write it with over='PATHS' if a measure per path instance is meant"
        )
    return _rewrite(fn, arg, refs), end + 1


def entity_aggregate(sql: str, aliases: Iterable[str]) -> str:
    """*sql* with each instance of the referenced aliases counted once."""
    aliases = sorted(set(aliases), key=lambda a: (-len(a), a))
    out, pos = [], 0
    while (m := _CALL.search(sql, pos)) is not None:
        out.append(sql[pos : m.start()])
        text, pos = _one_call(sql, m, aliases)
        out.append(text)
    out.append(sql[pos:])
    return "".join(out)
