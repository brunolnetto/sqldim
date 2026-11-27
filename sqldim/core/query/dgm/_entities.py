"""What an aggregate counts: ``over = ENTITIES | PATHS`` (§4.2).

A PathJoin puts one tuple in the context per path instance, so a node reached
by several paths appears in several tuples.  Under ``ENTITIES`` — the default
— each distinct combination of the instances bound to the aliases an
aggregate's argument references contributes once per group; under ``PATHS``
every tuple contributes, and the query must ask for it.

An aggregate expression of :class:`DGMQuery` is SQL text.  When the query has
a PathJoin, every ``SUM``, ``AVG`` and ``COUNT`` call in it whose argument
references an alias of the context is rewritten (DuckDB dialect) so that it
reads each instance once.  An instance is identified by its entity key where
the query declares one — the durable key ``dim_pk`` of a dimension join, so
the SCD versions of one entity are one instance (§4.2, Rule 1b) — and by its
whole row otherwise.  Within a group an entity contributes one value: if two
of its versions give the argument different values the evaluation fails
with :class:`EntityValueConflict` rather than pick one.  ``MIN`` and ``MAX`` are
the same under both units and are kept; so are ``COUNT(*)`` (a tuple count,
not an aggregate of §4.2), a ``DISTINCT`` argument and an argument that
references no alias.  An aggregate with ``FILTER`` or ``OVER`` has no
ENTITIES rendering and raises ``UnsupportedConstruct`` unless it is asked
for with ``over="PATHS"``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

from sqldim.exceptions import SemanticError

__all__ = [
    "ENTITIES",
    "EntityValueConflict",
    "PATHS",
    "check_over",
    "entity_aggregate",
    "entity_conflict",
]

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


class EntityValueConflict(SemanticError):
    """Under ENTITIES one entity gives an aggregate argument several values
    within a group (two SCD versions that disagree), so there is no single
    value for it to contribute once (§4.2)."""

    def __init__(self, alias: str) -> None:
        self.alias = alias
        super().__init__(
            f"over=ENTITIES: an entity of alias {alias!r} contributes conflicting "
            "values within a group (several versions with different values); "
            "resolve the version it reads (join_dim mode AS_WAS/AS_IS, "
            "version_key) or ask for over='PATHS'"
        )


_CONFLICT = "sqldim-entity-conflict"
_CONFLICT_RE = re.compile(rf"{_CONFLICT}\[([^\]]*)\]")


def entity_conflict(exc: BaseException) -> EntityValueConflict | None:
    """The typed error behind a backend error raised by an ENTITIES guard."""
    m = _CONFLICT_RE.search(str(exc))
    return None if m is None else EntityValueConflict(m.group(1))


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _rewrite(fn: str, arg: str, refs: list[str], names: list[str]) -> str:
    """*refs* are the identity expressions of the referenced aliases *names*.

    SUM and AVG read one value per entity; an entity with several values in
    the group raises an error tagged for :func:`entity_conflict`.
    """
    ident = refs[0] if len(refs) == 1 else f"row({', '.join(refs)})"
    if fn == "COUNT":
        return f"COUNT(DISTINCT CASE WHEN ({arg}) IS NOT NULL THEN {ident} END)"
    present = f"FILTER (WHERE ({arg}) IS NOT NULL)"
    entries = f"map_from_entries(list(DISTINCT row({ident}, {arg})) {present})"
    head = "list_sum" if fn == "SUM" else "list_avg"
    names_text = ", ".join(names)
    tag = _sql_string(f"{_CONFLICT}[{names_text}] ")
    # error() would make the aggregate side-effecting, which DuckDB refuses
    # to reference from HAVING; a failing cast of the tag raises instead and
    # keeps the result type of the ELSE branch.
    fail = f"{head}(list_value((list({arg}))[CAST(concat({tag}, COUNT(*)) AS BIGINT)]))"
    return (
        f"CASE WHEN COUNT(DISTINCT {ident}) {present}"
        f" < COUNT(DISTINCT row({ident}, {arg})) {present}"
        f" THEN {fail} ELSE {head}(map_values({entries})) END"
    )


def _one_call(
    sql: str, m: re.Match, aliases: Mapping[str, str], require: frozenset[str] | None
) -> tuple[str, int]:
    """The ENTITIES text of the call matched at *m*, and where it ends."""
    from sqldim.core.query.dgm.planner._ir import UnsupportedConstruct

    fn = m.group(1).upper()
    end = _close(sql, m.end())
    arg = sql[m.end() : end].strip()
    original = sql[m.start() : end + 1]
    refs = _referenced(arg, aliases)
    distinct = arg.upper().startswith("DISTINCT")
    skip = require is not None and not require & set(refs)
    if fn in ("MIN", "MAX") or arg == "*" or distinct or not refs or skip:
        return original, end + 1
    if _TRAILER.match(sql, end + 1):
        raise UnsupportedConstruct(
            f"{original}… with FILTER or OVER has no ENTITIES rendering (§4.2); "
            "write it with over='PATHS' if a measure per path instance is meant"
        )
    return _rewrite(fn, arg, [aliases[a] for a in refs], refs), end + 1


def entity_aggregate(
    sql: str,
    aliases: Iterable[str] | Mapping[str, str],
    *,
    require: Iterable[str] | None = None,
) -> str:
    """*sql* with each instance of the referenced aliases counted once.

    *aliases* maps each alias to the SQL expression that identifies its
    instance (its entity key); an alias given without one is identified by
    its whole row.  With *require*, only a call that references one of those
    aliases is rewritten.
    """
    needed = None if require is None else frozenset(require)
    ident = dict(aliases) if isinstance(aliases, Mapping) else {a: a for a in aliases}
    ordered = sorted(ident, key=lambda a: (-len(a), a))
    keyed = {a: ident[a] for a in ordered}
    out, pos = [], 0
    while (m := _CALL.search(sql, pos)) is not None:
        out.append(sql[pos : m.start()])
        text, pos = _one_call(sql, m, keyed, needed)
        out.append(text)
    out.append(sql[pos:])
    return "".join(out)
