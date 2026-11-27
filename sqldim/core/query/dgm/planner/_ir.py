"""Physical plan IR (DGM §6.2, §8.14; issue #148).

The planner lowers a canonical query (a :class:`~sqldim.core.query.dgm.core.DGMQuery`
or a :class:`~sqldim.core.query.dgm.algebra.QuestionAlgebra`) to this typed,
immutable IR.  SQL text is *emitted from* the IR (:mod:`._ir_emit`), the
canonical identity — ``dag_hash``, ``predicate_hash``, ``question_key`` — is
computed over it (:mod:`._ir_identity`), and so is ``deps(plan)``.

Every relation the emitted SQL can read appears in the IR as a :class:`Scan`.
Expressions that arrive as SQL text (ON clauses, aggregate and window
expressions, raw predicates) are carried as :class:`Fragment` values, which
are admitted only after a lexical check that they cannot open a relation of
their own: no ``SELECT``/``FROM``/``JOIN``/…, no table function, no comment,
no statement separator, and every qualified name starts with an alias that is
in scope.  A construct the IR cannot represent with a known read set is an
:class:`UnsupportedConstruct` error, never a guessed relation set.
"""

from __future__ import annotations

import functools
import re
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Union

from sqldim.core.query.dgm.algebra import ComposeOp
from sqldim.core.query.dgm.preds._repeat import RepeatMode
from sqldim.exceptions import SemanticError

if TYPE_CHECKING:
    from sqldim.core.query.dgm.planner._steps import PreComputation

__all__ = [
    "PlanIRError",
    "UnsupportedConstruct",
    "UnsafeFragment",
    "UnknownRelation",
    "UndeclaredReads",
    "PlanIdentityUnavailable",
    "ReadRole",
    "VersionKind",
    "Quantifier",
    "Fragment",
    "Literal",
    "Scan",
    "VersionResolution",
    "Join",
    "Compare",
    "RawCondition",
    "Conj",
    "Disj",
    "Neg",
    "PathHop",
    "PathRepeat",
    "RepeatJoin",
    "RepeatMode",
    "PathExists",
    "Pred",
    "BandQuery",
    "Compose",
    "PlanNode",
    "Cte",
    "PlanIR",
    "check_fragment",
    "check_identifier",
    "check_relation_name",
    "rename_aliases",
    "sql_string",
]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class PlanIRError(SemanticError):
    """Base class for physical-plan IR errors."""


class UnsupportedConstruct(PlanIRError):
    """The query uses a construct the IR cannot represent with a known read set."""


class UnsafeFragment(UnsupportedConstruct):
    """A SQL text fragment could read a relation that is not a declared Scan."""


class UnknownRelation(PlanIRError):
    """A relation is not declared by the catalog, so its dependencies are unknown."""


class UndeclaredReads(UnknownRelation):
    """A step of the plan does not declare the relations it reads."""


class PlanIdentityUnavailable(PlanIRError):
    """The plan has no canonical identity (no IR, or steps outside the IR)."""


# ---------------------------------------------------------------------------
# Lexical checks
# ---------------------------------------------------------------------------

_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
_IDENT_RE = re.compile(rf"^{_IDENT}$")
_RELATION_RE = re.compile(rf"^{_IDENT}(?:\.{_IDENT})?$")

_TOKEN_RE = re.compile(
    rf"""
     (?P<ws>\s+)
    |(?P<str>'(?:[^']|'')*')
    |(?P<num>\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)
    |(?P<name>{_IDENT}(?:\.(?:{_IDENT}|\*))*)
    |(?P<bad>--|/\*|\*/|;)
    |(?P<op>::|<>|!=|<=|>=|\|\||[-+*/%=<>(),\[\]])
    """,
    re.VERBOSE,
)

#: Words that introduce a relation read, a statement or a file/catalog scan.
_FORBIDDEN_WORDS = frozenset(
    {
        "SELECT", "FROM", "JOIN", "WITH", "TABLE", "UNION", "INTERSECT",
        "EXCEPT", "VALUES", "INTO", "LATERAL", "PIVOT", "UNPIVOT", "ATTACH",
        "DETACH", "COPY", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",
        "ALTER", "CALL", "PRAGMA", "SET", "EXECUTE", "EXPORT", "IMPORT",
        "INSTALL", "LOAD", "DESCRIBE", "SUMMARIZE", "SHOW", "QUERY",
        "QUERY_TABLE", "GLOB", "READ_CSV", "READ_CSV_AUTO", "READ_PARQUET",
        "PARQUET_SCAN", "READ_JSON", "READ_JSON_AUTO", "READ_NDJSON",
        "READ_TEXT", "READ_BLOB", "DELTA_SCAN", "ICEBERG_SCAN",
        "POSTGRES_SCAN", "SQLITE_SCAN", "GETVARIABLE",
    }
)  # fmt: skip


@functools.cache
def _reserved_words() -> frozenset[str]:
    """DuckDB's reserved keywords, which cannot be unquoted identifiers."""
    import duckdb

    rows = duckdb.sql(
        "SELECT keyword_name FROM duckdb_keywords() WHERE keyword_category = 'reserved'"
    ).fetchall()
    return frozenset(str(r[0]).upper() for r in rows)


def check_identifier(name: str, what: str) -> str:
    """Return *name* if it is a plain, unreserved SQL identifier, else raise."""
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise UnsupportedConstruct(f"{what} {name!r} is not a plain identifier")
    if name.upper() in _FORBIDDEN_WORDS | _reserved_words():
        raise UnsupportedConstruct(f"{what} {name!r} is a reserved word")
    return name


def check_relation_name(name: object) -> str:
    """Return *name* if it names a relation (``table`` or ``schema.table``)."""
    if not isinstance(name, str) or not _RELATION_RE.match(name):
        raise UnsupportedConstruct(
            f"relation {name!r} is not a table name; file scans, table "
            "functions and subqueries have no declared read set"
        )
    for part in name.split("."):
        check_identifier(part, "relation")
    return name


def _check_name_token(tok: str, scope: frozenset[str], sql: str) -> None:
    parts = tok.split(".")
    if len(parts) == 1:
        if tok.upper() in _FORBIDDEN_WORDS:
            raise UnsafeFragment(f"{tok!r} is not allowed in a fragment: {sql!r}")
        return
    if parts[0] not in scope:
        raise UnsafeFragment(
            f"{tok!r} qualifies a name with {parts[0]!r}, which is not an "
            f"alias in scope {sorted(scope)}: {sql!r}"
        )


def check_fragment(sql: object, scope: frozenset[str]) -> "Fragment":
    """Admit *sql* as a :class:`Fragment` that reads no relation of its own.

    *scope* is the set of aliases the fragment may qualify names with.
    """
    if not isinstance(sql, str) or not sql.strip():
        raise UnsafeFragment(f"fragment {sql!r} is not a non-empty SQL string")
    pos = 0
    while pos < len(sql):
        m = _TOKEN_RE.match(sql, pos)
        if m is None:
            raise UnsafeFragment(
                f"unexpected character {sql[pos]!r} at {pos} in fragment {sql!r}"
            )
        if m.lastgroup == "bad":
            raise UnsafeFragment(
                f"comments and statement separators are not allowed: {sql!r}"
            )
        if m.lastgroup == "name":
            _check_name_token(m.group(), scope, sql)
        pos = m.end()
    return Fragment(sql)


def rename_aliases(frag: "Fragment", mapping: dict[str, str]) -> "Fragment":
    """*frag* with every name qualified by a key of *mapping* requalified.

    Only the leading alias of a qualified name is replaced; string literals
    and unqualified names are left alone.  The result is a new Fragment; the
    caller re-checks it against the scope it is emitted in.
    """
    out: list[str] = []
    pos = 0
    sql = frag.sql
    while pos < len(sql):
        m = _TOKEN_RE.match(sql, pos)
        if m is None or m.lastgroup == "bad":  # a checked fragment has neither
            raise UnsafeFragment(f"fragment {sql!r} was not checked")
        tok = m.group()
        if m.lastgroup == "name" and "." in tok:
            head, rest = tok.split(".", 1)
            tok = f"{mapping.get(head, head)}.{rest}"
        out.append(tok)
        pos = m.end()
    return Fragment("".join(out))


def sql_string(value: str) -> str:
    """Render *value* as a SQL string literal (quotes doubled)."""
    return "'" + value.replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# IR node types
# ---------------------------------------------------------------------------


class ReadRole(Enum):
    """Why a relation is read (all roles count towards ``deps(plan)``)."""

    ANCHOR = "ANCHOR"  # B1 anchor (FROM)
    PATH_JOIN = "PATH_JOIN"  # path_join hop
    VERSIONED_JOIN = "VERSIONED_JOIN"  # SCD version resolution (join_dim)
    EXISTS_PATH = "EXISTS_PATH"  # hop inside a PathPred EXISTS / FORALL
    REPEAT = "REPEAT"  # hop inside the body of a Repeat (§3.2)


class VersionKind(Enum):
    """SCD version-resolution scheme attached to a join (§6.2 Rule 1c)."""

    VALID_RANGE = "VALID_RANGE"  # valid_from ≤ τ < valid_to, NULL bounds open
    IS_CURRENT = "IS_CURRENT"  # is_current = TRUE


class Quantifier(Enum):
    EXISTS = "EXISTS"
    FORALL = "FORALL"


@dataclass(frozen=True)
class Fragment:
    """A checked SQL expression fragment; build it with :func:`check_fragment`."""

    sql: str


@dataclass(frozen=True)
class Literal:
    """A literal value: ``None``, ``bool``, ``int``, finite ``float`` or ``str``."""

    value: None | bool | int | float | str


@dataclass(frozen=True)
class Scan:
    """A read of *relation* under *alias*."""

    relation: str
    alias: str
    role: ReadRole


@dataclass(frozen=True)
class VersionResolution:
    kind: VersionKind
    as_of: str | None = None  # required for VALID_RANGE, absent for IS_CURRENT


@dataclass(frozen=True)
class Join:
    """``LEFT JOIN scan ON on [AND version predicate]``."""

    scan: Scan
    on: Fragment
    version: VersionResolution | None = None


@dataclass(frozen=True)
class Compare:
    """``lhs op rhs`` — a ScalarPred atom."""

    lhs: Fragment
    op: str
    rhs: Literal


@dataclass(frozen=True)
class RawCondition:
    """A checked raw SQL condition (``RawPred`` / string ``where``)."""

    fragment: Fragment


@dataclass(frozen=True)
class Conj:
    preds: tuple["Pred", ...]


@dataclass(frozen=True)
class Disj:
    preds: tuple["Pred", ...]


@dataclass(frozen=True)
class Neg:
    pred: "Pred"


@dataclass(frozen=True)
class PathHop:
    """One hop of a path pattern; ``on`` correlates it with earlier aliases."""

    kind: str  # verb | verb_inverse | bridge
    label: str
    scan: Scan
    on: Fragment


@dataclass(frozen=True)
class PathRepeat:
    """``Repeat(body, min, max, mode, depth_alias)`` of §3.2.

    *start* is the alias the first iteration starts from; each later
    iteration starts from the end alias of the one before.  ``max = None`` is
    ``UNBOUNDED``.  *start_relation* is the relation *start* is read from
    when the planner knows it (it is needed to check R1 before unrolling).
    """

    start: str
    body: tuple[PathHop, ...]
    min: int
    max: int | None
    mode: RepeatMode
    depth_alias: str | None = None
    start_relation: str | None = None

    @property
    def end(self) -> str:
        """The body's end alias: outside the path, the node the instance ends at."""
        return self.body[-1].scan.alias


@dataclass(frozen=True)
class RepeatJoin:
    """A PathJoin over a Repeat: one joined row per instance (strategy ALL)."""

    repeat: PathRepeat


@dataclass(frozen=True)
class PathExists:
    """A PathPred: ∃ (or ∀) path over *hops* such that *sub_filter* holds."""

    quantifier: Quantifier
    strategy: str  # only ALL is executable in this IR version
    hops: tuple[PathHop | PathRepeat, ...]
    sub_filter: "Pred"


Pred = Union[Compare, RawCondition, Conj, Disj, Neg, PathExists]


@dataclass(frozen=True)
class BandQuery:
    """A ``Q_valid`` leaf: B1 context and filter, B2 aggregation, B3 ranking."""

    anchor: Scan
    joins: tuple[Join | RepeatJoin, ...] = ()
    where: Pred | None = None
    group_by: tuple[Fragment, ...] = ()
    aggregates: tuple[tuple[str, Fragment], ...] = ()
    having: Pred | None = None
    windows: tuple[tuple[str, Fragment], ...] = ()
    qualify: Pred | None = None

    def scans(self) -> tuple[Scan, ...]:
        """Every Scan of this leaf, including those inside path predicates."""
        out = [self.anchor]
        for j in self.joins:
            if isinstance(j, RepeatJoin):
                out.extend(h.scan for h in j.repeat.body)
            else:
                out.append(j.scan)
        for p in (self.where, self.having, self.qualify):
            if p is not None:
                out.extend(_pred_scans(p))
        return tuple(out)


@dataclass(frozen=True)
class Compose:
    """An internal DAG node: ``op(left, right)`` (``on`` only for JOIN)."""

    op: ComposeOp
    left: "PlanNode"
    right: "PlanNode"
    on: Fragment | None = None


PlanNode = Union[BandQuery, Compose]


@dataclass(frozen=True)
class Cte:
    """A named node of the plan; names are presentation only (not hashed)."""

    name: str
    node: PlanNode


@dataclass(frozen=True)
class PlanIR:
    """A physical plan: PreComputations, CTEs in topological order, the root.

    When ``ctes`` is empty the root is a single :class:`BandQuery` emitted as
    one ``SELECT``; otherwise the root is the last CTE and is emitted as
    ``WITH … SELECT * FROM <root>``.

    ``pre_compute`` are the steps run before the query, in order (§6.2
    Rule 3, §6.3 Step 1b).  Each step materialises a relation named after
    it; the query, or a later step, may read that relation.  A step's
    ``reads`` may name an earlier step but never itself or a later one.
    """

    root: PlanNode
    ctes: tuple[Cte, ...] = ()
    pre_compute: tuple["PreComputation", ...] = ()

    def __post_init__(self) -> None:
        _check_steps(self.pre_compute, {c.name for c in self.ctes})


def _check_steps(steps: tuple["PreComputation", ...], cte_names: set[str]) -> None:
    from sqldim.core.query.dgm.planner._steps import PreComputation

    for pc in steps:
        if not isinstance(pc, PreComputation):
            raise UnsupportedConstruct(f"{pc!r} is not a PreComputation")
    names = [pc.name for pc in steps]
    for i, pc in enumerate(steps):
        name = check_identifier(pc.name, "PreComputation name")
        if name in names[:i] or name in cte_names:
            raise UnsupportedConstruct(f"PreComputation name {name!r} is bound twice")
        ahead = sorted(set(pc.reads or ()) & set(names[i:]))
        if ahead:
            raise UnsupportedConstruct(
                f"PreComputation {name!r} reads {ahead}, which no earlier step produces"
            )


def _hop_scans(h: PathHop | PathRepeat) -> list[Scan]:
    if isinstance(h, PathRepeat):
        return [b.scan for b in h.body]
    return [h.scan]


def _pred_scans(p: Pred) -> list[Scan]:
    if isinstance(p, PathExists):
        hops = [s for h in p.hops for s in _hop_scans(h)]
        return hops + _pred_scans(p.sub_filter)
    if isinstance(p, (Conj, Disj)):
        return [s for sub in p.preds for s in _pred_scans(sub)]
    if isinstance(p, Neg):
        return _pred_scans(p.pred)
    return []
