"""Stratified Datalog¬,≤ and the translation of §8.13 Theorem 1 (#139).

:class:`Program` computes the perfect model of a stratified program with the
built-ins ``=, ≠, <, ≤`` (§8.13 *Datalog class*): stratum by stratum (the
components of the dependency graph, in order), each to its least fixpoint.  It refuses an unstratifiable or unsafe program, so a
translation that needed either would fail the check instead of passing.

:class:`Translation` builds the program of the proof of Theorem 1 for unary
Band 1 queries anchored on one node type, read literally:

* a condition is translated in the T⁺ form of §6.1 — one IDB for "TRUE" and
  one for "FALSE" — so a NULL property is an absent EDB fact and NOT swaps
  the two;
* a fixed-length path is a conjunction of edge literals, with the
  distinctness of SIMPLE (nodes) or TRAIL (edges) as ``≠`` literals; a
  Repeat with a finite max is the union over its admitted lengths;
* ``Repeat(hop_e, ·, UNBOUNDED)`` is the recursion of the proof's table;
* a negated PathPred, and EXCEPT, is a negated IDB literal in a higher stratum.

Nothing here imports ``sqldim``.  Queries are data:

``("cmp", key, op, const)``, ``("cmp2", key, op, key2)``, ``("isnull", key)``,
``("not", c)``, ``("and", c, d)``, ``("or", c, d)``,
``("pp", mode, phi, pattern)`` (EXISTS; mode ``"EVENTUALLY"``, ``"NEXT"`` or
``("UNTIL", psi)``; *phi*, *psi* and *pattern* in the form of
:mod:`tests.conformance.oracle.ctl` and :mod:`~tests.conformance.oracle.paths`),
and the compositions ``("union" | "intersect" | "except", q, r)`` over
``("where", c)`` leaves.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from itertools import product
from typing import Any

Lit = tuple[Any, ...]
Rule = tuple[tuple[str, tuple[str, ...]], tuple[Lit, ...]]

_BUILTIN = {
    "=": lambda a, b: a == b,
    "≠": lambda a, b: a != b,
    "<": lambda a, b: a < b,
    "≤": lambda a, b: a <= b,
}
# The class has =, ≠, <, ≤ only: > and ≥ swap their arguments.
_AS_BUILTIN = {
    "=": ("=", False),
    "!=": ("≠", False),
    "<": ("<", False),
    "<=": ("≤", False),
    ">": ("<", True),
    ">=": ("≤", True),
}
_NEGATED = {"=": "!=", "!=": "=", "<": ">=", "<=": ">", ">": "<=", ">=": "<"}


def _is_var(t: Any) -> bool:
    return isinstance(t, str) and t.startswith("?")


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


def strata(rules: Iterable[Rule]) -> list[tuple[list[Rule], bool]]:
    """Rules grouped by the strongly connected components of the predicate
    dependency graph, in evaluation order, each with whether it recurses.

    Raises if a negated literal is in its head's component (negation through
    recursion): the program is then not stratified.
    """
    rules = list(rules)
    deps: dict[str, set[str]] = {}
    for (head, _), body in rules:
        deps.setdefault(head, set()).update(
            lit[1] for lit in body if lit[0] in ("pos", "neg")
        )
    order, comp = _components(deps)
    for (head, _), body in rules:
        for lit in body:
            if lit[0] == "neg" and comp.get(lit[1]) == comp[head]:
                raise ValueError(f"not stratifiable: {head} negates {lit[1]}")
    groups: dict[int, list[Rule]] = {}
    for r in rules:
        groups.setdefault(comp[r[0][0]], []).append(r)
    out = []
    for c in order:
        if c in groups:
            members = {h for h, k in comp.items() if k == c}
            recursive = len(members) > 1 or any(
                lit[1] in members for r in groups[c] for lit in r[1] if lit[0] == "pos"
            )
            out.append((groups[c], recursive))
    return out


def _components(deps: Mapping[str, set[str]]) -> tuple[list[int], dict[str, int]]:
    """Tarjan's SCCs; components come out dependencies first."""
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    comp: dict[str, int] = {}
    stack: list[str] = []
    order: list[int] = []

    def visit(v: str) -> None:
        index[v] = low[v] = len(index)
        stack.append(v)
        for w in deps.get(v, ()):
            if w not in index:
                visit(w)
                low[v] = min(low[v], low[w])
            elif w in stack:
                low[v] = min(low[v], index[w])
        if low[v] == index[v]:
            c = len(order)
            while True:
                w = stack.pop()
                comp[w] = c
                if w == v:
                    break
            order.append(c)

    for v in sorted(deps):
        if v not in index:
            visit(v)
    return order, comp


def _check_safe(rule: Rule) -> None:
    (_, args), body = rule
    bound = {a for lit in body if lit[0] == "pos" for a in lit[2] if _is_var(a)}
    used = set(args)
    for lit in body:
        if lit[0] == "neg":
            used |= {a for a in lit[2] if _is_var(a)}
        elif lit[0] == "cmp":
            used |= {a for a in lit[2:] if _is_var(a)}
    if not {a for a in used if _is_var(a)} <= bound:
        raise ValueError(f"unsafe rule {rule}")


def _value(t: Any, env: Mapping[str, Any]) -> Any:
    return env[t] if _is_var(t) else t


def _match(args: tuple, fact: tuple, env: dict) -> dict | None:
    out = dict(env)
    for a, v in zip(args, fact):
        if _is_var(a):
            if a in out and out[a] != v:
                return None
            out[a] = v
        elif a != v:
            return None
    return out


def _compile(rule: Rule) -> tuple:
    """The positive literals in order, each followed by the negated and
    built-in literals whose variables it completes (filtered early)."""
    (head, head_args), body = rule
    rest = [lit for lit in body if lit[0] != "pos"]
    steps, bound = [], set()
    for _, pred, args in (lit for lit in body if lit[0] == "pos"):
        bound |= {a for a in args if _is_var(a)}
        ready = [lit for lit in rest if _vars(lit) <= bound]
        rest = [lit for lit in rest if lit not in ready]
        steps.append((pred, args, ready))
    return head, head_args, steps


def _vars(lit: Lit) -> set[str]:
    args = lit[2] if lit[0] == "neg" else lit[2:]
    return {a for a in args if _is_var(a)}


def _holds(lit: Lit, env: Mapping[str, Any], db: Mapping[str, set]) -> bool:
    if lit[0] == "neg":
        return tuple(_value(a, env) for a in lit[2]) not in db.get(lit[1], ())
    _, op, a, b = lit
    return _BUILTIN[op](_value(a, env), _value(b, env))


def _fire(compiled: tuple, db: Mapping[str, set]) -> set[tuple]:
    _head, head_args, steps = compiled
    envs: list[dict] = [{}]
    for pred, args, ready in steps:
        envs = [
            e2
            for e in envs
            for f in db.get(pred, ())
            if (e2 := _match(args, f, e)) is not None
            and all(_holds(lit, e2, db) for lit in ready)
        ]
        if not envs:
            return set()
    return {tuple(env[a] for a in head_args) for env in envs}


class Program:
    """A safe, stratified program, checked once and run on any EDB."""

    def __init__(self, rules: Iterable[Rule]) -> None:
        rules = list(rules)
        for r in rules:
            _check_safe(r)
        self.groups = [
            ([_compile(r) for r in group], recursive)
            for group, recursive in strata(rules)
        ]

    def run(self, edb: Mapping[str, set]) -> dict[str, set]:
        """The perfect model over *edb*: each component to its least fixpoint."""
        db: dict[str, set] = {k: set(v) for k, v in edb.items()}
        for rules, recursive in self.groups:
            changed = True
            while changed:
                changed = False
                for rule in rules:
                    have = db.setdefault(rule[0], set())
                    new = _fire(rule, db) - have
                    if new:
                        have |= new
                        changed = recursive
        return db


def edb(graph: Mapping[str, Any], keys: Iterable[str]) -> dict[str, set]:
    """The relational encoding of §2.6: node, one relation per property, edges."""
    out: dict[str, set] = {"node": {(n,) for n in graph["nodes"]}}
    for k in keys:
        out[f"p_{k}"] = {
            (n, props[k])
            for n, props in graph["nodes"].items()
            if props.get(k) is not None
        }
    for e in graph["edges"]:
        out.setdefault(f"e_{e[2]}", set()).add((e[0], e[1]))
    return out


# ---------------------------------------------------------------------------
# The translation of the proof of Theorem 1
# ---------------------------------------------------------------------------


class Translation:
    def __init__(self) -> None:
        self.rules: list[Rule] = []
        self._n = 0
        self._memo: dict[Any, str] = {}

    def _fresh(self, tag: str) -> str:
        self._n += 1
        return f"{tag}{self._n}"

    def _rule(self, head: str, body: Iterable[Lit], var: str = "?x") -> None:
        self.rules.append(((head, (var,)), tuple(body)))

    # -- conditions (T⁺ form: sign True is "TRUE", False is "FALSE") ----------

    def cond(self, c: tuple, sign: bool = True) -> str:
        memo = (c, sign)
        if memo not in self._memo:
            self._memo[memo] = self._cond(c, sign)
        return self._memo[memo]

    def _cond(self, c: tuple, sign: bool) -> str:
        tag = c[0]
        if tag == "not":
            return self.cond(c[1], not sign)
        if tag in ("and", "or"):
            return self._junction(c, sign)
        p = self._fresh("c")
        if tag == "cmp":
            _, k, op, const = c
            self._rule(p, [("pos", f"p_{k}", ("?x", "?v")),
                           _builtin(op if sign else _NEGATED[op], "?v", const)])  # fmt: skip
        elif tag == "cmp2":
            _, k, op, k2 = c
            self._rule(p, [("pos", f"p_{k}", ("?x", "?v")),
                           ("pos", f"p_{k2}", ("?x", "?w")),
                           _builtin(op if sign else _NEGATED[op], "?v", "?w")])  # fmt: skip
        elif tag == "isnull":  # IsNull is negation of the property's existence
            has = self._fresh("has")
            self._rule(has, [("pos", f"p_{c[1]}", ("?x", "?v"))])
            lit = ("neg" if sign else "pos", has, ("?x",))
            self._rule(p, [("pos", "node", ("?x",)), lit])
        elif tag == "pp":  # a PathPred is TRUE or FALSE, never UNKNOWN
            r = self.pathpred(c)
            lit = ("pos" if sign else "neg", r, ("?x",))
            self._rule(p, [("pos", "node", ("?x",)), lit])
        else:
            raise ValueError(f"not in F_D here: {c!r}")
        return p

    def _junction(self, c: tuple, sign: bool) -> str:
        p = self._fresh("j")
        parts = [self.cond(d, sign) for d in c[1:]]
        conjunction = (c[0] == "and") == sign
        if conjunction:  # one rule
            self._rule(p, [("pos", q, ("?x",)) for q in parts])
        else:  # one rule per disjunct
            for q in parts:
                self._rule(p, [("pos", q, ("?x",))])
        return p

    def pred(self, phi: tuple) -> str:
        """``sub_filter`` read where it is TRUE (§4.1): ctl data → a condition."""
        return self.cond(_ctl_cond(phi))

    # -- PathPreds -------------------------------------------------------------

    def pathpred(self, c: tuple) -> str:
        _, mode, phi, pattern = c
        if len(pattern) == 1 and pattern[0][0] == "repeat" and pattern[0][4] is None:
            return self._recursive(mode, phi, pattern[0])
        return self._unfolded(mode, phi, pattern)

    def _recursive(self, mode: Any, phi: tuple, elem: tuple) -> str:
        _, label, direction, lo, _hi, mode_ = elem
        assert direction == "fwd" and mode_ in ("SIMPLE", "TRAIL") and lo in (0, 1)
        e = ("pos", f"e_{label}", ("?x", "?y"))
        r = self._fresh("r")
        if mode == "EVENTUALLY":  # Rφ(x) :- φ(x).  Rφ(x) :- E(x, y), Rφ(y).
            self._rule(r, [("pos", self.pred(phi), ("?x",))])
            self._rule(r, [e, ("pos", r, ("?y",))])
        elif mode[0] == "UNTIL":  # Rφψ(x) :- ψ(x).  Rφψ(x) :- φ(x), E(x, y), Rφψ(y).
            self._rule(r, [("pos", self.pred(mode[1]), ("?x",))])
            self._rule(r, [("pos", self.pred(phi), ("?x",)), e, ("pos", r, ("?y",))])
        else:
            raise ValueError(f"not in F_D here: {mode!r}")
        if lo == 0:
            return r
        r1 = self._fresh("r")  # min 1: the start is not a position
        self._rule(r1, [e, ("pos", r, ("?y",))])
        return r1

    def _unfolded(self, mode: Any, phi: tuple, pattern: tuple) -> str:
        r = self._fresh("u")
        for nodes, positions, distinct in _shapes(pattern):
            path = [("pos", f"e_{lab}", (a, b)) for a, b, lab in nodes]
            if not nodes:
                path = [("pos", "node", ("?x0",))]
            for neq in distinct:
                for body in self._position_bodies(mode, phi, positions):
                    self._rule(r, [*path, *neq, *body], "?x0")
        return r

    def _position_bodies(self, mode: Any, phi: tuple, positions: list[str]):
        if mode in ("EVENTUALLY", "NEXT"):
            if mode == "NEXT":
                positions = positions[:1]
            for v in positions:
                yield [("pos", self.pred(phi), (v,))]
        elif mode[0] == "UNTIL":
            for j, v in enumerate(positions):
                before = [("pos", self.pred(phi), (u,)) for u in positions[:j]]
                yield [("pos", self.pred(mode[1]), (v,)), *before]
        else:
            raise ValueError(f"not in F_D here: {mode!r}")

    # -- queries ---------------------------------------------------------------

    def query(self, q: tuple) -> str:
        tag = q[0]
        if tag == "where":
            return self.cond(q[1])
        a, b = self.query(q[1]), self.query(q[2])
        p = self._fresh("q")
        x = ("?x",)
        if tag == "union":
            self._rule(p, [("pos", a, x)])
            self._rule(p, [("pos", b, x)])
        elif tag == "intersect":  # also JOIN of two questions on the anchor key
            self._rule(p, [("pos", a, x), ("pos", b, x)])
        elif tag == "except":
            self._rule(p, [("pos", a, x), ("neg", b, x)])
        else:
            raise ValueError(f"not in F_D: {q!r}")
        return p


def _builtin(op: str, left: Any, right: Any) -> Lit:
    name, swap = _AS_BUILTIN[op]
    return ("cmp", name, right, left) if swap else ("cmp", name, left, right)


def _ctl_cond(p: tuple) -> tuple:
    tag = p[0]
    if tag == "prop":
        return ("cmp", p[1], p[2], p[3])
    if tag in ("not", "and", "or"):
        return (tag, *(_ctl_cond(q) for q in p[1:]))
    raise ValueError(f"not translated: {p!r}")


def _shapes(pattern: tuple):
    """Each admitted length of a pattern with no UNBOUNDED Repeat, as
    ``(edges, positions, distinctness)``: the edge literals' variables, the
    evaluation positions in order, and the alternative conjunctions of ``≠``
    literals that state SIMPLE or TRAIL (an edge differs from another when its
    source or its target differs: one conjunction per choice)."""
    options: list[list[tuple]] = [[]]
    for elem in pattern:
        if elem[0] == "hop":
            assert elem[2] == "fwd"
            options = [o + [("hop", elem[1], elem[3], None)] for o in options]
        else:
            _, label, direction, lo, hi, mode = elem
            assert direction == "fwd" and hi is not None
            options = [
                o + [("rep", label, k, mode, lo == 0)]
                for o in options
                for k in range(lo, hi + 1)
            ]
    for steps in options:
        yield _shape(steps)


def _shape(steps: list[tuple]):
    edges, positions, segments = [], [], []
    cur, n = "?x0", 0
    for kind, label, arg, mode, *start in steps:
        if kind == "hop":
            n += 1
            nxt = f"?x{n}"
            edges.append((cur, nxt, label))
            if arg:
                positions.append(nxt)
            cur = nxt
            continue
        seg = [cur]
        if start[0] and cur not in positions:
            positions.append(cur)
        for _ in range(arg):
            n += 1
            nxt = f"?x{n}"
            edges.append((cur, nxt, label))
            positions.append(nxt)
            seg.append(nxt)
            cur = nxt
        segments.append((mode, seg))
    choices: list[list[list[Lit]]] = []
    for mode, seg in segments:
        if mode == "SIMPLE":
            choices.append([[("cmp", "≠", a, b) for i, a in enumerate(seg)
                             for b in seg[i + 1:]]])  # fmt: skip
        elif mode == "TRAIL":
            pairs = list(zip(seg, seg[1:]))
            per = [
                [("cmp", "≠", p[0], q[0]), ("cmp", "≠", p[1], q[1])]
                for i, p in enumerate(pairs)
                for q in pairs[i + 1 :]
            ]
            choices.append([list(c) for c in product(*per)] if per else [[]])
        else:  # WALK
            choices.append([[]])
    distinct = [sum(c, []) for c in product(*choices)] if choices else [[]]
    return edges, positions, distinct
