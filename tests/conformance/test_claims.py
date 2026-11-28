"""Checks behind the claims register (``claims.py``, ``docs/reference/dgm-claims.md``).

Every check decides a claim of the specification on a bounded domain, with the
reference semantics of :mod:`tests.conformance.oracle` or with code written
here from the definitions — never with ``sqldim``.  A passing check is
evidence on that domain, not a proof; a check named ``…counterexample`` pins a
counterexample the register records.

One check is a conformance comparison: ``test_claim_question_enumeration``
compares the lattice's output with an enumeration written from the spec
(``qvalid.py``).  It reads ``sqldim`` only for the value under test; the
expected set never comes from it.
"""

from __future__ import annotations

import inspect
import math
import sys
from collections import deque
from fractions import Fraction
from fnmatch import fnmatch
from itertools import combinations, permutations, product

import pytest

from tests.conformance import claims, datalog, qvalid
from tests.conformance.cases import all_cases
from tests.conformance.oracle import allen, cones, ctl, kleene, logic, paths

# --------------------------------------------------------------------------
# Small graph domains
# --------------------------------------------------------------------------


def digraphs(n: int, *, loops: bool, label: str = "e"):
    """Every directed graph on nodes ``v0 … v{n-1}`` with one edge label."""
    nodes = [f"v{i}" for i in range(n)]
    pairs = [(a, b) for a in nodes for b in nodes if loops or a != b]
    for bits in product((0, 1), repeat=len(pairs)):
        edges = [(a, b, label) for (a, b), bit in zip(pairs, bits) if bit]
        yield {"nodes": {v: {} for v in nodes}, "edges": edges}


def labelled(graph: dict, **props: tuple[bool, ...]) -> dict:
    nodes = {
        v: {k: vals[i] for k, vals in props.items()}
        for i, v in enumerate(graph["nodes"])
    }
    return {"nodes": nodes, "edges": graph["edges"]}


def simple_paths(graph: dict, start: str) -> list[tuple[tuple[str, ...], tuple]]:
    """All simple paths from *start* over every label: (nodes, edge instances)."""
    out = []

    def grow(ns: tuple[str, ...], es: tuple) -> None:
        out.append((ns, es))
        for e in graph["edges"]:
            if e[0] == ns[-1] and e[1] not in ns:
                grow(ns + (e[1],), es + (tuple(e[:3]),))

    grow((start,), ())
    return out


# --------------------------------------------------------------------------
# §3.3 strategies, §8.5 collapse
# --------------------------------------------------------------------------

REPEAT_1 = (("repeat", "e", "fwd", 1, None, "SIMPLE"),)


def test_claim_strategy_refinement_order() -> None:
    checked = 0
    for g in digraphs(3, loops=True):
        for s in g["nodes"]:
            insts = paths.instances(g, s, REPEAT_1)
            for t in g["nodes"]:
                inst = [i for i in insts if i[0][-1] == t]
                every = paths.select(g, inst, ("ALL",))
                short = paths.select(g, inst, ("SHORTEST",))
                assert short == paths.select(g, inst, ("K_SHORTEST", 1))
                prev = short
                for k in range(1, 5):
                    ks = paths.select(g, inst, ("K_SHORTEST", k))
                    assert set(prev) <= set(ks) <= set(every)
                    prev = ks
                mw = paths.select(g, inst, ("MIN_WEIGHT",))
                assert set(mw) <= set(every)
                assert len(short) <= 1 and len(mw) <= 1  # §8.5 collapse
                checked += 1
    assert checked == 512 * 9


def _incomparable_witness(k: int) -> dict:
    """k heavy two-hop paths s → h_i → t and one light three-hop path."""
    edges = []
    for i in range(k):
        edges += [("s", f"h{i}", "e", 10), (f"h{i}", "t", "e", 10)]
    edges += [("s", "l1", "e", 1), ("l1", "l2", "e", 1), ("l2", "t", "e", 1)]
    nodes = {"s", "t", "l1", "l2"} | {f"h{i}" for i in range(k)}
    return {"nodes": {n: {} for n in nodes}, "edges": edges}


def test_claim_min_weight_incomparable() -> None:
    for k in (1, 2, 3):
        g = _incomparable_witness(k)
        inst = [i for i in paths.instances(g, "s", REPEAT_1) if i[0][-1] == "t"]
        mw = set(paths.select(g, inst, ("MIN_WEIGHT",)))
        ks = set(paths.select(g, inst, ("K_SHORTEST", k)))
        assert not mw <= ks and not ks <= mw, k


# --------------------------------------------------------------------------
# §4.1 / ADR 0001 dualities
# --------------------------------------------------------------------------

PHI = ("prop", "p", "=", True)
PSI = ("prop", "q", "=", True)
TRUE = ("true",)
HOP = (("hop", "e", "fwd", True),)
REPEAT_0 = (("repeat", "e", "fwd", 0, None, "SIMPLE"),)


def _pp(g, s, quant, mode, phi, pattern, on_empty=ctl.VACUOUS) -> bool:
    return ctl.evaluate(
        g,
        s,
        {"quantifier": quant, "mode": mode, "phi": phi, "pattern": pattern,
         "on_empty": on_empty},
    )  # fmt: skip


def _constructions(g, s, pattern) -> dict:
    """The §4.1 FORALL elimination table, each side evaluated by the oracle."""
    nphi, npsi = ("not", PHI), ("not", PSI)
    ex = lambda mode, f, pat=pattern: _pp(g, s, "EXISTS", mode, f, pat)  # noqa: E731
    return {
        "GLOBALLY": (PHI, not ex("EVENTUALLY", nphi), ex("EVENTUALLY", TRUE)),
        "EVENTUALLY": (PHI, not ex("GLOBALLY", nphi), ex("EVENTUALLY", TRUE)),
        ("UNTIL", PSI): (
            PHI,
            not ex(("UNTIL", ("and", nphi, npsi)), npsi) and not ex("GLOBALLY", npsi),
            ex("EVENTUALLY", TRUE),
        ),
        "NEXT": (PHI, not ex("NEXT", nphi, HOP), ex("NEXT", TRUE, HOP)),
    }


def test_claim_ctl_dualities() -> None:
    s = "v0"  # every start is covered: the graph enumeration is closed under relabelling
    checked = 0
    for g0 in digraphs(3, loops=False):
        for p, q in product(product((False, True), repeat=3), repeat=2):
            g = labelled(g0, p=p, q=q)
            if True:
                for pattern in (REPEAT_0, REPEAT_1):
                    for mode, (phi, vac, guard) in _constructions(
                        g, s, pattern
                    ).items():
                        pat = HOP if mode == "NEXT" else pattern
                        assert _pp(g, s, "FORALL", mode, phi, pat) == vac
                        rw = _pp(g, s, "FORALL", mode, phi, pat, ctl.REQUIRE_WITNESS)
                        assert rw == (guard and vac)
                        checked += 1
    assert checked == 64 * 64 * 2 * 4


def test_claim_dualities_need_nottrue() -> None:
    """With Kleene ¬ for the inner NOT, AG φ = ¬EF ¬φ fails where φ is UNKNOWN."""
    g = {"nodes": {"a": {"p": None}}, "edges": []}
    ag = _pp(g, "a", "FORALL", "GLOBALLY", PHI, REPEAT_0)
    kleene = not _pp(g, "a", "EXISTS", "EVENTUALLY", ("not", PHI), REPEAT_0)
    assert ag is False and kleene is True


# --------------------------------------------------------------------------
# §6.3 Hoeffding
# --------------------------------------------------------------------------


def _n_star(k: int, delta: float, eps: float) -> int:
    return math.ceil(math.log(2 * k / delta) / (2 * eps**2))


def _error_bound(k: int, delta: float, n: int) -> float:
    return math.sqrt(math.log(2 * k / delta) / (2 * n))


def test_claim_hoeffding_sample_size() -> None:
    assert _n_star(1, 0.05, 0.05) == 738
    for k, delta, eps in product(
        range(1, 6), (0.01, 0.05, 0.1), (0.01, 0.02, 0.05, 0.1)
    ):
        n = _n_star(k, delta, eps)
        assert _error_bound(k, delta, n) <= eps
        assert n == 1 or _error_bound(k, delta, n - 1) > eps


def test_claim_hoeffding_exact() -> None:
    """Exact tail of the sample mean without replacement against the bound."""
    ts = [
        Fraction(1, 10),
        Fraction(1, 5),
        Fraction(1, 4),
        Fraction(1, 3),
        Fraction(1, 2),
    ]
    checked = 0
    for size in range(1, 17):
        for ones in range(size + 1):
            mu = Fraction(ones, size)
            for n in range(1, size + 1):
                total = math.comb(size, n)
                for t in ts:
                    tail = sum(
                        math.comb(ones, j) * math.comb(size - ones, n - j)
                        for j in range(max(0, n - (size - ones)), min(n, ones) + 1)
                        if abs(Fraction(j, n) - mu) >= t
                    )
                    bound = 2 * math.exp(-2 * n * float(t) ** 2)
                    assert tail / total <= bound + 1e-12, (size, ones, n, t)
                    checked += 1
    assert checked > 7000


# --------------------------------------------------------------------------
# §8.2 / §8.3
# --------------------------------------------------------------------------


def _minterm_tree(names: tuple[str, ...], bits: tuple[bool, ...]) -> tuple:
    if not any(bits):
        a = ("atom", names[0])
        return ("and", a, ("not", a))
    terms = []
    for assignment, bit in zip(product((False, True), repeat=len(names)), bits):
        if bit:
            lits = [
                ("atom", n) if v else ("not", ("atom", n))
                for n, v in zip(names, assignment)
            ]
            terms.append(("and",) + tuple(lits))
    return ("or",) + tuple(terms)


def test_claim_propositional_completeness() -> None:
    for m in (1, 2, 3):
        names = ("p", "q", "r")[:m]
        for bits in product((False, True), repeat=2**m):
            assert logic.truth_table(_minterm_tree(names, bits), names) == bits
    # Scope: x > 3 is not a Boolean function of the single atom x > 5.
    at4, at0 = (4 > 5, 4 > 3), (0 > 5, 0 > 3)
    assert at4[0] == at0[0] and at4[1] != at0[1]


def _diagram(bits: tuple[bool, ...], m: int, i: int = 0, prefix: tuple = ()) -> list:
    """Reduced ordered decision diagram by Shannon expansion: TRUE paths as cubes."""
    sub = [b for a, b in zip(product((0, 1), repeat=m), bits) if a[:i] == prefix]
    if all(sub):
        return [{}]
    if not any(sub):
        return []
    lo = _diagram(bits, m, i + 1, prefix + (0,))
    hi = _diagram(bits, m, i + 1, prefix + (1,))
    if lo == hi:  # elimination rule: variable i does not matter here
        return lo
    return [{**c, i: 0} for c in lo] + [{**c, i: 1} for c in hi]


def _covers(cube: dict, a: tuple) -> bool:
    return all(a[v] == bit for v, bit in cube.items())


def test_claim_cubes_and_bdd_paths() -> None:
    m = 3
    minterms = list(product((0, 1), repeat=m))
    for lits in product((None, 0, 1), repeat=m):
        cube = {v: b for v, b in enumerate(lits) if b is not None}
        assert sum(_covers(cube, a) for a in minterms) == 2 ** (m - len(cube))
    for bits in product((False, True), repeat=2**m):
        cubes = _diagram(bits, m)
        truth = {a for a, b in zip(minterms, bits) if b}
        covered = [{a for a in minterms if _covers(c, a)} for c in cubes]
        assert set().union(*covered) == truth
        assert sum(map(len, covered)) == len(truth)  # pairwise disjoint


# --------------------------------------------------------------------------
# §8.4 diamond chain
# --------------------------------------------------------------------------


def diamond_chain(n: int) -> dict:
    edges = []
    for i in range(1, n + 1):
        for side in ("a", "b"):
            edges += [
                (f"v{i - 1}", f"{side}{i}", "next"),
                (f"{side}{i}", f"v{i}", "next"),
            ]
    nodes = {e[0] for e in edges} | {e[1] for e in edges}
    return {"nodes": {v: {} for v in nodes}, "edges": edges}


def test_claim_diamond_chain_and_tree() -> None:
    chain = (("repeat", "next", "fwd", 1, None, "SIMPLE"),)
    for n in range(1, 7):
        g = diamond_chain(n)
        assert len(g["nodes"]) == 3 * n + 1 and len(g["edges"]) == 4 * n
        ends = paths.end_nodes(g, "v0", chain)
        assert ends.count(f"v{n}") == 2**n
    tree_edges = [
        (f"t{i}", f"t{2 * i + c}", "next") for i in range(1, 8) for c in (0, 1)
    ]
    tree = {"nodes": {f"t{i}": {} for i in range(1, 16)}, "edges": tree_edges}
    ends = paths.end_nodes(tree, "t1", chain)
    assert all(ends.count(v) == 1 for v in tree["nodes"] if v != "t1")  # k = 1


# --------------------------------------------------------------------------
# §8.6 bounds
# --------------------------------------------------------------------------


def _node_bound(n: int, m: int = 1) -> int:
    """§8.6: Σ_k n!/(n−k)! · m^k over paths of k nodes; m = 1 is the old bound."""
    return sum(
        math.factorial(n) // math.factorial(n - k) * m**k for k in range(1, n + 1)
    )


def _parallel(n: int, most: int):
    """Every graph on *n* nodes with 0 … *most* parallel edges per ordered pair.

    The parallel edges of a pair carry distinct labels ``l0 … l{most-1}``; the
    graph's ``m`` is its largest multiplicity (at least 1).
    """
    nodes = [f"v{i}" for i in range(n)]
    pairs = list(permutations(nodes, 2))
    for mult in product(range(most + 1), repeat=len(pairs)):
        edges = [(a, b, f"l{j}") for (a, b), k in zip(pairs, mult) for j in range(k)]
        yield {"nodes": {v: {} for v in nodes}, "edges": edges}, max((1, *mult))


def test_claim_simple_path_bound_one_label() -> None:
    for n in (1, 2, 3, 4):
        for g in digraphs(n, loops=n <= 3):
            count = sum(len(paths.instances(g, s, REPEAT_0)) for s in g["nodes"])
            assert count <= _node_bound(n)
    full = _complete(3)  # the bound is attained
    assert sum(len(paths.instances(full, s, REPEAT_0)) for s in full["nodes"]) == 15


def _complete(n: int) -> dict:
    nodes = [f"v{i}" for i in range(n)]
    return {
        "nodes": {v: {} for v in nodes},
        "edges": [(a, b, "e") for a, b in permutations(nodes, 2)],
    }


def test_claim_simple_path_bound_parallel_edges() -> None:
    for n, most in ((1, 0), (2, 3), (3, 2)):
        for g, m in _parallel(n, most):
            count = sum(len(simple_paths(g, s)) for s in g["nodes"])
            assert count <= _node_bound(n, m), (g["edges"], count)


def test_claim_simple_path_bound_counterexample() -> None:
    """The old bound counted node sequences; the corrected one counts edges."""
    g = {
        "nodes": {"a": {}, "b": {}},
        "edges": [("a", "b", "x"), ("a", "b", "y"), ("a", "b", "z")],
    }
    count = sum(len(simple_paths(g, s)) for s in g["nodes"])
    assert count == 5 > _node_bound(2) == 4
    assert count <= _node_bound(2, 3) == 24


def test_claim_pattern_count() -> None:
    for e_s, k in product(range(1, 4), range(0, 5)):
        hops = [(t, d) for t in range(e_s) for d in ("fwd", "rev")]
        patterns = {p for j in range(k + 1) for p in product(hops, repeat=j)}
        assert len(patterns) == sum((2 * e_s) ** j for j in range(k + 1))


# --------------------------------------------------------------------------
# §3.4.2 / §8.7 cones
# --------------------------------------------------------------------------


def _check_cones(g: dict) -> None:
    every_edge = {tuple(e[:3]) for e in g["edges"]}
    for a, b in product(g["nodes"], repeat=2):
        na, ea = cones.forward(g, a)
        nb, eb = cones.backward(g, b)
        nab, eab = cones.between(g, a, b)
        assert (na & nb, ea & eb) == (nab, eab)  # §8.7
        for ns, es in ((na, ea), (nb, eb), (nab, eab)):
            assert all(u in ns and v in ns for u, v, _l in es)  # subgraph
            assert es <= every_edge
        assert nab <= na and nab <= nb and eab <= ea and eab <= eb


def test_claim_cone_intersection_identity() -> None:
    for n in (1, 2, 3):
        for g in digraphs(n, loops=True):
            _check_cones(g)


def _simple_between(g: dict, a: str, b: str) -> set[str]:
    """N_AB under the pre-#139 reading: nodes on a simple path A →* B."""
    return {n for ns, _e in simple_paths(g, a) if ns[-1] == b for n in ns}


def test_claim_cone_old_counterexample_now_holds() -> None:
    g = {
        "nodes": {v: {} for v in ("A", "X", "B", "n")},
        "edges": [("A", "X", "e"), ("X", "B", "e"), ("B", "n", "e"), ("n", "X", "e")],
    }
    na, _ = cones.forward(g, "A")
    nb, _ = cones.backward(g, "B")
    # n is on a simple path from A and on one to B, but on none from A to B.
    assert "n" in {m for ns, _e in simple_paths(g, "A") for m in ns}
    assert any(ns[-1] == "B" for ns, _e in simple_paths(g, "n"))
    assert "n" not in _simple_between(g, "A", "B")
    # Under §3.4.2 (reachability) the identity holds on the same graph.
    nab, _ = cones.between(g, "A", "B")
    assert na & nb == nab == {"A", "X", "B", "n"}


# --------------------------------------------------------------------------
# §8.8 Allen
# --------------------------------------------------------------------------


def _proof_tree(a: float, b: float, c: float, d: float) -> str:
    """The three-comparison decision tree of the §8.8 proof."""
    if a == c:
        return "STARTS" if b < d else "EQUALS" if b == d else "STARTED_BY"
    if a < c:
        if b < c:
            return "PRECEDES"
        if b == c:
            return "MEETS"
        return "OVERLAPS" if b < d else "FINISHED_BY" if b == d else "CONTAINS"
    if a > d:
        return "PRECEDED_BY"
    if a == d:
        return "MET_BY"
    return "DURING" if b < d else "FINISHES" if b == d else "OVERLAPPED_BY"


def test_claim_allen_exhaustive() -> None:
    values = (0, 1, 2, 3, 4, 5, None)
    intervals = list(product(values, repeat=2))
    for lv, rv in product(intervals, repeat=2):
        rels = allen.relations(lv, rv)
        if allen.domain_of(lv, rv) == "degenerate":
            assert rels == []
            continue
        assert len(rels) == 1
        assert rels[0] == _proof_tree(*allen._bounds(lv), *allen._bounds(rv))


# --------------------------------------------------------------------------
# §8.10 entropy
# --------------------------------------------------------------------------


def _entropy(counts: tuple[int, ...]) -> float:
    total = sum(counts)
    return -sum(c / total * math.log2(c / total) for c in counts if c)


def test_claim_entropy() -> None:
    for k in range(1, 5):
        for counts in product(range(1, 5), repeat=k):
            h = _entropy(counts)
            assert h <= math.log2(k) + 1e-12
            assert (abs(h) < 1e-12) == (k == 1)
            if len(set(counts)) == 1:
                assert abs(h - math.log2(k)) < 1e-12
            elif k > 1:
                assert h < math.log2(k) - 1e-12


# --------------------------------------------------------------------------
# §8.13 walk vs simple
# --------------------------------------------------------------------------


def _walk_fixpoint(g: dict, base: set[str], guard: set[str] | None) -> set[str]:
    """Least fixpoint of R(x) :- base(x).  R(x) :- guard(x), E(x, y), R(y)."""
    r = set(base)
    changed = True
    while changed:
        changed = False
        for x, y, _l in g["edges"]:
            if y in r and x not in r and (guard is None or x in guard):
                r.add(x)
                changed = True
    return r


def _modes():
    return [(("repeat", "e", "fwd", 0, None, m),) for m in ("SIMPLE", "TRAIL")]


def test_claim_walk_equals_simple_one_hop() -> None:
    s = "v0"  # every start is covered: the graph enumeration is closed under relabelling
    bools = list(product((False, True), repeat=3))
    for g0 in digraphs(3, loops=True):  # EF and EX, self-loops included
        for p in bools:
            g = labelled(g0, p=p)
            phi = {v for v, b in zip(g["nodes"], p) if b}
            ef = s in _walk_fixpoint(g, phi, None)
            ex = any(x == s and y in phi for x, y, _l in g["edges"])
            for pat in _modes():
                assert _pp(g, s, "EXISTS", "EVENTUALLY", PHI, pat) == ef
            assert _pp(g, s, "EXISTS", "NEXT", PHI, HOP) == ex
    for g0 in digraphs(3, loops=False):  # E(φ U ψ)
        for p, q in product(bools, repeat=2):
            g = labelled(g0, p=p, q=q)
            phi = {v for v, b in zip(g["nodes"], p) if b}
            psi = {v for v, b in zip(g["nodes"], q) if b}
            eu = s in _walk_fixpoint(g, psi, phi)
            for pat in _modes():
                assert _pp(g, s, "EXISTS", ("UNTIL", PSI), PHI, pat) == eu


def test_claim_two_hop_body_breaks_walk_reading() -> None:
    g = {
        "nodes": {v: {} for v in ("s", "t", "u", "v")},
        "edges": [("s", "t", "a"), ("t", "u", "a"), ("u", "v", "a"), ("v", "t", "a")],
    }
    even_simple = any(
        ns[-1] == "t" and len(es) % 2 == 0 and len(es) > 0
        for ns, es in simple_paths(g, "s")
    )
    # Walks of even length from s: two-step reachability, iterated.
    step = {x: {y for a, y, _l in g["edges"] if a == x} for x in g["nodes"]}
    two = {x: {z for y in step[x] for z in step[y]} for x in g["nodes"]}
    seen, todo = set(), deque(two["s"])
    while todo:
        x = todo.popleft()
        if x not in seen:
            seen.add(x)
            todo.extend(two[x])
    assert "t" in seen and not even_simple  # walk s,t,u,v,t has length 4


# --------------------------------------------------------------------------
# §8.13 Theorem 1: the translation, executed (tests/conformance/datalog.py)
# --------------------------------------------------------------------------


def _kleene(g: dict, n: str, c: tuple):
    """A Band 1 condition at node *n* by the reference semantics."""
    props, tag = g["nodes"][n], c[0]
    if tag == "cmp":
        return kleene.compare(props.get(c[1]), c[2], c[3])
    if tag == "cmp2":
        return kleene.compare(props.get(c[1]), c[2], props.get(c[3]))
    if tag == "isnull":
        return props.get(c[1]) is None
    if tag == "not":
        return kleene.not_(_kleene(g, n, c[1]))
    if tag in ("and", "or"):
        fn = kleene.and_ if tag == "and" else kleene.or_
        return fn(_kleene(g, n, d) for d in c[1:])
    memo = g.setdefault("memo", {})  # a PathPred is evaluated once per node
    if (n, c) not in memo:
        _, mode, phi, pattern = c
        case = {"quantifier": "EXISTS", "mode": mode, "phi": phi, "pattern": pattern}
        memo[(n, c)] = ctl.evaluate(g, n, case)
    return memo[(n, c)]


def _reference(g: dict, q: tuple) -> set[str]:
    if q[0] == "where":
        return {n for n in g["nodes"] if kleene.is_true(_kleene(g, n, q[1]))}
    a, b = _reference(g, q[1]), _reference(g, q[2])
    return {"union": a | b, "intersect": a & b, "except": a - b}[q[0]]


def _agree(graphs, queries, keys) -> int:
    checked = 0
    tr = datalog.Translation()
    heads = [tr.query(q) for q in queries]
    program = datalog.Program(tr.rules)  # safe and stratified, or it raises
    for g in graphs:
        model = program.run(datalog.edb(g, keys))
        for q, head in zip(queries, heads):
            got = {t[0] for t in model.get(head, ())}
            assert got == _reference(g, q), (g, q)
            checked += 1
    return checked


def _conditions() -> list[tuple]:
    ops = ("=", "!=", "<", "<=", ">", ">=")
    atoms = [("cmp", "x", op, 1) for op in ops] + [("cmp2", "x", op, "y") for op in ops]
    atoms += [("isnull", "x"), ("isnull", "y")]
    out = atoms + [("not", a) for a in atoms]
    for a, b in combinations(atoms, 2):
        for j in (("and", a, b), ("or", a, b)):
            out += [j, ("not", j)]
    return out


_X1, _X0 = ("prop", "x", "=", 1), ("prop", "x", "=", 0)
_NOT_X0 = ("not", _X0)  # UNKNOWN where x is NULL
_PATTERNS = (
    HOP,
    (("hop", "e", "fwd", True), ("hop", "e", "fwd", True)),
    (("repeat", "e", "fwd", 0, None, "SIMPLE"),),
    (("repeat", "e", "fwd", 1, None, "TRAIL"),),
    (("repeat", "e", "fwd", 0, 2, "SIMPLE"),),
    (("repeat", "e", "fwd", 1, 2, "TRAIL"),),
    (("repeat", "e", "fwd", 0, 2, "WALK"),),
)


def _path_queries() -> list[tuple]:
    pps = [("pp", "NEXT", _NOT_X0, HOP)]
    for pat in _PATTERNS:
        pps += [("pp", "EVENTUALLY", _NOT_X0, pat), ("pp", ("UNTIL", _X0), _X1, pat)]
    leaves = [("where", p) for p in pps] + [("where", ("not", p)) for p in pps]
    leaves.append(("where", ("and", pps[3], ("not", ("cmp", "x", "=", 1)))))
    leaves.append(("where", ("or", ("not", pps[5]), ("isnull", "x"))))
    ef, ax = ("where", pps[3]), ("where", ("not", pps[0]))
    return leaves + [(op, ef, ax) for op in ("union", "intersect", "except")]


def _up_to_isomorphism(n: int, *, loops: bool) -> list[dict]:
    """One graph per isomorphism class of x-labelled graphs, x ∈ {NULL, 0, 1}.

    Every query here is invariant under renaming nodes, and the check compares
    whole result sets, so one representative per class decides the class.
    """
    seen, out = set(), []
    for g in digraphs(n, loops=loops):
        for v in product((None, 0, 1), repeat=n):
            nodes = list(g["nodes"])
            forms = []
            for perm in permutations(range(n)):
                ren = {nodes[i]: nodes[perm[i]] for i in range(n)}
                edges = tuple(sorted((ren[a], ren[b]) for a, b, _l in g["edges"]))
                labels = tuple(v[perm.index(i)] for i in range(n))
                forms.append((edges, tuple(map(repr, labels))))
            canon = min(forms)
            if canon not in seen:
                seen.add(canon)
                out.append(labelled(g, x=v))
    return out


def test_claim_datalog_translation() -> None:
    vals = (None, 0, 1, 2)
    single = [
        {"nodes": {"v0": {"x": x, "y": y}}, "edges": []} for x, y in product(vals, vals)
    ]
    checked = _agree(single, [("where", c) for c in _conditions()], ("x", "y"))
    graphs = _up_to_isomorphism(3, loops=False) + _up_to_isomorphism(2, loops=True)
    checked += _agree(graphs, _path_queries(), ("x",))
    assert checked > 0


# --------------------------------------------------------------------------
# §8.16.6 neighbourhood, ADR 0007 d_S
# --------------------------------------------------------------------------


def _groupby_neighbours(keys: frozenset, vp: tuple) -> set:
    return {(keys - {k}) | {k2} for k in keys for k2 in vp if k2 not in keys}


def test_claim_neighbourhood_bound() -> None:
    for n in range(0, 7):
        props = tuple(f"k{i}" for i in range(n))
        for g in range(0, n + 1):
            for keys in combinations(props, g):
                found = _groupby_neighbours(frozenset(keys), props)
                assert len(found) == g * (n - g), (keys, n)
    m, g, vp, length, e_ab, a, aggfn, p_ord = 10, 1, 15, 2, 5, 1, 6, 3
    total = m + g * (vp - g) + 2 * length * e_ab + a * aggfn + 2 * p_ord
    assert total == 56  # §8.16.8


def test_claim_neighbourhood_bound_counterexample() -> None:
    """The old line 'replace one GroupBy key ≤ |V_P|' fails for g ≥ 2."""
    props = tuple(f"k{i}" for i in range(5))
    two = _groupby_neighbours(frozenset({"k0", "k1"}), props)
    assert len(two) == 2 * (5 - 2) == 6 > len(props)


def _bfs(adj: dict, s) -> dict:
    dist, todo = {s: 0}, deque([s])
    while todo:
        x = todo.popleft()
        for y in adj[x]:
            if y not in dist:
                dist[y] = dist[x] + 1
                todo.append(y)
    return dist


def test_claim_shortest_path_metric() -> None:
    for n in range(1, 6):
        nodes = range(n)
        pairs = list(combinations(nodes, 2))
        for bits in product((0, 1), repeat=len(pairs)):
            adj: dict = {v: set() for v in nodes}
            for (x, y), bit in zip(pairs, bits):
                if bit:
                    adj[x].add(y)
                    adj[y].add(x)
            d = {v: _bfs(adj, v) for v in nodes}
            for x, y in product(nodes, repeat=2):
                if y not in d[x]:
                    assert x not in d[y]
                    continue
                assert d[x][y] == d[y][x] and (d[x][y] == 0) == (x == y)
                for z in nodes:
                    if z in d[y]:
                        assert d[x][z] <= d[x][y] + d[y][z]


_ATOMS, _KEYS, _HOPS, _FNS = ("a1", "a2", "a3"), ("k1", "k2", "k3"), ("h1", "h2"), (
    "SUM", "COUNT", "AVG",
)  # fmt: skip


def _moves(q: tuple) -> set:
    """The four structural moves of ADR 0007 on a toy skeleton."""
    where, key, hop, fn = q
    out = {(where ^ {a}, key, hop, fn) for a in _ATOMS}
    out |= {(where, k, hop, fn) for k in _KEYS if k != key}
    out |= {(where, key, h, fn) for h in _HOPS if h != hop}
    out |= {(where, key, hop, f) for f in _FNS if f != fn}
    return out


def test_claim_move_graph() -> None:
    wheres = [frozenset(c) for r in range(4) for c in combinations(_ATOMS, r)]
    space = list(product(wheres, _KEYS, _HOPS, _FNS))
    adj = {q: _moves(q) for q in space}
    for q, nbrs in adj.items():
        assert all(q in adj[r] for r in nbrs)  # every move has an inverse
    width = max(map(len, adj.values()))
    start = (frozenset({"a1"}), "k1", "h1", "SUM")
    dist = _bfs(adj, start)
    for k in range(4):
        assert sum(1 for v in dist.values() if v <= k) <= sum(
            width**j for j in range(k + 1)
        )
    assert dist[(frozenset(), "k2", "h1", "COUNT")] == 3  # the ADR's example


# --------------------------------------------------------------------------
# §8.11 Q_valid enumeration (the witness of tests/conformance/qvalid.py)
# --------------------------------------------------------------------------


def _schema(nodes: dict, edge: tuple, measures=None, bronze=()) -> dict:
    return {"nodes": nodes, "edge": edge, "measures": measures or {},
            "bronze": set(bronze)}  # fmt: skip


_QVALID_SCHEMAS = {
    "no properties": _schema({"A": ("dim", {}), "B": ("dim", {})}, ("A", "e", "B")),
    "numeric dimension attribute": _schema(
        {"A": ("dim", {"age": "Integer"}), "F": ("fact", {})}, ("A", "placed", "F")
    ),
    "String on a BRONZE dimension, Boolean": _schema(
        {"A": ("dim", {"s": "String"}), "B": ("dim", {"f": "Boolean"})},
        ("A", "e", "B"),
        bronze="A",
    ),
    "additive measure": _schema(
        {"A": ("dim", {}), "F": ("fact", {"rev": "Real"})},
        ("A", "placed", "F"),
        {("F", "rev"): "ADDITIVE"},
    ),
    "non-additive measure": _schema(
        {"A": ("dim", {}), "F": ("fact", {"qty": "Integer"})},
        ("A", "placed", "F"),
        {("F", "qty"): "NON_ADDITIVE"},
    ),
    "Degenerate attribute": _schema(
        {"A": ("dim", {}), "F": ("fact", {"ref": "Integer"})},
        ("A", "placed", "F"),
        {("F", "ref"): "DEGENERATE"},
    ),
}


def _lattice_prefix(schema: dict, most: int) -> list[tuple]:
    """``dgm_what_can_i_answer``'s order, as witness terms, up to grade *most*.

    The only place a claim check reads ``sqldim``: it supplies the value under
    test, never the expected one.
    """
    from sqldim.core.query.dgm.annotations import (
        Additivity,
        AdditivityKind,
        Degenerate,
        Grain,
        GrainKind,
        MedallionLayer,
        MedallionLayerKind,
    )
    from sqldim.core.query.dgm.lattice import QuestionLattice
    from sqldim.core.query.dgm.lattice._enumerate import skeletons
    from sqldim.core.query.dgm.model import (
        DGMModel,
        EdgeKind,
        EdgeType,
        NodeKind,
        NodeType,
        PropType,
    )

    kinds = {"dim": NodeKind.DIM, "fact": NodeKind.FACT}
    nodes = tuple(
        NodeType(t, kinds[k], {p: PropType(d) for p, d in props.items()})
        for t, (k, props) in schema["nodes"].items()
    )
    sigma: list = [
        Grain(t, GrainKind.EVENT)
        for t, (k, _) in schema["nodes"].items()
        if k == "fact"
    ]
    for (fact, prop), kind in schema["measures"].items():
        if kind == "NON_ADDITIVE":
            sigma.append(Additivity(fact, prop, AdditivityKind.NON_ADDITIVE))
        elif kind == "DEGENERATE":
            sigma.append(Degenerate(prop, fact=fact))
    sigma += [MedallionLayer(t, MedallionLayerKind.BRONZE) for t in schema["bronze"]]
    src, label, dst = schema["edge"]
    model = DGMModel(
        node_types=nodes,
        edge_types=(EdgeType(src, dst, label, EdgeKind.VERB),),
        sigma=tuple(sigma),
        sigma_kinds=frozenset(
            {"Additivity", "BridgeSemantics", "Conformed", "Degenerate",
             "FactlessFact", "Grain", "MedallionLayer", "RolePlaying"}
        ),
    )  # fmt: skip
    lat = QuestionLattice(model, max_anchors=2, max_hops=1)
    # ``enumerate`` is this generator with a question_key filter (QH6); the
    # generator is read directly, so a duplicate it emits is not filtered.
    out = []
    for sk in skeletons(lat.space([src, dst], max_hops=1)):
        assert sk.join == (("a1", ((src, label, dst, "+"),)),)
        mine = (_fn(sk.where), _band2(sk.band2), _band3(sk.band3))
        if qvalid.grade(mine) > most:
            break
        out.append(mine)
    return out


def _term(t: tuple) -> tuple:
    kind = t[0]
    if kind == "agg":
        return ("agg", t[1], _term(t[2]))
    if kind == "win":
        head = t[1][:2] + tuple(None if r is None else _term(r) for r in t[1][2:])
        order = tuple((_term(r), d) for r, d in t[3])
        return ("win", head, frozenset(_term(r) for r in t[2]), order)
    if kind == "cmp" and t[3][0] == "param":
        return ("cmp", _term(t[1]), t[2], "?")
    if kind == "cmp":
        return qvalid.pair(_term(t[1]), t[2], _term(t[3]))
    return tuple(t)


def _fn(flt: tuple) -> tuple:
    atoms, f = flt

    def value(bits: tuple) -> bool:
        node = f
        while not isinstance(node, bool):
            node = node[2] if bits[node[0]] else node[1]
        return node

    names = [_term(a) for a in atoms]
    models = frozenset(
        frozenset(a for a, b in zip(names, bits) if b)
        for bits in product((False, True), repeat=len(names))
        if value(bits)
    )
    return frozenset(names), models


def _band2(b2):
    if b2 is None:
        return None
    return (frozenset(map(_term, b2[0])), frozenset(map(_term, b2[1])), _fn(b2[2]))


def _band3(b3):
    return None if b3 is None else (frozenset(map(_term, b3[0])), _fn(b3[1]))


def test_claim_question_enumeration() -> None:
    for name, schema in _QVALID_SCHEMAS.items():
        expected = qvalid.Domain(schema).skeletons()
        emitted = _lattice_prefix(schema, qvalid.GRADE)
        grades = [qvalid.grade(s) for s in emitted]
        assert grades == sorted(grades), name  # §12.3: by grade within a Join
        assert len(set(emitted)) == len(emitted), name  # each element once
        missing, extra = expected - set(emitted), set(emitted) - expected
        assert not missing and not extra, (name, sorted(map(qvalid.key, missing))[:3],
                                           sorted(map(qvalid.key, extra))[:3])  # fmt: skip
        assert len(emitted) == len(expected), name


# --------------------------------------------------------------------------
# The register itself
# --------------------------------------------------------------------------


def _checks() -> set[str]:
    module = sys.modules[__name__]
    return {n for n, f in inspect.getmembers(module, inspect.isfunction)
            if n.startswith("test_claim_")}  # fmt: skip


def test_committed_register_is_current() -> None:
    assert claims.committed() == claims.render(), (
        "regenerate: PYTHONPATH=. python -m tests.conformance.claims --write"
    )


def test_every_artifact_exists_and_every_check_is_registered() -> None:
    checks, ids = _checks(), [c.id for c in all_cases()]
    used = set()
    for claim in claims.CLAIMS:
        for art in claim.artifacts:
            kind, _, ref = art.partition(":")
            if kind == "check":
                assert ref in checks, (claim.id, ref)
                used.add(ref)
            else:
                assert kind == "cases" and any(fnmatch(i, ref) for i in ids), (
                    claim.id,
                    art,
                )
    assert checks == used, sorted(checks - used)


@pytest.mark.parametrize("claim", claims.CLAIMS, ids=lambda c: c.id)
def test_status_matches_evidence(claim: claims.Claim) -> None:
    executable = {claims.Kind.EXHAUSTIVE, claims.Kind.ORACLE, claims.Kind.WITNESS}
    if claim.status in (claims.Status.HOLDS, claims.Status.COUNTEREXAMPLE):
        assert claim.artifacts and executable & set(claim.kinds), claim.id
    if claim.status is claims.Status.COUNTEREXAMPLE:
        assert claim.note, claim.id
    if claim.corrected:  # the old counterexample stays as a witness
        assert claim.status is claims.Status.HOLDS, claim.id
        assert any(a.endswith("counterexample") for a in claim.artifacts), claim.id
    if claim.status is claims.Status.UNCHECKED:
        assert not claim.artifacts, claim.id
    if claim.status is claims.Status.CITED:
        assert claims.Kind.CITED in claim.kinds and claim.note, claim.id


def test_review_table_lists_every_claim_once() -> None:
    text = claims.DOC.read_text(encoding="utf-8")
    section = text[text.index("## Independent review") :]
    rows = [line.split("|")[1].strip() for line in section.splitlines()
            if line.startswith("| `CLM-")]  # fmt: skip
    assert sorted(rows) == sorted(f"`{c.id}`" for c in claims.CLAIMS)
