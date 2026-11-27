"""Traceability matrix: spec clause → cases → status per backend.

The matrix is generated from the cases, the backends and the expectations,
and is kept in ``docs/reference/dgm-conformance.md`` between two markers.
``test_matrix.py`` fails when the committed text differs; regenerate with::

    PYTHONPATH=. python -m tests.conformance.matrix --write
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

from tests.conformance.cases import DERIVED, Case, all_cases
from tests.conformance.expectations import NOT_COVERED, Status, status
from tests.conformance.runners import BACKENDS

DOC = Path(__file__).resolve().parents[2] / "docs" / "reference" / "dgm-conformance.md"
BEGIN = "<!-- BEGIN GENERATED: tests/conformance/matrix.py -->"
END = "<!-- END GENERATED -->"

_SHORT = {
    Status.PASS: "pass",
    Status.XFAIL: "**non-conf.**",
    Status.REFUSED: "refused",
    Status.NOT_EXPRESSIBLE: "n/e",
}


def _cell(cases: list[Case], backend_name: str) -> str:
    counts: dict[Status, int] = defaultdict(int)
    for c in cases:
        st = status(c, backend_name)
        if st is not None:
            counts[st] += 1
    if not counts:
        return "—"
    return ", ".join(f"{n} {_SHORT[s]}" for s, n in sorted(counts.items(),
                                                       key=lambda kv: kv[0].value))  # fmt: skip


def _ids(cases: list[Case]) -> str:
    if len(cases) <= 4:
        return ", ".join(f"`{c.id}`" for c in cases)
    return f"`{cases[0].id}` … `{cases[-1].id}` ({len(cases)})"


def render() -> str:
    cases = all_cases()
    by_clause: dict[str, list[Case]] = defaultdict(list)
    for c in cases:
        by_clause[c.clause].append(c)
    names = [b.name for b in BACKENDS]
    lines = [
        BEGIN,
        "",
        f"{len(cases)} cases; "
        f"{sum(c.expected != DERIVED for c in cases)} carry a value stated by the "
        "specification, the others are derived by the oracle from the definitions. "
        "Every case is checked against the oracle.",
        "",
        "| Clause | Cases | " + " | ".join(f"`{n}`" for n in names) + " |",
        "|---|---|" + "---|" * len(names),
    ]
    for clause in sorted(by_clause):
        cs = by_clause[clause]
        cells = " | ".join(_cell(cs, n) for n in names)
        lines.append(f"| {clause} | {_ids(cs)} | {cells} |")
    lines += ["", "### Non-conformances (strict xfail)", ""]
    lines += ["| Case | Backend | Observed vs. specified |", "|---|---|---|"]
    for c in cases:
        for n in names:
            if status(c, n) is Status.XFAIL:
                lines.append(f"| `{c.id}` | `{n}` | {status_reason(c, n)} |")
    lines += ["", "### Refused (fail-closed) and not expressible", ""]
    lines += ["| Case | Backend | Status | Reason |", "|---|---|---|---|"]
    for c in cases:
        for n in names:
            st = status(c, n)
            if st in (Status.REFUSED, Status.NOT_EXPRESSIBLE):
                lines.append(
                    f"| `{c.id}` | `{n}` | {_SHORT[st]} | {status_reason(c, n)} |"
                )
    lines += ["", "### Not covered", ""]
    lines += ["| Clause | Why no executable case |", "|---|---|"]
    lines += [f"| {clause} | {why} |" for clause, why in NOT_COVERED]
    lines += ["", END]
    return "\n".join(lines) + "\n"


def status_reason(case: Case, backend_name: str) -> str:
    from tests.conformance.expectations import reason

    return reason(case, backend_name) or ""


def committed() -> str:
    text = DOC.read_text(encoding="utf-8")
    start, end = text.index(BEGIN), text.index(END) + len(END)
    return text[start:end] + "\n"


def write() -> None:
    text = DOC.read_text(encoding="utf-8")
    start, end = text.index(BEGIN), text.index(END) + len(END)
    DOC.write_text(text[:start] + render().rstrip("\n") + text[end:], encoding="utf-8")


if __name__ == "__main__":
    if "--write" in sys.argv:
        write()
    else:
        sys.stdout.write(render())
