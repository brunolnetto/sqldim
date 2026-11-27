"""The documented examples run and print what the documentation says (#140).

Each page's ``python`` blocks run in order, in one fresh interpreter (the
models of different pages declare the same table names), from a temporary
directory.  A block that the page labels as illustrative is listed in
``ILLUSTRATIVE`` by a fragment of its text and may fail; every other block
must run.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

PAGES = {
    "README.md": ["ORD-1 1", "ORD-2 2"],
    "docs/getting-started.md": [
        "(1, 'alice@example.com', 'free', 0, 0)",
        "(3, 'alice@example.com', 'pro', 1, 1)",
        "('Alice', 'free', 209.97)",
        "('Bob', 'pro', 89.99)",
    ],
    "docs/features/dimensional_graph_model.md": [],
    "docs/features/notifications.md": [],
    "docs/features/data_contracts.md": [
        "['NO_DUPLICATES', 'NULL_RATE']",
        "['no_rule_errors']",
    ],
    "docs/features/medallion_layers.md": ["['customer_snapshot']"],
    "docs/features/observability.md": [
        "ok True",
        "error boom",
        "['counter', 'histogram', 'gauge']",
    ],
    "docs/guides/big_data.md": [
        "2 1 1",
        "[('c1', 'free', False), ('c1', 'pro', True), ('c2', 'pro', True)]",
    ],
    "docs/guides/lazy_loaders.md": [
        "[('U-001', 32778), ('U-002', 2147483648)]",
        "[('U-001', 149.99), ('U-002', 20.0)]",
        "[('p1', 'p2', 22)]",
    ],
    "docs/guides/semantic_layer.md": ["[('Games', 7.0)]", "[('Toys', 10.0)]"],
    "docs/guides/vectorized_etl.md": ["1 1 1", "['B', 'C']"],
    "docs/patterns/dual_paradigm.md": ["['Pippen', 'Rodman']"],
    "docs/patterns/hybrid_scd.md": ["[(210, False), (300, True)]"],
}

# Blocks that need tables the page does not create, or a name of yours.
ILLUSTRATIVE = (
    "# ... create/populate tables ...",
    'final="my_final_cte"',
    "# After a quality gate failure",
    "def escalate_if_repeated",
)

_RUNNER = """
import json, sys, traceback
blocks = json.loads(sys.stdin.read())
ns = {"__name__": "__doc_example__"}
failed = []
for i, code in enumerate(blocks):
    try:
        exec(compile(code, f"block {i}", "exec"), ns)
    except Exception:
        failed.append((i, traceback.format_exc(limit=1)))
print("@@FAILED@@" + json.dumps(failed))
"""


def _blocks(page: str) -> list[str]:
    text = (ROOT / page).read_text(encoding="utf-8")
    return re.findall(r"```python\n(.*?)```", text, re.S)


@pytest.mark.parametrize("page", sorted(PAGES))
def test_page_examples_run(page: str, tmp_path: Path) -> None:
    blocks = _blocks(page)
    assert blocks, page
    env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONWARNINGS": "ignore"}
    proc = subprocess.run(
        [sys.executable, "-c", _RUNNER],
        input=json.dumps(blocks),
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env=env,
        timeout=300,
    )
    out, _, tail = proc.stdout.rpartition("@@FAILED@@")
    assert proc.returncode == 0 and tail, proc.stderr[-2000:]
    for i, error in json.loads(tail):
        assert any(f in blocks[i] for f in ILLUSTRATIVE), (page, i, error)
    for line in PAGES[page]:
        assert line in out, (page, line)
