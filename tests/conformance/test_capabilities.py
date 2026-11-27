"""The capability page is generated from the code, and the docs do not outrun it.

``docs/reference/capabilities.md`` is rendered by ``capabilities.py`` from the
supervisor registry, the NL graph builder, the CLI parser, the planner's
capability profiles and the claims register (#140, AUD-12).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.conformance import capabilities

ROOT = Path(__file__).resolve().parents[2]

# Public pages that describe capabilities; the generated evidence pages and
# the specification itself are excluded.
PUBLIC = [
    ROOT / "README.md",
    *sorted((ROOT / "docs").glob("*.md")),
    *sorted((ROOT / "docs" / "features").glob("*.md")),
    *sorted((ROOT / "docs" / "guides").glob("*.md")),
    *sorted((ROOT / "docs" / "patterns").glob("*.md")),
    *sorted((ROOT / "docs" / "reference").glob("*.md")),
]

# Claims retired by #140 because the evidence does not support them.
RETIRED = {
    r"tight lower bound": "§8.14: the CTE count is minimal within the canonical "
    "form only (CLM-8.14-CANON is not evidenced)",
    r"complete typed query algebra": "the algebra has refused constructs "
    "(dgm-backend-matrix.md)",
    r"like Claude Code": "the NL REPL is a demonstration (D47)",
    r"`bigdata`": "there is no bigdata command",
    r"billions of rows": "no benchmark states a row count",
    r"Vectorized Path": "DimensionalLoader takes a list of dicts; it never "
    "switches to a DataFrame path",
    r"Fan Trap": "DimensionalQuery does no grain or fan-trap validation",
}


def test_committed_page_is_current() -> None:
    assert capabilities.committed() == capabilities.render(), (
        "regenerate: PYTHONPATH=. python -m tests.conformance.capabilities --write"
    )


def test_nl_commands_say_demo_while_the_pipeline_is_missing() -> None:
    if not capabilities.nl_pipeline_missing():
        pytest.skip("the §11.10 pipeline builds: demo labels can go")
    for name, text in capabilities.nl_commands():
        assert "demo" in text.lower(), (name, text)


def test_every_supervisor_tool_is_listed() -> None:
    from sqldim.core.query.dgm.nl.supervisor._schemas import TOOL_SCHEMAS

    text = capabilities.render()
    for name in TOOL_SCHEMAS:
        assert f"| `{name}` |" in text, name


def test_unavailable_tools_name_what_is_missing() -> None:
    regs = capabilities.registries()
    for name, tool in regs["default"].items():
        if not tool.capability_schema.available:
            assert tool.missing, name


@pytest.mark.parametrize("page", PUBLIC, ids=lambda p: str(p.relative_to(ROOT)))
def test_public_pages_do_not_repeat_retired_claims(page: Path) -> None:
    text = page.read_text(encoding="utf-8")
    for pattern, why in RETIRED.items():
        assert not re.search(pattern, text, re.I), (pattern, why)


def test_feature_page_names_the_tools_still_unavailable() -> None:
    """The DGM feature page lists the tools no configuration makes available."""
    text = (ROOT / "docs" / "features" / "dimensional_graph_model.md").read_text(
        encoding="utf-8"
    )
    regs = capabilities.registries()
    missing = {
        n for n, t in regs["kb+moves"].items() if not t.capability_schema.available
    }
    stated = text.split("configured, these are still unavailable:", 1)[1]
    stated = stated.split(".", 1)[0]
    assert set(re.findall(r"`(dgm_\w+)`", stated)) == missing


def test_readme_links_the_capability_page_and_evidence() -> None:
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    for page in (
        "docs/reference/capabilities.md",
        "docs/reference/dgm-conformance.md",
        "docs/reference/dgm-backend-matrix.md",
        "docs/reference/dgm-claims.md",
    ):
        assert page in text, page
