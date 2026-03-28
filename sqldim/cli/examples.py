"""CLI example commands — ``sqldim example list/run``."""

from __future__ import annotations

import argparse


def _load_one_showcase(pkg_name: str, modname: str, kind: str) -> tuple | None:
    import importlib

    showcase_path = f"{pkg_name}.{modname}.showcase"
    try:
        mod = importlib.import_module(showcase_path)
    except ImportError:
        return None
    meta = getattr(mod, "EXAMPLE_METADATA", None)
    if meta is None:
        return None
    return (
        meta["name"],
        (meta["title"], meta["description"], showcase_path, meta["entry_point"], kind),
    )


def _scan_pkg(pkg_name: str, kind: str) -> dict:
    import importlib
    import pkgutil

    result: dict = {}
    try:
        pkg = importlib.import_module(pkg_name)
    except ImportError:
        return result
    for _, modname, ispkg in pkgutil.iter_modules(pkg.__path__):
        if not ispkg:
            continue
        loaded = _load_one_showcase(pkg_name, modname, kind)
        if loaded is not None:
            name, entry = loaded
            result[name] = entry
    return result


def _discover_examples() -> dict:
    """
    Auto-discover examples by scanning showcase modules for ``EXAMPLE_METADATA``.

    Scans all sub-packages of ``sqldim.application.examples.real_world`` and
    ``sqldim.application.examples.features`` that contain a ``showcase.py`` module
    exposing an ``EXAMPLE_METADATA`` dict.  Returns a mapping of CLI name →
    ``(title, description, module_path, entry_fn, kind)`` tuples.
    """
    result: dict = {}
    for pkg_name, kind in [
        ("sqldim.application.examples.real_world", "real_world"),
        ("sqldim.application.examples.features", "features"),
    ]:
        result.update(_scan_pkg(pkg_name, kind))
    return result


def _print_example_group(kind_key: str, items: list, labels: dict, first: bool) -> bool:
    if not items:
        return first
    if not first:
        print()
    print(
        f"[sqldim] {labels.get(kind_key, kind_key)} "
        f"(run with: sqldim example run <name>)\n"
    )
    for name, title, desc in sorted(items, key=lambda x: x[0]):
        print(f"  {name:<20} {title}")
        print(f"  {'':20} {desc}\n")
    return False


def cmd_example_list(args: argparse.Namespace) -> None:
    """List all available examples, grouped by kind.

    Prints each example name, title, and a one-line description so users
    can choose which showcase to run with ``sqldim example run <name>``.
    """
    examples = _discover_examples()
    if not examples:
        print("[sqldim] No examples found (is application/ on the Python path?)")
        return

    groups: dict[str, list] = {"real_world": [], "features": []}
    for name, (title, desc, _mod, _fn, kind) in examples.items():
        groups.setdefault(kind, []).append((name, title, desc))

    labels = {"real_world": "Real-world pipelines", "features": "Feature showcases"}
    first = True
    for kind_key, items in groups.items():
        first = _print_example_group(kind_key, items, labels, first)


def cmd_example_run(args: argparse.Namespace) -> int:
    """Run a named example showcase.

    Dynamically imports and invokes the showcase function for the given
    example name, supporting both async and synchronous entry-points.
    Returns 1 if the name is unrecognised, 0 on success.
    """
    import asyncio
    import importlib

    name = args.name.lower()
    examples = _discover_examples()
    if name not in examples:
        known = ", ".join(sorted(examples))
        print(
            f"[sqldim] Unknown example '{name}'. "
            f"Run 'sqldim example list' to see options.\n"
            f"  Known: {known}"
        )
        return 1
    _title, _desc, mod_path, fn_name, _kind = examples[name]
    mod = importlib.import_module(mod_path)
    fn = getattr(mod, fn_name)
    if asyncio.iscoroutinefunction(fn):
        asyncio.run(fn())
    else:
        fn()
    return 0
