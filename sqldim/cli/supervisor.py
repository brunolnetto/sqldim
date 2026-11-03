"""CLI supervisor command — ``sqldim supervisor``."""

from __future__ import annotations

import argparse


def cmd_supervisor(args: argparse.Namespace) -> int:
    """Run the supervisor agent across all known datasets."""
    from sqldim.application.ask import KNOWN_DATASETS
    from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph
    from sqldim.core.query.dgm.nl.supervisor._types import (
        SupervisorContext,
        SupervisorState,
    )
    from sqldim.core.query.dgm.nl._agents import make_model

    all_datasets = sorted(KNOWN_DATASETS.keys())
    selected: list[str] | None = getattr(args, "datasets", None)
    if selected:
        unknown = [d for d in selected if d not in all_datasets]
        if unknown:
            print(f"[sqldim supervisor] Unknown dataset(s): {', '.join(unknown)}")
            print(f"[sqldim supervisor] Available: {', '.join(all_datasets)}")
            return 1
        available = sorted(selected)
    else:
        available = all_datasets
    model_name: str | None = getattr(args, "model", None)
    model_provider: str | None = getattr(args, "model_provider", None)

    llm_model = None
    if model_provider:
        try:
            llm_model = make_model(provider=model_provider, model_name=model_name)
        except Exception as exc:
            print(f"[sqldim supervisor] Warning: could not build LLM model: {exc}")

    context = SupervisorContext(
        available_datasets=available,
        model=llm_model,
    )

    graph = build_supervisor_graph(context=context)
    initial = SupervisorState(utterance=args.question)

    result = graph.invoke(
        initial.model_dump(),
        config={
            "configurable": {"thread_id": "cli-supervisor"},
            "recursion_limit": 25,
        },
    )

    synthesis = result.get("synthesis")
    if synthesis:
        print(synthesis)
    else:  # pragma: no cover  — stub graph always produces synthesis
        print("[sqldim supervisor] No synthesis produced.")

    if args.verbose:
        visited = result.get("visited_nodes", [])
        timings = result.get("node_timings", {})
        print(f"\n[sqldim supervisor] Nodes visited: {visited}")
        for node, ms in timings.items():
            print(f"  {node}: {ms}ms")
        for hop in result.get("results", []):
            _name = (
                hop.get("tool_name")
                if isinstance(hop, dict)
                else getattr(hop, "tool_name", "?")
            )
            ok = (
                hop.get("success")
                if isinstance(hop, dict)
                else getattr(hop, "success", False)
            )
            data = (
                (hop.get("data") or {})
                if isinstance(hop, dict)
                else (getattr(hop, "data") or {})
            )
            err = (
                hop.get("error")
                if isinstance(hop, dict)
                else getattr(hop, "error", None)
            )
            status = "\u2713" if ok else "\u2717"
            if err:
                print(f"  [{_name}] {status} ERROR: {err}")
            else:
                display = {
                    k: v
                    for k, v in (data if isinstance(data, dict) else {}).items()
                    if k != "stub" and v is not None
                }
                print(f"  [{_name}] {status} {display}")

    return 0
