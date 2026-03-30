"""CLI supervisor command — ``sqldim supervisor``."""

from __future__ import annotations

import argparse


def cmd_supervisor(args: argparse.Namespace) -> int:
    """Run the supervisor agent across all known datasets."""
    from sqldim.application.ask import KNOWN_DATASETS
    from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph
    from sqldim.core.query.dgm.nl.supervisor._types import SupervisorContext, SupervisorState
    from sqldim.core.query.dgm.nl._agents import make_model

    available = sorted(KNOWN_DATASETS.keys())
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
    else:
        print("[sqldim supervisor] No synthesis produced.")

    if args.verbose:
        visited = result.get("visited_nodes", [])
        timings = result.get("node_timings", {})
        print(f"\n[sqldim supervisor] Nodes visited: {visited}")
        for node, ms in timings.items():
            print(f"  {node}: {ms}ms")

    return 0
