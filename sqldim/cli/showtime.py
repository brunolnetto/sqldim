"""CLI showtime command — ``sqldim showtime``.

A guided demo walkthrough showcasing the DGM Supervisor agent's capabilities.
Runs a curated sequence of questions against selected datasets,
presenting results with rich formatting and pauses between steps.
"""

from __future__ import annotations

import argparse


# Curated demo scenarios per dataset, plus cross-dataset questions
_DEMO_SCENARIOS: dict[str, list[dict[str, str]]] = {
    "__cross__": [
        {
            "title": "Discovery",
            "question": "What datasets are available and what can I ask about?",
        },
        {
            "title": "Cross-dataset exploration",
            "question": "Which analytical tools are available for exploring data?",
        },
    ],
    "commerce": [
        {
            "title": "Revenue query (commerce)",
            "question": "What is the total revenue by product category in the commerce dataset?",
        },
        {
            "title": "Customer analysis (commerce)",
            "question": "Show me the top commerce customers by order count",
        },
    ],
    "fintech": [
        {
            "title": "Transaction overview (fintech)",
            "question": "What are the most common fintech transaction types?",
        },
    ],
    "nba_analytics": [
        {
            "title": "Player statistics (nba_analytics)",
            "question": "Who are the top scorers in the nba_analytics dataset?",
        },
    ],
    "growth": [
        {
            "title": "Churn analysis (growth)",
            "question": "What is the growth domain subscription churn rate over time?",
        },
    ],
}


def _build_scenario_list(datasets: list[str]) -> list[dict[str, str]]:
    """Build the ordered list of demo scenarios for the given datasets."""
    scenarios = list(_DEMO_SCENARIOS.get("__cross__", []))
    for ds in datasets:
        scenarios.extend(_DEMO_SCENARIOS.get(ds, []))
    return scenarios


def cmd_showtime(args: argparse.Namespace) -> int:
    """Run the guided showtime demo."""
    try:
        from rich.console import Console
        from rich.markdown import Markdown
        from rich.panel import Panel
    except ImportError:
        print(
            "[sqldim showtime] Missing: rich. "
            "Install with: pip install 'sqldim[interactive]'"
        )
        return 1

    from sqldim.application.ask import KNOWN_DATASETS
    from sqldim.core.query.dgm.nl._agents import make_model
    from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph
    from sqldim.core.query.dgm.nl.supervisor._types import (
        SupervisorContext,
        SupervisorState,
    )
    from langgraph.checkpoint.memory import MemorySaver

    console = Console()
    all_datasets = sorted(KNOWN_DATASETS.keys())

    # Dataset selection
    selected: list[str] | None = getattr(args, "datasets", None)
    if selected:
        unknown = [d for d in selected if d not in all_datasets]
        if unknown:
            console.print(
                f"[red]Unknown dataset(s): {', '.join(unknown)}[/red]\n"
                f"Available: {', '.join(all_datasets)}"
            )
            return 1
        datasets = sorted(selected)
    else:
        datasets = all_datasets

    # Build model
    model_provider: str | None = getattr(args, "model_provider", None)
    model_name: str | None = getattr(args, "model", None)
    llm_model = None
    if model_provider:
        try:
            llm_model = make_model(provider=model_provider, model_name=model_name)
        except Exception as exc:
            console.print(f"[yellow]Warning: could not build LLM model: {exc}[/yellow]")

    context = SupervisorContext(
        available_datasets=datasets,
        model=llm_model,
    )
    scenarios = _build_scenario_list(datasets)

    if not scenarios:
        console.print("[yellow]No demo scenarios for the selected datasets.[/yellow]")
        return 0

    # Banner
    console.print(
        Panel.fit(
            "[bold magenta]sqldim showtime[/bold magenta] — Guided Demo\n"
            f"[dim]Datasets: {', '.join(datasets)}[/dim]\n"
            f"[dim]{len(scenarios)} scenario(s) queued[/dim]",
            border_style="magenta",
        )
    )
    console.print()

    conversation_history: list[dict[str, str]] = []

    for i, scenario in enumerate(scenarios, 1):
        title = scenario["title"]
        question = scenario["question"]

        console.print(
            Panel(
                f"[bold]Step {i}/{len(scenarios)}:[/bold] {title}\n"
                f'[italic]"{question}"[/italic]',
                border_style="cyan",
            )
        )

        initial = SupervisorState(
            utterance=question,
            conversation_history=list(conversation_history),
        )

        # Fresh checkpointer per step avoids deserialization warnings
        graph = build_supervisor_graph(
            checkpointer=MemorySaver(),
            context=context,
        )

        with console.status("[bold cyan]Running...[/bold cyan]", spinner="dots"):
            result = graph.invoke(
                initial.model_dump(),
                config={
                    "configurable": {"thread_id": f"showtime-{i}"},
                    "recursion_limit": 25,
                },
            )

        synthesis = result.get("synthesis", "")
        if synthesis:
            console.print(Panel(Markdown(synthesis), border_style="green"))
            conversation_history.append({"role": "user", "content": question})
            conversation_history.append({"role": "assistant", "content": synthesis})

        if getattr(args, "verbose", False):
            visited = result.get("visited_nodes", [])
            timings = result.get("node_timings", {})
            console.print(f"[dim]Nodes: {' → '.join(visited)}[/dim]")
            timing_parts = [f"{n}: {ms}ms" for n, ms in timings.items()]
            console.print(f"[dim]Timings: {', '.join(timing_parts)}[/dim]")
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
                    console.print(f"[dim]  [{_name}] {status} ERROR: {err}[/dim]")
                else:
                    display = {
                        k: v
                        for k, v in (data if isinstance(data, dict) else {}).items()
                        if k != "stub" and v is not None
                    }
                    console.print(f"[dim]  [{_name}] {status} {display}[/dim]")

        # Pause between scenarios (skip after last)
        if i < len(scenarios):
            console.print()
            try:
                console.input(
                    "[dim]Press Enter for next step (Ctrl-C to stop)...[/dim]"
                )
            except (EOFError, KeyboardInterrupt):
                console.print("\n[dim]Showtime ended early.[/dim]")
                return 0
            console.print()

    console.print(
        Panel.fit(
            "[bold green]Showtime complete![/bold green] "
            "Try [bold]sqldim nl chat[/bold] for interactive mode.",
            border_style="green",
        )
    )
    return 0
