"""CLI showtime command — ``sqldim nl showtime``.

A guided demonstration of the DGM Supervisor.  It always runs in the explicit
demo mode (D47, ADR 0013): the keyword planner and the heuristic
``dgm_query``, whose results are marked demo and are not contract results.
Runs a curated sequence of questions against selected datasets, presenting
results with rich formatting and pauses between steps.
"""

from __future__ import annotations

import argparse
from typing import Any


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


def _resolve_showtime_datasets(
    selected: list[str] | None, all_datasets: list[str], console: Any
) -> list[str] | None:
    """Validate the dataset selection; ``None`` means unknown names."""
    if not selected:
        return all_datasets
    unknown = [d for d in selected if d not in all_datasets]
    if unknown:
        console.print(
            f"[red]Unknown dataset(s): {', '.join(unknown)}[/red]\n"
            f"Available: {', '.join(all_datasets)}"
        )
        return None
    return sorted(selected)


def _build_showtime_model(args: argparse.Namespace, console: Any) -> Any:
    """Build the LLM model from CLI args, warning (not failing) on errors."""
    from sqldim.core.query.dgm.nl._agents import make_model

    model_provider: str | None = getattr(args, "model_provider", None)
    model_name: str | None = getattr(args, "model", None)
    if not model_provider:
        return None
    try:
        return make_model(provider=model_provider, model_name=model_name)
    except Exception as exc:
        console.print(f"[yellow]Warning: could not build LLM model: {exc}[/yellow]")
        return None


def _print_showtime_verbose(result: Any, console: Any) -> None:
    """Print nodes, timings and per-tool hops of one showtime step."""
    from sqldim.cli.ask import _format_hop

    console.print(f"[dim]Nodes: {' → '.join(result.visited_nodes)}[/dim]")
    timing_parts = [f"{n}: {ms}ms" for n, ms in result.node_timings.items()]
    console.print(f"[dim]Timings: {', '.join(timing_parts)}[/dim]")
    for hop in result.results.values():
        console.print(f"[dim]  {_format_hop(hop)}[/dim]")


def _run_showtime_step(
    i: int,
    scenario: dict[str, str],
    total: int,
    new_runtime: Any,
    console: Any,
    conversation_history: list[dict[str, str]],
) -> Any:
    """Run one scenario through a fresh supervisor graph and render it."""
    from rich.markdown import Markdown
    from rich.panel import Panel
    from sqldim.core.query.dgm.nl.supervisor._graph import run_supervisor

    title = scenario["title"]
    question = scenario["question"]

    console.print(
        Panel(
            f'[bold]Step {i}/{total}:[/bold] {title}\n[italic]"{question}"[/italic]',
            border_style="cyan",
        )
    )

    with console.status("[bold cyan]Running...[/bold cyan]", spinner="dots"):
        result = run_supervisor(
            question,
            new_runtime(),
            thread_id=f"showtime-{i}",
            history=list(conversation_history),
        )

    synthesis = result.synthesis or ""
    if synthesis:
        console.print(Panel(Markdown(synthesis), border_style="green"))
        conversation_history.append({"role": "user", "content": question})
        conversation_history.append({"role": "assistant", "content": synthesis})
    return result


def _pause_between_steps(console: Any) -> bool:
    """Wait for Enter; return False when the user stops the demo."""
    console.print()
    try:
        console.input("[dim]Press Enter for next step (Ctrl-C to stop)...[/dim]")
    except (EOFError, KeyboardInterrupt):
        console.print("\n[dim]Showtime ended early.[/dim]")
        return False
    console.print()
    return True


def cmd_showtime(args: argparse.Namespace) -> int:
    """Run the guided showtime demo."""
    try:
        from rich.console import Console
        from rich.panel import Panel
    except ImportError:
        print(
            "[sqldim showtime] Missing: rich. "
            "Install with: pip install 'sqldim[interactive]'"
        )
        return 1

    from sqldim.application.ask import KNOWN_DATASETS
    from sqldim.application.supervisor_host import local_supervisor_runtime

    console = Console()
    all_datasets = sorted(KNOWN_DATASETS.keys())

    # Dataset selection
    datasets = _resolve_showtime_datasets(
        getattr(args, "datasets", None), all_datasets, console
    )
    if datasets is None:
        return 1

    # Build model
    llm_model = _build_showtime_model(args, console)

    # Showtime is the demo walkthrough: choosing the command selects demo
    # mode explicitly for every scenario (D47).
    def new_runtime() -> Any:
        return local_supervisor_runtime(datasets, model=llm_model, demo=True)

    scenarios = _build_scenario_list(datasets)

    if not scenarios:
        console.print("[yellow]No demo scenarios for the selected datasets.[/yellow]")
        return 0

    # Banner
    console.print(
        Panel.fit(
            "[bold magenta]sqldim showtime[/bold magenta] — Guided Demo\n"
            "[dim]Demo mode: heuristic output, not contract results (D47)[/dim]\n"
            f"[dim]Datasets: {', '.join(datasets)}[/dim]\n"
            f"[dim]{len(scenarios)} scenario(s) queued[/dim]",
            border_style="magenta",
        )
    )
    console.print()

    conversation_history: list[dict[str, str]] = []

    for i, scenario in enumerate(scenarios, 1):
        result = _run_showtime_step(
            i, scenario, len(scenarios), new_runtime, console, conversation_history
        )

        if getattr(args, "verbose", False):
            _print_showtime_verbose(result, console)

        # Pause between scenarios (skip after last)
        if i < len(scenarios) and not _pause_between_steps(console):
            return 0

    console.print(
        Panel.fit(
            "[bold green]Showtime complete![/bold green] "
            "Try [bold]sqldim nl chat[/bold] for interactive mode.",
            border_style="green",
        )
    )
    return 0
