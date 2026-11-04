"""CLI chat command — ``sqldim chat``.

Interactive conversational REPL for the DGM Supervisor agent.
Uses rich for output rendering and prompt_toolkit for input handling,
maintaining conversation history across turns within a session.
"""

from __future__ import annotations

import argparse
import uuid


def _check_interactive_deps() -> tuple[bool, str]:
    """Check if interactive dependencies (rich, prompt_toolkit) are available."""
    missing: list[str] = []
    try:
        import rich  # noqa: F401
    except ImportError:
        missing.append("rich")
    try:
        import prompt_toolkit  # noqa: F401
    except ImportError:
        missing.append("prompt-toolkit")
    if missing:
        return False, (
            f"Missing: {', '.join(missing)}. "
            "Install with: pip install 'sqldim[interactive]'"
        )
    return True, ""


def _select_datasets_interactive(
    all_datasets: list[str],
    *,
    _prompt_fn: object | None = None,
) -> list[str]:
    """Present an interactive dataset picker using prompt_toolkit."""
    from rich.console import Console
    from rich.table import Table

    if _prompt_fn is None:
        from prompt_toolkit import prompt as _prompt_fn

    console = Console()

    table = Table(title="Available Datasets", show_lines=False)
    table.add_column("#", style="bold cyan", width=4)
    table.add_column("Dataset", style="green")
    for i, ds in enumerate(all_datasets, 1):
        table.add_row(str(i), ds)
    console.print(table)
    console.print(
        "\n[dim]Enter dataset numbers (comma-separated) or 'all' for all datasets:[/dim]"
    )

    while True:
        choice = _prompt_fn("datasets> ").strip()
        if not choice or choice.lower() == "all":
            return all_datasets
        try:
            indices = [int(x.strip()) for x in choice.split(",")]
            selected = []
            for idx in indices:
                if 1 <= idx <= len(all_datasets):
                    selected.append(all_datasets[idx - 1])
                else:
                    console.print(f"[red]Invalid number: {idx}[/red]")
                    selected = []
                    break
            if selected:
                return sorted(set(selected))
        except ValueError:
            # Try matching by name
            names = [x.strip() for x in choice.split(",")]
            valid = [n for n in names if n in all_datasets]
            invalid = [n for n in names if n not in all_datasets]
            if invalid:
                console.print(f"[red]Unknown: {', '.join(invalid)}[/red]")
                continue
            if valid:
                return sorted(set(valid))
        console.print("[yellow]Try again — enter numbers, names, or 'all'.[/yellow]")


def _run_repl(
    graph: object,
    datasets: list[str],
    *,
    verbose: bool = False,
    _session_factory: object | None = None,
) -> int:
    """Run the interactive REPL loop."""
    from rich.console import Console
    from rich.markdown import Markdown
    from rich.panel import Panel

    from sqldim.core.query.dgm.nl.supervisor._types import SupervisorState

    console = Console()

    if _session_factory is not None:
        session = _session_factory()
    else:
        from prompt_toolkit import PromptSession
        from prompt_toolkit.history import InMemoryHistory

        session = PromptSession(history=InMemoryHistory())

    thread_id_base = f"chat-{uuid.uuid4().hex[:8]}"
    turn_counter = 0
    conversation_history: list[dict[str, str]] = []

    # Welcome banner
    console.print(
        Panel.fit(
            "[bold cyan]sqldim chat[/bold cyan] — Interactive DGM Supervisor\n"
            f"[dim]Datasets: {', '.join(datasets)}[/dim]\n"
            "[dim]Type [bold]/help[/bold] for commands, "
            "[bold]/quit[/bold] or Ctrl-D to exit[/dim]",
            border_style="cyan",
        )
    )

    while True:
        try:
            utterance = session.prompt("\n🔍 sqldim> ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[dim]Goodbye![/dim]")
            return 0

        if not utterance:
            continue

        # Slash commands
        if utterance.startswith("/"):
            cmd = utterance.lower().split()[0]
            if cmd in ("/quit", "/exit", "/q"):
                console.print("[dim]Goodbye![/dim]")
                return 0
            if cmd == "/help":
                console.print(
                    Panel(
                        "[bold]/help[/bold]     — Show this help\n"
                        "[bold]/datasets[/bold] — List active datasets\n"
                        "[bold]/history[/bold]  — Show conversation history\n"
                        "[bold]/clear[/bold]    — Clear conversation history\n"
                        "[bold]/quit[/bold]     — Exit the REPL",
                        title="Commands",
                        border_style="blue",
                    )
                )
                continue
            if cmd == "/datasets":
                console.print(f"[cyan]Active datasets:[/cyan] {', '.join(datasets)}")
                continue
            if cmd == "/history":
                if not conversation_history:
                    console.print("[dim]No conversation history yet.[/dim]")
                else:
                    for entry in conversation_history:
                        role = entry["role"]
                        content = entry["content"]
                        if role == "user":
                            console.print(f"[bold green]You:[/bold green] {content}")
                        else:
                            console.print(
                                f"[bold cyan]sqldim:[/bold cyan] {content[:120]}..."
                                if len(content) > 120
                                else f"[bold cyan]sqldim:[/bold cyan] {content}"
                            )
                continue
            if cmd == "/clear":
                conversation_history.clear()
                console.print("[dim]Conversation history cleared.[/dim]")
                continue
            console.print(f"[yellow]Unknown command: {cmd}. Type /help.[/yellow]")
            continue

        # Invoke the supervisor graph
        initial = SupervisorState(
            utterance=utterance,
            conversation_history=list(conversation_history),
        )

        with console.status("[bold cyan]Thinking...[/bold cyan]", spinner="dots"):
            turn_counter += 1
            result = graph.invoke(
                initial.model_dump(),
                config={
                    "configurable": {"thread_id": f"{thread_id_base}-{turn_counter}"},
                    "recursion_limit": 25,
                },
            )

        synthesis = result.get("synthesis", "")
        if synthesis:
            console.print()
            console.print(Panel(Markdown(synthesis), border_style="green"))

            # Update conversation history
            conversation_history.append({"role": "user", "content": utterance})
            conversation_history.append({"role": "assistant", "content": synthesis})
        else:
            console.print("[yellow]No synthesis produced.[/yellow]")

        if verbose:
            visited = result.get("visited_nodes", [])
            timings = result.get("node_timings", {})
            console.print(f"\n[dim]Nodes: {' → '.join(visited)}[/dim]")
            timing_parts = [f"{n}: {ms}ms" for n, ms in timings.items()]
            console.print(f"[dim]Timings: {', '.join(timing_parts)}[/dim]")

    return 0  # pragma: no cover  — loop exits via return above


def cmd_chat(args: argparse.Namespace) -> int:
    """Run the interactive chat REPL."""
    ok, msg = _check_interactive_deps()
    if not ok:
        print(f"[sqldim chat] {msg}")
        return 1

    from sqldim.application.ask import KNOWN_DATASETS
    from sqldim.core.query.dgm.nl._agents import make_model
    from sqldim.core.query.dgm.nl.supervisor._graph import build_supervisor_graph
    from sqldim.core.query.dgm.nl.supervisor._types import SupervisorContext

    all_datasets = sorted(KNOWN_DATASETS.keys())
    if not all_datasets:
        print("[sqldim chat] No datasets discovered.")
        return 1  # pragma: no cover

    # Dataset selection
    selected: list[str] | None = getattr(args, "datasets", None)
    if selected:
        unknown = [d for d in selected if d not in all_datasets]
        if unknown:
            print(f"[sqldim chat] Unknown dataset(s): {', '.join(unknown)}")
            print(f"[sqldim chat] Available: {', '.join(all_datasets)}")
            return 1
        datasets = sorted(selected)
    else:
        datasets = _select_datasets_interactive(all_datasets)

    # Build LLM model
    model_provider: str | None = getattr(args, "model_provider", None)
    model_name: str | None = getattr(args, "model", None)

    llm_model = None
    if model_provider:
        try:
            llm_model = make_model(provider=model_provider, model_name=model_name)
        except Exception as exc:
            print(f"[sqldim chat] Warning: could not build LLM model: {exc}")

    context = SupervisorContext(
        available_datasets=datasets,
        model=llm_model,
    )
    graph = build_supervisor_graph(context=context)

    return _run_repl(graph, datasets, verbose=getattr(args, "verbose", False))
