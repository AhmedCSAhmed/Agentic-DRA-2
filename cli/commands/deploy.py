from __future__ import annotations

import asyncio

import typer
from rich.panel import Panel

from cli.display import console, mini_header


def deploy(
    image: str = typer.Argument(..., help="Full Docker image name from your registry"),
) -> None:
    """Deploy a Docker image to the best available machine."""

    mini_header()
    console.print(f"  Deploying [bold white]{image}[/bold white] ...\n")

    result_output: str = ""

    try:
        with console.status(
            "[purple]Agent is selecting a machine and pulling the image...[/purple]",
            spinner="dots",
            spinner_style="bright_magenta",
        ):
            from agent.run import run_dra_agent

            result = asyncio.run(run_dra_agent(image))
            result_output = result.final_output or ""

    except Exception as exc:
        console.print(
            Panel(
                f"[bold red]✗  Deployment failed[/bold red]\n\n[grey69]{exc}[/grey69]",
                border_style="red",
                padding=(1, 2),
            )
        )
        raise typer.Exit(1)

    console.print(
        Panel(
            f"[bold green]✓  Deployed[/bold green]  [white]{image}[/white]\n\n"
            f"[grey69]{result_output}[/grey69]",
            border_style="purple",
            padding=(1, 2),
        )
    )
    console.print()
