from __future__ import annotations

import typer
from rich import box
from rich.table import Table

from cli.display import console, mini_header


def status() -> None:
    """Show all registered machines and their cluster status."""

    mini_header()

    try:
        from agent.env import load_project_dotenv
        from dra.database import Database
        from dra.repositories.machines import MachineRepository

        load_project_dotenv()
        repo = MachineRepository(Database())
        machines = repo.list_machines()

    except Exception as exc:
        console.print(f"  [red]Could not reach cluster:[/red] [grey69]{exc}[/grey69]\n")
        raise typer.Exit(1)

    if not machines:
        console.print("  [grey69]No machines registered in the cluster.[/grey69]\n")
        return

    table = Table(
        box=box.SIMPLE,
        border_style="purple",
        header_style="medium_purple1",
        show_header=True,
        padding=(0, 2),
    )
    table.add_column("Machine", style="white")
    table.add_column("Type", style="grey69")
    table.add_column("gRPC Target", style="grey69")
    table.add_column("Status", style="bold green")

    for m in machines:
        table.add_row(
            m.machine_name or m.machine_id,
            m.machine_type or "—",
            getattr(m, "dra_grpc_target", None) or "—",
            "● Online",
        )

    console.print(table)
    console.print()
