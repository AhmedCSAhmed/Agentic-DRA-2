from __future__ import annotations

import typer
from rich import box
from rich.table import Table

from cli.display import console, mini_header
from cli.health import probe_grpc_target


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
    table.add_column("Memory", style="grey69")
    table.add_column("Status", style="white")

    shown_any = False
    for m in machines:
        raw_mem = getattr(m, "available_gb", None)
        if isinstance(raw_mem, (int, float)) and float(raw_mem) <= 0.0:
            # Hide machines with no available resources.
            continue

        shown_any = True
        table.add_row(
            m.machine_name or m.machine_id,
            m.machine_type or "—",
            getattr(m, "dra_grpc_target", None) or "—",
            (f"{raw_mem:.0f} GB" if isinstance(raw_mem, (int, float)) else "—"),
            _status_cell(repo, m.machine_id, getattr(m, "dra_grpc_target", None)),
        )

    if not shown_any:
        console.print("  [grey69]No machines with available resources.[/grey69]\n")
        return

    console.print(table)
    console.print()


def _status_cell(repo, machine_id: str, grpc_target: str | None) -> str:
    result = probe_grpc_target(grpc_target)
    if result.ok:
        return "[bold green]● Online[/bold green]"

    # “Offline” here specifically means the connection failed (e.g. refused/unavailable/timeout).
    detail = (result.error or "").strip()
    detail = detail if detail else "connection failed"
    return f"[bold red]● Offline[/bold red] [grey69]({detail})[/grey69]"
