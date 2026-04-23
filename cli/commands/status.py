from __future__ import annotations

from datetime import datetime, timezone

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
    table.add_column("Cores", style="grey69")
    table.add_column("Heartbeat", style="grey69")
    table.add_column("Status", style="white")

    shown_any = False
    for m in machines:
        raw_mem = getattr(m, "available_gb", None)
        if isinstance(raw_mem, (int, float)) and float(raw_mem) <= 0.0:
            # Hide machines with no available resources.
            continue

        shown_any = True
        raw_cores = getattr(m, "available_cores", None)
        cores_cell = f"{raw_cores:.1f}" if isinstance(raw_cores, (int, float)) else "—"
        table.add_row(
            m.machine_name or m.machine_id,
            m.machine_type or "—",
            getattr(m, "dra_grpc_target", None) or "—",
            (f"{raw_mem:.0f} GB" if isinstance(raw_mem, (int, float)) else "—"),
            cores_cell,
            _heartbeat_cell(getattr(m, "last_heartbeat_at", None)),
            _status_cell(repo, m.machine_id, getattr(m, "dra_grpc_target", None)),
        )

    if not shown_any:
        console.print("  [grey69]No machines with available resources.[/grey69]\n")
        return

    console.print(table)
    console.print()


def _heartbeat_cell(last_heartbeat_at: datetime | None) -> str:
    if last_heartbeat_at is None:
        return "—"
    hb = last_heartbeat_at
    if hb.tzinfo is None:
        hb = hb.replace(tzinfo=timezone.utc)
    age_s = int((datetime.now(timezone.utc) - hb).total_seconds())
    if age_s < 60:
        return f"{age_s}s ago"
    if age_s < 3600:
        return f"{age_s // 60}m ago"
    return f"{age_s // 3600}h ago"


def _status_cell(repo, machine_id: str, grpc_target: str | None) -> str:
    result = probe_grpc_target(grpc_target)
    if result.ok:
        latency = f" [grey69]{result.latency_ms:.0f}ms[/grey69]" if result.latency_ms is not None else ""
        return f"[bold green]● Online[/bold green]{latency}"

    detail = (result.error or "").strip()
    detail = detail if detail else "connection failed"
    return f"[bold red]● Offline[/bold red] [grey69]({detail})[/grey69]"
