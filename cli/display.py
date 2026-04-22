from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.align import Align
from rich import box
from rich.table import Table

console = Console()


def _get_cluster_status() -> tuple[int, list]:
    from agent.env import load_project_dotenv
    from dra.database import Database
    from dra.repositories.machines import MachineRepository

    load_project_dotenv()
    repo = MachineRepository(Database())
    machines = repo.list_machines()
    return len(machines), machines


def boot_screen() -> None:
    title = Text(justify="center")
    title.append("A T L A S\n", style="bold bright_magenta")
    title.append("Distributed Resource Allocator", style="grey69")

    console.print()
    console.print(
        Panel(
            Align.center(title),
            border_style="purple",
            padding=(1, 6),
            expand=False,
        ),
        justify="center",
    )
    console.print()

    console.print("  Deploy any Docker image to the cluster.", style="white")
    console.print("  Provide the full image name from your registry:\n", style="grey69")

    examples = [
        ("Docker Hub  ", "atlas deploy mycompany/myapp:v1.0"),
        ("GitHub GHCR ", "atlas deploy ghcr.io/org/app:latest"),
        ("AWS ECR     ", "atlas deploy 123456.dkr.ecr.us-east-1.amazonaws.com/app:prod"),
    ]
    for label, cmd in examples:
        console.print(f"    [medium_purple1]{label}[/medium_purple1]  [white]{cmd}[/white]")

    console.print()
    console.print("  [grey69]Commands:[/grey69]")
    console.print(
        "    [bold white]deploy [italic]<image>[/italic][/bold white]"
        "      [grey69]Deploy a container to the best available machine[/grey69]"
    )
    console.print(
        "    [bold white]help[/bold white]"
        "               [grey69]Show all commands[/grey69]"
    )
    console.print(
        "    [bold white]q[/bold white]"
        "                  [grey69]Quit[/grey69]"
    )
    console.print()



def admin_boot_screen() -> None:
    try:
        count, machines = _get_cluster_status()
        status = "Ready"
        status_style = "bold green"
    except Exception:
        count, machines, status, status_style = 0, [], "Unavailable", "bold red"

    title = Text(justify="center")
    title.append("A T L A S\n", style="bold bright_magenta")
    title.append("Distributed Resource Allocator\n\n", style="grey69")
    title.append("Machines online: ", style="white")
    title.append(str(count), style="bold white")
    title.append("        Status: ", style="white")
    title.append(status, style=status_style)

    console.print()
    console.print(
        Panel(
            Align.center(title),
            border_style="purple",
            padding=(1, 6),
            expand=False,
        ),
        justify="center",
    )
    console.print()

    if machines:
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
        table.add_column("Status", style="bold green")

        for m in machines:
            raw_mem = getattr(m, "available_gb", None)
            mem = f"{raw_mem:.0f} GB" if raw_mem is not None else "—"
            table.add_row(
                m.machine_name or m.machine_id,
                m.machine_type or "—",
                getattr(m, "dra_grpc_target", None) or "—",
                mem,
                "● Online",
            )
        console.print(table)
        console.print()

    console.print("  [grey69]Commands:[/grey69]")
    console.print(
        "    [bold white]deploy [italic]<image>[/italic][/bold white]"
        "      [grey69]Deploy a container to the best available machine[/grey69]"
    )
    console.print(
        "    [bold white]status[/bold white]"
        "             [grey69]Show cluster status[/grey69]"
    )
    console.print(
        "    [bold white]help[/bold white]"
        "               [grey69]Show all commands[/grey69]"
    )
    console.print(
        "    [bold white]q[/bold white]"
        "                  [grey69]Quit[/grey69]"
    )
    console.print()


def mini_header() -> None:
    console.print()
    console.print(
        "  [bold bright_magenta]ATLAS[/bold bright_magenta]"
        "  [grey50]Distributed Resource Allocator[/grey50]"
    )
    console.print("  [purple]" + "─" * 44 + "[/purple]")
    console.print()
