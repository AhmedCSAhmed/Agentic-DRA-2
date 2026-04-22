from __future__ import annotations

from rich.panel import Panel

from cli.commands.deploy import deploy_via_scheduler_sync, parse_deploy_repl_arg
from cli.commands.stop import stop as stop_command
from cli.display import admin_boot_screen, boot_screen, console


def _run_deploy(arg: str) -> None:
    image, memory_gb, machine_type, command, restart_policy = parse_deploy_repl_arg(arg)
    if not image:
        console.print(
            "\n  [red]Usage:[/red] deploy [italic]<image>[/italic] "
            "[grey69][--memory-gb N] [--machine-type T] [--command \"...\"] [--restart-policy unless-stopped][/grey69]\n"
        )
        return

    console.print(f"\n  Deploying [bold white]{image}[/bold white] (scheduler) ...\n")
    try:
        with console.status(
            "[purple]Agent selecting machine from registry and pulling on remote host...[/purple]",
            spinner="dots",
            spinner_style="bright_magenta",
        ):
            ok, output = deploy_via_scheduler_sync(
                image,
                memory_gb=memory_gb,
                machine_type=machine_type,
                command=command,
                restart_policy=restart_policy,
            )

        if ok:
            console.print(
                Panel(
                    f"[bold green]✓  Deployed[/bold green]  [white]{image}[/white]\n\n"
                    f"[grey69]{output}[/grey69]",
                    border_style="purple",
                    padding=(1, 2),
                )
            )
        else:
            console.print(
                Panel(
                    f"[bold red]✗  Deployment failed[/bold red]  [white]{image}[/white]\n\n"
                    f"[grey69]{output}[/grey69]",
                    border_style="red",
                    padding=(1, 2),
                )
            )
    except Exception as exc:
        console.print(
            Panel(
                f"[bold red]✗  Deployment failed[/bold red]\n\n[grey69]{exc}[/grey69]",
                border_style="red",
                padding=(1, 2),
            )
        )


def _run_status() -> None:
    try:
        from agent.env import load_project_dotenv
        from dra.database import Database
        from dra.repositories.machines import MachineRepository
        from rich import box
        from rich.table import Table

        load_project_dotenv()
        repo = MachineRepository(Database())
        machines = repo.list_machines()

        if not machines:
            console.print("\n  [grey69]No machines registered in the cluster.[/grey69]\n")
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

        console.print()
        console.print(table)

    except Exception as exc:
        console.print(f"\n  [red]Could not reach cluster:[/red] [grey69]{exc}[/grey69]\n")


def _show_help(admin: bool = False) -> None:
    console.print()
    console.print("  [grey69]Available commands:[/grey69]")
    console.print(
        "    [bold white]deploy [italic]<image>[/italic][/bold white]   "
        "[grey69][--memory-gb N] [--machine-type T] [--command \"...\"] [--restart-policy unless-stopped][/grey69]"
    )
    console.print("    [bold white]stop [italic]<container_id>[/italic][/bold white]    Stop a deployment and release reserved memory")
    console.print("    [bold white]off [italic]<container_id>[/italic][/bold white]     Alias for stop")
    console.print("    [bold white]status[/bold white]          Show machines")
    console.print("    [bold white]help[/bold white]             Show this message")
    console.print("    [bold white]q[/bold white]                Quit")
    console.print()


def run_repl(admin: bool = False) -> None:
    if admin:
        admin_boot_screen()
    else:
        boot_screen()

    if not admin:
        console.print("  [grey69]Type [white]help[/white] for commands or [white]q[/white] to quit.[/grey69]\n")
    else:
        console.print("  [grey69]Logged in as admin. Type [white]help[/white] for commands or [white]q[/white] to quit.[/grey69]\n")

    while True:
        try:
            raw = input("\n  \033[35matlas ❯\033[0m ").strip()
        except (KeyboardInterrupt, EOFError):
            console.print("\n\n  [grey69]Goodbye.[/grey69]\n")
            break

        if not raw:
            continue

        if raw.lower() in ("q", "quit", "exit"):
            console.print("\n  [grey69]Goodbye.[/grey69]\n")
            break

        parts = raw.split(maxsplit=1)
        cmd = parts[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""

        if cmd == "deploy":
            if not arg:
                console.print(
                    "\n  [red]Usage:[/red] deploy [italic]<image>[/italic] "
                    "[grey69][--memory-gb N] [--machine-type T] ...[/grey69]\n"
                )
            else:
                _run_deploy(arg)

        elif cmd in ("stop", "off"):
            if not arg:
                console.print("\n  [red]Usage:[/red] stop [italic]<container_id>[/italic]\n")
            else:
                stop_command(arg)

        elif cmd == "status":
            _run_status()

        elif cmd in ("help", "?", "--help", "-h"):
            _show_help(admin=admin)

        else:
            console.print(
                f"\n  [red]Invalid command:[/red] [white]{raw}[/white]  "
                f"[grey69](type [white]help[/white] or [white]q[/white] to quit)[/grey69]\n"
            )
