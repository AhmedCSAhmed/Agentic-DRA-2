from __future__ import annotations

import typer
from rich.panel import Panel

from cli.display import console, mini_header


def stop(container_id: str = typer.Argument(..., help="Docker container id to stop/remove")) -> None:
    """Stop a deployment by container id and release reserved memory back to the machine.

    Looks up the container in Supabase/Postgres (jobs.image_id == container_id) to find the machine,
    then calls the machine's DRA gRPC server StopContainer RPC.
    """

    mini_header()
    cid = (container_id or "").strip()
    if not cid:
        console.print("  [red]container_id is required[/red]\n")
        raise typer.Exit(2)

    try:
        from agent.env import load_project_dotenv
        from agent.client import DRAGrpcClient
        from dra.database import Database
        from dra.repositories.jobs import JobNotFoundError, JobsRepository
        from dra.repositories.machines import MachineNotFoundError, MachineRepository

        load_project_dotenv()
        db = Database()
        jobs = JobsRepository(db)
        machines = MachineRepository(db)

        job = jobs.find_job_by_image_id(cid)

        rr = getattr(job, "resource_requirements", None)
        machine_id: str | None = None
        if isinstance(rr, dict):
            raw_mid = rr.get("machine_id")
            if isinstance(raw_mid, str) and raw_mid.strip():
                machine_id = raw_mid.strip()

        if not machine_id:
            console.print(
                Panel(
                    f"[bold red]✗  Stop failed[/bold red]\n\n"
                    f"[grey69]Job {job.id} for container '{cid}' has no machine_id in resource_requirements.[/grey69]",
                    border_style="red",
                    padding=(1, 2),
                )
            )
            raise typer.Exit(1)

        machine = machines.find_machine_by_id(machine_id)
        grpc_target = (getattr(machine, "dra_grpc_target", None) or "").strip()
        if not grpc_target:
            console.print(
                Panel(
                    f"[bold red]✗  Stop failed[/bold red]\n\n"
                    f"[grey69]Machine '{machine_id}' has no dra_grpc_target in DB.[/grey69]",
                    border_style="red",
                    padding=(1, 2),
                )
            )
            raise typer.Exit(1)

        console.print(f"  Stopping [bold white]{cid}[/bold white] on [grey69]{grpc_target}[/grey69] ...\n")
        client = DRAGrpcClient()
        try:
            result = client.stop_container(cid, grpc_target=grpc_target)
        finally:
            client.close()

        if result.get("rpc_error"):
            console.print(
                Panel(
                    f"[bold red]✗  Stop failed[/bold red]\n\n"
                    f"[grey69]gRPC error to {grpc_target}: {result.get('details','')} ({result.get('code','')})[/grey69]",
                    border_style="red",
                    padding=(1, 2),
                )
            )
            raise typer.Exit(1)

        if not result.get("success"):
            console.print(
                Panel(
                    f"[bold red]✗  Stop failed[/bold red]\n\n"
                    f"[grey69]{result.get('message','unknown error')}[/grey69]",
                    border_style="red",
                    padding=(1, 2),
                )
            )
            raise typer.Exit(1)

        released = float(result.get("memory_gb_released") or 0.0)
        console.print(
            Panel(
                f"[bold green]✓  Stopped[/bold green]  [white]{cid}[/white]\n\n"
                f"[grey69]- Machine: {machine_id} → {grpc_target}\n"
                f"- Memory released: {released:.2f} GB\n"
                f"- {result.get('message','')}[/grey69]",
                border_style="purple",
                padding=(1, 2),
            )
        )
        console.print()

    except JobNotFoundError:
        console.print(
            Panel(
                f"[bold red]✗  Stop failed[/bold red]\n\n"
                f"[grey69]No job found for container_id '{cid}'.[/grey69]",
                border_style="red",
                padding=(1, 2),
            )
        )
        raise typer.Exit(1)
    except MachineNotFoundError as exc:
        console.print(
            Panel(
                f"[bold red]✗  Stop failed[/bold red]\n\n[grey69]{exc}[/grey69]",
                border_style="red",
                padding=(1, 2),
            )
        )
        raise typer.Exit(1)
