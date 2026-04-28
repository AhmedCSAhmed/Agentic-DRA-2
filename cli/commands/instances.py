from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any

import typer
from rich import box
from rich.table import Table

from cli.display import console, mini_header


def instances(
    username: str | None = typer.Argument(
        None,
        help="Owner username to filter by.",
    ),
    username_option: str | None = typer.Option(
        None,
        "--username",
        "-u",
        help="Filter running deployments by owner username.",
    ),
) -> None:
    """Show running deployed instances; username filters require password verification."""

    mini_header()
    normalized_username = _normalize_username(username_option, username)

    try:
        from agent.env import load_project_dotenv
        from dra.database import Database
        from dra.repositories.jobs import JobsRepository
        from dra.repositories.machines import MachineRepository
        from dra.repositories.users import UsersRepository

        load_project_dotenv()
        db = Database()
        jobs_repo = JobsRepository(db)
        machine_repo = MachineRepository(db)

        if normalized_username is not None:
            entered_password = typer.prompt("\n  Password", hide_input=True).strip()
            users_repo = UsersRepository(db)
            ok, user = users_repo.verify_password(
                username=normalized_username,
                password=entered_password,
            )
            if user is None:
                console.print(
                    f"  [red]No credential found for username '{normalized_username}'.[/red]"
                    " [grey69]Deploy with --username and --password first.[/grey69]\n"
                )
                raise typer.Exit(1)
            if not ok:
                console.print("  [red]Invalid password for requested username.[/red]\n")
                raise typer.Exit(1)

        running = jobs_repo.list_running_jobs(username=normalized_username)
        machine_map = {m.machine_id: m for m in machine_repo.list_machines()}
    except typer.Exit:
        raise
    except Exception as exc:
        console.print(f"  [red]Could not load deployments:[/red] [grey69]{exc}[/grey69]\n")
        raise typer.Exit(1)

    if not running:
        if normalized_username:
            console.print(
                f"  [grey69]No running deployments found for username '{normalized_username}'.[/grey69]\n"
            )
        else:
            console.print("  [grey69]No running deployments found.[/grey69]\n")
        return

    table = Table(
        box=box.SIMPLE,
        border_style="purple",
        header_style="medium_purple1",
        show_header=True,
        padding=(0, 2),
    )
    table.add_column("Container", style="white")
    table.add_column("Image", style="grey69")
    table.add_column("User", style="grey69")
    table.add_column("Machine", style="grey69")
    table.add_column("Started", style="grey69")
    table.add_column("Status", style="white")

    for job in running:
        rr = _resource_requirements_obj(getattr(job, "resource_requirements", None))
        machine_id = _coerce_str(rr.get("machine_id")) or "—"
        owner = _coerce_str(getattr(job, "username", None)) or _coerce_str(rr.get("username")) or "—"
        machine = machine_map.get(machine_id)
        machine_cell = machine.machine_name if machine is not None else machine_id
        table.add_row(
            getattr(job, "image_id", "") or "—",
            getattr(job, "image_name", "") or "—",
            owner,
            machine_cell,
            _time_ago(getattr(job, "created_at", None)),
            f"[bold green]{getattr(job, 'status', 'RUNNING')}[/bold green]",
        )

    console.print(table)
    console.print()


def _resource_requirements_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _coerce_str(value: Any) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    return None


def _normalize_username(*values: Any) -> str | None:
    for value in values:
        coerced = _coerce_str(value)
        if coerced:
            return coerced
    return None


def _time_ago(value: Any) -> str:
    if not isinstance(value, datetime):
        return "—"
    timestamp = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    age_s = int((datetime.now(timezone.utc) - timestamp).total_seconds())
    if age_s < 60:
        return f"{age_s}s ago"
    if age_s < 3600:
        return f"{age_s // 60}m ago"
    if age_s < 86400:
        return f"{age_s // 3600}h ago"
    return f"{age_s // 86400}d ago"
