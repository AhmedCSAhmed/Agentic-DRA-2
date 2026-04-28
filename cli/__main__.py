from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

def _discover_project_root() -> Path:
    """Best-effort repo root discovery for both editable and installed CLI.

    When `atlas` is installed into a venv, `__file__` points into site-packages.
    In that case we want to treat the *current working directory* (or its parents)
    as the project root when the user runs `atlas` from the repo.
    """

    def looks_like_repo_root(p: Path) -> bool:
        return (p / "pyproject.toml").exists() and (p / "cli").is_dir()

    cwd = Path.cwd().resolve()
    for p in (cwd, *cwd.parents):
        if looks_like_repo_root(p):
            return p

    # Fallback: adjacent to this file (works for editable/dev runs).
    return Path(__file__).resolve().parent.parent


# Ensure the project root is on the path regardless of where atlas is invoked from.
_PROJECT_ROOT = _discover_project_root()
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import typer

from cli.commands.deploy import deploy as atlas_deploy_command
from cli.commands.instances import instances as atlas_instances_command
from cli.commands.status import status as atlas_status_command
from cli.commands.stop import stop as atlas_stop_command
from cli.display import console
from cli.repl import run_repl

app = typer.Typer(
    help="Atlas — Distributed Resource Allocator",
    add_completion=False,
    no_args_is_help=False,
    pretty_exceptions_enable=False,
)


def _check_passcode() -> bool:
    from dotenv import load_dotenv
    load_dotenv(_PROJECT_ROOT / ".env", override=False)

    expected = os.environ.get("ATLAS_ADMIN_PASSCODE", "").strip()
    if not expected:
        console.print("\n  [red]ATLAS_ADMIN_PASSCODE is not set in your .env file.[/red]\n")
        return False

    entered = typer.prompt("\n  Admin passcode", hide_input=False)
    return entered.strip() == expected


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    admin: Optional[bool] = typer.Option(None, "--admin", help="Boot in admin mode", is_flag=True),
) -> None:
    """Boot Atlas."""
    if ctx.invoked_subcommand is not None:
        return

    if admin:
        if _check_passcode():
            console.print("\n  [bold green]✓ Access granted[/bold green]")
            run_repl(admin=True)
        else:
            console.print("\n  [bold red]✗ Incorrect passcode[/bold red]\n")
            raise typer.Exit(1)
    else:
        run_repl(admin=False)


app.command("deploy")(atlas_deploy_command)
app.command("instances")(atlas_instances_command)
app.command("status")(atlas_status_command)
app.command("stop")(atlas_stop_command)


if __name__ == "__main__":
    app()
