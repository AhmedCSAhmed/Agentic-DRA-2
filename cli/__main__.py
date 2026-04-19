from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

# Ensure the project root is on the path regardless of where atlas is invoked from
_PROJECT_ROOT = Path(__file__).parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import typer

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
    load_dotenv(_PROJECT_ROOT / ".env")

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


if __name__ == "__main__":
    app()
