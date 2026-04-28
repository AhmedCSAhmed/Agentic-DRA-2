from __future__ import annotations

import asyncio
import getpass
import shlex
from typing import Optional

import typer
from rich.panel import Panel

from cli.display import console, mini_header


def deploy(
    image: str = typer.Argument(..., help="Full Docker image name from your registry"),
    memory_gb: float = typer.Option(
        2.0,
        "--memory-gb",
        help="Minimum free memory (GB) required on the chosen machine (scheduler constraint).",
    ),
    cpu_cores: Optional[float] = typer.Option(
        None,
        "--cpu-cores",
        help="Minimum free CPU cores required on the chosen machine (scheduler constraint).",
    ),
    machine_type: Optional[str] = typer.Option(
        None,
        "--machine-type",
        help="Only consider machines of this type (matches DB ``machine_type``).",
    ),
    command: Optional[str] = typer.Option(
        None,
        "--command",
        help="Extra args after the image for ``docker run`` (shell-style), e.g. ``sleep infinity``.",
    ),
    restart_policy: Optional[str] = typer.Option(
        None,
        "--restart-policy",
        help="Docker restart policy, e.g. ``unless-stopped``.",
    ),
    username: Optional[str] = typer.Option(
        None,
        "--username",
        "-u",
        help="Owner username for this deployment (defaults to your local OS user).",
    ),
    password: Optional[str] = typer.Option(
        None,
        "--password",
        help="Owner password to create/update username credentials.",
        hide_input=True,
    ),
) -> None:
    """Deploy a Docker image using the same Postgres-backed scheduler as the HTTP API."""

    mini_header()
    console.print(f"  Deploying [bold white]{image}[/bold white] (scheduler) ...\n")

    try:
        with console.status(
            "[purple]Selecting machine from registry and pulling on remote host...[/purple]",
            spinner="dots",
            spinner_style="bright_magenta",
        ):
            ok, text = asyncio.run(
                _deploy_via_scheduler(
                    image,
                    memory_gb=memory_gb,
                    cpu_cores=cpu_cores,
                    machine_type=machine_type,
                    command=command,
                    restart_policy=restart_policy,
                    username=username,
                    password=password,
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
        raise typer.Exit(1)

    if not ok:
        console.print(
            Panel(
                f"[bold red]✗  Deployment failed[/bold red]  [white]{image}[/white]\n\n[grey69]{text}[/grey69]",
                border_style="red",
                padding=(1, 2),
            )
        )
        raise typer.Exit(1)

    console.print(
        Panel(
            f"[bold green]✓  Deployed[/bold green]  [white]{image}[/white]\n\n[grey69]{text}[/grey69]",
            border_style="purple",
            padding=(1, 2),
        )
    )
    console.print()


async def _deploy_via_scheduler(
    image: str,
    *,
    memory_gb: float,
    cpu_cores: float | None,
    machine_type: str | None,
    command: str | None,
    restart_policy: str | None,
    username: str | None,
    password: str | None,
) -> tuple[bool, str]:
    from routes.contracts import ResourceRequirements
    from scheduled_deploy import execute_scheduled_deploy

    resolved_username = (username or "").strip() or None
    if resolved_username is None:
        try:
            resolved_username = getpass.getuser().strip() or None
        except Exception:
            resolved_username = None

    decision, rpc_result = await execute_scheduled_deploy(
        image_name=image,
        resource_requirements=ResourceRequirements(memory_gb=memory_gb, cpu_cores=cpu_cores),
        machine_type=machine_type,
        command=command,
        restart_policy=restart_policy,
        username=resolved_username,
        password=password,
    )

    if decision.selected is None:
        reasons = decision.reject_reasons or {}
        return (
            False,
            (
                "No machine satisfied the request.\n"
                f"- Scanned: {decision.scanned}, eligible: {decision.eligible}\n"
                f"- Reject counts: {reasons}\n"
                "Lower --memory-gb / --cpu-cores or fix machine resources in Postgres."
            ),
        )

    assert rpc_result is not None
    sel = decision.selected

    if rpc_result.get("error"):
        return (
            False,
            (
                f"Scheduler chose {sel.machine_id} "
                f"({sel.grpc_target}) but the deploy tool failed: "
                f"{rpc_result.get('message', 'unknown error')}"
            ),
        )

    if rpc_result.get("rpc_error"):
        return (
            False,
            (
                f"gRPC error to {sel.grpc_target}: {rpc_result.get('details', '')} "
                f"({rpc_result.get('code', '')})"
            ),
        )

    if not rpc_result.get("success"):
        return (
            False,
            f"Remote reported failure: {rpc_result.get('message', 'unknown')}",
        )

    cid = rpc_result.get("container_id", "")
    state = rpc_result.get("workload_state", "RUNNING")
    msg = rpc_result.get("message", "")
    cpu = rpc_result.get("cpu_used", 0.0)
    mem = rpc_result.get("memory_gb_used", 0.0)
    cores_reserved = cpu_cores
    attempts = rpc_result.get("attempts") or []
    failover_note = ""
    if len(attempts) > 1:
        steps: list[str] = []
        for attempt in attempts:
            status = attempt.get("status", "failed")
            detail = attempt.get("code") or attempt.get("message") or ""
            suffix = f" ({detail})" if detail else ""
            steps.append(
                f"{attempt.get('machine_id')} → {attempt.get('grpc_target')} [{status}{suffix}]"
            )
        failover_note = "\n- Attempts: " + "; ".join(steps)
    cores_line = f"\n- CPU cores reserved: {cores_reserved}" if cores_reserved else ""
    return (
        True,
        (
            f"- Machine: {sel.machine_id} ({sel.machine_type}) → {sel.grpc_target}\n"
            f"- Owner: {resolved_username or '—'}\n"
            f"- Container ID: {cid}\n"
            f"- Workload: {state}\n"
            f"- Memory reserved: {memory_gb:.1f} GB{cores_line}\n"
            f"- CPU / memory used (reported): {cpu:.1f}% , {mem:.2f} GB\n"
            f"- {msg}"
            f"{failover_note}"
        ),
    )


def deploy_via_scheduler_sync(
    image: str,
    *,
    memory_gb: float = 2.0,
    cpu_cores: float | None = None,
    machine_type: str | None = None,
    command: str | None = None,
    restart_policy: str | None = None,
    username: str | None = None,
    password: str | None = None,
) -> tuple[bool, str]:
    """Used by the interactive REPL; returns ``(ok, message)``."""

    return asyncio.run(
        _deploy_via_scheduler(
            image,
            memory_gb=memory_gb,
            cpu_cores=cpu_cores,
            machine_type=machine_type,
            command=command,
            restart_policy=restart_policy,
            username=username,
            password=password,
        )
    )


def parse_deploy_repl_arg(
    arg: str,
) -> tuple[
    str,
    float,
    float | None,
    str | None,
    str | None,
    str | None,
    str | None,
    str | None,
]:
    """``deploy <image>`` flags including ``--username`` and optional ``--password``."""

    parts = shlex.split(arg)
    if not parts:
        return "", 2.0, None, None, None, None, None, None
    image = parts[0]
    memory_gb = 2.0
    cpu_cores: float | None = None
    machine_type: str | None = None
    command: str | None = None
    restart_policy: str | None = None
    username: str | None = None
    password: str | None = None
    i = 1
    while i < len(parts):
        token = parts[i]

        if token.startswith("--memory-gb=") or token.startswith("--memory="):
            _, value = token.split("=", 1)
            memory_gb = float(value)
            i += 1
            continue

        if token in ("--memory-gb", "--memory"):
            next_is_value = i + 1 < len(parts) and not parts[i + 1].startswith("--")
            if next_is_value:
                memory_gb = float(parts[i + 1])
                i += 2
                continue
            from cli.display import console
            entered = console.input("  Minimum free memory (GB) [default 2]: ").strip()
            memory_gb = float(entered) if entered else 2.0
            i += 1
            continue

        if token.startswith("--cpu-cores="):
            _, value = token.split("=", 1)
            cpu_cores = float(value)
            i += 1
            continue

        if token == "--cpu-cores" and i + 1 < len(parts):
            cpu_cores = float(parts[i + 1])
            i += 2
            continue

        if token == "--machine-type" and i + 1 < len(parts):
            machine_type = parts[i + 1]
            i += 2
            continue
        if token == "--command" and i + 1 < len(parts):
            command = parts[i + 1]
            i += 2
            continue
        if token == "--restart-policy" and i + 1 < len(parts):
            restart_policy = parts[i + 1]
            i += 2
            continue

        if token in ("--username", "-u") and i + 1 < len(parts):
            username = parts[i + 1]
            i += 2
            continue

        if token == "--password" and i + 1 < len(parts):
            password = parts[i + 1]
            i += 2
            continue
        i += 1
    return image, memory_gb, cpu_cores, machine_type, command, restart_policy, username, password
