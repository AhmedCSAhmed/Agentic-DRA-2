"""Agent tools: one tool per ``dra.DRAService`` RPC, plus optional Postgres helpers.

RPC mapping (see ``dra.proto`` / ``dra_pb2_grpc.py``):

- ``pull_and_run_image`` → unary RPC ``PullAndRunImage`` (the only gRPC method).

``list_dra_machines`` reads the local machine registry; it is **not** a gRPC call.

``start_dra_grpc_server`` runs ``python -m dra`` (``dra/serve.py``) in a background process — not an RPC.
"""

from __future__ import annotations

import json
import shlex
from typing import Any

from agents import Tool, function_tool

from dra.repositories.machines import (
    MachineNotFoundError,
    MachineRepository,
    MachineRepositoryDatabaseError,
)

from .client import DRAGrpcClient
from .dra_serve_process import start_dra_grpc_server


def _machine_row(m: Any) -> dict[str, Any]:
    return {
        "machine_id": m.machine_id,
        "machine_name": m.machine_name,
        "machine_type": m.machine_type,
        "dra_grpc_target": getattr(m, "dra_grpc_target", None),
    }


def build_dra_tools(client: DRAGrpcClient, machine_repo: MachineRepository) -> list[Tool]:
    """Build tools: RPC ``PullAndRunImage``, optional DB listing, optional local server start."""

    @function_tool(
        name_override="start_dra_grpc_server",
        description_override=(
            "Start the DRA gRPC server locally by running ``python -m dra`` (``dra/serve.py``): "
            "binds PullAndRunImage/Docker on DRA_GRPC_BIND (default 0.0.0.0:50051). "
            "Not a gRPC call — spawns a subprocess. Use when no DRA server is listening yet."
        ),
    )
    def start_dra_grpc_server_tool(grpc_bind: str | None = None) -> str:
        """Start the DRA gRPC server process (same as CLI ``python -m dra``).

        Args:
            grpc_bind: Optional listen address host:port (sets env DRA_GRPC_BIND); omit for default.
        """
        payload = start_dra_grpc_server(grpc_bind=grpc_bind)
        return json.dumps(payload)

    @function_tool(
        name_override="list_dra_machines",
        description_override=(
            "NOT a gRPC RPC: reads the Postgres ``machines`` table (DATABASE_URL). "
            "Returns machine_id, machine_name, machine_type, dra_grpc_target for choosing a host. "
            "Use before pull_and_run_image when you need registry data."
        ),
    )
    def list_dra_machines(machine_type: str | None = None) -> str:
        """List registered machines and their DRA gRPC endpoints.

        Args:
            machine_type: If set, filter (e.g. gpu, cpu); omit to return all.
        """
        try:
            rows = machine_repo.list_machines(machine_type=machine_type)
        except MachineRepositoryDatabaseError as exc:
            return json.dumps({"error": True, "message": str(exc)})
        payload = {"machines": [_machine_row(m) for m in rows]}
        return json.dumps(payload)

    @function_tool(
        name_override="pull_and_run_image",
        description_override=(
            "gRPC RPC: ``dra.DRAService/PullAndRunImage`` — same as ``dra/grpc_server.py``. "
            "Resolves host:port via machine_id (Postgres dra_grpc_target), grpc_target, or default. "
            "Then invokes Docker pull/run on the remote DRA host. "
            "Use ``restart_policy`` ``unless-stopped`` for long-running services (survives crashes/reboots until docker stop). "
            "Use ``command`` (e.g. ``sleep infinity``) only if the image CMD exits immediately."
        ),
    )
    def pull_and_run_image(
        image_name: str,
        machine_id: str | None = None,
        grpc_target: str | None = None,
        command: str | None = None,
        restart_policy: str | None = None,
    ) -> str:
        """Implements RPC PullAndRunImage (request field image_name).

        Args:
            image_name: Passed to the RPC as ``PullAndRunRequest.image_name``.
            machine_id: If set, load ``dra_grpc_target`` from Postgres for the gRPC channel.
            grpc_target: Optional ``host:port`` when not using machine_id.
            command: Optional ``docker run`` args after the image (shell-style), e.g. ``sleep infinity``.
            restart_policy: Docker restart policy: ``no``, ``on-failure``, ``always``, ``unless-stopped``.
        """
        resolved: str | None = None
        source: str | None = None

        mid = (machine_id or "").strip()
        if mid:
            try:
                machine = machine_repo.find_machine_by_id(mid)
            except MachineNotFoundError as exc:
                return json.dumps(
                    {"error": True, "message": str(exc), "machine_id": mid}
                )
            except MachineRepositoryDatabaseError as exc:
                return json.dumps(
                    {"error": True, "message": str(exc), "machine_id": mid}
                )
            target = getattr(machine, "dra_grpc_target", None)
            if not target or not str(target).strip():
                return json.dumps(
                    {
                        "error": True,
                        "message": "Machine has no dra_grpc_target in the database",
                        "machine_id": mid,
                    }
                )
            resolved = str(target).strip()
            source = "database"
        else:
            gt = (grpc_target or "").strip()
            if gt:
                resolved = gt
                source = "grpc_target"

        cmd = shlex.split((command or "").strip()) if (command or "").strip() else None
        rp = (restart_policy or "").strip() or None

        kwargs: dict[str, Any] = {"grpc_target": resolved}
        if cmd:
            kwargs["command"] = cmd
        if rp:
            kwargs["restart_policy"] = rp

        payload = client.pull_and_run_image(image_name, **kwargs)
        if source:
            payload = {**payload, "connection_source": source}
        if mid:
            payload = {**payload, "machine_id": mid}
        return json.dumps(payload)

    return [start_dra_grpc_server_tool, list_dra_machines, pull_and_run_image]
