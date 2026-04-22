"""Start the DRA gRPC server (`DRAServiceServicer` from `dra/grpc_server.py`).

Run from the repository root::

    python -m dra
    python -m dra --machine-name worker-east-01

When ``--machine-name`` (or env ``DRA_MACHINE_NAME``) is set, the process loads that row from
Postgres (``machines.machine_name``) and logs ``machine_id``, ``machine_name``, and
``dra_grpc_target`` so this host is tied to the registry record.

Environment:

- ``DATABASE_URL``: Postgres (see ``dra/database.py``).
- ``DRA_GRPC_BIND``: listen address (default ``0.0.0.0:50051``).
- ``DRA_GRPC_MAX_WORKERS``: thread pool size (default ``10``).
- ``DRA_MACHINE_NAME``: same as ``--machine-name`` if CLI is omitted.

The agent connects with ``DRA_GRPC_TARGET`` or per-tool ``grpc_target``.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import ipaddress
import logging
import os
import sys

import grpc

import dra_pb2_grpc

from dra.grpc_server import DRAServiceServicer
from dra.models import MachineModelORM

logger = logging.getLogger(__name__)


def _default_bind() -> str:
    return "0.0.0.0:50051"


def _env_bind() -> str | None:
    raw = (os.environ.get("DRA_GRPC_BIND") or "").strip()
    return raw or None


def _max_workers() -> int:
    return int(os.environ.get("DRA_GRPC_MAX_WORKERS", "10"))


def _resolve_machine_name(cli_machine_name: str | None) -> str | None:
    name = (cli_machine_name or "").strip()
    if name:
        return name
    env = (os.environ.get("DRA_MACHINE_NAME") or "").strip()
    return env or None


def load_machine_from_database(machine_name: str) -> MachineModelORM:
    """Load the ``machines`` row for this ``machine_name`` (must match exactly after strip)."""

    from dra.database import Database
    from dra.repositories.machines import (
        MachineNotFoundError,
        MachineRepository,
        MachineRepositoryDatabaseError,
    )

    repo = MachineRepository(Database())
    try:
        return repo.find_machine_by_name(machine_name)
    except MachineNotFoundError:
        logger.error(
            "No machine row for machine_name=%r — check Postgres ``machines`` table",
            machine_name,
        )
        raise SystemExit(1) from None
    except MachineRepositoryDatabaseError as exc:
        logger.error("Database error while loading machine_name=%r: %s", machine_name, exc)
        raise SystemExit(1) from exc


def _split_host_port(target: str) -> tuple[str, int]:
    text = (target or "").strip()
    if not text:
        raise ValueError("Empty gRPC target")
    if text.count(":") != 1:
        raise ValueError(f"Invalid gRPC target {target!r}")
    host, port_text = text.rsplit(":", 1)
    host = host.strip()
    if not host or not port_text.isdigit():
        raise ValueError(f"Invalid gRPC target {target!r}")
    port = int(port_text)
    if not 1 <= port <= 65535:
        raise ValueError(f"Invalid gRPC port {port} in target {target!r}")
    return host, port


def _is_loopback_host(host: str) -> bool:
    lowered = host.strip().lower()
    if lowered == "localhost":
        return True
    try:
        return ipaddress.ip_address(lowered).is_loopback
    except ValueError:
        return False


def _bind_from_machine(machine: MachineModelORM) -> str | None:
    target = (getattr(machine, "dra_grpc_target", None) or "").strip()
    if not target:
        return None

    host, port = _split_host_port(target)
    if _is_loopback_host(host):
        return f"{host}:{port}"
    return f"0.0.0.0:{port}"


def _resolve_bind(bind: str | None, machine: MachineModelORM | None) -> str:
    explicit = (bind or "").strip()
    if explicit:
        return explicit

    if machine is not None:
        derived = _bind_from_machine(machine)
        if derived:
            return derived
        logger.warning(
            "Machine %r has no dra_grpc_target in Postgres; falling back to %s",
            machine.machine_name,
            _default_bind(),
        )

    env_bind = _env_bind()
    if env_bind:
        return env_bind

    return _default_bind()


def serve(*, bind: str | None = None, machine_name: str | None = None) -> None:
    """Listen for gRPC until interrupted.

    If ``machine_name`` is set, looks up the machine in the database and logs registry fields.
    """

    resolved_name = _resolve_machine_name(machine_name)
    machine: MachineModelORM | None = None

    if resolved_name:
        machine = load_machine_from_database(resolved_name)
        logger.info(
            "Registry match: machine_name=%r machine_id=%r machine_type=%r dra_grpc_target=%r",
            machine.machine_name,
            machine.machine_id,
            machine.machine_type,
            getattr(machine, "dra_grpc_target", None),
        )

    address = _resolve_bind(bind, machine)
    if machine is not None and not (bind or _env_bind()):
        logger.info(
            "Derived DRA gRPC bind %s from machine_name=%r / dra_grpc_target=%r",
            address,
            machine.machine_name,
            getattr(machine, "dra_grpc_target", None),
        )

    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=_max_workers()))
    dra_pb2_grpc.add_DRAServiceServicer_to_server(
        DRAServiceServicer(machine_id=(machine.machine_id if machine is not None else None)),
        server,
    )
    server.add_insecure_port(address)
    server.start()
    logger.info("DRA gRPC listening on %s (PullAndRunImage -> Docker pull/run)", address)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down DRA gRPC server")
        server.stop(grace=5).wait()
        raise SystemExit(0) from None


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    parser = argparse.ArgumentParser(description="DRA gRPC server (PullAndRunImage)")
    parser.add_argument(
        "--machine-name",
        metavar="NAME",
        default=None,
        help="machines.machine_name in Postgres: load machine_id, machine_name, dra_grpc_target at startup",
    )
    args = parser.parse_args()
    serve(machine_name=args.machine_name)


if __name__ == "__main__":
    main()
