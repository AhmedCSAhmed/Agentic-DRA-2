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
import logging
import os
import sys

import grpc

import dra_pb2_grpc

from dra.grpc_server import DRAServiceServicer
from dra.models import MachineModelORM

logger = logging.getLogger(__name__)


def _default_bind() -> str:
    return os.environ.get("DRA_GRPC_BIND", "0.0.0.0:50051")


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


def serve(*, bind: str | None = None, machine_name: str | None = None) -> None:
    """Listen for gRPC until interrupted.

    If ``machine_name`` is set, looks up the machine in the database and logs registry fields.
    """

    address = bind or _default_bind()
    resolved_name = _resolve_machine_name(machine_name)

    if resolved_name:
        machine = load_machine_from_database(resolved_name)
        logger.info(
            "Registry match: machine_name=%r machine_id=%r machine_type=%r dra_grpc_target=%r",
            machine.machine_name,
            machine.machine_id,
            machine.machine_type,
            getattr(machine, "dra_grpc_target", None),
        )

    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=_max_workers()))
    dra_pb2_grpc.add_DRAServiceServicer_to_server(DRAServiceServicer(), server)
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
