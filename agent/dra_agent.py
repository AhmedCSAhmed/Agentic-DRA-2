"""Agent factory: `Agent` + gRPC client + machine registry (Postgres).

- **Tools**: `build_dra_tools` → RPC tool ``pull_and_run_image`` (``PullAndRunImage``), optional DB list.
- **Run loop**: use `run_dra_agent` in `agent/run.py` or call `Runner.run` yourself.
"""

from __future__ import annotations

from agents import Agent

from dra.database import Database
from dra.repositories.machines import MachineRepository

from .client import DRAGrpcClient
from .env import load_project_dotenv
from .tools import build_dra_tools


def create_dra_agent(
    *,
    grpc_target: str | None = None,
    database: Database | None = None,
    instructions: str | None = None,
) -> tuple[Agent[None], DRAGrpcClient]:
    """Build an `Agent` whose tools query Postgres for machines then call DRA gRPC."""

    load_project_dotenv()
    db = database or Database()
    machine_repo = MachineRepository(db)
    client = DRAGrpcClient(grpc_target)
    tools = build_dra_tools(client, machine_repo)
    agent = Agent(
        name="DRA toolchain",
        instructions=instructions
        or (
            "The user's message is the Docker image name to deploy (e.g. nginx:latest, alpine:3.19). "
            "Always call list_dra_machines first, then pull_and_run_image with image_name and "
            "machine_id (or grpc_target from the registry) so Docker runs on the cluster host — "
            "never 127.0.0.1 unless that row is the only machine. "
            "Do not call start_dra_grpc_server unless the user explicitly asks to run DRA on this machine; "
            "it starts a local gRPC server and is wrong for normal cluster deploys. "
            "Summarize tool JSON clearly."
        ),
        tools=tools,
    )
    return agent, client
