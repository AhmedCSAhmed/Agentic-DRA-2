"""Execution loop: `Runner.run` + lifecycle for the gRPC client.

`Agent.tools` (see `dra_agent.py`) holds `Tool` instances produced in `tools.py` via
`@function_tool` (`FunctionTool` subclasses). This module is where those tools are actually
invoked—by the OpenAI Agents SDK inside `Runner.run`.
"""

from __future__ import annotations

from typing import Any

from agents import Runner, RunResult
from agents.items import TResponseInputItem

from .dra_agent import create_dra_agent


async def run_dra_agent(
    user_input: str | list[TResponseInputItem],
    *,
    grpc_target: str | None = None,
    instructions: str | None = None,
    **runner_kwargs: Any,
) -> RunResult:
    """Run the DRA toolchain agent to completion.

    - **Agent**: built by `create_dra_agent` (instructions, `tools` from `build_dra_tools`).
    - **Runner**: `agents.Runner.run` drives the model and tool calls until a final output.
    - **gRPC**: `DRAGrpcClient` is closed in `finally` after the run. The model may pass
      ``grpc_target`` per tool call to hit different machines; ``grpc_target`` here only sets
      the **default** when the model omits it (same as env ``DRA_GRPC_TARGET``).

    After a run, use `tool_call_names_from_result` / `tool_call_details_from_result` in
    `agent/inspect_run.py` to confirm tool calls (`start_dra_grpc_server`, `pull_and_run_image`, etc.).
    """

    agent, client = create_dra_agent(grpc_target=grpc_target, instructions=instructions)
    try:
        return await Runner.run(agent, user_input, **runner_kwargs)
    finally:
        client.close()
