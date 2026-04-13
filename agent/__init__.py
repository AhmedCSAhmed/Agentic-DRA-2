"""Agents SDK integration for DRA gRPC (`dra/grpc_server.py`).

- **Tool** / **FunctionTool**: `build_dra_tools` (`agent/tools.py`).
- **Agent**: `create_dra_agent` (`agent/dra_agent.py`).
- **Runner**: `run_dra_agent` wraps `agents.Runner.run` (`agent/run.py`); import `Runner` from
  `agents` or `agent` for custom loops.
"""

from agents import Runner

from .client import DEFAULT_TARGET, DRAGrpcClient
from .dra_agent import create_dra_agent
from .env import load_project_dotenv
from .inspect_run import tool_call_details_from_result, tool_call_names_from_result
from .run import run_dra_agent
from .tools import build_dra_tools

__all__ = [
    "DEFAULT_TARGET",
    "DRAGrpcClient",
    "Runner",
    "load_project_dotenv",
    "build_dra_tools",
    "create_dra_agent",
    "run_dra_agent",
    "tool_call_details_from_result",
    "tool_call_names_from_result",
]
