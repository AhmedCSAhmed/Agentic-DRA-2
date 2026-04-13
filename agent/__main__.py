"""Run the DRA toolchain agent once.

`OPENAI_API_KEY` is read from the process environment. `create_dra_agent` loads the repo-root
`.env` (see `agent/env.py`) so you can keep the key in `.env` without exporting it manually.

Start the DRA gRPC server (Docker pull/run) on the target machine first::

    python -m dra

Configure ``DATABASE_URL`` in ``.env`` for machine registry. The gRPC tool is ``pull_and_run_image``
(RPC ``PullAndRunImage``). Optional: ``DRA_GRPC_TARGET``, ``DEBUG_AGENT=1``.

Run with a **Docker image name** as the argument (passed through as the user message)::

    python -m agent nginx:latest
    python -m agent library/alpine:3.19
"""

from __future__ import annotations

import asyncio
import os
import sys

from agents import enable_verbose_stdout_logging

from .inspect_run import tool_call_details_from_result, tool_call_names_from_result
from .run import run_dra_agent


async def _main() -> None:
    if os.environ.get("DEBUG_AGENT"):
        enable_verbose_stdout_logging()

    # Primary input: Docker image name (e.g. nginx:latest). Join argv in case of names with spaces.
    user_text = " ".join(sys.argv[1:]).strip()
    if not user_text:
        user_text = "nginx:latest"

    result = await run_dra_agent(user_text)

    if os.environ.get("DEBUG_AGENT"):
        names = tool_call_names_from_result(result)
        print("[DEBUG_AGENT] tool calls:", names, file=sys.stderr)
        for row in tool_call_details_from_result(result):
            print("[DEBUG_AGENT] tool:", row, file=sys.stderr)

    print(result.final_output)


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
