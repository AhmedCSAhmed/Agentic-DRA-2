"""Start the DRA gRPC server process (``dra/serve.py`` via ``python -m dra``)."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path


def repo_root() -> Path:
    """Repository root (parent of the ``agent/`` package)."""

    return Path(__file__).resolve().parent.parent


def start_dra_grpc_server(
    *,
    grpc_bind: str | None = None,
    machine_name: str | None = None,
) -> dict[str, object]:
    """Spawn ``python -m dra`` in the background — equivalent to running ``dra/serve.py`` main.

    Uses the same env vars as the CLI: ``DRA_GRPC_BIND``, ``DRA_GRPC_MAX_WORKERS``,
    ``DRA_MACHINE_NAME`` (optional; also pass ``machine_name`` to append ``--machine-name``).

    Returns a dict suitable for JSON serialization (includes pid on success).
    """

    env = os.environ.copy()
    if grpc_bind is not None and str(grpc_bind).strip():
        env["DRA_GRPC_BIND"] = str(grpc_bind).strip()
    if machine_name is not None and str(machine_name).strip():
        env["DRA_MACHINE_NAME"] = str(machine_name).strip()

    cmd: list[str] = [sys.executable, "-m", "dra"]
    if machine_name is not None and str(machine_name).strip():
        cmd.extend(["--machine-name", str(machine_name).strip()])

    proc = subprocess.Popen(
        cmd,
        cwd=str(repo_root()),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    time.sleep(0.4)
    if proc.poll() is not None:
        err = (proc.stderr.read() if proc.stderr else b"").decode(errors="replace")
        return {
            "error": True,
            "message": "DRA gRPC server exited immediately (port in use, bind error, or import failure).",
            "stderr": err.strip()[:4000],
        }

    bind = env.get("DRA_GRPC_BIND", "0.0.0.0:50051")
    out: dict[str, object] = {
        "started": True,
        "pid": proc.pid,
        "entry": "python -m dra (dra/serve.py)",
        "DRA_GRPC_BIND": bind,
        "note": "Use pull_and_run_image with grpc_target like 127.0.0.1:50051 when bind is 0.0.0.0:50051",
    }
    if env.get("DRA_MACHINE_NAME"):
        out["DRA_MACHINE_NAME"] = env["DRA_MACHINE_NAME"]
    return out
