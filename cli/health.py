from __future__ import annotations

import os
from dataclasses import dataclass

import grpc


@dataclass(frozen=True)
class GrpcProbeResult:
    ok: bool
    error: str | None = None


def _probe_timeout_s() -> float:
    raw = (os.environ.get("ATLAS_STATUS_GRPC_TIMEOUT_S") or "").strip()
    if not raw:
        return 0.6
    try:
        val = float(raw)
    except ValueError:
        return 0.6
    return 0.6 if val <= 0 else val


def probe_grpc_target(target: str | None) -> GrpcProbeResult:
    """Best-effort connectivity probe (no RPC method required).

    This just checks whether the gRPC channel becomes ready (i.e. TCP connect + handshake).
    """

    t = (target or "").strip()
    if not t or t == "—":
        return GrpcProbeResult(ok=False, error="missing gRPC target")

    channel = grpc.insecure_channel(t)
    try:
        grpc.channel_ready_future(channel).result(timeout=_probe_timeout_s())
        return GrpcProbeResult(ok=True, error=None)
    except Exception as exc:
        return GrpcProbeResult(ok=False, error=str(exc))
    finally:
        channel.close()

