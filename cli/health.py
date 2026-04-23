from __future__ import annotations

import os
import time
from dataclasses import dataclass

import grpc


@dataclass(frozen=True)
class GrpcProbeResult:
    ok: bool
    latency_ms: float | None = None
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
        t0 = time.monotonic()
        grpc.channel_ready_future(channel).result(timeout=_probe_timeout_s())
        latency_ms = (time.monotonic() - t0) * 1000
        return GrpcProbeResult(ok=True, latency_ms=round(latency_ms, 1))
    except Exception as exc:
        return GrpcProbeResult(ok=False, error=str(exc))
    finally:
        channel.close()

