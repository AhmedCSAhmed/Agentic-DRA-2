from __future__ import annotations

import ipaddress
import os
import time
from dataclasses import dataclass

import grpc


@dataclass(frozen=True)
class GrpcProbeResult:
    ok: bool
    latency_ms: float | None = None
    error: str | None = None


# Tailscale uses the 100.64.0.0/10 CGNAT range for its virtual IPs.
_TAILSCALE_CGNAT = ipaddress.ip_network("100.64.0.0/10")


def is_tailscale_target(target: str) -> bool:
    """Return True when the gRPC target host is a Tailscale CGNAT address (100.64.0.0/10)."""
    raw = (target or "").strip()
    host = raw.rsplit(":", 1)[0].strip().strip("[]")
    try:
        return ipaddress.ip_address(host) in _TAILSCALE_CGNAT
    except ValueError:
        return False


def _probe_timeout_s() -> float:
    raw = (os.environ.get("ATLAS_STATUS_GRPC_TIMEOUT_S") or "").strip()
    if not raw:
        # Default raised from 0.6 → 3.0 so Tailscale DERP-relay connections
        # (which add 50-200ms of overhead) don't time out prematurely.
        return 3.0
    try:
        val = float(raw)
    except ValueError:
        return 3.0
    return 3.0 if val <= 0 else val


def probe_grpc_target(target: str | None) -> GrpcProbeResult:
    """Best-effort connectivity probe (no RPC method required).

    Checks whether the gRPC channel becomes ready (TCP connect + TLS handshake).
    Timeout is intentionally generous (3 s default) to accommodate Tailscale
    DERP relay latency.
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
        hint = "Tailscale not connected?" if is_tailscale_target(t) else str(exc)
        return GrpcProbeResult(ok=False, error=hint)
    finally:
        channel.close()

