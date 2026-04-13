"""gRPC client for `dra.DRAService` (see `dra/grpc_server.py` and `dra.proto`).

Default address comes from the constructor or ``DRA_GRPC_TARGET`` (e.g. ``192.168.1.10:50051``).
Each tool call can also pass ``grpc_target`` to open a one-off connection to another machine
without changing the default channel.
"""

from __future__ import annotations

import os
from typing import Any

import grpc

import dra_pb2
import dra_pb2_grpc


DEFAULT_TARGET = "localhost:50051"
DEFAULT_RPC_TIMEOUT_S = 130.0


class DRAGrpcClient:
    """Thin wrapper around `DRAServiceStub` for toolchain use from the Agents SDK."""

    def __init__(self, target: str | None = None) -> None:
        self._target = target or os.environ.get("DRA_GRPC_TARGET", DEFAULT_TARGET)
        self._channel = grpc.insecure_channel(self._target)
        self._stub = dra_pb2_grpc.DRAServiceStub(self._channel)

    @property
    def target(self) -> str:
        return self._target

    def close(self) -> None:
        self._channel.close()

    def pull_and_run_image(
        self,
        image_name: str,
        *,
        grpc_target: str | None = None,
        timeout: float | None = DEFAULT_RPC_TIMEOUT_S,
    ) -> dict[str, Any]:
        """Calls `PullAndRunImage` and returns a JSON-serializable dict.

        If ``grpc_target`` is set (``host:port``), uses a short-lived channel to that address.
        Otherwise uses the client's default stub (constructor / ``DRA_GRPC_TARGET``).
        """

        request = dra_pb2.PullAndRunRequest(image_name=image_name.strip())
        override = (grpc_target or "").strip()
        if override:
            channel = grpc.insecure_channel(override)
            try:
                stub = dra_pb2_grpc.DRAServiceStub(channel)
                return self._pull_and_run_with_stub(stub, request, timeout, connected_to=override)
            finally:
                channel.close()
        return self._pull_and_run_with_stub(
            self._stub, request, timeout, connected_to=self._target
        )

    def _pull_and_run_with_stub(
        self,
        stub: dra_pb2_grpc.DRAServiceStub,
        request: dra_pb2.PullAndRunRequest,
        timeout: float | None,
        *,
        connected_to: str,
    ) -> dict[str, Any]:
        try:
            resp = stub.PullAndRunImage(request, timeout=timeout)
        except grpc.RpcError as exc:
            return {
                "rpc_error": True,
                "code": exc.code().name,
                "details": exc.details() or "",
                "grpc_target": connected_to,
            }

        return {
            "success": resp.success,
            "container_id": resp.container_id,
            "workload_state": dra_pb2.WorkloadState.Name(resp.workload_state),
            "cpu_used": resp.cpu_used,
            "memory_gb_used": resp.memory_gb_used,
            "message": resp.message,
            "grpc_target": connected_to,
        }
