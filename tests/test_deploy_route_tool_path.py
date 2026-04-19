from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.responses import JSONResponse

from routes.contracts import DeployRequest, MachineCandidate, ResourceRequirements
from routes.deploy import deploy


class DeployRouteToolPathTests(unittest.TestCase):
    def test_deploy_invokes_pull_and_run_through_tool_path(self) -> None:
        candidate = MachineCandidate(
            machine_id="node-2",
            machine_type="cpu",
            grpc_target="10.0.0.2:50051",
            available_gb=64.0,
        )
        request = DeployRequest(
            image_name="nginx:latest",
            resource_requirements=ResourceRequirements(memory_gb=2),
            command="sleep infinity",
            restart_policy="unless-stopped",
            request_id="req-123",
        )

        with patch("routes.deploy._load_scheduler_candidates", return_value=[candidate]):
            with patch(
                "routes.deploy.invoke_pull_and_run_image_via_tool",
                new_callable=AsyncMock,
            ) as mock_invoke:
                mock_invoke.return_value = {
                    "success": True,
                    "container_id": "abc123",
                    "workload_state": "RUNNING",
                    "cpu_used": 10.0,
                    "memory_gb_used": 4.0,
                    "message": "ok",
                }
                with patch("routes.deploy.DRAGrpcClient.close", MagicMock()):
                    result = asyncio.run(deploy(request))

        self.assertEqual(result["status"], "DEPLOYED")
        self.assertEqual(result["selected_machine"]["machine_id"], "node-2")
        mock_invoke.assert_awaited_once()
        kwargs = mock_invoke.await_args.kwargs
        self.assertEqual(kwargs["image_name"], "nginx:latest")
        self.assertEqual(kwargs["command"], "sleep infinity")
        self.assertEqual(kwargs["restart_policy"], "unless-stopped")
        self.assertEqual(kwargs["grpc_target"], "10.0.0.2:50051")

    def test_deploy_returns_failed_response_when_tool_invocation_errors(self) -> None:
        candidate = MachineCandidate(
            machine_id="node-2",
            machine_type="cpu",
            grpc_target="10.0.0.2:50051",
            available_gb=64.0,
        )
        request = DeployRequest(
            image_name="nginx:latest",
            resource_requirements=ResourceRequirements(memory_gb=2),
            request_id="req-123",
        )

        with patch("routes.deploy._load_scheduler_candidates", return_value=[candidate]):
            with patch(
                "routes.deploy.invoke_pull_and_run_image_via_tool",
                new_callable=AsyncMock,
            ) as mock_invoke:
                mock_invoke.return_value = {"error": True, "message": "tool failed"}
                with patch("routes.deploy.DRAGrpcClient.close", MagicMock()):
                    result = asyncio.run(deploy(request))

        self.assertIsInstance(result, JSONResponse)
        self.assertEqual(result.status_code, 502)


if __name__ == "__main__":
    unittest.main()
