from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from routes.contracts import MachineCandidate, ResourceRequirements
from scheduled_deploy import execute_scheduled_deploy


class ScheduledDeployFailoverTests(unittest.TestCase):
    def test_execute_scheduled_deploy_fails_over_on_unavailable_rpc(self) -> None:
        candidates = [
            MachineCandidate(
                machine_id="primary",
                machine_type="cpu",
                grpc_target="100.111.68.57:50051",
                available_gb=30.0,
            ),
            MachineCandidate(
                machine_id="fallback",
                machine_type="cpu",
                grpc_target="100.104.168.47:50051",
                available_gb=8.0,
            ),
        ]

        async def fake_invoke(**kwargs):
            if kwargs["machine_id"] == "primary":
                return {
                    "rpc_error": True,
                    "code": "UNAVAILABLE",
                    "details": "connection refused",
                    "grpc_target": kwargs["grpc_target"],
                }
            return {
                "success": True,
                "container_id": "abc123",
                "workload_state": "RUNNING",
                "cpu_used": 10.0,
                "memory_gb_used": 4.0,
                "message": "ok",
                "grpc_target": kwargs["grpc_target"],
            }

        with patch("scheduled_deploy._load_scheduler_candidates", return_value=candidates):
            with patch(
                "scheduled_deploy.invoke_pull_and_run_image_via_tool",
                new=AsyncMock(side_effect=fake_invoke),
            ) as mock_invoke:
                with patch("scheduled_deploy.DRAGrpcClient.close", MagicMock()):
                    decision, rpc_result = asyncio.run(
                        execute_scheduled_deploy(
                            image_name="nginx:latest",
                            resource_requirements=ResourceRequirements(memory_gb=2.0),
                        )
                    )

        self.assertIsNotNone(decision.selected)
        assert decision.selected is not None
        self.assertEqual(decision.selected.machine_id, "fallback")
        assert rpc_result is not None
        self.assertTrue(rpc_result["success"])
        self.assertEqual(len(rpc_result["attempts"]), 2)
        self.assertEqual(rpc_result["attempts"][0]["machine_id"], "primary")
        self.assertEqual(rpc_result["attempts"][1]["machine_id"], "fallback")
        self.assertEqual(mock_invoke.await_count, 2)


if __name__ == "__main__":
    unittest.main()
