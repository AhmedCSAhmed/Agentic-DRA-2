"""Tests for DRA gRPC client and agent tools (no OpenAI API key required)."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import dra_pb2

from agent.client import DRAGrpcClient
from agent.tools import build_dra_tools
from dra.models import MachineModelORM


class DRAGrpcClientTests(unittest.TestCase):
    def test_pull_and_run_image_calls_stub_and_maps_response(self) -> None:
        resp = dra_pb2.PullAndRunResponse(
            success=True,
            container_id="abc123",
            workload_state=dra_pb2.RUNNING,
            cpu_used=12.5,
            memory_gb_used=8.0,
            message="ok",
        )
        stub = MagicMock()
        stub.PullAndRunImage.return_value = resp

        client = object.__new__(DRAGrpcClient)
        client._target = "test:123"
        client._channel = MagicMock()
        client._stub = stub

        out = DRAGrpcClient.pull_and_run_image(client, "nginx:latest", timeout=1.0)

        stub.PullAndRunImage.assert_called_once()
        (req,), kwargs = stub.PullAndRunImage.call_args
        self.assertEqual(req.image_name, "nginx:latest")
        self.assertEqual(kwargs.get("timeout"), 1.0)
        self.assertTrue(out["success"])
        self.assertEqual(out["container_id"], "abc123")
        self.assertEqual(out["workload_state"], "RUNNING")
        self.assertEqual(out["cpu_used"], 12.5)
        self.assertEqual(out["memory_gb_used"], 8.0)
        self.assertEqual(out["grpc_target"], "test:123")

    def test_pull_and_run_image_uses_ephemeral_channel_when_grpc_target_set(self) -> None:
        resp = dra_pb2.PullAndRunResponse(
            success=True,
            container_id="z",
            workload_state=dra_pb2.RUNNING,
            cpu_used=1.0,
            memory_gb_used=2.0,
            message="ok",
        )
        remote_stub = MagicMock()
        remote_stub.PullAndRunImage.return_value = resp

        channel = MagicMock()
        channel.close = MagicMock()

        client = object.__new__(DRAGrpcClient)
        client._target = "default:1"
        client._channel = MagicMock()
        client._stub = MagicMock()

        def fake_stub_ctor(ch: MagicMock) -> MagicMock:
            self.assertIs(ch, channel)
            return remote_stub

        with patch("agent.client.grpc.insecure_channel", return_value=channel):
            with patch("agent.client.dra_pb2_grpc.DRAServiceStub", side_effect=fake_stub_ctor):
                out = DRAGrpcClient.pull_and_run_image(
                    client, "nginx:latest", grpc_target="10.0.0.7:50051", timeout=1.0
                )

        client._stub.PullAndRunImage.assert_not_called()
        remote_stub.PullAndRunImage.assert_called_once()
        channel.close.assert_called_once()
        self.assertEqual(out["grpc_target"], "10.0.0.7:50051")
        self.assertTrue(out["success"])

    def test_pull_and_run_image_maps_rpc_error(self) -> None:
        import grpc

        stub = MagicMock()
        err = grpc.RpcError()
        err.code = MagicMock(return_value=grpc.StatusCode.UNAVAILABLE)  # type: ignore[assignment]
        err.details = MagicMock(return_value="connection refused")  # type: ignore[assignment]
        stub.PullAndRunImage.side_effect = err

        client = object.__new__(DRAGrpcClient)
        client._target = "test:123"
        client._channel = MagicMock()
        client._stub = stub

        out = DRAGrpcClient.pull_and_run_image(client, "alpine:3.19", timeout=1.0)
        self.assertTrue(out.get("rpc_error"))
        self.assertEqual(out.get("code"), "UNAVAILABLE")
        self.assertEqual(out.get("grpc_target"), "test:123")


class BuildDraToolsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.grpc_client = MagicMock()
        self.grpc_client.pull_and_run_image.return_value = {
            "success": True,
            "grpc_target": "x:1",
        }
        self.repo = MagicMock()

    def test_build_dra_tools_registers_list_and_pull_and_run_image(self) -> None:
        tools = build_dra_tools(self.grpc_client, self.repo)
        names = sorted(t.name for t in tools)
        self.assertEqual(
            names,
            ["list_dra_machines", "pull_and_run_image", "start_dra_grpc_server"],
        )

    @patch("agent.tools.json.dumps")
    def test_start_dra_grpc_server_tool_calls_subprocess_helper(
        self, mock_dumps: MagicMock
    ) -> None:
        mock_dumps.side_effect = lambda x: f"JSON:{x}"
        with patch(
            "agent.tools.start_dra_grpc_server",
            return_value={"started": True, "pid": 999},
        ) as mock_start:
            tools = build_dra_tools(self.grpc_client, self.repo)
            st = next(t for t in tools if t.name == "start_dra_grpc_server")
            loop_body = st.on_invoke_tool

            async def _run() -> object:
                ctx = MagicMock()
                ctx.tool_name = st.name
                return await loop_body(ctx, '{"grpc_bind":"127.0.0.1:50051"}')

            import asyncio

            asyncio.run(_run())
        mock_start.assert_called_once_with(grpc_bind="127.0.0.1:50051")

    @patch("agent.tools.json.dumps")
    def test_list_dra_machines_calls_repository(self, mock_dumps: MagicMock) -> None:
        mock_dumps.side_effect = lambda x: f"JSON:{x}"
        now = datetime.now(timezone.utc)
        m = MachineModelORM(
            machine_id="n1",
            machine_name="w1",
            machine_type="cpu",
            machine_created_at=now,
            machine_updated_at=now,
            dra_grpc_target="10.0.0.1:50051",
        )
        self.repo.list_machines.return_value = [m]

        tools = build_dra_tools(self.grpc_client, self.repo)
        list_tool = next(t for t in tools if t.name == "list_dra_machines")
        loop_body = list_tool.on_invoke_tool

        async def _run() -> object:
            ctx = MagicMock()
            ctx.tool_name = list_tool.name
            return await loop_body(ctx, "{}")

        import asyncio

        asyncio.run(_run())
        self.repo.list_machines.assert_called_once_with(machine_type=None)

    @patch("agent.tools.json.dumps")
    def test_pull_and_run_image_tool_invokes_grpc_with_machine_id(self, mock_dumps: MagicMock) -> None:
        mock_dumps.side_effect = lambda x: f"JSON:{x}"
        now = datetime.now(timezone.utc)
        self.repo.find_machine_by_id.return_value = MachineModelORM(
            machine_id="n1",
            machine_name="w1",
            machine_type="cpu",
            machine_created_at=now,
            machine_updated_at=now,
            dra_grpc_target="192.168.1.5:50051",
        )

        tools = build_dra_tools(self.grpc_client, self.repo)
        rpc_tool = next(t for t in tools if t.name == "pull_and_run_image")
        loop_body = rpc_tool.on_invoke_tool

        async def _run() -> object:
            ctx = MagicMock()
            ctx.tool_name = rpc_tool.name
            payload = '{"image_name":"nginx:latest","machine_id":"n1"}'
            return await loop_body(ctx, payload)

        import asyncio

        asyncio.run(_run())
        self.repo.find_machine_by_id.assert_called_once_with("n1")
        self.grpc_client.pull_and_run_image.assert_called_once_with(
            "nginx:latest", grpc_target="192.168.1.5:50051"
        )

    @patch("agent.tools.json.dumps")
    def test_pull_and_run_image_tool_invokes_grpc_with_explicit_target(
        self, mock_dumps: MagicMock
    ) -> None:
        mock_dumps.side_effect = lambda x: f"JSON:{x}"

        tools = build_dra_tools(self.grpc_client, self.repo)
        rpc_tool = next(t for t in tools if t.name == "pull_and_run_image")
        loop_body = rpc_tool.on_invoke_tool

        async def _run() -> object:
            ctx = MagicMock()
            ctx.tool_name = rpc_tool.name
            payload = '{"image_name":"alpine:3.19","grpc_target":"10.0.0.2:50051"}'
            return await loop_body(ctx, payload)

        import asyncio

        asyncio.run(_run())
        self.repo.find_machine_by_id.assert_not_called()
        self.grpc_client.pull_and_run_image.assert_called_once_with(
            "alpine:3.19", grpc_target="10.0.0.2:50051"
        )


class InspectRunTests(unittest.TestCase):
    def test_tool_call_names_from_result(self) -> None:
        from agent.inspect_run import tool_call_names_from_result

        class _Item:
            type = "tool_call_item"
            raw_item = {"name": "pull_and_run_image", "arguments": '{"image_name":"x"}'}

        class _Result:
            new_items = [_Item()]

        names = tool_call_names_from_result(_Result())  # type: ignore[arg-type]
        self.assertEqual(names, ["pull_and_run_image"])
