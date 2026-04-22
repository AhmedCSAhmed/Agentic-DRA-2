from __future__ import annotations

import importlib
import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from dra.models import MachineModelORM


class DRAServiceBindTests(unittest.TestCase):
    def _load_serve_module(self):
        fake_pb2_grpc = types.SimpleNamespace(
            DRAServiceServicer=object,
            add_DRAServiceServicer_to_server=MagicMock(),
        )
        with patch.dict(sys.modules, {"dra_pb2_grpc": fake_pb2_grpc}):
            sys.modules.pop("dra.grpc_server", None)
            sys.modules.pop("dra.serve", None)
            import dra.serve as serve_module

            return importlib.reload(serve_module)

    def test_serve_derives_bind_port_from_machine_registry_target(self) -> None:
        serve_module = self._load_serve_module()
        machine = MachineModelORM(
            machine_id="node-1",
            machine_name="worker-1",
            machine_type="cpu",
            machine_created_at=datetime.now(timezone.utc),
            machine_updated_at=datetime.now(timezone.utc),
            dra_grpc_target="100.111.68.57:61000",
        )
        server = MagicMock()
        stop_result = MagicMock()
        server.stop.return_value = stop_result
        server.wait_for_termination.side_effect = KeyboardInterrupt

        with patch.object(serve_module, "load_machine_from_database", return_value=machine):
            with patch.object(serve_module.grpc, "server", return_value=server):
                with patch.object(serve_module.dra_pb2_grpc, "add_DRAServiceServicer_to_server"):
                    with self.assertRaises(SystemExit) as exc:
                        serve_module.serve(machine_name="worker-1")

        self.assertEqual(exc.exception.code, 0)
        server.add_insecure_port.assert_called_once_with("0.0.0.0:61000")
        server.start.assert_called_once()
        server.stop.assert_called_once_with(grace=5)
        stop_result.wait.assert_called_once()

    def test_serve_keeps_loopback_bind_when_registry_target_is_local(self) -> None:
        serve_module = self._load_serve_module()
        machine = MachineModelORM(
            machine_id="node-1",
            machine_name="worker-1",
            machine_type="cpu",
            machine_created_at=datetime.now(timezone.utc),
            machine_updated_at=datetime.now(timezone.utc),
            dra_grpc_target="127.0.0.1:50051",
        )
        server = MagicMock()
        stop_result = MagicMock()
        server.stop.return_value = stop_result
        server.wait_for_termination.side_effect = KeyboardInterrupt

        with patch.object(serve_module, "load_machine_from_database", return_value=machine):
            with patch.object(serve_module.grpc, "server", return_value=server):
                with patch.object(serve_module.dra_pb2_grpc, "add_DRAServiceServicer_to_server"):
                    with self.assertRaises(SystemExit):
                        serve_module.serve(machine_name="worker-1")

        server.add_insecure_port.assert_called_once_with("127.0.0.1:50051")
