from __future__ import annotations

import unittest

from routes.contracts import MachineCandidate, ResourceRequirements
from routes.scheduler import grpc_target_is_loopback, select_best_machine


class SchedulerLoopbackTests(unittest.TestCase):
    def test_grpc_target_is_loopback(self) -> None:
        self.assertTrue(grpc_target_is_loopback("127.0.0.1:50051"))
        self.assertTrue(grpc_target_is_loopback("localhost:50051"))
        self.assertFalse(grpc_target_is_loopback("100.111.68.57:50051"))

    def test_prefers_non_loopback_when_both_eligible(self) -> None:
        local = MachineCandidate(
            machine_id="local-1",
            machine_type="cpu",
            grpc_target="127.0.0.1:50051",
            available_gb=1000.0,
        )
        remote = MachineCandidate(
            machine_id="remote-1",
            machine_type="cpu",
            grpc_target="100.111.68.57:50051",
            available_gb=16.0,
        )
        decision = select_best_machine(
            [local, remote],
            ResourceRequirements(memory_gb=2.0),
            prefer_non_loopback=True,
        )
        self.assertIsNotNone(decision.selected)
        assert decision.selected is not None
        self.assertEqual(decision.selected.machine_id, "remote-1")

    def test_only_loopback_still_selects(self) -> None:
        local = MachineCandidate(
            machine_id="local-1",
            machine_type="cpu",
            grpc_target="127.0.0.1:50051",
            available_gb=64.0,
        )
        decision = select_best_machine(
            [local],
            ResourceRequirements(memory_gb=2.0),
            prefer_non_loopback=True,
        )
        self.assertIsNotNone(decision.selected)
        assert decision.selected is not None
        self.assertEqual(decision.selected.grpc_target, "127.0.0.1:50051")


if __name__ == "__main__":
    unittest.main()
