from __future__ import annotations

import subprocess
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from dra.grpc_server import DRAServiceServicer


class GrpcServerStopSyncTests(unittest.TestCase):
    def test_sync_running_jobs_updates_stopped_containers_for_machine(self) -> None:
        servicer = object.__new__(DRAServiceServicer)
        servicer._machine_id = "node-1"
        servicer.IMAGE_CHECK_TIMEOUT_SECONDS = 1

        job_local = SimpleNamespace(
            image_id="cid-1",
            resource_requirements={"machine_id": "node-1", "memory_gb": 2.0},
        )
        job_other = SimpleNamespace(
            image_id="cid-2",
            resource_requirements={"machine_id": "node-2", "memory_gb": 2.0},
        )

        servicer._container_is_running = MagicMock(return_value=False)
        servicer._record_job_stopped_and_release = MagicMock(return_value=2.0)

        with patch("dra.database.Database", return_value=MagicMock()):
            with patch("dra.repositories.jobs.JobsRepository") as repo_cls:
                repo_cls.return_value.list_running_jobs.return_value = [job_local, job_other]

                synced = servicer._sync_running_jobs_with_docker()

        self.assertEqual(synced, 1)
        servicer._container_is_running.assert_called_once_with("cid-1")
        servicer._record_job_stopped_and_release.assert_called_once_with(container_id="cid-1")

    def test_resource_requirements_obj_handles_json_string(self) -> None:
        payload = DRAServiceServicer._resource_requirements_obj('{"machine_id":"node-1"}')
        self.assertEqual(payload.get("machine_id"), "node-1")

    def test_container_is_running_timeout_treated_as_running(self) -> None:
        servicer = object.__new__(DRAServiceServicer)
        servicer.IMAGE_CHECK_TIMEOUT_SECONDS = 1

        with patch("dra.grpc_server.subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="docker", timeout=1)):
            self.assertTrue(servicer._container_is_running("cid-timeout"))


if __name__ == "__main__":
    unittest.main()
