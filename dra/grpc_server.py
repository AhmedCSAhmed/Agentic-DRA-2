"""gRPC servicer implementation for image pull-and-run workflows in DRA."""

from __future__ import annotations

import logging
import os
import re
import shlex
import subprocess
from typing import Sequence

import grpc
import psutil

import dra_pb2
import dra_pb2_grpc


logger = logging.getLogger(__name__)


class DockerPullError(Exception):
    """Raised when `docker pull` fails or times out."""


class DockerRunError(Exception):
    """Raised when `docker run` fails, times out, or returns invalid output."""


class DRAServiceServicer(dra_pb2_grpc.DRAServiceServicer):
    """Implements DRA gRPC endpoints responsible for image lifecycle operations."""

    COMMAND_TIMEOUT_SECONDS = 120
    IMAGE_CHECK_TIMEOUT_SECONDS = 30
    IMAGE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._/\-:@]{0,254}$")
    RESTART_POLICY_PATTERN = re.compile(
        r"^(no|on-failure|always|unless-stopped|on-failure:\d+)$"
    )

    def __init__(self, *, machine_id: str | None = None) -> None:
        super().__init__()
        self._machine_id = (machine_id or "").strip() or None

    def PullAndRunImage(self, request, context):
        """Pulls an image if needed, runs a container, and returns runtime metrics.

        Flow:
        1. Validate input image name.
        2. Pull image if it is not already present locally.
        3. Run container in detached mode.
        4. Collect host CPU and memory usage metrics.

        The method always returns a structured `PullAndRunResponse` and never allows
        uncaught exceptions to crash the gRPC server process.
        """

        image_name = (request.image_name or "").strip()
        if not self._is_valid_image_name(image_name):
            logger.warning("Invalid image_name received: %r", request.image_name)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Invalid image_name")
            return self._error_response("Invalid image_name")

        logger.info("PullAndRunImage request received for image=%s", image_name)

        restart_policy = self._resolve_restart_policy(request)
        if restart_policy is not None and not self._is_valid_restart_policy(restart_policy):
            logger.warning("Invalid restart_policy received: %r", restart_policy)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Invalid restart_policy")
            return self._error_response(
                "Invalid restart_policy (use no, on-failure, always, unless-stopped, or on-failure:N)"
            )

        reserved_gb = float(getattr(request, "memory_gb", 0.0) or 0.0)
        if reserved_gb < 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("memory_gb must be >= 0")
            return self._error_response("memory_gb must be >= 0")

        reserved_applied = False
        succeeded = False
        try:
            # Reserve capacity in DB before starting the container.
            if self._machine_id and reserved_gb > 0:
                self._apply_capacity_delta(delta_gb=-reserved_gb)
                reserved_applied = True

            pulled = self.pull_image(image_name)
            cmd_args = self._resolve_run_command(request)
            container_id = self.run_container(
                image_name,
                command=cmd_args,
                restart_policy=restart_policy,
            )
            cpu_used, memory_gb_used = self.get_metrics()

            # Persist a job record for stop/release bookkeeping (best-effort).
            if self._machine_id:
                self._record_job_started(
                    container_id=container_id,
                    image_name=image_name,
                    reserved_gb=reserved_gb,
                )

            message = (
                f"Image '{image_name}' pulled and container started successfully."
                if pulled
                else f"Image '{image_name}' already present locally; container started successfully."
            )
            logger.info(
                "Container started image=%s container_id=%s cpu_used=%.2f memory_gb_used=%.2f",
                image_name,
                container_id,
                cpu_used,
                memory_gb_used,
            )
            succeeded = True
            return dra_pb2.PullAndRunResponse(
                success=True,
                container_id=container_id,
                workload_state=dra_pb2.RUNNING,
                cpu_used=cpu_used,
                memory_gb_used=memory_gb_used,
                message=message,
            )

        except DockerPullError as exc:
            logger.exception("Docker pull failed for image=%s", image_name)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return self._error_response(str(exc))

        except DockerRunError as exc:
            logger.exception("Docker run failed for image=%s", image_name)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return self._error_response(str(exc))

        except Exception as exc:  # pragma: no cover - defensive guard
            logger.exception("Unexpected failure in PullAndRunImage")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Unexpected server error")
            return self._error_response(f"Unexpected error: {exc}")
        finally:
            # If anything failed after reserving capacity, release it back.
            if reserved_applied and not succeeded:
                try:
                    self._apply_capacity_delta(delta_gb=reserved_gb)
                except Exception:
                    logger.exception("Failed to rollback reserved capacity for machine_id=%s", self._machine_id)

    def StopContainer(self, request, context):
        container_id = (request.container_id or "").strip()
        if not container_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("container_id is required")
            return dra_pb2.StopContainerResponse(success=False, message="container_id is required", memory_gb_released=0.0)

        try:
            self._run_command(["docker", "rm", "-f", container_id], timeout=self.COMMAND_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(exc))
            return dra_pb2.StopContainerResponse(success=False, message="docker rm timed out", memory_gb_released=0.0)
        except subprocess.CalledProcessError as exc:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(self._stderr_or_stdout(exc))
            return dra_pb2.StopContainerResponse(success=False, message=self._stderr_or_stdout(exc), memory_gb_released=0.0)

        released = 0.0
        if self._machine_id:
            released = self._record_job_stopped_and_release(container_id=container_id)

        return dra_pb2.StopContainerResponse(
            success=True,
            message=f"Container '{container_id}' stopped/removed",
            memory_gb_released=float(released),
        )

    def pull_image(self, image_name: str) -> bool:
        """Pulls a Docker image when it is not already available locally.

        Returns:
            `True` if `docker pull` was executed.
            `False` if the image already existed and pull was skipped.
        """

        logger.info("Checking local image cache for image=%s", image_name)
        if self._image_exists_locally(image_name):
            logger.info("Image already exists locally, skipping pull: image=%s", image_name)
            return False

        logger.info("Pulling docker image=%s", image_name)
        try:
            self._run_command(
                ["docker", "pull", image_name],
                timeout=self.COMMAND_TIMEOUT_SECONDS,
            )
            logger.info("Image pull completed for image=%s", image_name)
            return True
        except subprocess.TimeoutExpired as exc:
            raise DockerPullError(f"Docker pull timed out for image '{image_name}'") from exc
        except subprocess.CalledProcessError as exc:
            raise DockerPullError(
                f"Docker pull failed for image '{image_name}': {self._stderr_or_stdout(exc)}"
            ) from exc

    def _resolve_run_command(self, request: dra_pb2.PullAndRunRequest) -> list[str]:
        """Extra args after the image: request.command, else DRA_RUN_COMMAND env."""

        if request.command:
            return [str(x).strip() for x in request.command if str(x).strip()]
        env_cmd = (os.environ.get("DRA_RUN_COMMAND") or "").strip()
        if env_cmd:
            return shlex.split(env_cmd)
        return []

    def _resolve_restart_policy(self, request: dra_pb2.PullAndRunRequest) -> str | None:
        """Docker --restart value: request field, else DRA_DOCKER_RESTART_POLICY env."""

        raw = (request.restart_policy or "").strip()
        if raw:
            return raw
        env_pol = (os.environ.get("DRA_DOCKER_RESTART_POLICY") or "").strip()
        return env_pol or None

    @classmethod
    def _is_valid_restart_policy(cls, policy: str) -> bool:
        return bool(cls.RESTART_POLICY_PATTERN.fullmatch(policy.strip()))

    def run_container(
        self,
        image_name: str,
        *,
        command: Sequence[str] | None = None,
        restart_policy: str | None = None,
    ) -> str:
        """Runs a container in detached mode and returns its container ID."""

        extra = list(command) if command else []
        restart: list[str] = []
        if restart_policy:
            restart = ["--restart", restart_policy]

        logger.info(
            "Starting container for image=%s restart=%s command=%s",
            image_name,
            restart_policy or "(none)",
            extra or "(image default)",
        )
        try:
            run_cmd = ["docker", "run", "-d", *restart, image_name, *extra]
            result = self._run_command(
                run_cmd,
                timeout=self.COMMAND_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            raise DockerRunError(f"Docker run timed out for image '{image_name}'") from exc
        except subprocess.CalledProcessError as exc:
            raise DockerRunError(
                f"Docker run failed for image '{image_name}': {self._stderr_or_stdout(exc)}"
            ) from exc

        container_id = (result.stdout or "").strip()
        if not container_id:
            raise DockerRunError(
                f"Docker run returned empty container id for image '{image_name}'"
            )

        logger.info("Container started: image=%s container_id=%s", image_name, container_id)
        return container_id

    def _apply_capacity_delta(self, *, delta_gb: float) -> None:
        """Best-effort update of machines.available_gb for this server's machine_id."""
        from dra.database import Database
        from dra.repositories.machines import MachineRepository

        if not self._machine_id:
            return
        repo = MachineRepository(Database())
        repo.increment_machine_availability(self._machine_id, delta_gb=float(delta_gb))

    def _record_job_started(self, *, container_id: str, image_name: str, reserved_gb: float) -> None:
        from dra.database import Database
        from dra.repositories.jobs import JobsRepository

        repo = JobsRepository(Database())
        repo.create_job(
            image_id=container_id,
            image_name=image_name,
            status="RUNNING",
            resource_requirements={"memory_gb": float(reserved_gb), "machine_id": self._machine_id},
        )

    def _record_job_stopped_and_release(self, *, container_id: str) -> float:
        """Mark job STOPPED and release reserved memory back to this machine."""
        from dra.database import Database
        from dra.repositories.jobs import JobNotFoundError, JobsRepository

        db = Database()
        repo = JobsRepository(db)
        try:
            job = repo.find_job_by_image_id(container_id)
        except JobNotFoundError:
            return 0.0

        reserved = 0.0
        rr = getattr(job, "resource_requirements", None)
        if isinstance(rr, dict):
            raw = rr.get("memory_gb")
            if isinstance(raw, (int, float)):
                reserved = float(raw)

        # Update job status best-effort.
        try:
            repo.update_job_status(job.id, "STOPPED")
        except Exception:
            logger.exception("Failed to update job status for container_id=%s", container_id)

        if reserved > 0:
            self._apply_capacity_delta(delta_gb=reserved)
        return reserved

    def get_metrics(self) -> tuple[float, float]:
        """Collects host-level CPU usage percent and used memory in GB."""

        logger.info("Collecting system metrics after container start")
        cpu_used = float(psutil.cpu_percent(interval=0.5))
        memory_gb_used = float(psutil.virtual_memory().used / (1024**3))
        logger.info(
            "System metrics collected cpu_used=%.2f memory_gb_used=%.2f",
            cpu_used,
            memory_gb_used,
        )
        return cpu_used, memory_gb_used

    def _image_exists_locally(self, image_name: str) -> bool:
        """Checks whether the Docker image already exists locally."""

        try:
            self._run_command(
                ["docker", "image", "inspect", image_name],
                timeout=self.IMAGE_CHECK_TIMEOUT_SECONDS,
            )
            return True
        except subprocess.CalledProcessError:
            return False
        except subprocess.TimeoutExpired:
            logger.warning(
                "Timed out while checking local image existence for image=%s", image_name
            )
            return False

    @staticmethod
    def _run_command(command: Sequence[str], *, timeout: int) -> subprocess.CompletedProcess:
        """Executes a command safely without shell invocation and with timeout."""

        logger.debug("Executing command: %s", " ".join(command))
        return subprocess.run(
            list(command),
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    @staticmethod
    def _stderr_or_stdout(exc: subprocess.CalledProcessError) -> str:
        """Returns the most useful error text from a subprocess failure."""

        output = (exc.stderr or exc.stdout or "").strip()
        return output if output else "unknown docker error"

    @classmethod
    def _is_valid_image_name(cls, image_name: str) -> bool:
        """Validates Docker image name input for basic safety and correctness."""

        return bool(image_name) and bool(cls.IMAGE_NAME_PATTERN.fullmatch(image_name))

    @staticmethod
    def _error_response(message: str) -> dra_pb2.PullAndRunResponse:
        """Builds a standard error response payload for PullAndRunImage failures."""

        return dra_pb2.PullAndRunResponse(
            success=False,
            container_id="",
            workload_state=dra_pb2.ERROR,
            cpu_used=0.0,
            memory_gb_used=0.0,
            message=message,
        )
