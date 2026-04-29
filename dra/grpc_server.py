"""gRPC servicer implementation for image pull-and-run workflows in DRA."""

from __future__ import annotations

import logging
import os
import re
import shlex
import subprocess
import threading
import time
import json
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
    DOCKER_MIN_MEMORY_BYTES = 6 * 1024 * 1024
    IMAGE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._/\-:@]{0,254}$")
    RESTART_POLICY_PATTERN = re.compile(
        r"^(no|on-failure|always|unless-stopped|on-failure:\d+)$"
    )

    def __init__(self, *, machine_id: str | None = None) -> None:
        super().__init__()
        self._machine_id = (machine_id or "").strip() or None
        if self._machine_id:
            t = threading.Thread(target=self._heartbeat_loop, daemon=True)
            t.start()

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

        reserved_cores = float(getattr(request, "cpu_cores", 0.0) or 0.0)
        if reserved_cores < 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("cpu_cores must be >= 0")
            return self._error_response("cpu_cores must be >= 0")

        reserved_applied = False
        cores_applied = False
        succeeded = False
        try:
            # Reserve memory and cores in DB before starting the container.
            if self._machine_id and reserved_gb > 0:
                self._apply_capacity_delta(delta_gb=-reserved_gb)
                reserved_applied = True
            if self._machine_id and reserved_cores > 0:
                self._apply_cores_delta(delta_cores=-reserved_cores)
                cores_applied = True

            pulled = self.pull_image(image_name)
            cmd_args = self._resolve_run_command(request)
            container_id = self.run_container(
                image_name,
                command=cmd_args,
                restart_policy=restart_policy,
                memory_gb=reserved_gb,
                cpu_cores=reserved_cores,
            )
            if self._machine_id:
                threading.Thread(
                    target=self._watch_container,
                    args=(container_id,),
                    daemon=True,
                ).start()
            cpu_used, memory_gb_used = self.get_container_metrics(container_id)

            # Persist a job record for stop/release bookkeeping (best-effort).
            if self._machine_id:
                self._record_job_started(
                    container_id=container_id,
                    image_name=image_name,
                    reserved_gb=reserved_gb,
                    reserved_cores=reserved_cores,
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
            if cores_applied and not succeeded:
                try:
                    self._apply_cores_delta(delta_cores=reserved_cores)
                except Exception:
                    logger.exception("Failed to rollback reserved cores for machine_id=%s", self._machine_id)

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
        memory_gb: float = 0.0,
        cpu_cores: float = 0.0,
    ) -> str:
        """Runs a container in detached mode and returns its container ID.

        ``memory_gb`` and ``cpu_cores``, when > 0, are enforced via ``--memory`` /
        ``--memory-swap`` and ``--cpus`` so the kernel actually caps the container
        at what was reserved. Without this, the registry's bookkeeping drifts from
        reality whenever a workload exceeds its request.
        """

        extra = list(command) if command else []
        restart: list[str] = []
        if restart_policy:
            restart = ["--restart", restart_policy]

        limits: list[str] = []
        if memory_gb and memory_gb > 0:
            memory_bytes = int(float(memory_gb) * (1024 ** 3))
            if memory_bytes < self.DOCKER_MIN_MEMORY_BYTES:
                raise DockerRunError(
                    f"memory_gb={memory_gb} is below Docker's minimum of "
                    f"{self.DOCKER_MIN_MEMORY_BYTES} bytes (~6 MB)"
                )
            # memory-swap == memory => no swap headroom beyond the cap.
            limits += [
                f"--memory={memory_bytes}",
                f"--memory-swap={memory_bytes}",
            ]
        if cpu_cores and cpu_cores > 0:
            limits += [f"--cpus={float(cpu_cores):g}"]

        logger.info(
            "Starting container for image=%s restart=%s command=%s memory_gb=%s cpu_cores=%s",
            image_name,
            restart_policy or "(none)",
            extra or "(image default)",
            memory_gb,
            cpu_cores,
        )
        try:
            run_cmd = ["docker", "run", "-d", *restart, *limits, image_name, *extra]
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

    def _apply_capacity_delta(self, *, delta_gb: float, machine_id: str | None = None) -> None:
        """Best-effort update of machines.available_gb.

        Targets ``machine_id`` if provided, else falls back to this server's identity.
        Falling back is only useful when the server boots with --machine-name; jobs
        always carry their own ``machine_id`` in ``resource_requirements``, so callers
        on the release path should pass it explicitly.
        """
        from dra.database import Database
        from dra.repositories.machines import MachineRepository

        target = (machine_id or self._machine_id or "").strip() or None
        if not target:
            return
        repo = MachineRepository(Database())
        repo.increment_machine_availability(target, delta_gb=float(delta_gb))

    def _apply_cores_delta(self, *, delta_cores: float, machine_id: str | None = None) -> None:
        """Best-effort update of machines.available_cores. See ``_apply_capacity_delta``."""
        from dra.database import Database
        from dra.repositories.machines import MachineRepository

        target = (machine_id or self._machine_id or "").strip() or None
        if not target:
            return
        repo = MachineRepository(Database())
        repo.increment_machine_cores(target, delta_cores=float(delta_cores))

    def _record_job_started(
        self, *, container_id: str, image_name: str, reserved_gb: float, reserved_cores: float = 0.0
    ) -> None:
        from dra.database import Database
        from dra.repositories.jobs import JobsRepository

        repo = JobsRepository(Database())
        repo.create_job(
            image_id=container_id,
            image_name=image_name,
            status="RUNNING",
            resource_requirements={
                "memory_gb": float(reserved_gb),
                "cpu_cores": float(reserved_cores),
                "machine_id": self._machine_id,
            },
        )

    def _record_job_stopped_and_release(self, *, container_id: str) -> float:
        """Mark job STOPPED and release reserved memory and cores back to this machine.

        Idempotent: only releases capacity if the job was still RUNNING (prevents
        double-release when StopContainer RPC and the container watcher race).
        Returns the GB released (0.0 if job was already stopped).
        """
        from dra.database import Database
        from dra.repositories.jobs import JobNotFoundError, JobsRepository

        db = Database()
        repo = JobsRepository(db)
        try:
            job = repo.find_job_by_image_id(container_id)
        except JobNotFoundError:
            return 0.0

        reserved = 0.0
        reserved_cores = 0.0
        target_machine_id: str | None = None
        rr = getattr(job, "resource_requirements", None)
        if isinstance(rr, dict):
            raw = rr.get("memory_gb")
            if isinstance(raw, (int, float)):
                reserved = float(raw)
            raw_cores = rr.get("cpu_cores")
            if isinstance(raw_cores, (int, float)):
                reserved_cores = float(raw_cores)
            raw_mid = rr.get("machine_id")
            if isinstance(raw_mid, str) and raw_mid.strip():
                target_machine_id = raw_mid.strip()

        was_running = False
        try:
            was_running = repo.update_job_status_if_running(job.id)
        except Exception:
            logger.exception("Failed to conditionally stop job for container_id=%s", container_id)

        if was_running and reserved > 0:
            self._apply_capacity_delta(delta_gb=reserved, machine_id=target_machine_id)
        if was_running and reserved_cores > 0:
            self._apply_cores_delta(delta_cores=reserved_cores, machine_id=target_machine_id)
        return reserved if was_running else 0.0

    def _watch_container(self, container_id: str) -> None:
        """Poll until the container exits, then auto-release reserved capacity."""
        poll_interval = float(os.environ.get("DRA_CONTAINER_POLL_INTERVAL_SECONDS", "30"))
        while True:
            time.sleep(poll_interval)
            is_running = self._container_is_running(container_id)

            if not is_running:
                logger.info(
                    "Container exited, auto-releasing capacity: container_id=%s", container_id
                )
                self._record_job_stopped_and_release(container_id=container_id)
                break

    def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats to DB so the scheduler can detect stale machines."""
        from dra.database import Database
        from dra.repositories.machines import MachineRepository, MachineRepositoryError

        interval = float(os.environ.get("DRA_HEARTBEAT_INTERVAL_SECONDS", "30"))
        sync_interval = float(os.environ.get("DRA_STOP_SYNC_INTERVAL_SECONDS", str(interval)))
        last_sync_at = 0.0
        repo = MachineRepository(Database())
        while True:
            time.sleep(interval)
            try:
                repo.record_heartbeat(self._machine_id)
            except MachineRepositoryError:
                logger.exception("Heartbeat failed for machine_id=%s", self._machine_id)
            except Exception:
                logger.exception("Unexpected heartbeat error for machine_id=%s", self._machine_id)

            now = time.monotonic()
            if now - last_sync_at < sync_interval:
                continue
            last_sync_at = now
            try:
                self._sync_running_jobs_with_docker()
            except Exception:
                logger.exception("Unexpected stop-sync error for machine_id=%s", self._machine_id)

    def _sync_running_jobs_with_docker(self) -> int:
        from dra.database import Database
        from dra.repositories.jobs import JobsRepository

        if not self._machine_id:
            return 0

        repo = JobsRepository(Database())
        running_jobs = repo.list_running_jobs()
        synced = 0
        for job in running_jobs:
            rr = self._resource_requirements_obj(getattr(job, "resource_requirements", None))
            machine_id = (rr.get("machine_id") or "").strip() if isinstance(rr.get("machine_id"), str) else ""
            if machine_id != self._machine_id:
                continue

            container_id = (getattr(job, "image_id", "") or "").strip()
            if not container_id:
                continue

            if self._container_is_running(container_id):
                continue

            released = self._record_job_stopped_and_release(container_id=container_id)
            synced += 1
            logger.info(
                "Synced stopped container from host state: container_id=%s machine_id=%s released_gb=%.2f",
                container_id,
                self._machine_id,
                float(released),
            )
        return synced

    def _container_is_running(self, container_id: str) -> bool:
        try:
            result = subprocess.run(
                ["docker", "inspect", "--format={{.State.Running}}", container_id],
                capture_output=True,
                text=True,
                timeout=self.IMAGE_CHECK_TIMEOUT_SECONDS,
            )
            return result.returncode == 0 and result.stdout.strip() == "true"
        except subprocess.TimeoutExpired:
            logger.warning("docker inspect timed out for container_id=%s", container_id)
            return True
        except Exception:
            logger.exception("Unexpected error inspecting container_id=%s", container_id)
            return False

    @staticmethod
    def _resource_requirements_obj(value: object) -> dict:
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value.strip():
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return {}
            if isinstance(parsed, dict):
                return parsed
        return {}

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

    def get_container_metrics(self, container_id: str) -> tuple[float, float]:
        """Per-container CPU% and memory in GB via ``docker stats --no-stream``.

        Returns ``(0.0, 0.0)`` on any failure — these are reporting metrics, not
        bookkeeping, so a transient failure shouldn't fail the deploy.
        """

        try:
            result = subprocess.run(
                [
                    "docker", "stats", "--no-stream",
                    "--format", "{{.CPUPerc}};{{.MemUsage}}",
                    container_id,
                ],
                capture_output=True,
                text=True,
                timeout=self.IMAGE_CHECK_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired:
            logger.warning("docker stats timed out for container_id=%s", container_id)
            return 0.0, 0.0
        except Exception:
            logger.exception("docker stats failed for container_id=%s", container_id)
            return 0.0, 0.0

        if result.returncode != 0:
            logger.warning(
                "docker stats returned %d for container_id=%s: %s",
                result.returncode,
                container_id,
                (result.stderr or "").strip(),
            )
            return 0.0, 0.0

        text = (result.stdout or "").strip()
        if not text or ";" not in text:
            return 0.0, 0.0
        cpu_part, mem_part = text.split(";", 1)
        cpu = self._parse_percent(cpu_part)
        mem = self._parse_memory_to_gb(mem_part.split(" / ", 1)[0])
        logger.info(
            "Container metrics: container_id=%s cpu=%.2f mem_gb=%.4f",
            container_id,
            cpu,
            mem,
        )
        return cpu, mem

    @staticmethod
    def _parse_percent(value: str) -> float:
        text = (value or "").strip().rstrip("%").strip()
        try:
            return float(text)
        except ValueError:
            return 0.0

    @staticmethod
    def _parse_memory_to_gb(value: str) -> float:
        text = (value or "").strip()
        multipliers = {
            "TiB": 1024 ** 4,
            "GiB": 1024 ** 3,
            "MiB": 1024 ** 2,
            "KiB": 1024,
            "TB": 1000 ** 4,
            "GB": 1000 ** 3,
            "MB": 1000 ** 2,
            "KB": 1000,
            "B": 1,
        }
        for unit, mult in multipliers.items():
            if text.endswith(unit):
                num_text = text[: -len(unit)].strip()
                try:
                    return float(num_text) * mult / (1024 ** 3)
                except ValueError:
                    return 0.0
        return 0.0

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
