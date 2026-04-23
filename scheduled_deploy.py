"""Scheduler + remote gRPC deploy shared by the HTTP API and the Atlas CLI.

Avoids importing FastAPI so ``atlas`` can run without the API stack installed.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from agent.client import DRAGrpcClient
from agent.tools import invoke_pull_and_run_image_via_tool
from dra.database import Database
from dra.repositories.jobs import JobsRepository
from dra.repositories.machines import MachineRepository

from routes.contracts import MachineCandidate, ResourceRequirements, SchedulerDecision
from routes.scheduler import rank_eligible_machines, select_best_machine

_machine_repository = MachineRepository(Database())


def _load_scheduler_candidates(machine_type: str | None) -> list[MachineCandidate]:
    """Build scheduler candidates; missing ``available_gb`` uses env fallback so remotes are not dropped."""

    fallback_gb = float(os.environ.get("DRA_SCHEDULER_FALLBACK_AVAILABLE_GB", "64.0"))
    fallback_cores = float(os.environ.get("DRA_SCHEDULER_FALLBACK_AVAILABLE_CORES", "8.0"))
    rows = _machine_repository.list_machines(machine_type=machine_type)
    candidates: list[MachineCandidate] = []
    for row in rows:
        grpc_target = (getattr(row, "dra_grpc_target", None) or "").strip()
        if not grpc_target:
            continue
        raw_gb = getattr(row, "available_gb", None)
        available_gb = float(raw_gb) if isinstance(raw_gb, (int, float)) else fallback_gb
        raw_cores = getattr(row, "available_cores", None)
        available_cores = float(raw_cores) if isinstance(raw_cores, (int, float)) else fallback_cores
        raw_hb = getattr(row, "last_heartbeat_at", None)
        last_heartbeat_at = raw_hb if isinstance(raw_hb, datetime) else None
        candidates.append(
            MachineCandidate(
                machine_id=row.machine_id,
                machine_type=row.machine_type,
                grpc_target=grpc_target,
                available_gb=available_gb,
                available_cores=available_cores,
                last_heartbeat_at=last_heartbeat_at,
            )
        )
    return candidates


def run_deploy_scheduler(
    *,
    machine_type: str | None,
    resource_requirements: ResourceRequirements,
) -> SchedulerDecision:
    """Load machines from Postgres and pick the best host for ``resource_requirements``."""

    candidates = _load_scheduler_candidates(machine_type)
    return select_best_machine(
        candidates,
        resource_requirements,
        machine_type=machine_type,
    )


async def execute_scheduled_deploy(
    *,
    image_name: str,
    resource_requirements: ResourceRequirements,
    machine_type: str | None = None,
    command: str | None = None,
    restart_policy: str | None = None,
) -> tuple[SchedulerDecision, dict[str, Any] | None]:
    """Run the scheduler; if a machine is chosen, call ``pull_and_run_image`` on that host.

    Returns ``(decision, None)`` when no machine can satisfy the request; otherwise
    ``(decision, rpc_result)`` where ``rpc_result`` is the parsed tool/RPC payload.
    """

    candidates = _load_scheduler_candidates(machine_type)
    decision = select_best_machine(
        candidates,
        resource_requirements,
        machine_type=machine_type,
    )
    if decision.selected is None:
        return decision, None

    ordered_candidates, eligible_count, rejected_reasons = rank_eligible_machines(
        candidates,
        resource_requirements,
        machine_type=machine_type,
    )

    client = DRAGrpcClient()
    try:
        attempts: list[dict[str, Any]] = []
        for index, candidate in enumerate(ordered_candidates):
            rpc_result = await invoke_pull_and_run_image_via_tool(
                client=client,
                machine_repo=_machine_repository,
                image_name=image_name,
                machine_id=candidate.machine_id,
                command=command,
                restart_policy=restart_policy,
                memory_gb=resource_requirements.memory_gb,
                grpc_target=candidate.grpc_target,
            )
            attempts.append(_attempt_record(candidate, rpc_result))

            if _is_retryable_rpc_result(rpc_result) and index + 1 < len(ordered_candidates):
                continue

            final_result = dict(rpc_result)
            final_result["attempts"] = attempts

            # Ensure stop() can find this container later via jobs table even if the
            # remote DRA server isn't running with a DB-linked machine identity.
            if final_result.get("success") and final_result.get("container_id"):
                # DB accounting: reserve memory and cores on the selected machine.
                try:
                    req_gb = float(resource_requirements.memory_gb or 0.0)
                except Exception:
                    req_gb = 0.0
                if req_gb > 0:
                    try:
                        _machine_repository.increment_machine_availability(
                            candidate.machine_id, delta_gb=-req_gb
                        )
                    except Exception:
                        pass

                try:
                    req_cores = float(resource_requirements.cpu_cores or 0.0)
                except Exception:
                    req_cores = 0.0
                if req_cores > 0:
                    try:
                        _machine_repository.increment_machine_cores(
                            candidate.machine_id, delta_cores=-req_cores
                        )
                    except Exception:
                        pass

                try:
                    JobsRepository(Database()).create_job(
                        image_id=str(final_result.get("container_id")),
                        image_name=image_name,
                        status="RUNNING",
                        resource_requirements={
                            "memory_gb": float(resource_requirements.memory_gb),
                            "cpu_cores": float(resource_requirements.cpu_cores or 0.0),
                            "machine_id": candidate.machine_id,
                        },
                    )
                except Exception:
                    pass

            return (
                SchedulerDecision(
                    selected=candidate,
                    scanned=len(candidates),
                    eligible=eligible_count,
                    reject_reasons=rejected_reasons,
                ),
                final_result,
            )
    finally:
        client.close()

    return decision, None


def _is_retryable_rpc_result(rpc_result: dict[str, Any]) -> bool:
    return rpc_result.get("rpc_error") and rpc_result.get("code") in {
        "UNAVAILABLE",
        "DEADLINE_EXCEEDED",
    }


def _attempt_record(
    candidate: MachineCandidate,
    rpc_result: dict[str, Any],
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "machine_id": candidate.machine_id,
        "grpc_target": candidate.grpc_target,
    }
    if rpc_result.get("success"):
        record["status"] = "success"
        return record
    if rpc_result.get("rpc_error"):
        record["status"] = "rpc_error"
        record["code"] = rpc_result.get("code")
        record["details"] = rpc_result.get("details")
        return record
    if rpc_result.get("error"):
        record["status"] = "tool_error"
        record["message"] = rpc_result.get("message")
        return record
    record["status"] = "failed"
    record["message"] = rpc_result.get("message")
    return record
