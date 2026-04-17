from uuid import uuid4
import shlex

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from agent.client import DRAGrpcClient
from dra.database import Database
from dra.repositories.machines import MachineRepository

from .contracts import DeployRequest, MachineCandidate, make_no_capacity_error
from .scheduler import select_best_machine

app = FastAPI()
machine_repository = MachineRepository(Database())


def _load_scheduler_candidates(machine_type: str | None) -> list[MachineCandidate]:
    rows = machine_repository.list_machines(machine_type=machine_type)
    candidates: list[MachineCandidate] = []
    for row in rows:
        grpc_target = (getattr(row, "dra_grpc_target", None) or "").strip()
        if not grpc_target:
            continue
        available_gb = getattr(row, "available_gb", None)
        if not isinstance(available_gb, (int, float)):
            continue
        candidates.append(
            MachineCandidate(
                machine_id=row.machine_id,
                machine_type=row.machine_type,
                grpc_target=grpc_target,
                available_gb=float(available_gb),
            )
        )
    return candidates

@app.post(f"/deploy")
async def deploy(request: DeployRequest, ):
    request_id = request.request_id or f"req-{uuid4().hex[:12]}"
    candidates = _load_scheduler_candidates(request.machine_type)
    decision = select_best_machine(candidates, request.resource_requirements)
    if decision.selected is None:
        error = make_no_capacity_error(
            request_id=request_id,
            requested=request.resource_requirements,
            scanned=decision.scanned,
            eligible=decision.eligible,
            reject_reasons=decision.reject_reasons,
        )
        return JSONResponse(status_code=409, content=error.model_dump())

    command = shlex.split(request.command) if request.command else None
    client = DRAGrpcClient()
    rpc_result = client.pull_and_run_image(
        request.image_name,
        command=command,
        restart_policy=request.restart_policy,
        grpc_target=decision.selected.grpc_target,
    )
    client.close()

    if rpc_result.get("rpc_error"):
        is_unavailable = rpc_result.get("code") in {"UNAVAILABLE", "DEADLINE_EXCEEDED"}
        error_code = "GRPC_UNAVAILABLE" if is_unavailable else "REMOTE_EXECUTION_FAILED"
        status_code = 503 if is_unavailable else 502
        return JSONResponse(
            status_code=status_code,
            content={
                "status": "FAILED",
                "request_id": request_id,
                "error_code": error_code,
                "message": rpc_result.get("details") or "Remote deployment call failed",
                "retryable": is_unavailable,
                "details": {
                    "grpc_target": decision.selected.grpc_target,
                    "rpc_code": rpc_result.get("code"),
                },
            },
        )

    if not rpc_result.get("success"):
        return JSONResponse(
            status_code=502,
            content={
                "status": "FAILED",
                "request_id": request_id,
                "error_code": "REMOTE_EXECUTION_FAILED",
                "message": rpc_result.get("message") or "Remote machine failed to run image",
                "retryable": False,
                "details": {
                    "grpc_target": decision.selected.grpc_target,
                },
            },
        )

    return {
        "status": "DEPLOYED",
        "request_id": request_id,
        "selected_machine": {
            "machine_id": decision.selected.machine_id,
            "machine_type": decision.selected.machine_type,
            "grpc_target": decision.selected.grpc_target,
        },
        "container": {
            "container_id": rpc_result.get("container_id", ""),
            "workload_state": rpc_result.get("workload_state", "RUNNING"),
        },
        "metrics": {
            "cpu_used": rpc_result.get("cpu_used", 0.0),
            "memory_gb_used": rpc_result.get("memory_gb_used", 0.0),
        },
        "message": rpc_result.get("message", "Image pulled and container started"),
    }
    
