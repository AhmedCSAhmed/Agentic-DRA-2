from __future__ import annotations

from uuid import uuid4

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from scheduled_deploy import execute_scheduled_deploy

from .contracts import DeployRequest, make_no_capacity_error

router = APIRouter()


@router.post("/deploy")
async def deploy(request: DeployRequest):
    request_id = request.request_id or f"req-{uuid4().hex[:12]}"
    decision, rpc_result = await execute_scheduled_deploy(
        image_name=request.image_name,
        resource_requirements=request.resource_requirements,
        machine_type=request.machine_type,
        command=request.command,
        restart_policy=request.restart_policy,
    )
    if decision.selected is None:
        error = make_no_capacity_error(
            request_id=request_id,
            requested=request.resource_requirements,
            scanned=decision.scanned,
            eligible=decision.eligible,
            reject_reasons=decision.reject_reasons,
        )
        return JSONResponse(status_code=409, content=error.model_dump())

    assert rpc_result is not None

    if rpc_result.get("error"):
        return JSONResponse(
            status_code=502,
            content={
                "status": "FAILED",
                "request_id": request_id,
                "error_code": "REMOTE_EXECUTION_FAILED",
                "message": rpc_result.get("message") or "Tool invocation failed",
                "retryable": False,
                "details": {
                    "grpc_target": decision.selected.grpc_target,
                },
            },
        )

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
