from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class ErrorCode(str, Enum):
    INVALID_INPUT = "INVALID_INPUT"
    NO_CAPACITY = "NO_CAPACITY"
    MACHINE_CONFIG_ERROR = "MACHINE_CONFIG_ERROR"
    GRPC_UNAVAILABLE = "GRPC_UNAVAILABLE"
    REMOTE_EXECUTION_FAILED = "REMOTE_EXECUTION_FAILED"


class ResourceRequirements(BaseModel):
    memory_gb: float = Field(gt=0)
    cpu_cores: float | None = Field(default=None, gt=0)


class DeployRequest(BaseModel):
    image_name: str = Field(min_length=1)
    resource_requirements: ResourceRequirements
    command: str | None = None
    restart_policy: str | None = None
    machine_type: str | None = None
    request_id: str | None = None


class SelectedMachine(BaseModel):
    machine_id: str
    machine_type: str
    grpc_target: str


class ContainerInfo(BaseModel):
    container_id: str
    workload_state: str


class RuntimeMetrics(BaseModel):
    cpu_used: float
    memory_gb_used: float


class DeploySuccessResponse(BaseModel):
    status: Literal["DEPLOYED"]
    request_id: str
    selected_machine: SelectedMachine
    container: ContainerInfo
    metrics: RuntimeMetrics
    message: str


class DeployErrorResponse(BaseModel):
    status: Literal["FAILED"]
    request_id: str
    error_code: ErrorCode
    message: str
    retryable: bool
    details: dict[str, Any] = Field(default_factory=dict)

class MachineCandidate(BaseModel):
    machine_id: str
    machine_type: str
    grpc_target: str
    available_gb: float
    available_cores: float = 8.0
    last_heartbeat_at: datetime | None = None

class SchedulerDecision(BaseModel):
    selected: MachineCandidate | None
    scanned: int
    eligible: int
    reject_reasons: dict[str, int] = Field(default_factory=dict)

def make_no_capacity_error(
    *,
    request_id: str,
    requested: ResourceRequirements,
    scanned: int,
    eligible: int,
    reject_reasons: dict[str, int] | None = None,
    retry_hint: str = "Retry in 1-2 minutes or lower resource requirements",
) -> DeployErrorResponse:
    requested_payload = (
        requested.model_dump()
        if hasattr(requested, "model_dump")
        else requested.dict()  # pragma: no cover - pydantic v1 fallback
    )
    return DeployErrorResponse(
        status="FAILED",
        request_id=request_id,
        error_code=ErrorCode.NO_CAPACITY,
        message="No machine currently satisfies requested resources",
        retryable=True,
        details={
            "requested": requested_payload,
            "considered_machines": {"scanned": scanned, "eligible": eligible},
            "reject_reasons": reject_reasons or {},
            "retry_hint": retry_hint,
        },
    )
