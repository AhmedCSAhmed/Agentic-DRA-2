from __future__ import annotations

import ipaddress
import os

from .contracts import MachineCandidate, ResourceRequirements, SchedulerDecision


def grpc_target_is_loopback(target: str) -> bool:
    """True when the gRPC host is loopback (local DRA), e.g. ``127.0.0.1:50051``."""

    raw = (target or "").strip()
    if not raw:
        return True
    host = raw.rsplit(":", 1)[0].strip()
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    lowered = host.lower()
    if lowered == "localhost":
        return True
    try:
        return ipaddress.ip_address(lowered).is_loopback
    except ValueError:
        return False


def select_best_machine(
    candidates: list[MachineCandidate],
    requirements: ResourceRequirements,
    *,
    machine_type: str | None = None,
    prefer_non_loopback: bool | None = None,
) -> SchedulerDecision:
    ordered_candidates, eligible_count, rejected_reasons = rank_eligible_machines(
        candidates,
        requirements,
        machine_type=machine_type,
        prefer_non_loopback=prefer_non_loopback,
    )
    scanned = len(candidates)
    best_machine = ordered_candidates[0] if ordered_candidates else None

    if best_machine is not None:
        return SchedulerDecision(
            selected=best_machine,
            scanned=scanned,
            eligible=eligible_count,
            reject_reasons=rejected_reasons,
        )

    return SchedulerDecision(
        selected=None,
        scanned=scanned,
        eligible=0,
        reject_reasons=rejected_reasons,
    )


def rank_eligible_machines(
    candidates: list[MachineCandidate],
    requirements: ResourceRequirements,
    *,
    machine_type: str | None = None,
    prefer_non_loopback: bool | None = None,
) -> tuple[list[MachineCandidate], int, dict[str, int]]:
    rejected_reasons: dict[str, int] = {}
    eligible_candidates: list[MachineCandidate] = []

    for candidate in candidates:
        if machine_type is not None and candidate.machine_type != machine_type:
            rejected_reasons["machine_type_mismatch"] = (
                rejected_reasons.get("machine_type_mismatch", 0) + 1
            )
            continue
        if candidate.available_gb >= requirements.memory_gb:
            eligible_candidates.append(candidate)
        else:
            rejected_reasons["insufficient_memory"] = (
                rejected_reasons.get("insufficient_memory", 0) + 1
            )

    if prefer_non_loopback is None:
        prefer_non_loopback = os.environ.get(
            "DRA_SCHEDULER_PREFER_REMOTE", "1"
        ).lower() not in ("0", "false", "no")

    pool = eligible_candidates
    if prefer_non_loopback and eligible_candidates:
        remote_ok = [c for c in eligible_candidates if not grpc_target_is_loopback(c.grpc_target)]
        if remote_ok:
            pool = remote_ok

    ordered_candidates = sorted(
        pool,
        key=lambda machine: (-machine.available_gb, machine.machine_id),
    )
    return ordered_candidates, len(eligible_candidates), rejected_reasons
