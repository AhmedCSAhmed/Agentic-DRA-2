from .contracts import MachineCandidate, ResourceRequirements, SchedulerDecision


def select_best_machine(
    candidates: list[MachineCandidate],
    requirements: ResourceRequirements,
    *,
    machine_type: str | None = None,
) -> SchedulerDecision:
    scanned = len(candidates)
    rejected_reasons: dict[str, int] = {}
    best_candidates: list[MachineCandidate] = []

    for candidate in candidates:
        if machine_type is not None and candidate.machine_type != machine_type:
            rejected_reasons["machine_type_mismatch"] = (
                rejected_reasons.get("machine_type_mismatch", 0) + 1
            )
            continue
        if candidate.available_gb >= requirements.memory_gb:
            best_candidates.append(candidate)
        else:
            rejected_reasons["insufficient_memory"] = (
                rejected_reasons.get("insufficient_memory", 0) + 1
            )

    best_machine = None
    if best_candidates:
        best_machine = min(
            best_candidates,
            key=lambda machine: (-machine.available_gb, machine.machine_id),
        )

    if best_machine is not None:
        return SchedulerDecision(
            selected=best_machine,
            scanned=scanned,
            eligible=len(best_candidates),
            reject_reasons=rejected_reasons,
        )

    return SchedulerDecision(
        selected=None,
        scanned=scanned,
        eligible=0,
        reject_reasons=rejected_reasons,
    )
