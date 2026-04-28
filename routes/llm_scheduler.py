from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from agents import Agent, Runner, function_tool

from dra.models import JobQueueORM
from routes.contracts import MachineCandidate

logger = logging.getLogger(__name__)

SCHEDULER_INSTRUCTIONS = """
You are a workload scheduler for a distributed container cluster.

For every job in pending_jobs call make_scheduling_decisions exactly once with ALL decisions.

Decision types:
  dispatch  - run now on machine_id (must be in the job's eligible_machine_ids list)
  delay     - wait delay_seconds before retrying (max 300)
              Use when: machines >85% utilised, or 3+ identical jobs just ran on a machine
  batch     - group job_ids (same image preferred) onto one machine_id (max 5 per batch)
              Only batch when all jobs in the group share at least one eligible machine

Guidelines:
- Prefer machines with more available_gb when cores are otherwise equal
- Prefer machines with more available_cores when memory is otherwise equal
- If a job has queued_seconds_ago > 900 (15 min), dispatch it regardless of utilisation
- Never assign a machine that is not in a job's eligible_machine_ids list
- Call make_scheduling_decisions once with the complete list of decisions
"""


@dataclass
class SchedulingDecisionItem:
    action: str                          # dispatch | delay | batch
    job_queue_id: int
    machine_id: str | None = None
    delay_seconds: int | None = None
    batch_id: str | None = None
    batch_with_job_ids: list[int] = field(default_factory=list)
    reason: str = ""


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


async def evaluate_with_llm(
    jobs: list[JobQueueORM],
    eligible_per_job: dict[int, list[MachineCandidate]],
) -> list[SchedulingDecisionItem]:
    """Call the LLM scheduler agent; returns a decision for every job."""

    context = {
        "pending_jobs": [
            {
                "job_queue_id": j.id,
                "image_name": j.image_name,
                "resource_requirements": j.resource_requirements,
                "queued_seconds_ago": (_now_utc() - _ensure_utc(j.created_at)).total_seconds(),
                "eligible_machines": [
                    {
                        "machine_id": m.machine_id,
                        "available_gb": m.available_gb,
                        "available_cores": m.available_cores,
                    }
                    for m in eligible_per_job[j.id]
                ],
            }
            for j in jobs
        ],
        "time_utc": _now_utc().isoformat(),
    }

    captured: list[SchedulingDecisionItem] = []

    @function_tool
    def make_scheduling_decisions(decisions: list[dict]) -> str:  # type: ignore[type-arg]
        """Submit all scheduling decisions at once.

        Each decision must have:
          action: 'dispatch' | 'delay' | 'batch'
          job_queue_id: int
          machine_id: str (required for dispatch/batch)
          delay_seconds: int (required for delay, max 300)
          batch_id: str (required for batch, shared UUID for grouped jobs)
          batch_with_job_ids: list[int] (other job_queue_ids in same batch)
          reason: str
        """
        for raw in decisions:
            try:
                item = SchedulingDecisionItem(
                    action=raw.get("action", ""),
                    job_queue_id=int(raw["job_queue_id"]),
                    machine_id=raw.get("machine_id"),
                    delay_seconds=raw.get("delay_seconds"),
                    batch_id=raw.get("batch_id"),
                    batch_with_job_ids=raw.get("batch_with_job_ids") or [],
                    reason=raw.get("reason", ""),
                )
                captured.append(item)
            except Exception:
                logger.warning("Skipped malformed decision: %r", raw)
        return json.dumps({"accepted": len(captured)})

    agent = Agent(
        name="DRA Scheduler",
        instructions=SCHEDULER_INSTRUCTIONS,
        tools=[make_scheduling_decisions],
    )

    await Runner.run(agent, json.dumps(context))
    return captured


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt
