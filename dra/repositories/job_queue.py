from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from dra.database import Database
from dra.models import JobQueueORM, SchedulerDecisionORM


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class JobQueueRepository:
    def __init__(self, db: Database) -> None:
        self._db = db

    def enqueue(
        self,
        *,
        image_name: str,
        resource_requirements: dict[str, Any],
        machine_type: str | None = None,
        command: str | None = None,
        restart_policy: str | None = None,
    ) -> JobQueueORM:
        now = _now()
        job = JobQueueORM(
            image_name=image_name,
            resource_requirements=resource_requirements,
            machine_type=machine_type,
            command=command,
            restart_policy=restart_policy,
            status="PENDING",
            created_at=now,
            updated_at=now,
        )
        session = self._db.start_session()
        session.expire_on_commit = False
        try:
            session.add(job)
            session.commit()
            return job
        finally:
            session.close()

    def list_pending_and_ready(self) -> list[JobQueueORM]:
        now = _now()
        session = self._db.start_session()
        session.expire_on_commit = False
        try:
            rows = (
                session.query(JobQueueORM)
                .filter(
                    (JobQueueORM.status == "PENDING")
                    | (
                        (JobQueueORM.status == "DELAYED")
                        & (JobQueueORM.scheduled_for <= now)
                    )
                )
                .order_by(JobQueueORM.created_at)
                .all()
            )
            session.expunge_all()
            return rows
        finally:
            session.close()

    def find_by_id(self, job_id: int) -> JobQueueORM:
        session = self._db.start_session()
        session.expire_on_commit = False
        try:
            job = session.query(JobQueueORM).filter(JobQueueORM.id == job_id).one()
            session.expunge(job)
            return job
        finally:
            session.close()

    def mark_dispatched(
        self,
        job_id: int,
        *,
        machine_id: str,
        container_id: str,
        reason: str,
        mode: str,
    ) -> None:
        session = self._db.start_session()
        try:
            session.query(JobQueueORM).filter(JobQueueORM.id == job_id).update(
                {
                    "status": "DISPATCHED",
                    "machine_id": machine_id,
                    "container_id": container_id,
                    "decision_reason": reason,
                    "decision_mode": mode,
                    "updated_at": _now(),
                },
                synchronize_session=False,
            )
            session.commit()
        finally:
            session.close()

    def mark_delayed(self, job_id: int, delay_seconds: int, reason: str, mode: str) -> None:
        scheduled_for = _now() + timedelta(seconds=delay_seconds)
        session = self._db.start_session()
        try:
            session.query(JobQueueORM).filter(JobQueueORM.id == job_id).update(
                {
                    "status": "DELAYED",
                    "scheduled_for": scheduled_for,
                    "decision_reason": reason,
                    "decision_mode": mode,
                    "updated_at": _now(),
                },
                synchronize_session=False,
            )
            session.commit()
        finally:
            session.close()

    def mark_batched(
        self,
        job_ids: list[int],
        *,
        batch_id: str,
        machine_id: str,
        reason: str,
        mode: str,
    ) -> None:
        session = self._db.start_session()
        try:
            session.query(JobQueueORM).filter(JobQueueORM.id.in_(job_ids)).update(
                {
                    "status": "BATCHED",
                    "batch_id": batch_id,
                    "machine_id": machine_id,
                    "decision_reason": reason,
                    "decision_mode": mode,
                    "updated_at": _now(),
                },
                synchronize_session=False,
            )
            session.commit()
        finally:
            session.close()

    def mark_failed(self, job_id: int, error_message: str, mode: str) -> None:
        session = self._db.start_session()
        try:
            session.query(JobQueueORM).filter(JobQueueORM.id == job_id).update(
                {
                    "status": "FAILED",
                    "error_message": error_message,
                    "decision_mode": mode,
                    "updated_at": _now(),
                },
                synchronize_session=False,
            )
            session.commit()
        finally:
            session.close()

    def seconds_until_next_delayed(self) -> float | None:
        now = _now()
        session = self._db.start_session()
        try:
            row = (
                session.query(JobQueueORM.scheduled_for)
                .filter(JobQueueORM.status == "DELAYED")
                .order_by(JobQueueORM.scheduled_for)
                .first()
            )
        finally:
            session.close()
        if row is None or row[0] is None:
            return None
        delta = (row[0] - now).total_seconds()
        return max(delta, 0.0)

    def save_decision(
        self,
        *,
        job_queue_ids: list[int],
        action: str,
        machine_id: str | None,
        delay_seconds: int | None,
        batch_id: str | None,
        reason: str,
        mode: str,
    ) -> None:
        rec = SchedulerDecisionORM(
            job_queue_ids=job_queue_ids,
            action=action,
            machine_id=machine_id,
            delay_seconds=delay_seconds,
            batch_id=batch_id,
            reason=reason,
            mode=mode,
            decided_at=_now(),
        )
        session = self._db.start_session()
        try:
            session.add(rec)
            session.commit()
        finally:
            session.close()
