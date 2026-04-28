"""Repository for persisting and querying job placement metadata."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from dra.database import Database
from dra.models import JobModelORM


class JobsRepositoryError(Exception):
    """Base repository exception."""


class JobsRepositoryDatabaseError(JobsRepositoryError):
    """Raised when a database operation fails."""


class JobNotFoundError(JobsRepositoryError):
    """Raised when a job does not exist."""


class InvalidJobDataError(JobsRepositoryError):
    """Raised when invalid data is provided for a job mutation."""


class JobsRepository:
    def __init__(self, db: Database) -> None:
        self._db = db

    def create_job(
        self,
        *,
        image_id: str,
        username: str | None = None,
        user_id: int | None = None,
        resource_requirements: Any,
        image_name: str,
        status: str,
    ) -> JobModelORM:
        normalized_username = self._normalize_optional_username(username)
        self._validate_create_payload(
            image_id=image_id,
            username=normalized_username,
            user_id=user_id,
            resource_requirements=resource_requirements,
            image_name=image_name,
            status=status,
        )

        now = self._now()
        new_job = JobModelORM(
            image_id=image_id,
            username=normalized_username,
            user_id=user_id,
            resource_requirements=resource_requirements,
            image_name=image_name,
            status=status,
            created_at=now,
            updated_at=now,
        )

        session = self._db.start_session()
        session.expire_on_commit = False
        try:
            session.add(new_job)
            session.commit()
            return new_job
        except SQLAlchemyError as exc:
            session.rollback()
            raise JobsRepositoryDatabaseError("Failed to create job") from exc
        finally:
            session.close()

    def find_job_by_id(self, job_id: int) -> JobModelORM:
        if job_id <= 0:
            raise InvalidJobDataError("job_id must be a positive integer")

        session = self._db.start_session()
        try:
            job = session.query(JobModelORM).filter(JobModelORM.id == job_id).first()
            if job is None:
                raise JobNotFoundError(f"Job with id '{job_id}' was not found")
            return job
        except SQLAlchemyError as exc:
            raise JobsRepositoryDatabaseError(f"Failed to find job by id '{job_id}'") from exc
        finally:
            session.close()

    def find_job_by_image_id(self, image_id: str) -> JobModelORM:
        if not image_id or not image_id.strip():
            raise InvalidJobDataError("image_id is required")

        session = self._db.start_session()
        try:
            job = (
                session.query(JobModelORM)
                .filter(JobModelORM.image_id == image_id.strip())
                .order_by(JobModelORM.created_at.desc())
                .first()
            )
            if job is None:
                raise JobNotFoundError(f"Job with image_id '{image_id}' was not found")
            return job
        except SQLAlchemyError as exc:
            raise JobsRepositoryDatabaseError(
                f"Failed to find job by image_id '{image_id}'"
            ) from exc
        finally:
            session.close()

    def find_job_by_image_name(self, image_name: str) -> JobModelORM:
        if not image_name or not image_name.strip():
            raise InvalidJobDataError("image_name is required")

        session = self._db.start_session()
        try:
            job = (
                session.query(JobModelORM)
                .filter(JobModelORM.image_name == image_name)
                .order_by(JobModelORM.created_at.desc())
                .first()
            )
            if job is None:
                raise JobNotFoundError(f"Job with image_name '{image_name}' was not found")
            return job
        except SQLAlchemyError as exc:
            raise JobsRepositoryDatabaseError(
                f"Failed to find job by image_name '{image_name}'"
            ) from exc
        finally:
            session.close()

    def find_jobs_by_status(self, status: str) -> list[JobModelORM]:
        if not status or not status.strip():
            raise InvalidJobDataError("status is required")

        session = self._db.start_session()
        try:
            return (
                session.query(JobModelORM)
                .filter(JobModelORM.status == status)
                .order_by(JobModelORM.created_at.asc())
                .all()
            )
        except SQLAlchemyError as exc:
            raise JobsRepositoryDatabaseError(
                f"Failed to list jobs by status '{status}'"
            ) from exc
        finally:
            session.close()

    def list_jobs(self) -> list[JobModelORM]:
        session = self._db.start_session()
        try:
            return session.query(JobModelORM).order_by(JobModelORM.created_at.asc()).all()
        except SQLAlchemyError as exc:
            raise JobsRepositoryDatabaseError("Failed to list jobs") from exc
        finally:
            session.close()

    def list_running_jobs(self, *, username: str | None = None) -> list[JobModelORM]:
        normalized_username = self._normalize_optional_username(username)

        session = self._db.start_session()
        try:
            query = session.query(JobModelORM).filter(JobModelORM.status == "RUNNING")
            if normalized_username is not None:
                query = query.filter(JobModelORM.username == normalized_username)
            return query.order_by(JobModelORM.created_at.desc()).all()
        except SQLAlchemyError as exc:
            raise JobsRepositoryDatabaseError("Failed to list running jobs") from exc
        finally:
            session.close()

    def update_job_status(self, job_id: int, new_status: str) -> JobModelORM:
        if job_id <= 0:
            raise InvalidJobDataError("job_id must be a positive integer")
        if not new_status or not new_status.strip():
            raise InvalidJobDataError("new_status is required")

        session = self._db.start_session()
        session.expire_on_commit = False
        try:
            job = session.query(JobModelORM).filter(JobModelORM.id == job_id).first()
            if job is None:
                raise JobNotFoundError(f"Job with id '{job_id}' was not found")

            setattr(job, "status", new_status)
            setattr(job, "updated_at", self._now())
            session.commit()
            return job
        except SQLAlchemyError as exc:
            session.rollback()
            raise JobsRepositoryDatabaseError(
                f"Failed to update status for job '{job_id}'"
            ) from exc
        finally:
            session.close()

    def update_job_status_if_running(self, job_id: int) -> bool:
        """Atomically set status='STOPPED' only when current status='RUNNING'.

        Returns True if the row was updated (prevents double capacity-release when
        StopContainer RPC and the container watcher race each other).
        """
        if job_id <= 0:
            raise InvalidJobDataError("job_id must be a positive integer")

        session = self._db.start_session()
        try:
            rows_updated = (
                session.query(JobModelORM)
                .filter(JobModelORM.id == job_id, JobModelORM.status == "RUNNING")
                .update(
                    {"status": "STOPPED", "updated_at": self._now()},
                    synchronize_session=False,
                )
            )
            session.commit()
            return rows_updated > 0
        except SQLAlchemyError as exc:
            session.rollback()
            raise JobsRepositoryDatabaseError(
                f"Failed to conditionally stop job '{job_id}'"
            ) from exc
        finally:
            session.close()

    def delete_job(self, job_id: int) -> None:
        if job_id <= 0:
            raise InvalidJobDataError("job_id must be a positive integer")

        session = self._db.start_session()
        try:
            job = session.query(JobModelORM).filter(JobModelORM.id == job_id).first()
            if job is None:
                raise JobNotFoundError(f"Job with id '{job_id}' was not found")

            session.delete(job)
            session.commit()
        except SQLAlchemyError as exc:
            session.rollback()
            raise JobsRepositoryDatabaseError(f"Failed to delete job '{job_id}'") from exc
        finally:
            session.close()

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _validate_create_payload(
        *,
        image_id: str,
        username: str | None,
        user_id: int | None,
        resource_requirements: Any,
        image_name: str,
        status: str,
    ) -> None:
        if not image_id or not image_id.strip():
            raise InvalidJobDataError("image_id is required")
        if username is not None and not username.strip():
            raise InvalidJobDataError("username cannot be blank")
        if user_id is not None and user_id <= 0:
            raise InvalidJobDataError("user_id must be a positive integer")
        if resource_requirements is None:
            raise InvalidJobDataError("resource_requirements is required")
        if not image_name or not image_name.strip():
            raise InvalidJobDataError("image_name is required")
        if not status or not status.strip():
            raise InvalidJobDataError("status is required")

    @staticmethod
    def _normalize_optional_username(username: str | None) -> str | None:
        if username is None:
            return None
        normalized = username.strip()
        return normalized if normalized else None
