from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from sqlalchemy.exc import SQLAlchemyError

from dra.models import JobModelORM
from dra.repositories.jobs import (
    InvalidJobDataError,
    JobNotFoundError,
    JobsRepository,
    JobsRepositoryDatabaseError,
)


class JobsRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db = MagicMock()
        self.session = MagicMock()
        self.db.start_session.return_value = self.session
        self.repository = JobsRepository(self.db)

    def test_create_job_sets_timestamps_and_persists(self) -> None:
        result = self.repository.create_job(
            image_id="img-1",
            username="ahmed",
            resource_requirements={"cpu": 2, "memory": "4Gi"},
            image_name="my-image:v1",
            status="pending",
        )

        self.assertIsInstance(result, JobModelORM)
        self.assertEqual(result.image_id, "img-1")
        self.assertEqual(result.username, "ahmed")
        self.assertEqual(result.image_name, "my-image:v1")
        self.assertEqual(result.status, "pending")
        self.assertIsNotNone(result.created_at)
        self.assertEqual(result.created_at, result.updated_at)
        self.session.add.assert_called_once_with(result)
        self.session.commit.assert_called_once()
        self.session.close.assert_called_once()

    def test_find_job_by_id_raises_when_missing(self) -> None:
        self.session.query.return_value.filter.return_value.first.return_value = None

        with self.assertRaises(JobNotFoundError):
            self.repository.find_job_by_id(404)

        self.session.close.assert_called_once()

    def test_find_job_by_image_name_returns_latest_job(self) -> None:
        job = JobModelORM(
            id=3,
            image_id="img-3",
            resource_requirements={"gpu": 1},
            image_name="trainer:latest",
            status="running",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        self.session.query.return_value.filter.return_value.order_by.return_value.first.return_value = (
            job
        )

        result = self.repository.find_job_by_image_name("trainer:latest")

        self.assertEqual(result.id, 3)
        self.assertEqual(result.status, "running")

    def test_update_job_status_updates_timestamp(self) -> None:
        earlier = datetime.now(timezone.utc) - timedelta(minutes=10)
        job = JobModelORM(
            id=22,
            image_id="img-22",
            resource_requirements={"cpu": 1},
            image_name="encoder:v2",
            status="pending",
            created_at=earlier,
            updated_at=earlier,
        )
        self.session.query.return_value.filter.return_value.first.return_value = job

        result = self.repository.update_job_status(22, "completed")

        self.assertEqual(result.status, "completed")
        self.assertGreater(result.updated_at, earlier)
        self.session.commit.assert_called_once()

    def test_delete_job_raises_when_missing(self) -> None:
        self.session.query.return_value.filter.return_value.first.return_value = None

        with self.assertRaises(JobNotFoundError):
            self.repository.delete_job(77)

    def test_create_job_wraps_database_failures(self) -> None:
        self.session.commit.side_effect = SQLAlchemyError("boom")

        with self.assertRaises(JobsRepositoryDatabaseError):
            self.repository.create_job(
                image_id="img-9",
                resource_requirements={"cpu": 2},
                image_name="service:v3",
                status="pending",
            )

        self.session.rollback.assert_called_once()
        self.session.close.assert_called_once()

    def test_create_job_validates_input(self) -> None:
        with self.assertRaises(InvalidJobDataError):
            self.repository.create_job(
                image_id="",
                resource_requirements={"cpu": 2},
                image_name="service:v3",
                status="pending",
            )

    def test_list_running_jobs_filters_by_username(self) -> None:
        running_job = JobModelORM(
            id=31,
            image_id="cid-31",
            username="alice",
            resource_requirements={"cpu": 1},
            image_name="svc:v1",
            status="RUNNING",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )

        first_filter = MagicMock()
        second_filter = MagicMock()
        ordered = MagicMock()

        self.session.query.return_value.filter.return_value = first_filter
        first_filter.filter.return_value = second_filter
        second_filter.order_by.return_value = ordered
        ordered.all.return_value = [running_job]

        result = self.repository.list_running_jobs(username="alice")

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].image_id, "cid-31")
        self.assertEqual(result[0].username, "alice")


if __name__ == "__main__":
    unittest.main()
