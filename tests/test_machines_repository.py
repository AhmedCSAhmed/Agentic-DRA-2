from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from sqlalchemy.exc import SQLAlchemyError

from dra.models import MachineModelORM
from dra.repositories.machines import (
    InconsistentMachineAvailabilityError,
    InvalidMachineDataError,
    MachineNotFoundError,
    MachineRepository,
    MachineRepositoryDatabaseError,
)


class MachineRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db = MagicMock()
        self.session = MagicMock()
        self.db.start_session.return_value = self.session
        self.repository = MachineRepository(self.db)

    def test_create_machine_sets_timestamps_and_persists(self) -> None:
        result = self.repository.create_machine(
            machine_id="node-1",
            machine_name="gpu-a100-01",
            machine_type="gpu",
        )

        self.assertIsInstance(result, MachineModelORM)
        self.assertEqual(result.machine_id, "node-1")
        self.assertEqual(result.machine_name, "gpu-a100-01")
        self.assertEqual(result.machine_type, "gpu")
        self.assertIsNotNone(result.machine_created_at)
        self.assertEqual(result.machine_created_at, result.machine_updated_at)
        self.session.add.assert_called_once_with(result)
        self.session.commit.assert_called_once()
        self.session.close.assert_called_once()

    def test_find_machine_by_id_raises_when_missing(self) -> None:
        self.session.query.return_value.filter.return_value.first.return_value = None

        with self.assertRaises(MachineNotFoundError):
            self.repository.find_machine_by_id("node-missing")

    def test_find_machine_by_name_returns_exact_match(self) -> None:
        machine = MachineModelORM(
            machine_id="node-1",
            machine_name="worker-1",
            machine_type="cpu",
            machine_created_at=datetime.now(timezone.utc),
            machine_updated_at=datetime.now(timezone.utc),
        )
        self.session.query.return_value.filter.return_value.first.return_value = machine

        result = self.repository.find_machine_by_name(" worker-1 ")

        self.assertEqual(result.machine_id, "node-1")
        self.assertEqual(result.machine_name, "worker-1")

    def test_update_machine_metadata_updates_timestamp(self) -> None:
        earlier = datetime.now(timezone.utc) - timedelta(minutes=5)
        machine = MachineModelORM(
            machine_id="node-2",
            machine_name="worker-old",
            machine_type="cpu",
            machine_created_at=earlier,
            machine_updated_at=earlier,
        )
        self.session.query.return_value.filter.return_value.first.return_value = machine

        result = self.repository.update_machine_metadata(
            "node-2", machine_name="worker-new"
        )

        self.assertEqual(result.machine_name, "worker-new")
        self.assertEqual(result.machine_type, "cpu")
        self.assertGreater(result.machine_updated_at, earlier)
        self.session.commit.assert_called_once()

    def test_update_machine_availability_updates_value_and_timestamp(self) -> None:
        earlier = datetime.now(timezone.utc) - timedelta(minutes=5)
        machine = MachineModelORM(
            machine_id="node-2",
            machine_name="worker-old",
            machine_type="cpu",
            machine_created_at=earlier,
            machine_updated_at=earlier,
            available_gb=0.0,
        )
        self.session.query.return_value.filter.return_value.first.return_value = machine

        result = self.repository.update_machine_availability(
            "node-2",
            available_gb=7.75,
        )

        self.assertEqual(result.available_gb, 7.75)
        self.assertGreater(result.machine_updated_at, earlier)
        self.session.commit.assert_called_once()

    def test_delete_machine_raises_when_missing(self) -> None:
        self.session.query.return_value.filter.return_value.first.return_value = None

        with self.assertRaises(MachineNotFoundError):
            self.repository.delete_machine("node-missing")

    def test_select_best_machine_by_available_gb_returns_highest(self) -> None:
        m1 = MachineModelORM(
            machine_id="node-1",
            machine_name="worker-1",
            machine_type="cpu",
            machine_created_at=datetime.now(timezone.utc),
            machine_updated_at=datetime.now(timezone.utc),
        )
        m2 = MachineModelORM(
            machine_id="node-2",
            machine_name="worker-2",
            machine_type="cpu",
            machine_created_at=datetime.now(timezone.utc),
            machine_updated_at=datetime.now(timezone.utc),
        )
        self.session.query.return_value.filter.return_value.all.return_value = [m1, m2]

        availability = {"node-1": 128.0, "node-2": 256.0, "node-3": 999.0}
        best = self.repository.select_best_machine_by_available_gb(availability)

        self.assertEqual(best.machine_id, "node-2")

    def test_filter_machines_by_minimum_available_gb(self) -> None:
        m1 = MachineModelORM(
            machine_id="node-1",
            machine_name="worker-1",
            machine_type="cpu",
            machine_created_at=datetime.now(timezone.utc),
            machine_updated_at=datetime.now(timezone.utc),
        )
        m2 = MachineModelORM(
            machine_id="node-2",
            machine_name="worker-2",
            machine_type="cpu",
            machine_created_at=datetime.now(timezone.utc),
            machine_updated_at=datetime.now(timezone.utc),
        )
        self.session.query.return_value.filter.return_value.all.return_value = [m1, m2]

        matches = self.repository.filter_machines_by_minimum_available_gb(
            {"node-1": 60.0, "node-2": 25.0},
            minimum_required_gb=50.0,
        )

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].machine.machine_id, "node-1")
        self.assertEqual(matches[0].available_gb, 60.0)

    def test_select_best_machine_raises_for_no_db_overlap(self) -> None:
        self.session.query.return_value.filter.return_value.all.return_value = []

        with self.assertRaises(InconsistentMachineAvailabilityError):
            self.repository.select_best_machine_by_available_gb(
                {"unknown-node": 100.0}
            )

    def test_create_machine_wraps_database_failures(self) -> None:
        self.session.commit.side_effect = SQLAlchemyError("write failed")

        with self.assertRaises(MachineRepositoryDatabaseError):
            self.repository.create_machine(
                machine_id="node-3",
                machine_name="gpu-3",
                machine_type="gpu",
            )

        self.session.rollback.assert_called_once()
        self.session.close.assert_called_once()

    def test_select_best_machine_validates_availability_input(self) -> None:
        with self.assertRaises(InvalidMachineDataError):
            self.repository.select_best_machine_by_available_gb({"node-1": -1})

    def test_scheduler_usage_example(self) -> None:
        m1 = MachineModelORM(
            machine_id="gpu-1",
            machine_name="gpu-a100-01",
            machine_type="gpu",
            machine_created_at=datetime.now(timezone.utc),
            machine_updated_at=datetime.now(timezone.utc),
        )
        m2 = MachineModelORM(
            machine_id="gpu-2",
            machine_name="gpu-a100-02",
            machine_type="gpu",
            machine_created_at=datetime.now(timezone.utc),
            machine_updated_at=datetime.now(timezone.utc),
        )
        self.session.query.return_value.filter.return_value.filter.return_value.all.return_value = [
            m1,
            m2,
        ]

        runtime_gb = {"gpu-1": 42.0, "gpu-2": 88.0}
        eligible = self.repository.filter_machines_by_minimum_available_gb(
            runtime_gb,
            minimum_required_gb=40.0,
            machine_type="gpu",
        )
        chosen = self.repository.select_best_machine_by_available_gb(
            {item.machine.machine_id: item.available_gb for item in eligible},
            machine_type="gpu",
        )

        self.assertEqual(chosen.machine_id, "gpu-2")


if __name__ == "__main__":
    unittest.main()
