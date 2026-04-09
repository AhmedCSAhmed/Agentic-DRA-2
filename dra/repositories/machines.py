"""Repository for machine metadata and resource-aware selection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from typing import Mapping

from sqlalchemy.exc import SQLAlchemyError

from dra.database import Database
from dra.models import MachineModelORM


class MachineRepositoryError(Exception):
    """Base repository exception for machine operations."""


class MachineRepositoryDatabaseError(MachineRepositoryError):
    """Raised when a machine DB operation fails."""


class MachineNotFoundError(MachineRepositoryError):
    """Raised when a machine does not exist in storage."""


class InvalidMachineDataError(MachineRepositoryError):
    """Raised when machine input data is invalid."""


class InconsistentMachineAvailabilityError(MachineRepositoryError):
    """Raised when runtime availability cannot be aligned with DB machines."""


@dataclass(frozen=True)
class MachineAvailability:
    """Dynamic availability for one machine known to the repository."""

    machine: MachineModelORM
    available_gb: float


class MachineRepository:
    def __init__(self, db: Database) -> None:
        self._db = db

    def create_machine(
        self,
        *,
        machine_id: str,
        machine_name: str,
        machine_type: str,
    ) -> MachineModelORM:
        self._validate_machine_id(machine_id)
        self._validate_machine_name(machine_name)
        self._validate_machine_type(machine_type)

        now = self._now()
        machine = MachineModelORM(
            machine_id=machine_id,
            machine_name=machine_name,
            machine_type=machine_type,
            machine_created_at=now,
            machine_updated_at=now,
        )

        session = self._db.start_session()
        session.expire_on_commit = False
        try:
            session.add(machine)
            session.commit()
            return machine
        except SQLAlchemyError as exc:
            session.rollback()
            raise MachineRepositoryDatabaseError("Failed to create machine") from exc
        finally:
            session.close()

    def find_machine_by_id(self, machine_id: str) -> MachineModelORM:
        self._validate_machine_id(machine_id)

        session = self._db.start_session()
        try:
            machine = (
                session.query(MachineModelORM)
                .filter(MachineModelORM.machine_id == machine_id)
                .first()
            )
            if machine is None:
                raise MachineNotFoundError(
                    f"Machine with machine_id '{machine_id}' was not found"
                )
            return machine
        except SQLAlchemyError as exc:
            raise MachineRepositoryDatabaseError(
                f"Failed to find machine '{machine_id}'"
            ) from exc
        finally:
            session.close()

    def list_machines(self, machine_type: str | None = None) -> list[MachineModelORM]:
        if machine_type is not None:
            self._validate_machine_type(machine_type)

        session = self._db.start_session()
        try:
            query = session.query(MachineModelORM)
            if machine_type is not None:
                query = query.filter(MachineModelORM.machine_type == machine_type)
            return query.order_by(MachineModelORM.machine_name.asc()).all()
        except SQLAlchemyError as exc:
            raise MachineRepositoryDatabaseError("Failed to list machines") from exc
        finally:
            session.close()

    def update_machine_metadata(
        self,
        machine_id: str,
        *,
        machine_name: str | None = None,
        machine_type: str | None = None,
    ) -> MachineModelORM:
        self._validate_machine_id(machine_id)
        if machine_name is None and machine_type is None:
            raise InvalidMachineDataError(
                "At least one field must be provided to update machine metadata"
            )
        if machine_name is not None:
            self._validate_machine_name(machine_name)
        if machine_type is not None:
            self._validate_machine_type(machine_type)

        session = self._db.start_session()
        session.expire_on_commit = False
        try:
            machine = (
                session.query(MachineModelORM)
                .filter(MachineModelORM.machine_id == machine_id)
                .first()
            )
            if machine is None:
                raise MachineNotFoundError(
                    f"Machine with machine_id '{machine_id}' was not found"
                )

            if machine_name is not None:
                setattr(machine, "machine_name", machine_name)
            if machine_type is not None:
                setattr(machine, "machine_type", machine_type)
            setattr(machine, "machine_updated_at", self._now())

            session.commit()
            return machine
        except SQLAlchemyError as exc:
            session.rollback()
            raise MachineRepositoryDatabaseError(
                f"Failed to update machine '{machine_id}'"
            ) from exc
        finally:
            session.close()

    def delete_machine(self, machine_id: str) -> None:
        self._validate_machine_id(machine_id)

        session = self._db.start_session()
        try:
            machine = (
                session.query(MachineModelORM)
                .filter(MachineModelORM.machine_id == machine_id)
                .first()
            )
            if machine is None:
                raise MachineNotFoundError(
                    f"Machine with machine_id '{machine_id}' was not found"
                )

            session.delete(machine)
            session.commit()
        except SQLAlchemyError as exc:
            session.rollback()
            raise MachineRepositoryDatabaseError(
                f"Failed to delete machine '{machine_id}'"
            ) from exc
        finally:
            session.close()

    def select_best_machine_by_available_gb(
        self,
        available_gb_by_machine_id: Mapping[str, int | float],
        *,
        machine_type: str | None = None,
    ) -> MachineModelORM:
        candidates = self.filter_machines_by_minimum_available_gb(
            available_gb_by_machine_id,
            minimum_required_gb=0,
            machine_type=machine_type,
        )
        if not candidates:
            raise InconsistentMachineAvailabilityError(
                "No machine from runtime availability exists in the DB"
            )
        return max(
            candidates,
            key=lambda candidate: (candidate.available_gb, candidate.machine.machine_id),
        ).machine

    def filter_machines_by_minimum_available_gb(
        self,
        available_gb_by_machine_id: Mapping[str, int | float],
        *,
        minimum_required_gb: int | float,
        machine_type: str | None = None,
    ) -> list[MachineAvailability]:
        if minimum_required_gb < 0:
            raise InvalidMachineDataError("minimum_required_gb must be >= 0")

        normalized_availability = self._normalize_availability_map(
            available_gb_by_machine_id
        )
        if machine_type is not None:
            self._validate_machine_type(machine_type)

        machine_ids = list(normalized_availability.keys())
        session = self._db.start_session()
        try:
            query = session.query(MachineModelORM).filter(
                MachineModelORM.machine_id.in_(machine_ids)
            )
            if machine_type is not None:
                query = query.filter(MachineModelORM.machine_type == machine_type)
            machines = query.all()
        except SQLAlchemyError as exc:
            raise MachineRepositoryDatabaseError(
                "Failed to resolve machines for availability map"
            ) from exc
        finally:
            session.close()

        if not machines:
            return []

        matches: list[MachineAvailability] = []
        for machine in machines:
            machine_id = getattr(machine, "machine_id")
            available_gb = normalized_availability[machine_id]
            if available_gb >= float(minimum_required_gb):
                matches.append(
                    MachineAvailability(machine=machine, available_gb=available_gb)
                )
        return matches

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _validate_machine_id(machine_id: str) -> None:
        if not machine_id or not machine_id.strip():
            raise InvalidMachineDataError("machine_id is required")

    @staticmethod
    def _validate_machine_name(machine_name: str) -> None:
        if not machine_name or not machine_name.strip():
            raise InvalidMachineDataError("machine_name is required")

    @staticmethod
    def _validate_machine_type(machine_type: str) -> None:
        if not machine_type or not machine_type.strip():
            raise InvalidMachineDataError("machine_type is required")

    @staticmethod
    def _normalize_availability_map(
        available_gb_by_machine_id: Mapping[str, int | float],
    ) -> dict[str, float]:
        if not available_gb_by_machine_id:
            raise InvalidMachineDataError("available_gb_by_machine_id cannot be empty")

        normalized: dict[str, float] = {}
        for machine_id, available_gb in available_gb_by_machine_id.items():
            if not machine_id or not machine_id.strip():
                raise InvalidMachineDataError("availability map contains invalid machine_id")
            if isinstance(available_gb, bool) or not isinstance(available_gb, (int, float)):
                raise InvalidMachineDataError(
                    f"available_gb for machine '{machine_id}' must be numeric"
                )
            availability_value = float(available_gb)
            if availability_value < 0:
                raise InvalidMachineDataError(
                    f"available_gb for machine '{machine_id}' must be >= 0"
                )
            if not isfinite(availability_value):
                raise InvalidMachineDataError(
                    f"available_gb for machine '{machine_id}' must be finite"
                )
            normalized[machine_id] = availability_value
        return normalized
