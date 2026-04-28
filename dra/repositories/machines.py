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
        dra_grpc_target: str | None = None,
    ) -> MachineModelORM:
        self._validate_machine_id(machine_id)
        self._validate_machine_name(machine_name)
        self._validate_machine_type(machine_type)
        if dra_grpc_target is not None:
            self._validate_dra_grpc_target(dra_grpc_target)

        now = self._now()
        machine = MachineModelORM(
            machine_id=machine_id,
            machine_name=machine_name,
            machine_type=machine_type,
            machine_created_at=now,
            machine_updated_at=now,
            dra_grpc_target=dra_grpc_target.strip() if dra_grpc_target else None,
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

    def find_machine_by_name(self, machine_name: str) -> MachineModelORM:
        self._validate_machine_name(machine_name)

        normalized_name = machine_name.strip()
        session = self._db.start_session()
        try:
            machine = (
                session.query(MachineModelORM)
                .filter(MachineModelORM.machine_name == normalized_name)
                .first()
            )
            if machine is None:
                raise MachineNotFoundError(
                    f"Machine with machine_name '{normalized_name}' was not found"
                )
            return machine
        except SQLAlchemyError as exc:
            raise MachineRepositoryDatabaseError(
                f"Failed to find machine by name '{normalized_name}'"
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
        dra_grpc_target: str | None = None,
    ) -> MachineModelORM:
        self._validate_machine_id(machine_id)
        if machine_name is None and machine_type is None and dra_grpc_target is None:
            raise InvalidMachineDataError(
                "At least one field must be provided to update machine metadata"
            )
        if machine_name is not None:
            self._validate_machine_name(machine_name)
        if machine_type is not None:
            self._validate_machine_type(machine_type)
        if dra_grpc_target is not None:
            self._validate_dra_grpc_target(dra_grpc_target)

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
            if dra_grpc_target is not None:
                setattr(machine, "dra_grpc_target", dra_grpc_target.strip())
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

    def update_machine_availability(
        self,
        machine_id: str,
        *,
        available_gb: int | float,
    ) -> MachineModelORM:
        self._validate_machine_id(machine_id)
        normalized_available_gb = self._normalize_available_gb_value(
            machine_id, available_gb
        )

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

            setattr(machine, "available_gb", normalized_available_gb)
            setattr(machine, "machine_updated_at", self._now())
            session.commit()
            return machine
        except SQLAlchemyError as exc:
            session.rollback()
            raise MachineRepositoryDatabaseError(
                f"Failed to update availability for machine '{machine_id}'"
            ) from exc
        finally:
            session.close()

    def increment_machine_availability(
        self,
        machine_id: str,
        *,
        delta_gb: int | float,
        floor_at_zero: bool = True,
    ) -> MachineModelORM:
        """Atomically adjust ``machines.available_gb`` by ``delta_gb``.

        This avoids read-modify-write races when multiple deployments update the same machine.
        """

        self._validate_machine_id(machine_id)
        if isinstance(delta_gb, bool) or not isinstance(delta_gb, (int, float)):
            raise InvalidMachineDataError("delta_gb must be numeric")
        if not isfinite(float(delta_gb)):
            raise InvalidMachineDataError("delta_gb must be finite")

        session = self._db.start_session()
        session.expire_on_commit = False
        try:
            machine = (
                session.query(MachineModelORM)
                .filter(MachineModelORM.machine_id == machine_id)
                .with_for_update()
                .first()
            )
            if machine is None:
                raise MachineNotFoundError(
                    f"Machine with machine_id '{machine_id}' was not found"
                )

            current = float(getattr(machine, "available_gb", 0.0) or 0.0)
            new_val = current + float(delta_gb)
            if floor_at_zero and new_val < 0:
                new_val = 0.0

            setattr(machine, "available_gb", float(new_val))
            setattr(machine, "machine_updated_at", self._now())
            session.commit()
            return machine
        except SQLAlchemyError as exc:
            session.rollback()
            raise MachineRepositoryDatabaseError(
                f"Failed to increment availability for machine '{machine_id}'"
            ) from exc
        finally:
            session.close()

    def increment_machine_cores(
        self,
        machine_id: str,
        *,
        delta_cores: int | float,
        floor_at_zero: bool = True,
    ) -> MachineModelORM:
        """Atomically adjust ``machines.available_cores`` by ``delta_cores``."""

        self._validate_machine_id(machine_id)
        if isinstance(delta_cores, bool) or not isinstance(delta_cores, (int, float)):
            raise InvalidMachineDataError("delta_cores must be numeric")
        if not isfinite(float(delta_cores)):
            raise InvalidMachineDataError("delta_cores must be finite")

        session = self._db.start_session()
        session.expire_on_commit = False
        try:
            machine = (
                session.query(MachineModelORM)
                .filter(MachineModelORM.machine_id == machine_id)
                .with_for_update()
                .first()
            )
            if machine is None:
                raise MachineNotFoundError(
                    f"Machine with machine_id '{machine_id}' was not found"
                )

            current = float(getattr(machine, "available_cores", 0.0) or 0.0)
            new_val = current + float(delta_cores)
            if floor_at_zero and new_val < 0:
                new_val = 0.0

            setattr(machine, "available_cores", float(new_val))
            setattr(machine, "machine_updated_at", self._now())
            session.commit()
            return machine
        except SQLAlchemyError as exc:
            session.rollback()
            raise MachineRepositoryDatabaseError(
                f"Failed to increment cores for machine '{machine_id}'"
            ) from exc
        finally:
            session.close()

    def record_heartbeat(self, machine_id: str) -> None:
        """Update last_heartbeat_at timestamp for a machine."""

        self._validate_machine_id(machine_id)
        now = self._now()
        session = self._db.start_session()
        try:
            rows_updated = (
                session.query(MachineModelORM)
                .filter(MachineModelORM.machine_id == machine_id)
                .update(
                    {"last_heartbeat_at": now, "machine_updated_at": now},
                    synchronize_session=False,
                )
            )
            session.commit()
            if rows_updated == 0:
                raise MachineNotFoundError(
                    f"Machine with machine_id '{machine_id}' was not found"
                )
        except SQLAlchemyError as exc:
            session.rollback()
            raise MachineRepositoryDatabaseError(
                f"Failed to record heartbeat for machine '{machine_id}'"
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
    def _validate_dra_grpc_target(value: str) -> None:
        if not value or not value.strip():
            raise InvalidMachineDataError("dra_grpc_target cannot be empty")
        text = value.strip()

        # Support both IPv4/hostname (host:port) and IPv6 bracket notation ([::1]:port).
        if text.startswith("["):
            # IPv6: [addr]:port
            bracket_end = text.find("]")
            if bracket_end == -1 or bracket_end + 1 >= len(text) or text[bracket_end + 1] != ":":
                raise InvalidMachineDataError(
                    "dra_grpc_target IPv6 must be [addr]:port (e.g. [fd7a::1]:50051)"
                )
            host = text[1:bracket_end]
            port_str = text[bracket_end + 2:]
        else:
            if text.count(":") != 1:
                raise InvalidMachineDataError(
                    "dra_grpc_target must be host:port (e.g. 10.0.0.5:50051 or [fd7a::1]:50051)"
                )
            host, port_str = text.split(":", 1)

        if not host or not port_str.isdigit():
            raise InvalidMachineDataError(
                "dra_grpc_target must be host:port (e.g. 10.0.0.5:50051 or [fd7a::1]:50051)"
            )
        port = int(port_str)
        if not 1 <= port <= 65535:
            raise InvalidMachineDataError("dra_grpc_target port must be between 1 and 65535")

    @staticmethod
    def _normalize_availability_map(
        available_gb_by_machine_id: Mapping[str, int | float],
    ) -> dict[str, float]:
        if not available_gb_by_machine_id:
            raise InvalidMachineDataError("available_gb_by_machine_id cannot be empty")

        normalized: dict[str, float] = {}
        for machine_id, available_gb in available_gb_by_machine_id.items():
            normalized[machine_id] = MachineRepository._normalize_available_gb_value(
                machine_id, available_gb
            )
        return normalized

    @staticmethod
    def _normalize_available_gb_value(
        machine_id: str,
        available_gb: int | float,
    ) -> float:
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
        return availability_value
