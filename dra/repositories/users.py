"""Repository for deployment user credentials."""

from __future__ import annotations

import hashlib
import hmac
import os
from datetime import datetime, timezone

from sqlalchemy.exc import SQLAlchemyError

from dra.database import Database
from dra.models import DeploymentUserORM


class UsersRepositoryError(Exception):
    """Base repository exception."""


class UsersRepositoryDatabaseError(UsersRepositoryError):
    """Raised when a database operation fails."""


class UsersRepository:
    def __init__(self, db: Database) -> None:
        self._db = db

    def create_or_update_user(self, *, username: str, password: str) -> DeploymentUserORM:
        normalized_username = self._normalize_username(username)
        self._validate_password(password)

        session = self._db.start_session()
        session.expire_on_commit = False
        try:
            row = (
                session.query(DeploymentUserORM)
                .filter(DeploymentUserORM.username == normalized_username)
                .first()
            )
            now = self._now()
            if row is None:
                row = DeploymentUserORM(
                    username=normalized_username,
                    password_hash=self._hash_password(password),
                    created_at=now,
                    updated_at=now,
                )
                session.add(row)
            else:
                row.password_hash = self._hash_password(password)
                row.updated_at = now
            session.commit()
            return row
        except SQLAlchemyError as exc:
            session.rollback()
            raise UsersRepositoryDatabaseError("Failed to upsert deployment user") from exc
        finally:
            session.close()

    def find_user_by_username(self, username: str) -> DeploymentUserORM | None:
        normalized_username = self._normalize_username(username)

        session = self._db.start_session()
        try:
            return (
                session.query(DeploymentUserORM)
                .filter(DeploymentUserORM.username == normalized_username)
                .first()
            )
        except SQLAlchemyError as exc:
            raise UsersRepositoryDatabaseError("Failed to find deployment user") from exc
        finally:
            session.close()

    def verify_password(self, *, username: str, password: str) -> tuple[bool, DeploymentUserORM | None]:
        self._validate_password(password)
        user = self.find_user_by_username(username)
        if user is None:
            return False, None
        ok = self._verify_hash(password, user.password_hash)
        return ok, user

    @staticmethod
    def _normalize_username(username: str) -> str:
        normalized = (username or "").strip()
        if not normalized:
            raise UsersRepositoryError("username is required")
        return normalized

    @staticmethod
    def _validate_password(password: str) -> None:
        if not isinstance(password, str) or len(password.strip()) < 4:
            raise UsersRepositoryError("password must be at least 4 characters")

    @staticmethod
    def _hash_password(password: str) -> str:
        iterations = 200_000
        salt = os.urandom(16)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
        return f"pbkdf2_sha256${iterations}${salt.hex()}${digest.hex()}"

    @staticmethod
    def _verify_hash(password: str, encoded_hash: str) -> bool:
        try:
            algorithm, iter_text, salt_hex, digest_hex = encoded_hash.split("$", 3)
            if algorithm != "pbkdf2_sha256":
                return False
            iterations = int(iter_text)
            salt = bytes.fromhex(salt_hex)
            expected = bytes.fromhex(digest_hex)
        except Exception:
            return False

        actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
        return hmac.compare_digest(actual, expected)

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)
