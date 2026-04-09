from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


class Database:
    def __init__(self) -> None:
        self.engine = create_engine(
            'postgresql+psycopg2://postgres:postgres@localhost:5432/machines_db'
        )
        self._session_factory = sessionmaker(bind=self.engine)

    def start_session(self) -> Session:
        return self._session_factory()