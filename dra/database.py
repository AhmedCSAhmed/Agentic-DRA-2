import os

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker


class Database:
    def __init__(self) -> None:
        url = os.environ.get(
            "DATABASE_URL",
            "postgresql+psycopg://postgres:postgres@localhost:5432/machines_db",
        ).strip()
        # Supabase (and some providers) expose postgres:// URLs; SQLAlchemy expects postgresql://
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://") :]

        self.engine: Engine = create_engine(url, pool_pre_ping=True)
        self._session_factory = sessionmaker(bind=self.engine)

    def start_session(self) -> Session:
        return self._session_factory()
