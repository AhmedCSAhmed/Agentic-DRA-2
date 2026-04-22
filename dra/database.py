import os

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from dra.env import load_dotenv_for


class Database:
    def __init__(self) -> None:
        load_dotenv_for(__file__)
        url = os.environ.get(
            "DATABASE_URL",
            "postgresql+psycopg://postgres:postgres@localhost:5432/machines_db",
        ).strip()
        # Supabase (and some providers) expose postgres:// URLs; SQLAlchemy expects postgresql://
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://") :]
        # If the URL doesn't specify a driver, SQLAlchemy defaults to psycopg2.
        # This project depends on psycopg (v3), so prefer that driver implicitly.
        if url.startswith("postgresql://"):
            url = "postgresql+psycopg://" + url[len("postgresql://") :]

        self.engine: Engine = create_engine(url, pool_pre_ping=True)
        self._session_factory = sessionmaker(bind=self.engine)

    def start_session(self) -> Session:
        return self._session_factory()
