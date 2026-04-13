from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from dotenv import load_dotenv

import os


class Database:
    def __init__(self) -> None:
        load_dotenv()
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL must be set for the database connection")
        self.engine = create_engine(database_url, pool_pre_ping=True)
        self._session_factory = sessionmaker(bind=self.engine)

    def start_session(self) -> Session:
        return self._session_factory()
