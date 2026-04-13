from datetime import datetime
from typing import Any

from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class JobModelORM(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True)
    image_id = Column(String, nullable=False)
    resource_requirements = Column[Any](JSONB, nullable=False)
    image_name = Column(String, nullable=False)
    status = Column[str](String, nullable=False)
    created_at = Column[datetime](DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    
    
class MachineModelORM(Base):
    __tablename__ = "machines"
    machine_id = Column(String, primary_key=True)
    machine_name = Column(String, nullable=False)
    machine_type = Column(String, nullable=False)
    machine_created_at = Column(DateTime, nullable=False)
    machine_updated_at = Column(DateTime, nullable=False)
    dra_grpc_target = Column(String, nullable=True)

    