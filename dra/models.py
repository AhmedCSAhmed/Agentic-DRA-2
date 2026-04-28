from datetime import datetime
from typing import Any

from sqlalchemy import ARRAY, Column, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class JobModelORM(Base):
    __tablename__ = "jobs"
    id = Column[int](Integer, primary_key=True)
    image_id = Column[str](String, nullable=False)
    username = Column[str](String, nullable=True)
    user_id = Column[int](Integer, ForeignKey("deployment_users.id"), nullable=True)
    resource_requirements = Column[Any](JSONB, nullable=False)
    image_name = Column[str](String, nullable=False)
    status = Column[str](String, nullable=False)
    created_at = Column[datetime](DateTime, nullable=False)
    updated_at = Column[datetime](DateTime, nullable=False)
    
    
class MachineModelORM(Base):
    __tablename__ = "machines"
    machine_id = Column(String, primary_key=True)
    machine_name = Column(String, nullable=False)
    machine_type = Column(String, nullable=False)
    machine_created_at = Column(DateTime, nullable=False)
    machine_updated_at = Column(DateTime, nullable=False)
    dra_grpc_target = Column(String, nullable=True)
    available_gb = Column(Float, nullable=True)
    available_cores = Column(Float, nullable=True)
    last_heartbeat_at = Column(DateTime, nullable=True)


class DeploymentUserORM(Base):
    __tablename__ = "deployment_users"
    id = Column(Integer, primary_key=True)
    username = Column(String, nullable=False, unique=True)
    password_hash = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class JobQueueORM(Base):
    __tablename__ = "job_queue"
    id = Column(Integer, primary_key=True)
    image_name = Column(String, nullable=False)
    resource_requirements = Column(JSONB, nullable=False)
    machine_type = Column(String, nullable=True)
    command = Column(String, nullable=True)
    restart_policy = Column(String, nullable=True)
    status = Column(String, nullable=False, default="PENDING")
    scheduled_for = Column(DateTime, nullable=True)
    batch_id = Column(String, nullable=True)
    container_id = Column(String, nullable=True)
    machine_id = Column(String, nullable=True)
    decision_reason = Column(Text, nullable=True)
    decision_mode = Column(String, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)


class SchedulerDecisionORM(Base):
    __tablename__ = "scheduler_decisions"
    id = Column(Integer, primary_key=True)
    job_queue_ids = Column(ARRAY(Integer), nullable=False)
    action = Column(String, nullable=False)
    machine_id = Column(String, nullable=True)
    delay_seconds = Column(Integer, nullable=True)
    batch_id = Column(String, nullable=True)
    reason = Column(Text, nullable=False)
    mode = Column(String, nullable=False)
    decided_at = Column(DateTime, nullable=False)
