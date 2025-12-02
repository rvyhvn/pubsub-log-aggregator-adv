from sqlalchemy import Column, String, DateTime, JSON, Integer, UniqueConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime
from pydantic import BaseModel, Field, validator
from typing import Dict, Any, Optional
import re

Base = declarative_base()


class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    topic = Column(String(255), nullable=False, index=True)
    event_id = Column(String(255), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    source = Column(String(255), nullable=False)
    payload = Column(JSON, nullable=False)
    processed_at = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("topic", "event_id", name="uq_topic_event_id"),
        Index("idx_topic_timestamp", "topic", "timestamp"),
    )


class EventStats(Base):
    __tablename__ = "event_stats"

    id = Column(Integer, primary_key=True)
    received = Column(Integer, default=0, nullable=False)
    unique_processed = Column(Integer, default=0, nullable=False)
    duplicate_dropped = Column(Integer, default=0, nullable=False)
    last_updated = Column(DateTime, server_default=func.now(), onupdate=func.now())


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_topic = Column(String(255), nullable=False)
    event_id = Column(String(255), nullable=False)
    action = Column(String(50), nullable=False)  # 'processed', 'duplicate', 'error'
    details = Column(JSON, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_audit_created", "created_at"),
        Index("idx_audit_action", "action"),
    )


# Pydantic Models


class EventPayload(BaseModel):

    class Config:
        extra = "allow"


class Event(BaseModel):

    topic: str = Field(..., min_length=1, max_length=255, description="Event topic")
    event_id: str = Field(
        ..., min_length=1, max_length=255, description="Unique event ID"
    )
    timestamp: str = Field(..., description="ISO8601 timestamp")
    source: str = Field(..., min_length=1, max_length=255, description="Event source")
    payload: Dict[str, Any] = Field(..., description="Event payload data")

    @validator("topic")
    def validate_topic(cls, v):
        if not re.match(r"^[a-zA-Z0-9._-]+$", v):
            raise ValueError("Topic harus alphanumeric dengan ._- saja")
        return v

    @validator("event_id")
    def validate_event_id(cls, v):
        if not v.strip():
            raise ValueError("event_id tidak boleh kosong")
        return v

    @validator("timestamp")
    def validate_timestamp(cls, v):
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("timestamp harus format ISO8601")
        return v

    class Config:
        schema_extra = {
            "example": {
                "topic": "user.login",
                "event_id": "evt_1234567890",
                "timestamp": "2025-12-02T10:30:00Z",
                "source": "auth-service",
                "payload": {"user_id": "user_123", "ip": "192.168.1.1"},
            }
        }


class EventBatch(BaseModel):

    events: list[Event] = Field(..., min_items=1, max_items=1000)


class EventResponse(BaseModel):

    topic: str
    event_id: str
    timestamp: str
    processed_at: str
    source: str
    payload: Dict[str, Any]


class StatsResponse(BaseModel):

    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: int
    uptime_seconds: float
    last_updated: Optional[str] = None


class PublishResponse(BaseModel):

    status: str
    accepted: int
    message: str
