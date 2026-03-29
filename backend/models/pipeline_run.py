"""
Enterprise ETL Platform - ORM Models

SQLAlchemy models reflecting the PostgreSQL schema.
"""
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import (
    BigInteger, Boolean, DateTime, Float, ForeignKey,
    Integer, Numeric, String, Text, func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.core.database import Base


class PipelineRun(Base):
    """Tracks each execution of an ETL pipeline."""

    __tablename__ = "pipeline_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4
    )
    pipeline_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    dag_id: Mapped[Optional[str]] = mapped_column(String(255))
    dag_run_id: Mapped[Optional[str]] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="pending", index=True)
    triggered_by: Mapped[str] = mapped_column(String(100), default="scheduler")
    config_snapshot: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)
    records_extracted: Mapped[int] = mapped_column(BigInteger, default=0)
    records_transformed: Mapped[int] = mapped_column(BigInteger, default=0)
    records_loaded: Mapped[int] = mapped_column(BigInteger, default=0)
    records_failed: Mapped[int] = mapped_column(BigInteger, default=0)
    quality_score: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    tasks: Mapped[list["PipelineTask"]] = relationship(
        "PipelineTask", back_populates="run", cascade="all, delete-orphan"
    )
    quality_metrics: Mapped[list["DataQualityMetric"]] = relationship(
        "DataQualityMetric", back_populates="run", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<PipelineRun {self.pipeline_name} [{self.status}] {self.run_id}>"


class PipelineTask(Base):
    """Tracks individual tasks within a pipeline run."""

    __tablename__ = "pipeline_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4
    )
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pipeline_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    task_name: Mapped[str] = mapped_column(String(255), nullable=False)
    task_type: Mapped[Optional[str]] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="pending")
    attempt_number: Mapped[int] = mapped_column(Integer, default=1)
    records_in: Mapped[int] = mapped_column(BigInteger, default=0)
    records_out: Mapped[int] = mapped_column(BigInteger, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    metadata_: Mapped[Optional[Dict[str, Any]]] = mapped_column("metadata", JSONB)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    run: Mapped["PipelineRun"] = relationship("PipelineRun", back_populates="tasks")

    def __repr__(self) -> str:
        return f"<PipelineTask {self.task_name} [{self.status}]>"


class DataQualityMetric(Base):
    """Stores data quality evaluation results per pipeline run."""

    __tablename__ = "data_quality_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pipeline_runs.run_id", ondelete="CASCADE"), nullable=False
    )
    pipeline_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    metric_name: Mapped[str] = mapped_column(String(255), nullable=False)
    metric_category: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    passed: Mapped[Optional[bool]] = mapped_column(Boolean)
    expected_value: Mapped[Optional[float]] = mapped_column(Numeric)
    actual_value: Mapped[Optional[float]] = mapped_column(Numeric)
    threshold: Mapped[Optional[float]] = mapped_column(Numeric)
    details: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)
    evaluated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    run: Mapped["PipelineRun"] = relationship("PipelineRun", back_populates="quality_metrics")

    def __repr__(self) -> str:
        return f"<DataQualityMetric {self.metric_name} passed={self.passed}>"


class DataSource(Base):
    """Registry of all registered data source connections."""

    __tablename__ = "data_sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    source_type: Mapped[str] = mapped_column(String(100), nullable=False)
    connection_config: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_tested_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_test_status: Mapped[Optional[str]] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self) -> str:
        return f"<DataSource {self.name} [{self.source_type}]>"


class ETLCheckpoint(Base):
    """Stores watermark values for incremental pipeline loads."""

    __tablename__ = "etl_checkpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_name: Mapped[str] = mapped_column(String(255), nullable=False)
    checkpoint_key: Mapped[str] = mapped_column(String(255), nullable=False)
    checkpoint_value: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self) -> str:
        return f"<ETLCheckpoint {self.pipeline_name}/{self.checkpoint_key}={self.checkpoint_value}>"
