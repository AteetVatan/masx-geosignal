"""
SQLAlchemy ORM models for pipeline sidecar tables.

The existing date-partitioned tables (feed_entries_YYYYMMDD,
flash_point_YYYYMMDD, news_clusters_YYYYMMDD) are queried via raw SQL
through the table_resolver module — they are NOT mapped here.

Only the NEW sidecar tables are defined as ORM models and managed
by Alembic migrations. These tables use UUIDs that reference entries
in the date-partitioned tables by value (no FK constraint to avoid
cross-table dependency issues).
"""

from __future__ import annotations

import enum
import uuid
from datetime import datetime

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all models."""

    type_annotation_map = {
        dict: JSONB,
        list: JSONB,
    }


# ╔══════════════════════════════════════════════════════════╗
# ║  SIDECAR TABLES — managed by Alembic migrations         ║
# ╚══════════════════════════════════════════════════════════╝


class RunStatus(str, enum.Enum):
    """Processing run states."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


class ProcessingRun(Base):
    """Tracks each daily pipeline run."""

    __tablename__ = "processing_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    status: Mapped[RunStatus] = mapped_column(
        Enum(RunStatus, name="run_status", create_constraint=True),
        default=RunStatus.PENDING,
        nullable=False,
    )
    pipeline_tier: Mapped[str] = mapped_column(String(1), nullable=False, default="A")
    target_date: Mapped[str | None] = mapped_column(
        String(10), nullable=True,
        comment="YYYY-MM-DD date of the feed tables being processed",
    )
    total_entries: Mapped[int] = mapped_column(Integer, default=0)
    processed_entries: Mapped[int] = mapped_column(Integer, default=0)
    failed_entries: Mapped[int] = mapped_column(Integer, default=0)
    dedupe_skipped: Mapped[int] = mapped_column(Integer, default=0)
    clusters_created: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[str | None] = mapped_column(Text)
    metrics: Mapped[dict | None] = mapped_column(JSONB, server_default=text("'{}'::jsonb"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class JobStatus(str, enum.Enum):
    """Per-entry job states — forms the state machine."""

    QUEUED = "queued"
    FETCHING = "fetching"
    EXTRACTED = "extracted"
    DEDUPED = "deduped"
    EMBEDDED = "embedded"
    CLUSTERED = "clustered"
    SUMMARIZED = "summarized"
    SCORED = "scored"
    FAILED = "failed"
    SKIPPED_DUPLICATE = "skipped_duplicate"


class FailureReason(str, enum.Enum):
    """Categorized failure reasons for failed extractions."""

    BLOCKED = "blocked"
    JS_REQUIRED = "js_required"
    PAYWALL = "paywall"
    CONSENT = "consent"
    NO_TEXT = "no_text"
    TIMEOUT = "timeout"
    HTTP_ERROR = "http_error"
    UNKNOWN = "unknown"


class FeedEntryJob(Base):
    """Per-entry processing state for a given run.

    References feed_entries by UUID value (no FK to date-partitioned tables).
    """

    __tablename__ = "feed_entry_jobs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    feed_entry_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False,
        comment="References an entry in a date-partitioned feed_entries_YYYYMMDD table",
    )
    run_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, name="job_status", create_constraint=True),
        default=JobStatus.QUEUED,
        nullable=False,
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)
    failure_reason: Mapped[FailureReason | None] = mapped_column(
        Enum(FailureReason, name="failure_reason", create_constraint=True),
        nullable=True,
    )
    extraction_method: Mapped[str | None] = mapped_column(String(32))
    extraction_chars: Mapped[int | None] = mapped_column(Integer)
    content_hash: Mapped[str | None] = mapped_column(String(64), index=True)
    simhash: Mapped[str | None] = mapped_column(String(32))
    is_duplicate: Mapped[bool] = mapped_column(default=False)
    duplicate_of: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    fetch_duration_ms: Mapped[int | None] = mapped_column(Integer)
    extract_duration_ms: Mapped[int | None] = mapped_column(Integer)
    embed_duration_ms: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        UniqueConstraint("feed_entry_id", "run_id", name="uq_job_entry_run"),
    )


class FeedEntryVector(Base):
    """Embedding vectors stored separately (pgvector)."""

    __tablename__ = "feed_entry_vectors"

    feed_entry_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        comment="References an entry in a date-partitioned feed_entries_YYYYMMDD table",
    )
    embedding: Mapped[list[float]] = mapped_column(Vector(384), nullable=False)
    model_name: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class FeedEntryTopic(Base):
    """IPTC Media Topic classifications per entry."""

    __tablename__ = "feed_entry_topics"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    feed_entry_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, index=True,
        comment="References an entry in a date-partitioned feed_entries_YYYYMMDD table",
    )
    iptc_top_level: Mapped[str] = mapped_column(String(128), nullable=False)
    iptc_path: Mapped[str] = mapped_column(String(512), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class ClusterMember(Base):
    """Links feed_entries to internal clusters within a flashpoint."""

    __tablename__ = "cluster_members"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    flashpoint_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, index=True,
        comment="References a flashpoint in a date-partitioned flash_point_YYYYMMDD table",
    )
    cluster_uuid: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, default=uuid.uuid4
    )
    feed_entry_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False,
        comment="References an entry in a date-partitioned feed_entries_YYYYMMDD table",
    )
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    similarity: Mapped[float] = mapped_column(Float, default=1.0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("feed_entry_id", "run_id", name="uq_cluster_member_entry_run"),
    )
