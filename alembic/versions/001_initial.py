"""Create sidecar tables + pgvector extension

Revision ID: 001_initial
Revises: None
Create Date: 2026-02-12

NOTE: ForeignKey constraints to feed_entries and flash_point are NOT
created because those tables are date-partitioned (e.g. feed_entries_20251103).
The sidecar tables reference entries by UUID value only.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID


revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Enable pgvector extension
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # ── Enums ─────────────────────────────────────────
    run_status = sa.Enum(
        "pending", "running", "completed", "failed", "partial",
        name="run_status",
    )
    run_status.create(op.get_bind(), checkfirst=True)

    job_status = sa.Enum(
        "queued", "fetching", "extracted", "deduped", "embedded",
        "clustered", "summarized", "scored", "failed", "skipped_duplicate",
        name="job_status",
    )
    job_status.create(op.get_bind(), checkfirst=True)

    failure_reason = sa.Enum(
        "blocked", "js_required", "paywall", "consent", "no_text",
        "timeout", "http_error", "unknown",
        name="failure_reason",
    )
    failure_reason.create(op.get_bind(), checkfirst=True)

    # ── processing_runs ───────────────────────────────
    op.create_table(
        "processing_runs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("run_id", sa.String(64), unique=True, nullable=False),
        sa.Column("status", run_status, nullable=False, server_default="pending"),
        sa.Column("pipeline_tier", sa.String(1), nullable=False, server_default="A"),
        sa.Column("target_date", sa.String(10), nullable=True,
                  comment="YYYY-MM-DD date of the feed tables being processed"),
        sa.Column("total_entries", sa.Integer(), server_default="0"),
        sa.Column("processed_entries", sa.Integer(), server_default="0"),
        sa.Column("failed_entries", sa.Integer(), server_default="0"),
        sa.Column("dedupe_skipped", sa.Integer(), server_default="0"),
        sa.Column("clusters_created", sa.Integer(), server_default="0"),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("error_message", sa.Text()),
        sa.Column("metrics", JSONB, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # ── feed_entry_jobs ───────────────────────────────
    # NOTE: feed_entry_id references entries in date-partitioned tables
    # (e.g. feed_entries_20251103), so no FK constraint is created.
    op.create_table(
        "feed_entry_jobs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("feed_entry_id", UUID(as_uuid=True), nullable=False,
                  comment="References an entry in a date-partitioned feed_entries_YYYYMMDD table"),
        sa.Column("run_id", sa.String(64), nullable=False, index=True),
        sa.Column("status", job_status, nullable=False, server_default="queued"),
        sa.Column("attempts", sa.Integer(), server_default="0"),
        sa.Column("last_error", sa.Text()),
        sa.Column("failure_reason", failure_reason),
        sa.Column("extraction_method", sa.String(32)),
        sa.Column("extraction_chars", sa.Integer()),
        sa.Column("content_hash", sa.String(64), index=True),
        sa.Column("simhash", sa.String(32)),
        sa.Column("is_duplicate", sa.Boolean(), server_default="false"),
        sa.Column("duplicate_of", UUID(as_uuid=True)),
        sa.Column("fetch_duration_ms", sa.Integer()),
        sa.Column("extract_duration_ms", sa.Integer()),
        sa.Column("embed_duration_ms", sa.Integer()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("feed_entry_id", "run_id", name="uq_job_entry_run"),
    )

    # ── feed_entry_vectors (pgvector) ─────────────────
    # NOTE: feed_entry_id references entries in date-partitioned tables.
    op.create_table(
        "feed_entry_vectors",
        sa.Column("feed_entry_id", UUID(as_uuid=True), primary_key=True,
                  comment="References an entry in a date-partitioned feed_entries_YYYYMMDD table"),
        sa.Column("model_name", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    # Add pgvector column with raw SQL
    op.execute("ALTER TABLE feed_entry_vectors ADD COLUMN embedding vector(384) NOT NULL")

    # Create HNSW index for fast ANN queries
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_feed_entry_vectors_embedding_hnsw "
        "ON feed_entry_vectors USING hnsw (embedding vector_cosine_ops) "
        "WITH (m = 16, ef_construction = 64)"
    )

    # ── feed_entry_topics ─────────────────────────────
    op.create_table(
        "feed_entry_topics",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("feed_entry_id", UUID(as_uuid=True), nullable=False, index=True,
                  comment="References an entry in a date-partitioned feed_entries_YYYYMMDD table"),
        sa.Column("iptc_top_level", sa.String(128), nullable=False),
        sa.Column("iptc_path", sa.String(512), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # ── cluster_members ───────────────────────────────
    # NOTE: Both flashpoint_id and feed_entry_id reference entries in
    # date-partitioned tables. No FK constraints.
    op.create_table(
        "cluster_members",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("flashpoint_id", UUID(as_uuid=True), nullable=False, index=True,
                  comment="References a flashpoint in a date-partitioned flash_point_YYYYMMDD table"),
        sa.Column("cluster_uuid", UUID(as_uuid=True), nullable=False),
        sa.Column("feed_entry_id", UUID(as_uuid=True), nullable=False,
                  comment="References an entry in a date-partitioned feed_entries_YYYYMMDD table"),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("similarity", sa.Float(), server_default="1.0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("feed_entry_id", "run_id", name="uq_cluster_member_entry_run"),
    )


def downgrade() -> None:
    op.drop_table("cluster_members")
    op.drop_table("feed_entry_topics")
    op.drop_table("feed_entry_vectors")
    op.drop_table("feed_entry_jobs")
    op.drop_table("processing_runs")

    op.execute("DROP TYPE IF EXISTS failure_reason")
    op.execute("DROP TYPE IF EXISTS job_status")
    op.execute("DROP TYPE IF EXISTS run_status")
