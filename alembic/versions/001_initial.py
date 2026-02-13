"""Create sidecar tables + pgvector extension

Revision ID: 001_initial
Revises: None
Create Date: 2026-02-12

NOTE: ForeignKey constraints to feed_entries and flash_point are NOT
created because those tables are date-partitioned (e.g. feed_entries_20251103).
The sidecar tables reference entries by UUID value only.

This migration uses raw SQL for all DDL to work reliably through
Supabase's pgBouncer (transaction-mode pooling).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence

revision: str = "001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # All DDL as raw SQL — safe through pgBouncer transaction pooling.
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # ── Enums ─────────────────────────────────────────
    op.execute("""
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'run_status') THEN
                CREATE TYPE run_status AS ENUM (
                    'pending', 'running', 'completed', 'failed', 'partial'
                );
            END IF;
        END $$
    """)
    op.execute("""
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_status') THEN
                CREATE TYPE job_status AS ENUM (
                    'queued', 'fetching', 'extracted', 'deduped', 'embedded',
                    'clustered', 'summarized', 'scored', 'failed', 'skipped_duplicate'
                );
            END IF;
        END $$
    """)
    op.execute("""
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'failure_reason') THEN
                CREATE TYPE failure_reason AS ENUM (
                    'blocked', 'js_required', 'paywall', 'consent',
                    'no_text', 'timeout', 'http_error', 'unknown'
                );
            END IF;
        END $$
    """)

    # ── processing_runs ───────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS processing_runs (
            id              BIGSERIAL PRIMARY KEY,
            run_id          VARCHAR(64) NOT NULL UNIQUE,
            status          run_status NOT NULL DEFAULT 'pending',
            pipeline_tier   VARCHAR(1) NOT NULL DEFAULT 'A',
            target_date     VARCHAR(10),
            total_entries       INTEGER DEFAULT 0,
            processed_entries   INTEGER DEFAULT 0,
            failed_entries      INTEGER DEFAULT 0,
            dedupe_skipped      INTEGER DEFAULT 0,
            clusters_created    INTEGER DEFAULT 0,
            started_at      TIMESTAMPTZ,
            completed_at    TIMESTAMPTZ,
            error_message   TEXT,
            metrics         JSONB DEFAULT '{}'::jsonb,
            created_at      TIMESTAMPTZ DEFAULT now()
        )
    """)

    # ── feed_entry_jobs ───────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS feed_entry_jobs (
            id                  BIGSERIAL PRIMARY KEY,
            feed_entry_id       UUID NOT NULL,
            run_id              VARCHAR(64) NOT NULL,
            status              job_status NOT NULL DEFAULT 'queued',
            attempts            INTEGER DEFAULT 0,
            last_error          TEXT,
            failure_reason      failure_reason,
            extraction_method   VARCHAR(32),
            extraction_chars    INTEGER,
            content_hash        VARCHAR(64),
            simhash             VARCHAR(32),
            is_duplicate        BOOLEAN DEFAULT FALSE,
            duplicate_of        UUID,
            fetch_duration_ms   INTEGER,
            extract_duration_ms INTEGER,
            embed_duration_ms   INTEGER,
            created_at          TIMESTAMPTZ DEFAULT now(),
            updated_at          TIMESTAMPTZ DEFAULT now(),
            CONSTRAINT uq_job_entry_run UNIQUE (feed_entry_id, run_id)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_feed_entry_jobs_run_id ON feed_entry_jobs (run_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_feed_entry_jobs_content_hash ON feed_entry_jobs (content_hash)")

    # ── feed_entry_vectors (pgvector) ─────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS feed_entry_vectors (
            feed_entry_id   UUID PRIMARY KEY,
            embedding       vector(384) NOT NULL,
            model_name      VARCHAR(64) NOT NULL,
            created_at      TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_feed_entry_vectors_embedding_hnsw "
        "ON feed_entry_vectors USING hnsw (embedding vector_cosine_ops) "
        "WITH (m = 16, ef_construction = 64)"
    )

    # ── feed_entry_topics ─────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS feed_entry_topics (
            id              BIGSERIAL PRIMARY KEY,
            feed_entry_id   UUID NOT NULL,
            iptc_top_level  VARCHAR(128) NOT NULL,
            iptc_path       VARCHAR(512) NOT NULL,
            confidence      REAL NOT NULL,
            created_at      TIMESTAMPTZ DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_feed_entry_topics_feed_entry_id ON feed_entry_topics (feed_entry_id)")

    # ── cluster_members ───────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS cluster_members (
            id              BIGSERIAL PRIMARY KEY,
            flashpoint_id   UUID NOT NULL,
            cluster_uuid    UUID NOT NULL,
            feed_entry_id   UUID NOT NULL,
            run_id          VARCHAR(64) NOT NULL,
            similarity      REAL DEFAULT 1.0,
            created_at      TIMESTAMPTZ DEFAULT now(),
            CONSTRAINT uq_cluster_member_entry_run UNIQUE (feed_entry_id, run_id)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_cluster_members_flashpoint_id ON cluster_members (flashpoint_id)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS cluster_members")
    op.execute("DROP TABLE IF EXISTS feed_entry_topics")
    op.execute("DROP TABLE IF EXISTS feed_entry_vectors")
    op.execute("DROP TABLE IF EXISTS feed_entry_jobs")
    op.execute("DROP TABLE IF EXISTS processing_runs")
    op.execute("DROP TYPE IF EXISTS failure_reason")
    op.execute("DROP TYPE IF EXISTS job_status")
    op.execute("DROP TYPE IF EXISTS run_status")
