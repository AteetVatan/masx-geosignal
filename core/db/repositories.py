"""
Database repository layer — async CRUD operations for all tables.

Adapted for date-partitioned Supabase tables:
  - feed_entries_YYYYMMDD  (existing, read + write enrichment fields)
  - flash_point_YYYYMMDD   (existing, read-only)
  - news_clusters_YYYYMMDD (output, write)

The sidecar tables (processing_runs, feed_entry_jobs, etc.) remain
ORM-managed. The date-partitioned tables use raw SQL via text().

Each repository method that touches partitioned tables requires a
`table_name` or `TableContext` parameter.

Enrichment fields written back to feed_entries_YYYYMMDD:
  title_en, images, hostname, content,
  summary, entities (NER), geo_entities
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog
from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.db.engine import retry_on_disconnect

logger = structlog.get_logger(__name__)

from core.db.models import (
    ClusterMember,
    FeedEntryJob,
    FeedEntryTopic,
    FeedEntryVector,
    JobStatus,
    ProcessingRun,
    RunStatus,
)

if TYPE_CHECKING:
    import uuid

    from sqlalchemy.ext.asyncio import AsyncSession

    from core.db.table_resolver import TableContext


class ProcessingRunRepo:
    """CRUD for processing_runs table (static, ORM-managed)."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_run(self, run_id: str, tier: str) -> ProcessingRun:
        try:
            run = ProcessingRun(
                run_id=run_id,
                status=RunStatus.PENDING,
                pipeline_tier=tier,
                started_at=datetime.now(UTC),
            )
            self.session.add(run)
            await self.session.flush()
            return run
        except Exception as e:
            logger.error("error_creating_run", error=str(e))
            raise

    async def update_status(self, run_id: str, status: RunStatus, **kwargs: object) -> None:
        stmt = (
            update(ProcessingRun)
            .where(ProcessingRun.run_id == run_id)
            .values(status=status, **kwargs)
        )
        await self.session.execute(stmt)

    async def mark_completed(self, run_id: str, metrics: dict[str, Any]) -> None:
        await self.update_status(
            run_id,
            RunStatus.COMPLETED,
            completed_at=datetime.now(UTC),
            metrics=metrics,
        )


class FeedEntryRepo:
    """Queries against date-partitioned feed_entries tables.

    All methods require a table_ctx or explicit table_name parameter
    since the physical table name changes with each date partition.

    The feed_entries table has two kinds of columns:
    - Filled on insert (by upstream project): id, flashpoint_id, url, title,
      seendate, domain, language, sourcecountry, description, image
    - Filled by THIS project (enrichment): title_en, images, hostname,
      content, summary, entities, geo_entities
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_unprocessed(
        self, table_ctx: TableContext, run_id: str, limit: int = 1000
    ) -> list[dict[str, Any]]:
        """Get entries that haven't been fully processed yet.

        An entry is considered "unprocessed" when:
        1. It has a flashpoint_id (belongs to a flashpoint)
        2. It does NOT have a job in ANY run that reached a terminal
           success state (summarized / scored)
        3. It doesn't already have a job in the CURRENT run

        This ensures partially-processed entries from failed/interrupted
        runs are picked up again on the next run.

        Returns dicts (not ORM objects) since we query date-partitioned tables.
        """
        feed_table = table_ctx.feed_entries

        result = await self.session.execute(
            text(f"""
                SELECT fe.id, fe.flashpoint_id, fe.url, fe.title, fe.title_en,
                       fe.seendate, fe.domain, fe.language, fe.sourcecountry,
                       fe.description, fe.image, fe.images, fe.hostname,
                       fe.content, fe.summary,
                       fe.entities, fe.geo_entities, fe.created_at, fe.updated_at
                FROM "{feed_table}" fe
                WHERE fe.flashpoint_id IS NOT NULL
                AND fe.id NOT IN (
                    SELECT feed_entry_id FROM feed_entry_jobs
                    WHERE status IN (:done_summarized, :done_scored)
                )
                AND fe.id NOT IN (
                    SELECT feed_entry_id FROM feed_entry_jobs
                    WHERE run_id = :run_id
                )
                LIMIT :limit
            """),
            {
                "run_id": run_id,
                "done_summarized": JobStatus.SUMMARIZED.value,
                "done_scored": JobStatus.SCORED.value,
                "limit": limit,
            },
        )
        columns = result.keys()
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]

    async def get_entries_for_flashpoint(
        self,
        table_ctx: TableContext,
        flashpoint_id: uuid.UUID,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """Get all entries for a flashpoint that passed extraction in this run."""
        feed_table = table_ctx.feed_entries

        valid_statuses = [
            JobStatus.EXTRACTED.value,
            JobStatus.DEDUPED.value,
            JobStatus.EMBEDDED.value,
        ]

        result = await self.session.execute(
            text(f"""
                SELECT fe.id, fe.flashpoint_id, fe.url, fe.title, fe.title_en,
                       fe.seendate, fe.domain, fe.language, fe.sourcecountry,
                       fe.description, fe.image, fe.images, fe.hostname,
                       fe.content, fe.compressed_content, fe.summary,
                       fe.entities, fe.geo_entities, fe.created_at, fe.updated_at
                FROM "{feed_table}" fe
                JOIN feed_entry_jobs jej ON fe.id = jej.feed_entry_id
                WHERE fe.flashpoint_id = :flashpoint_id
                AND jej.run_id = :run_id
                AND jej.status IN :statuses
                AND jej.is_duplicate = false
            """),
            {
                "flashpoint_id": flashpoint_id,
                "run_id": run_id,
                "statuses": tuple(valid_statuses),
            },
        )
        columns = result.keys()
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]

    async def update_enrichment(
        self,
        table_ctx: TableContext,
        entry_id: uuid.UUID,
        *,
        content: str | None = None,
        title_en: str | None = None,
        hostname: str | None = None,
        summary: str | None = None,
        entities: dict[str, Any] | None = None,
        geo_entities: list[dict[str, Any]] | None = None,
        images: list[str] | None = None,
    ) -> None:
        """Update all enrichment fields for a feed entry.

        Only updates fields that are explicitly passed (not None).
        This writes back to the date-partitioned feed_entries table.
        """
        feed_table = table_ctx.feed_entries

        # Build SET clause dynamically based on what's provided
        set_parts: list[str] = []
        params: dict[str, Any] = {"entry_id": entry_id}

        if content is not None:
            set_parts.append("content = :content")
            params["content"] = content

        if title_en is not None:
            set_parts.append("title_en = :title_en")
            params["title_en"] = title_en

        if hostname is not None:
            set_parts.append("hostname = :hostname")
            params["hostname"] = hostname

        if summary is not None:
            set_parts.append("summary = :summary")
            params["summary"] = summary

        if entities is not None:
            set_parts.append("entities = CAST(:entities AS jsonb)")
            params["entities"] = json.dumps(entities)

        if geo_entities is not None:
            set_parts.append("geo_entities = CAST(:geo_entities AS jsonb)")
            params["geo_entities"] = json.dumps(geo_entities)

        if images is not None:
            # images is TEXT[] in the DB, not JSONB
            set_parts.append("images = :images")
            params["images"] = images

        if not set_parts:
            return

        set_parts.append("updated_at = :updated_at")
        params["updated_at"] = datetime.now(UTC)

        set_clause = ", ".join(set_parts)

        await self.session.execute(
            text(f'UPDATE "{feed_table}" SET {set_clause} WHERE id = :entry_id'),
            params,
        )

    async def get_flashpoint_ids_for_run(
        self, table_ctx: TableContext, run_id: str
    ) -> list[uuid.UUID]:
        """Get distinct flashpoint_ids with processed entries in this run."""
        feed_table = table_ctx.feed_entries

        result = await self.session.execute(
            text(f"""
                SELECT DISTINCT fe.flashpoint_id
                FROM "{feed_table}" fe
                JOIN feed_entry_jobs jej ON fe.id = jej.feed_entry_id
                WHERE fe.flashpoint_id IS NOT NULL
                AND jej.run_id = :run_id
                AND jej.is_duplicate = false
                AND jej.status != :failed_status
            """),
            {"run_id": run_id, "failed_status": JobStatus.FAILED.value},
        )
        return [row[0] for row in result.fetchall()]


class FlashPointRepo:
    """Read-only queries against date-partitioned flash_point tables."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_all(self, table_ctx: TableContext) -> list[dict[str, Any]]:
        """Get all flashpoints for the target date."""
        fp_table = table_ctx.flash_point

        result = await self.session.execute(
            text(f"""
                SELECT id, title, description, entities, domains,
                       run_id, created_at, updated_at
                FROM "{fp_table}"
                ORDER BY created_at DESC
            """),
        )
        columns = result.keys()
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]

    async def get_by_id(
        self, table_ctx: TableContext, flashpoint_id: uuid.UUID
    ) -> dict[str, Any] | None:
        """Get a single flashpoint by ID."""
        fp_table = table_ctx.flash_point

        result = await self.session.execute(
            text(f"""
                SELECT id, title, description, entities, domains,
                       run_id, created_at, updated_at
                FROM "{fp_table}"
                WHERE id = :fp_id
            """),
            {"fp_id": flashpoint_id},
        )
        row = result.fetchone()
        if not row:
            return None
        return dict(zip(result.keys(), row, strict=True))


class FeedEntryJobRepo:
    """CRUD for per-entry job tracking (static sidecar table)."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_job(self, feed_entry_id: uuid.UUID, run_id: str) -> FeedEntryJob:
        job = FeedEntryJob(
            feed_entry_id=feed_entry_id,
            run_id=run_id,
            status=JobStatus.QUEUED,
        )
        self.session.add(job)
        await self.session.flush()
        return job

    async def claim_job(self, feed_entry_id: uuid.UUID, run_id: str) -> bool:
        """Idempotent claim — uses ON CONFLICT to prevent double-processing."""
        stmt = pg_insert(FeedEntryJob).values(
            feed_entry_id=feed_entry_id,
            run_id=run_id,
            status=JobStatus.FETCHING,
            attempts=1,
        )
        stmt = stmt.on_conflict_do_nothing(constraint="uq_job_entry_run")
        result = await self.session.execute(stmt)
        return (result.rowcount or 0) > 0  # type: ignore[attr-defined]

    async def update_status(
        self, feed_entry_id: uuid.UUID, run_id: str, status: JobStatus, **kwargs: object
    ) -> None:
        stmt = (
            update(FeedEntryJob)
            .where(
                FeedEntryJob.feed_entry_id == feed_entry_id,
                FeedEntryJob.run_id == run_id,
            )
            .values(status=status, **kwargs)
        )
        await self.session.execute(stmt)

    async def mark_failed(
        self, feed_entry_id: uuid.UUID, run_id: str, error: str, reason: str | None = None
    ) -> None:
        values: dict[str, Any] = {
            "status": JobStatus.FAILED,
            "last_error": error[:2000],
        }
        if reason:
            values["failure_reason"] = reason
        stmt = (
            update(FeedEntryJob)
            .where(
                FeedEntryJob.feed_entry_id == feed_entry_id,
                FeedEntryJob.run_id == run_id,
            )
            .values(**values)
        )
        await self.session.execute(stmt)

    async def get_run_stats(self, run_id: str) -> dict[str, int]:
        """Aggregate status counts for a run."""
        stmt = (
            select(
                FeedEntryJob.status,
                func.count().label("count"),
            )
            .where(FeedEntryJob.run_id == run_id)
            .group_by(FeedEntryJob.status)
        )
        result = await self.session.execute(stmt)
        return {row.status.value: row.count for row in result}  # type: ignore[misc]


class VectorRepo:
    """CRUD for feed_entry_vectors (pgvector)."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert_embedding(
        self, feed_entry_id: uuid.UUID, embedding: list[float], model_name: str
    ) -> None:
        stmt = pg_insert(FeedEntryVector).values(
            feed_entry_id=feed_entry_id,
            embedding=embedding,
            model_name=model_name,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[FeedEntryVector.feed_entry_id],
            set_={"embedding": embedding, "model_name": model_name},
        )
        await self.session.execute(stmt)

    async def get_embeddings_for_flashpoint(
        self,
        table_ctx: TableContext,
        flashpoint_id: uuid.UUID,
        run_id: str,
    ) -> list[tuple[uuid.UUID, list[float]]]:
        """Get all embeddings for entries in a flashpoint (non-duplicate)."""
        feed_table = table_ctx.feed_entries

        result = await self.session.execute(
            text(f"""
                SELECT fev.feed_entry_id, fev.embedding
                FROM feed_entry_vectors fev
                JOIN "{feed_table}" fe ON fe.id = fev.feed_entry_id
                JOIN feed_entry_jobs jej ON fe.id = jej.feed_entry_id
                WHERE fe.flashpoint_id = :flashpoint_id
                AND jej.run_id = :run_id
                AND jej.is_duplicate = false
            """),
            {"flashpoint_id": flashpoint_id, "run_id": run_id},
        )
        return [
            (row[0], json.loads(row[1]) if isinstance(row[1], str) else row[1])
            for row in result.fetchall()
        ]


class ClusterRepo:
    """CRUD for cluster_members and news_clusters (date-partitioned output)."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert_cluster_members(self, members: list[dict[str, Any]]) -> None:
        if not members:
            return
        stmt = pg_insert(ClusterMember).values(members)
        stmt = stmt.on_conflict_do_nothing(constraint="uq_cluster_member_entry_run")
        await self.session.execute(stmt)

    @retry_on_disconnect()
    async def write_news_cluster(
        self,
        table_ctx: TableContext,
        flashpoint_id: uuid.UUID,
        cluster_id: int,
        summary: str,
        article_count: int,
        top_domains: list[str],
        languages: list[str],
        urls: list[str],
        images: list[str],
    ) -> None:
        """Write a news cluster to the date-partitioned output table."""
        nc_table = table_ctx.news_clusters

        await self.session.execute(
            text(f"""
                INSERT INTO "{nc_table}" (
                    flashpoint_id, cluster_id, summary, article_count,
                    top_domains, languages, urls, images
                )
                VALUES (
                    :flashpoint_id, :cluster_id, :summary, :article_count,
                    CAST(:top_domains AS jsonb), CAST(:languages AS jsonb), CAST(:urls AS jsonb), CAST(:images AS jsonb)
                )
            """),
            {
                "flashpoint_id": flashpoint_id,
                "cluster_id": cluster_id,
                "summary": summary,
                "article_count": article_count,
                "top_domains": json.dumps(top_domains),
                "languages": json.dumps(languages),
                "urls": json.dumps(urls),
                "images": json.dumps(images),
            },
        )

    @retry_on_disconnect()
    async def delete_clusters_for_flashpoint(
        self,
        table_ctx: TableContext,
        flashpoint_id: uuid.UUID,
    ) -> int:
        """Delete existing clusters for a flashpoint (for idempotent re-runs)."""
        nc_table = table_ctx.news_clusters

        result = await self.session.execute(
            text(f"""
                DELETE FROM "{nc_table}"
                WHERE flashpoint_id = :flashpoint_id
            """),
            {"flashpoint_id": flashpoint_id},
        )
        return result.rowcount or 0  # type: ignore[attr-defined]


class TopicRepo:
    """CRUD for feed_entry_topics."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert_topic(
        self,
        feed_entry_id: uuid.UUID,
        iptc_top_level: str,
        iptc_path: str,
        confidence: float,
    ) -> None:
        topic = FeedEntryTopic(
            feed_entry_id=feed_entry_id,
            iptc_top_level=iptc_top_level,
            iptc_path=iptc_path,
            confidence=confidence,
        )
        self.session.add(topic)
