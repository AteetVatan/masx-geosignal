"""
Summary Worker Service — produces news_clusters rows.

For each flashpoint_id:
1. Load cluster_members + associated feed_entries (from date-partitioned table)
2. Group by cluster_uuid
3. Summarize each cluster (local or LLM batch)
4. Write rows to date-partitioned news_clusters table
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from typing import Sequence

import structlog
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config.settings import Settings
from core.db.models import ClusterMember, JobStatus
from core.db.repositories import ClusterRepo, FeedEntryJobRepo
from core.db.table_resolver import TableContext
from core.pipeline.summarize import (
    ClusterSummaryInput,
    aggregate_cluster_metadata,
    build_batch_request,
    summarize_cluster_local,
    write_batch_file,
)

logger = structlog.get_logger(__name__)


class SummaryService:
    """Produces news_clusters summaries."""

    def __init__(
        self,
        session: AsyncSession,
        run_id: str,
        settings: Settings,
        table_ctx: TableContext,
    ) -> None:
        self.session = session
        self.run_id = run_id
        self.settings = settings
        self.table_ctx = table_ctx
        self.cluster_repo = ClusterRepo(session)
        self.job_repo = FeedEntryJobRepo(session)

    async def summarize_all_clusters(
        self, flashpoint_ids: Sequence[uuid.UUID]
    ) -> int:
        """Summarize clusters for all flashpoints. Returns total clusters written."""
        total = 0
        for fp_id in flashpoint_ids:
            count = await self._summarize_flashpoint(fp_id)
            total += count
        return total

    async def _summarize_flashpoint(self, flashpoint_id: uuid.UUID) -> int:
        """Summarize all clusters for one flashpoint → write news_clusters."""
        structlog.contextvars.bind_contextvars(flashpoint_id=str(flashpoint_id))

        feed_table = self.table_ctx.feed_entries

        # 1. Load cluster members with their entries from the date-partitioned table
        result = await self.session.execute(
            text(f"""
                SELECT cm.cluster_uuid, cm.feed_entry_id, cm.similarity,
                       fe.title, fe.title_en, fe.content, fe.description,
                       fe.url, fe.domain, fe.hostname, fe.language,
                       fe.image, fe.images
                FROM cluster_members cm
                JOIN "{feed_table}" fe ON fe.id = cm.feed_entry_id
                WHERE cm.flashpoint_id = :flashpoint_id
                AND cm.run_id = :run_id
                ORDER BY cm.cluster_uuid, cm.similarity DESC
            """),
            {"flashpoint_id": flashpoint_id, "run_id": self.run_id},
        )
        rows = result.fetchall()

        if not rows:
            logger.info("no_clusters_to_summarize")
            return 0

        # 2. Group by cluster_uuid
        clusters: dict[uuid.UUID, list[dict]] = defaultdict(list)
        for row in rows:
            clusters[row[0]].append({
                "feed_entry_id": str(row[1]),
                "title": row[3],
                "title_en": row[4],
                "content": row[5],
                "description": row[6],
                "url": row[7],
                "domain": row[8],
                "hostname": row[9],
                "language": row[10],
                "image": row[11],
                "images": row[12] or [],
                "similarity": row[2],
            })

        # 3. Sort clusters by size (desc) → assign dense-rank cluster_id
        sorted_clusters = sorted(
            clusters.items(),
            key=lambda x: len(x[1]),
            reverse=True,
        )

        # 4. Delete existing clusters for this flashpoint (idempotent)
        await self.cluster_repo.delete_clusters_for_flashpoint(
            self.table_ctx, flashpoint_id
        )

        # 5. Summarize + write
        batch_requests: list[dict] = []
        cluster_inputs: list[ClusterSummaryInput] = []
        written = 0

        for cluster_rank, (cluster_uuid, articles) in enumerate(sorted_clusters, start=1):
            cluster_input = ClusterSummaryInput(
                flashpoint_id=flashpoint_id,
                cluster_id=cluster_rank,
                cluster_uuid=cluster_uuid,
                articles=articles,
            )

            if self.settings.tier_has_llm:
                # Tier C: prepare batch request
                batch_requests.append(build_batch_request(cluster_input))
                cluster_inputs.append(cluster_input)
            else:
                # Tier A/B: local extractive summary
                summary_result = summarize_cluster_local(cluster_input)

                await self.cluster_repo.write_news_cluster(
                    table_ctx=self.table_ctx,
                    flashpoint_id=flashpoint_id,
                    cluster_id=cluster_rank,
                    summary=summary_result.summary,
                    article_count=summary_result.article_count,
                    top_domains=summary_result.top_domains,
                    languages=summary_result.languages,
                    urls=summary_result.urls,
                    images=summary_result.images,
                )
                written += 1

        # Handle batch LLM (Tier C)
        if batch_requests and self.settings.tier_has_llm:
            # Write batch file for async processing
            batch_file = f"/tmp/batch_{flashpoint_id}_{self.run_id}.jsonl"
            write_batch_file(batch_requests, batch_file)

            # For now, fall back to local summary (batch is async)
            for ci in cluster_inputs:
                sr = summarize_cluster_local(ci)
                await self.cluster_repo.write_news_cluster(
                    table_ctx=self.table_ctx,
                    flashpoint_id=flashpoint_id,
                    cluster_id=sr.cluster_id,
                    summary=sr.summary,
                    article_count=sr.article_count,
                    top_domains=sr.top_domains,
                    languages=sr.languages,
                    urls=sr.urls,
                    images=sr.images,
                )
                written += 1

            logger.info(
                "batch_file_ready",
                path=batch_file,
                clusters=len(batch_requests),
            )

        # Update job statuses
        for _, articles in sorted_clusters:
            for article in articles:
                try:
                    await self.job_repo.update_status(
                        uuid.UUID(article["feed_entry_id"]),
                        self.run_id,
                        JobStatus.SUMMARIZED,
                    )
                except Exception:
                    pass

        logger.info("flashpoint_summarized", clusters_written=written)
        return written
