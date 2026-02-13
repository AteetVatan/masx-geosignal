"""
Summary Worker Service — produces news_clusters rows.

For each flashpoint_id:
1. Load cluster_members + associated feed_entries (from date-partitioned table)
2. Group by cluster_uuid
3. Summarize each cluster (local or LLM batch)
4. Write rows to date-partitioned news_clusters table
"""

from __future__ import annotations

import time
import uuid
from collections import defaultdict
from typing import TYPE_CHECKING, Any

import structlog
from sqlalchemy import text

from core.db.engine import retry_on_disconnect
from core.db.models import JobStatus
from core.db.repositories import ClusterRepo, FeedEntryJobRepo
from core.pipeline.summarize import (
    ClusterSummaryInput,
    aggregate_cluster_metadata,
    summarize_batch_llm,
    summarize_cluster_local,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession

    from core.config.settings import Settings
    from core.db.table_resolver import TableContext

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

    async def summarize_all_clusters(self, flashpoint_ids: Sequence[uuid.UUID]) -> int:
        """Summarize clusters for all flashpoints. Returns total clusters written."""
        total = 0
        for fp_id in flashpoint_ids:
            count = await self._summarize_flashpoint(fp_id)
            total += count
        return total

    @retry_on_disconnect()
    async def _summarize_flashpoint(self, flashpoint_id: uuid.UUID) -> int:
        """Summarize all clusters for one flashpoint → write news_clusters."""
        structlog.contextvars.bind_contextvars(flashpoint_id=str(flashpoint_id))
        _t0 = time.perf_counter()

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
        clusters: dict[uuid.UUID, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            clusters[row[0]].append(
                {
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
                }
            )

        # 3. Sort clusters by size (desc) → assign dense-rank cluster_id
        sorted_clusters = sorted(
            clusters.items(),
            key=lambda x: len(x[1]),
            reverse=True,
        )

        # 4. Delete existing clusters for this flashpoint (idempotent)
        await self.cluster_repo.delete_clusters_for_flashpoint(self.table_ctx, flashpoint_id)

        # 5. Build ClusterSummaryInput list
        cluster_inputs: list[ClusterSummaryInput] = []
        for cluster_rank, (cluster_uuid, articles) in enumerate(sorted_clusters, start=1):
            cluster_inputs.append(
                ClusterSummaryInput(
                    flashpoint_id=flashpoint_id,
                    cluster_id=cluster_rank,
                    cluster_uuid=cluster_uuid,
                    articles=articles,
                )
            )

        # 6. Summarize — LLM batches (Tier C) or local extractive (Tier A/B)
        written = 0

        if self.settings.tier_has_llm:
            # Tier C: concurrent batched LLM summarization
            batch_size = self.settings.llm_summarize_batch_size
            logger.info(
                "llm_summarization_starting",
                clusters=len(cluster_inputs),
                batch_size=batch_size,
            )
            summaries = await summarize_batch_llm(cluster_inputs, batch_size=batch_size)

            for ci, summary_text in zip(cluster_inputs, summaries):
                metadata = aggregate_cluster_metadata(ci.articles)
                await self.cluster_repo.write_news_cluster(
                    table_ctx=self.table_ctx,
                    flashpoint_id=flashpoint_id,
                    cluster_id=ci.cluster_id,
                    summary=summary_text,
                    article_count=len(ci.articles),
                    top_domains=metadata["top_domains"],
                    languages=metadata["languages"],
                    urls=metadata["urls"],
                    images=metadata["images"],
                )
                written += 1
        else:
            # Tier A/B: local extractive summary
            for ci in cluster_inputs:
                summary_result = summarize_cluster_local(ci)
                await self.cluster_repo.write_news_cluster(
                    table_ctx=self.table_ctx,
                    flashpoint_id=flashpoint_id,
                    cluster_id=summary_result.cluster_id,
                    summary=summary_result.summary,
                    article_count=summary_result.article_count,
                    top_domains=summary_result.top_domains,
                    languages=summary_result.languages,
                    urls=summary_result.urls,
                    images=summary_result.images,
                )
                written += 1

        # Bulk update job statuses
        all_entry_ids = [
            uuid.UUID(article["feed_entry_id"])
            for _, articles in sorted_clusters
            for article in articles
        ]
        if all_entry_ids:
            await self.job_repo.bulk_update_status(
                all_entry_ids, self.run_id, JobStatus.SUMMARIZED
            )

        logger.info("summarize_flashpoint_done", clusters_written=written, elapsed_s=round(time.perf_counter() - _t0, 2))
        return written

