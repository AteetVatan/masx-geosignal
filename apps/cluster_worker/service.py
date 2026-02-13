"""
Cluster Worker Service — clusters entries per flashpoint_id.

For each flashpoint_id:
1. Retrieve embeddings for non-duplicate extracted entries
2. Run kNN + Union-Find clustering
3. Write cluster_members
4. Prepare cluster data for summarization
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import structlog

from core.db.models import JobStatus
from core.db.repositories import ClusterRepo, FeedEntryJobRepo, VectorRepo
from core.pipeline.cluster import cluster_entries

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from core.config.settings import Settings
    from core.db.table_resolver import TableContext

logger = structlog.get_logger(__name__)


class ClusterService:
    """Clusters articles per flashpoint."""

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
        self.vector_repo = VectorRepo(session)
        self.cluster_repo = ClusterRepo(session)
        self.job_repo = FeedEntryJobRepo(session)

    async def cluster_flashpoint(self, flashpoint_id: uuid.UUID) -> int:
        """
        Cluster all entries for a single flashpoint.

        Returns number of clusters created.
        """
        structlog.contextvars.bind_contextvars(
            flashpoint_id=str(flashpoint_id),
        )

        # 1. Get embeddings
        embeddings_data = await self.vector_repo.get_embeddings_for_flashpoint(
            self.table_ctx, flashpoint_id, self.run_id
        )

        if len(embeddings_data) < 2:
            logger.info("too_few_entries_for_clustering", count=len(embeddings_data))
            if embeddings_data:
                # Single entry = single cluster
                entry_id = embeddings_data[0][0]
                cluster_uuid = uuid.uuid4()
                await self.cluster_repo.insert_cluster_members(
                    [
                        {
                            "flashpoint_id": flashpoint_id,
                            "cluster_uuid": cluster_uuid,
                            "feed_entry_id": entry_id,
                            "run_id": self.run_id,
                            "similarity": 1.0,
                        }
                    ]
                )
                await self.job_repo.update_status(entry_id, self.run_id, JobStatus.CLUSTERED)
                return 1
            return 0

        entry_ids = [d[0] for d in embeddings_data]
        embeddings = [d[1] for d in embeddings_data]

        # 2. Run clustering algorithm
        assignments = cluster_entries(
            entry_ids=entry_ids,
            embeddings=embeddings,
            k=self.settings.cluster_knn_k,
            cosine_threshold=self.settings.cluster_cosine_threshold,
        )

        if not assignments:
            return 0

        # 3. Write cluster members
        members = [
            {
                "flashpoint_id": flashpoint_id,
                "cluster_uuid": a.cluster_uuid,
                "feed_entry_id": a.feed_entry_id,
                "run_id": self.run_id,
                "similarity": a.similarity,
            }
            for a in assignments
        ]
        await self.cluster_repo.insert_cluster_members(members)

        # 4. Update job status
        for a in assignments:
            await self.job_repo.update_status(a.feed_entry_id, self.run_id, JobStatus.CLUSTERED)

        # Count unique clusters
        unique_clusters = len(set(a.cluster_uuid for a in assignments))
        logger.info(
            "flashpoint_clustered",
            entries=len(assignments),
            clusters=unique_clusters,
        )

        return unique_clusters
