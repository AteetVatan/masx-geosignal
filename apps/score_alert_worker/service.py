"""
Score & Alert Worker Service — hotspot scoring + alert dispatch.

Week 3 scope:
1. For each cluster in news_clusters (this run):
   - Compute hotspot score
   - Flag top N% as "hot"
2. Dispatch alerts for hot clusters (webhook/Slack/email stubs)
"""

from __future__ import annotations

import uuid
from typing import Sequence

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.config.settings import Settings
from core.db.models import ClusterMember, FeedEntryTopic
from core.db.table_resolver import TableContext
from core.pipeline.score import HotspotScore, compute_hotspot_score

logger = structlog.get_logger(__name__)


class ScoreAlertService:
    """Computes hotspot scores and dispatches alerts."""

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

    async def score_clusters(
        self, flashpoint_ids: Sequence[uuid.UUID]
    ) -> list[HotspotScore]:
        """Score all clusters for the given flashpoints."""
        all_scores: list[HotspotScore] = []
        nc_table = self.table_ctx.news_clusters

        for fp_id in flashpoint_ids:
            # Get clusters for this flashpoint from date-partitioned table
            result = await self.session.execute(
                text(f"""
                    SELECT id, flashpoint_id, cluster_id, summary, article_count,
                           top_domains, languages, urls, images, created_at
                    FROM "{nc_table}"
                    WHERE flashpoint_id = :fp_id
                """),
                {"fp_id": fp_id},
            )
            clusters = result.fetchall()

            for cluster in clusters:
                cluster_id = cluster[2]
                article_count = cluster[4]
                top_domains = cluster[5] or []
                created_at = cluster[9]

                # Compute diversity
                unique_domains = len(set(top_domains)) if isinstance(top_domains, list) else 0

                # Get primary topic (if available)
                primary_topic = await self._get_primary_topic(fp_id, cluster_id)

                score = compute_hotspot_score(
                    article_count=article_count,
                    unique_domains=unique_domains,
                    max_recency=created_at,
                    primary_topic=primary_topic,
                )
                score.cluster_id = cluster_id
                all_scores.append(score)

        # Determine top hotspots
        if all_scores:
            all_scores.sort(key=lambda s: s.score, reverse=True)
            top_n = max(1, int(len(all_scores) * self.settings.premium_llm_top_pct))
            for i in range(top_n):
                all_scores[i].is_top_hotspot = True

        logger.info(
            "scoring_complete",
            total_clusters=len(all_scores),
            top_hotspots=sum(1 for s in all_scores if s.is_top_hotspot),
        )

        return all_scores

    async def _get_primary_topic(
        self, flashpoint_id: uuid.UUID, cluster_id: int
    ) -> str:
        """Get the most common IPTC topic for entries in a cluster."""
        try:
            from sqlalchemy import select

            stmt = (
                select(FeedEntryTopic.iptc_top_level)
                .join(ClusterMember, ClusterMember.feed_entry_id == FeedEntryTopic.feed_entry_id)
                .where(
                    ClusterMember.flashpoint_id == flashpoint_id,
                    ClusterMember.run_id == self.run_id,
                )
                .limit(5)
            )
            result = await self.session.execute(stmt)
            topics = result.scalars().all()

            if topics:
                # Most common topic
                from collections import Counter

                counter = Counter(topics)
                return counter.most_common(1)[0][0]
        except Exception:
            pass

        return "unclassified"
