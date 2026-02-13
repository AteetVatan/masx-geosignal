"""
Orchestrator — daily run coordinator.

This is the entry point for the daily pipeline. Designed as a Railway
cron job that runs and terminates.

Responsibilities:
1. Resolve date-partitioned tables (feed_entries_YYYYMMDD etc.)
2. Create a processing_run record
3. Select feed_entries to process
4. Dispatch to ingestion, clustering, summarization stages
5. Record metrics and exit
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from datetime import UTC, date, datetime

import click
import structlog

from core.config import get_settings
from core.config.logging import setup_logging
from core.db.engine import get_async_session
from core.db.models import RunStatus
from core.db.repositories import FeedEntryJobRepo, FeedEntryRepo, ProcessingRunRepo
from core.db.table_resolver import TableContext, ensure_output_table

logger = structlog.get_logger(__name__)


async def run_pipeline(target_date: date | None = None) -> None:
    """Execute the full pipeline for a single daily run."""
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_format)

    run_id = f"run_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    structlog.contextvars.bind_contextvars(
        run_id=run_id,
        tier=settings.pipeline_tier.value,
    )

    logger.info(
        "pipeline_starting",
        tier=settings.pipeline_tier.value,
        max_concurrent=settings.max_concurrent_fetches,
        target_date=str(target_date) if target_date else "latest",
    )

    session_factory = get_async_session()

    async with session_factory() as session:
        run_repo = ProcessingRunRepo(session)
        entry_repo = FeedEntryRepo(session)
        job_repo = FeedEntryJobRepo(session)

        # 1. Resolve date-partitioned tables
        table_ctx = await TableContext.create(session, target_date)
        logger.info("tables_resolved", table_ctx=repr(table_ctx))

        # 2. Ensure output table exists
        await ensure_output_table(session, table_ctx.target_date)

        # 3. Create processing run
        await run_repo.create_run(run_id, settings.pipeline_tier.value)
        await session.commit()

        try:
            # 4. Mark run as running
            await run_repo.update_status(run_id, RunStatus.RUNNING)
            await session.commit()

            # 5. Select entries to process
            entries = await entry_repo.get_unprocessed(table_ctx, run_id, limit=10000)
            total = len(entries)
            logger.info("entries_selected", total=total)

            if total == 0:
                logger.info("no_entries_to_process")
                await run_repo.update_status(run_id, RunStatus.COMPLETED, total_entries=0)
                await session.commit()
                return

            # 6. Create job records (claims)
            claimed = 0
            for entry in entries:
                if await job_repo.claim_job(entry["id"], run_id):
                    claimed += 1
            await session.commit()

            logger.info("jobs_claimed", claimed=claimed, total=total)

            # 7. Run ingestion stage
            from apps.ingest_worker.service import IngestService

            ingest_svc = IngestService(session, run_id, settings, table_ctx)
            await ingest_svc.process_batch(entries)
            await session.commit()

            # 8. Run clustering if tier allows
            if settings.tier_has_clustering:
                from apps.cluster_worker.service import ClusterService

                cluster_svc = ClusterService(session, run_id, settings, table_ctx)
                flashpoint_ids = await entry_repo.get_flashpoint_ids_for_run(table_ctx, run_id)
                logger.info("clustering_flashpoints", count=len(flashpoint_ids))

                clusters_created = 0
                for fp_id in flashpoint_ids:
                    count = await cluster_svc.cluster_flashpoint(fp_id)
                    clusters_created += count
                await session.commit()

                logger.info("clustering_complete", clusters_created=clusters_created)

                # 9. Summarization
                from apps.summary_worker.service import SummaryService

                summary_svc = SummaryService(session, run_id, settings, table_ctx)
                await summary_svc.summarize_all_clusters(flashpoint_ids)
                await session.commit()

                logger.info("summarization_complete")

            # 10. Gather stats
            stats = await job_repo.get_run_stats(run_id)
            metrics = {
                "total_entries": total,
                "claimed": claimed,
                "stats": stats,
                "tier": settings.pipeline_tier.value,
                "target_date": str(table_ctx.target_date),
                "tables": {
                    "feed_entries": table_ctx.feed_entries,
                    "flash_point": table_ctx.flash_point,
                    "news_clusters": table_ctx.news_clusters,
                },
            }

            await run_repo.mark_completed(run_id, metrics)
            await session.commit()

            logger.info("pipeline_completed", **metrics)

        except Exception as exc:
            logger.exception("pipeline_failed", error=str(exc))
            await run_repo.update_status(
                run_id,
                RunStatus.FAILED,
                error_message=str(exc)[:2000],
            )
            await session.commit()
            raise


@click.command()
@click.option("--tier", type=click.Choice(["A", "B", "C"]), default=None)
@click.option(
    "--date",
    "target_date_str",
    type=str,
    default=None,
    help="Target date (YYYY-MM-DD) for feed tables. Defaults to latest available.",
)
def cli(tier: str | None, target_date_str: str | None) -> None:
    """Run the daily news ingestion pipeline."""
    import os

    if tier:
        os.environ["PIPELINE_TIER"] = tier

    target_date = None
    if target_date_str:
        target_date = date.fromisoformat(target_date_str)

    try:
        asyncio.run(run_pipeline(target_date))
    except Exception:
        logger.exception("orchestrator_crashed")
        sys.exit(1)


if __name__ == "__main__":
    cli()
