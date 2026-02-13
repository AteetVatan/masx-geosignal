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
import time
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
from sqlalchemy import text

logger = structlog.get_logger(__name__)


async def run_pipeline(
    target_date: date | None = None,
    *,
    raw_date_suffix: str | None = None,
) -> None:
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
        target_date=str(target_date) if target_date else (raw_date_suffix or "latest"),
    )

    session_factory = get_async_session()

    async with session_factory() as session:
        run_repo = ProcessingRunRepo(session)
        entry_repo = FeedEntryRepo(session)
        job_repo = FeedEntryJobRepo(session)

        # 1. Resolve date-partitioned tables
        if raw_date_suffix:
            # Raw suffix mode — bypass TableContext.create
            suffix = raw_date_suffix.replace("-", "")
            table_ctx = TableContext(
                feed_entries=f"feed_entries_{suffix}",
                flash_point=f"flash_point_{suffix}",
                news_clusters=f"news_clusters_{suffix}",
                target_date=date.today(),  # placeholder for logging
            )
        else:
            table_ctx = await TableContext.create(session, target_date)
        logger.info("tables_resolved", table_ctx=repr(table_ctx))

        # 2. Ensure output table exists
        if raw_date_suffix:
            # Create the output table directly (can't use ensure_output_table with fake date)
            suffix = raw_date_suffix.replace("-", "")
            nc_table = f"news_clusters_{suffix}"
            await session.execute(
                text(f"""
                    CREATE TABLE IF NOT EXISTS "{nc_table}" (
                        id BIGSERIAL PRIMARY KEY,
                        flashpoint_id uuid NOT NULL,
                        cluster_id integer NOT NULL,
                        summary text NOT NULL,
                        article_count integer NOT NULL,
                        top_domains jsonb DEFAULT '[]'::jsonb,
                        languages jsonb DEFAULT '[]'::jsonb,
                        urls jsonb DEFAULT '[]'::jsonb,
                        images jsonb DEFAULT '[]'::jsonb,
                        created_at timestamptz DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            )
            await session.commit()
        else:
            await ensure_output_table(session, table_ctx.target_date)

        # 3. Create processing run
        await run_repo.create_run(run_id, settings.pipeline_tier.value, target_date=str(table_ctx.target_date))
        await session.commit()

        try:
            # 4. Mark run as running
            await run_repo.update_status(run_id, RunStatus.RUNNING)
            await session.commit()

            # ── Timing bookkeeping ──
            timings: dict[str, float] = {}
            pipeline_t0 = time.perf_counter()

            # 5. Select entries to process
            t0 = time.perf_counter()
            entries = await entry_repo.get_unprocessed(table_ctx, run_id, limit=20000)
            total = len(entries)
            timings["entry_selection"] = time.perf_counter() - t0
            logger.info("entries_selected", total=total)

            if total == 0:
                logger.info("no_entries_to_process")
                await run_repo.update_status(run_id, RunStatus.COMPLETED, total_entries=0)
                await session.commit()
                return

            # 6. Create job records (claims) — single bulk INSERT
            t0 = time.perf_counter()
            entry_ids = [entry["id"] for entry in entries]
            claimed = await job_repo.claim_jobs_bulk(entry_ids, run_id)
            await session.commit()
            timings["job_claiming"] = time.perf_counter() - t0

            logger.info("jobs_claimed", claimed=claimed, total=total)

            # 7. Run ingestion stage (fetch + extract + enrich + dedupe + embed)
            from apps.ingest_worker.service import IngestService

            t0 = time.perf_counter()
            ingest_svc = IngestService(session, run_id, settings, table_ctx)
            await ingest_svc.process_batch(entries)
            await session.commit()
            session.expunge_all()  # free ORM identity map after ingestion
            timings["ingestion_total"] = time.perf_counter() - t0
            logger.info("pipeline_stage_done", stage="ingestion", entries=total, elapsed_s=round(timings["ingestion_total"], 2))

            # Free DistilBART worker processes (~5 GB) now that
            # local pre-summarisation is complete.
            from core.pipeline.local_summarizer import shutdown_pool
            shutdown_pool()

            # 8. Run clustering if tier allows
            if settings.tier_has_clustering:
                from apps.cluster_worker.service import ClusterService

                t0 = time.perf_counter()
                cluster_svc = ClusterService(session, run_id, settings, table_ctx)
                flashpoint_ids = await entry_repo.get_flashpoint_ids_for_run(table_ctx, run_id)
                logger.info("clustering_flashpoints", count=len(flashpoint_ids))

                clusters_created = 0
                for fp_id in flashpoint_ids:
                    count = await cluster_svc.cluster_flashpoint(fp_id)
                    clusters_created += count
                await session.commit()
                session.expunge_all()  # free ORM identity map after clustering
                timings["clustering"] = time.perf_counter() - t0

                logger.info(
                    "pipeline_stage_done",
                    stage="clustering",
                    clusters_created=clusters_created,
                    elapsed_s=round(timings["clustering"], 2),
                )

                # 9. Summarization
                from apps.summary_worker.service import SummaryService

                t0 = time.perf_counter()
                summary_svc = SummaryService(session, run_id, settings, table_ctx)
                await summary_svc.summarize_all_clusters(flashpoint_ids)
                await session.commit()
                session.expunge_all()  # free ORM identity map after summarization
                timings["summarization"] = time.perf_counter() - t0

                logger.info(
                    "pipeline_stage_done",
                    stage="summarization",
                    elapsed_s=round(timings["summarization"], 2),
                )

            # 10. Gather stats
            timings["pipeline_total"] = time.perf_counter() - pipeline_t0

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

            # ── Timing summary → logs/pipeline_timings.log ──
            from pathlib import Path

            timing_lines = []
            for stage, secs in timings.items():
                mins, s = divmod(secs, 60)
                timing_lines.append(f"  {stage:.<30s} {int(mins)}m {s:05.2f}s")

            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            with open(log_dir / "pipeline_timings.log", "a", encoding="utf-8") as f:
                f.write(f"\n{'='*50}\n")
                f.write(f"  Run:     {run_id}\n")
                f.write(f"  Date:    {table_ctx.target_date}\n")
                f.write(f"  Tier:    {settings.pipeline_tier.value}\n")
                f.write(f"  Entries: {total}\n")
                f.write(f"  Time:    {datetime.now(UTC).isoformat()}\n")
                f.write(f"{'='*50}\n")
                for line in timing_lines:
                    f.write(line + "\n")
                f.write(f"{'='*50}\n")

            logger.info(
                "pipeline_timing_summary",
                timings={k: round(v, 2) for k, v in timings.items()},
            )

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
    raw_suffix = None
    if target_date_str:
        try:
            target_date = date.fromisoformat(target_date_str)
        except ValueError:
            # Non-ISO date string (e.g. 8888-88-88) — use as raw suffix
            raw_suffix = target_date_str

    try:
        asyncio.run(run_pipeline(target_date, raw_date_suffix=raw_suffix))
    except Exception:
        logger.exception("orchestrator_crashed")
        sys.exit(1)


if __name__ == "__main__":
    cli()
