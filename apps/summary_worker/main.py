"""
Summary Worker CLI — standalone entry point.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import date

import click
import structlog

from core.config import get_settings
from core.config.logging import setup_logging

logger = structlog.get_logger(__name__)


async def run_summary(run_id: str, target_date: date | None = None) -> None:
    """Execute summarization for all flashpoints in a run."""
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_format)

    from core.db.engine import get_async_session
    from core.db.repositories import FeedEntryRepo
    from core.db.table_resolver import TableContext

    session_factory = get_async_session()

    async with session_factory() as session:
        table_ctx = await TableContext.create(session, target_date)
        entry_repo = FeedEntryRepo(session)
        flashpoint_ids = await entry_repo.get_flashpoint_ids_for_run(table_ctx, run_id)

        if not flashpoint_ids:
            logger.info("no_flashpoints_to_summarize")
            return

        from apps.summary_worker.service import SummaryService

        svc = SummaryService(session, run_id, settings, table_ctx)
        total = await svc.summarize_all_clusters(flashpoint_ids)
        await session.commit()
        logger.info("summary_complete", total_clusters=total)


@click.command()
@click.argument("run_id")
@click.option("--date", "target_date_str", default=None, help="Target date (YYYY-MM-DD)")
def cli(run_id: str, target_date_str: str | None) -> None:
    """Run summarization for a given run."""
    target_date = date.fromisoformat(target_date_str) if target_date_str else None
    try:
        asyncio.run(run_summary(run_id, target_date))
    except Exception:
        logger.exception("summary_worker_crashed")
        sys.exit(1)


if __name__ == "__main__":
    cli()
